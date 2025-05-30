"""
Trailing profit/loss sell operations for pump.fun tokens.
"""

import asyncio
from typing import Optional, TYPE_CHECKING

from core.client import SolanaClient
from core.curve import BondingCurveManager
from core.priority_fee.manager import PriorityFeeManager
from core.pubkeys import LAMPORTS_PER_SOL, TOKEN_DECIMALS
from core.wallet import Wallet
from monitoring.price_listener import PriceListener
from trading.base import TokenInfo, TradeResult
from utils.sol_to_usd_converter import SolToUsdConverter
from trading.seller import TokenSeller
from utils.logger import get_logger
from decimal import Decimal

# For type hinting SharedWebsocketListener without circular imports
if TYPE_CHECKING:
    from monitoring.shared_websocket_listener import SharedWebsocketListener

logger = get_logger(__name__)


class TrailingTokenSeller(TokenSeller):
    """Handles selling tokens on pump.fun with trailing stop loss and take profit."""

    def __init__(
        self,
        client: SolanaClient,
        wallet: Wallet,
        curve_manager: BondingCurveManager,
        priority_fee_manager: PriorityFeeManager,
        shared_listener: 'SharedWebsocketListener',
        sol_to_usd_converter: SolToUsdConverter,
        slippage: float = 0.25,
        max_retries: int = 5,
        trailing_stop_percentage: float = 0.15,  # 15% trailing stop
        take_profit_percentage: float = 0.50,    # 50% take profit
        percent_sell_amount: float = 1.0,        # 100% of tokens to sell by default
        check_interval: float = 1.0,             # Only used as fallback if WebSocket fails
        stagnation_timeout: int = 15,            # Seconds of no price change before selling
        balance_fetch_retries: int = 5,          # Retries for fetching token balance
        balance_fetch_delay: float = 5.0,        # Delay between balance fetch retries
    ):
        """Initialize trailing token seller.

        Args:
            client: Solana client for RPC calls
            wallet: Wallet for signing transactions
            curve_manager: Bonding curve manager
            priority_fee_manager: Priority fee manager
            shared_listener: Instance of SharedWebsocketListener for price updates.
            slippage: Slippage tolerance (0.25 = 25%)
            max_retries: Maximum number of retry attempts
            trailing_stop_percentage: Trailing stop percentage (0.15 = 15%)
            take_profit_percentage: Take profit percentage (0.50 = 50%)
            percent_sell_amount: Percentage of tokens to sell at take profit (1.0 = 100%)
            check_interval: Price check interval in seconds (fallback only)
            stagnation_timeout: Seconds of no price change before selling
            balance_fetch_retries: Number of retries for fetching token balance
            balance_fetch_delay: Delay in seconds between balance fetch retries
        """
        super().__init__(
            client, wallet, curve_manager, priority_fee_manager, slippage, max_retries
        )
        self.shared_listener = shared_listener
        self.trailing_stop_percentage = trailing_stop_percentage
        self.take_profit_percentage = take_profit_percentage
        self.percent_sell_amount = percent_sell_amount
        self.check_interval = check_interval
        self.stagnation_timeout = stagnation_timeout
        self.balance_fetch_retries = balance_fetch_retries
        self.balance_fetch_delay = balance_fetch_delay
        self.sol_to_usd_converter = sol_to_usd_converter

        self.price_listener_instance: Optional[PriceListener] = None
        self.use_websocket = shared_listener is not None
        self._sell_in_progress = asyncio.Lock()  # Add a lock to prevent multiple simultaneous sells

    async def execute(self, token_info: TokenInfo, 
                      token_balance: int,
                     entry_price: Optional[float] = None,
                     percent_sell_amount: Optional[float] = None,
                     take_profit_percentage: Optional[float] = None,
                     *args, **kwargs) -> TradeResult:
        """Execute trailing sell operation.

        Args:
            token_info: Token information
            token_balance: Token balance in raw units
            entry_price: Entry price in SOL (if None, will be fetched)
            percent_sell_amount: Override for percentage of tokens to sell at take profit
            take_profit_percentage: Override for take profit percentage

        Returns:
            TradeResult with sell outcome
        """
        try:
            # Get associated token account
            associated_token_account = self.wallet.get_associated_token_address(
                token_info.mint
            )
            
            token_balance_decimal = token_balance / 10**TOKEN_DECIMALS  # TOKEN_DECIMALS

            logger.info(f"Token balance: {token_balance_decimal}")

            if token_balance == 0:
                logger.info("No tokens to sell.")
                return TradeResult(success=False, error_message="No tokens to sell")

            # Determine entry price if not provided
            if entry_price is None:
                # Estimate the entry price from current price if not provided
                curve_state = await self.curve_manager.get_curve_state(
                    token_info.bonding_curve
                )
                entry_price = curve_state.calculate_price()
                logger.info(f"Using current price as entry price: {entry_price} SOL")
            else:
                logger.info(f"Using provided entry price: {entry_price} SOL")

            # Use developer-specific parameters if provided, otherwise use the defaults
            sell_percent = percent_sell_amount if percent_sell_amount is not None else self.percent_sell_amount
            profit_target = take_profit_percentage if take_profit_percentage is not None else self.take_profit_percentage
            
            if percent_sell_amount is not None:
                logger.info(f"Using custom percent_sell_amount: {sell_percent}")
            if take_profit_percentage is not None:
                logger.info(f"Using custom take_profit_percentage: {profit_target}")

            return await self._monitor_price_and_sell(token_info, token_balance, entry_price, 
                                                    sell_percent, profit_target)

        except Exception as e:
            logger.error(f"Trailing sell operation failed: {e!s}")
            return TradeResult(success=False, error_message=str(e))

    async def _monitor_price_and_sell(
        self, token_info: TokenInfo, token_balance: int, entry_price: float,
        percent_sell_amount: float = None, take_profit_percentage: float = None
    ) -> TradeResult:
        """Monitor price and sell based on trailing stop/take profit conditions.

        Args:
            token_info: Token information
            token_balance: Token balance in raw units
            entry_price: Entry price in SOL
            percent_sell_amount: Percentage of tokens to sell at take profit (overrides instance default)
            take_profit_percentage: Take profit percentage (overrides instance default)

        Returns:
            TradeResult with sell outcome
        """
        # Use provided parameters or defaults
        sell_percent = percent_sell_amount if percent_sell_amount is not None else self.percent_sell_amount
        profit_target_percentage = take_profit_percentage if take_profit_percentage is not None else self.take_profit_percentage
        
        # Check if entry_price is a Decimal and handle accordingly
        from decimal import Decimal
        
        if isinstance(entry_price, Decimal):
            # If entry_price is Decimal, convert percentage values to Decimal
            highest_price = entry_price
            take_profit_percentage_decimal = Decimal(str(profit_target_percentage))
            trailing_stop_percentage_decimal = Decimal(str(self.trailing_stop_percentage))
            
            take_profit_target = entry_price * (Decimal('1') + take_profit_percentage_decimal)
            trailing_stop = entry_price * (Decimal('1') - trailing_stop_percentage_decimal)
        else:
            # Handle as regular float values
            highest_price = entry_price
            take_profit_target = entry_price * (1 + profit_target_percentage)
            trailing_stop = entry_price * (1 - self.trailing_stop_percentage)

        logger.info(f"Starting price monitoring with:")
        logger.info(f"  Entry price: {entry_price} SOL")
        logger.info(f"  Initial trailing stop: {trailing_stop} SOL")
        logger.info(f"  Take profit target: {take_profit_target} SOL")
        logger.info(f"  Percent to sell at take profit: {sell_percent*100:.1f}%")
        
        # Result will be set by the callback function
        result_event = asyncio.Event()
        sell_result: Optional[TradeResult] = None
        
        # Timeout to sell if no price changes occur
        price_stagnation_timeout = self.stagnation_timeout
        last_price_update_time = asyncio.get_event_loop().time()
        last_known_price = None  # Track the last known price for stagnation check fallback
        timeout_task = None
        
        if self.use_websocket:
            # Use WebSocket for real-time price monitoring via SharedWebsocketListener
            logger.info("Using SharedWebsocketListener for real-time price monitoring")
            try:
                # Create a PriceListener instance that uses the shared_listener
                self.price_listener_instance = PriceListener(
                    shared_listener=self.shared_listener, 
                    curve_manager=self.curve_manager, 
                    mint=token_info.mint,
                    sol_to_usd_converter=self.sol_to_usd_converter
                )
                
                # Define the price update callback
                async def on_price_update(price: float) -> None:
                    nonlocal highest_price, trailing_stop, sell_result, last_price_update_time, last_known_price
                    
                    # Check if a sell is already in progress or completed
                    if result_event.is_set() or not await self._try_acquire_sell_lock():
                        return  # Skip if sell is already in progress/completed or lock couldn't be acquired
                    
                    try:
                        # Update the last price update time and last known price
                        last_price_update_time = asyncio.get_event_loop().time()
                        last_known_price = price
                        
                        # Convert price to Decimal if entry_price is Decimal
                        if isinstance(entry_price, Decimal):
                            price_decimal = Decimal(str(price))
                            profit_percent = (price_decimal - entry_price) / entry_price * Decimal('100')
                            
                            logger.info(
                                f"WebSocket price update: {price:.8f} SOL (P/L: {profit_percent:.2f}%), "
                                f"Trailing stop: {trailing_stop:.8f} SOL"
                            )
                            
                            # Update trailing stop if price goes higher
                            if price_decimal > highest_price:
                                highest_price = price_decimal
                                trailing_stop = highest_price * (Decimal('1') - trailing_stop_percentage_decimal)
                                logger.info(f"New highest price: {highest_price:.8f} SOL, updated trailing stop: {trailing_stop:.8f} SOL")
                            
                            # Check if we should sell
                            if price_decimal <= trailing_stop:
                                logger.info(f"Trailing stop triggered at price: {price:.8f} SOL")
                                sell_result = await self._execute_sell(token_info, token_balance, price, is_take_profit=False)
                                result_event.set()
                            
                            # Check take profit target
                            elif price_decimal >= take_profit_target:
                                logger.info(f"Take profit target reached at price: {price:.8f} SOL")
                                sell_result = await self._execute_sell(token_info, token_balance, price, is_take_profit=True, percent_sell_amount=sell_percent)
                                result_event.set()
                        else:
                            # Original float-based logic
                            profit_percent = (price - entry_price) / entry_price * 100
                            
                            logger.info(
                                f"WebSocket price update: {price:.8f} SOL (P/L: {profit_percent:.2f}%), "
                                f"Trailing stop: {trailing_stop:.8f} SOL"
                            )
                            
                            # Update trailing stop if price goes higher
                            if price > highest_price:
                                highest_price = price
                                trailing_stop = highest_price * (1 - self.trailing_stop_percentage)
                                logger.info(f"New highest price: {highest_price:.8f} SOL, updated trailing stop: {trailing_stop:.8f} SOL")
                            
                            # Check if we should sell
                            if price <= trailing_stop:
                                logger.info(f"Trailing stop triggered at price: {price:.8f} SOL")
                                sell_result = await self._execute_sell(token_info, token_balance, price, is_take_profit=False)
                                result_event.set()
                            
                            # Check take profit target
                            elif price >= take_profit_target:
                                logger.info(f"Take profit target reached at price: {price:.8f} SOL")
                                sell_result = await self._execute_sell(token_info, token_balance, price, is_take_profit=True, percent_sell_amount=sell_percent)
                                result_event.set()
                    finally:
                        # Always release the lock when done
                        self._sell_in_progress.release()
                
                # Function to check for price stagnation
                async def check_price_stagnation():
                    nonlocal sell_result, last_known_price
                    
                    while not result_event.is_set():
                        current_time = asyncio.get_event_loop().time()
                        time_since_last_update = current_time - last_price_update_time
                        
                        if time_since_last_update >= price_stagnation_timeout:
                            # Try to acquire the sell lock
                            if not await self._try_acquire_sell_lock():
                                await asyncio.sleep(1)
                                continue

                            try:
                                # Check again after acquiring lock in case another thread sold already
                                if result_event.is_set():
                                    break
                                
                                # No price updates for the timeout period
                                logger.info(f"No price changes detected for {price_stagnation_timeout} seconds, selling token")
                                
                                # Try to get current price, but handle curve state errors gracefully
                                current_price = None
                                try:
                                    curve_state = await self.curve_manager.get_curve_state(token_info.bonding_curve)
                                    current_price = curve_state.calculate_price()
                                    logger.info(f"Using current price for stagnation sell: {current_price:.8f} SOL")
                                except (ValueError, Exception) as e:
                                    # If we can't get the current price, use the last known price or entry price
                                    if last_known_price is not None:
                                        current_price = last_known_price
                                        logger.warning(f"Could not fetch current price ({e}), using last known price: {current_price:.8f} SOL")
                                    else:
                                        # Fallback to entry price if no last known price
                                        current_price = float(entry_price) if isinstance(entry_price, Decimal) else entry_price
                                        logger.warning(f"Could not fetch current price ({e}), using entry price: {current_price:.8f} SOL")
                                
                                sell_result = await self._execute_sell(token_info, token_balance, current_price, is_take_profit=False)
                                result_event.set()
                                break
                            finally:
                                self._sell_in_progress.release()
                            
                        # Check again in 1 second
                        await asyncio.sleep(1)
                
                # Start the price listener (which registers with the shared listener)
                await self.price_listener_instance.start_monitoring(
                    price_callback=on_price_update
                )
                
                # Start the timeout checker
                timeout_task = asyncio.create_task(check_price_stagnation())
                
                # Wait for a sell signal or cancellation
                try:
                    await result_event.wait()
                    return sell_result
                except asyncio.CancelledError:
                    logger.info("WebSocket price monitoring cancelled")
                    raise
                finally:
                    # Make sure to stop the price listener and timeout task
                    if timeout_task and not timeout_task.done():
                        timeout_task.cancel()
                    if self.price_listener_instance:
                        await self.price_listener_instance.stop_monitoring()
                        self.price_listener_instance = None
                    
            except Exception as e:
                logger.error(f"An error occurred during SharedWS-based monitoring or its cleanup: {e!s}", exc_info=True)
                if result_event.is_set() and sell_result is not None:
                    logger.warning(
                        "Sell was already processed by WebSocket before the error. "
                        "Returning that result instead of falling back."
                    )
                    return sell_result
                else:
                    logger.info("Falling back to interval polling due to WebSocket error and no prior sell.")
                    # Fall back to interval polling if WebSocket fails
                    self.use_websocket = False
        
        # Fallback to interval polling if WebSocket is not available or failed
        if not self.use_websocket:
            logger.info(f"Using interval polling (every {self.check_interval}s) for price monitoring")
            
            # Reset stagnation timer for polling method
            last_price_change_time = asyncio.get_event_loop().time()
            last_price = None
            
            while True:
                try:
                    # Try to get current price, handle curve state errors gracefully
                    current_price = None
                    try:
                        curve_state = await self.curve_manager.get_curve_state(
                            token_info.bonding_curve
                        )
                        current_price = curve_state.calculate_price()
                    except (ValueError, Exception) as e:
                        # If we can't get the current price, use the last known price or entry price
                        if last_known_price is not None:
                            current_price = last_known_price
                            logger.warning(f"Could not fetch current price in polling mode ({e}), using last known price: {current_price:.8f} SOL")
                        else:
                            # Fallback to entry price if no last known price
                            current_price = float(entry_price) if isinstance(entry_price, Decimal) else entry_price
                            logger.warning(f"Could not fetch current price in polling mode ({e}), using entry price: {current_price:.8f} SOL")
                        
                        # If we can't get price, trigger stagnation sell after timeout
                        current_time = asyncio.get_event_loop().time()
                        time_since_last_change = current_time - last_price_change_time
                        
                        if time_since_last_change >= price_stagnation_timeout:
                            if await self._try_acquire_sell_lock():
                                try:
                                    logger.info(f"Could not fetch price for {price_stagnation_timeout} seconds, selling token")
                                    return await self._execute_sell(token_info, token_balance, current_price, is_take_profit=False)
                                finally:
                                    self._sell_in_progress.release()
                        
                        # Continue to next iteration
                        await asyncio.sleep(self.check_interval)
                        continue
                    
                    # Update last known price
                    last_known_price = current_price
                    
                    # Handle Decimal arithmetic if entry_price is Decimal
                    if isinstance(entry_price, Decimal):
                        if not isinstance(current_price, Decimal):
                            current_price = Decimal(str(current_price))
                        
                        current_value = current_price * Decimal(str(token_balance)) / Decimal('1e9')  # TOKEN_DECIMALS
                        profit_percent = (current_price - entry_price) / entry_price * Decimal('100')
                        
                        logger.info(
                            f"Current price: {current_price:.8f} SOL (P/L: {profit_percent:.2f}%), "
                            f"Trailing stop: {trailing_stop:.8f} SOL"
                        )
                        
                        # Check for price stagnation
                        if last_price is not None:
                            if abs(current_price - last_price) > Decimal('0.0000001'):  # Price changed (accounting for floating point precision)
                                last_price_change_time = asyncio.get_event_loop().time()
                            else:
                                # Check if price hasn't changed for the timeout period
                                current_time = asyncio.get_event_loop().time()
                                time_since_last_change = current_time - last_price_change_time
                                
                                if time_since_last_change >= price_stagnation_timeout:
                                    # Try to acquire the sell lock
                                    if await self._try_acquire_sell_lock():
                                        try:
                                            logger.info(f"No price changes detected for {price_stagnation_timeout} seconds, selling token")
                                            return await self._execute_sell(token_info, token_balance, float(current_price), is_take_profit=False)
                                        finally:
                                            self._sell_in_progress.release()
                        
                        # Store current price for next comparison
                        last_price = current_price

                        # Update trailing stop if price goes higher
                        if current_price > highest_price:
                            highest_price = current_price
                            trailing_stop = highest_price * (Decimal('1') - trailing_stop_percentage_decimal)
                            logger.info(f"New highest price: {highest_price:.8f} SOL, updated trailing stop: {trailing_stop:.8f} SOL")

                        # Check if we should sell - with lock protection
                        if current_price <= trailing_stop:
                            if await self._try_acquire_sell_lock():
                                try:
                                    logger.info(f"Trailing stop triggered at price: {current_price:.8f} SOL")
                                    return await self._execute_sell(token_info, token_balance, float(current_price), is_take_profit=False)
                                finally:
                                    self._sell_in_progress.release()
                        
                        # Check take profit target - with lock protection
                        if current_price >= take_profit_target:
                            if await self._try_acquire_sell_lock():
                                try:
                                    logger.info(f"Take profit target reached at price: {current_price:.8f} SOL")
                                    return await self._execute_sell(token_info, token_balance, float(current_price), is_take_profit=True, percent_sell_amount=sell_percent)
                                finally:
                                    self._sell_in_progress.release()
                    else:
                        # Original float-based logic
                        current_value = current_price * token_balance / 10**9  # TOKEN_DECIMALS
                        profit_percent = (current_price - entry_price) / entry_price * 100

                        logger.info(
                            f"Current price: {current_price:.8f} SOL (P/L: {profit_percent:.2f}%), "
                            f"Trailing stop: {trailing_stop:.8f} SOL"
                        )
                        
                        # Check for price stagnation
                        if last_price is not None:
                            if abs(current_price - last_price) > 0.0000001:  # Price changed (accounting for floating point precision)
                                last_price_change_time = asyncio.get_event_loop().time()
                            else:
                                # Check if price hasn't changed for the timeout period
                                current_time = asyncio.get_event_loop().time()
                                time_since_last_change = current_time - last_price_change_time
                                
                                if time_since_last_change >= price_stagnation_timeout:
                                    # Try to acquire the sell lock
                                    if await self._try_acquire_sell_lock():
                                        try:
                                            logger.info(f"No price changes detected for {price_stagnation_timeout} seconds, selling token")
                                            return await self._execute_sell(token_info, token_balance, current_price, is_take_profit=False)
                                        finally:
                                            self._sell_in_progress.release()
                        
                        # Store current price for next comparison
                        last_price = current_price

                        # Update trailing stop if price goes higher
                        if current_price > highest_price:
                            highest_price = current_price
                            trailing_stop = highest_price * (1 - self.trailing_stop_percentage)
                            logger.info(f"New highest price: {highest_price:.8f} SOL, updated trailing stop: {trailing_stop:.8f} SOL")

                        # Check if we should sell - with lock protection
                        if current_price <= trailing_stop:
                            if await self._try_acquire_sell_lock():
                                try:
                                    logger.info(f"Trailing stop triggered at price: {current_price:.8f} SOL")
                                    return await self._execute_sell(token_info, token_balance, current_price, is_take_profit=False)
                                finally:
                                    self._sell_in_progress.release()
                        
                        # Check take profit target - with lock protection
                        if current_price >= take_profit_target:
                            if await self._try_acquire_sell_lock():
                                try:
                                    logger.info(f"Take profit target reached at price: {current_price:.8f} SOL")
                                    return await self._execute_sell(token_info, token_balance, current_price, is_take_profit=True, percent_sell_amount=sell_percent)
                                finally:
                                    self._sell_in_progress.release()

                    await asyncio.sleep(self.check_interval)
                
                except asyncio.CancelledError:
                    logger.info("Price monitoring cancelled")
                    raise
                except Exception as e:
                    logger.error(f"Error monitoring price: {e!s}")
                    await asyncio.sleep(self.check_interval)

    async def _try_acquire_sell_lock(self, timeout: float = 0.5) -> bool:
        """Try to acquire the sell lock with a timeout.
        
        Args:
            timeout: Time in seconds to wait for the lock
            
        Returns:
            True if the lock was acquired, False otherwise
        """
        try:
            # Try to acquire the lock with a timeout
            await asyncio.wait_for(self._sell_in_progress.acquire(), timeout)
            return True
        except asyncio.TimeoutError:
            # Lock could not be acquired within the timeout
            return False

    async def _execute_sell(
        self, token_info: TokenInfo, token_balance: int, current_price: float, 
        is_take_profit: bool = False, percent_sell_amount: float = None
    ) -> TradeResult:
        """Execute the sell transaction.

        Args:
            token_info: Token information
            token_balance: Token balance in raw units
            current_price: Current token price in SOL
            is_take_profit: Whether this is a take profit trigger
            percent_sell_amount: Override for percentage of tokens to sell (for take profit)

        Returns:
            TradeResult with sell outcome
        """
        # Use provided sell percentage or default
        sell_percent = percent_sell_amount if percent_sell_amount is not None else self.percent_sell_amount
        
        # Double-check token balance to prevent selling already sold tokens
        associated_token_account = self.wallet.get_associated_token_address(
            token_info.mint
        )
        try:
            actual_token_balance = await self.client.get_token_account_balance(
                associated_token_account
            )
            if actual_token_balance == 0:
                logger.warning("Tokens already sold, preventing duplicate sell transaction")
                return TradeResult(success=False, error_message="Tokens already sold")
                
            # Update token_balance if different from what was passed in
            if actual_token_balance != token_balance:
                logger.info(f"Token balance changed from {token_balance} to {actual_token_balance}")
                token_balance = actual_token_balance
        except Exception as e:
            logger.warning(f"Failed to verify token balance before selling: {e!s}")
            # Continue with the original token_balance

        # Check if this is a take profit trigger
        if is_take_profit:
            # Calculate the sell amount based on percent_sell_amount
            sell_amount = int(token_balance * sell_percent)
            logger.info(f"Take profit reached - selling {sell_percent * 100}% of tokens ({sell_amount / 10**TOKEN_DECIMALS})")
        else:
            # For trailing stop or stagnation, sell the entire amount
            sell_amount = token_balance
            logger.info(f"Trailing stop or stagnation - selling 100% of tokens ({sell_amount / 10**TOKEN_DECIMALS})")

        token_balance_decimal = sell_amount / 10**TOKEN_DECIMALS
        expected_sol_output = token_balance_decimal * current_price
        slippage_factor = 1 - self.slippage
        min_sol_output = int((expected_sol_output * slippage_factor) * LAMPORTS_PER_SOL)

        logger.info(f"Selling {token_balance_decimal} tokens")
        logger.info(f"Expected SOL output: {expected_sol_output:.8f} SOL")
        logger.info(
            f"Minimum SOL output (with {self.slippage * 100}% slippage): {min_sol_output / LAMPORTS_PER_SOL:.8f} SOL"
        )

        tx_signature = await self._send_sell_transaction(
            token_info,
            associated_token_account,
            sell_amount,
            min_sol_output,
        )

        success = await self.client.confirm_transaction(tx_signature)

        if success:
            logger.info(f"Sell transaction confirmed: {tx_signature} | Retrieving transaction details for further confirmation")
            tx_details = await self.client.get_transaction_details(tx_signature)
            if tx_details and tx_details.transaction.meta:
                # Check transaction success
                if tx_details.transaction.meta.err:
                    error_info = tx_details.transaction.meta.err
                    logger.error(f"Sell operation failed: Transaction {tx_signature} failed with error: {error_info}")
                    return TradeResult(
                        success=False,
                        error_message=f"Sell transaction failed: {error_info}",
                    )
            logger.info(f"Sell transaction successful: {tx_signature}")
            # Store whether this was a partial sell due to take profit
            result = TradeResult(
                success=True,
                tx_signature=tx_signature,
                amount=token_balance_decimal,
                price=current_price,
            )
            result.is_partial = is_take_profit
            result.percent_sold = sell_percent
            return result
        else:
            return TradeResult(
                success=False,
                error_message=f"Transaction failed to confirm: {tx_signature}",
            ) 
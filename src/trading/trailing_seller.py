"""
Trailing profit/loss sell operations for pump.fun tokens.
"""

import asyncio
from typing import Optional, TYPE_CHECKING

from core.client import SolanaClient
from core.curve import BondingCurveManager
from core.priority_fee.manager import PriorityFeeManager
from core.pubkeys import LAMPORTS_PER_SOL
from core.wallet import Wallet
from monitoring.price_listener import PriceListener
from trading.base import TokenInfo, TradeResult
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
        slippage: float = 0.25,
        max_retries: int = 5,
        trailing_stop_percentage: float = 0.15,  # 15% trailing stop
        take_profit_percentage: float = 0.50,    # 50% take profit
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
        self.check_interval = check_interval
        self.stagnation_timeout = stagnation_timeout
        self.balance_fetch_retries = balance_fetch_retries
        self.balance_fetch_delay = balance_fetch_delay
        
        self.price_listener_instance: Optional[PriceListener] = None
        self.use_websocket = shared_listener is not None

    async def execute(self, token_info: TokenInfo, 
                      token_balance: int,
                     entry_price: Optional[float] = None,
                     *args, **kwargs) -> TradeResult:
        """Execute trailing sell operation.

        Args:
            token_info: Token information
            entry_price: Entry price in SOL (if None, will be fetched)

        Returns:
            TradeResult with sell outcome
        """
        try:
            # Get associated token account
            # associated_token_account = self.wallet.get_associated_token_address(
            #     token_info.mint
            # )

            # # Get token balance
            # token_balance = 0  # Initialize to a default
            # for attempt in range(self.balance_fetch_retries):
            #     try:
            #         token_balance = await self.client.get_token_account_balance(
            #             associated_token_account
            #         )
            #         logger.info(
            #             f"Successfully fetched token balance: {token_balance} "
            #             f"on attempt {attempt + 1}/{self.balance_fetch_retries}"
            #         )
            #         break  # Exit loop on success
            #     except Exception as e:
            #         logger.warning(
            #             f"Attempt {attempt + 1}/{self.balance_fetch_retries} to fetch token balance for "
            #             f"{associated_token_account} failed: {e!s}"
            #         )
            #         if attempt < self.balance_fetch_retries - 1:
            #             logger.info(f"Retrying in {self.balance_fetch_delay} seconds...")
            #             await asyncio.sleep(self.balance_fetch_delay)
            #         else:
            #             logger.error(
            #                 f"Failed to fetch token balance for {associated_token_account} "
            #                 f"after {self.balance_fetch_retries} attempts."
            #             )
            #             raise # Re-raise the exception to be caught by the outer handler

            token_balance_decimal = token_balance / 10**9  # TOKEN_DECIMALS

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

            return await self._monitor_price_and_sell(token_info, token_balance * 10**9, entry_price)

        except Exception as e:
            logger.error(f"Trailing sell operation failed: {e!s}")
            return TradeResult(success=False, error_message=str(e))

    async def _monitor_price_and_sell(
        self, token_info: TokenInfo, token_balance: int, entry_price: float
    ) -> TradeResult:
        """Monitor price and sell based on trailing stop/take profit conditions.

        Args:
            token_info: Token information
            token_balance: Token balance in raw units
            entry_price: Entry price in SOL

        Returns:
            TradeResult with sell outcome
        """
        # Check if entry_price is a Decimal and handle accordingly
        from decimal import Decimal
        
        if isinstance(entry_price, Decimal):
            # If entry_price is Decimal, convert percentage values to Decimal
            highest_price = entry_price
            take_profit_percentage_decimal = Decimal(str(self.take_profit_percentage))
            trailing_stop_percentage_decimal = Decimal(str(self.trailing_stop_percentage))
            
            take_profit_target = entry_price * (Decimal('1') + take_profit_percentage_decimal)
            trailing_stop = entry_price * (Decimal('1') - trailing_stop_percentage_decimal)
        else:
            # Handle as regular float values
            highest_price = entry_price
            take_profit_target = entry_price * (1 + self.take_profit_percentage)
            trailing_stop = entry_price * (1 - self.trailing_stop_percentage)

        logger.info(f"Starting price monitoring with:")
        logger.info(f"  Entry price: {entry_price} SOL")
        logger.info(f"  Initial trailing stop: {trailing_stop} SOL")
        logger.info(f"  Take profit target: {take_profit_target} SOL")
        
        # Result will be set by the callback function
        result_event = asyncio.Event()
        sell_result: Optional[TradeResult] = None
        
        # Timeout to sell if no price changes occur
        price_stagnation_timeout = self.stagnation_timeout
        last_price_update_time = asyncio.get_event_loop().time()
        timeout_task = None
        
        if self.use_websocket:
            # Use WebSocket for real-time price monitoring via SharedWebsocketListener
            logger.info("Using SharedWebsocketListener for real-time price monitoring")
            try:
                # Create a PriceListener instance that uses the shared_listener
                self.price_listener_instance = PriceListener(
                    shared_listener=self.shared_listener, 
                    curve_manager=self.curve_manager, 
                    mint=token_info.mint
                )
                
                # Define the price update callback
                async def on_price_update(price: float) -> None:
                    nonlocal highest_price, trailing_stop, sell_result, last_price_update_time
                    
                    if not result_event.is_set():
                        # Update the last price update time
                        last_price_update_time = asyncio.get_event_loop().time()
                        
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
                                sell_result = await self._execute_sell(token_info, token_balance, price)
                                result_event.set()
                            
                            # Check take profit target
                            elif price_decimal >= take_profit_target:
                                logger.info(f"Take profit target reached at price: {price:.8f} SOL")
                                sell_result = await self._execute_sell(token_info, token_balance, price)
                                result_event.set()
                        else:
                            # Original float-based logic
                            profit_percent = (price - entry_price) / entry_price * 100
                            
                            logger.info(
                                f"WebSocket price update: {price} SOL (P/L: {profit_percent:.2f}%), "
                                f"Trailing stop: {trailing_stop} SOL"
                            )
                            
                            # Update trailing stop if price goes higher
                            if price > highest_price:
                                highest_price = price
                                trailing_stop = highest_price * (1 - self.trailing_stop_percentage)
                                logger.info(f"New highest price: {highest_price} SOL, updated trailing stop: {trailing_stop:.8f} SOL")
                            
                            # Check if we should sell
                            if price <= trailing_stop:
                                logger.info(f"Trailing stop triggered at price: {price} SOL")
                                sell_result = await self._execute_sell(token_info, token_balance, price)
                                result_event.set()
                            
                            # Check take profit target
                            elif price >= take_profit_target:
                                logger.info(f"Take profit target reached at price: {price} SOL")
                                sell_result = await self._execute_sell(token_info, token_balance, price)
                                result_event.set()
                
                # Function to check for price stagnation
                async def check_price_stagnation():
                    nonlocal sell_result
                    while not result_event.is_set():
                        current_time = asyncio.get_event_loop().time()
                        time_since_last_update = current_time - last_price_update_time
                        
                        if time_since_last_update >= price_stagnation_timeout:
                            # No price updates for the timeout period
                            logger.info(f"No price changes detected for {price_stagnation_timeout} seconds, selling token")
                            # Get current price for the sell
                            curve_state = await self.curve_manager.get_curve_state(token_info.bonding_curve)
                            current_price = curve_state.calculate_price()
                            
                            sell_result = await self._execute_sell(token_info, token_balance, current_price)
                            result_event.set()
                            break
                            
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
                    # Get current price
                    curve_state = await self.curve_manager.get_curve_state(
                        token_info.bonding_curve
                    )
                    current_price = curve_state.calculate_price()
                    
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
                                    logger.info(f"No price changes detected for {price_stagnation_timeout} seconds, selling token")
                                    return await self._execute_sell(token_info, token_balance, float(current_price))
                        
                        # Store current price for next comparison
                        last_price = current_price

                        # Update trailing stop if price goes higher
                        if current_price > highest_price:
                            highest_price = current_price
                            trailing_stop = highest_price * (Decimal('1') - trailing_stop_percentage_decimal)
                            logger.info(f"New highest price: {highest_price:.8f} SOL, updated trailing stop: {trailing_stop:.8f} SOL")

                        # Check if we should sell
                        if current_price <= trailing_stop:
                            logger.info(f"Trailing stop triggered at price: {current_price:.8f} SOL")
                            return await self._execute_sell(token_info, token_balance, float(current_price))
                        
                        # Check take profit target
                        if current_price >= take_profit_target:
                            logger.info(f"Take profit target reached at price: {current_price:.8f} SOL")
                            return await self._execute_sell(token_info, token_balance, float(current_price))
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
                                    logger.info(f"No price changes detected for {price_stagnation_timeout} seconds, selling token")
                                    return await self._execute_sell(token_info, token_balance, current_price)
                        
                        # Store current price for next comparison
                        last_price = current_price

                        # Update trailing stop if price goes higher
                        if current_price > highest_price:
                            highest_price = current_price
                            trailing_stop = highest_price * (1 - self.trailing_stop_percentage)
                            logger.info(f"New highest price: {highest_price:.8f} SOL, updated trailing stop: {trailing_stop:.8f} SOL")

                        # Check if we should sell
                        if current_price <= trailing_stop:
                            logger.info(f"Trailing stop triggered at price: {current_price:.8f} SOL")
                            return await self._execute_sell(token_info, token_balance, current_price)
                        
                        # Check take profit target
                        if current_price >= take_profit_target:
                            logger.info(f"Take profit target reached at price: {current_price:.8f} SOL")
                            return await self._execute_sell(token_info, token_balance, current_price)

                    await asyncio.sleep(self.check_interval)
                
                except asyncio.CancelledError:
                    logger.info("Price monitoring cancelled")
                    raise
                except Exception as e:
                    logger.error(f"Error monitoring price: {e!s}")
                    await asyncio.sleep(self.check_interval)

    async def _execute_sell(
        self, token_info: TokenInfo, token_balance: int, current_price: float
    ) -> TradeResult:
        """Execute the sell transaction.

        Args:
            token_info: Token information
            token_balance: Token balance in raw units
            current_price: Current token price in SOL

        Returns:
            TradeResult with sell outcome
        """
        associated_token_account = self.wallet.get_associated_token_address(
            token_info.mint
        )

        token_balance_decimal = token_balance / 10**9  # TOKEN_DECIMALS
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
            token_balance,
            min_sol_output,
        )

        success = await self.client.confirm_transaction(tx_signature)

        if success:
            logger.info(f"Sell transaction confirmed: {tx_signature}")
            return TradeResult(
                success=True,
                tx_signature=tx_signature,
                amount=token_balance_decimal,
                price=current_price,
            )
        else:
            return TradeResult(
                success=False,
                error_message=f"Transaction failed to confirm: {tx_signature}",
            ) 
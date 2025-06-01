"""
Trailing profit/loss sell operations for pump.fun tokens with grid selling.
"""

import asyncio
from typing import Optional, TYPE_CHECKING, List, Dict
from dataclasses import dataclass

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


@dataclass
class GridLevel:
    """Represents a grid selling level."""
    profit_percentage: float  # Profit percentage to trigger this level
    cumulative_sell_percentage: float  # Cumulative percentage of ORIGINAL tokens to have sold by this level
    executed: bool = False    # Whether this level has been executed


class TrailingTokenSeller(TokenSeller):
    """Handles selling tokens on pump.fun with trailing stop loss and either take profit OR grid selling."""

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
        take_profit_percentage: Optional[float] = 0.50,    # 50% take profit (set to None to disable)
        percent_sell_amount: float = 1.0,        # 100% of tokens to sell by default
        stagnation_timeout: int = 15,            # Seconds of no profit percentage change before selling
        profit_stagnation_threshold: float = 0.01,  # 1% - minimum profit change to reset stagnation timer
        balance_fetch_retries: int = 5,          # Retries for fetching token balance
        balance_fetch_delay: float = 5.0,        # Delay between balance fetch retries
        grid_levels: Optional[List[GridLevel]] = None,  # Grid levels (if provided, take_profit_percentage is ignored)
    ):
        """Initialize trailing token seller with either take profit OR grid selling.

        Args:
            client: Solana client for RPC calls
            wallet: Wallet for signing transactions
            curve_manager: Bonding curve manager
            priority_fee_manager: Priority fee manager
            shared_listener: Instance of SharedWebsocketListener for price updates.
            slippage: Slippage tolerance (0.25 = 25%)
            max_retries: Maximum number of retry attempts
            trailing_stop_percentage: Trailing stop percentage (0.15 = 15%)
            take_profit_percentage: Take profit percentage (0.50 = 50%). Set to None to disable.
            percent_sell_amount: Percentage of tokens to sell at take profit (1.0 = 100%)
            stagnation_timeout: Seconds of no profit percentage change before selling
            profit_stagnation_threshold: Minimum profit percentage change to reset stagnation timer
            balance_fetch_retries: Number of retries for fetching token balance
            balance_fetch_delay: Delay in seconds between balance fetch retries
            grid_levels: Grid selling levels. If provided, take_profit_percentage is ignored.
        """
        super().__init__(
            client, wallet, curve_manager, priority_fee_manager, slippage, max_retries
        )
        self.shared_listener = shared_listener
        self.trailing_stop_percentage = trailing_stop_percentage
        self.stagnation_timeout = stagnation_timeout
        self.profit_stagnation_threshold = profit_stagnation_threshold
        self.balance_fetch_retries = balance_fetch_retries
        self.balance_fetch_delay = balance_fetch_delay
        self.sol_to_usd_converter = sol_to_usd_converter
        
        # Determine selling strategy: either take profit OR grid selling
        if grid_levels is not None and len(grid_levels) > 0:
            # Grid selling mode
            self.use_grid_selling = True
            self.grid_levels = grid_levels
            self.take_profit_percentage = None
            self.percent_sell_amount = 1.0  # Not used in grid selling
            logger.info(f"Configured for GRID SELLING with {len(grid_levels)} levels")
        else:
            # Take profit mode
            self.use_grid_selling = False
            self.grid_levels = []
            self.take_profit_percentage = take_profit_percentage
            self.percent_sell_amount = percent_sell_amount
            if take_profit_percentage is not None:
                logger.info(f"Configured for TAKE PROFIT at {take_profit_percentage * 100:.1f}%")
            else:
                logger.info("Configured for TRAILING STOP ONLY (no take profit)")

        self.price_listener_instance: Optional[PriceListener] = None
        self._sell_in_progress = asyncio.Lock()  # Add a lock to prevent multiple simultaneous sells
        self.remaining_tokens = 0  # Track remaining tokens for grid selling
        self.original_token_balance = 0  # Track original token balance for cumulative calculations
        self.total_sold_percentage = 0.0  # Track cumulative percentage sold

    async def execute(self, token_info: TokenInfo, 
                      token_balance: int,
                      entry_price: Optional[float] = None) -> TradeResult:
        """Execute trailing sell operation.

        Args:
            token_info: Token information
            token_balance: Token balance in raw units
            entry_price: Entry price in SOL (if None, will be fetched)

        Returns:
            TradeResult with sell outcome
        """
        try:
            # Get associated token account
            # associated_token_account = self.wallet.get_associated_token_address(
            #     token_info.mint
            # )
            
            token_balance_decimal = token_balance / 10**TOKEN_DECIMALS

            logger.info(f"Token balance: {token_balance_decimal}")

            if token_balance == 0:
                logger.info("No tokens to sell.")
                return TradeResult(success=False, error_message="No tokens to sell")

            # Initialize remaining tokens for grid selling
            self.remaining_tokens = token_balance
            self.original_token_balance = token_balance
            self.total_sold_percentage = 0.0

            # Determine entry price if not provided
            if entry_price is None:
                # Estimate the entry price from current price if not provided
                curve_state = await self.curve_manager.get_curve_state(
                    token_info.bonding_curve
                )
                entry_price = curve_state.calculate_price()
                logger.info(f"Using current price as entry price: {entry_price} SOL (${float(entry_price) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
            else:
                logger.info(f"Using provided entry price: {entry_price} SOL (${float(entry_price) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")

            return await self._monitor_price_and_sell(token_info, token_balance, entry_price)

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
            trailing_stop_percentage_decimal = Decimal(str(self.trailing_stop_percentage))
            trailing_stop = entry_price * (Decimal('1') - trailing_stop_percentage_decimal)
            
            # Calculate take profit target only if using take profit mode
            take_profit_target = None
            if not self.use_grid_selling and self.take_profit_percentage is not None:
                take_profit_percentage_decimal = Decimal(str(self.take_profit_percentage))
                take_profit_target = entry_price * (Decimal('1') + take_profit_percentage_decimal)
        else:
            # Handle as regular float values
            highest_price = entry_price
            trailing_stop = entry_price * (1 - self.trailing_stop_percentage)
            
            # Calculate take profit target only if using take profit mode
            take_profit_target = None
            if not self.use_grid_selling and self.take_profit_percentage is not None:
                take_profit_target = entry_price * (1 + self.take_profit_percentage)

        logger.info(f"Starting price monitoring with:")
        logger.info(f"  Entry price: {entry_price} SOL (${float(entry_price) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
        logger.info(f"  Initial trailing stop: {trailing_stop} SOL (${float(trailing_stop) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
        
        if self.use_grid_selling:
            logger.info(f"  GRID SELLING MODE:")
            for i, level in enumerate(self.grid_levels):
                logger.info(f"    Level {i+1}: {level.profit_percentage * 100:.0f}% profit -> {level.cumulative_sell_percentage * 100:.0f}% total sold")
        elif take_profit_target is not None:
            logger.info(f"  TAKE PROFIT MODE:")
            logger.info(f"    Take profit target: {take_profit_target} SOL (${float(take_profit_target) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
            logger.info(f"    Percent to sell at take profit: {self.percent_sell_amount*100:.1f}%")
        else:
            logger.info(f"  TRAILING STOP ONLY MODE")
        
        logger.info(f"  SAFETY NET: Profit stagnation timeout: {self.stagnation_timeout}s (threshold: {self.profit_stagnation_threshold * 100:.1f}%)")
        
        # Result will be set by the callback function
        result_event = asyncio.Event()
        sell_result: Optional[TradeResult] = None
        
        # Timeout to sell if no profit percentage changes occur
        profit_stagnation_timeout = self.stagnation_timeout
        last_profit_update_time = asyncio.get_event_loop().time()
        last_known_profit_percent = None  # Track the last known profit percentage for stagnation check
        last_known_price = None  # Track the last known price for fallback
        timeout_task = None
        
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
                nonlocal highest_price, trailing_stop, sell_result, last_profit_update_time, last_known_profit_percent, last_known_price
                
                # Check if a sell is already in progress or completed
                if result_event.is_set() or not await self._try_acquire_sell_lock():
                    return  # Skip if sell is already in progress/completed or lock couldn't be acquired
                
                try:
                    # Update the last known price
                    last_known_price = price
                    
                    # Convert price to Decimal if entry_price is Decimal
                    if isinstance(entry_price, Decimal):
                        price_decimal = Decimal(str(price))
                        current_profit_percent = (price_decimal - entry_price) / entry_price * Decimal('100')
                        current_profit_decimal = current_profit_percent / Decimal('100')
                        
                        # Check for profit percentage stagnation
                        if last_known_profit_percent is not None:
                            profit_change = abs(current_profit_decimal - last_known_profit_percent)
                            if profit_change >= Decimal(str(self.profit_stagnation_threshold)):
                                last_profit_update_time = asyncio.get_event_loop().time()
                                last_known_profit_percent = current_profit_decimal
                        else:
                            last_known_profit_percent = current_profit_decimal
                            last_profit_update_time = asyncio.get_event_loop().time()
                        
                        logger.info(
                            f"WebSocket price update: {price:.8f} SOL (${price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD) "
                            f"(P/L: {current_profit_percent:.2f}%), Trailing stop: {trailing_stop:.8f} SOL "
                            f"(${float(trailing_stop) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)"
                        )
                        
                        # Check for grid selling opportunities
                        if self.use_grid_selling:
                            grid_sell_result = await self._check_grid_selling(
                                token_info, float(current_profit_decimal), price
                            )
                            if grid_sell_result:
                                if grid_sell_result.success:
                                    logger.info(f"Grid sell executed successfully")
                                    # Continue monitoring for remaining tokens
                                else:
                                    logger.error(f"Grid sell failed: {grid_sell_result.error_message}")
                        
                        # Update trailing stop if price goes higher
                        if price_decimal > highest_price:
                            highest_price = price_decimal
                            trailing_stop = highest_price * (Decimal('1') - trailing_stop_percentage_decimal)
                            logger.info(f"New highest price: {highest_price:.8f} SOL (${float(highest_price) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD), updated trailing stop: {trailing_stop:.8f} SOL (${float(trailing_stop) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
                        
                        # Check if we should sell due to trailing stop
                        if price_decimal <= trailing_stop:
                            logger.info(f"Trailing stop triggered at price: {price:.8f} SOL (${price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
                            sell_result = await self._execute_sell(token_info, self.remaining_tokens, price, is_take_profit=False)
                            result_event.set()
                        
                        # Check take profit target (only if not using grid selling)
                        elif not self.use_grid_selling and take_profit_target is not None and price_decimal >= take_profit_target:
                            logger.info(f"Take profit target reached at price: {price:.8f} SOL (${price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
                            sell_result = await self._execute_sell(token_info, self.remaining_tokens, price, is_take_profit=True, percent_sell_amount=self.percent_sell_amount)
                            result_event.set()
                    else:
                        # Original float-based logic
                        current_profit_percent = (price - entry_price) / entry_price * 100
                        current_profit_decimal = current_profit_percent / 100
                        
                        # Check for profit percentage stagnation
                        if last_known_profit_percent is not None:
                            profit_change = abs(current_profit_decimal - last_known_profit_percent)
                            if profit_change >= self.profit_stagnation_threshold:
                                last_profit_update_time = asyncio.get_event_loop().time()
                                last_known_profit_percent = current_profit_decimal
                        else:
                            last_known_profit_percent = current_profit_decimal
                            last_profit_update_time = asyncio.get_event_loop().time()
                        
                        logger.info(
                            f"WebSocket price update: {price:.8f} SOL (${price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD) "
                            f"(P/L: {current_profit_percent:.2f}%), Trailing stop: {trailing_stop:.8f} SOL "
                            f"(${float(trailing_stop) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)"
                        )
                        
                        # Check for grid selling opportunities
                        if self.use_grid_selling:
                            grid_sell_result = await self._check_grid_selling(
                                token_info, current_profit_decimal, price
                            )
                            if grid_sell_result:
                                if grid_sell_result.success:
                                    logger.info(f"Grid sell executed successfully")
                                    # Continue monitoring for remaining tokens
                                else:
                                    logger.error(f"Grid sell failed: {grid_sell_result.error_message}")
                        
                        # Update trailing stop if price goes higher
                        if price > highest_price:
                            highest_price = price
                            trailing_stop = highest_price * (1 - self.trailing_stop_percentage)
                            logger.info(f"New highest price: {highest_price:.8f} SOL (${float(highest_price) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD), updated trailing stop: {trailing_stop:.8f} SOL (${float(trailing_stop) * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
                        
                        # Check if we should sell due to trailing stop
                        if price <= trailing_stop:
                            logger.info(f"Trailing stop triggered at price: {price:.8f} SOL (${price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
                            sell_result = await self._execute_sell(token_info, self.remaining_tokens, price, is_take_profit=False)
                            result_event.set()
                        
                        # Check take profit target (only if not using grid selling)
                        elif not self.use_grid_selling and take_profit_target is not None and price >= take_profit_target:
                            logger.info(f"Take profit target reached at price: {price:.8f} SOL (${price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
                            sell_result = await self._execute_sell(token_info, self.remaining_tokens, price, is_take_profit=True, percent_sell_amount=self.percent_sell_amount)
                            result_event.set()
                finally:
                    # Always release the lock when done
                    self._sell_in_progress.release()
            
            # Function to check for profit percentage stagnation
            async def check_profit_stagnation():
                nonlocal sell_result, last_known_price, last_known_profit_percent
                
                while not result_event.is_set():
                    current_time = asyncio.get_event_loop().time()
                    time_since_last_update = current_time - last_profit_update_time
                    
                    if time_since_last_update >= profit_stagnation_timeout:
                        # Try to acquire the sell lock
                        if not await self._try_acquire_sell_lock():
                            await asyncio.sleep(1)
                            continue

                        try:
                            # Check again after acquiring lock in case another thread sold already
                            if result_event.is_set():
                                break
                            
                            # Calculate remaining percentage for logging
                            remaining_percentage = (self.remaining_tokens / self.original_token_balance) * 100 if self.original_token_balance > 0 else 0
                            
                            # Log stagnation trigger with selling strategy context
                            strategy_context = "grid selling" if self.use_grid_selling else "take profit/trailing stop"
                            logger.info(
                                f"SAFETY NET: No significant profit changes for {profit_stagnation_timeout}s during {strategy_context}. "
                                f"Selling all remaining tokens ({remaining_percentage:.1f}% of original position)"
                            )
                            
                            # Try to get current price, but handle curve state errors gracefully
                            current_price = None
                            try:
                                curve_state = await self.curve_manager.get_curve_state(token_info.bonding_curve)
                                current_price = curve_state.calculate_price()
                                logger.info(f"Using current price for stagnation sell: {current_price:.8f} SOL (${current_price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD)")
                            except (ValueError, Exception) as e:
                                # If we can't get the current price, use the last known price or entry price
                                if last_known_price is not None:
                                    current_price = last_known_price
                                    logger.warning(f"Could not fetch current price ({e}), using last known price: {current_price:.8f} SOL")
                                else:
                                    # Fallback to entry price if no last known price
                                    current_price = float(entry_price) if isinstance(entry_price, Decimal) else entry_price
                                    logger.warning(f"Could not fetch current price ({e}), using entry price: {current_price:.8f} SOL")
                            
                            sell_result = await self._execute_sell(token_info, self.remaining_tokens, current_price, is_take_profit=False)
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
            timeout_task = asyncio.create_task(check_profit_stagnation())
            
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
            logger.error(f"An error occurred during WebSocket-based monitoring: {e!s}", exc_info=True)
            # Re-raise the exception since we no longer have a polling fallback
            raise RuntimeError(f"WebSocket monitoring failed and no fallback available: {e}")

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
        if is_take_profit and not self.use_grid_selling:
            # Calculate the sell amount based on percent_sell_amount
            sell_amount = int(token_balance * sell_percent)
            logger.info(f"Take profit reached - selling {sell_percent * 100}% of tokens ({sell_amount / 10**TOKEN_DECIMALS})")
        else:
            # For trailing stop, stagnation, or grid selling, sell the entire amount
            sell_amount = token_balance
            if self.use_grid_selling:
                logger.info(f"Grid sell - selling {sell_amount / 10**TOKEN_DECIMALS} tokens")
            else:
                logger.info(f"Trailing stop or stagnation - selling 100% of tokens ({sell_amount / 10**TOKEN_DECIMALS})")

        token_balance_decimal = sell_amount / 10**TOKEN_DECIMALS
        expected_sol_output = token_balance_decimal * current_price
        slippage_factor = 1 - self.slippage
        min_sol_output = int((expected_sol_output * slippage_factor) * LAMPORTS_PER_SOL)

        logger.info(f"Selling {token_balance_decimal} tokens")
        logger.info(f"Expected SOL output: {expected_sol_output:.8f} SOL (${expected_sol_output * float(self.sol_to_usd_converter.get_current_price()):.2f} USD)")
        logger.info(
            f"Minimum SOL output (with {self.slippage * 100}% slippage): {min_sol_output / LAMPORTS_PER_SOL:.8f} SOL "
            f"(${(min_sol_output / LAMPORTS_PER_SOL) * float(self.sol_to_usd_converter.get_current_price()):.2f} USD)"
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
            result.is_partial = is_take_profit and not self.use_grid_selling
            result.percent_sold = sell_percent if is_take_profit and not self.use_grid_selling else 1.0
            return result
        else:
            return TradeResult(
                success=False,
                error_message=f"Transaction failed to confirm: {tx_signature}",
            ) 

    async def _check_grid_selling(
        self, token_info: TokenInfo, current_profit_percentage: float, current_price: float
    ) -> Optional[TradeResult]:
        """Check if any grid selling levels should be triggered.

        Args:
            token_info: Token information
            current_profit_percentage: Current profit percentage (as decimal, e.g., 0.50 for 50%)
            current_price: Current token price

        Returns:
            TradeResult if a grid sell was executed, None otherwise
        """
        if not self.use_grid_selling:
            return None

        # Check each grid level
        for level in self.grid_levels:
            if (current_profit_percentage >= level.profit_percentage and 
                not level.executed and 
                self.remaining_tokens > 0):
                
                # Calculate how much more we need to sell to reach this cumulative percentage
                target_cumulative_percentage = level.cumulative_sell_percentage
                additional_percentage_to_sell = target_cumulative_percentage - self.total_sold_percentage
                
                if additional_percentage_to_sell <= 0:
                    logger.info(f"Grid level {level.profit_percentage * 100:.0f}% - already sold enough ({self.total_sold_percentage * 100:.1f}%)")
                    level.executed = True
                    continue
                
                # Calculate sell amount based on ORIGINAL token balance
                sell_amount = int(self.original_token_balance * additional_percentage_to_sell)
                
                # Make sure we don't sell more than we have remaining
                sell_amount = min(sell_amount, self.remaining_tokens)
                
                if sell_amount == 0:
                    logger.info(f"Grid level {level.profit_percentage * 100:.0f}% - no tokens to sell")
                    level.executed = True
                    continue

                # Calculate the actual percentage this sell represents
                actual_sell_percentage = sell_amount / self.original_token_balance

                logger.info(
                    f"Grid sell triggered at {current_profit_percentage * 100:.1f}% profit "
                    f"(level: {level.profit_percentage * 100:.0f}%) at {current_price:.8f} SOL "
                    f"(${current_price * float(self.sol_to_usd_converter.get_current_price()):.8f} USD) - "
                    f"selling {actual_sell_percentage * 100:.1f}% more "
                    f"({sell_amount / 10**TOKEN_DECIMALS:.3f} tokens) "
                    f"to reach {target_cumulative_percentage * 100:.1f}% total sold"
                )

                # Execute the grid sell
                result = await self._execute_sell(
                    token_info, 
                    sell_amount, 
                    current_price, 
                    is_take_profit=True, 
                    percent_sell_amount=actual_sell_percentage
                )

                if result.success:
                    # Update tracking variables
                    self.remaining_tokens -= sell_amount
                    self.total_sold_percentage += actual_sell_percentage
                    level.executed = True
                    
                    logger.info(
                        f"Grid sell successful. "
                        f"Remaining tokens: {self.remaining_tokens / 10**TOKEN_DECIMALS:.3f} "
                        f"({(1 - self.total_sold_percentage) * 100:.1f}% of original)"
                    )
                    
                    # Check if we've sold all tokens
                    if self.remaining_tokens <= 10**TOKEN_DECIMALS or self.total_sold_percentage >= 0.99:  # 99% threshold to account for rounding
                        logger.info("All tokens sold through grid selling")
                        # Mark all remaining levels as executed
                        for remaining_level in self.grid_levels:
                            remaining_level.executed = True
                    
                    return result
                else:
                    logger.error(f"Grid sell failed: {result.error_message}")
                    return result

        return None 
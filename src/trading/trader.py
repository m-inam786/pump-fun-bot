"""
Main trading coordinator for pump.fun tokens.
Refactored PumpTrader to only process fresh tokens from WebSocket.
"""

import asyncio
import json
import os
from datetime import datetime
from time import monotonic
from decimal import Decimal
import uvloop
from solders.pubkey import Pubkey

from cleanup.modes import (
    handle_cleanup_after_failure,
    handle_cleanup_after_sell,
    handle_cleanup_post_session,
)
from core.client import SolanaClient
from core.curve import BondingCurveManager
from core.priority_fee.manager import PriorityFeeManager
from core.pubkeys import PumpAddresses
from core.wallet import Wallet
from monitoring.block_listener import BlockListener
from monitoring.developer_manager import DeveloperManager
from monitoring.geyser_listener import GeyserListener
from monitoring.logs_listener import LogsListener
from monitoring.pump_portal_listener import PumpPortalListener
from monitoring.shared_websocket_listener import SharedWebsocketListener
from monitoring.shreder_socket_listener import ShrederSocketListener
from trading.base import TokenInfo, TradeResult
from trading.buyer import TokenBuyer
from trading.seller import TokenSeller
from trading.trailing_seller import TrailingTokenSeller
from utils.logger import get_logger
from core.pubkeys import TOKEN_DECIMALS
from utils.discord_notifications import (
    DiscordNotifier,
    notify_token_buy,
    notify_token_sell,
    notify_pnl,
    notify_error,
)
from utils.sol_to_usd_converter import SolToUsdConverter
from templates.buy_tx import BuyTxBuilder

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logger = get_logger(__name__)


class PumpTrader:
    """Coordinates trading operations for pump.fun tokens with focus on freshness."""
    def __init__(
        self,
        rpc_endpoint: str,
        wss_endpoint: str,
        private_key: str,
        buy_amount: float,
        token_amount: int,
        buy_slippage: float,
        sell_slippage: float,
        listener_type: str = "logs",
        geyser_endpoint: str | None = None,
        geyser_api_token: str | None = None,
        geyser_auth_type: str = "x-token",
        
        # Shreder socket configuration
        shreder_socket_host: str = "localhost",
        shreder_socket_port: int = 8765,
        
        # Priority fee configuration
        enable_dynamic_priority_fee: bool = False,
        enable_fixed_priority_fee: bool = True,
        fixed_priority_fee: int = 200_000,
        extra_priority_fee: float = 0.0,
        hard_cap_prior_fee: int = 200_000,
        
        # Trailing profit/loss settings
        use_trailing_profit_loss: bool = False,
        trailing_stop_percentage: float = 0.15,
        take_profit_percentage: float = 0.50,
        percent_sell_amount: float = 1.0,
        price_check_interval: float = 1.0,
        stagnation_timeout: int = 15,
        
        # Retry and timeout settings
        max_retries: int = 3,
        wait_time_after_creation: int = 15, # here and further - seconds
        wait_time_after_buy: int = 15,
        wait_time_before_new_token: int = 15,
        max_token_age: int | float = 0.001,
        token_wait_timeout: int = 30,
        
        # Cleanup settings
        cleanup_mode: str = "disabled",
        cleanup_force_close_with_burn: bool = False,
        cleanup_with_priority_fee: bool = False,
        
        # Trading filters
        match_string: str | None = None,
        bro_address: str | None = None,
        marry_mode: bool = False,
        yolo_mode: bool = False,
        
        # Developer manager settings
        enable_developer_manager: bool = False,
        db_host: str | None = None,
        db_port: int | None = None,
        db_name: str | None = None,
        db_user: str | None = None,
        db_password: str | None = None,
        db_query_file: str | None = None,
        db_refresh_interval: int = 1800,  # 30 minutes
        max_developers: int = 1000,
        developer_age_days: int = 1,
        sniped_devs_file: str = "data/sniped_developers.json",
        
        # Concurrency settings
        processor_count: int = 1,

        # Discord notification settings
        discord_webhook_url: str | None = None,
        discord_queue_size: int = 1000,
        discord_worker_count: int = 2,
        discord_retry_limit: int = 3,
        
        # SOL/USD converter settings
        sol_price_update_interval: int = 480,  # 8 minutes

        # Custom tip settings
        use_custom_tip: bool = False,
        tip_lamports: int = 2000000,
        tip_account: str | None = None,
        tip_rpc_url: str | None = None,

        # Nonce account settings
        nonce_account_file: str | None = None,
    ):
        """Initialize the pump trader.
        Args:
            rpc_endpoint: RPC endpoint URL
            wss_endpoint: WebSocket endpoint URL
            private_key: Wallet private key
            buy_amount: Amount of SOL to spend on buys
            token_amount: Amount of tokens to buy
            buy_slippage: Slippage tolerance for buys
            sell_slippage: Slippage tolerance for sells

            listener_type: Type of listener to use ('logs', 'blocks', 'geyser', or 'shreder_socket')
            geyser_endpoint: Geyser endpoint URL (required for geyser listener)
            geyser_api_token: Geyser API token (required for geyser listener)
            geyser_auth_type: Geyser authentication type ('x-token' or 'basic')

            shreder_socket_host: WebSocket server host for Shreder listener
            shreder_socket_port: WebSocket server port for Shreder listener

            enable_dynamic_priority_fee: Whether to enable dynamic priority fees
            enable_fixed_priority_fee: Whether to enable fixed priority fees
            fixed_priority_fee: Fixed priority fee amount
            extra_priority_fee: Extra percentage for priority fees
            hard_cap_prior_fee: Hard cap for priority fees

            max_retries: Maximum number of retry attempts
            wait_time_after_creation: Time to wait after token creation (seconds)
            wait_time_after_buy: Time to wait after buying a token (seconds)
            wait_time_before_new_token: Time to wait before processing a new token (seconds)
            max_token_age: Maximum age of token to process (seconds)
            token_wait_timeout: Timeout for waiting for a token in single-token mode (seconds)

            cleanup_mode: Cleanup mode ("disabled", "auto", or "manual")
            cleanup_force_close_with_burn: Whether to force close with burn during cleanup
            cleanup_with_priority_fee: Whether to use priority fees during cleanup
            
            match_string: Optional string to match in token name/symbol
            bro_address: Optional creator address to filter by
            marry_mode: If True, only buy tokens and skip selling
            yolo_mode: If True, trade continuously
            
            enable_developer_manager: Whether to enable the developer manager
            db_host: PostgreSQL host
            db_port: PostgreSQL port
            db_name: PostgreSQL database name
            db_user: PostgreSQL username
            db_password: PostgreSQL password
            db_query: SQL query to fetch developers
            db_refresh_interval: Time between developer list refreshes in seconds
            max_developers: Maximum number of developers to store in memory
            developer_age_days: Maximum age of developers in days before removal
            sniped_devs_file: Path to file for persisting sniped developers
            
            processor_count: Number of concurrent token processor tasks

            discord_webhook_url: Discord webhook URL for notifications
            discord_queue_size: Maximum size of the Discord notification queue
            discord_worker_count: Number of worker threads for Discord notifications
            discord_retry_limit: Maximum retry attempts for Discord notifications
            
            sol_price_update_interval: Time between SOL price updates in seconds

            use_custom_tip: Whether to enable custom tip manager for faster transactions
            tip_lamports: Default tip amount in lamports
            tip_account: Tip account public key
            tip_rpc_url: Tip RPC URL

            nonce_account_file: Path to nonce account file
        """

        # Initialize custom tip based on configuration
        if use_custom_tip:
            logger.info("Custom tip enabled. Solana client will use custom tip RPC URL.")
            self.tip_rpc_url = tip_rpc_url
            self.tip_lamports = tip_lamports
            self.tip_account = Pubkey.from_string(tip_account)
            self.solana_client = SolanaClient(rpc_endpoint, tip_rpc_url=tip_rpc_url, nonce_file_path=nonce_account_file)
        else:
            logger.info("Custom tip disabled. Using regular Solana client.")
            self.tip_account = None
            self.solana_client = SolanaClient(rpc_endpoint, nonce_file_path=nonce_account_file)

        self.wallet = Wallet(private_key)
        self.curve_manager = BondingCurveManager(self.solana_client)
        self.shared_websocket_listener = SharedWebsocketListener(wss_endpoint)
        
        # Initialize SOL to USD converter
        self.sol_to_usd_converter = SolToUsdConverter(update_interval=sol_price_update_interval)
        
        self.priority_fee_manager = PriorityFeeManager(
            client=self.solana_client,
            enable_dynamic_fee=enable_dynamic_priority_fee,
            enable_fixed_fee=enable_fixed_priority_fee,
            fixed_fee=fixed_priority_fee,
            extra_fee=extra_priority_fee,
            hard_cap=hard_cap_prior_fee,
        )

        self.buyer = TokenBuyer(
            self.solana_client,
            self.wallet,
            self.curve_manager,
            max_retries
        )
        
        # Initialize bot configuration for static templates
        # Set bot config for static templates (regular mode)
        BuyTxBuilder.update_buy_tx_with_bot_config(
            buy_amount=buy_amount,
            buy_slippage=buy_slippage,
            priority_fee_microlamports=fixed_priority_fee,
            wallet_instance=self.wallet,
            tip_amount_lamports=tip_lamports if use_custom_tip else None,
            tip_destination=self.tip_account if use_custom_tip else None,
            token_amount=token_amount
        )
        logger.info(f"Initialized static buy template with: buy_amount={buy_amount}, slippage={buy_slippage}, priority_fee={fixed_priority_fee}")
        
        # Initialize seller based on configuration
        if use_trailing_profit_loss:
            self.seller = TrailingTokenSeller(
                client=self.solana_client,
                wallet=self.wallet,
                curve_manager=self.curve_manager,
                priority_fee_manager=self.priority_fee_manager,
                shared_listener=self.shared_websocket_listener,
                slippage=sell_slippage,
                max_retries=max_retries,
                trailing_stop_percentage=trailing_stop_percentage,
                take_profit_percentage=take_profit_percentage,
                percent_sell_amount=percent_sell_amount,
                check_interval=price_check_interval,
                stagnation_timeout=stagnation_timeout,
                sol_to_usd_converter=self.sol_to_usd_converter
            )
            logger.info("Using trailing profit/loss seller with real-time WebSocket monitoring")
            logger.info(f"  Trailing stop: {trailing_stop_percentage * 100:.1f}%")
            logger.info(f"  Take profit: {take_profit_percentage * 100:.1f}%")
            logger.info(f"  Price stagnation timeout: {stagnation_timeout} seconds")
        else:
            self.seller = TokenSeller(
                self.solana_client,
                self.wallet,
                self.curve_manager,
                self.priority_fee_manager,
                sell_slippage,
                max_retries,
            )
            

        # Discord notification settings
        self.discord_notifier = None
        if discord_webhook_url:
            self.discord_notifier = DiscordNotifier(
                webhook_url=discord_webhook_url,
                queue_size=discord_queue_size,
                worker_count=discord_worker_count,
                retry_limit=discord_retry_limit,
            )
            logger.info("Discord notifications enabled")
            logger.info(f"  Queue size: {discord_queue_size}")
            logger.info(f"  Worker count: {discord_worker_count}")

        # Initialize the developer manager if enabled
        self.developer_manager = None
        if enable_developer_manager:
            if not all([db_host, db_port, db_name, db_user, db_password, db_query_file]):
                raise ValueError("Database configuration required when developer manager is enabled")
                
            self.developer_manager = DeveloperManager(
                db_host=db_host,
                db_port=db_port,
                db_name=db_name,
                db_user=db_user,
                db_password=db_password,
                db_query=db_query_file,
                refresh_interval=db_refresh_interval,
                max_developers=max_developers,
                max_age_days=developer_age_days,
                persisted_whitelist_filepath=sniped_devs_file,
                discord_notifier=self.discord_notifier,
                tip_destination=self.tip_account if self.tip_account else None,
            )
            logger.info("Developer manager enabled")
            logger.info(f"  Max developers: {max_developers}")
            logger.info(f"  Developer age limit: {developer_age_days} days")
            logger.info(f"  Refresh interval: {db_refresh_interval} seconds")
        
        # Initialize the appropriate listener type
        listener_type = listener_type.lower()
        
        if listener_type == "geyser":
            if not geyser_endpoint or not geyser_api_token:
                raise ValueError("Geyser endpoint and API token are required for geyser listener")
                
            self.token_listener = GeyserListener(
                geyser_endpoint, 
                geyser_api_token,
                geyser_auth_type, 
                PumpAddresses.PROGRAM,
                self.developer_manager
            )
            logger.info("Using Geyser listener for token monitoring")
        elif listener_type == "shreder_socket":
            self.token_listener = ShrederSocketListener(
                socket_host=shreder_socket_host,
                socket_port=shreder_socket_port,
                developer_manager=self.developer_manager
            )
            logger.info(f"Using Shreder Socket listener for token monitoring on {shreder_socket_host}:{shreder_socket_port}")
        elif listener_type == "logs":
            self.token_listener = LogsListener(
                wss_endpoint, 
                PumpAddresses.PROGRAM,
                self.developer_manager
            )
            logger.info("Using logsSubscribe listener for token monitoring")
        elif listener_type == "blocks":
            self.token_listener = BlockListener(
                wss_endpoint, 
                PumpAddresses.PROGRAM,
                self.developer_manager
            )
            logger.info("Using blockSubscribe listener for token monitoring")
        else:
            self.token_listener = PumpPortalListener(
                PumpAddresses.PROGRAM,
                self.developer_manager
            )
            logger.info("Using PumpPortal listener for additional token monitoring")
            
        # Trading parameters
        self.buy_amount = buy_amount
        self.token_amount = token_amount
        self.buy_slippage = buy_slippage
        self.sell_slippage = sell_slippage
        self.max_retries = max_retries
        
        # Timing parameters
        self.wait_time_after_creation = wait_time_after_creation
        self.wait_time_after_buy = wait_time_after_buy
        self.wait_time_before_new_token = wait_time_before_new_token
        self.max_token_age = max_token_age
        self.token_wait_timeout = token_wait_timeout
        
        # Cleanup parameters
        self.cleanup_mode = cleanup_mode
        self.cleanup_force_close_with_burn = cleanup_force_close_with_burn
        self.cleanup_with_priority_fee = cleanup_with_priority_fee

        # Trading filters/modes
        self.match_string = match_string
        self.bro_address = bro_address
        self.marry_mode = marry_mode
        self.yolo_mode = yolo_mode
        
        # Concurrency settings
        self.processor_count = processor_count
        
        # State tracking
        self.traded_mints: set[Pubkey] = set()
        self.token_queue: asyncio.Queue = asyncio.Queue()
        self.processing: bool = False
        self.processed_tokens: set[str] = set()
        self.token_timestamps: dict[str, float] = {}
        self.processor_tasks: list[asyncio.Task] = []
        
        # Thread safety
        self.processed_tokens_lock = asyncio.Lock()
        self.traded_mints_lock = asyncio.Lock()
        
    async def start(self) -> None:
        """Start the trading bot and listen for new tokens."""
        logger.info("Starting pump.fun trader")
        logger.info(f"Match filter: {self.match_string if self.match_string else 'None'}")
        logger.info(f"Creator filter: {self.bro_address if self.bro_address else 'None'}")
        logger.info(f"Marry mode: {self.marry_mode}")
        logger.info(f"YOLO mode: {self.yolo_mode}")
        logger.info(f"Max token age: {self.max_token_age} seconds")
        logger.info(f"Concurrent processors: {self.processor_count}")

        # Start the shared WebSocket listener
        await self.shared_websocket_listener.start()
        
        # Start the SOL to USD converter
        await self.sol_to_usd_converter.start()

        # Start the Solana client
        await self.solana_client.start()

        # Start the Discord notifier if enabled
        if self.discord_notifier:
            await self.discord_notifier.start()

        # Start the developer manager if enabled
        if self.developer_manager:
            await self.developer_manager.start()

        # Warm up the RPC
        try:
            health_resp = await self.solana_client.get_health()
            logger.info(f"RPC warm-up successful (getHealth passed: {health_resp})")
        except Exception as e:
            logger.warning(f"RPC warm-up failed: {e!s}")

        try:
            # Choose operating mode based on yolo_mode
            if not self.yolo_mode:
                # Single token mode: process one token and exit
                logger.info("Running in single token mode - will process one token and exit")
                token_info = await self._wait_for_token()
                if token_info:
                    await self._handle_token(token_info)
                    logger.info("Finished processing single token. Exiting...")
                else:
                    logger.info(f"No suitable token found within timeout period ({self.token_wait_timeout}s). Exiting...")
            else:
                # Continuous mode: process tokens until interrupted
                logger.info("Running in continuous mode - will process tokens until interrupted")
                
                # Create multiple processor tasks
                logger.info(f"Starting {self.processor_count} concurrent token processors")
                for i in range(self.processor_count):
                    processor_task = asyncio.create_task(
                        self._process_token_queue(i)
                    )
                    self.processor_tasks.append(processor_task)

                try:
                    await self.token_listener.listen_for_tokens(
                        lambda token: self._queue_token(token),
                        self.match_string,
                        self.bro_address,
                    )
                except Exception as e:
                    logger.error(f"Token listening stopped due to error: {e!s}")
                    if self.discord_notifier:
                        await notify_error(
                            self.discord_notifier,
                            "Token Listening Error",
                            f"Token listening stopped due to error: {str(e)}"
                        )
                finally:
                    # Cancel all processor tasks
                    for task in self.processor_tasks:
                        task.cancel()
                    
                    # Wait for all tasks to be cancelled
                    if self.processor_tasks:
                        await asyncio.gather(*self.processor_tasks, return_exceptions=True)
        
        except Exception as e:
            logger.error(f"Trading stopped due to error: {e!s}")
            if self.discord_notifier:
                await notify_error(
                    self.discord_notifier,
                    "Trading Error",
                    f"Trading stopped due to error: {str(e)}"
                )
        
        finally:
            await self._cleanup_resources()
            logger.info("Pump trader has shut down")
            
    async def _cleanup_resources(self) -> None:
        """Perform cleanup operations before shutting down."""
        if self.traded_mints:
            try:
                logger.info(f"Cleaning up {len(self.traded_mints)} traded token(s)...")
                await handle_cleanup_post_session(
                    self.solana_client, 
                    self.wallet, 
                    list(self.traded_mints), 
                    self.priority_fee_manager,
                    self.cleanup_mode,
                    self.cleanup_with_priority_fee,
                    self.cleanup_force_close_with_burn
                )
            except Exception as e:
                logger.error(f"Error during cleanup: {e!s}")
                if self.discord_notifier:
                    await notify_error(
                        self.discord_notifier,
                        "Cleanup Error",
                        f"Error cleaning up traded tokens: {str(e)}"
                    )
        
        # Stop all services
        logger.info("Stopping services...")
        
        # Stop token listener - especially important for PumpPortal to avoid blacklisting
        try:
            if hasattr(self.token_listener, 'stop') and callable(self.token_listener.stop):
                logger.info(f"Stopping {self.token_listener.__class__.__name__}...")
                await self.token_listener.stop()
        except Exception as e:
            logger.error(f"Error stopping token listener: {e!s}")
        
        # Stop SOL/USD converter
        try:
            await self.sol_to_usd_converter.stop()
        except Exception as e:
            logger.error(f"Error stopping SOL/USD converter: {e!s}")

        # Stop the Solana client
        try:
            await self.solana_client.close()
        except Exception as e:
            logger.error(f"Error stopping Solana client: {e!s}")

        # Stop the shared WebSocket listener
        try:
            await self.shared_websocket_listener.stop()
        except Exception as e:
            logger.error(f"Error stopping WebSocket listener: {e!s}")
            
        # Stop the developer manager if running
        if self.developer_manager:
            try:
                await self.developer_manager.stop()
            except Exception as e:
                logger.error(f"Error stopping developer manager: {e!s}")
                
        # Stop Discord notifier if running
        if self.discord_notifier:
            try:
                await self.discord_notifier.stop()
            except Exception as e:
                logger.error(f"Error stopping Discord notifier: {e!s}")
                
    async def _wait_for_token(self) -> TokenInfo | None:
        """Wait for a single token to be detected.
        
        Returns:
            TokenInfo or None if timeout occurs
        """
        # Create a one-time event to signal when a token is found
        token_found = asyncio.Event()
        found_token = None
        
        async def token_callback(token: TokenInfo) -> None:
            nonlocal found_token
            token_key = str(token.mint)
            
            # Only process if not already processed and fresh
            async with self.processed_tokens_lock:
                if token_key not in self.processed_tokens:
                    # Record when the token was discovered
                    self.token_timestamps[token_key] = monotonic()
                    found_token = token
                    self.processed_tokens.add(token_key)
                    token_found.set()
        
        listener_task = asyncio.create_task(
            self.token_listener.listen_for_tokens(
                token_callback,
                self.match_string,
                self.bro_address,
            )
        )
        
        # Wait for a token with a timeout
        try:
            logger.info(f"Waiting for a suitable token (timeout: {self.token_wait_timeout}s)...")
            await asyncio.wait_for(token_found.wait(), timeout=self.token_wait_timeout)
            logger.info(f"Found token: {found_token.symbol} ({found_token.mint})")
            return found_token
        except TimeoutError:
            logger.info(f"Timed out after waiting {self.token_wait_timeout}s for a token")
            if self.discord_notifier:
                await notify_error(
                    self.discord_notifier,
                    "Token Wait Timeout",
                    f"No suitable token found within {self.token_wait_timeout} seconds",
                    "The bot will exit as configured for single-token mode."
                )
            return None
        finally:
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass

    async def _queue_token(
        self, token_info: TokenInfo
    ) -> None:
        """Queue a token for processing if not already processed.
        
        Args:
            token_info: Token information to queue
        """
        token_key = str(token_info.mint)

        async with self.processed_tokens_lock:
            if token_key in self.processed_tokens:
                logger.debug(f"Token {token_info.symbol} already processed. Skipping...")
                return

            # Record timestamp when token was discovered
            self.token_timestamps[token_key] = monotonic()
            self.processed_tokens.add(token_key)

        await self.token_queue.put(token_info)
        logger.info(f"Queued new token: {token_info.symbol} ({token_info.mint})")

    async def _process_token_queue(self, processor_id: int) -> None:
        """Continuously process tokens from the queue, only if they're fresh.
        
        Args:
            processor_id: Unique identifier for this processor task
        """
        logger.info(f"Token processor {processor_id} started")
        
        while True:
            try:
                token_info = await self.token_queue.get()
                token_key = str(token_info.mint)

                # Check if token is still "fresh" and not already processed
                current_time = monotonic()
                token_age = current_time - self.token_timestamps.get(
                    token_key, current_time
                )
                
                is_processed = False
                async with self.processed_tokens_lock:
                    if token_key not in self.processed_tokens:
                        # This should not happen as we add to processed_tokens in _queue_token
                        # But just in case, we add it here
                        self.processed_tokens.add(token_key)
                    else:
                        # Check if this token was already processed by this processor
                        if token_key in self.traded_mints:
                            is_processed = True

                if is_processed:
                    logger.debug(f"Processor {processor_id}: Token {token_info.symbol} already processed by another processor")
                    continue
                
                if token_age > self.max_token_age:
                    logger.info(
                        f"Processor {processor_id}: Skipping token {token_info.symbol} - too old ({token_age:.1f}s > {self.max_token_age}s)"
                    )
                    continue

                logger.info(
                    f"Processor {processor_id}: Processing fresh token: {token_info.symbol} (age: {token_age:.1f}s)"
                )
                await self._handle_token(token_info)

            except asyncio.CancelledError:
                # Handle cancellation gracefully
                logger.info(f"Token processor {processor_id} was cancelled")
                break
            except Exception as e:
                logger.error(f"Error in token processor {processor_id}: {e!s}")
                if self.discord_notifier:
                    await notify_error(
                        self.discord_notifier,
                        f"Processor {processor_id} Error",
                        f"Error processing token: {str(e)}"
                    )
            finally:
                self.token_queue.task_done()
                
        logger.info(f"Token processor {processor_id} stopped")

    async def _handle_token(
        self, token_info: TokenInfo
    ) -> None:
        """Handle a new token creation event.

        Args:
            token_info: Token information
        """
        try:
            # Set trading parameters based on source
            if self.developer_manager is not None:
                # Developer manager mode - use prestored template for ultra-fast execution
                prestored_template = token_info.prestored_template
                
                if prestored_template:
                    logger.info(f"Using prestored template for developer {token_info.user} with params: {token_info.trading_params}")
                    
                    # Execute with prestored template (FASTEST mode)
                    buy_result: TradeResult = await self.buyer.execute(
                        token_info,
                        use_prestored_template=True,
                        prestored_instructions=prestored_template,
                        token_amount=int(token_info.trading_params.get("token_amount", 0))
                    )
                else:
                    logger.warning(f"No prestored template found for developer {token_info.user}, falling back to regular mode")
                    # Fallback to non developer manager mode
                    buy_result: TradeResult = await self.buyer.execute(token_info, token_amount=int(token_info.trading_params.get("token_amount", 0)))
            else:
                #  Non developer manager mode - use static template with bot config
                logger.info(f"Using static template for non developer manager mode")
                buy_result: TradeResult = await self.buyer.execute(token_info, token_amount=int(self.token_amount))

            if buy_result.success:
                await self._handle_successful_buy(token_info, buy_result)
            else:
                await self._handle_failed_buy(token_info, buy_result)

            # Only wait for next token in yolo mode
            if self.yolo_mode:
                logger.info(
                    f"YOLO mode enabled. Waiting {self.wait_time_before_new_token} seconds before looking for next token..."
                )
                await asyncio.sleep(self.wait_time_before_new_token)

        except Exception as e:
            logger.error(f"Error handling token {token_info.symbol}: {e!s}")
            if self.discord_notifier:
                await notify_error(
                    self.discord_notifier,
                    "Token Processing Error",
                    f"Error handling token {token_info.symbol}",
                    f"Details: {str(e)}"
                )

    async def _handle_successful_buy(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle successful token purchase.
        
        Args:
            token_info: Token information
            buy_result: The result of the buy operation
        """
        logger.info(f"Successfully bought {token_info.symbol}")
        self._log_trade(
            "buy",
            token_info,
            buy_result.price,  # type: ignore
            buy_result.amount,  # type: ignore
            buy_result.tx_signature,
        )
        
        # Send Discord notification for token purchase
        if self.discord_notifier:
            await notify_token_buy(
                self.discord_notifier,
                token_info.name,
                str(token_info.mint),
                buy_result.amount,  # type: ignore
                buy_result.price,   # type: ignore
                buy_result.tx_signature,  # type: ignore
                self.sol_to_usd_converter.convert_sol_to_usd(Decimal(str(buy_result.price * buy_result.amount)))
            )
        
        async with self.traded_mints_lock:
            self.traded_mints.add(token_info.mint)
        
        # Sell token if not in marry mode
        if not self.marry_mode:            
            # If using TrailingTokenSeller, pass the buy price as entry_price
            if isinstance(self.seller, TrailingTokenSeller):
                logger.info(f"Using trailing profit/loss strategy for selling | mint: {token_info.mint} | symbol: {token_info.symbol} | entry_price: {buy_result.price}")
                sell_result: TradeResult = await self.seller.execute(
                    token_info, 
                    entry_price=buy_result.price,
                    token_balance=buy_result.amount * 10**TOKEN_DECIMALS,
                    percent_sell_amount=token_info.trading_params.get("percent_sell_amount", None),
                    take_profit_percentage=token_info.trading_params.get("take_profit_percentage", None),
                )
            else:
                logger.info(f"Waiting for {self.wait_time_after_buy} seconds before selling...")
                await asyncio.sleep(self.wait_time_after_buy)
                logger.info(f"Selling {token_info.symbol}...")
                sell_result: TradeResult = await self.seller.execute(token_info)

            if sell_result.success:
                logger.info(f"Successfully sold {token_info.symbol}")
                self._log_trade(
                    "sell",
                    token_info,
                    sell_result.price,  # type: ignore
                    sell_result.amount,  # type: ignore
                    sell_result.tx_signature,
                )
                
                # Send Discord notification for token sale
                if self.discord_notifier:
                    await notify_token_sell(
                        self.discord_notifier,
                        token_info.name,
                        str(token_info.mint),
                        sell_result.amount,  # type: ignore
                        sell_result.price,   # type: ignore
                        sell_result.tx_signature,  # type: ignore
                        getattr(sell_result, 'is_partial', False),
                        getattr(sell_result, 'percent_sold', 1.0),
                        self.sol_to_usd_converter.convert_sol_to_usd(Decimal(str(sell_result.price * sell_result.amount)))
                    )
                    
                    # Calculate and send PnL notification
                    if buy_result.price is not None and sell_result.price is not None:
                        profit_loss = (Decimal(sell_result.price) * Decimal(sell_result.amount)) - (Decimal(buy_result.price) * Decimal(buy_result.amount))
                        profit_loss_usd = profit_loss * Decimal(self.sol_to_usd_converter.get_current_price())
                        profit_loss_percent = ((Decimal(sell_result.price) / Decimal(buy_result.price)) - 1) * 100
                        
                        await notify_pnl(
                            self.discord_notifier,
                            token_info.name,
                            str(token_info.mint),
                            buy_result.price,
                            sell_result.price,
                            profit_loss,
                            profit_loss_percent,
                            profit_loss_usd
                        )
                
                # Close ATA if enabled
                await handle_cleanup_after_sell(
                    self.solana_client, 
                    self.wallet, 
                    token_info.mint, 
                    self.priority_fee_manager,
                    self.cleanup_mode,
                    self.cleanup_with_priority_fee,
                    self.cleanup_force_close_with_burn
                )
            else:
                logger.error(
                    f"Failed to sell {token_info.symbol}: {sell_result.error_message}"
                )
                
                # Send Discord notification for failed sell
                if self.discord_notifier:
                    await notify_error(
                        self.discord_notifier,
                        "Sell Error",
                        f"Failed to sell {token_info.symbol}",
                        f"Details: {sell_result.error_message}"
                    )
        else:
            logger.info("Marry mode enabled. Skipping sell operation.")

    async def _handle_failed_buy(
        self, token_info: TokenInfo, buy_result: TradeResult
    ) -> None:
        """Handle failed token purchase.
        
        Args:
            token_info: Token information
            buy_result: The result of the buy operation
        """
        logger.error(
            f"Failed to buy {token_info.symbol} | token address: {str(token_info.mint)} | creator address: {str(token_info.user)} | error: {buy_result.error_message}"
        )
        
        # Send Discord notification for failed buy
        if self.discord_notifier:
            await notify_error(
                self.discord_notifier,
                "Buy Error",
                f"Failed to buy {token_info.symbol} | token address: {str(token_info.mint)} | creator address: {str(token_info.user)}",
                f"Details: {buy_result.error_message}"
            )
        
        # Close ATA if enabled
        await handle_cleanup_after_failure(
            self.solana_client, 
            self.wallet, 
            token_info.mint, 
            self.priority_fee_manager,
            self.cleanup_mode,
            self.cleanup_with_priority_fee,
            self.cleanup_force_close_with_burn
        )

    async def _save_token_info(
        self, token_info: TokenInfo
    ) -> None:
        """Save token information to a file.

        Args:
            token_info: Token information
        """
        try:
            os.makedirs("trades", exist_ok=True)
            file_name = os.path.join("trades", f"{token_info.mint}.txt")

            with open(file_name, "w") as file:
                file.write(json.dumps(token_info.to_dict(), indent=2))

            logger.info(f"Token information saved to {file_name}")
        except Exception as e:
            logger.error(f"Failed to save token information: {e!s}")

    def _log_trade(
        self,
        action: str,
        token_info: TokenInfo,
        price: float,
        amount: float,
        tx_hash: str | None,
    ) -> None:
        """Log trade information.

        Args:
            action: Trade action (buy/sell)
            token_info: Token information
            price: Token price in SOL
            amount: Trade amount in SOL
            tx_hash: Transaction hash
        """
        try:
            os.makedirs("trades", exist_ok=True)

            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "action": action,
                "token_address": str(token_info.mint),
                "symbol": token_info.symbol,
                "price": float(price),
                "amount": float(amount),
                "tx_hash": str(tx_hash) if tx_hash else None,
            }

            with open("trades/trades.log", "a") as log_file:
                log_file.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            logger.error(f"Failed to log trade information: {e!s}")
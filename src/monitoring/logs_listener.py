"""
WebSocket monitoring for pump.fun tokens using logsSubscribe.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Optional

import websockets
from solders.pubkey import Pubkey

from monitoring.base_listener import BaseTokenListener
from monitoring.developer_manager import DeveloperManager
from monitoring.logs_event_processor import LogsEventProcessor
from trading.base import TokenInfo
from utils.logger import get_logger

logger = get_logger(__name__)


class LogsListener(BaseTokenListener):
    """WebSocket listener for pump.fun token creation events using logsSubscribe."""

    def __init__(
        self, 
        wss_endpoint: str, 
        pump_program: Pubkey, 
        developer_manager: Optional[DeveloperManager] = None
    ):
        """Initialize token listener.

        Args:
            wss_endpoint: WebSocket endpoint URL
            pump_program: Pump.fun program address
            developer_manager: Optional manager for developer whitelist
        """
        super().__init__(developer_manager)
        self.wss_endpoint = wss_endpoint
        self.pump_program = pump_program
        self.event_processor = LogsEventProcessor(pump_program)
        self.ping_interval = 20  # seconds

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        """Listen for new token creations using logsSubscribe.

        Args:
            token_callback: Callback function for new tokens
            match_string: Optional string to match in token name/symbol
            creator_address: Optional creator address to filter by
        """
        while True:
            try:
                async with websockets.connect(self.wss_endpoint) as websocket:
                    await self._subscribe_to_logs(websocket)
                    ping_task = asyncio.create_task(self._ping_loop(websocket))

                    try:
                        while True:
                            token_info = await self._wait_for_token_creation(websocket)
                            if not token_info:
                                continue

                            logger.info(
                                f"New token detected: {token_info.name} ({token_info.symbol})"
                            )

                            # Check with creator_address if provided
                            if creator_address is not None and str(token_info.user) != creator_address:
                                continue

                            # Use the base class method to check if we should process this token
                            # and get trading parameters in one step
                            trading_params = await self.should_process_token(str(token_info.user))
                            if trading_params is None:
                                continue
                            
                            # Attach parameters directly to token_info - no additional lookup needed
                            token_info.trading_params = trading_params
                            if trading_params:
                                logger.info(f"Attached trading parameters to token {token_info.symbol}: {trading_params}")

                            # If using developer manager and no specific creator_address was provided,
                            # mark this developer as sniped to prevent duplicate processing
                            if self.developer_manager is not None and creator_address is None:
                                async def _mark_sniped():
                                    try:
                                        await self.developer_manager.mark_as_sniped(str(token_info.user))
                                    except Exception as e:
                                        logger.error(f"Error marking developer {token_info.user} as sniped: {e}")
                                
                                asyncio.create_task(_mark_sniped())

                            await token_callback(token_info)

                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("WebSocket connection closed. Reconnecting...")
                        ping_task.cancel()

            except Exception as e:
                logger.error(f"WebSocket connection error: {str(e)}")
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def _subscribe_to_logs(self, websocket) -> None:
        """Subscribe to logs mentioning the pump.fun program.

        Args:
            websocket: Active WebSocket connection
        """
        subscription_message = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [str(self.pump_program)]},
                    {"commitment": "processed"},
                ],
            }
        )

        await websocket.send(subscription_message)
        logger.info(f"Subscribed to logs mentioning program: {self.pump_program}")

        # Wait for subscription confirmation
        response = await websocket.recv()
        response_data = json.loads(response)
        if "result" in response_data:
            logger.info(f"Subscription confirmed with ID: {response_data['result']}")
        else:
            logger.warning(f"Unexpected subscription response: {response}")

    async def _ping_loop(self, websocket) -> None:
        """Keep connection alive with pings.

        Args:
            websocket: Active WebSocket connection
        """
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                try:
                    pong_waiter = await websocket.ping()
                    await asyncio.wait_for(pong_waiter, timeout=10)
                except asyncio.TimeoutError:
                    logger.warning("Ping timeout - server not responding")
                    # Force reconnection
                    await websocket.close()
                    return
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Ping error: {str(e)}")

    async def _wait_for_token_creation(self, websocket) -> TokenInfo | None:
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=30)
            data = json.loads(response)

            if "method" not in data or data["method"] != "logsNotification":
                return None

            log_data = data["params"]["result"]["value"]
            logs = log_data.get("logs", [])
            signature = log_data.get("signature", "unknown")

            # Use the processor to extract token info
            return self.event_processor.process_program_logs(logs, signature)

        except asyncio.TimeoutError:
            logger.debug("No data received for 30 seconds")
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            raise
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {str(e)}")

        return None

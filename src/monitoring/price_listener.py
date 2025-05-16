"""
Real-time price monitoring for pump.fun tokens using WebSocket.
"""

import asyncio
import base64
import json
from typing import Callable, Optional

import websockets
from solders.pubkey import Pubkey

from core.curve import BondingCurveManager
from core.pubkeys import PumpAddresses
from utils.logger import get_logger

logger = get_logger(__name__)


class PriceListener:
    """Listens for price changes on a specific token in real-time."""

    def __init__(self, wss_endpoint: str, curve_manager: BondingCurveManager,
                 max_connect_retries: int = 5, initial_retry_delay: float = 1.0,
                 max_retry_delay: float = 30.0):
        """Initialize price listener.

        Args:
            wss_endpoint: WebSocket endpoint URL
            curve_manager: Bonding curve manager for price calculations
            max_connect_retries: Maximum number of connection retry attempts
            initial_retry_delay: Initial delay in seconds for retries
            max_retry_delay: Maximum delay in seconds for retries
        """
        self.wss_endpoint = wss_endpoint
        self.curve_manager = curve_manager
        self.subscription_id: Optional[int] = None
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self._listen_task = None
        self.max_connect_retries = max_connect_retries
        self.initial_retry_delay = initial_retry_delay
        self.max_retry_delay = max_retry_delay

    async def start_monitoring(
        self, 
        bonding_curve: Pubkey, 
        price_callback: Callable[[float], None]
    ) -> None:
        """Start monitoring price changes for a specific token.

        Args:
            bonding_curve: Bonding curve address to monitor
            price_callback: Callback function to call when price changes
        """
        if self.running:
            logger.warning("Price listener is already running. Stop it first.")
            return

        self.running = True
        self._listen_task = asyncio.create_task(
            self._listen_for_price_changes(bonding_curve, price_callback)
        )

    async def stop_monitoring(self) -> None:
        """Stop monitoring price changes."""
        self.running = False
        
        if self._listen_task:
            if not self._listen_task.done():
                self._listen_task.cancel()
                try:
                    await self._listen_task
                except asyncio.CancelledError:
                    logger.info("Price listener task cancelled as expected.")
                except Exception as e:
                    logger.error(f"Error during price listener task shutdown: {e}")
            self._listen_task = None
        
        if self.websocket and self.websocket.open:
            try:
                logger.warning("Attempting fallback websocket close in stop_monitoring.")
                await self.websocket.close()
            except Exception as e:
                logger.error(f"Error during fallback websocket close in stop_monitoring: {e}")
        self.websocket = None
        self.subscription_id = None

    async def _listen_for_price_changes(
        self, 
        bonding_curve: Pubkey,
        price_callback: Callable[[float], None]
    ) -> None:
        """Listen for price changes on a specific bonding curve.

        Args:
            bonding_curve: Bonding curve address to monitor
            price_callback: Callback function to call when price changes
        """
        retry_attempt = 0
        current_delay = self.initial_retry_delay

        while self.running and retry_attempt < self.max_connect_retries:
            try:
                logger.info(f"Attempting to connect to WebSocket: {self.wss_endpoint} (Attempt {retry_attempt + 1})")
                async with websockets.connect(self.wss_endpoint, ping_interval=20, ping_timeout=20) as websocket:
                    self.websocket = websocket
                    logger.info("WebSocket connected successfully.")

                    try:
                        curve_state = await self.curve_manager.get_curve_state(bonding_curve)
                        current_price = curve_state.calculate_price()
                        asyncio.create_task(price_callback(current_price))
                    except Exception as e:
                        logger.error(f"Failed to get initial price for {bonding_curve}: {e}")

                    subscribe_message = json.dumps({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {
                                "mentions": [str(PumpAddresses.PROGRAM)]
                            },
                            {
                                "commitment": "confirmed",
                                "encoding": "base64"
                            }
                        ]
                    })
                    
                    await websocket.send(subscribe_message)
                    response_raw = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    response_json = json.loads(response_raw)
                    
                    if "error" in response_json:
                        logger.error(f"Subscription error: {response_json['error']}")
                        raise websockets.exceptions.ConnectionClosed
                    
                    self.subscription_id = response_json["result"]
                    logger.info(f"Subscribed to pump.fun program logs, ID: {self.subscription_id}")

                    retry_attempt = 0 
                    current_delay = self.initial_retry_delay

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                            await self._process_log_message(msg, bonding_curve, price_callback)
                        except asyncio.TimeoutError:
                            logger.debug("WebSocket recv timed out, checking connection.")
                            if not websocket.open:
                                logger.warning("WebSocket closed during recv timeout.")
                                raise websockets.exceptions.ConnectionClosed
                            continue
                        except (websockets.exceptions.ConnectionClosed, 
                                websockets.exceptions.ConnectionClosedError) as e:
                            logger.error(f"WebSocket connection closed during message processing: {e}")
                            raise
                        except Exception as e:
                            logger.error(f"Error processing WebSocket message: {e}")
                            continue
                
            except asyncio.TimeoutError:
                logger.error(f"Timeout during WebSocket connection/subscription phase.")
            except (websockets.exceptions.InvalidURI,
                    websockets.exceptions.InvalidHandshake,
                    websockets.exceptions.WebSocketException,
                    ConnectionRefusedError, OSError) as e:
                logger.error(f"WebSocket connection/subscription failed: {e!s} (Attempt {retry_attempt + 1})")
            except Exception as e:
                logger.error(f"Unexpected error in price listener's connection phase: {e!s} (Attempt {retry_attempt + 1})")
            
            if self.running and retry_attempt < self.max_connect_retries -1:
                retry_attempt += 1
                logger.info(f"Retrying connection in {current_delay:.2f} seconds...")
                await asyncio.sleep(current_delay)
                current_delay = min(current_delay * 2, self.max_retry_delay)
            elif self.running and retry_attempt >= self.max_connect_retries -1:
                logger.critical(f"Max connection retries ({self.max_connect_retries}) reached. Stopping listener for {bonding_curve}.")
                self.running = False
                break
            elif not self.running:
                logger.info("Listener stop was requested during connection attempts.")
                break

        current_ws = self.websocket
        sub_id = self.subscription_id
        
        if current_ws and current_ws.open and sub_id is not None:
            try:
                logger.info(f"Attempting to unsubscribe from logs (ID: {sub_id}) in _listen_for_price_changes finally block.")
                unsubscribe_message = json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsUnsubscribe",
                    "params": [sub_id]
                })
                await current_ws.send(unsubscribe_message)
                logger.info(f"Sent logsUnsubscribe for ID: {sub_id}.")
            except asyncio.CancelledError:
                logger.info("Unsubscribe cancelled.")
                raise
            except Exception as e:
                logger.error(f"Error during logsUnsubscribe in _listen_for_price_changes: {e}")
        
        if current_ws and current_ws.open:
            try:
                await current_ws.close()
                logger.info("WebSocket closed in _listen_for_price_changes finally block.")
            except Exception as e:
                logger.error(f"Error closing websocket in _listen_for_price_changes finally: {e}")

        self.websocket = None
        self.subscription_id = None
        logger.info(f"Price listener for {bonding_curve} has stopped.")

    async def _process_log_message(
        self, 
        message: str, 
        target_bonding_curve: Pubkey,
        price_callback: Callable[[float], None]
    ) -> None:
        """Process a log message from the WebSocket.

        Args:
            message: WebSocket message string
            target_bonding_curve: The bonding curve address we're monitoring
            price_callback: Callback function to call when price changes
        """
        try:
            msg_json = json.loads(message)
            
            if "method" not in msg_json or msg_json["method"] != "logsNotification":
                return
            
            result = msg_json["params"]["result"]
            
            logs = result.get("value", {}).get("logs", [])
            
            bonding_curve_affected = False
            for log in logs:
                if str(target_bonding_curve) in log:
                    bonding_curve_affected = True
                    break
            
            if bonding_curve_affected:
                curve_state = await self.curve_manager.get_curve_state(target_bonding_curve)
                new_price = curve_state.calculate_price()
                
                logger.info(f"Price update detected: {new_price:.8f} SOL")
                price_callback(new_price)
        
        except Exception as e:
            logger.error(f"Error processing log message: {e}") 
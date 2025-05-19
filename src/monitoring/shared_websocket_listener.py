"""
Manages a single shared WebSocket connection for monitoring pump.fun logs.
"""
import asyncio
import json
from typing import List, Optional, Callable, Dict, Any

import websockets
from core.pubkeys import PumpAddresses
from utils.logger import get_logger

logger = get_logger(__name__)

# Define a type for the processor's handler function
LogNotificationHandler = Callable[[Dict[str, Any]], None]

class SharedWebsocketListener:
    """
    Manages a single shared WebSocket connection and dispatches log notifications
    to registered processors.
    """
    def __init__(self, wss_endpoint: str,
                 max_connect_retries: int = 5, initial_retry_delay: float = 1.0,
                 max_retry_delay: float = 30.0):
        """
        Initialize the shared WebSocket listener.

        Args:
            wss_endpoint: WebSocket endpoint URL.
            max_connect_retries: Maximum number of connection retry attempts.
            initial_retry_delay: Initial delay in seconds for retries.
            max_retry_delay: Maximum delay in seconds for retries.
        """
        self.wss_endpoint = wss_endpoint
        self.max_connect_retries = max_connect_retries
        self.initial_retry_delay = initial_retry_delay
        self.max_retry_delay = max_retry_delay

        self.processors: List[LogNotificationHandler] = []
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.subscription_id: Optional[int] = None
        self.running = False
        self._listen_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock() # To protect access to self.processors

    async def add_processor(self, handler: LogNotificationHandler) -> None:
        """Register a log notification handler."""
        async with self._lock:
            if handler not in self.processors:
                self.processors.append(handler)
                logger.info(f"Added processor. Total processors: {len(self.processors)}")

    async def remove_processor(self, handler: LogNotificationHandler) -> None:
        """Unregister a log notification handler."""
        async with self._lock:
            if handler in self.processors:
                self.processors.remove(handler)
                logger.info(f"Removed processor. Total processors: {len(self.processors)}")

    async def start(self) -> None:
        """Start the shared WebSocket listener."""
        if self.running:
            logger.warning("Shared WebSocket listener is already running.")
            return

        self.running = True
        self._listen_task = asyncio.create_task(self._listen_loop())
        logger.info("Shared WebSocket listener started.")

    async def stop(self) -> None:
        """Stop the shared WebSocket listener."""
        self.running = False
        if self._listen_task:
            if not self._listen_task.done():
                self._listen_task.cancel()
                try:
                    await self._listen_task
                except asyncio.CancelledError:
                    logger.info("Shared listener task cancelled as expected.")
                except Exception as e:
                    logger.error(f"Error during shared listener task shutdown: {e}")
            self._listen_task = None
        
        # Ensure processors list is cleared if needed or handled appropriately on stop
        async with self._lock:
            self.processors.clear()
            logger.info("All processors cleared during stop.")

        # WebSocket closing is handled in _listen_loop's finally block

        logger.info("Shared WebSocket listener stopped.")


    async def _listen_loop(self) -> None:
        """Main loop for listening to WebSocket messages and dispatching them."""
        retry_attempt = 0
        current_delay = self.initial_retry_delay

        while self.running:
            try:
                logger.info(f"SharedWS: Attempting to connect to WebSocket: {self.wss_endpoint} (Attempt {retry_attempt + 1})")
                async with websockets.connect(self.wss_endpoint, ping_interval=20, ping_timeout=20) as websocket:
                    self.websocket = websocket
                    logger.info("SharedWS: WebSocket connected successfully.")

                    subscribe_message = json.dumps({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [str(PumpAddresses.PROGRAM)]},
                            {"commitment": "confirmed", "encoding": "base64"}
                        ]
                    })
                    
                    await websocket.send(subscribe_message)
                    response_raw = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    response_json = json.loads(response_raw)
                    
                    if "error" in response_json:
                        logger.error(f"SharedWS: Subscription error: {response_json['error']}")
                        # If subscription fails, we should treat it like a connection closure to retry
                        self.websocket = None # Ensure we don't try to unsubscribe
                        raise websockets.exceptions.ConnectionClosed(None, None) # Trigger retry
                    
                    self.subscription_id = response_json["result"]
                    logger.info(f"SharedWS: Subscribed to pump.fun program logs, ID: {self.subscription_id}")

                    retry_attempt = 0  # Reset retries on successful connection and subscription
                    current_delay = self.initial_retry_delay

                    while self.running:
                        try:
                            message_raw = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                            message_json = json.loads(message_raw)
                            
                            if message_json.get("method") == "logsNotification":
                                async with self._lock:
                                    if not self.processors: # No point processing if no one is listening
                                        continue
                                    # Dispatch to all registered processors
                                    # Create a list of tasks to run handlers concurrently
                                    # This prevents one slow handler from blocking others
                                    # However, consider if sequential processing is desired for some reason
                                    dispatch_tasks = [
                                        asyncio.create_task(handler(message_json))
                                        for handler in self.processors
                                    ]
                                    # Optionally await these if you need to ensure they complete
                                    # or handle exceptions from individual handlers
                                    # For now, fire and forget.
                                    # await asyncio.gather(*dispatch_tasks, return_exceptions=True)

                        except asyncio.TimeoutError:
                            logger.debug("SharedWS: WebSocket recv timed out, checking connection.")
                            if not websocket.open: # pragma: no cover
                                logger.warning("SharedWS: WebSocket closed during recv timeout.")
                                raise websockets.exceptions.ConnectionClosed(None, None)
                            continue # Continue listening
                        except (websockets.exceptions.ConnectionClosed, 
                                websockets.exceptions.ConnectionClosedError) as e:
                            logger.error(f"SharedWS: WebSocket connection closed during message processing: {e}")
                            raise # Re-raise to trigger outer loop's retry mechanism
                        except json.JSONDecodeError:
                            logger.warning(f"SharedWS: Received invalid JSON: {message_raw[:200]}...") # Log snippet
                            continue
                        except Exception as e: # pragma: no cover
                            logger.error(f"SharedWS: Error processing WebSocket message: {e}", exc_info=True)
                            continue # Continue listening
                
            except asyncio.TimeoutError: # pragma: no cover
                logger.error(f"SharedWS: Timeout during WebSocket connection/subscription phase.")
            except (websockets.exceptions.InvalidURI,
                    websockets.exceptions.InvalidHandshake,
                    websockets.exceptions.WebSocketException,
                    ConnectionRefusedError, OSError) as e: # pragma: no cover
                logger.error(f"SharedWS: WebSocket connection/subscription failed: {e!s} (Attempt {retry_attempt + 1})")
            except Exception as e: # pragma: no cover
                logger.error(f"SharedWS: Unexpected error in listener's connection phase: {e!s} (Attempt {retry_attempt + 1})", exc_info=True)
            
            # Common handling for connection closure or failure to connect/subscribe
            current_ws = self.websocket
            sub_id = self.subscription_id
            self.websocket = None # Ensure it's None so we don't try to reuse a closed/failed ws
            self.subscription_id = None

            if self.running and retry_attempt < self.max_connect_retries -1:
                retry_attempt += 1
                logger.info(f"SharedWS: Retrying connection in {current_delay:.2f} seconds...")
                try:
                    await asyncio.sleep(current_delay)
                except asyncio.CancelledError: # pragma: no cover
                    logger.info("SharedWS: Sleep interrupted by stop request during retry.")
                    break # Exit _listen_loop
                current_delay = min(current_delay * 2, self.max_retry_delay)
            elif self.running and retry_attempt >= self.max_connect_retries -1 : # pragma: no cover
                logger.critical(f"SharedWS: Max connection retries ({self.max_connect_retries}) reached. Stopping listener.")
                self.running = False # This will break the outer while loop
                # Potentially notify the application that the shared listener has failed terminally
                break 
            elif not self.running: # pragma: no cover
                logger.info("SharedWS: Listener stop was requested during connection attempts or error handling.")
                break # Exit _listen_loop

        # Cleanup when _listen_loop exits (either normally or due to error/stop)
        logger.info("SharedWS: Exited _listen_loop.")
        
        ws_to_close = current_ws if current_ws else self.websocket # Use the one that was active
        id_to_unsubscribe = sub_id if sub_id else self.subscription_id

        if ws_to_close and ws_to_close.open and id_to_unsubscribe is not None:
            try:
                logger.info(f"SharedWS: Attempting to unsubscribe from logs (ID: {id_to_unsubscribe}) in _listen_loop finally.")
                unsubscribe_message = json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "logsUnsubscribe", "params": [id_to_unsubscribe]
                })
                await ws_to_close.send(unsubscribe_message)
                logger.info(f"SharedWS: Sent logsUnsubscribe for ID: {id_to_unsubscribe}.")
            except asyncio.CancelledError: # pragma: no cover
                logger.info("SharedWS: Unsubscribe cancelled.")
                # If cancelled, the task is stopping, so we don't re-raise
            except Exception as e: # pragma: no cover
                logger.error(f"SharedWS: Error during logsUnsubscribe: {e}")
        
        if ws_to_close and ws_to_close.open:
            try:
                await ws_to_close.close()
                logger.info("SharedWS: WebSocket closed in _listen_loop finally.")
            except Exception as e: # pragma: no cover
                logger.error(f"SharedWS: Error closing websocket in _listen_loop finally: {e}")

        self.websocket = None
        self.subscription_id = None
        if not self.running: #If we stopped because running was set to False explicitly
             logger.info(f"SharedWS: Listener definitively stopped.")
        else: # If we exited the loop due to max retries
            logger.critical(f"SharedWS: Listener stopped due to persistent connection/subscription issues after max retries.")
            # Here you might want to add a callback or event to notify the main application
            # that the SharedWebsocketListener is no longer functional.
            self.running = False # Ensure running is false if exited due to max retries

    async def _dispatch_notification(self, message_json: Dict[str, Any]) -> None:
        """Helper to dispatch notifications, possibly for more complex scenarios later."""
        # This method isn't strictly necessary for the current fire-and-forget dispatch,
        # but could be expanded if more sophisticated dispatch logic is needed.
        async with self._lock:
            if not self.processors:
                return
            
            # Log the raw message being dispatched if debugging is needed
            # logger.debug(f"SharedWS: Dispatching to {len(self.processors)} processors: {message_json}")

            # Using asyncio.create_task for each handler for non-blocking dispatch
            for handler in self.processors:
                asyncio.create_task(self._safe_call_handler(handler, message_json))

    async def _safe_call_handler(self, handler: LogNotificationHandler, message_json: Dict[str, Any]):
        """Safely call a handler and log exceptions."""
        try:
            await handler(message_json) # Assuming handlers are async; if not, adjust
        except Exception as e: # pragma: no cover
            # Log the exception but don't let one failing handler stop others or the listener.
            # The specific processor should handle its own errors gracefully.
            logger.error(f"SharedWS: Error in processor handler {getattr(handler, '__name__', handler)}: {e}", exc_info=True) 
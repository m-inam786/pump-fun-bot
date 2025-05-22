"""
WebSocket monitoring for pump.fun tokens using PumpPortal's WebSocket API.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Optional

import websockets
from solders.pubkey import Pubkey

from monitoring.base_listener import BaseTokenListener
from monitoring.developer_manager import DeveloperManager
from trading.base import TokenInfo
from utils.logger import get_logger

logger = get_logger(__name__)

# PumpPortal WebSocket URL
PUMP_PORTAL_WS_URL = "wss://pumpportal.fun/api/data"


class PumpPortalListener(BaseTokenListener):
    """WebSocket listener for pump.fun token creation events using PumpPortal's WebSocket API."""

    def __init__(
        self,
        pump_program: Pubkey,
        developer_manager: Optional[DeveloperManager] = None
    ):
        """Initialize token listener.

        Args:
            pump_program: Pump.fun program address
            developer_manager: Optional manager for developer whitelist
        """
        super().__init__(developer_manager)
        self.pump_program = pump_program
        self.ping_interval = 20  # seconds
        self.active_websocket = None
        self.running = True
        self.ping_task = None

    async def stop(self):
        """Stop the listener and clean up resources."""
        logger.info("Stopping PumpPortal listener...")
        self.running = False
        
        # Cancel ping task if it exists
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
            
        # Unsubscribe and close websocket if active
        if self.active_websocket:
            try:
                # Send unsubscribe message
                try:
                    await self._unsubscribe_from_new_tokens(self.active_websocket)
                    logger.info("Successfully unsubscribed from PumpPortal WebSocket")
                except Exception as e:
                    logger.warning(f"Failed to unsubscribe from PumpPortal: {e}")
                
                # Close the WebSocket connection properly
                await self.active_websocket.close()
                logger.info("PumpPortal WebSocket connection closed")
            except Exception as e:
                logger.error(f"Error closing PumpPortal WebSocket connection: {e}")

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        """Listen for new token creations using PumpPortal WebSocket.

        Args:
            token_callback: Callback function for new tokens
            match_string: Optional string to match in token name/symbol
            creator_address: Optional creator address to filter by
        """
        self.running = True
        
        while self.running:
            try:
                async with websockets.connect(PUMP_PORTAL_WS_URL) as websocket:
                    self.active_websocket = websocket
                    await self._subscribe_to_new_tokens(websocket)
                    self.ping_task = asyncio.create_task(self._ping_loop(websocket))

                    try:
                        while self.running:
                            token_info = await self._wait_for_token_creation(websocket)
                            if not token_info:
                                continue

                            # Filter by name/symbol if match_string provided
                            if match_string is not None:
                                match_string_lower = match_string.lower()
                                if (match_string_lower not in token_info.name.lower() and
                                    match_string_lower not in token_info.symbol.lower()):
                                    continue

                            # Check with creator_address if provided
                            if creator_address is not None and str(token_info.user) != creator_address:
                                continue

                            logger.info(
                                f"New token detected via PumpPortal: {token_info.name} ({token_info.symbol})"
                            )

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
                                await self.developer_manager.mark_as_sniped(str(token_info.user))

                            await token_callback(token_info)

                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("PumpPortal WebSocket connection closed. Reconnecting...")
                        if self.ping_task:
                            self.ping_task.cancel()
                    finally:
                        self.active_websocket = None

            except Exception as e:
                logger.error(f"PumpPortal WebSocket connection error: {str(e)}")
                if self.running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)

        logger.info("PumpPortal listener stopped")

    async def _subscribe_to_new_tokens(self, websocket) -> None:
        """Subscribe to new token events on PumpPortal.

        Args:
            websocket: Active WebSocket connection
        """
        subscription_message = json.dumps({"method": "subscribeNewToken", "params": []})
        await websocket.send(subscription_message)
        logger.info("Subscribed to new token events on PumpPortal")

    async def _unsubscribe_from_new_tokens(self, websocket) -> None:
        """Unsubscribe from new token events on PumpPortal.

        Args:
            websocket: Active WebSocket connection
        """
        subscription_message = json.dumps({"method": "unsubscribeNewToken", "params": []})
        await websocket.send(subscription_message)
        logger.info("Unsubscribed from new token events on PumpPortal")

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
        """Wait for and process a token creation event.

        Args:
            websocket: Active WebSocket connection

        Returns:
            TokenInfo object or None if error occurred
        """
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=30)
            data = json.loads(response)

            # Handle PumpPortal format messages
            if "method" in data and data["method"] == "newToken":
                token_info = data.get("params", [{}])[0]
            elif "signature" in data and "mint" in data:
                token_info = data
            else:
                return None

            # Extract necessary fields and create TokenInfo object
            mint_address = token_info.get("mint")
            if not mint_address:
                logger.warning("Received token without mint address")
                return None

            name = token_info.get("name", "")
            symbol = token_info.get("symbol", "")
            creator = token_info.get("traderPublicKey")
            
            if not creator:
                logger.warning(f"Missing creator for token {symbol}")
                return None

            # Create TokenInfo object
            token = TokenInfo(
                mint=Pubkey.from_string(mint_address),
                name=name,
                symbol=symbol,
                user=Pubkey.from_string(creator),
                signature=token_info.get("signature", ""),
                # Optional fields if available
                uri=token_info.get("uri", ""),
                # Convert to program-specific format if needed
                bonding_curve=token_info.get("bondingCurveKey", ""),
                init_price=float(token_info.get("initialBuy", 0.0)),
                init_supply=int(token_info.get("vTokensInBondingCurve", 0)),
                v_sol=float(token_info.get("vSolInBondingCurve", 0.0))
            )

            return token

        except asyncio.TimeoutError:
            logger.debug("No data received for 30 seconds")
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            raise
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {str(e)}")

        return None 
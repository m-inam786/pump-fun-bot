"""
Shreder socket listener for pump.fun tokens via Node.js client.
"""

import asyncio
import json
import websockets
from collections.abc import Awaitable, Callable
from typing import Optional
from datetime import datetime

from monitoring.base_listener import BaseTokenListener
from monitoring.developer_manager import DeveloperManager
from trading.base import TokenInfo
from utils.logger import get_logger
from solders.pubkey import Pubkey

logger = get_logger(__name__)


class ShrederSocketListener(BaseTokenListener):
    """Socket listener for Shreder data from Node.js client."""

    def __init__(
        self, 
        socket_host: str = "localhost",
        socket_port: int = 8765,
        developer_manager: Optional[DeveloperManager] = None
    ):
        """Initialize socket listener.
        
        Args:
            socket_host: WebSocket server host
            socket_port: WebSocket server port
            developer_manager: Optional manager for developer whitelist
        """
        super().__init__(developer_manager)
        self.socket_host = socket_host
        self.socket_port = socket_port
        self.server = None
        self.connected_clients = set()
        
    async def _handle_client(self, websocket, path=None):
        """Handle incoming WebSocket client connections."""
        self.connected_clients.add(websocket)
        logger.info(f"New client connected from {websocket.remote_address}")
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    logger.debug(f"Received data from Node.js client: {data}")
                    
                    # Process the Shreder data
                    await self._process_shreder_data(data)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON received: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {websocket.remote_address} disconnected")
        finally:
            self.connected_clients.discard(websocket)

    async def _process_shreder_data(self, data):
        """Process incoming Shreder data and extract token information.
        
        Args:
            data: Raw data from Shreder Node.js client
        """
        try:
            # Store the callback for processing
            if hasattr(self, '_token_callback') and self._token_callback:
                # Extract token info from Shreder data
                token_info = await self._extract_token_info(data)
                if token_info:
                    logger.info(f"Extracted token from Shreder data: {token_info.name} ({token_info.symbol})")
                    
                    # Check if we should process this token
                    trading_params = await self.should_process_token(str(token_info.user))
                    if trading_params is None:
                        return
                        
                    # Attach parameters to token_info
                    token_info.trading_params = trading_params
                    if trading_params:
                        logger.info(f"Attached trading parameters to token {token_info.symbol}: {trading_params}")

                    # Mark developer as sniped if using developer manager
                    if self.developer_manager is not None:
                        await self.developer_manager.mark_as_sniped(str(token_info.user))

                    await self._token_callback(token_info)
                    
        except Exception as e:
            logger.error(f"Error processing Shreder data: {e}")

    async def _extract_token_info(self, data) -> TokenInfo | None:
        """Extract TokenInfo from Shreder data.
        
        Args:
            data: Raw Shreder data
            
        Returns:
            TokenInfo if extraction successful, None otherwise
        """
        try:
            # This is a placeholder - you'll need to adapt this based on 
            # the actual structure of Shreder data
            
            # Example structure - adjust based on actual Shreder data format
            if 'data' in data and 'transactions' in data['data']:
                transaction_data = data['data']['transactions']
                
                # Extract relevant fields from the transaction
                # This will need to be customized based on Shreder's data structure
                if 'pumpfun' in transaction_data:
                    pump_data = transaction_data['pumpfun']
                    
                    # Create TokenInfo object
                    # You'll need to map Shreder fields to TokenInfo fields
                    token_info = TokenInfo(
                        mint=Pubkey.from_string(pump_data.get('mint', '')),
                        name=pump_data.get('name', 'Unknown'),
                        symbol=pump_data.get('symbol', 'UNK'),
                        uri=pump_data.get('uri', ''),
                        user=Pubkey.from_string(pump_data.get('creator', '')),
                        # Add other required fields based on your TokenInfo structure
                    )
                    
                    return token_info
                    
            # Log the data structure for debugging
            logger.debug(f"Shreder data structure: {json.dumps(data, indent=2)}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting token info from Shreder data: {e}")
            return None

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        """Listen for new token creations via WebSocket from Node.js client.
        
        Args:
            token_callback: Callback function for new tokens
            match_string: Optional string to match in token name/symbol (not used in socket mode)
            creator_address: Optional creator address to filter by
        """
        self._token_callback = token_callback
        self._creator_address = creator_address
        
        logger.info(f"Starting WebSocket server on {self.socket_host}:{self.socket_port}")
        
        try:
            # Start WebSocket server
            self.server = await websockets.serve(
                self._handle_client,
                self.socket_host,
                self.socket_port
            )
            
            logger.info(f"WebSocket server listening on ws://{self.socket_host}:{self.socket_port}")
            logger.info("Waiting for Node.js Shreder client to connect...")
            
            # Keep the server running
            await self.server.wait_closed()
            
        except Exception as e:
            logger.error(f"WebSocket server error: {e}")
            raise

    async def stop(self):
        """Stop the WebSocket server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket server stopped") 
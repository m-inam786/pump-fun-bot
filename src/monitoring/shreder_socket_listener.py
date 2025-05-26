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
from core.pubkeys import SystemAddresses

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
            # Send a welcome message to confirm connection
            welcome_msg = json.dumps({"type": "welcome", "message": "Connected to Python WebSocket server"})
            await websocket.send(welcome_msg)
            logger.info(f"Sent welcome message to client {websocket.remote_address}")
            
            async for message in websocket:
                try:
                    # logger.info(f"Raw message received from {websocket.remote_address}: {message[:200]}...")  # Log first 200 chars
                    data = json.loads(message)
                    logger.info(f"Parsed JSON data from Node.js client: {json.dumps(data, indent=2)[:500]}...")
                    
                    # Process the Shreder data
                    await self._process_shreder_data(data['data'])
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON received from {websocket.remote_address}: {e}")
                    logger.error(f"Raw message was: {message}")
                except Exception as e:
                    logger.error(f"Error processing message from {websocket.remote_address}: {e}")
                    logger.exception("Full traceback:")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {websocket.remote_address} disconnected")
        except Exception as e:
            logger.error(f"Unexpected error in client handler for {websocket.remote_address}: {e}")
            logger.exception("Full traceback:")
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
            # Check if this is transaction data with the expected structure
            if 'transaction' not in data or 'message' not in data['transaction']:
                logger.info(f"No transaction data found in: {json.dumps(data, indent=2)}")
                return None
                
            message = data['transaction']['message']
            if 'instructions' not in message:
                logger.info("No instructions found in transaction message")
                return None
                
            # Look for create instruction (discriminator: [24, 30, 200, 40, 5, 28, 7, 119])
            create_discriminator = [24, 30, 200, 40, 5, 28, 7, 119]
            
            for instruction in message['instructions']:
                if 'data' not in instruction or 'data' not in instruction['data']:
                    continue
                    
                instruction_data = instruction['data']['data']
                
                # Check if this is a create instruction by comparing discriminator
                if len(instruction_data) >= 8 and instruction_data[:8] == create_discriminator:
                    logger.info("Found create instruction in transaction data")
                    
                    # Parse the create instruction data
                    token_info = await self._parse_create_instruction_data(instruction_data, message)
                    if token_info:
                        return token_info
                        
            # If no create instruction found, log the discriminators we did find
            discriminators = []
            for instruction in message['instructions']:
                if 'data' in instruction and 'data' in instruction['data']:
                    data_bytes = instruction['data']['data']
                    if len(data_bytes) >= 8:
                        discriminators.append(data_bytes[:8])
            
            logger.info(f"No create instruction found. Found discriminators: {discriminators}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting token info from Shreder data: {e}")
            logger.info(f"Data structure: {json.dumps(data, indent=2)}")
            return None

    async def _parse_create_instruction_data(self, instruction_data: list, message: dict) -> TokenInfo | None:
        """Parse create instruction data to extract token information.
        
        Args:
            instruction_data: The instruction data bytes as a list
            message: The full transaction message for account lookups
            
        Returns:
            TokenInfo if parsing successful, None otherwise
        """
        try:
            # Convert list to bytes for easier parsing
            data_bytes = bytes(instruction_data)
            
            # Skip the 8-byte discriminator
            offset = 8
            
            # Parse the create instruction arguments: name, symbol, uri, creator
            # Each string is prefixed with a 4-byte length
            
            # Parse name
            if offset + 4 > len(data_bytes):
                return None
            name_length = int.from_bytes(data_bytes[offset:offset+4], 'little')
            offset += 4
            
            if offset + name_length > len(data_bytes):
                return None
            name = data_bytes[offset:offset+name_length].decode('utf-8')
            offset += name_length
            
            # Parse symbol
            if offset + 4 > len(data_bytes):
                return None
            symbol_length = int.from_bytes(data_bytes[offset:offset+4], 'little')
            offset += 4
            
            if offset + symbol_length > len(data_bytes):
                return None
            symbol = data_bytes[offset:offset+symbol_length].decode('utf-8')
            offset += symbol_length
            
            # Parse uri
            if offset + 4 > len(data_bytes):
                return None
            uri_length = int.from_bytes(data_bytes[offset:offset+4], 'little')
            offset += 4
            
            if offset + uri_length > len(data_bytes):
                return None
            uri = data_bytes[offset:offset+uri_length].decode('utf-8')
            offset += uri_length
            
            # Parse creator (32 bytes)
            if offset + 32 > len(data_bytes):
                return None
            creator_bytes = data_bytes[offset:offset+32]
            creator = Pubkey(creator_bytes)
            
            # Extract account addresses from the transaction message
            # We need to map the account indices to actual addresses
            accounts = message.get('accountKeys', [])
            if not accounts:
                logger.error("No account keys found in transaction message")
                return None
                
            # For create instruction, we need:
            # - mint (account index 0)
            # - user (signer, usually one of the accounts)
            # We can derive bonding_curve and other PDAs
            
            if len(accounts) == 0:
                logger.error("No accounts found in transaction")
                return None
                
            # The mint is typically the first account in create transactions
            mint = Pubkey.from_string(accounts[0])
            
            # Find the user (signer) - this requires checking the transaction structure
            # For now, we'll use the creator as the user since they're often the same
            user = creator
            
            # Derive the bonding curve PDA
            bonding_curve, _ = Pubkey.find_program_address(
                [b"bonding-curve", bytes(mint)],
                Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")  # Pump program
            )
            
            # Derive associated bonding curve (ATA)
            from core.pubkeys import SystemAddresses
            associated_bonding_curve, _ = Pubkey.find_program_address(
                [
                    bytes(bonding_curve),
                    bytes(SystemAddresses.TOKEN_PROGRAM),
                    bytes(mint),
                ],
                SystemAddresses.ASSOCIATED_TOKEN_PROGRAM,
            )
            
            # Derive creator vault
            creator_vault, _ = Pubkey.find_program_address(
                [b"creator-vault", bytes(creator)],
                Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")  # Pump program
            )
            
            token_info = TokenInfo(
                name=name,
                symbol=symbol,
                uri=uri,
                mint=mint,
                bonding_curve=bonding_curve,
                associated_bonding_curve=associated_bonding_curve,
                user=user,
                creator=creator,
                creator_vault=creator_vault,
            )
            
            logger.info(f"Successfully parsed token info: {name} ({symbol}) by {creator}")
            return token_info
            
        except Exception as e:
            logger.error(f"Error parsing create instruction data: {e}")
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
        logger.info(f"Token callback set: {token_callback is not None}")
        logger.info(f"Creator address filter: {creator_address}")
        
        try:
            # Start WebSocket server with additional configuration
            self.server = await websockets.serve(
                self._handle_client,
                self.socket_host,
                self.socket_port,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10
            )
            
            logger.info(f"WebSocket server listening on ws://{self.socket_host}:{self.socket_port}")
            logger.info("Server configuration:")
            logger.info(f"  - Host: {self.socket_host}")
            logger.info(f"  - Port: {self.socket_port}")
            logger.info(f"  - Ping interval: 20s")
            logger.info(f"  - Ping timeout: 20s")
            logger.info("Waiting for Node.js Shreder client to connect...")
            
            # Keep the server running
            await self.server.wait_closed()
            
        except Exception as e:
            logger.error(f"WebSocket server error: {e}")
            logger.exception("Full traceback:")
            raise

    async def stop(self):
        """Stop the WebSocket server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket server stopped") 
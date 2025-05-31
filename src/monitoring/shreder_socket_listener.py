"""
Shreder socket listener for pump.fun tokens via Node.js client.
"""

import json
import websockets
from collections.abc import Awaitable, Callable
from typing import Optional

from monitoring.base_listener import BaseTokenListener
from monitoring.developer_manager import DeveloperManager
from trading.base import TokenInfo
from utils.logger import get_logger
from solders.pubkey import Pubkey

logger = get_logger(__name__)

# Pump.fun instruction discriminators for fast comparison
CREATE_DISCRIMINATOR = bytes([24, 30, 200, 40, 5, 28, 7, 119])
BUY_DISCRIMINATOR = bytes([102, 6, 61, 18, 1, 218, 235, 234])


class ShrederSocketListener(BaseTokenListener):
    """Socket listener for Shreder data from Node.js client."""

    def __init__(
        self, 
        socket_host: str = "localhost",
        socket_port: int = 8765,
        developer_manager: Optional[DeveloperManager] = None,
        dev_buy_min_sol: float = 0.01,
        dev_buy_max_sol: float = 10.0,
    ):
        """Initialize socket listener.
        
        Args:
            socket_host: WebSocket server host
            socket_port: WebSocket server port
            developer_manager: Optional manager for developer whitelist
            dev_buy_min_sol: Minimum SOL amount for dev buy filtering
            dev_buy_max_sol: Maximum SOL amount for dev buy filtering
        """
        super().__init__(developer_manager)
        self.socket_host = socket_host
        self.socket_port = socket_port
        self.server = None
        self.connected_clients = set()
        
        # Convert SOL amounts to lamports for fast comparison
        self.min_dev_buy_lamports = int(dev_buy_min_sol * 1_000_000_000)
        self.max_dev_buy_lamports = int(dev_buy_max_sol * 1_000_000_000)
        
        logger.info(f"Dev buy filter configured: {dev_buy_min_sol} - {dev_buy_max_sol} SOL ({self.min_dev_buy_lamports} - {self.max_dev_buy_lamports} lamports)")
        
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
                    # logger.info(f"Parsed JSON data from Node.js client: {json.dumps(data, indent=2)[:500]}...")
                    
                    # Process the Shreder data
                    await self._process_shreder_data(data)
                    
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
                # Extract token info from Shreder data with dev buy analysis
                token_info, dev_buy_amount = await self._extract_token_info_with_dev_buy(data)
                if token_info:
                    # Apply dev buy filtering - only proceed if within acceptable range
                    if dev_buy_amount is not None:
                        if self.min_dev_buy_lamports <= dev_buy_amount <= self.max_dev_buy_lamports:
                            dev_buy_sol = dev_buy_amount / 1_000_000_000
                            logger.info(f"✅ DEV BUY DETECTED within range: {dev_buy_sol:.4f} SOL for token {token_info.symbol}")
                        else:
                            dev_buy_sol = dev_buy_amount / 1_000_000_000
                            logger.info(f"❌ Dev buy outside range: {dev_buy_sol:.4f} SOL for token {token_info.symbol} - skipping")
                            return
                    else:
                        logger.info(f"❌ No dev buy detected for token {token_info.symbol} - skipping")
                        return
                    
                    logger.info(f"Extracted token from Shreder data: {token_info.name} ({token_info.symbol})")
                    
                    # Check if we should process this token
                    result = await self.should_process_token(str(token_info.creator))
                    if result is None:
                        return
                        
                    # Extract trading parameters and prestored template
                    trading_params, prestored_template = result
                    
                    # Attach parameters and prestored template to token_info
                    token_info.trading_params = trading_params
                    token_info.prestored_template = prestored_template
                    if trading_params:
                        logger.info(f"Attached trading parameters to token {token_info.symbol}: {trading_params} (developer mode)")
                    if prestored_template:
                        logger.info(f"Attached prestored template to token {token_info.symbol} (developer mode)")

                    # Mark developer as sniped if using developer manager
                    if self.developer_manager is not None:
                        await self.developer_manager.mark_as_sniped(str(token_info.creator))

                    await self._token_callback(token_info)
                    
        except Exception as e:
            logger.error(f"Error processing Shreder data: {e}")
            logger.exception("Full traceback:")

    def _convert_buffer_to_bytes(self, buffer_obj):
        """Convert Node.js Buffer object to Python bytes.
        
        Args:
            buffer_obj: Buffer object with 'type': 'Buffer' and 'data': [...]
            
        Returns:
            bytes object
        """
        if isinstance(buffer_obj, dict) and buffer_obj.get('type') == 'Buffer':
            return bytes(buffer_obj['data'])
        return buffer_obj

    def _parse_buy_amount_lamports(self, instruction_data: bytes) -> int | None:
        """Parse buy amount in lamports from buy instruction data.
        
        Args:
            instruction_data: Buy instruction data as bytes
            
        Returns:
            Amount in lamports if parsing successful, None otherwise
        """
        try:
            # Skip 8-byte discriminator, then skip first u64 (amount)
            # max_sol_cost is the second u64 at bytes 16-24 (little endian)
            if len(instruction_data) >= 24:
                # Extract 8 bytes starting at offset 16
                amount_bytes = instruction_data[16:24]
                # Convert from little endian bytes to int
                return int.from_bytes(amount_bytes, 'little')
        except Exception:
            pass
        return None

    async def _extract_token_info_with_dev_buy(self, data) -> tuple[TokenInfo | None, int | None]:
        """Extract TokenInfo and dev buy amount from Shreder data.
        
        Args:
            data: Raw Shreder data from Node.js client
            
        Returns:
            Tuple of (TokenInfo if extraction successful, dev_buy_amount_lamports if detected)
        """
        try:
            # Check if this is the expected Shreder data structure
            data = data['data']
            if 'transaction' not in data:
                return None, None
                
            transaction_data = data['transaction']
            if 'transaction' not in transaction_data:
                return None, None
                
            transaction = transaction_data['transaction']
            if 'message' not in transaction:
                return None, None
                
            message = transaction['message']
            instructions = message.get('instructions')
            if not instructions:
                return None, None
                
            # Convert account keys from Buffer objects to bytes (pre-allocate for speed)
            account_keys = []
            if 'accountKeys' in message:
                account_keys_data = message['accountKeys']
                account_keys = [self._convert_buffer_to_bytes(acc) for acc in account_keys_data]
            
            # Optimized single-pass instruction analysis
            create_instruction = None
            create_accounts = None
            buy_instructions = []
            found_create = False
            
            # Single loop with early optimization checks
            for instruction in instructions:
                if 'data' not in instruction:
                    continue
                    
                # Convert instruction data from Buffer to bytes
                instruction_data = self._convert_buffer_to_bytes(instruction['data'])
                
                # Fast discriminator check with early exit
                if len(instruction_data) < 8:
                    continue
                    
                discriminator = instruction_data[:8]
                
                # Check for create instruction first (most important)
                if discriminator == CREATE_DISCRIMINATOR:
                    logger.info("Found create instruction in Shreder transaction data")
                    create_instruction = instruction_data
                    found_create = True
                    
                    # Get the instruction accounts using account indices
                    if 'accounts' in instruction:
                        account_indices = self._convert_buffer_to_bytes(instruction['accounts'])
                        # Pre-allocate list for speed
                        instruction_accounts = []
                        for i in range(len(account_indices)):
                            account_index = account_indices[i]
                            if account_index < len(account_keys):
                                instruction_accounts.append(account_keys[account_index])
                        create_accounts = instruction_accounts
                        
                # Only collect buy instructions if we found a create (optimization)
                elif found_create and discriminator == BUY_DISCRIMINATOR:
                    # Store buy instruction with accounts for dev buy detection
                    if 'accounts' in instruction:
                        account_indices = self._convert_buffer_to_bytes(instruction['accounts'])
                        buy_accounts = []
                        for i in range(len(account_indices)):
                            account_index = account_indices[i]
                            if account_index < len(account_keys):
                                buy_accounts.append(account_keys[account_index])
                        
                        buy_instructions.append({
                            'data': instruction_data,
                            'accounts': buy_accounts
                        })
            
            # Early exit if no create instruction found
            if not found_create or create_instruction is None or create_accounts is None:
                return None, None
                
            # Parse token info (only if we have create instruction)
            token_info = await self._parse_create_instruction_data(create_instruction, create_accounts)
            if not token_info:
                return None, None
                
            # Check for dev buy pattern (only if we have buy instructions)
            dev_buy_amount = None
            if buy_instructions:
                dev_buy_amount = self._detect_dev_buy(token_info.creator, buy_instructions)
                
            return token_info, dev_buy_amount
            
        except Exception as e:
            logger.error(f"Error extracting token info with dev buy from Shreder data: {e}")
            logger.exception("Full traceback:")
            return None, None

    def _detect_dev_buy(self, creator_pubkey: Pubkey, buy_instructions: list) -> int | None:
        """Detect if creator also bought in the same transaction (dev buy).
        
        Args:
            creator_pubkey: Creator pubkey 
            buy_instructions: List of buy instruction data and accounts
            
        Returns:
            Buy amount in lamports if dev buy detected, None otherwise
        """
        if not buy_instructions:
            return None
            
        # Convert creator pubkey to bytes once for fast comparison
        creator_bytes = bytes(creator_pubkey)
        
        # Fast iteration with early exit
        for buy_instr in buy_instructions:
            buy_accounts = buy_instr['accounts']
            
            # Quick length check before accessing index 6
            if len(buy_accounts) <= 6:
                continue
                
            buyer_pubkey = buy_accounts[6]
            
            # Fast bytes comparison - check if creator == buyer
            if buyer_pubkey == creator_bytes:
                # Parse buy amount from instruction data (only when match found)
                buy_amount = self._parse_buy_amount_lamports(buy_instr['data'])
                if buy_amount and buy_amount > 0:  # Additional sanity check
                    return buy_amount
                    
        return None

    async def _parse_create_instruction_data(self, instruction_data: bytes, instruction_accounts: list) -> TokenInfo | None:
        """Parse create instruction data to extract token information.
        
        Args:
            instruction_data: The instruction data as bytes
            instruction_accounts: List of instruction account keys as bytes (in order)
            
        Returns:
            TokenInfo if parsing successful, None otherwise
        """
        try:
            # Skip the 8-byte discriminator
            offset = 8
            
            # Parse the create instruction arguments: name, symbol, uri, creator
            # Each string is prefixed with a 4-byte length
            
            # Parse name
            if offset + 4 > len(instruction_data):
                return None
            name_length = int.from_bytes(instruction_data[offset:offset+4], 'little')
            offset += 4
            
            if offset + name_length > len(instruction_data):
                return None
            name = instruction_data[offset:offset+name_length].decode('utf-8')
            offset += name_length
            
            # Parse symbol
            if offset + 4 > len(instruction_data):
                return None
            symbol_length = int.from_bytes(instruction_data[offset:offset+4], 'little')
            offset += 4
            
            if offset + symbol_length > len(instruction_data):
                return None
            symbol = instruction_data[offset:offset+symbol_length].decode('utf-8')
            offset += symbol_length
            
            # Parse uri
            if offset + 4 > len(instruction_data):
                return None
            uri_length = int.from_bytes(instruction_data[offset:offset+4], 'little')
            offset += 4
            
            if offset + uri_length > len(instruction_data):
                return None
            uri = instruction_data[offset:offset+uri_length].decode('utf-8')
            offset += uri_length
            
            # Parse creator (32 bytes) - this is embedded in the instruction data
            if offset + 32 > len(instruction_data):
                return None
            creator_bytes = instruction_data[offset:offset+32]
            creator = Pubkey(creator_bytes)
            
            # Extract account addresses from the instruction accounts
            # Based on the IDL, the create instruction accounts are:
            # 0: mint (writable, signer)
            # 1: mint_authority (PDA)
            # 2: bonding_curve (writable, PDA)
            # 3: associated_bonding_curve (writable, PDA)
            # 4: metadata (writable, PDA)
            # 5: user (writable, signer)
            # 6: system_program
            # 7: token_program
            # 8: associated_token_program
            # 9: rent
            
            if len(instruction_accounts) < 6:
                logger.error(f"Not enough accounts in create instruction: {len(instruction_accounts)}")
                return None
                
            # Extract the key accounts
            mint = Pubkey(instruction_accounts[0])
            bonding_curve = Pubkey(instruction_accounts[2])
            associated_bonding_curve = Pubkey(instruction_accounts[3])
            user = Pubkey(instruction_accounts[5])  # The user/signer account
            
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
            
            logger.info(f"Successfully parsed token info from Shreder data: {token_info.to_dict()}")
            return token_info
            
        except Exception as e:
            logger.error(f"Error parsing create instruction data from Shreder: {e}")
            logger.exception("Full traceback:")
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
"""
Solana client abstraction for blockchain operations.
"""

import asyncio
import json
from typing import Any, Union, Optional

import aiohttp
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed
from solana.rpc.types import TxOpts
from solders.hash import Hash
from solders.instruction import Instruction
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.transaction import Transaction
from utils.logger import get_logger
from core.nonce_manager import NonceManager
from solders.system_program import transfer, TransferParams

logger = get_logger(__name__)

class SolanaClient:
    """Abstraction for Solana RPC client operations."""

    def __init__(self, rpc_endpoint: str, tip_configs: list[dict] | None = None, nonce_file_path: str | None = None):
        """Initialize Solana client with RPC endpoint and optional tip configurations.

        Args:
            rpc_endpoint: URL of the Solana RPC endpoint
            tip_configs: List of tip configurations with tip_lamports, tip_account, and tip_rpc_url
            nonce_file_path: Path to nonce account file for durable transactions
        """
        self.rpc_endpoint = rpc_endpoint
        self._client = None
        
        # Initialize multiple tip clients
        self.tip_configs = tip_configs or []
        self._tip_clients = []
        
        self._cached_blockhash: Hash | None = None
        self._blockhash_lock = asyncio.Lock()
        self._blockhash_updater_task = None
        
        # Initialize nonce manager if nonce file is provided
        self.nonce_manager: Optional[NonceManager] = None
        self.nonce_file_path = nonce_file_path
        self.use_durable_nonce = nonce_file_path is not None
        
        # Keep-alive task for 0slot.trade tip clients
        self._tip_keepalive_task = None

    async def start(self):
        """Start the client and tip clients."""
        self._client = await self.get_client()
        
        # Initialize tip clients for each tip configuration
        for i, tip_config in enumerate(self.tip_configs):
            tip_client = AsyncClient(tip_config['tip_rpc_url'])
            self._tip_clients.append(tip_client)
            logger.info(f"Initialized tip client {i+1} for {tip_config['tip_rpc_url']}")
        
        # Initialize nonce manager if using durable nonces
        if self.use_durable_nonce and self.nonce_file_path:
            self.nonce_manager = NonceManager(self._client)
            await self.nonce_manager.load_nonce_account(self.nonce_file_path)
            logger.info("Durable nonce system initialized")

        # Start blockhash updater
        self._blockhash_updater_task = asyncio.create_task(self.start_blockhash_updater())
        logger.info("Recent blockhash system initialized")
        
        # Start keep-alive checker for 0slot.trade tip clients
        self._tip_keepalive_task = asyncio.create_task(self.start_tip_keepalive())
        logger.info("Tip client keep-alive system initialized")

    async def start_blockhash_updater(self, interval: float = 1):
        """Start background task to update recent blockhash."""
        while True:
            try:
                blockhash = await self.get_latest_blockhash()
                async with self._blockhash_lock:
                    self._cached_blockhash = blockhash
            except Exception as e:
                logger.warning(f"Blockhash fetch failed: {e!s}")
            finally:
                await asyncio.sleep(interval)

    async def get_cached_blockhash(self) -> Hash:
        """Return the most recently cached blockhash."""
        async with self._blockhash_lock:
            if self._cached_blockhash is None:
                raise RuntimeError("No cached blockhash available yet")
            return self._cached_blockhash

    async def is_connected(self, client: AsyncClient) -> bool:
        """Check if a client connection is healthy.
        
        Args:
            client: AsyncClient instance to check
            
        Returns:
            True if client is connected and healthy, False otherwise
        """
        try:
            # Try to get the health status of the client
            response = await client.is_connected()
            return response is not None
        except Exception as e:
            logger.warning(f"Client health check failed: {e!s}")
            return False

    async def start_tip_keepalive(self, interval: float = 60):
        """Start background task to check keep-alive for 0slot.trade tip clients.
        
        Args:
            interval: Interval in seconds between keep-alive checks
        """
        while True:
            try:
                # Check each tip client that has 0slot.trade in the URL
                for i, (tip_client, tip_config) in enumerate(zip(self._tip_clients, self.tip_configs)):
                    tip_url = tip_config.get('tip_rpc_url', '')
                    if '0slot.trade' in tip_url:
                        is_healthy = await self.is_connected(tip_client)
                        if is_healthy:
                            logger.debug(f"Tip client {i+1} (0slot.trade) is healthy")
                        else:
                            logger.warning(f"Tip client {i+1} (0slot.trade) connection issue detected: {tip_url}")
                            
            except Exception as e:
                logger.error(f"Tip keep-alive check failed: {e!s}")
            finally:
                await asyncio.sleep(interval)

    async def get_client(self) -> AsyncClient:
        """Get or create the AsyncClient instance.

        Returns:
            AsyncClient instance
        """
        if self._client is None:
            self._client = AsyncClient(self.rpc_endpoint)
        return self._client

    async def close(self):
        """Close the client connection and stop the blockhash updater."""
        if self._blockhash_updater_task:
            self._blockhash_updater_task.cancel()
            try:
                await self._blockhash_updater_task
            except asyncio.CancelledError:
                pass

        if self._tip_keepalive_task:
            self._tip_keepalive_task.cancel()
            try:
                await self._tip_keepalive_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.close()
            self._client = None
        
        # Close all tip clients
        for tip_client in self._tip_clients:
            await tip_client.close()
        self._tip_clients = []

    async def get_health(self) -> str | None:
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth",
        }
        result = await self.post_rpc(body)
        if result and "result" in result:
            return result["result"]
        return None

    async def get_account_info(self, pubkey: Pubkey) -> dict[str, Any]:
        """Get account info from the blockchain.

        Args:
            pubkey: Public key of the account

        Returns:
            Account info response

        Raises:
            ValueError: If account doesn't exist or has no data
        """
        client = await self.get_client()
        response = await client.get_account_info(pubkey, encoding="base64") # base64 encoding for account data by default
        if not response.value:
            raise ValueError(f"Account {pubkey} not found")
        return response.value

    async def get_token_account_balance(self, token_account: Pubkey) -> int:
        """Get token balance for an account.

        Args:
            token_account: Token account address

        Returns:
            Token balance as integer
        """
        client = await self.get_client()
        response = await client.get_token_account_balance(token_account)
        if response.value:
            return int(response.value.amount)
        return 0

    async def get_latest_blockhash(self) -> Hash:
        """Get the latest blockhash.

        Returns:
            Recent blockhash as string
        """
        client = await self.get_client()
        response = await client.get_latest_blockhash(commitment="processed")
        return response.value.blockhash

    async def build_and_send_transaction(
        self,
        instructions: list[Instruction],
        signer_keypair: Keypair,
        skip_preflight: bool = True,
        max_retries: int = 3,
        tx_type: str = "sell",
    ) -> str:
        """
        Send a transaction with durable nonce or recent blockhash.
        For buy transactions, uses multiple tip services concurrently if available.

        Args:
            instructions: List of instructions to include in the transaction.
            signer_keypair: Primary signer keypair
            skip_preflight: Whether to skip preflight checks.
            max_retries: Maximum number of retry attempts.
            tx_type: Type of transaction for routing

        Returns:
            Transaction signature.
        """
        client = await self.get_client()

        # Prepare transaction instructions and signers
        tx_instructions = []
        signers = [signer_keypair]
        
        if tx_type == "buy":
            if self.use_durable_nonce and self.nonce_manager:
                # Use durable nonce - add advance nonce instruction as first instruction
                advance_nonce_ix = await self.nonce_manager.create_advance_nonce_instruction()
                tx_instructions.append(advance_nonce_ix)
                
                # Add nonce authority as signer
                nonce_authority = self.nonce_manager.get_authority_keypair()
                if nonce_authority.pubkey() != signer_keypair.pubkey():
                    signers.append(nonce_authority)
                
                # Get current nonce as blockhash
                nonce_hash = await self.nonce_manager.get_current_nonce()
                blockhash = nonce_hash
                
                logger.info(f"Using durable nonce: {nonce_hash}")
            else:
                # Use recent blockhash
                blockhash = await self.get_cached_blockhash()
                logger.info(f"Using recent cached blockhash: {blockhash}")
        else:
            blockhash = await self.get_cached_blockhash()
            logger.info(f"Using recent cached blockhash for sell transaction: {blockhash}")

        # Add the provided instructions
        tx_instructions.extend(instructions)

        for attempt in range(max_retries):
            try:
                tx_opts = TxOpts(
                    skip_preflight=skip_preflight, preflight_commitment=Processed
                )
                
                # Use multiple tip services concurrently for buy transactions
                if self._tip_clients and tx_type == "buy":
                    logger.info(f"Sending buy transaction via {len(self._tip_clients)} tip services concurrently")
                    
                    # Create transactions with different tip instructions for each tip service
                    tip_tasks = []
                    for i, (tip_client, tip_config) in enumerate(zip(self._tip_clients, self.tip_configs)):
                        # Create a copy of instructions and add tip instruction for this service
                        tx_instructions_with_tip = tx_instructions.copy()
                        tip_instruction = transfer(
                            TransferParams(
                                from_pubkey=signer_keypair.pubkey(),
                                to_pubkey=Pubkey.from_string(tip_config['tip_account']),
                                lamports=tip_config['tip_lamports'],
                            )
                        )
                        
                        # Insert tip instruction after nonce (if present) or at the beginning
                        if self.use_durable_nonce and self.nonce_manager:
                            # Insert tip instruction after nonce instruction (index 1)
                            tx_instructions_with_tip.insert(1, tip_instruction)
                        else:
                            # Insert tip instruction at the beginning
                            tx_instructions_with_tip.insert(0, tip_instruction)
                        
                        # Create message and transaction for this tip service
                        message = Message.new_with_blockhash(
                            tx_instructions_with_tip,
                            signer_keypair.pubkey(),  # payer
                            blockhash
                        )
                        transaction = Transaction(signers, message, blockhash)
                        
                        # Add task for this tip service
                        tip_tasks.append(tip_client.send_transaction(transaction, tx_opts))
                    
                    tip_responses = await asyncio.gather(*tip_tasks, return_exceptions=True)
                    
                    # Process responses and return the first successful one
                    for i, tip_response in enumerate(tip_responses):
                        if isinstance(tip_response, Exception):
                            logger.warning(f"Tip service {i+1} failed: {str(tip_response)}")
                            continue
                        response = tip_response
                        logger.info(f"Successful buy transaction via tip service {i+1}: {tip_response!s}")
                    else:
                        # All tip services failed, fallback to regular client
                        logger.warning("All tip services failed, falling back to regular RPC")
                        message = Message.new_with_blockhash(
                            tx_instructions,
                            signer_keypair.pubkey(),  # payer
                            blockhash
                        )
                        transaction = Transaction(signers, message, blockhash)
                        response = await client.send_transaction(transaction, tx_opts)
                else:
                    # Use regular client for sell transactions or when no tip clients
                    message = Message.new_with_blockhash(
                        tx_instructions,
                        signer_keypair.pubkey(),  # payer
                        blockhash
                    )
                    transaction = Transaction(signers, message, blockhash)
                    response = await client.send_transaction(transaction, tx_opts)
                
                # If using durable nonce, advance it after successful transaction
                if self.use_durable_nonce and self.nonce_manager:
                    # Note: We advance the nonce optimistically here
                    # In production, you might want to wait for confirmation first
                    logger.info(f"Advancing nonce for next buy transaction")
                    await self.nonce_manager.advance_nonce_and_update()
                
                return response.value

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        f"Failed to send transaction after {max_retries} attempts"
                    )
                    raise

                wait_time = 2**attempt
                logger.warning(
                    f"Transaction attempt {attempt + 1} failed: {e!s}, retrying in {wait_time}s, error: {e!s}"
                )
                await asyncio.sleep(wait_time)

    async def confirm_transaction(
        self, signature: str, commitment: str = "confirmed"
    ) -> bool | None:
        """Wait for transaction confirmation.

        Args:
            signature: Transaction signature
            commitment: Confirmation commitment level

        Returns:
            Whether transaction was confirmed
        """
        client = await self.get_client()
        try:
            await client.confirm_transaction(signature, commitment=commitment, sleep_seconds=1)
            return True
        except Exception as e:
            logger.error(f"Failed to confirm transaction {signature}: {e!s}")
            return False

    async def get_transaction_details(self, 
                                      signature: str, 
                                      commitment: str = "confirmed") -> dict[str, Any] | None:
        """Get transaction details."""
        client = await self.get_client()
        try:
            response = await client.get_transaction(signature, commitment=commitment)
            return response.value
        except Exception as e:
            logger.error(f"Failed to get transaction details {signature}: {e!s}")
            return None

    async def post_rpc(self, body: dict[str, Any]) -> dict[str, Any] | None:
        """
        Send a raw RPC request to the Solana node.

        Args:
            body: JSON-RPC request body.

        Returns:
            Optional[Dict[str, Any]]: Parsed JSON response, or None if the request fails.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.rpc_endpoint,
                    json=body,
                    timeout=aiohttp.ClientTimeout(10),  # 10-second timeout
                ) as response:
                    response.raise_for_status()
                    return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"RPC request failed: {e!s}", exc_info=True)
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode RPC response: {e!s}", exc_info=True)
            return None

"""
Solana client abstraction for blockchain operations.
"""

import asyncio
import json
from typing import Any, Union

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

logger = get_logger(__name__)

# Jito sandwich protection public key
JITO_DONT_FRONT_PUBKEY = Pubkey.from_string("jitodontfront11111111111111111111111Wrecker")

class SolanaClient:
    """Abstraction for Solana RPC client operations."""

    def __init__(self, rpc_endpoint: str, tip_rpc_url: str | None = None):
        """Initialize Solana client with RPC endpoint.

        Args:
            rpc_endpoint: URL of the Solana RPC endpoint
            tip_rpc_url: URL of the custom tip RPC endpoint
        """
        self.rpc_endpoint = rpc_endpoint
        self._client = None
        self.tip_rpc_url = tip_rpc_url
        self._tip_client = None
        self._cached_blockhash: Hash | None = None
        self._blockhash_lock = asyncio.Lock()
        self._blockhash_updater_task = asyncio.create_task(self.start_blockhash_updater())

    async def start(self):
        """Start the client and tip client."""
        self._client = await self.get_client()
        self._tip_client = await self.get_tip_client()

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

    async def get_client(self) -> AsyncClient:
        """Get or create the AsyncClient instance.

        Returns:
            AsyncClient instance
        """
        if self._client is None:
            self._client = AsyncClient(self.rpc_endpoint)
        return self._client
    
    async def get_tip_client(self) -> AsyncClient:
        """Get or create the AsyncClient instance for tip RPC URL."""
        if self._tip_client is None:
            self._tip_client = AsyncClient(self.tip_rpc_url)
        return self._tip_client

    async def close(self):
        """Close the client connection and stop the blockhash updater."""
        if self._blockhash_updater_task:
            self._blockhash_updater_task.cancel()
            try:
                await self._blockhash_updater_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.close()
            self._client = None
        
        if self._tip_client:
            await self._tip_client.close()
            self._tip_client = None

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
        Send a transaction with optional priority fee and tip.

        Args:
            instructions: List of instructions to include in the transaction.
            skip_preflight: Whether to skip preflight checks.
            max_retries: Maximum number of retry attempts.

        Returns:
            Transaction signature.
        """
        client = await self.get_client()

        recent_blockhash = await self.get_cached_blockhash()
        
        # Create message with instructions and payer
        message = Message.new_with_blockhash(
            instructions,
            signer_keypair.pubkey(),  # payer
            recent_blockhash
        )
        
        # Create transaction with signers and message
        transaction = Transaction([signer_keypair], message, recent_blockhash)

        for attempt in range(max_retries):
            try:
                tx_opts = TxOpts(
                    skip_preflight=skip_preflight, preflight_commitment=Processed
                )
                # Use tip RPC client if tip is provided, otherwise use default client
                if self.tip_rpc_url and tx_type == "buy":
                    tip_client = await self.get_tip_client()
                    response = await tip_client.send_transaction(transaction, tx_opts)
                else:
                    response = await client.send_transaction(transaction, tx_opts)
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
            await client.confirm_transaction(signature, commitment=commitment)
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

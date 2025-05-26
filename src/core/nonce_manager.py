"""
Durable nonce account manager for reliable transaction execution.
"""

import asyncio
import base64
import json
import struct
from typing import Optional

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import advance_nonce_account, AdvanceNonceAccountParams
from solders.instruction import Instruction
from solders.hash import Hash
from utils.logger import get_logger
from solana.rpc.async_api import AsyncClient

logger = get_logger(__name__)


class NonceAccount:
    """Represents a durable nonce account with its current state."""
    
    def __init__(self, nonce_keypair: Keypair, authority_keypair: Keypair, current_nonce: Hash):
        """Initialize nonce account.
        
        Args:
            nonce_keypair: The nonce account keypair
            authority_keypair: The authority keypair that can advance the nonce
            current_nonce: The current nonce value (Hash)
        """
        self.nonce_keypair = nonce_keypair
        self.authority_keypair = authority_keypair
        self.current_nonce = current_nonce
        self._lock = asyncio.Lock()
    
    @property
    def pubkey(self) -> Pubkey:
        """Get the nonce account public key."""
        return self.nonce_keypair.pubkey()
    
    @property
    def authority_pubkey(self) -> Pubkey:
        """Get the authority public key."""
        return self.authority_keypair.pubkey()


class NonceManager:
    """Manages durable nonce accounts for transaction execution."""
    
    def __init__(self, client: AsyncClient):
        """Initialize nonce manager.
        
        Args:
            client: SolanaClient instance for blockchain operations
        """
        self.client: AsyncClient = client
        self.nonce_account: Optional[NonceAccount] = None
        self._nonce_lock = asyncio.Lock()
    
    async def load_nonce_account(self, nonce_file_path: str) -> None:
        """Load nonce account from file.
        
        Args:
            nonce_file_path: Path to the nonce account JSON file
        """
        try:
            with open(nonce_file_path, 'r') as f:
                nonce_data = json.load(f)
            
            # Load nonce account keypair
            nonce_secret_key = base64.b64decode(nonce_data['nonce_account']['secret_key'])
            nonce_keypair = Keypair.from_bytes(nonce_secret_key)
            
            # Load authority keypair
            authority_secret_key = base64.b64decode(nonce_data['authority']['secret_key'])
            authority_keypair = Keypair.from_bytes(authority_secret_key)
            
            # Get current nonce value from blockchain
            current_nonce = await self._fetch_current_nonce(nonce_keypair.pubkey())
            
            self.nonce_account = NonceAccount(nonce_keypair, authority_keypair, current_nonce)
            
            logger.info(f"Loaded nonce account: {nonce_keypair.pubkey()}")
            logger.info(f"Current nonce: {current_nonce}")
            
        except Exception as e:
            logger.error(f"Failed to load nonce account: {e}")
            raise
    
    async def _fetch_current_nonce(self, nonce_pubkey: Pubkey) -> Hash:
        """Fetch the current nonce value from the blockchain.
        
        Args:
            nonce_pubkey: Public key of the nonce account
            
        Returns:
            Current nonce value as Hash
        """
        try:
            response = await self.client.get_account_info_json_parsed(pubkey=nonce_pubkey)
            if not response or not response.value:
                raise ValueError(f"Nonce account {nonce_pubkey} not found or has no data")
            nonce = response.value.data.parsed['info']["blockhash"]
            return Hash.from_string(nonce)
            
        except Exception as e:
            logger.error(f"Failed to fetch current nonce: {e}")
            raise
    
    async def get_current_nonce(self) -> Hash:
        """Get the current nonce value.
        
        Returns:
            Current nonce value as Hash
        """
        if not self.nonce_account:
            raise RuntimeError("Nonce account not loaded")
        
        async with self._nonce_lock:
            return self.nonce_account.current_nonce
    
    async def create_advance_nonce_instruction(self) -> Instruction:
        """Create an advance nonce instruction.
        
        Returns:
            Advance nonce instruction
        """
        if not self.nonce_account:
            raise RuntimeError("Nonce account not loaded")
        
        return advance_nonce_account(
            AdvanceNonceAccountParams(
                nonce_pubkey=self.nonce_account.pubkey,
                authorized_pubkey=self.nonce_account.authority_pubkey
            )
        )
    
    async def advance_nonce_and_update(self) -> Hash:
        """Advance the nonce and update the cached value.
        
        This should be called after a transaction using the nonce is confirmed.
        
        Returns:
            New nonce value
        """
        if not self.nonce_account:
            raise RuntimeError("Nonce account not loaded")
        
        async with self._nonce_lock:
            try:
                # Fetch the new nonce value from blockchain
                new_nonce = await self._fetch_current_nonce(self.nonce_account.pubkey)
                self.nonce_account.current_nonce = new_nonce
                
                logger.debug(f"Advanced nonce to: {new_nonce}")
                return new_nonce
                
            except Exception as e:
                logger.error(f"Failed to advance nonce: {e}")
                raise
    
    def get_authority_keypair(self) -> Keypair:
        """Get the nonce authority keypair for signing.
        
        Returns:
            Authority keypair
        """
        if not self.nonce_account:
            raise RuntimeError("Nonce account not loaded")
        
        return self.nonce_account.authority_keypair
    
    def get_nonce_pubkey(self) -> Pubkey:
        """Get the nonce account public key.
        
        Returns:
            Nonce account public key
        """
        if not self.nonce_account:
            raise RuntimeError("Nonce account not loaded")
        
        return self.nonce_account.pubkey 
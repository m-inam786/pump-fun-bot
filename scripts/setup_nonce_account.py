#!/usr/bin/env python3
"""
One-time setup script to create a durable nonce account for the pump.fun bot.

This script creates a nonce account and saves the keypair information to a file
that can be loaded by the main trading bot for faster transaction execution.
"""

import asyncio
import base64
import json
import os
import sys
from pathlib import Path
from solders.pubkey import Pubkey

# Add the src directory to the Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

NONCE_ACCOUNT_SIZE = 80  # Size of a nonce account in bytes
NONCE_ACCOUNT_RENT_EXEMPTION_LAMPORTS = 1447680  # Minimum lamports for rent exemption
PROGRAM_ID = Pubkey.from_string("11111111111111111111111111111111")

from solders.keypair import Keypair
from solders.system_program import initialize_nonce_account, create_account
from core.client import SolanaClient
from utils.logger import get_logger

logger = get_logger(__name__)


class NonceAccountSetup:
    """Setup utility for creating durable nonce accounts."""
    
    def __init__(self, rpc_endpoint: str):
        """Initialize the setup utility.
        
        Args:
            rpc_endpoint: Solana RPC endpoint URL
        """
        self.rpc_endpoint = rpc_endpoint
        self.client = SolanaClient(rpc_endpoint)
    
    async def create_nonce_account(self, authority_keypair: Keypair, output_file: str = "data/nonce_account.json") -> None:
        """Create a new nonce account and save keypair information.
        
        Args:
            authority_keypair: The keypair that will be the nonce authority
            output_file: Path to save the nonce account information
        """
        try:
            # Generate a new keypair for the nonce account
            nonce_keypair = Keypair()
            
            logger.info(f"Creating nonce account {nonce_keypair.pubkey()} with authority {authority_keypair.pubkey()}")
            
            # Create account creation instruction
            create_account_ix = create_account(
                from_pubkey=authority_keypair.pubkey(),
                to_pubkey=nonce_keypair.pubkey(),
                lamports=NONCE_ACCOUNT_RENT_EXEMPTION_LAMPORTS,
                space=NONCE_ACCOUNT_SIZE,
                owner=PROGRAM_ID
            )
            
            # Create nonce initialization instruction
            init_nonce_ix = initialize_nonce_account(
                nonce_pubkey=nonce_keypair.pubkey(),
                authority_pubkey=authority_keypair.pubkey()
            )
            
            # Send the transaction to create and initialize the nonce account
            instructions = [create_account_ix, init_nonce_ix]
            
            tx_signature = await self.client.build_and_send_transaction(
                instructions=instructions,
                signer_keypair=authority_keypair,
                additional_signers=[nonce_keypair]
            )
            
            # Wait for confirmation
            confirmed = await self.client.confirm_transaction(tx_signature)
            if not confirmed:
                raise Exception("Nonce account creation transaction failed to confirm")
            
            logger.info(f"Successfully created nonce account. Transaction: {tx_signature}")
            
            # Save the keypair information to file
            await self._save_nonce_account_info(nonce_keypair, authority_keypair, output_file)
            
            # Verify the account was created correctly
            await self._verify_nonce_account(nonce_keypair)
            
            logger.info(f"Nonce account setup completed successfully!")
            logger.info(f"Account details saved to: {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to create nonce account: {e}")
            raise
        finally:
            await self.client.close()
    
    async def _save_nonce_account_info(self, nonce_keypair: Keypair, authority_keypair: Keypair, output_file: str) -> None:
        """Save nonce account information to a JSON file.
        
        Args:
            nonce_keypair: The nonce account keypair
            authority_keypair: The authority keypair
            output_file: Path to save the information
        """
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Prepare the account information
        account_info = {
            "nonce_account": {
                "pubkey": str(nonce_keypair.pubkey()),
                "secret_key": base64.b64encode(bytes(nonce_keypair)).decode('utf-8')
            },
            "authority": {
                "pubkey": str(authority_keypair.pubkey()),
                "secret_key": base64.b64encode(bytes(authority_keypair)).decode('utf-8')
            },
            "creation_info": {
                "rpc_endpoint": self.rpc_endpoint,
                "rent_exemption_lamports": NONCE_ACCOUNT_RENT_EXEMPTION_LAMPORTS,
                "account_size": NONCE_ACCOUNT_SIZE
            }
        }
        
        # Save to file
        with open(output_file, 'w') as f:
            json.dump(account_info, f, indent=2)
        
        logger.info(f"Nonce account information saved to {output_file}")
    
    async def _verify_nonce_account(self, nonce_keypair: Keypair) -> None:
        """Verify that the nonce account was created correctly.
        
        Args:
            nonce_keypair: The nonce account keypair to verify
        """
        try:
            account_info = await self.client.get_account_info(nonce_keypair.pubkey())
            
            if not account_info:
                raise Exception("Nonce account not found on blockchain")
            
            if account_info.lamports < NONCE_ACCOUNT_RENT_EXEMPTION_LAMPORTS:
                logger.warning(f"Nonce account has insufficient lamports for rent exemption: {account_info.lamports}")
            
            logger.info(f"Nonce account verification successful:")
            logger.info(f"  Address: {nonce_keypair.pubkey()}")
            logger.info(f"  Lamports: {account_info.lamports}")
            logger.info(f"  Owner: {account_info.owner}")
            
        except Exception as e:
            logger.error(f"Nonce account verification failed: {e}")
            raise


def load_keypair_from_file(keypair_file: str) -> Keypair:
    """Load a keypair from a JSON file.
    
    Args:
        keypair_file: Path to the keypair file
        
    Returns:
        Loaded Keypair object
    """
    if not os.path.exists(keypair_file):
        raise FileNotFoundError(f"Keypair file not found: {keypair_file}")
    
    with open(keypair_file, 'r') as f:
        data = json.load(f)
    
    # Handle both array format and base64 format
    if isinstance(data, list):
        # Array format (e.g., [1, 2, 3, ...])
        keypair_bytes = bytes(data)
    elif isinstance(data, dict) and 'secret_key' in data:
        # Our format with base64 encoding
        keypair_bytes = base64.b64decode(data['secret_key'])
    elif isinstance(data, str):
        # Base64 string format
        keypair_bytes = base64.b64decode(data)
    else:
        raise ValueError("Unsupported keypair file format")
    
    return Keypair.from_bytes(keypair_bytes)


async def main():
    """Main setup function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Setup durable nonce account for pump.fun bot")
    parser.add_argument("--rpc-endpoint", required=True, help="Solana RPC endpoint URL")
    parser.add_argument("--wallet-file", required=True, help="Path to wallet keypair file")
    parser.add_argument("--output-file", default="data/nonce_account.json", help="Output file for nonce account info")
    parser.add_argument("--verify-only", action="store_true", help="Only verify existing nonce account")
    
    args = parser.parse_args()
    
    try:
        # Load the wallet keypair (authority)
        logger.info(f"Loading wallet keypair from {args.wallet_file}")
        authority_keypair = load_keypair_from_file(args.wallet_file)
        logger.info(f"Loaded wallet: {authority_keypair.pubkey()}")
        
        # Initialize setup utility
        setup = NonceAccountSetup(args.rpc_endpoint)
        
        if args.verify_only:
            # Just verify existing nonce account
            if not os.path.exists(args.output_file):
                logger.error(f"Nonce account file not found: {args.output_file}")
                return
            
            with open(args.output_file, 'r') as f:
                nonce_info = json.load(f)
            
            nonce_keypair = Keypair.from_bytes(base64.b64decode(nonce_info['nonce_account']['secret_key']))
            await setup._verify_nonce_account(nonce_keypair)
            await setup.client.close()
        else:
            # Create new nonce account
            logger.info("Creating new nonce account...")
            await setup.create_nonce_account(authority_keypair, args.output_file)
            
        logger.info("Setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 
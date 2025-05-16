"""
Serializer utilities for parsing pump.fun program data.
"""

import base64
from typing import Dict, Any, Optional
from borsh_construct import U8, U64, Bool, I64, CStruct
from base58 import b58encode
from utils.logger import get_logger

logger = get_logger(__name__)


class PumpFunSerializer:
    """Utilities for parsing pump.fun program data."""
    
    def __init__(self):
        self.pumpStructs = self.pumpStructs()

    class pumpStructs:
        def __init__(self):
            self.TransactionData = CStruct(
                "discriminator" / U8[8],
                "mint" / U8[32],
                "sol_amount" / U64,
                "token_amount" / U64,
                "is_buy" / Bool,
                "user" / U8[32],
                "timestamp" / I64,
                "virtual_sol_reserves" / U64,
                "virtual_token_reserves" / U64,
            )

    # Transaction data layout constants
    DISCRIMINATOR_SIZE = 8
    PUBKEY_SIZE = 32
    U64_SIZE = 8
    BOOL_SIZE = 1

    def parse_transaction_data(self, b64_data: str) -> Dict[str, Any]:
        """Parse transaction data structure.
        
        Args:
            b64_data: Base64 encoded transaction data
            
        Returns:
            Dictionary with parsed transaction data
        """

        # Decode base64 data
        try:
            data = base64.b64decode(b64_data)
            parsed_data = self.pumpStructs.TransactionData.parse(data)
            return {
                "mint": b58encode(bytes(parsed_data.mint)).decode("utf-8"),
                "sol_amount": parsed_data.sol_amount,
                "token_amount": parsed_data.token_amount,
                "is_buy": parsed_data.is_buy,
                "user": b58encode(bytes(parsed_data.user)).decode("utf-8"),
                "timestamp": parsed_data.timestamp,
                "virtual_sol_reserves": str(parsed_data.virtual_sol_reserves),
                "virtual_token_reserves": str(parsed_data.virtual_token_reserves)
            }
        except Exception as e:
            logger.error(f"Error parsing transaction data: {e}")
            return {}
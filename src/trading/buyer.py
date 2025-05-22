"""
Buy operations for pump.fun tokens.
"""

import struct
from typing import Final

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey
from spl.token.instructions import create_idempotent_associated_token_account

from core.client import SolanaClient
from core.curve import BondingCurveManager
from core.priority_fee.manager import PriorityFeeManager
from core.pubkeys import (
    LAMPORTS_PER_SOL,
    TOKEN_DECIMALS,
    PumpAddresses,
    SystemAddresses,
)
from core.wallet import Wallet
from trading.base import TokenInfo, Trader, TradeResult
from utils.logger import get_logger
from utils.serializer import PumpFunSerializer
from decimal import Decimal

logger = get_logger(__name__)

# Discriminator for the buy instruction
EXPECTED_DISCRIMINATOR: Final[bytes] = struct.pack("<Q", 16927863322537952870)


class TokenBuyer(Trader):
    """Handles buying tokens on pump.fun."""

    def __init__(
        self,
        client: SolanaClient,
        wallet: Wallet,
        curve_manager: BondingCurveManager,
        max_retries: int = 5,
        extreme_fast_mode: bool = False,
    ):
        """Initialize token buyer.

        Args:
            client: Solana client for RPC calls
            wallet: Wallet for signing transactions
            curve_manager: Bonding curve manager
            max_retries: Maximum number of retry attempts
            extreme_fast_mode: If enabled, avoid fetching associated bonding curve state
        """
        self.client = client
        self.wallet = wallet
        self.curve_manager = curve_manager
        self.max_retries = max_retries
        self.extreme_fast_mode = extreme_fast_mode
        self.serializer = PumpFunSerializer()

    async def execute(
        self, 
        token_info: TokenInfo, 
        token_amount: int | None = None, 
        base_sol_amount: float | None = None, 
        slippage_perc: float | None = None, 
        priority_fee_microlamports: int | None = None, 
        tip_amount_lamports: int | None = None, 
        *args, 
        **kwargs
    ) -> TradeResult:
        """Execute buy operation with configurable parameters.

        Args:
            token_info: Token information
            token_amount: Amount of tokens to buy (for extreme fast mode)
            base_sol_amount: Amount of SOL to spend on the buy
            slippage_perc: Slippage tolerance percentage (0.1 = 10%)
            priority_fee_microlamports: Priority fee in microlamports
            tip_amount_lamports: Zero slot tip amount in lamports

        Returns:
            TradeResult with buy outcome
        """
        try:
            # If parameters are not provided, log an error and return failure
            if base_sol_amount is None:
                logger.error("Buy operation failed: No SOL amount specified")
                return TradeResult(success=False, error_message="No SOL amount specified for buy")

            if slippage_perc is None:
                logger.error("Buy operation failed: No slippage percentage specified")
                return TradeResult(success=False, error_message="No slippage percentage specified for buy")
                
            if priority_fee_microlamports is None:
                logger.error("Buy operation failed: No priority fee specified")
                return TradeResult(success=False, error_message="No priority fee specified for buy")
            
            # Convert amount to lamports
            amount_lamports = int(base_sol_amount * LAMPORTS_PER_SOL)

            if self.extreme_fast_mode:
                # Skip the wait and directly calculate the amount
                if token_amount is None:
                    logger.error("Buy operation failed: No token amount specified for extreme fast mode")
                    return TradeResult(success=False, error_message="No token amount specified for extreme fast mode")
                    
                token_price_sol = base_sol_amount / token_amount
                logger.info(f"EXTREME FAST Mode: Buying {token_amount} tokens.")
            else:
                # Regular behavior with RPC call
                curve_state = await self.curve_manager.get_curve_state(token_info.bonding_curve)
                token_price_sol = curve_state.calculate_price()
                token_amount = int(base_sol_amount / token_price_sol)

            # Calculate maximum SOL to spend with slippage
            max_amount_lamports = int(amount_lamports * (1 + slippage_perc))

            associated_token_account = self.wallet.get_associated_token_address(
                token_info.mint
            )

            logger.info(
                f"Buying {token_amount} tokens at {token_price_sol} SOL per token"
            )
            logger.info(
                f"Total cost: {base_sol_amount} SOL (max: {max_amount_lamports / LAMPORTS_PER_SOL} SOL)"
            )
            logger.info(f"Slippage: {slippage_perc * 100}%, Priority fee: {priority_fee_microlamports} microlamports ({priority_fee_microlamports / LAMPORTS_PER_SOL} SOL)")
            if tip_amount_lamports:
                logger.info(f"Zero slot tip: {tip_amount_lamports} lamports ({tip_amount_lamports / LAMPORTS_PER_SOL} SOL)")

            tx_signature = await self._send_buy_transaction(
                token_info,
                associated_token_account,
                token_amount,
                max_amount_lamports,
                priority_fee_microlamports,
                tip_amount_lamports=tip_amount_lamports
            )

            logger.info(f"Buy transaction sent: {tx_signature}")

            logger.info(f"Confirming buy transaction {tx_signature}")
            if await self.client.confirm_transaction(tx_signature):
                logger.info(f"Transaction confirmed, getting details for {tx_signature}")
                tx_details = await self.client.get_transaction_details(tx_signature)
                logger.info(f"Transaction details received for {tx_signature}")
            else:
                return TradeResult(
                    success=False,
                    error_message=f"Transaction failed to confirm: {tx_signature}",
                )

            if tx_details and tx_details.transaction.meta:
                # Double check that the transaction was successful
                if tx_details.transaction.meta.err:
                    error_info = tx_details.transaction.meta.err
                    
                    # Log the raw error information
                    logger.error(f"Buy operation failed: Transaction {tx_signature} failed with error: {error_info}")
                    
                    # For more detailed debugging, log the full transaction info
                    try:
                        # Extract more error details from logs if available
                        if hasattr(tx_details.transaction.meta, 'log_messages') and tx_details.transaction.meta.log_messages:
                            logs = tx_details.transaction.meta.log_messages
                            error_logs = [log for log in logs if "Error" in log or "error" in log or "failed" in log or "Failed" in log]
                            if error_logs:
                                logger.error(f"Error details from logs: {error_logs}")
                            else:
                                logger.error(f"All logs: {logs}")
                    
                    except Exception as log_error:
                        logger.error(f"Error extracting detailed error information: {log_error}")
                    
                    return TradeResult(
                        success=False,
                        error_message=f"Transaction failed: {error_info}",
                    )
                
                # parse tx_details to get the amount of tokens bought
                for log_entry in tx_details.transaction.meta.log_messages:
                    if "Program data:" in log_entry:
                        try:
                            idx = log_entry.find("Program data: ")
                            raw_data = log_entry[idx + len("Program data: "):]
                            
                            # Ensure raw_data is a string and matches expected prefix
                            if isinstance(raw_data, str) and raw_data.startswith("vdt"):
                                parsed_data = self.serializer.parse_transaction_data(raw_data)
                            else:
                                # logger.debug(f"Skipping non-vdt program data: {raw_data[:50]}...")
                                continue 
                            
                            if "virtual_sol_reserves" in parsed_data and "virtual_token_reserves" in parsed_data:
                                try:
                                    # Ensure these are strings before creating Decimal
                                    if not isinstance(parsed_data["virtual_sol_reserves"], str) or not isinstance(parsed_data["virtual_token_reserves"], str):
                                        logger.warning(f"Reserve values are not strings for mint {self.mint}. VSR: {type(parsed_data['virtual_sol_reserves'])}, VTR: {type(parsed_data['virtual_token_reserves'])}")
                                        continue

                                    vsr_str = parsed_data["virtual_sol_reserves"]
                                    vtr_str = parsed_data["virtual_token_reserves"]
                                    
                                    vsr = Decimal(vsr_str) / Decimal('1e9')  # SOL has 9 decimals
                                    vtr = Decimal(vtr_str) / Decimal('1e6')  # Tokens have 6 decimals
                                    
                                    token_price_sol = vsr / vtr
                                    logger.info(f"Token price from tx details: {token_price_sol:.8f} SOL")

                                except ValueError as ve:
                                    logger.error(f"ValueError converting reserves to Decimal for mint {self.mint}: {ve}. Data: {parsed_data}")
                        except Exception as e:
                            logger.error(f"Error calculating price from parsed data for mint {self.mint}: {e}. Data: {parsed_data}")
                
                logger.info(f"Buy transaction successful: {tx_signature}")
                return TradeResult(
                    success=True,
                    tx_signature=tx_signature,
                    amount=token_amount,
                    price=token_price_sol,
                )
            else:
                logger.error(f"Buy operation failed: Transaction details not received for {tx_signature}")
                return TradeResult(
                    success=False,
                    error_message=f"Transaction failed to confirm: {tx_signature}",
                )

        except Exception as e:
            logger.error(f"Buy operation failed: {e!s}")
            return TradeResult(success=False, error_message=str(e))

    async def _send_buy_transaction(
        self,
        token_info: TokenInfo,
        associated_token_account: Pubkey,
        token_amount: float,
        max_amount_lamports: int,
        priority_fee_microlamports: int,
        tip_amount_lamports: int | None = None
    ) -> str:
        """Send buy transaction.

        Args:
            token_info: Token information
            associated_token_account: User's token account
            token_amount: Amount of tokens to buy
            max_amount_lamports: Maximum SOL to spend in lamports

        Returns:
            Transaction signature

        Raises:
            Exception: If transaction fails after all retries
        """
        logger.info(f"_send_buy_transaction: Starting buy transaction function for {token_info.symbol}...")
        
        accounts = [
            AccountMeta(
                pubkey=PumpAddresses.GLOBAL, is_signer=False, is_writable=False
            ),
            AccountMeta(pubkey=PumpAddresses.FEE, is_signer=False, is_writable=True),
            AccountMeta(pubkey=token_info.mint, is_signer=False, is_writable=False),
            AccountMeta(
                pubkey=token_info.bonding_curve, is_signer=False, is_writable=True
            ),
            AccountMeta(
                pubkey=token_info.associated_bonding_curve,
                is_signer=False,
                is_writable=True,
            ),
            AccountMeta(
                pubkey=associated_token_account, is_signer=False, is_writable=True
            ),
            AccountMeta(pubkey=self.wallet.pubkey, is_signer=True, is_writable=True),
            AccountMeta(
                pubkey=SystemAddresses.PROGRAM, is_signer=False, is_writable=False
            ),
            AccountMeta(
                pubkey=SystemAddresses.TOKEN_PROGRAM, is_signer=False, is_writable=False
            ),
            AccountMeta(
                pubkey=token_info.creator_vault, is_signer=False, is_writable=True
            ),
            AccountMeta(
                pubkey=PumpAddresses.EVENT_AUTHORITY, is_signer=False, is_writable=False
            ),
            AccountMeta(
                pubkey=PumpAddresses.PROGRAM, is_signer=False, is_writable=False
            ),
        ]

        logger.info(f"_send_buy_transaction: Preparing idempotent create ATA instruction...")
        
        # Prepare idempotent create ATA instruction: it will not fail if ATA already exists
        idempotent_ata_ix = create_idempotent_associated_token_account(
            self.wallet.pubkey,
            self.wallet.pubkey,
            token_info.mint,
            SystemAddresses.TOKEN_PROGRAM
        )

        logger.info(f"_send_buy_transaction: Idempotent create ATA instruction prepared.")

        # Prepare buy instruction data
        token_amount_raw = int(token_amount * 10**TOKEN_DECIMALS)
        data = (
            EXPECTED_DISCRIMINATOR
            + struct.pack("<Q", token_amount_raw)
            + struct.pack("<Q", max_amount_lamports)
        )

        logger.info(f"_send_buy_transaction: Buy instruction data prepared.")

        buy_ix = Instruction(PumpAddresses.PROGRAM, data, accounts)

        logger.info(f"_send_buy_transaction: Sending buy instruction...")
        try:
            return await self.client.build_and_send_transaction(
                [idempotent_ata_ix, buy_ix],
                self.wallet.keypair,
                skip_preflight=True,
                max_retries=self.max_retries,
                priority_fee=priority_fee_microlamports,
                tip_amount_lamports=tip_amount_lamports
            )
        except Exception as e:
            logger.error(f"Buy transaction failed: {e!s}")
            raise

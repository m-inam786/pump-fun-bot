"""
Buy operations for pump.fun tokens.
"""

import struct
from typing import Final
from solders.pubkey import Pubkey
from core.client import SolanaClient
from core.curve import BondingCurveManager
from core.wallet import Wallet
from trading.base import TokenInfo, Trader, TradeResult
from templates.buy_tx import BuyTxBuilder
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
    ):
        """Initialize token buyer.

        Args:
            client: Solana client for RPC calls
            wallet: Wallet for signing transactions
            curve_manager: Bonding curve manager
            max_retries: Maximum number of retry attempts
        """
        self.client = client
        self.wallet = wallet
        self.curve_manager = curve_manager
        self.max_retries = max_retries
        self.serializer = PumpFunSerializer()

    async def execute(
        self, 
        token_info: TokenInfo, 
        use_prestored_template: bool = False,
        prestored_instructions: list | None = None,
        token_amount: int = 0,
        *args, 
        **kwargs
    ) -> TradeResult:
        """Execute buy operation using ultra-fast template-based approach.

        Args:
            token_info: Token information
            use_prestored_template: Whether to use pre-stored template for fastest execution (developer manager)
            prestored_instructions: Pre-stored instruction template (required if use_prestored_template=True)

        Returns:
            TradeResult with buy outcome
        """
        try:
            if use_prestored_template:
                if prestored_instructions is None:
                    logger.error("Buy operation failed: No prestored instructions provided")
                    return TradeResult(success=False, error_message="No prestored instructions provided")
                
                # FASTEST execution using pre-stored template (FOR DEVELOPER MANAGER)
                instructions = BuyTxBuilder.fill_prestored_tx_ultra_fast(
                    prestored_instructions=prestored_instructions,
                    mint=token_info.mint,
                    bonding_curve=token_info.bonding_curve,
                    associated_bonding_curve=token_info.associated_bonding_curve,
                    creator_vault=token_info.creator_vault
                )
                logger.info(f"Prestored template buy for developer manager")
            else:
                # Non developer manager mode - use static template with bot config values
                instructions = BuyTxBuilder.get_buy_tx_fast(
                    mint=token_info.mint,
                    bonding_curve=token_info.bonding_curve,
                    associated_bonding_curve=token_info.associated_bonding_curve,
                    creator_vault=token_info.creator_vault
                )
                logger.info(f"Static template buy for non developer manager mode")

            # Send transaction using template-based instructions
            tx_signature = await self.client.build_and_send_transaction(
                instructions,
                self.wallet.keypair,
                skip_preflight=True,
                max_retries=self.max_retries,
                tx_type="buy"
            )

            logger.info(f"Template-based buy transaction sent: {tx_signature}")

            logger.info(f"Confirming buy transaction {tx_signature}")
            if await self.client.robust_confirm_transaction(tx_signature):
                logger.info(f"Transaction confirmed, getting details for {tx_signature}")
                tx_details = await self.client.get_transaction_details(tx_signature)
                logger.info(f"Transaction details received for {tx_signature}")
            else:
                logger.error(f"Transaction failed to confirm: {tx_signature}")
                return TradeResult(
                    success=False,
                    error_message=f"Transaction failed to confirm: {tx_signature}",
                )

            if tx_details and tx_details.transaction.meta:
                # Check transaction success
                if tx_details.transaction.meta.err:
                    error_info = tx_details.transaction.meta.err
                    logger.error(f"Buy operation failed: Transaction {tx_signature} failed with error: {error_info}")
                    
                    # Extract error details from logs if available
                    try:
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
                
                # Parse transaction details for price information
                token_price_sol = 0.0
                
                for log_entry in tx_details.transaction.meta.log_messages:
                    if "Program data:" in log_entry:
                        try:
                            idx = log_entry.find("Program data: ")
                            raw_data = log_entry[idx + len("Program data: "):]
                            
                            if isinstance(raw_data, str) and raw_data.startswith("vdt"):
                                parsed_data = self.serializer.parse_transaction_data(raw_data)
                            else:
                                continue 
                            
                            if "virtual_sol_reserves" in parsed_data and "virtual_token_reserves" in parsed_data:
                                try:
                                    if not isinstance(parsed_data["virtual_sol_reserves"], str) or not isinstance(parsed_data["virtual_token_reserves"], str):
                                        logger.warning(f"Reserve values are not strings. VSR: {type(parsed_data['virtual_sol_reserves'])}, VTR: {type(parsed_data['virtual_token_reserves'])}")
                                        continue

                                    vsr_str = parsed_data["virtual_sol_reserves"]
                                    vtr_str = parsed_data["virtual_token_reserves"]
                                    
                                    vsr = Decimal(vsr_str) / Decimal('1e9')  # SOL has 9 decimals
                                    vtr = Decimal(vtr_str) / Decimal('1e6')  # Tokens have 6 decimals
                                    
                                    token_price_sol = vsr / vtr
                                    logger.info(f"Token price from tx details: {token_price_sol} SOL")

                                except ValueError as ve:
                                    logger.error(f"ValueError converting reserves to Decimal: {ve}. Data: {parsed_data}")
                        except Exception as e:
                            logger.error(f"Error calculating price from parsed data: {e}. Data: {parsed_data}")
                
                logger.info(f"Template-based buy transaction successful: {tx_signature} with price {token_price_sol} SOL")
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
            logger.error(f"Template-based buy operation failed: {e!s}")
            return TradeResult(success=False, error_message=str(e))
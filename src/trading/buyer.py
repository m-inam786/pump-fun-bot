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
import asyncio

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
            tx_result = await self.client.build_and_send_transaction(
                instructions,
                self.wallet.keypair,
                skip_preflight=True,
                max_retries=self.max_retries,
                tx_type="buy"
            )

            # Handle both single signature and multiple signatures
            if isinstance(tx_result, list):
                tx_signatures = tx_result
                logger.info(f"Template-based buy transactions sent: {len(tx_signatures)} signatures: {tx_signatures}")
            else:
                tx_signatures = [tx_result]
                logger.info(f"Template-based buy transaction sent: {tx_result}")

            # Confirm transactions concurrently and use first confirmed one
            logger.info(f"Confirming {len(tx_signatures)} buy transaction(s) concurrently")
            
            # Create confirmation tasks
            tasks = {
                asyncio.create_task(self._confirm_and_get_details(tx_signature)): tx_signature
                for tx_signature in tx_signatures
            }
            
            confirmed_signature = None
            primary_tx_details = None
            
            try:
                # Wait for first successful confirmation
                while tasks and confirmed_signature is None:
                    done, pending = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
                    
                    for task in done:
                        tx_signature = tasks[task]
                        try:
                            signature, details = await task
                            # First successful confirmation - use it and cancel others
                            confirmed_signature = signature
                            primary_tx_details = details
                            if pending:
                                logger.info(f"First transaction confirmed: {signature} - cancelling {len(pending)} remaining tasks")
                                # Cancel all pending tasks
                                for pending_task in pending:
                                    pending_task.cancel()
                            else:
                                logger.info(f"Transaction confirmed: {signature}")
                            break
                            
                        except Exception as e:
                            logger.warning(f"Transaction {tx_signature} confirmation failed: {str(e)}")
                            # Remove failed task and continue
                            del tasks[task]
            
            except Exception as e:
                # Cancel all tasks on error
                for task in tasks.keys():
                    if not task.done():
                        task.cancel()
                logger.error(f"Error during transaction confirmation: {e}")
                return TradeResult(
                    success=False,
                    error_message=f"Error during transaction confirmation: {e}",
                )
            
            # Check if any transactions were confirmed
            if confirmed_signature is None:
                logger.error(f"All transactions failed to confirm: {tx_signatures}")
                return TradeResult(
                    success=False,
                    error_message=f"All transactions failed to confirm: {tx_signatures}",
                )

            # Process the confirmed transaction for result details
            if primary_tx_details and primary_tx_details.transaction.meta:
                # Check transaction success
                if primary_tx_details.transaction.meta.err:
                    error_info = primary_tx_details.transaction.meta.err
                    logger.error(f"Buy operation failed: Transaction {confirmed_signature} failed with error: {error_info}")
                    
                    # Extract error details from logs if available
                    try:
                        if hasattr(primary_tx_details.transaction.meta, 'log_messages') and primary_tx_details.transaction.meta.log_messages:
                            logs = primary_tx_details.transaction.meta.log_messages
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
                
                for log_entry in primary_tx_details.transaction.meta.log_messages:
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
                
                logger.info(f"Template-based buy transactions successful: {confirmed_signature} confirmed out of {len(tx_signatures)} sent")
                logger.info(f"Primary transaction: {confirmed_signature} with price {token_price_sol} SOL")
                
                return TradeResult(
                    success=True,
                    tx_signature=confirmed_signature,  # Primary signature for backward compatibility
                    amount=token_amount,
                    price=token_price_sol,
                )
            else:
                logger.error(f"Buy operation failed: Transaction details not received for {confirmed_signature}")
                return TradeResult(
                    success=False,
                    error_message=f"Transaction failed to confirm: {confirmed_signature}",
                )

        except Exception as e:
            logger.error(f"Template-based buy operation failed: {e!s}")
            return TradeResult(success=False, error_message=str(e))

    async def _confirm_and_get_details(self, tx_signature: str):
        """Confirm transaction and get details, raising exception on failure."""
        logger.info(f"Confirming buy transaction {tx_signature}")
        if await self.client.confirm_transaction(tx_signature):
            logger.info(f"Transaction confirmed, getting details for {tx_signature}")
            tx_details = await self.client.get_transaction_details(tx_signature)
            if tx_details:
                return tx_signature, tx_details
            else:
                logger.warning(f"Transaction confirmed but details not received for {tx_signature}")
                raise Exception(f"Transaction details not received for {tx_signature}")
        else:
            logger.error(f"Transaction failed to confirm: {tx_signature}")
            raise Exception(f"Transaction failed to confirm: {tx_signature}")
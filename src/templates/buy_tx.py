from solders.instruction import Instruction, AccountMeta
from core.pubkeys import PumpAddresses
from solders.pubkey import Pubkey
import struct
from typing import Final, List
from spl.token.instructions import create_associated_token_account
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from core.pubkeys import (
    LAMPORTS_PER_SOL,
    TOKEN_DECIMALS,
    PumpAddresses,
    SystemAddresses,
)
from solders.system_program import transfer, TransferParams
from core.wallet import Wallet

# Jito sandwich protection public key
JITO_DONT_FRONT_PUBKEY = Pubkey.from_string("jitodontfront11111111111111111111111Wrecker")

EXPECTED_DISCRIMINATOR: Final[bytes] = struct.pack("<Q", 16927863322537952870)

WALLET_INSTANCE = None

SET_COMPUTE_UNIT_LIMIT_INSTRUCTION = set_compute_unit_limit(72_000)

BUY_TX = [

# 1. Set compute unit limit
Instruction(
    program_id=SET_COMPUTE_UNIT_LIMIT_INSTRUCTION.program_id,
    data=SET_COMPUTE_UNIT_LIMIT_INSTRUCTION.data,
    accounts=[AccountMeta(pubkey=JITO_DONT_FRONT_PUBKEY, is_signer=False, is_writable=False)]
    ),

# 2. Set compute unit price (priority fee)
set_compute_unit_price(10000),

# 3. Create ATA instruction (will be replaced with proper create_associated_token_account)
create_associated_token_account(
    Pubkey.new_unique(),  # payer (will be replaced with wallet pubkey)
    Pubkey.new_unique(),  # owner (will be replaced with wallet pubkey)  
    Pubkey.new_unique(),  # mint (will be replaced with actual mint)
    SystemAddresses.TOKEN_PROGRAM
),

# 4. Buy instruction
Instruction(
    program_id=PumpAddresses.PROGRAM,
    data=b'',
    accounts = [
            AccountMeta(
                pubkey=PumpAddresses.GLOBAL, is_signer=False, is_writable=False
            ),
            AccountMeta(pubkey=PumpAddresses.FEE, is_signer=False, is_writable=True),
            # mint
            AccountMeta(pubkey=Pubkey.new_unique(), is_signer=False, is_writable=False),
            # bonding curve
            AccountMeta(
                pubkey=Pubkey.new_unique(), is_signer=False, is_writable=True
            ),
            # associated bonding curve
            AccountMeta(
                pubkey=Pubkey.new_unique(),
                is_signer=False,
                is_writable=True,
            ),
            # user's associated token account
            AccountMeta(
                pubkey=Pubkey.new_unique(), is_signer=False, is_writable=True
            ),
            # wallet pubkey (signer)
            AccountMeta(pubkey=Pubkey.new_unique(), is_signer=True, is_writable=True),
            # system program
            AccountMeta(
                pubkey=SystemAddresses.PROGRAM, is_signer=False, is_writable=False
            ),
            # token program
            AccountMeta(
                pubkey=SystemAddresses.TOKEN_PROGRAM, is_signer=False, is_writable=False
            ),
            # creator vault
            AccountMeta(
                pubkey=Pubkey.new_unique(), is_signer=False, is_writable=True
            ),
            # event authority
            AccountMeta(
                pubkey=PumpAddresses.EVENT_AUTHORITY, is_signer=False, is_writable=False
            ),
            # program
            AccountMeta(
                pubkey=PumpAddresses.PROGRAM, is_signer=False, is_writable=False
            ),
        ]
)]

class BuyTxBuilder:
    
    @staticmethod
    def update_buy_tx_with_bot_config(
        buy_amount: float,
        buy_slippage: float,
        priority_fee_microlamports: int,
        wallet_instance: Wallet,
        token_amount: int = 1000000,
    ):
        """Set bot configuration values for static templates."""
        global BUY_TX, WALLET_INSTANCE
        WALLET_INSTANCE = wallet_instance
        
        # Update compute unit price (priority fee)
        BUY_TX[1] = set_compute_unit_price(priority_fee_microlamports)
        
        # Prepare buy instruction data
        token_amount_raw = int(token_amount * 10**TOKEN_DECIMALS)
        max_amount_lamports = int(buy_amount * LAMPORTS_PER_SOL * (1 + buy_slippage))
        buy_data = (
            EXPECTED_DISCRIMINATOR
            + struct.pack("<Q", token_amount_raw)
            + struct.pack("<Q", max_amount_lamports)
        )
        
        # Update buy instruction with proper data
        BUY_TX[3] = Instruction(
            program_id=BUY_TX[3].program_id,
            data=buy_data,
            accounts=BUY_TX[3].accounts
        )
    
    @staticmethod
    def get_buy_tx_fast(
        mint: Pubkey, 
        bonding_curve: Pubkey,
        associated_bonding_curve: Pubkey,
        creator_vault: Pubkey
    ) -> List[Instruction]:
        """
        Ultra-fast buy transaction builder using static bot config values.
        Replicates the exact structure from working buyer.py code.
        """
        # Calculate user's associated token account
        user_ata = WALLET_INSTANCE.get_associated_token_address(mint)
        
        # Build instructions exactly like the working buyer.py code
        instructions = [
            # Compute budget instructions
            BUY_TX[0],  # compute_limit_ix
            BUY_TX[1],  # compute_price_ix
            
            # ATA instruction - create associated token account
            create_associated_token_account(
                WALLET_INSTANCE.pubkey,  # payer
                WALLET_INSTANCE.pubkey,  # owner
                mint,                    # mint
                SystemAddresses.TOKEN_PROGRAM
            ),
            
            # Buy instruction with updated accounts
            Instruction(
                program_id=PumpAddresses.PROGRAM,
                data=BUY_TX[3].data,  # Use pre-configured buy data
                accounts=[
                    AccountMeta(pubkey=PumpAddresses.GLOBAL, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PumpAddresses.FEE, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=mint, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=associated_bonding_curve, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=user_ata, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=WALLET_INSTANCE.pubkey, is_signer=True, is_writable=True),
                    AccountMeta(pubkey=SystemAddresses.PROGRAM, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=SystemAddresses.TOKEN_PROGRAM, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=creator_vault, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=PumpAddresses.EVENT_AUTHORITY, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PumpAddresses.PROGRAM, is_signer=False, is_writable=False),
                ]
            )
        ]
        
        return instructions

    @staticmethod
    def get_prestored_tx_template(
        token_amount: float,
        max_amount_lamports: int,
        priority_fee_microlamports: int,
        tip_amount_lamports: int | None = None,
        tip_destination: Pubkey | None = None
    ) -> List[Instruction]:
        """
        Creates a pre-stored transaction template for developer manager.
        Missing: mint, bonding_curve, associated_bonding_curve, creator_vault
        These will be filled on-the-fly when snipe is triggered.
        
        Note: The ATA instruction (index 2) will need to be completely replaced
        since create_associated_token_account creates a complete instruction.
        """
        # Prepare buy instruction data
        token_amount_raw = int(token_amount * 10**TOKEN_DECIMALS)
        buy_data = (
            EXPECTED_DISCRIMINATOR
            + struct.pack("<Q", token_amount_raw)
            + struct.pack("<Q", max_amount_lamports)
        )
        
        # Create template with placeholders for fastest snipe execution
        instructions = [
            # 0. Compute unit limit
            BUY_TX[0],
            
            # 1. Set priority fee
            set_compute_unit_price(priority_fee_microlamports),
            
            # 2. Placeholder ATA instruction (will be completely replaced)
            create_associated_token_account(
                Pubkey.new_unique(),  # payer (placeholder)
                Pubkey.new_unique(),  # owner (placeholder)
                Pubkey.new_unique(),  # mint (placeholder)
                SystemAddresses.TOKEN_PROGRAM
            ),
            
            # 3. Buy instruction with placeholders
            Instruction(
                program_id=PumpAddresses.PROGRAM,
                data=buy_data,  # Pre-calculated data
                accounts=[
                    AccountMeta(pubkey=PumpAddresses.GLOBAL, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PumpAddresses.FEE, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=Pubkey.new_unique(), is_signer=False, is_writable=False),  # PLACEHOLDER: mint
                    AccountMeta(pubkey=Pubkey.new_unique(), is_signer=False, is_writable=True),   # PLACEHOLDER: bonding_curve
                    AccountMeta(pubkey=Pubkey.new_unique(), is_signer=False, is_writable=True),   # PLACEHOLDER: associated_bonding_curve
                    AccountMeta(pubkey=Pubkey.new_unique(), is_signer=False, is_writable=True),   # PLACEHOLDER: user_ata
                    AccountMeta(pubkey=WALLET_INSTANCE.pubkey, is_signer=True, is_writable=True), # WALLET_PUBKEY
                    AccountMeta(pubkey=SystemAddresses.PROGRAM, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=SystemAddresses.TOKEN_PROGRAM, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=Pubkey.new_unique(), is_signer=False, is_writable=True),   # PLACEHOLDER: creator_vault
                    AccountMeta(pubkey=PumpAddresses.EVENT_AUTHORITY, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PumpAddresses.PROGRAM, is_signer=False, is_writable=False),
                ]
            )
        ]
        
        # Add tip instruction if provided
        if tip_amount_lamports and tip_destination:
            instructions.append(
                transfer(
                    TransferParams(
                        from_pubkey=WALLET_INSTANCE.pubkey,
                        to_pubkey=tip_destination,
                        lamports=tip_amount_lamports,
                    )
                )
            )
        
        return instructions

    @staticmethod
    def fill_prestored_tx_ultra_fast(
        prestored_instructions: List[Instruction],
        mint: Pubkey,
        bonding_curve: Pubkey,
        associated_bonding_curve: Pubkey,
        creator_vault: Pubkey
    ) -> List[Instruction]:
        """
        Ultra-fast fill of pre-stored transaction template.
        Replaces the ATA instruction and updates buy instruction accounts.
        This is the FASTEST possible execution for sniping!
        """
        # Calculate user ATA once
        user_ata = WALLET_INSTANCE.get_associated_token_address(mint)
        
        # Replace the entire ATA instruction (index 2)
        prestored_instructions[2] = create_associated_token_account(
            WALLET_INSTANCE.pubkey,  # payer
            WALLET_INSTANCE.pubkey,  # owner
            mint,                    # mint
            SystemAddresses.TOKEN_PROGRAM
        )
        
        # Update buy instruction accounts (index 3)
        prestored_instructions[3] = Instruction(
                program_id=prestored_instructions[3].program_id,
                data=prestored_instructions[3].data,  # Pre-calculated data
                accounts=[
                    AccountMeta(pubkey=PumpAddresses.GLOBAL, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PumpAddresses.FEE, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=mint, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=associated_bonding_curve, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=user_ata, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=WALLET_INSTANCE.pubkey, is_signer=True, is_writable=True),
                    AccountMeta(pubkey=SystemAddresses.PROGRAM, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=SystemAddresses.TOKEN_PROGRAM, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=creator_vault, is_signer=False, is_writable=True),
                    AccountMeta(pubkey=PumpAddresses.EVENT_AUTHORITY, is_signer=False, is_writable=False),
                    AccountMeta(pubkey=PumpAddresses.PROGRAM, is_signer=False, is_writable=False),
                ]
            )
        
        return prestored_instructions
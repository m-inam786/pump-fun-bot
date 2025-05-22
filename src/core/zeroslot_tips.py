import asyncio
import aiohttp
from solders.pubkey import Pubkey
from solders.system_program import transfer, TransferParams
from solders.instruction import Instruction
from utils.logger import get_logger
from solana.rpc.async_api import AsyncClient

logger = get_logger(__name__)

ZERO_SLOT_TIP_INTERVAL_SECONDS = 60

class ZeroSlotTradeTipManager:
    """
    Provides tip instructions for 0 slot trade tip.
    """
    def __init__(self, tip_account_str: str, rpc_endpoint: str):
        try:
            self.tip_account = Pubkey.from_string(tip_account_str)
        except ValueError:
            logger.error(f"Invalid zero slot tip account public key: {tip_account_str}")
            raise
        self.zeroslot_rpc_client = AsyncClient(rpc_endpoint)
        self._ping_task: asyncio.Task | None = None # Added for pinging

    async def start(self):
        """Start the WebSocket connection, tip listener, and HTTP pinger."""
        if not (self.tip_account):
            logger.warning("Zero slot tip manager not started due to missing tip account or zero hardcap.")
            return
        logger.info(f"Starting zero slot tip manager for {self.tip_account} | keeping alive every {ZERO_SLOT_TIP_INTERVAL_SECONDS}s")
        self._ping_task = asyncio.create_task(self._send_ping_periodically()) # Start ping task

    async def _send_ping_periodically(self):
        """Periodically sends HTTP GET to /ping to keep the connection alive."""
        logger.info(f"Starting Zero slot tip keep-alive pinger every {ZERO_SLOT_TIP_INTERVAL_SECONDS}s.")
        while True:
            try:
                response = await self.zeroslot_rpc_client.is_connected()
                if response:
                    logger.debug(f"Zero slot tip keep-alive pinger | status: connected.")
                else:
                    logger.warning(f"Zero slot tip keep-alive pinger | status: disconnected.")
            except aiohttp.ClientError as e:
                logger.warning(f"Error sending Zero slot tip keep-alive pinger: {e!s}")
            except asyncio.TimeoutError:
                logger.warning("Timeout sending Zero slot tip keep-alive pinger.")
            except Exception as e:
                logger.error(f"Unexpected error in Zero slot tip keep-alive pinger: {e!s}", exc_info=True)
            
            await asyncio.sleep(ZERO_SLOT_TIP_INTERVAL_SECONDS)

    async def get_tip_instruction(self, payer_pubkey: Pubkey, tip_amount_lamports: int) -> Instruction | None:
        """
        Get a tip instruction.
        """
        if tip_amount_lamports > 0:
            logger.info(f"Creating instruction for Zero slot tip: {tip_amount_lamports} lamports transfer to {self.tip_account}")
            return transfer(
                TransferParams(
                    from_pubkey=payer_pubkey,
                    to_pubkey=self.tip_account,
                    lamports=tip_amount_lamports,
                )
            )
        return None

    async def send_transaction(self, signed_transaction, tx_opts):
        """
        Send a transaction with the zero slot tip instruction.
        """
        return await self.zeroslot_rpc_client.send_transaction(signed_transaction, tx_opts)

    async def stop(self):
        """Stop the the HTTP pinger."""
        logger.info("Stopping Zero slot tip keep-alive pinger...")
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                logger.info("Zero slot tip keep-alive pinger task cancelled.")
            self._ping_task = None      
        logger.info("Zero slot tip keep-alive pinger stopped.")
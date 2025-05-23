"""
Manages Nozomi tip stream for faster transaction landing.
"""
import asyncio
import json
import websockets
import aiohttp # Added for HTTP pings
from solders.pubkey import Pubkey
from solders.system_program import transfer, TransferParams
from solders.instruction import Instruction
from utils.logger import get_logger
from core.pubkeys import LAMPORTS_PER_SOL
from solana.rpc.async_api import AsyncClient

logger = get_logger(__name__)

# Default Nozomi ping URL, can be made configurable if needed
NOZOMI_PING_URL = "http://nozomi.temporal.xyz/ping"
NOZOMI_PING_INTERVAL_SECONDS = 60

class NozomiTipStreamManager:
    """
    Connects to Nozomi WebSocket, listens for tips, provides tip instructions,
    and handles keep-alive pings.
    """
    def __init__(self, websocket_url: str, tip_account_str: str, tip_hardcap_sol: float, rpc_endpoint: str):
        self.websocket_url = websocket_url
        try:
            self.tip_account = Pubkey.from_string(tip_account_str)
        except ValueError:
            logger.error(f"Invalid Nozomi tip account public key: {tip_account_str}")
            raise
        self.nozomi_rpc_client = AsyncClient(rpc_endpoint)
        self.tip_hardcap_lamports = int(tip_hardcap_sol * LAMPORTS_PER_SOL)
        self._current_tip_lamports: int = 0
        self._tip_lock = asyncio.Lock()
        self._websocket_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None # Added for pinging
        self.is_connected = False
        self._http_session: aiohttp.ClientSession | None = None

    async def start(self):
        """Start the WebSocket connection, tip listener, and HTTP pinger."""
        if not (self.websocket_url and self.tip_account and self.tip_hardcap_lamports > 0):
            logger.warning("Nozomi tip stream not started due to missing URL, tip account, or zero hardcap.")
            return

        logger.info(f"Starting Nozomi tip stream manager for {self.websocket_url}")
        logger.info(f"Tip account: {self.tip_account}")
        logger.info(f"Tip hardcap: {self.tip_hardcap_lamports} lamports ({self.tip_hardcap_lamports / LAMPORTS_PER_SOL:.6f} SOL)")
        
        self._http_session = aiohttp.ClientSession()
        self._websocket_task = asyncio.create_task(self._listen_for_tips())
        self._ping_task = asyncio.create_task(self._send_ping_periodically()) # Start ping task

    async def _send_ping_periodically(self):
        """Periodically sends HTTP GET to /ping to keep the connection alive."""
        if not self._http_session:
            logger.error("HTTP session not initialized for Nozomi pinger.")
            return
        logger.info(f"Starting Nozomi keep-alive pinger to {NOZOMI_PING_URL} every {NOZOMI_PING_INTERVAL_SECONDS}s.")
        while True:
            try:
                async with self._http_session.get(NOZOMI_PING_URL, timeout=10) as response:
                    if response.status == 200:
                        logger.debug(f"Nozomi /ping successful (status {response.status}).")
                    else:
                        logger.warning(f"Nozomi /ping returned status {response.status}.")
                # Also ping the RPC endpoint
                response = await self.nozomi_rpc_client.is_connected()
                if response:
                    logger.debug(f"Nozomi RPC endpoint is connected.")
                else:
                    logger.warning(f"Nozomi RPC endpoint is disconnected.")
            except aiohttp.ClientError as e:
                logger.warning(f"Error sending Nozomi /ping: {e!s}")
            except asyncio.TimeoutError:
                logger.warning("Timeout sending Nozomi /ping.")
            except Exception as e:
                logger.error(f"Unexpected error in Nozomi pinger: {e!s}", exc_info=True)
            
            await asyncio.sleep(NOZOMI_PING_INTERVAL_SECONDS)

    async def _listen_for_tips(self):
        """Continuously listen for tip updates from the Nozomi WebSocket."""
        while True:
            try:
                async with websockets.connect(self.websocket_url) as ws:
                    self.is_connected = True
                    logger.info("Connected to Nozomi tip stream.")
                    async for message_str in ws:
                        try:
                            data = json.loads(message_str)
                            # Expecting a list with one dictionary element:
                            # [{"time": ..., "landed_tips_95th_percentile": ...}]
                            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                                message = data[0]
                                if "landed_tips_95th_percentile" in message:
                                    # Assuming the tip value is in lamports
                                    tip_in_lamports = int(message["landed_tips_95th_percentile"])
                                    async with self._tip_lock:
                                        self._current_tip_lamports = tip_in_lamports
                                        # logger.debug(f"Nozomi suggested tip (95th percentile): {tip_in_lamports} lamports")
                                else:
                                    logger.debug(f"'landed_tips_95th_percentile' not in Nozomi message: {message}")
                            else:
                                logger.debug(f"Received non-standard format from Nozomi: {message_str[:200]}...")
                                continue

                        except json.JSONDecodeError:
                            logger.warning(f"Failed to decode JSON from Nozomi: {message_str}")
                        except KeyError:
                            logger.warning(f"Unexpected message structure from Nozomi: {message_str}")
                        except Exception as e:
                            logger.error(f"Error processing Nozomi tip message: {e!s}", exc_info=True)
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Nozomi WebSocket connection closed. Reconnecting in 5 seconds...")
                self.is_connected = False
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error connecting to Nozomi WebSocket: {e!s}. Retrying in 10 seconds...")
                self.is_connected = False
                await asyncio.sleep(10)

    # async def get_tip_instruction(self, payer_pubkey: Pubkey) -> Instruction | None:
    #     """
    #     Get a tip instruction. Uses the latest received tip, even if currently disconnected.
    #     The actual tip paid will be the minimum of the suggested tip and the hardcap.
    #     """
    #     if not self.is_connected:
    #         # User requested this log message.
    #         logger.debug("Nozomi tip stream not connected. Latest tip will be added to the transaction.")

    #     async with self._tip_lock:
    #         if self._current_tip_lamports > 0:
    #             tip_to_pay = min(self._current_tip_lamports, self.tip_hardcap_lamports)
    #             if tip_to_pay > 0:
    #                 logger.info(f"Adding Nozomi tip: {tip_to_pay} lamports to {self.tip_account}")
    #                 return transfer(
    #                     TransferParams(
    #                         from_pubkey=payer_pubkey,
    #                         to_pubkey=self.tip_account,
    #                         lamports=tip_to_pay,
    #                     )
    #                 )
    #     # If no current tip is stored, or tip_to_pay is 0, return None
    #     if self._current_tip_lamports <= 0:
    #         logger.debug("No valid Nozomi tip available to create instruction returning hardcap.")
    #     return transfer(
    #         TransferParams(
    #             from_pubkey=payer_pubkey,
    #             to_pubkey=self.tip_account,
    #             lamports=self.tip_hardcap_lamports,
    #         )
    #     )

    async def get_tip_instruction(self, payer_pubkey: Pubkey, tip_amount_lamports: int) -> Instruction | None:
        """
        Get a tip instruction.
        """
        if tip_amount_lamports > 0:
            logger.info(f"Creating instruction for Nozomi tip: {tip_amount_lamports} lamports transfer to {self.tip_account}")
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
        Send a transaction with the Nozomi tip instruction.
        """
        return await self.nozomi_rpc_client.send_transaction(signed_transaction, tx_opts)

    async def stop(self):
        """Stop the WebSocket connection listener and the HTTP pinger."""
        logger.info("Stopping Nozomi tip stream manager...")
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                logger.info("Nozomi pinger task cancelled.")
            self._ping_task = None

        if self._websocket_task:
            self._websocket_task.cancel()
            try:
                await self._websocket_task
            except asyncio.CancelledError:
                logger.info("Nozomi tip listener task cancelled.")
            self._websocket_task = None
        
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            logger.info("Nozomi HTTP session closed.")
            self._http_session = None
            
        self.is_connected = False
        logger.info("Nozomi tip stream manager stopped.")
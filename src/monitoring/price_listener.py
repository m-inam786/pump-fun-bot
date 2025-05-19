"""
Real-time price monitoring for pump.fun tokens using a shared WebSocket connection.
"""

import asyncio
import base64
import json
from typing import Callable, Optional, Dict, Any
from decimal import Decimal

from solders.pubkey import Pubkey
import base58
from aiohttp import ClientSession
from core.curve import BondingCurveManager
from utils.logger import get_logger
from utils.serializer import PumpFunSerializer
from utils.sol_to_usd_converter import SolToUsdConverter
from .shared_websocket_listener import SharedWebsocketListener

logger = get_logger(__name__)


class PriceListener:
    """
    Listens for price changes on a specific token by processing logs
    from a SharedWebsocketListener.
    """

    def __init__(self, 
                 shared_listener: SharedWebsocketListener, # Use 'Any' or forward reference 'SharedWebsocketListener'
                 curve_manager: BondingCurveManager, 
                 mint: Pubkey,
                 sol_to_usd_converter: SolToUsdConverter):
        """Initialize price listener.

        Args:
            shared_listener: An instance of SharedWebsocketListener.
            curve_manager: Bonding curve manager for price calculations.
            mint: The mint Pubkey of the token to monitor.
            sol_to_usd_converter: Converter to get SOL price in USD
        """
        self.shared_listener = shared_listener
        self.curve_manager = curve_manager
        self.mint: Pubkey = mint
        self.serializer = PumpFunSerializer()
        self.is_monitoring = False
        self._price_callback: Optional[Callable[[float], None]] = None
        self.sol_to_usd_converter = sol_to_usd_converter
        self.stop_event = asyncio.Event()

    async def start_monitoring(
        self, 
        price_callback: Callable[[float], None]
    ) -> None:
        """Start monitoring price changes for the specific token.

        Args:
            price_callback: Callback function to call when price changes.
        """
        if self.is_monitoring:
            logger.warning(f"Price listener for {self.mint} is already monitoring.")
            return

        self._price_callback = price_callback
        # The handler passed to shared_listener must be an async method.
        await self.shared_listener.add_processor(self._handle_log_notification)
        self.is_monitoring = True
        logger.info(f"Price listener for {self.mint} started monitoring.")

        # Optional: Immediately try to fetch and report current price if needed
        # This is outside the scope of WebSocket logs, so needs careful consideration
        # try:
        #     curve_state = await self.curve_manager.get_curve_state_by_mint(self.mint) # Assuming such a method exists or can be added
        #     if curve_state:
        #         current_price = curve_state.calculate_price()
        #         if self._price_callback:
        #             asyncio.create_task(self._price_callback(current_price))
        #     else:
        #         logger.warning(f"Could not fetch initial curve state for mint {self.mint}")
        # except Exception as e:
        #     logger.error(f"Failed to get initial price for {self.mint}: {e}")

    async def stop_monitoring(self) -> None:
        """Stop monitoring price changes."""
        if not self.is_monitoring:
            logger.warning(f"Price listener for {self.mint} is not currently monitoring.")
            return
            
        await self.shared_listener.remove_processor(self._handle_log_notification)
        self.is_monitoring = False
        self._price_callback = None # Clear callback
        logger.info(f"Price listener for {self.mint} stopped monitoring.")

    async def _handle_log_notification(self, message_json: Dict[str, Any]) -> None:
        """
        Process a log notification message received from the SharedWebsocketListener.
        This method is called by the SharedWebsocketListener for every log notification.
        It filters for logs relevant to this listener's specific mint.
        """
        if not self.is_monitoring or not self._price_callback:
            # Not expecting messages if not monitoring or no callback is set
            return

        # message_json is already parsed by SharedWebsocketListener
        # It should have format like:
        # {
        #   "jsonrpc": "2.0",
        #   "method": "logsNotification",
        #   "params": {
        #     "result": {
        #       "context": { "slot": <slot> },
        #       "value": {
        #         "signature": "<signature>",
        #         "err": null,
        #         "logs": ["Log 1", "Program data: <data>", "Log 3"]
        #       }
        #     },
        #     "subscription": <subscription_id>
        #   }
        # }

        try:
            if message_json.get("method") != "logsNotification":
                return # Should have been filtered by shared listener, but double check
            
            params = message_json.get("params", {})
            result = params.get("result", {})
            value = result.get("value", {})
            logs = value.get("logs", [])
                
            parsed_data = None
            for log_entry in logs:
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
                        
                        if parsed_data and parsed_data.get("mint") == str(self.mint) and \
                           "virtual_sol_reserves" in parsed_data and \
                           "virtual_token_reserves" in parsed_data:
                            
                            try:
                                # Ensure these are strings before creating Decimal
                                if not isinstance(parsed_data["virtual_sol_reserves"], str) or not isinstance(parsed_data["virtual_token_reserves"], str):
                                    logger.warning(f"Reserve values are not strings for mint {self.mint}. VSR: {type(parsed_data['virtual_sol_reserves'])}, VTR: {type(parsed_data['virtual_token_reserves'])}")
                                    continue

                                vsr_str = parsed_data["virtual_sol_reserves"]
                                vtr_str = parsed_data["virtual_token_reserves"]
                                
                                vsr = Decimal(vsr_str) / Decimal('1e9')  # SOL has 9 decimals
                                vtr = Decimal(vtr_str) / Decimal('1e6')  # Tokens have 6 decimals
                                
                                price = self._compute_price(vsr, vtr)
                                usd_price = self.sol_to_usd_converter.convert_sol_to_usd(price)
                                
                                # show price in SOL and USD in scientific notation
                                logger.info(f"Price update for {self.mint}: {price:.8f} SOL (${usd_price:.2e} USD)")
                                if self._price_callback: # Check again, could have been stopped concurrently
                                   await self._price_callback(float(price)) # Await if callback is async
                                return # Processed this mint's update
                            except ValueError as ve:
                                logger.error(f"ValueError converting reserves to Decimal for mint {self.mint}: {ve}. Data: {parsed_data}")
                            except Exception as e:
                                logger.error(f"Error calculating price from parsed data for mint {self.mint}: {e}. Data: {parsed_data}")
                        # else:
                        #     # Minimal logging if data is not for this mint to avoid spam
                        #     if parsed_data and parsed_data.get("mint"):
                        #        logger.info(f"Log for other mint: {parsed_data.get('mint')}, listening for {self.mint}")

                    except Exception as e:
                        logger.error(f"Error parsing individual program data log entry for {self.mint}: {e}. Log: {log_entry[:100]}")
            
        except Exception as e:
            logger.error(f"General error processing log notification for mint {self.mint}: {e}. Message: {str(message_json)[:500]}", exc_info=True)
            
    def _compute_price(self, vsr: Decimal, vtr: Decimal) -> Decimal:
        """Compute the price from virtual reserves.
        
        Args:
            vsr: Virtual SOL reserves in SOL (not lamports)
            vtr: Virtual token reserves in tokens (not raw units)
            
        Returns:
            Current token price in SOL
        """
        if vtr == Decimal('0'): # Compare with Decimal
            return Decimal('0')
        return vsr / vtr
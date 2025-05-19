"""
Solana to USD conversion utility.
Provides a reusable component to fetch and update SOL/USD price from CoinGecko.
"""

import asyncio
from decimal import Decimal
from typing import Optional

from aiohttp import ClientSession
from utils.logger import get_logger

logger = get_logger(__name__)


class SolToUsdConverter:
    """
    Manages Solana to USD conversion with periodic updates.
    Designed to be initialized once and shared across components.
    """

    def __init__(self, update_interval: int = 480):
        """Initialize the SOL to USD converter.

        Args:
            update_interval: Time between price updates in seconds (default: 8 minutes)
        """
        self.update_interval = update_interval
        self.sol_price_usd = Decimal('0')
        self.update_task: Optional[asyncio.Task] = None
        self.stop_event = asyncio.Event()

    async def start(self):
        """Start the automatic price update task."""
        if self.update_task and not self.update_task.done():
            logger.warning("SOL to USD converter is already running")
            return

        # Get initial price
        self.sol_price_usd = await self._get_solana_price_usd()
        logger.info(f"Initial SOL price: ${self.sol_price_usd} USD")

        # Start background task for periodic updates
        self.stop_event.clear()
        self.update_task = asyncio.create_task(self._update_price_periodically())

    async def stop(self):
        """Stop the automatic price update task."""
        if not self.update_task or self.update_task.done():
            return

        self.stop_event.set()
        try:
            await self.update_task
        except asyncio.CancelledError:
            pass
        self.update_task = None

    async def _update_price_periodically(self):
        """Periodically update the SOL price."""
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(self.update_interval)
                new_price = await self._get_solana_price_usd()
                
                # Only log if price changed significantly (more than 0.5%)
                if abs((new_price - self.sol_price_usd) / self.sol_price_usd) > Decimal('0.005'):
                    logger.info(f"SOL price updated: ${new_price} USD (was ${self.sol_price_usd})")
                
                self.sol_price_usd = new_price
        except asyncio.CancelledError:
            logger.info("SOL price update task cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in SOL price update task: {e}")

    async def _get_solana_price_usd(self) -> Decimal:
        """Fetch the current SOL price from CoinGecko.

        Returns:
            Current SOL price in USD as Decimal
        """
        try:
            async with ClientSession() as session:
                async with session.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd') as response:
                    if response.status == 200:
                        data = await response.json()
                        price = data['solana']['usd']
                        return Decimal(str(price))
                    else:
                        logger.warning(f"Failed to get Solana price from CoinGecko: HTTP {response.status}")
                        # Return current price if we already have one, otherwise fallback value
                        return self.sol_price_usd if self.sol_price_usd > 0 else Decimal('140')
        except Exception as e:
            logger.warning(f"Failed to get Solana price from CoinGecko: {str(e)}")
            # Return current price if we already have one, otherwise fallback value
            return self.sol_price_usd if self.sol_price_usd > 0 else Decimal('140')

    def convert_sol_to_usd(self, sol_amount: Decimal) -> Decimal:
        """Convert a SOL amount to USD.

        Args:
            sol_amount: Amount in SOL

        Returns:
            Equivalent amount in USD
        """
        return sol_amount * self.sol_price_usd

    def get_current_price(self) -> Decimal:
        """Get the current SOL price in USD.

        Returns:
            Current SOL price in USD
        """
        return self.sol_price_usd

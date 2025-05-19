"""
Base class for WebSocket token listeners.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Optional

from trading.base import TokenInfo
from monitoring.developer_manager import DeveloperManager


class BaseTokenListener(ABC):
    """Base abstract class for token listeners."""

    def __init__(self, developer_manager: Optional[DeveloperManager] = None):
        """Initialize the token listener.
        
        Args:
            developer_manager: Optional developer manager for whitelist filtering
        """
        self.developer_manager = developer_manager

    @abstractmethod
    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        """
        Listen for new token creations.

        Args:
            token_callback: Callback function for new tokens
            match_string: Optional string to match in token name/symbol
            creator_address: Optional creator address to filter by
        """
        pass

    async def should_process_token(
        self, 
        creator_address_to_check: str | None = None
    ) -> bool:
        """Determine if a token should be processed based on creator address.
        
        Args:
            creator_address: Optional creator address to filter by
            
        Returns:
            True if the token should be processed, False otherwise
        """
        if self.developer_manager is not None and creator_address_to_check is not None:
            return await self.developer_manager.is_whitelisted(creator_address_to_check)
        else:
            return True

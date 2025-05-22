"""
Base interfaces for trading operations.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from solders.pubkey import Pubkey

from core.pubkeys import PumpAddresses


@dataclass
class TokenInfo:
    """Token information."""

    name: str
    symbol: str
    uri: str
    mint: Pubkey
    bonding_curve: Pubkey
    associated_bonding_curve: Pubkey
    user: Pubkey
    creator: Pubkey
    creator_vault: Pubkey
    # Trading parameters attached directly to avoid lookup in critical path
    trading_params: dict[str, Any] = None

    def __post_init__(self):
        """Initialize default values."""
        if self.trading_params is None:
            self.trading_params = {}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TokenInfo":
        """Create TokenInfo from dictionary.

        Args:
            data: Dictionary with token data

        Returns:
            TokenInfo instance
        """
        # Extract trading params if included
        trading_params = data.get("trading_params", {})
        
        return cls(
            name=data["name"],
            symbol=data["symbol"],
            uri=data["uri"],
            mint=Pubkey.from_string(data["mint"]),
            bonding_curve=Pubkey.from_string(data["bondingCurve"]),
            associated_bonding_curve=Pubkey.from_string(data["associatedBondingCurve"]),
            user=Pubkey.from_string(data["user"]),
            creator=Pubkey.from_string(data["creator"]),
            creator_vault=Pubkey.from_string(data["creator_vault"]),
            trading_params=trading_params
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        result = {
            "name": self.name,
            "symbol": self.symbol,
            "uri": self.uri,
            "mint": str(self.mint),
            "bondingCurve": str(self.bonding_curve),
            "associatedBondingCurve": str(self.associated_bonding_curve),
            "user": str(self.user),
            "creator": str(self.creator),
            "creatorVault": str(self.creator_vault),
        }
        
        # Include trading params if present
        if self.trading_params:
            result["trading_params"] = self.trading_params
            
        return result


@dataclass
class TradeResult:
    """Result of a trading operation."""

    success: bool
    tx_signature: str | None = None
    error_message: str | None = None
    amount: float | None = None
    price: float | None = None
    is_partial: bool = False
    percent_sold: float = 1.0


class Trader(ABC):
    """Base interface for trading operations."""

    @abstractmethod
    async def execute(self, *args, **kwargs) -> TradeResult:
        """Execute trading operation.

        Returns:
            TradeResult with operation outcome
        """
        pass

    def _get_relevant_accounts(self, token_info: TokenInfo) -> list[Pubkey]:
        """
        Get the list of accounts relevant for calculating the priority fee.

        Args:
            token_info: Token information for the buy/sell operation.

        Returns:
            list[Pubkey]: List of relevant accounts.
        """
        return [
            token_info.mint,  # Token mint address
            token_info.bonding_curve,  # Bonding curve address
            PumpAddresses.PROGRAM,  # Pump.fun program address
            PumpAddresses.FEE,  # Pump.fun fee account
        ]

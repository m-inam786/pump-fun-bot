"""
Discord notification system with queue mechanism.

This module provides a non-blocking way to send notifications to Discord
through webhooks while ensuring the main application flow isn't interrupted.
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Union, TypedDict, cast

import aiohttp

from utils.logger import get_logger

logger = get_logger(__name__)

# Discord message size limits
DISCORD_LIMITS = {
    "content": 2000,
    "embed_title": 256,
    "embed_description": 4096,
    "field_name": 256,
    "field_value": 1024,
    "total_embed_chars": 6000,
    "max_fields": 25
}

class NotificationType(Enum):
    """Types of notifications that can be sent."""
    BULK_SNIPE_ADD = auto()
    SNIPE_ADD = auto()
    SNIPE_EXPIRE = auto()
    SNIPE_DELETE = auto()
    TOKEN_BUY = auto()
    TOKEN_SELL = auto()
    PNL = auto()
    SYSTEM = auto()
    ERROR = auto()


class EmbedField(TypedDict, total=False):
    """Type definition for a Discord embed field."""
    name: str
    value: str
    inline: bool


@dataclass
class DiscordMessage:
    """Represents a message to be sent to Discord."""
    notification_type: NotificationType
    content: str = ""
    embed_title: Optional[str] = None
    embed_description: Optional[str] = None
    embed_fields: Optional[List[EmbedField]] = None
    embed_color: Optional[int] = None
    timestamp: float = 0
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()
    
    def _truncate_str(self, text: str, limit: int) -> str:
        """Truncate a string to the specified limit."""
        if len(text) <= limit:
            return text
        return text[:limit-3] + "..."
    
    def to_payload(self) -> Dict[str, Any]:
        """Convert the message to a Discord webhook payload."""
        payload = {}
        
        if self.content:
            payload["content"] = self._truncate_str(self.content, DISCORD_LIMITS["content"])
            
        if any([self.embed_title, self.embed_description, self.embed_fields]):
            embed = {}
            total_chars = 0
            
            if self.embed_title:
                truncated_title = self._truncate_str(self.embed_title, DISCORD_LIMITS["embed_title"])
                embed["title"] = truncated_title
                total_chars += len(truncated_title)
                
            if self.embed_description:
                truncated_desc = self._truncate_str(self.embed_description, DISCORD_LIMITS["embed_description"])
                embed["description"] = truncated_desc
                total_chars += len(truncated_desc)
                
            if self.embed_fields:
                # Limit number of fields to maximum allowed
                fields = self.embed_fields[:DISCORD_LIMITS["max_fields"]]
                
                # Truncate field content to fit limits
                processed_fields = []
                for field in fields:
                    # Stop adding fields if we're approaching total embed char limit
                    if total_chars >= DISCORD_LIMITS["total_embed_chars"] - 100:
                        break
                        
                    name = self._truncate_str(field["name"], DISCORD_LIMITS["field_name"])
                    value = self._truncate_str(field["value"], DISCORD_LIMITS["field_value"])
                    
                    field_chars = len(name) + len(value)
                    if total_chars + field_chars > DISCORD_LIMITS["total_embed_chars"]:
                        # Skip this field if it would push us over the limit
                        continue
                        
                    processed_fields.append({
                        "name": name,
                        "value": value,
                        "inline": field.get("inline", False)
                    })
                    
                    total_chars += field_chars
                
                if processed_fields:
                    embed["fields"] = processed_fields
                
            if self.embed_color:
                embed["color"] = self.embed_color
            else:
                # Default colors based on notification type
                colors = {
                    NotificationType.BULK_SNIPE_ADD: 0x0000FF, # Blue
                    NotificationType.SNIPE_ADD: 0x00FF00,      # Green
                    NotificationType.SNIPE_EXPIRE: 0xFFA500,   # Orange
                    NotificationType.SNIPE_DELETE: 0xFF0000,   # Red
                    NotificationType.TOKEN_BUY: 0x00FFFF,      # Cyan
                    NotificationType.TOKEN_SELL: 0xFF00FF,     # Magenta
                    NotificationType.PNL: 0xFFFF00,            # Yellow
                    NotificationType.SYSTEM: 0x808080,         # Gray
                    NotificationType.ERROR: 0xFF0000,          # Red
                }
                embed["color"] = colors.get(self.notification_type, 0x0000FF)
                
            # Add timestamp
            embed["timestamp"] = datetime.fromtimestamp(self.timestamp).isoformat()
            
            payload["embeds"] = [embed]
            
        return payload


class DiscordNotifier:
    """
    Manages Discord webhook notifications with a queue system to avoid blocking.
    
    This class maintains a queue of messages and processes them in the background,
    ensuring that the main application flow isn't blocked by webhook requests.
    """
    
    def __init__(self, webhook_url: str, queue_size: int = 1000, 
                 worker_count: int = 1, retry_limit: int = 3):
        """
        Initialize the Discord notifier.
        
        Args:
            webhook_url: Discord webhook URL
            queue_size: Maximum size of the queue (0 for unlimited)
            worker_count: Number of background workers processing the queue
            retry_limit: Maximum number of retries for failed messages
        """
        self.webhook_url = webhook_url
        self.queue_size = queue_size
        self.worker_count = worker_count
        self.retry_limit = retry_limit
        
        # Create the message queue
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        
        # Worker tasks
        self.workers: List[asyncio.Task] = []
        
        # Session for HTTP requests
        self.session: Optional[aiohttp.ClientSession] = None
        
        # State
        self.running = False
        
    async def start(self):
        """Start the Discord notification service."""
        if self.running:
            logger.warning("Discord notification service is already running")
            return
            
        self.running = True
        self.session = aiohttp.ClientSession()
        
        # Start worker tasks
        for i in range(self.worker_count):
            worker = asyncio.create_task(self._worker_loop(i))
            self.workers.append(worker)
            
        logger.info(f"Started Discord notification service with {self.worker_count} workers")
        
    async def stop(self):
        """Stop the Discord notification service."""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel all worker tasks
        for worker in self.workers:
            worker.cancel()
            
        # Wait for workers to finish
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
            self.workers = []
            
        # Close the HTTP session
        if self.session:
            await self.session.close()
            self.session = None
            
        logger.info("Stopped Discord notification service")
        
    async def send(self, message: DiscordMessage) -> bool:
        """
        Queue a message to be sent to Discord.
        
        Args:
            message: The message to send
            
        Returns:
            True if the message was queued successfully, False otherwise
        """
        if not self.running:
            logger.warning("Discord notification service is not running")
            return False
            
        try:
            # Try to add the message to the queue with a timeout
            await asyncio.wait_for(self.queue.put(message), timeout=1.0)
            return True
        except asyncio.TimeoutError:
            logger.warning("Discord notification queue is full, message dropped")
            return False
        except Exception as e:
            logger.error(f"Error queuing Discord message: {e}")
            return False
            
    async def _worker_loop(self, worker_id: int):
        """
        Background worker that processes the message queue.
        
        Args:
            worker_id: Identifier for this worker
        """
        logger.info(f"Discord notification worker {worker_id} started")
        
        while self.running:
            try:
                # Get a message from the queue with a timeout
                message = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                
                # Try to send the message with retries
                success = False
                for attempt in range(self.retry_limit):
                    try:
                        await self._send_to_webhook(message)
                        success = True
                        break
                    except Exception as e:
                        if attempt < self.retry_limit - 1:
                            # Exponential backoff with jitter
                            delay = (2 ** attempt) + (0.1 * attempt)
                            logger.warning(f"Discord notification attempt {attempt+1} failed, retrying in {delay:.1f}s: {e}")
                            await asyncio.sleep(delay)
                        else:
                            logger.error(f"Failed to send Discord notification after {self.retry_limit} attempts: {e}")
                
                # Mark the task as done
                self.queue.task_done()
                
                if success:
                    logger.debug(f"Discord notification sent: {message.notification_type.name}")
                    
            except asyncio.TimeoutError:
                # No messages in the queue, continue the loop
                continue
            except asyncio.CancelledError:
                # Worker is being cancelled
                logger.info(f"Discord notification worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Error in Discord notification worker {worker_id}: {e}")
                
        logger.info(f"Discord notification worker {worker_id} stopped")
        
    async def _send_to_webhook(self, message: DiscordMessage):
        """
        Send a message to the Discord webhook.
        
        Args:
            message: The message to send
            
        Raises:
            Exception: If the webhook request fails
        """
        if not self.session:
            raise RuntimeError("HTTP session is not initialized")
            
        payload = message.to_payload()
        
        async with self.session.post(
            self.webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"}
        ) as response:
            if response.status >= 400:
                text = await response.text()
                raise RuntimeError(f"Discord webhook error: {response.status} - {text}")


# Helper functions for common notification types

async def notify_bulk_snipe_add(
    notifier: DiscordNotifier,
    developer_data: List[Dict[str, Any]],
    extra_info: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send a notification when multiple developers are added to the whitelist with their configs.
    
    Args:
        notifier: Discord notifier instance
        developer_data: List of dictionaries containing developer addresses and configs
        extra_info: Additional information to include
        
    Returns:
        True if notification was queued successfully
    """
    # Extract just the addresses for the message
    addresses = [dev["address"] for dev in developer_data]
    message = f"Added {len(addresses)} snipes"

    fields: List[EmbedField] = []
    
    # Add a field for each developer with their config
    for i, dev_info in enumerate(developer_data, 1):
        address = dev_info["address"]
        config = dev_info.get("config", {})
        
        # Format the value with the config parameters
        value = f"`{address}`\n"
        
        if config:
            config_str = "\n".join([f"• {key}: {val}" for key, val in config.items()])
            value += f"```{config_str}```"
        
        # Add field with the address and config
        fields.append({
            "name": f"Developer {i}",
            "value": value,
            "inline": True
        })

    # Add extra info fields
    if extra_info:
        for key, value in extra_info.items():
            fields.append({"name": key, "value": str(value), "inline": True})

    message = DiscordMessage(
        notification_type=NotificationType.BULK_SNIPE_ADD,
        embed_title="🎯 Bulk Snipe Added",
        embed_description=message,
        embed_fields=fields
    )

    return await notifier.send(message)

async def notify_snipe_add(
    notifier: DiscordNotifier, 
    developer_address: str, 
    extra_info: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send a notification when a developer is added to the whitelist.
    
    Args:
        notifier: Discord notifier instance
        developer_address: Developer address
        extra_info: Additional information to include
        
    Returns:
        True if notification was queued successfully
    """
    fields: List[EmbedField] = [
        {"name": "Address", "value": f"`{developer_address}`", "inline": True}
    ]
    
    if extra_info:
        for key, value in extra_info.items():
            fields.append({"name": key, "value": str(value), "inline": True})
    
    message = DiscordMessage(
        notification_type=NotificationType.SNIPE_ADD,
        embed_title="🎯 Snipe Added",
        embed_description=f"Added snipe for developer {developer_address}",
        embed_fields=fields
    )
    
    return await notifier.send(message)


async def notify_snipe_expire(
    notifier: DiscordNotifier, 
    developer_address: str,
    age_days: float,
    reason: str
) -> bool:
    """
    Send a notification when a developer is removed from the whitelist due to age.
    
    Args:
        notifier: Discord notifier instance
        developer_address: Address of the developer
        age_days: Age of the developer in days
        reason: Reason for removal
        
    Returns:
        True if notification was queued successfully
    """
    fields: List[EmbedField] = [
        {"name": "Developer", "value": f"`{developer_address}`", "inline": True},
        {"name": "Age", "value": f"{age_days:.1f} days", "inline": True},
        {"name": "Reason", "value": reason, "inline": False}
    ]
    
    message = DiscordMessage(
        notification_type=NotificationType.SNIPE_EXPIRE,
        embed_title="⌛ Developer Expired",
        embed_description=f"Developer removed from whitelist",
        embed_fields=fields
    )
    
    return await notifier.send(message)

async def notify_token_buy(
    notifier: DiscordNotifier, 
    token_name: str, 
    token_address: str,
    amount: float,
    price: float,
    tx_signature: str,
    price_usd: float = None
) -> bool:
    """Send a notification when a token is bought."""
    fields: List[EmbedField] = [
        {"name": "Token", "value": token_name, "inline": True},
        {"name": "Address", "value": f"`{token_address}`", "inline": True},
        {"name": "Amount", "value": str(amount), "inline": True},
        {"name": "Price", "value": f"{price} SOL{f' (${price_usd:.2f} USD)' if price_usd else ''}", "inline": True},
        {"name": "Transaction", "value": f"[View on Explorer](https://solscan.io/tx/{tx_signature})", "inline": False}
    ]
    
    message = DiscordMessage(
        notification_type=NotificationType.TOKEN_BUY,
        embed_title="🛒 Token Bought",
        embed_description=f"Bought {amount} {token_name} for {price} SOL{f' (${price_usd:.2f} USD)' if price_usd else ''}",
        embed_fields=fields
    )
    
    return await notifier.send(message)


async def notify_token_sell(
    notifier: DiscordNotifier, 
    token_name: str, 
    token_address: str,
    amount: float,
    price: float,
    tx_signature: str,
    is_partial: bool = False,
    percent_sold: float = 1.0,
    price_usd: float = None
) -> bool:
    """Send a notification when a token is sold."""
    fields: List[EmbedField] = [
        {"name": "Token", "value": token_name, "inline": True},
        {"name": "Address", "value": f"`{token_address}`", "inline": True},
        {"name": "Amount", "value": str(amount), "inline": True},
        {"name": "Price", "value": f"{price} SOL{f' (${price_usd:.2f} USD)' if price_usd else ''}", "inline": True},
        {"name": "Transaction", "value": f"[View on Explorer](https://solscan.io/tx/{tx_signature})", "inline": False}
    ]
    
    # Add information about partial sell if applicable
    if is_partial:
        fields.append({"name": "Sell Type", "value": f"Partial Sell ({percent_sold * 100:.0f}%)", "inline": True})
    
    title = "💰 Token Sold"
    if is_partial:
        title = "💰 Token Partially Sold"
    
    description = f"Sold {amount} {token_name} for {price} SOL{f' (${price_usd:.2f} USD)' if price_usd else ''}"
    if is_partial:
        description = f"Partially sold {amount} {token_name} ({percent_sold * 100:.0f}%) for {price} SOL{f' (${price_usd:.2f} USD)' if price_usd else ''}"
    
    message = DiscordMessage(
        notification_type=NotificationType.TOKEN_SELL,
        embed_title=title,
        embed_description=description,
        embed_fields=fields
    )
    
    return await notifier.send(message)


async def notify_pnl(
    notifier: DiscordNotifier, 
    token_name: str, 
    token_address: str,
    buy_price: float,
    sell_price: float,
    profit_loss: float,
    profit_loss_percent: float,
    profit_loss_usd: float
) -> bool:
    """Send a notification about profit and loss for a token."""
    is_profit = profit_loss > 0
    emoji = "🟢" if is_profit else "🔴"
    
    fields: List[EmbedField] = [
        {"name": "Token", "value": token_name, "inline": True},
        {"name": "Address", "value": f"`{token_address}`", "inline": True},
        {"name": "Buy Price", "value": f"{buy_price} SOL", "inline": True},
        {"name": "Sell Price", "value": f"{sell_price} SOL", "inline": True},
        {"name": f"{'Profit' if is_profit else 'Loss'}", "value": f"{profit_loss:.4f} SOL (USD {profit_loss_usd:.2f})", "inline": True},
        {"name": "Percentage", "value": f"{profit_loss_percent:.2f}%", "inline": True}
    ]
    
    message = DiscordMessage(
        notification_type=NotificationType.PNL,
        embed_title=f"{emoji} {'Profit' if is_profit else 'Loss'} Report",
        embed_description=f"PnL for {token_name}",
        embed_fields=fields,
        embed_color=0x00FF00 if is_profit else 0xFF0000
    )
    
    return await notifier.send(message)


async def notify_error(
    notifier: DiscordNotifier, 
    error_title: str,
    error_message: str,
    error_details: Optional[str] = None
) -> bool:
    """Send an error notification."""
    fields: List[EmbedField] = [
        {"name": "Error Message", "value": error_message, "inline": False}
    ]
    
    if error_details:
        fields.append({"name": "Details", "value": error_details, "inline": False})
    
    message = DiscordMessage(
        notification_type=NotificationType.ERROR,
        embed_title=f"❌ {error_title}",
        embed_fields=fields
    )
    
    return await notifier.send(message) 
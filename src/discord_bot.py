"""
Discord bot for managing developer whitelist through Discord commands.

This module provides a Discord bot that allows users to add, remove, and list
developers in the snipe whitelist through Discord slash commands.
"""

import asyncio
import os
import re
from typing import Optional, Dict, Any

import discord
from discord.ext import commands
from discord import app_commands

from monitoring.developer_manager import DeveloperManager
from utils.discord_notifications import (
    DiscordNotifier,
    notify_manual_snipe_add,
    notify_manual_snipe_remove,
    notify_snipe_list,
    notify_snipe_search
)
from utils.logger import get_logger

logger = get_logger(__name__)


class DeveloperManagementBot(commands.Bot):
    """Discord bot for managing developer whitelist."""
    
    def __init__(self, developer_manager: DeveloperManager, discord_notifier: Optional[DiscordNotifier] = None):
        """Initialize the Discord bot.
        
        Args:
            developer_manager: DeveloperManager instance to manage
            discord_notifier: Optional Discord notifier for notifications
        """
        intents = discord.Intents.default()
        intents.message_content = True
        
        super().__init__(command_prefix='!', intents=intents)
        
        self.developer_manager = developer_manager
        self.discord_notifier = discord_notifier
        
        # Regex pattern for validating Solana addresses
        self.solana_address_pattern = re.compile(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$')
        
    async def setup_hook(self):
        """Called when the bot is starting up."""
        logger.info("Discord bot is starting up...")
        await self.tree.sync()
        logger.info("Command tree synced")
        
    async def on_ready(self):
        """Called when the bot is ready."""
        logger.info(f'Discord bot logged in as {self.user} (ID: {self.user.id})')
        
    def is_valid_solana_address(self, address: str) -> bool:
        """Validate if a string is a valid Solana address format.
        
        Args:
            address: Address string to validate
            
        Returns:
            True if valid format, False otherwise
        """
        return bool(self.solana_address_pattern.match(address))
        
    def parse_trading_params(self, params_str: Optional[str]) -> Dict[str, Any]:
        """Parse trading parameters from a string.
        
        DEPRECATED: This method is kept for backward compatibility but is no longer used
        in the new add_snipe command which uses individual parameters.
        
        Expected format: "buy_amount:0.1,buy_slippage:0.05,priority_fee:50000"
        
        Args:
            params_str: Parameters string to parse
            
        Returns:
            Dictionary of parsed parameters
        """
        if not params_str:
            return {}
            
        params = {}
        try:
            for param_pair in params_str.split(','):
                if ':' in param_pair:
                    key, value = param_pair.strip().split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Convert to appropriate types
                    if key in ['buy_amount', 'buy_slippage', 'take_profit_percentage', 'percent_sell_amount']:
                        params[key] = float(value)
                    elif key in ['priority_fee', 'tip_amount', 'token_amount']:
                        params[key] = int(value)
                    else:
                        params[key] = value
                        
        except Exception as e:
            logger.error(f"Error parsing trading parameters '{params_str}': {e}")
            
        return params
    
    def format_parameter_display(self, params: Dict[str, Any]) -> str:
        """Format trading parameters for display in Discord messages.
        
        Args:
            params: Dictionary of trading parameters
            
        Returns:
            Formatted string for Discord display
        """
        if not params:
            return "*No trading parameters specified - will use default settings*"
        
        lines = []
        
        if "buy_amount" in params:
            lines.append(f"• Buy Amount: **{params['buy_amount']:.3f} SOL**")
        
        if "buy_slippage" in params:
            lines.append(f"• Buy Slippage: **{params['buy_slippage']*100:.1f}%**")
        
        if "priority_fee" in params:
            priority_sol = params['priority_fee'] / 1_000_000  # Convert microlamports to SOL
            lines.append(f"• Priority Fee: **{priority_sol:.6f} SOL** ({params['priority_fee']:,} microlamports)")
        
        if "tip_amount" in params:
            tip_sol = params['tip_amount'] / 1_000_000_000
            lines.append(f"• Tip Amount: **{tip_sol:.6f} SOL** ({params['tip_amount']:,} lamports)")
        
        if "take_profit_percentage" in params:
            lines.append(f"• Take Profit: **{params['take_profit_percentage']*100:.1f}%**")
        
        if "percent_sell_amount" in params:
            lines.append(f"• Sell Amount: **{params['percent_sell_amount']*100:.1f}%**")
        
        if "token_amount" in params:
            token_amt = params['token_amount']
            if token_amt >= 1_000_000_000_000:
                lines.append(f"• Token Amount: **{token_amt / 1_000_000_000_000:.2f}T**")
            elif token_amt >= 1_000_000_000:
                lines.append(f"• Token Amount: **{token_amt / 1_000_000_000:.2f}B**")
            elif token_amt >= 1_000_000:
                lines.append(f"• Token Amount: **{token_amt / 1_000_000:.2f}M**")
            else:
                lines.append(f"• Token Amount: **{token_amt:,}**")
        
        return "\n".join(lines)


# Create the cog for developer management commands
class DeveloperCommands(commands.Cog):
    """Cog containing developer management commands."""
    
    def __init__(self, bot: DeveloperManagementBot):
        self.bot = bot
        
    @app_commands.command(name="add_snipe", description="Add a developer to the snipe whitelist with trading parameters")
    @app_commands.describe(
        address="Developer's Solana address (required)",
        buy_amount_sol="Amount of SOL to buy with (e.g., 0.1)",
        buy_slippage_percent="Buy slippage percentage (e.g., 5 for 5%)",
        priority_fee_sol="Priority fee in SOL (e.g., 0.00005 for 50k lamports)",
        tip_amount_sol="Tip amount in SOL (e.g., 0.001)",
        take_profit_percent="Take profit percentage (e.g., 200 for 200%)",
        sell_percent="Percentage of tokens to sell (e.g., 100 for 100%)",
        token_amount="Specific token amount to buy"
    )
    async def add_snipe(
        self, 
        interaction: discord.Interaction, 
        address: str,
        buy_amount_sol: Optional[float] = None,
        buy_slippage_percent: Optional[float] = None,
        priority_fee_sol: Optional[float] = None,
        tip_amount_sol: Optional[float] = None,
        take_profit_percent: Optional[float] = None,
        sell_percent: Optional[float] = None,
        token_amount: Optional[int] = None
    ):
        """Add a developer to the snipe whitelist with clear trading parameters."""
        await interaction.response.defer()
        
        try:
            # Validate address format
            if not self.bot.is_valid_solana_address(address):
                await interaction.followup.send(
                    f"❌ Invalid Solana address format: `{address}`", 
                    ephemeral=True
                )
                return
            
            # Build trading parameters dictionary with validation
            trading_params = {}
            
            # Buy amount validation
            if buy_amount_sol is not None:
                if buy_amount_sol <= 0:
                    await interaction.followup.send(
                        "❌ Buy amount must be greater than 0 SOL", 
                        ephemeral=True
                    )
                    return
                trading_params["buy_amount"] = buy_amount_sol
            
            # Buy slippage validation (convert percentage to decimal)
            if buy_slippage_percent is not None:
                if buy_slippage_percent < 0 or buy_slippage_percent > 100:
                    await interaction.followup.send(
                        "❌ Buy slippage must be between 0% and 100%", 
                        ephemeral=True
                    )
                    return
                trading_params["buy_slippage"] = buy_slippage_percent / 100.0
            
            # Priority fee validation and conversion (SOL to microlamports)
            if priority_fee_sol is not None:
                if priority_fee_sol < 0:
                    await interaction.followup.send(
                        "❌ Priority fee cannot be negative", 
                        ephemeral=True
                    )
                    return
                # Convert SOL to microlamports (1 SOL = 1,000,000 microlamports)
                # Note: Priority fees are stored in microlamports for set_compute_unit_price
                trading_params["priority_fee"] = int(priority_fee_sol * 1_000_000)
            
            # Tip amount validation and conversion (SOL to lamports)
            if tip_amount_sol is not None:
                if tip_amount_sol < 0:
                    await interaction.followup.send(
                        "❌ Tip amount cannot be negative", 
                        ephemeral=True
                    )
                    return
                # Convert SOL to lamports (1 SOL = 1,000,000,000 lamports)
                trading_params["tip_amount"] = int(tip_amount_sol * 1_000_000_000)
            
            # Take profit validation (convert percentage to decimal)
            if take_profit_percent is not None:
                if take_profit_percent <= 0:
                    await interaction.followup.send(
                        "❌ Take profit percentage must be greater than 0%", 
                        ephemeral=True
                    )
                    return
                trading_params["take_profit_percentage"] = take_profit_percent / 100.0
            
            # Sell percentage validation (convert percentage to decimal)
            if sell_percent is not None:
                if sell_percent <= 0 or sell_percent > 100:
                    await interaction.followup.send(
                        "❌ Sell percentage must be between 0% and 100%", 
                        ephemeral=True
                    )
                    return
                trading_params["percent_sell_amount"] = sell_percent / 100.0
            
            # Token amount validation
            if token_amount is not None:
                if token_amount <= 0:
                    await interaction.followup.send(
                        "❌ Token amount must be greater than 0", 
                        ephemeral=True
                    )
                    return
                trading_params["token_amount"] = token_amount
            
            # Add developer to whitelist
            success = await self.bot.developer_manager.add_developer(address, trading_params)
            
            if success:
                # Send notification
                if self.bot.discord_notifier:
                    await notify_manual_snipe_add(
                        self.bot.discord_notifier,
                        address,
                        trading_params,
                        f"{interaction.user.display_name}#{interaction.user.discriminator}"
                    )
                
                # Create detailed response
                response = f"✅ **Successfully added developer to snipe whitelist**\n\n"
                response += f"**Address:** `{address}`\n"
                
                if trading_params:
                    response += f"\n**Trading Parameters:**\n"
                    response += self.bot.format_parameter_display(trading_params)
                else:
                    response += "\n*No trading parameters specified - will use default settings*"
                    
                await interaction.followup.send(response)
            else:
                await interaction.followup.send(
                    f"⚠️ Developer `{address}` already exists in whitelist", 
                    ephemeral=True
                )
                
        except Exception as e:
            logger.error(f"Error adding developer {address}: {e}")
            await interaction.followup.send(
                f"❌ Error adding developer: {str(e)}", 
                ephemeral=True
            )
            
    @app_commands.command(name="remove_snipe", description="Remove a developer from the snipe whitelist")
    @app_commands.describe(address="Developer's Solana address to remove")
    async def remove_snipe(self, interaction: discord.Interaction, address: str):
        """Remove a developer from the snipe whitelist."""
        await interaction.response.defer()
        
        try:
            # Validate address format
            if not self.bot.is_valid_solana_address(address):
                await interaction.followup.send(
                    f"❌ Invalid Solana address format: `{address}`", 
                    ephemeral=True
                )
                return
                
            # Remove developer from whitelist
            success = await self.bot.developer_manager.remove_developer(address)
            
            # Send notification
            if self.bot.discord_notifier:
                await notify_manual_snipe_remove(
                    self.bot.discord_notifier,
                    address,
                    f"{interaction.user.display_name}#{interaction.user.discriminator}",
                    success
                )
            
            if success:
                await interaction.followup.send(f"✅ Successfully removed developer `{address}` from snipe whitelist")
            else:
                await interaction.followup.send(f"⚠️ Developer `{address}` not found in whitelist")
                
        except Exception as e:
            logger.error(f"Error removing developer {address}: {e}")
            await interaction.followup.send(
                f"❌ Error removing developer: {str(e)}", 
                ephemeral=True
            )
            
    @app_commands.command(name="list_snipes", description="List developers in the snipe whitelist")
    @app_commands.describe(limit="Maximum number of developers to show (default: 20)")
    async def list_snipes(self, interaction: discord.Interaction, limit: Optional[int] = 20):
        """List developers in the snipe whitelist."""
        await interaction.response.defer()
        
        try:
            # Validate limit
            if limit and (limit < 1 or limit > 50):
                await interaction.followup.send(
                    "❌ Limit must be between 1 and 50", 
                    ephemeral=True
                )
                return
                
            # Get developers from whitelist
            developers = await self.bot.developer_manager.list_developers(limit)
            total_count = len(self.bot.developer_manager.developer_whitelist)
            
            # Send notification
            if self.bot.discord_notifier:
                await notify_snipe_list(
                    self.bot.discord_notifier,
                    developers,
                    total_count,
                    f"{interaction.user.display_name}#{interaction.user.discriminator}",
                    limit
                )
            
            # Send response to user
            if not developers:
                await interaction.followup.send("📋 No developers currently in snipe whitelist")
            else:
                response = f"📋 **Developer Snipe List** ({len(developers)} shown, {total_count} total)\n\n"
                
                for i, dev in enumerate(developers[:10], 1):  # Limit to 10 for direct response
                    address = dev["address"]
                    age_hours = dev["age_hours"]
                    params = dev.get("params", {})
                    
                    # Format age
                    if age_hours < 1:
                        age_str = f"{age_hours * 60:.0f}m"
                    elif age_hours < 24:
                        age_str = f"{age_hours:.1f}h"
                    else:
                        age_str = f"{age_hours / 24:.1f}d"
                    
                    response += f"`{i}.` `{address}` (Age: {age_str})\n"
                    
                    # Add key parameters if they exist
                    if params:
                        # Show only key parameters in the list view for brevity
                        param_info = []
                        if "token_amount" in params:
                            param_info.append(f"Token: {params['token_amount']}")
                        if "buy_amount" in params:
                            param_info.append(f"Buy: {params['buy_amount']:.2f} SOL")
                        if "buy_slippage" in params:
                            param_info.append(f"Slippage: {params['buy_slippage']*100:.1f}%")
                        if "priority_fee" in params:
                            priority_sol = params['priority_fee'] / 1_000_000
                            param_info.append(f"Priority: {priority_sol:.6f} SOL")
                        if "tip_amount" in params:
                            tip_sol = params['tip_amount'] / 1_000_000_000
                            param_info.append(f"Tip: {tip_sol:.6f} SOL")
                        if "take_profit_percentage" in params:
                            param_info.append(f"Take Profit: {params['take_profit_percentage']*100:.1f}%")
                        if "percent_sell_amount" in params:
                            param_info.append(f"Sell: {params['percent_sell_amount']*100:.1f}%")
                        
                        if param_info:
                            response += f"    {' | '.join(param_info)}\n"
                    
                    response += "\n"
                
                if len(developers) > 10:
                    response += f"... and {len(developers) - 10} more (see notification for full list)"
                
                await interaction.followup.send(response)
                
        except Exception as e:
            logger.error(f"Error listing developers: {e}")
            await interaction.followup.send(
                f"❌ Error listing developers: {str(e)}", 
                ephemeral=True
            )
            
    @app_commands.command(name="snipe_stats", description="Show statistics about the snipe whitelist")
    async def snipe_stats(self, interaction: discord.Interaction):
        """Show statistics about the snipe whitelist."""
        await interaction.response.defer()
        
        try:
            stats = self.bot.developer_manager.get_stats()
            developers = await self.bot.developer_manager.list_developers()
            
            # Calculate additional stats
            if developers:
                ages_hours = [dev["age_hours"] for dev in developers]
                avg_age = sum(ages_hours) / len(ages_hours)
                newest_age = min(ages_hours)
                oldest_age = max(ages_hours)
                
                # Count developers with parameters
                devs_with_params = sum(1 for dev in developers if dev.get("params"))
            else:
                avg_age = newest_age = oldest_age = 0
                devs_with_params = 0
            
            response = f"""📊 **Snipe Whitelist Statistics**

**Total Developers:** {stats['whitelist_count']}
**Manager Status:** {'🟢 Running' if stats['running'] else '🔴 Stopped'}
**Max Capacity:** {self.bot.developer_manager.max_developers}
**Max Age:** {self.bot.developer_manager.max_age_days} days

**Developer Stats:**
• With Parameters: {devs_with_params}
• Average Age: {avg_age:.1f} hours
• Newest: {newest_age:.1f} hours
• Oldest: {oldest_age:.1f} hours

**Refresh Interval:** {self.bot.developer_manager.refresh_interval / 60:.0f} minutes
**Save Interval:** {self.bot.developer_manager.save_interval / 60:.0f} minutes
"""
            
            await interaction.followup.send(response)
            
        except Exception as e:
            logger.error(f"Error getting snipe stats: {e}")
            await interaction.followup.send(
                f"❌ Error getting statistics: {str(e)}", 
                ephemeral=True
            )

    @app_commands.command(name="search_snipe", description="Search for a developer in the snipe whitelist")
    @app_commands.describe(query="Developer address (full or partial, minimum 8 characters)")
    async def search_snipe(self, interaction: discord.Interaction, query: str):
        """Search for a developer in the snipe whitelist."""
        await interaction.response.defer()
        
        try:
            # Validate query length for partial searches
            if len(query) < 8:
                await interaction.followup.send(
                    "❌ Search query must be at least 8 characters long", 
                    ephemeral=True
                )
                return
                
            # Search for developer
            developer_info = await self.bot.developer_manager.search_developer(query)
            
            # Send notification
            if self.bot.discord_notifier:
                await notify_snipe_search(
                    self.bot.discord_notifier,
                    query,
                    developer_info,
                    f"{interaction.user.display_name}#{interaction.user.discriminator}"
                )
            
            # Send response to user
            if developer_info:
                address = developer_info["address"]
                age_hours = developer_info["age_hours"]
                params = developer_info.get("params", {})
                match_type = developer_info.get("match_type", "exact")
                
                # Format age
                if age_hours < 1:
                    age_str = f"{age_hours * 60:.0f}m"
                elif age_hours < 24:
                    age_str = f"{age_hours:.1f}h"
                else:
                    age_str = f"{age_hours / 24:.1f}d"
                
                response = f"🔍 **Developer Found** ({match_type} match)\n\n"
                response += f"**Address:** `{address}`\n"
                response += f"**Age:** {age_str}\n"
                
                # Add parameters if they exist
                if params:
                    response += f"\n**Trading Parameters:**\n"
                    response += self.bot.format_parameter_display(params)
                
                await interaction.followup.send(response)
            else:
                await interaction.followup.send(f"❌ No developer matching `{query}` found in snipe whitelist")
                
        except Exception as e:
            logger.error(f"Error searching for developer {query}: {e}")
            await interaction.followup.send(
                f"❌ Error searching for developer: {str(e)}", 
                ephemeral=True
            )


async def start_discord_bot(
    bot_token: str,
    developer_manager: DeveloperManager,
    discord_notifier: Optional[DiscordNotifier] = None
) -> DeveloperManagementBot:
    """Start the Discord bot.
    
    Args:
        bot_token: Discord bot token
        developer_manager: DeveloperManager instance
        discord_notifier: Optional Discord notifier
        
    Returns:
        DeveloperManagementBot instance
    """
    # Create bot instance
    bot = DeveloperManagementBot(developer_manager, discord_notifier)
    
    # Add the cog
    await bot.add_cog(DeveloperCommands(bot))
    
    # Start the bot
    await bot.start(bot_token)
    
    return bot


async def run_discord_bot(
    bot_token: str,
    developer_manager: DeveloperManager,
    discord_notifier: Optional[DiscordNotifier] = None
):
    """Run the Discord bot (blocking).
    
    Args:
        bot_token: Discord bot token
        developer_manager: DeveloperManager instance
        discord_notifier: Optional Discord notifier
    """
    try:
        bot = await start_discord_bot(bot_token, developer_manager, discord_notifier)
        logger.info("Discord bot started successfully")
    except Exception as e:
        logger.error(f"Failed to start Discord bot: {e}")
        raise
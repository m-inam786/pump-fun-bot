"""
Developer management system for filtering tokens by creator addresses.
Manages a whitelist of developers fetched from PostgreSQL with efficient in-memory lookup.
"""

import asyncio
import json
import os
import time
from typing import Dict, List, Optional, Set, Any, Tuple

import asyncpg
from solders.pubkey import Pubkey
from utils.logger import get_logger
from utils.discord_notifications import (
    DiscordNotifier,
    notify_bulk_snipe_add,
    notify_snipe_expire
)
from templates.buy_tx import BuyTxBuilder

logger = get_logger(__name__)


class DeveloperManager:
    """Manages a whitelist of developers to monitor for token creation."""

    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
        db_query: str,
        refresh_interval: int = 1800,  # 30 minutes
        max_developers: int = 1000,
        max_age_days: int = 1,
        persisted_whitelist_filepath: str = "data/developer_whitelist.json",
        discord_notifier: Optional[DiscordNotifier] = None,
        tip_destination: Pubkey | None = None,
        fetch_from_db: bool = True,  # Flag to control database fetching
    ):
        """Initialize the developer manager.

        Args:
            db_host: PostgreSQL host
            db_port: PostgreSQL port
            db_name: PostgreSQL database name
            db_user: PostgreSQL username
            db_password: PostgreSQL password
            db_query: Path to a file containing a SQL query to fetch developers
            refresh_interval: Time between refreshes in seconds
            max_developers: Maximum number of developers to store in memory
            max_age_days: Maximum age of developers in days before removal
            persisted_whitelist_filepath: Path to file for persisting the developer whitelist
            discord_notifier: Optional Discord notifier for notifications
            tip_destination: Tip destination pubkey for prestored templates
            fetch_from_db: Whether to fetch developers from database (True) or only use manually added developers (False)
        """
        # Database connection parameters
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_query = self._load_db_query(db_query)
        self.db_pool = None  # Will be initialized when start() is called

        # Developer list management parameters
        self.refresh_interval = refresh_interval
        self.max_developers = max_developers
        self.max_age_seconds = max_age_days * 86400 # Initialize max_age_seconds
        self.max_age_days = max_age_days
        self.persisted_whitelist_filepath = persisted_whitelist_filepath
        self.save_interval = 300  # Save to file every 5 minutes
        
        # Tip destination
        self.tip_destination = tip_destination
        
        # Discord notifications
        self.discord_notifier = discord_notifier

        # Data structures for O(1) lookup
        # New structure: address -> {timestamp: float, params: {buy_amount, slippage, etc}, prestored_tx: [Instructions]}
        self.developer_whitelist: Dict[str, Dict[str, Any]] = {}
        
        # Ensure persisted whitelist directory exists
        os.makedirs(os.path.dirname(self.persisted_whitelist_filepath), exist_ok=True)

        # Locks for thread safety
        self.whitelist_lock = asyncio.Lock()  # For whitelist operations (critical path)
        self.file_lock = asyncio.Lock()       # For file operations
        
        # Tasks for background operations
        self.refresh_task = None
        self.save_task = None
        self.running = False

        # Flag to control database fetching
        self.fetch_from_db = fetch_from_db

    async def start(self) -> None:
        """Start the developer manager and periodic refresh task."""
        if self.running:
            return
            
        logger.info("Starting developer manager...")
        
        # Create database connection pool
        self.db_pool = await asyncpg.create_pool(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )
        
        # Load previously persisted whitelist
        await self._load_persisted_whitelist()
        
        # Initial fetch
        try:
            await self.refresh_developers()
        except Exception as e:
            logger.error(f"Initial developer fetch failed: {e}")
        
        # Start periodic refresh and save tasks
        self.running = True
        self.refresh_task = asyncio.create_task(self._periodic_refresh())
        self.save_task = asyncio.create_task(self._periodic_save())
        logger.info(f"Developer manager started. Refreshing every {self.refresh_interval} seconds. Saving every {self.save_interval} seconds.")

    async def stop(self) -> None:
        """Stop the developer manager and cleanup resources."""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel background tasks
        if self.refresh_task:
            self.refresh_task.cancel()
            try:
                await self.refresh_task
            except asyncio.CancelledError:
                pass
                
        if self.save_task:
            self.save_task.cancel()
            try:
                await self.save_task
            except asyncio.CancelledError:
                pass
                
        # Final save to ensure we don't lose any data
        await self._save_persisted_whitelist()
        
        # Close database pool
        if self.db_pool:
            await self.db_pool.close()
            
        logger.info("Developer manager stopped.")

    def _load_db_query(self, db_query_path: str) -> str:
        """Load the SQL query from a file."""
        if db_query_path:
            if not os.path.exists(db_query_path):
                raise FileNotFoundError(f"Developer manager SQL file not found: {db_query_path}")
            with open(db_query_path, "r") as f:
                sql = f.read()
                return sql
        raise ValueError("No SQL query path provided")

    async def _periodic_refresh(self) -> None:
        """Periodically refresh the developer list in the background."""
        try:
            while self.running:
                await asyncio.sleep(self.refresh_interval)
                try:
                    # This operation is isolated from the critical path
                    await self.refresh_developers()
                except Exception as e:
                    logger.error(f"Error refreshing developers: {e}")
        except asyncio.CancelledError:
            logger.info("Developer refresh task cancelled.")
            raise

    async def _periodic_save(self) -> None:
        """Periodically save the developer whitelist in the background."""
        try:
            while self.running:
                await asyncio.sleep(self.save_interval)
                try:
                    # Save the current whitelist
                    await self._save_persisted_whitelist()
                except Exception as e:
                    logger.error(f"Error saving developer whitelist: {e}")
        except asyncio.CancelledError:
            logger.info("Developer whitelist save task cancelled.")
            raise

    async def refresh_developers(self) -> None:
        """Fetch developers from the database and update the whitelist.
        
        The query is expected to return at least one column (developer address),
        but can also return additional columns for trading parameters in this order:
        - developer_address (required) - developer public key
        - buy_amount (optional, float) - amount of SOL to spend
        - buy_slippage (optional, float) - slippage tolerance
        - priority_fee (optional, int) - priority fee in microlamports
        - tip_amount (optional, int) - zero slot tip amount in lamports
        - token_amount (optional, int) - token amount for extreme fast mode
        """
        if not self.db_pool:
            logger.error("Database pool not initialized")
            return
            
        try:
            now = time.time()
            
            # 1. Initialize developers dict - either from DB or empty for manual-only mode
            db_developers: Dict[str, Dict[str, Any]] = {}
            
            if self.fetch_from_db:
                # Fetch developers and their parameters from DB
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch(self.db_query)
                    for row in rows:
                        # First column is always the developer address
                        developer = str(row[0])
                        
                        # Initialize with timestamp and empty params dict
                        dev_data = {"timestamp": now, "params": {}}
                        
                        # Extract additional parameters if available in the query results
                        param_names = ["buy_amount", "buy_slippage", "priority_fee", "tip_amount", "token_amount", 
                                      "percent_sell_amount", "take_profit_percentage"]
                        
                        # Check row length to determine available parameters
                        for i, param_name in enumerate(param_names, start=1):
                            if len(row) > i and row[i] is not None:
                                # Store parameter in the params dictionary
                                dev_data["params"][param_name] = row[i]
                        
                        # Store developer with parameters
                        db_developers[developer] = dev_data
                        
                        # Log if we found parameters
                        if dev_data["params"]:
                            logger.info(f"Found parameters for developer {developer}: {dev_data['params']}")
                            
                logger.info(f"Fetched {len(db_developers)} developers from database")
            else:
                logger.info("Database fetching disabled - using only manually added developers")

            async with self.whitelist_lock:
                current_whitelist = self.developer_whitelist.copy()
                
                # 2. Clean stale developers from the current whitelist
                cleaned_whitelist: Dict[str, Dict[str, Any]] = {}
                for dev, dev_data in current_whitelist.items():
                    timestamp = dev_data.get("timestamp", 0)
                    if (now - timestamp) <= self.max_age_seconds:
                        cleaned_whitelist[dev] = dev_data
                    else:
                        logger.debug(f"Removing stale developer {dev} from whitelist (age: {now-timestamp}s)")
                        # Send notification for expired developer
                        if self.discord_notifier:
                            age_days = (now - timestamp) / 86400  # Convert seconds to days
                            asyncio.create_task(
                                notify_snipe_expire(
                                    self.discord_notifier,
                                    dev,
                                    age_days,
                                    f"Developer exceeded maximum age of {self.max_age_days} days"
                                )
                            )
                
                # 3. Add/Update new developers from DB and create prestored templates
                # Keep track of newly added developers
                new_developers = []
                for dev, dev_data in db_developers.items():
                    if dev not in cleaned_whitelist:
                        new_developers.append(dev)
                    else:
                        # For existing developers, update timestamp but preserve params if none in DB
                        if not dev_data["params"] and "params" in cleaned_whitelist[dev]:
                            dev_data["params"] = cleaned_whitelist[dev]["params"]
                    
                    # Create prestored transaction template for this developer
                    dev_data["prestored_tx"] = self._create_prestored_template(dev_data["params"])
                    cleaned_whitelist[dev] = dev_data

                # 4. Enforce max_developers limit, prioritizing newest
                if len(cleaned_whitelist) > self.max_developers:
                    # Sort by timestamp (descending for newest) then address for tie-breaking
                    sorted_devs = sorted(
                        cleaned_whitelist.items(), 
                        key=lambda x: (x[1].get("timestamp", 0), x[0]), 
                        reverse=True
                    )
                    self.developer_whitelist = dict(sorted_devs[:self.max_developers])
                else:
                    self.developer_whitelist = cleaned_whitelist
                
                if self.fetch_from_db:
                    logger.info(f"Refreshed developer whitelist: {len(self.developer_whitelist)} active developers. Fetched {len(db_developers)} from DB.")
                    logger.info(f"Created {len(db_developers)} prestored transaction templates for ultra-fast sniping.")
                else:
                    logger.info(f"Refreshed developer whitelist: {len(self.developer_whitelist)} active developers (manual-only mode).")
                    logger.info(f"Created {len(self.developer_whitelist)} prestored transaction templates for ultra-fast sniping.")
                
                # Send notifications for newly added developers
                if new_developers and len(new_developers) > 0:
                    logger.info(f"Added {len(new_developers)} new developers to whitelist")
                    if self.discord_notifier:
                        # Prepare developer data with their configs
                        dev_configs = []
                        for dev in new_developers:
                            config = db_developers.get(dev, {}).get("params", {})
                            
                            # Format values for readability
                            formatted_config = {}
                            for key, value in config.items():
                                if key == "priority_fee" and value is not None:
                                    # Convert microlamports to SOL
                                    formatted_value = f"{value / 1_000_000_000_000:.5f} SOL" 
                                elif key == "tip_amount" and value is not None:
                                    # Convert lamports to SOL
                                    formatted_value = f"{value / 1_000_000_000:.5f} SOL"
                                elif key == "buy_amount" and value is not None:
                                    # Format SOL amount with proper precision
                                    formatted_value = f"{value:.2f} SOL"
                                elif key == "buy_slippage" and value is not None:
                                    # Format percentage
                                    formatted_value = f"{value*100:.1f}%"
                                elif key == "take_profit_percentage" and value is not None:
                                    # Format percentage
                                    formatted_value = f"{value*100:.1f}%"
                                elif key == "percent_sell_amount" and value is not None:
                                    # Format percentage
                                    formatted_value = f"{value*100:.1f}%"
                                elif key == "token_amount" and value is not None:
                                    # Format token amount in M, K, B, T
                                    if value >= 1_000_000_000_000:
                                        formatted_value = f"{value / 1_000_000_000_000:.2f}T"
                                    elif value >= 1_000_000_000:
                                        formatted_value = f"{value / 1_000_000_000:.2f}B"
                                    elif value >= 1_000_000:
                                        formatted_value = f"{value / 1_000_000:.2f}M"
                                    else:
                                        formatted_value = str(value)
                                else:
                                    formatted_value = str(value)
                                
                                formatted_config[key] = formatted_value
                            
                            dev_configs.append({"address": dev, "config": formatted_config})
                            
                        # split list into chunks of 20
                        for i in range(0, len(dev_configs), 20):
                            chunk = dev_configs[i:i+20]
                            asyncio.create_task(
                                notify_bulk_snipe_add(
                                    self.discord_notifier, 
                                    chunk, 
                                    extra_info={"Total Snipes": len(self.developer_whitelist)}
                                )
                            )
                            # sleep for 2 seconds to avoid hitting discord rate limits
                            await asyncio.sleep(2)
                
        except Exception as e:
            logger.error(f"Error fetching/refreshing developers from database: {e}")
            if isinstance(e, asyncpg.exceptions.UndefinedColumnError):
                logger.error("This error might indicate your SQL query doesn't return the expected columns. Check your query.")
                
    async def mark_as_sniped(self, developer_address: str | Pubkey) -> None:
        """Mark a developer as sniped by removing them from the whitelist.

        Args:
            developer_address: Developer address to mark
        """
        # Remove from whitelist
        developer_address = str(developer_address)
        async with self.whitelist_lock:
            if developer_address in self.developer_whitelist:
                del self.developer_whitelist[developer_address]
                logger.info(f"Developer {developer_address} marked as sniped (removed from whitelist)")
            else:
                logger.info(f"Developer {developer_address} was not in whitelist to mark as sniped.")

    async def add_developer(self, developer_address: str | Pubkey, params: Optional[Dict[str, Any]] = None) -> bool:
        """Manually add a developer to the whitelist (e.g., through Discord bot).

        Args:
            developer_address: Developer address to add
            params: Optional trading parameters for the developer

        Returns:
            True if developer was added, False if already exists
        """
        developer_address = str(developer_address)
        now = time.time()
        
        async with self.whitelist_lock:
            if developer_address in self.developer_whitelist:
                logger.info(f"Developer {developer_address} already exists in whitelist")
                return False
            
            # Create new developer entry
            dev_data = {
                "timestamp": now,
                "params": params or {}
            }
            
            # Create prestored transaction template
            dev_data["prestored_tx"] = self._create_prestored_template(dev_data["params"])
            
            # Add to whitelist
            self.developer_whitelist[developer_address] = dev_data
            
            # Check if we exceed max_developers limit
            if len(self.developer_whitelist) > self.max_developers:
                # Remove oldest developer
                oldest_dev = min(
                    self.developer_whitelist.items(),
                    key=lambda x: x[1].get("timestamp", 0)
                )
                del self.developer_whitelist[oldest_dev[0]]
                logger.info(f"Removed oldest developer {oldest_dev[0]} due to max_developers limit")
            
            logger.info(f"Manually added developer {developer_address} to whitelist with params: {params}")
            return True

    async def remove_developer(self, developer_address: str | Pubkey) -> bool:
        """Manually remove a developer from the whitelist (e.g., through Discord bot).

        Args:
            developer_address: Developer address to remove

        Returns:
            True if developer was removed, False if not found
        """
        developer_address = str(developer_address)
        async with self.whitelist_lock:
            if developer_address in self.developer_whitelist:
                del self.developer_whitelist[developer_address]
                logger.info(f"Manually removed developer {developer_address} from whitelist")
                return True
            else:
                logger.info(f"Developer {developer_address} not found in whitelist for removal")
                return False

    async def list_developers(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get a list of all developers in the whitelist with their details.

        Args:
            limit: Optional limit on number of developers to return

        Returns:
            List of dictionaries containing developer info
        """
        async with self.whitelist_lock:
            developers = []
            
            # Sort by timestamp (newest first)
            sorted_devs = sorted(
                self.developer_whitelist.items(),
                key=lambda x: x[1].get("timestamp", 0),
                reverse=True
            )
            
            # Apply limit if specified
            if limit:
                sorted_devs = sorted_devs[:limit]
            
            for address, dev_data in sorted_devs:
                timestamp = dev_data.get("timestamp", 0)
                params = dev_data.get("params", {})
                
                developers.append({
                    "address": address,
                    "timestamp": timestamp,
                    "age_hours": (time.time() - timestamp) / 3600,
                    "params": params
                })
            
            return developers

    async def search_developer(self, search_query: str | Pubkey) -> Optional[Dict[str, Any]]:
        """Search for a specific developer in the whitelist.

        Args:
            search_query: Developer address (exact match) or partial address to search for

        Returns:
            Dictionary containing developer info if found, None otherwise
        """
        search_query = str(search_query)
        
        async with self.whitelist_lock:
            # First try exact match
            if search_query in self.developer_whitelist:
                dev_data = self.developer_whitelist[search_query]
                timestamp = dev_data.get("timestamp", 0)
                params = dev_data.get("params", {})
                
                return {
                    "address": search_query,
                    "timestamp": timestamp,
                    "age_hours": (time.time() - timestamp) / 3600,
                    "params": params,
                    "match_type": "exact"
                }
            
            # If no exact match, try partial match (for convenience)
            # Only if search query is at least 8 characters to avoid too many matches
            if len(search_query) >= 8:
                partial_matches = []
                for address, dev_data in self.developer_whitelist.items():
                    if search_query.lower() in address.lower():
                        timestamp = dev_data.get("timestamp", 0)
                        params = dev_data.get("params", {})
                        
                        partial_matches.append({
                            "address": address,
                            "timestamp": timestamp,
                            "age_hours": (time.time() - timestamp) / 3600,
                            "params": params,
                            "match_type": "partial"
                        })
                
                # Return the first partial match if found
                if partial_matches:
                    # Sort by timestamp (newest first) and return the first one
                    partial_matches.sort(key=lambda x: x["timestamp"], reverse=True)
                    return partial_matches[0]
            
            return None

    async def set_developer_params(self, developer_address: str | Pubkey, params: Dict[str, Any]) -> bool:
        """Set trading parameters for a specific developer.

        Args:
            developer_address: Developer address to set parameters for
            params: Dictionary with trading parameters

        Returns:
            True if parameters were set, False if developer not found
        """
        developer_address = str(developer_address)
        async with self.whitelist_lock:
            if developer_address in self.developer_whitelist:
                if "params" not in self.developer_whitelist[developer_address]:
                    self.developer_whitelist[developer_address]["params"] = {}
                self.developer_whitelist[developer_address]["params"].update(params)
                logger.info(f"Updated trading parameters for developer {developer_address}: {params}")
                return True
            logger.warning(f"Attempted to set parameters for non-whitelisted developer {developer_address}")
            return False

    async def get_developer_params(self, developer_address: str | Pubkey) -> Dict[str, Any]:
        """Get trading parameters for a specific developer.

        Note: This method uses locks for safety but is slower than direct access.
        For the critical path, the BaseTokenListener accesses the whitelist directly
        without using this method to eliminate function call overhead.
        
        Args:
            developer_address: Developer address to get parameters for

        Returns:
            Dictionary with trading parameters or empty dict if developer not found
        """
        developer_address = str(developer_address)
        async with self.whitelist_lock:
            if developer_address in self.developer_whitelist:
                return self.developer_whitelist[developer_address].get("params", {})
            return {}

    async def _load_persisted_whitelist(self) -> None:
        """Load the developer whitelist from file."""
        try:
            if os.path.exists(self.persisted_whitelist_filepath):
                async with self.file_lock:
                    with open(self.persisted_whitelist_filepath, 'r') as f:
                        # Load the data and convert if needed
                        loaded_data = json.load(f)
                        converted_data = {}
                        
                        # Handle legacy format (string -> timestamp) conversion
                        if loaded_data and isinstance(loaded_data, dict):
                            for dev, value in loaded_data.items():
                                if isinstance(value, (int, float)):
                                    # Legacy format - convert to new format
                                    converted_data[dev] = {"timestamp": value, "params": {}}
                                elif isinstance(value, dict) and "timestamp" in value:
                                    # Already in new format - exclude prestored_tx as it's not JSON serializable
                                    dev_data = {
                                        "timestamp": value["timestamp"],
                                        "params": value.get("params", {})
                                    }
                                    # Recreate prestored template from params
                                    dev_data["prestored_tx"] = self._create_prestored_template(dev_data["params"])
                                    converted_data[dev] = dev_data
                                else:
                                    logger.warning(f"Unrecognized format for developer {dev}: {value}. Skipping.")
                        else:
                            logger.warning(f"Persisted whitelist file {self.persisted_whitelist_filepath} does not contain a valid dictionary. Initializing empty whitelist.")
                    
                async with self.whitelist_lock:
                    self.developer_whitelist = converted_data
                    
                logger.info(f"Loaded {len(self.developer_whitelist)} developers into whitelist from {self.persisted_whitelist_filepath}")
                logger.info(f"Recreated {len(self.developer_whitelist)} prestored transaction templates")
            else:
                logger.info(f"Whitelist file {self.persisted_whitelist_filepath} not found. Starting with an empty whitelist.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {self.persisted_whitelist_filepath}: {e}. Initializing empty whitelist.")
            async with self.whitelist_lock:
                self.developer_whitelist = {}
        except Exception as e:
            logger.error(f"Error loading persisted whitelist: {e}. Initializing empty whitelist.")
            async with self.whitelist_lock:
                self.developer_whitelist = {}


    async def _save_persisted_whitelist(self) -> None:
        """Save the current developer whitelist to file (excluding prestored templates)."""
        try:
            # Get a snapshot of the current whitelist
            async with self.whitelist_lock:
                # Create a copy to avoid issues if it's modified during dump
                whitelist_to_save = {}
                for dev, dev_data in self.developer_whitelist.items():
                    # Only save JSON-serializable data (exclude prestored_tx)
                    whitelist_to_save[dev] = {
                        "timestamp": dev_data.get("timestamp", 0),
                        "params": dev_data.get("params", {})
                        # prestored_tx is excluded as it's not JSON serializable
                    }
            
            # Write to file (potentially slow I/O operation)
            async with self.file_lock:
                with open(self.persisted_whitelist_filepath, 'w') as f:
                    json.dump(whitelist_to_save, f, indent=4) # Added indent for readability
                    
            logger.info(f"Saved {len(whitelist_to_save)} developers from whitelist to {self.persisted_whitelist_filepath}")
            logger.debug("Note: Prestored transaction templates are excluded from persistence and will be recreated on startup")
        except Exception as e:
            logger.error(f"Error saving persisted whitelist: {e}")
            
    # For testing and monitoring purposes
    def get_stats(self) -> dict:
        """Get statistics about the developer manager state.
        
        Returns:
            Dictionary containing stats about whitelist
        """
        return {
            "whitelist_count": len(self.developer_whitelist),
            "running": self.running
        } 

    def _create_prestored_template(self, params: Dict[str, Any]) -> list:
        """Create a prestored transaction template for a developer with their specific parameters.
        
        Args:
            params: Developer-specific trading parameters
            
        Returns:
            List of prestored instructions ready for ultra-fast filling
        """
        try:
            # Use developer-specific parameters with defaults
            buy_amount = params.get("buy_amount", 0.1)
            buy_slippage = params.get("buy_slippage", 0.05)
            priority_fee = params.get("priority_fee", 50000)
            tip_amount = params.get("tip_amount", None)
            tip_destination = self.tip_destination
            token_amount = params.get("token_amount", 1000000)
            
            # Create prestored template with developer's parameters
            prestored_template = BuyTxBuilder.get_prestored_tx_template(
                token_amount=token_amount,
                max_amount_lamports=int(buy_amount * 1_000_000_000 * (1 + buy_slippage)),  # SOL to lamports with slippage
                priority_fee_microlamports=priority_fee,
                tip_amount_lamports=tip_amount,
                tip_destination=tip_destination
            )
            
            logger.debug(f"Created prestored template with params: buy_amount={buy_amount}, slippage={buy_slippage}, priority_fee={priority_fee}")
            return prestored_template
            
        except Exception as e:
            logger.error(f"Error creating prestored template: {e}")
            # Return empty list as fallback
            return []
"""
Developer management system for filtering tokens by creator addresses.
Manages a whitelist of developers fetched from PostgreSQL with efficient in-memory lookup.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import asyncpg
from solders.pubkey import Pubkey

from utils.logger import get_logger
from utils.discord_notifications import (
    DiscordNotifier,
    notify_snipe_add,
    notify_snipe_expire
)

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
        
        # Discord notifications
        self.discord_notifier = discord_notifier

        # Data structures for O(1) lookup
        self.developer_whitelist: Dict[str, float] = {}  # address -> timestamp
        
        # Ensure persisted whitelist directory exists
        os.makedirs(os.path.dirname(self.persisted_whitelist_filepath), exist_ok=True)

        # Locks for thread safety
        self.whitelist_lock = asyncio.Lock()  # For whitelist operations (critical path)
        self.file_lock = asyncio.Lock()       # For file operations
        
        # Tasks for background operations
        self.refresh_task = None
        self.save_task = None
        self.running = False

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
        """Fetch developers from the database and update the whitelist."""
        if not self.db_pool:
            logger.error("Database pool not initialized")
            return
            
        try:
            now = time.time()
            
            # 1. Fetch new developers from DB
            db_developers: Dict[str, float] = {}
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(self.db_query)
                for row in rows:
                    developer = str(row[0]) 
                    db_developers[developer] = now # Timestamp them as current

            async with self.whitelist_lock:
                current_whitelist = self.developer_whitelist.copy()
                
                # 2. Clean stale developers from the current whitelist
                cleaned_whitelist: Dict[str, float] = {}
                for dev, ts in current_whitelist.items():
                    if (now - ts) <= self.max_age_seconds:
                        cleaned_whitelist[dev] = ts
                    else:
                        logger.debug(f"Removing stale developer {dev} from whitelist (age: {now-ts}s)")
                        # Send notification for expired developer
                        if self.discord_notifier:
                            age_days = (now - ts) / 86400  # Convert seconds to days
                            asyncio.create_task(
                                notify_snipe_expire(
                                    self.discord_notifier,
                                    dev,
                                    age_days,
                                    f"Developer exceeded maximum age of {self.max_age_days} days"
                                )
                            )
                
                # 3. Add/Update new developers from DB
                # Keep track of newly added developers
                new_developers = []
                for dev, ts in db_developers.items():
                    if dev not in cleaned_whitelist:
                        new_developers.append(dev)
                    cleaned_whitelist[dev] = ts 

                # 4. Enforce max_developers limit, prioritizing newest
                if len(cleaned_whitelist) > self.max_developers:
                    # Sort by timestamp (descending for newest) then address for tie-breaking
                    sorted_devs = sorted(
                        cleaned_whitelist.items(), 
                        key=lambda x: (x[1], x[0]), 
                        reverse=True
                    )
                    self.developer_whitelist = dict(sorted_devs[:self.max_developers])
                else:
                    self.developer_whitelist = cleaned_whitelist
                
                logger.info(f"Refreshed developer whitelist: {len(self.developer_whitelist)} active developers. Fetched {len(db_developers)} from DB.")
                
                # Send notifications for newly added developers
                if new_developers and len(new_developers) > 0:
                    logger.info(f"Added {len(new_developers)} new developers to whitelist")
                    if self.discord_notifier:
                        for dev in new_developers:
                            asyncio.create_task(
                                notify_snipe_add(
                                    self.discord_notifier,
                                    dev
                                )
                            )
                
        except Exception as e:
            logger.error(f"Error fetching/refreshing developers from database: {e}")

    async def is_whitelisted(self, developer_address: str | Pubkey) -> bool:
        """Check if a developer is in the whitelist.

        Args:
            developer_address: Developer address to check

        Returns:
            True if the developer is in the whitelist, False otherwise
        """
        # Fast lookup with minimal locking - this is the critical path
        logger.info(f"Checking if {developer_address} is whitelisted")
        async with self.whitelist_lock:
            return developer_address in self.developer_whitelist

    async def mark_as_sniped(self, developer_address: str | Pubkey) -> None:
        """Mark a developer as sniped by removing them from the whitelist.

        Args:
            developer_address: Developer address to mark
        """
        # Remove from whitelist
        async with self.whitelist_lock:
            if developer_address in self.developer_whitelist:
                del self.developer_whitelist[developer_address]
                logger.info(f"Developer {developer_address} marked as sniped (removed from whitelist)")
            else:
                logger.info(f"Developer {developer_address} was not in whitelist to mark as sniped.")

    async def _load_persisted_whitelist(self) -> None:
        """Load the developer whitelist from file."""
        try:
            if os.path.exists(self.persisted_whitelist_filepath):
                async with self.file_lock:
                    with open(self.persisted_whitelist_filepath, 'r') as f:
                        # Ensure we load into the correct structure
                        loaded_data = json.load(f)
                        if not isinstance(loaded_data, dict):
                            logger.warning(f"Persisted whitelist file {self.persisted_whitelist_filepath} does not contain a valid dictionary. Initializing empty whitelist.")
                            loaded_data = {}
                    
                async with self.whitelist_lock:
                    # We can choose to merge or replace. Replacing is simpler.
                    # If merging, need to consider timestamps and max_developers.
                    # For now, let's replace. The refresh will soon fetch live data.
                    self.developer_whitelist = loaded_data 
                    
                logger.info(f"Loaded {len(self.developer_whitelist)} developers into whitelist from {self.persisted_whitelist_filepath}")
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
        """Save the current developer whitelist to file."""
        try:
            # Get a snapshot of the current whitelist
            async with self.whitelist_lock:
                # Create a copy to avoid issues if it's modified during dump
                whitelist_to_save = self.developer_whitelist.copy() 
            
            # Write to file (potentially slow I/O operation)
            async with self.file_lock:
                with open(self.persisted_whitelist_filepath, 'w') as f:
                    json.dump(whitelist_to_save, f, indent=4) # Added indent for readability
                    
            logger.info(f"Saved {len(whitelist_to_save)} developers from whitelist to {self.persisted_whitelist_filepath}")
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
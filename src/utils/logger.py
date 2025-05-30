"""
Logging utilities for the pump.fun trading bot.
"""

import logging
from logging.handlers import RotatingFileHandler
from .async_logger import AsyncLogger, get_async_logger, stop_all_async_loggers
import atexit

# Global dict to store loggers
_loggers: dict[str, AsyncLogger] = {}

# Register cleanup function to stop async loggers on exit
atexit.register(stop_all_async_loggers)


def get_logger(name: str, level: int = logging.INFO, use_async: bool = True) -> AsyncLogger | logging.Logger:
    """Get or create a logger with the given name.

    Args:
        name: Logger name, typically __name__
        level: Logging level
        use_async: Whether to use async logging (default: True)

    Returns:
        Configured logger (AsyncLogger if use_async=True, otherwise standard Logger)
    """
    global _loggers

    if name in _loggers:
        return _loggers[name]

    if use_async:
        # Create the underlying logger first
        base_logger = logging.getLogger(name)
        base_logger.setLevel(level)
        
        # Create async wrapper
        async_logger = get_async_logger(name)
        _loggers[name] = async_logger
        return async_logger
    else:
        # Fallback to regular logger
        logger = logging.getLogger(name)
        logger.setLevel(level)
        return logger


def setup_file_logging(
    filename: str = "pump_trading.log",
    level: int = logging.INFO,
    max_bytes: int = 1024 * 1024 * 5,  # 5 MB
    backup_count: int = 5,
) -> None:
    """Set up file logging for all loggers.

    Args:
        filename: Log file path
        level: Logging level for file handler
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup log files to keep
    """
    root_logger = logging.getLogger()

    # Check if rotating file handler with same filename already exists
    for handler in root_logger.handlers:
        if isinstance(handler, RotatingFileHandler) and handler.baseFilename == filename:
            return  # File handler already added
    
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Use RotatingFileHandler instead of FileHandler
    file_handler = RotatingFileHandler(
        filename, maxBytes=max_bytes, backupCount=backup_count
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)


def get_async_logger_stats() -> dict:
    """Get statistics from all async loggers.
    
    Returns:
        Dictionary with stats for each logger
    """
    stats = {}
    for name, async_logger in _loggers.items():
        stats[name] = async_logger.get_stats()
    return stats

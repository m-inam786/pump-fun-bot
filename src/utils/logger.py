"""
Logging utilities for the pump.fun trading bot.
"""

import logging
from logging.handlers import RotatingFileHandler

# Global dict to store loggers
_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get or create a logger with the given name.

    Args:
        name: Logger name, typically __name__
        level: Logging level

    Returns:
        Configured logger
    """
    global _loggers

    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)

    _loggers[name] = logger
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

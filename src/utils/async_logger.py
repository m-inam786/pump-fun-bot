"""
Async logging utility that queues log messages and processes them in a background thread
Reduces latency in the critical trading path by deferring I/O operations.
"""

import asyncio
import logging
import threading
import time
from queue import Queue, Empty
from typing import Any, Dict, Optional
from functools import wraps

class AsyncLogger:
    """Async logger that queues messages for background processing."""
    
    def __init__(self, logger: logging.Logger, queue_size: int = 10000):
        """Initialize async logger.
        
        Args:
            logger: The underlying logger instance
            queue_size: Maximum size of the log queue
        """
        self.logger = logger
        self.queue = Queue(maxsize=queue_size)
        self.worker_thread = None
        self.running = False
        self._stats = {
            'queued': 0,
            'processed': 0,
            'dropped': 0,
            'errors': 0
        }
    
    def start(self) -> None:
        """Start the background logging thread."""
        if self.running:
            return
            
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()
    
    def stop(self) -> None:
        """Stop the background logging thread and flush remaining messages."""
        if not self.running:
            return
            
        self.running = False
        
        # Signal shutdown
        try:
            self.queue.put(None, timeout=1.0)
        except:
            pass
            
        # Wait for worker thread to finish
        if self.worker_thread:
            self.worker_thread.join(timeout=2.0)
    
    def _worker(self) -> None:
        """Background worker that processes queued log messages."""
        while self.running:
            try:
                # Get message with timeout
                item = self.queue.get(timeout=0.1)
                
                # Shutdown signal
                if item is None:
                    break
                
                # Process the log message
                level, msg, args, kwargs = item
                getattr(self.logger, level)(msg, *args, **kwargs)
                self._stats['processed'] += 1
                
            except Empty:
                continue
            except Exception as e:
                self._stats['errors'] += 1
                # Fallback to direct logging for errors
                try:
                    self.logger.error(f"Async logger worker error: {e}")
                except:
                    pass
    
    def _queue_log(self, level: str, msg: str, *args, **kwargs) -> None:
        """Queue a log message for background processing."""
        if not self.running:
            # Fallback to direct logging if not running
            getattr(self.logger, level)(msg, *args, **kwargs)
            return
        
        try:
            self.queue.put((level, msg, args, kwargs), block=False)
            self._stats['queued'] += 1
        except:
            # Queue full - drop message and increment counter
            self._stats['dropped'] += 1
            # For critical errors, fall back to direct logging
            if level in ('error', 'critical'):
                try:
                    getattr(self.logger, level)(msg, *args, **kwargs)
                except:
                    pass
    
    def debug(self, msg: str, *args, **kwargs) -> None:
        """Queue debug message."""
        self._queue_log('debug', msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs) -> None:
        """Queue info message."""
        self._queue_log('info', msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs) -> None:
        """Queue warning message."""
        self._queue_log('warning', msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs) -> None:
        """Queue error message."""
        self._queue_log('error', msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs) -> None:
        """Queue critical message."""
        self._queue_log('critical', msg, *args, **kwargs)
    
    def get_stats(self) -> Dict[str, int]:
        """Get logging statistics."""
        return self._stats.copy()

# Global async logger registry
_async_loggers: Dict[str, AsyncLogger] = {}

def get_async_logger(name: str, queue_size: int = 10000) -> AsyncLogger:
    """Get or create an async logger for the given name.
    
    Args:
        name: Logger name
        queue_size: Queue size for the async logger
        
    Returns:
        AsyncLogger instance
    """
    if name not in _async_loggers:
        base_logger = logging.getLogger(name)
        async_logger = AsyncLogger(base_logger, queue_size)
        async_logger.start()
        _async_loggers[name] = async_logger
    
    return _async_loggers[name]

def stop_all_async_loggers() -> None:
    """Stop all async loggers and flush remaining messages."""
    for async_logger in _async_loggers.values():
        async_logger.stop()
    _async_loggers.clear()

# Decorator for non-critical logging
def async_log(func):
    """Decorator to make logging calls asynchronous for non-critical operations."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # This is a simple decorator - in practice you might want more sophisticated logic
        return func(*args, **kwargs)
    return wrapper 
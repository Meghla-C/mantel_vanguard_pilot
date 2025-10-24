import logging
import sys
from typing import Optional, Any
from loggers.ILogger import ILogger


class STErrorLogger(ILogger):
    """
    Logger implementation for handling error-level logging
    Outputs ERROR and CRITICAL messages to stderr with JSON formatting
    Uses Chain of Responsibility pattern to handle error logging
    """

    def __init__(self, name: str = "default", level: str = "ERROR", **kwargs):
        
        self.name = name
        self.level = level.upper()
        
        # Create logger instance
        self.logger = logging.getLogger(f"{name}_error")
        self.logger.setLevel(self._get_logging_level(level))
        
        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        json_formatter = self._get_JSON_Formatter()
        
        # Create stderr handler for ERROR and CRITICAL only
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(json_formatter)
        
        # Add handler to logger
        self.logger.addHandler(stderr_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    def debug(self, message: str, *args, **kwargs) -> None:
        """Log debug message - pass to next handler"""
        self.handle(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs) -> None:
        """Log info message - pass to next handler"""
        self.handle(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs) -> None:
        """Log warning message - pass to next handler"""
        self.handle(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs) -> None:
        """Log error message and handle it"""
        self.logger.error(message, *args, **kwargs)
        # Pass to next handler in chain for additional error handling
        self.handle(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs) -> None:
        """Log critical message and handle it"""
        self.logger.critical(message, *args, **kwargs)
        # Pass to next handler in chain for additional error handling
        self.handle(message, *args, **kwargs)
    
    def set_level(self, level: str) -> None:
        """Set the logging level"""
        logging_level = self._get_logging_level(level)
        self.logger.setLevel(logging_level)
        self.level = level.upper()
    
    def get_level(self) -> str:
        """Get the current logging level"""
        return self.level
    
    def handle(self, request: Any, *args, **kwargs):
        """Handle the request and pass to next handler in chain"""
        if hasattr(self, '_next_handler') and self._next_handler:
            return self._next_handler.handle(request, *args, **kwargs)
        return None

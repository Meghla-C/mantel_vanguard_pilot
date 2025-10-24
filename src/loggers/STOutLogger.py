import logging
import sys
from typing import Optional, Any
from loggers.ILogger import ILogger


class STOutLogger(ILogger):
    """
    Logger implementation using Python's built-in logging library
    Outputs to stdout/stderr with JSON formatting
    """

    def __init__(self, name: str = "default", level: str = "INFO", **kwargs):        

        self.name = name
        self.level = level.upper()

        # Create logger instance
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self._get_logging_level(level))
        
        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
    
        json_formatter = self._get_JSON_Formatter()
        
        # Create stdout handler for INFO and below
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.addFilter(lambda record: record.levelno < logging.WARNING)
        stdout_handler.setFormatter(json_formatter)
        
        # Add handlers to logger
        self.logger.addHandler(stdout_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    def debug(self, message: str, *args, **kwargs) -> None:
        """Log debug message"""
        self.logger.debug(message, *args, **kwargs)
        self.handle(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs) -> None:
        """Log info message"""
        self.logger.info(message, *args, **kwargs)
        self.handle(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs) -> None:
        """Log warning message"""
        self.logger.warning(message, *args, **kwargs)
        # Pass to next handler in chain for error handling
        self.handle(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs) -> None:
        """Log error message"""
        # Pass to next handler in chain for error handling
        self.handle(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs) -> None:
        """Log critical message"""
        # Pass to next handler in chain for error handling
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

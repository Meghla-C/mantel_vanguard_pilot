import logging
import json

from abc import abstractmethod
from typing import Any, Optional
from loggers.AbsractHandler import AbstractHandler


class JSONFormatter(logging.Formatter):
    """Structured JSON formatter for logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Formats log records as JSON."""
        log_record = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage(),
            'logger': record.name,
            'line': record.lineno,
        }
        return json.dumps(log_record)


class ILogger(AbstractHandler):
    """
    Abstract Logger Interface class for concrete class implementation
    """
    @abstractmethod
    def __init__(self, name: str = "default", level: str = "INFO", **kwargs):
        raise NotImplementedError

    def _get_JSON_Formatter(self):
        return JSONFormatter(datefmt='%Y-%m-%d %H:%M:%S')

    def _get_logging_level(self, level: str) -> int:
        """Convert string level to logging constant"""
        level_mapping = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        return level_mapping.get(level.upper(), logging.INFO)

    @abstractmethod
    def debug(self, message: str, *args, **kwargs) -> None:
        """
        Definition for a method that logs debug level messages

        Inputs:
            - message : str, The message to be logged
            - *args : Variable length argument list
            - **kwargs : Variable length keyword arguments
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def info(self, message: str, *args, **kwargs) -> None:
        """
        Definition for a method that logs info level messages

        Inputs:
            - message : str, The message to be logged
            - *args : Variable length argument list
            - **kwargs : Variable length keyword arguments
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def warning(self, message: str, *args, **kwargs) -> None:
        """
        Definition for a method that logs warning level messages

        Inputs:
            - message : str, The message to be logged
            - *args : Variable length argument list
            - **kwargs : Variable length keyword arguments
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def error(self, message: str, *args, **kwargs) -> None:
        """
        Definition for a method that logs error level messages

        Inputs:
            - message : str, The message to be logged
            - *args : Variable length argument list
            - **kwargs : Variable length keyword arguments
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def critical(self, message: str, *args, **kwargs) -> None:
        """
        Definition for a method that logs critical level messages

        Inputs:
            - message : str, The message to be logged
            - *args : Variable length argument list
            - **kwargs : Variable length keyword arguments
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def set_level(self, level: str) -> None:
        """
        Definition for a method that sets the logging level

        Inputs:
            - level : str, The new logging level to set
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def get_level(self) -> str:
        """
        Definition for a method that gets the current logging level

        Inputs:
            - None.
        Outputs:
            - str, The current logging level
        """
        raise NotImplementedError
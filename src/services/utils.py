"""Utility functions for the project services."""
import sys
from loguru import logger

def get_logger(log_name="default"):
    """
    Configure and return a logger instance with a name.
    """
    logger.remove()
    logger.add(
        sys.stdout,
        format=f"{log_name} | <green>{{time}}</green> | <level>{{level}}</level> | {{message}}",
        level="INFO",
        colorize=True
    )
    return logger

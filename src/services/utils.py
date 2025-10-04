"""Utility functions for the project services."""
import sys
from loguru import logger

def get_logger():
    """
    Configure and return a logger instance.
    """
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time}</green> | <level>{level}</level> | {message}",
        level="INFO",
        colorize=True
    )
    return logger

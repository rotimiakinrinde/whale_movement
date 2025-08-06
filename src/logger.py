# src/logger.py

import logging
from pathlib import Path
import sys

# Set up log directory and file path
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "pipeline.log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Define logging format
LOG_FORMAT = "%(asctime)s — %(name)s — %(levelname)s — %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Create and configure logger
logger = logging.getLogger("WhaleTracker")
logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all log levels

# Avoid adding duplicate handlers
if not logger.handlers:
    # File handler (DEBUG level)
    file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(file_handler)

    # Console handler (DEBUG level)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(console_handler)

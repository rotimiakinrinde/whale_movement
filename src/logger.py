# src/logger.py

import logging
from pathlib import Path

# Define log directory path
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Setup basic logging config
logging.basicConfig(
    filename=LOG_DIR / "pipeline.log",
    filemode="a",
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    level=logging.INFO
)

# Global logger instance
logger = logging.getLogger("WhaleTracker")

import logging
import sys
import os
from datetime import datetime


def get_logger(name: str):

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

  
    # Use Project Root (Current Dir)
    PROJECT_ROOT = os.getcwd()  

    LOG_DIR = os.path.join(PROJECT_ROOT, "Logs")

    os.makedirs(LOG_DIR, exist_ok=True)

    # Log file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"etl_{timestamp}.log")


    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )


    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)


    # File Handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)


    # Add Handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

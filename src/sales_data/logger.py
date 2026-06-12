"""
Logging setup for the sales-data pipeline.

:description: Creates a common logger that writes log messages both to
    the console and to log files. The log files are automatically rotated
    when they reach a certain size, preventing the logs from growing too
    large while still keeping logs from previous pipeline runs.
    
"""

import logging
import logging.handlers
import os


def get_logger(name: str = "sales_data", log_dir: str = "logs") -> logging.Logger:

    #Return a configured :class:`logging.Logger` instance.

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger is already configured.
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler – INFO and above.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Rotating file handler – DEBUG and above.
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "sales_data.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

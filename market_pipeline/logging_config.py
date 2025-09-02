# market_pipeline/logging_config.py
import os
import logging
import logging.config
from pathlib import Path


def setup_logging():
    """
    Configures logging for the application:
      - Console output (StreamHandler)
      - Rotating file output (TimedRotatingFileHandler, daily)
      - Reads LOG_LEVEL from environment (default INFO)
    """
    # Determine project root and logs directory
    base_dir = Path(__file__).parent.parent
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Read log level from env
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s %(name)s [%(levelname)s] %(message)s"
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": log_level,
                "stream": "ext://sys.stdout"
            },
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "formatter": "standard",
                "level": log_level,
                "filename": str(logs_dir / "pipeline.log"),
                "when": "midnight",
                "backupCount": 14,
            },
        },
        "root": {
            "handlers": ["console", "file"],
            "level": log_level,
        }
    }

    logging.config.dictConfig(config)


# scripts/logger.py
import logging
from market_pipeline.logging_config import setup_logging

# Initialize the global logging configuration once
setup_logging()


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Returns a logger configured with the application-wide handlers.

    Args:
        name: Logger name (typically __name__ of the calling module).
    """
    return logging.getLogger(name)

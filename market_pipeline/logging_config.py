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
import logging# market_pipeline/logging_config.py
import os
import sys
import logging
import logging.config
from pathlib import Path
import io

def _safe_console_stream():
    """
    Wrap sys.stdout with a TextIOWrapper that won't crash on non-encodable chars.
    On Windows CP1252 terminals, emojis cause UnicodeEncodeError; we set errors='replace'.
    """
    # If already UTF-8 capable console, just return sys.stdout
    enc = getattr(sys.stdout, "encoding", None) or ""
    if enc.lower().replace("-", "") == "utf8":
        return sys.stdout

    # Otherwise wrap the underlying buffer with a tolerant encoder
    try:
        return io.TextIOWrapper(sys.stdout.buffer, encoding=enc or "cp1252", errors="replace")
    except Exception:
        # Fallback to UTF-8 replace (better to lose glyphs than crash)
        return io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

def setup_logging():
    base_dir = Path(__file__).parent.parent
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    # Build the config dict as before
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {"format": "%(asctime)s %(name)s [%(levelname)s] %(message)s"},
        },
        "handlers": {
            "console": {
                "()": "logging.StreamHandler",     # callable-based handler
                "formatter": "standard",
                "level": LOG_LEVEL,
                # stream will be patched after dictConfig
            },
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "formatter": "standard",
                "level": LOG_LEVEL,
                "filename": str(logs_dir / "pipeline.log"),
                "when": "midnight",
                "backupCount": 14,
                "encoding": "utf-8",
            },
        },
        "root": {"handlers": ["console", "file"], "level": LOG_LEVEL},
    }

    logging.config.dictConfig(config)

    # Patch console handler's stream to be emoji-safe
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, logging.StreamHandler):
            try:
                h.setStream(_safe_console_stream())
            except Exception:
                pass

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

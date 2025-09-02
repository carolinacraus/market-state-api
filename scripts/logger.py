# scripts/logger.py

import logging
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LoggingConfig:
    """
    Configuration for application logging.

    Attributes:
        name: Name of the logger (also used for filename).
        base_dir: Project root directory.
        logs_dir: Directory where log files are stored.
        level: Logging level.
        fmt: Log message format.
    """
    name: str = "pipeline"
    base_dir: Path = Path(__file__).parent.parent
    logs_dir: Path = base_dir / "logs"
    level: int = logging.INFO
    fmt: str = "%(asctime)s - %(levelname)s - %(message)s"


class LoggerFactory:
    """
    Factory to create and configure file-based loggers.
    """
    def __init__(self, config: LoggingConfig):
        self.config = config
        # Ensure the logs directory exists
        self.config.logs_dir.mkdir(parents=True, exist_ok=True)

    def get_logger(self) -> logging.Logger:
        """
        Returns a configured logger instance.
        """
        logger = logging.getLogger(self.config.name)
        logger.setLevel(self.config.level)

        # Avoid adding multiple handlers if already configured
        if not logger.handlers:
            handler = logging.FileHandler(self.config.logs_dir / f"{self.config.name}.log")
            formatter = logging.Formatter(self.config.fmt)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger


def get_logger(name: str = "pipeline") -> logging.Logger:
    """
    Convenience function to retrieve a named logger.

    Args:
        name: Identifier for the logger and log file.

    Returns:
        A configured logging.Logger instance.
    """
    config = LoggingConfig(name=name)
    factory = LoggerFactory(config)
    return factory.get_logger()

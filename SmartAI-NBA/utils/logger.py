# utils/logger.py
# Structured logging configuration for SmartAI-NBA.
# Sets up a module-aware logger with console and rotating file handlers.

import logging
import os
from logging.handlers import RotatingFileHandler

# Ensure logs directory exists
_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

_LOG_FILE = os.path.join(_LOG_DIR, "smartai_nba.log")

# Custom formatter
_FORMATTER = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Module-level flag to avoid adding duplicate handlers
_configured = False


def _configure_root_logger():
    """Configure root logger with console + rotating file handler (once)."""
    global _configured
    if _configured:
        return

    root = logging.getLogger("smartai_nba")
    root.setLevel(logging.DEBUG)

    # Console handler — respect LOG_LEVEL env var (default: INFO)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler)
               for h in root.handlers):
        console_handler = logging.StreamHandler()
        _log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
        _resolved_level = getattr(logging, _log_level, None)
        if _resolved_level is None:
            _resolved_level = logging.INFO
            import sys
            print(f"[WARNING] Invalid LOG_LEVEL '{_log_level}', falling back to INFO", file=sys.stderr)
        console_handler.setLevel(_resolved_level)
        console_handler.setFormatter(_FORMATTER)
        root.addHandler(console_handler)

    # Rotating file handler — DEBUG and above, max 5MB, keep 3 backups
    if not any(isinstance(h, RotatingFileHandler) for h in root.handlers):
        try:
            file_handler = RotatingFileHandler(
                _LOG_FILE,
                maxBytes=5 * 1024 * 1024,  # 5MB
                backupCount=3,
                encoding="utf-8",
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(_FORMATTER)
            root.addHandler(file_handler)
        except (OSError, PermissionError):
            # Silently skip file logging if we can't write (e.g. read-only deployment)
            pass

    _configured = True


def get_logger(module_name):
    """
    Get a configured logger for a module.

    Usage:
        from utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Loading player stats...")
        logger.warning("Rate limit approaching")
        logger.error("API call failed: %s", error)

    Args:
        module_name (str): Module name, typically __name__

    Returns:
        logging.Logger: Configured logger instance
    """
    _configure_root_logger()
    # Use a child logger under the "smartai_nba" namespace
    name = f"smartai_nba.{module_name}" if not module_name.startswith("smartai_nba") else module_name
    return logging.getLogger(name)


def setup_logging(log_level: str = "INFO", log_file: str = None):
    """
    Configure logging for the application.

    This is a convenience wrapper that mirrors the auto-configuration done
    by :func:`get_logger` but allows callers to override the log level and
    optionally direct output to a specific file.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional file path for log output.

    Returns:
        logging.Logger: The root ``smartai_nba`` logger.
    """
    global _configured
    # Allow reconfiguration when called explicitly
    _configured = False
    if log_level:
        os.environ["LOG_LEVEL"] = log_level.upper()
    _configure_root_logger()

    root = logging.getLogger("smartai_nba")

    if log_file:
        from pathlib import Path
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(_FORMATTER)
        root.addHandler(file_handler)

    return root

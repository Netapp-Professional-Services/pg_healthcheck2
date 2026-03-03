"""
Project-level logging configuration.

Configures the root logger to send all log output to stderr with a consistent
format. Use configure_logging() at application startup from each entry point.

Log level can be controlled by:
- Environment variable LOG_LEVEL (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Argument verbose=True for DEBUG (used when LOG_LEVEL is not set)
"""

import logging
import os
import sys

# Map common env values to logging constants
_LOG_LEVEL_NAMES = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def _resolve_level(verbose=False):
    """Resolve log level: env LOG_LEVEL if set, else verbose -> DEBUG, else INFO."""
    env_level = os.environ.get("LOG_LEVEL", "").strip().upper()
    if env_level and env_level in _LOG_LEVEL_NAMES:
        return _LOG_LEVEL_NAMES[env_level]
    return logging.DEBUG if verbose else logging.INFO


def configure_logging(verbose=False):
    """
    Configure the root logger to output to stderr.

    Log level is determined by (first wins):
    1. Environment variable LOG_LEVEL (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    2. verbose=True -> DEBUG, else INFO

    Args:
        verbose: If True and LOG_LEVEL is not set, use DEBUG; otherwise INFO.
    """
    root = logging.getLogger()
    level = _resolve_level(verbose=verbose)
    root.setLevel(level)

    # Avoid duplicate handlers if called more than once (e.g. from tests or re-imports)
    if not any(
        isinstance(h, logging.StreamHandler) and h.stream is sys.stderr
        for h in root.handlers
    ):
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )
        root.addHandler(handler)

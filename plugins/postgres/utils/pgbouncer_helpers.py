"""
PgBouncer Common Helpers

Shared utilities for PgBouncer health checks following the self-opt-in pattern.
Provides consistent skip logic for PgBouncer-related checks.
"""

import logging
from typing import Dict, Optional, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.pgbouncer_client import test_pgbouncer_connection

logger = logging.getLogger(__name__)


def skip_if_not_pgbouncer(settings: Dict) -> Optional[Tuple[str, Dict]]:
    """
    Check if PgBouncer is available, return skip result if not.

    This function performs a quick, low-cost detection:
    1. If explicit PgBouncer config provided -> Proceed (user wants monitoring)
    2. If Aurora environment -> Skip (Aurora never has PgBouncer)
    3. If no config AND quick detection fails -> Skip silently

    Args:
        settings: Configuration dictionary

    Returns:
        None if PgBouncer detected or explicitly configured,
        otherwise tuple of (adoc_content, findings)
    """
    # Check if user explicitly configured PgBouncer
    has_explicit_config = bool(
        settings.get('pgbouncer_host') or
        settings.get('pgbouncer_admin_user') or
        settings.get('pgbouncer_port')
    )

    # If user provided config, don't skip - they want monitoring
    if has_explicit_config:
        logger.debug("PgBouncer config provided, proceeding with check")
        return None

    # Aurora never has PgBouncer - skip automatically
    if settings.get('is_aurora'):
        logger.debug("Aurora environment detected, skipping PgBouncer check")
        builder = CheckContentBuilder()
        builder.text("⏭️  Skipped - PgBouncer not available on Aurora")
        return builder.build(), {
            'status': 'skipped',
            'reason': 'Aurora environment (PgBouncer not supported)'
        }

    # No explicit config - try quick detection
    logger.debug("No PgBouncer config found, attempting quick detection")
    detected = _quick_detect_pgbouncer(settings)

    if detected:
        logger.info("PgBouncer detected via quick detection")
        return None

    # Not detected and no config - skip gracefully
    logger.debug("PgBouncer not detected and no config provided, skipping check")
    builder = CheckContentBuilder()
    builder.text("⏭️  Skipped - PgBouncer not detected")

    return builder.build(), {
        'status': 'skipped',
        'reason': 'PgBouncer not detected and no configuration provided'
    }


def _quick_detect_pgbouncer(settings: Dict) -> bool:
    """
    Perform quick, low-cost PgBouncer detection.

    This attempts minimal detection methods:
    1. Port scan on default PgBouncer port (6432)
    2. Connection pattern analysis (if available)

    Args:
        settings: Configuration dictionary

    Returns:
        True if PgBouncer detected, False otherwise
    """
    import socket

    # Try default PgBouncer port on same host as PostgreSQL
    pgbouncer_host = settings.get('host')
    if not pgbouncer_host:
        return False

    pgbouncer_port = 6432  # Default PgBouncer port
    timeout = 2  # Quick timeout

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((pgbouncer_host, pgbouncer_port))
        sock.close()

        if result == 0:
            logger.debug(f"PgBouncer port {pgbouncer_port} is open on {pgbouncer_host}")
            return True
    except Exception as e:
        logger.debug(f"Quick detection failed: {e}")

    return False


def should_run_pgbouncer_check(settings: Dict) -> Tuple[bool, Optional[str]]:
    """
    Determine if PgBouncer check should run and provide reason.

    This is a higher-level function that returns both decision and reason
    for more detailed control flow.

    Args:
        settings: Configuration dictionary

    Returns:
        tuple: (should_run: bool, skip_reason: Optional[str])
    """
    # Check for explicit config
    has_explicit_config = bool(
        settings.get('pgbouncer_host') or
        settings.get('pgbouncer_admin_user')
    )

    if has_explicit_config:
        return True, None

    # Try quick detection
    if _quick_detect_pgbouncer(settings):
        return True, None

    return False, "PgBouncer not detected and no configuration provided"


def get_pgbouncer_connection_params(settings: Dict) -> Dict:
    """
    Extract PgBouncer connection parameters from settings.

    Args:
        settings: Configuration dictionary

    Returns:
        Dictionary with connection parameters
    """
    return {
        'host': settings.get('pgbouncer_host') or settings.get('host'),
        'port': settings.get('pgbouncer_port', 6432),
        'user': settings.get('pgbouncer_admin_user') or settings.get('user'),
        'password': settings.get('pgbouncer_admin_password') or settings.get('password'),
        'timeout': settings.get('pgbouncer_timeout', 5)
    }

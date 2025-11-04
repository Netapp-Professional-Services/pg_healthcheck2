"""
Patroni Common Helpers

Shared utilities for Patroni health checks following the Instaclustr pattern.
Provides consistent patterns for API-first with SSH fallback.
"""

import logging
from typing import Dict, Optional, Tuple, Callable, Any, List
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def skip_if_not_patroni(connector) -> Optional[Tuple[str, Dict]]:
    """
    Check if environment is Patroni, return skip result if not.

    Args:
        connector: PostgreSQL connector with environment detection

    Returns:
        None if Patroni detected, otherwise tuple of (adoc_content, findings)
    """
    if connector.environment != 'patroni':
        builder = CheckContentBuilder()
        builder.text("⏭️  Skipped - Not a Patroni-managed cluster")

        return builder.build(), {
            'status': 'skipped',
            'reason': 'Not a Patroni-managed cluster',
            'environment': connector.environment
        }

    return None


def fetch_with_fallback(
    primary_fn: Callable,
    fallback_fn: Optional[Callable] = None,
    error_message: str = "All data fetching methods failed"
) -> Tuple[bool, Any]:
    """
    Try primary method, fallback to secondary, return error if both fail.

    This implements the Instaclustr pattern: API-first with SSH fallback.

    Args:
        primary_fn: Primary function to call (usually API)
        fallback_fn: Fallback function to call (usually SSH), None to skip
        error_message: Error message if all methods fail

    Returns:
        tuple: (success: bool, data: any)

    Example:
        success, data = fetch_with_fallback(
            lambda: fetch_from_api(),
            lambda: fetch_from_ssh()
        )
    """
    # Try primary method
    try:
        logger.debug("Attempting primary data fetch method")
        result = primary_fn()

        if result:
            logger.debug("Primary method succeeded")
            return True, result

    except Exception as e:
        logger.debug(f"Primary method failed: {e}")

    # Try fallback if available
    if fallback_fn:
        try:
            logger.info("Primary method failed, trying fallback")
            result = fallback_fn()

            if result:
                logger.debug("Fallback method succeeded")
                return True, result

        except Exception as e:
            logger.error(f"Fallback method failed: {e}")

    # All methods failed
    logger.error(error_message)
    return False, {'error': error_message, 'success': False}


def analyze_lag_status(lag_bytes: Optional[int]) -> Dict:
    """
    Analyze replication lag and return status with recommendations.

    Args:
        lag_bytes: Replication lag in bytes, None if unknown

    Returns:
        Dictionary with:
        - lag_mb: Lag in megabytes
        - status: 'ok', 'warning', 'critical', 'unknown'
        - severity: Severity level for trending
        - message: Human-readable message
        - recommendation: Action to take
    """
    if lag_bytes is None:
        return {
            'lag_mb': None,
            'lag_bytes': None,
            'status': 'unknown',
            'severity': 'info',
            'message': 'Replication lag could not be determined',
            'recommendation': 'Check replica connectivity and PostgreSQL logs'
        }

    lag_mb = round(lag_bytes / (1024 * 1024), 2)

    # Critical: >1GB lag
    if lag_mb > 1000:
        return {
            'lag_mb': lag_mb,
            'lag_bytes': lag_bytes,
            'status': 'critical',
            'severity': 'critical',
            'message': f'Critical replication lag: {lag_mb} MB',
            'recommendation': 'Immediate action required. Check network, disk I/O, and replica load. Consider increasing wal_sender_timeout.'
        }

    # Warning: >100MB lag
    elif lag_mb > 100:
        return {
            'lag_mb': lag_mb,
            'lag_bytes': lag_bytes,
            'status': 'warning',
            'severity': 'warning',
            'message': f'High replication lag: {lag_mb} MB',
            'recommendation': 'Monitor closely. Check for network issues or slow replica performance.'
        }

    # Moderate: >10MB lag
    elif lag_mb > 10:
        return {
            'lag_mb': lag_mb,
            'lag_bytes': lag_bytes,
            'status': 'moderate',
            'severity': 'info',
            'message': f'Moderate replication lag: {lag_mb} MB',
            'recommendation': 'Acceptable for async replication. Monitor for increases.'
        }

    # OK: <=10MB lag
    else:
        return {
            'lag_mb': lag_mb,
            'lag_bytes': lag_bytes,
            'status': 'ok',
            'severity': 'info',
            'message': f'Healthy replication lag: {lag_mb} MB',
            'recommendation': 'No action needed'
        }


def format_timestamp(ts: Any) -> str:
    """
    Format Patroni timestamps consistently.

    Handles various timestamp formats from Patroni API:
    - ISO 8601 strings
    - Unix timestamps
    - datetime objects

    Args:
        ts: Timestamp in various formats

    Returns:
        Formatted timestamp string (YYYY-MM-DD HH:MM:SS UTC)
    """
    if ts is None:
        return 'N/A'

    try:
        # If it's already a datetime object
        if isinstance(ts, datetime):
            return ts.strftime('%Y-%m-%d %H:%M:%S UTC')

        # If it's a string (ISO format)
        if isinstance(ts, str):
            # Remove 'Z' suffix if present
            ts_clean = ts.rstrip('Z')
            dt = datetime.fromisoformat(ts_clean)
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')

        # If it's a Unix timestamp (int or float)
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts)
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')

    except Exception as e:
        logger.warning(f"Could not parse timestamp {ts}: {e}")
        return str(ts)

    return str(ts)


def format_patroni_table(
    data: List[Dict],
    builder: CheckContentBuilder,
    title: Optional[str] = None
) -> None:
    """
    Format Patroni data as AsciiDoc table and add to builder.

    This is a convenience wrapper around CheckContentBuilder.table()
    with common formatting for Patroni checks.

    Args:
        data: List of dictionaries with consistent keys
        builder: CheckContentBuilder instance
        title: Optional title to display above table

    Example:
        data = [
            {'Node': 'vm1', 'Role': 'leader', 'State': 'running'},
            {'Node': 'vm2', 'Role': 'replica', 'State': 'streaming'}
        ]
        format_patroni_table(data, builder, "Node Status")
    """
    if title:
        builder.text(f"*{title}*")
        builder.blank()

    if not data:
        builder.text("_No data available_")
        builder.blank()
        return

    builder.table(data)
    builder.blank()


def format_lag_indicator(lag_mb: Optional[float]) -> str:
    """
    Format lag as colored indicator for AsciiDoc output.

    Args:
        lag_mb: Lag in megabytes

    Returns:
        Formatted string with emoji indicator
    """
    if lag_mb is None:
        return "❓ Unknown"

    if lag_mb > 1000:
        return f"❌ {lag_mb} MB"
    elif lag_mb > 100:
        return f"⚠️ {lag_mb} MB"
    elif lag_mb > 10:
        return f"⚡ {lag_mb} MB"
    else:
        return f"✅ {lag_mb} MB"


def format_state_indicator(state: str) -> str:
    """
    Format PostgreSQL/Patroni state with emoji indicator.

    Args:
        state: State string (running, streaming, stopped, etc.)

    Returns:
        State with appropriate emoji
    """
    state_lower = state.lower()

    healthy_states = ['running', 'streaming', 'in archive recovery']
    warning_states = ['starting', 'stopping', 'restarting', 'creating replica']
    error_states = ['stopped', 'crashed', 'failed', 'start failed']

    if state_lower in healthy_states:
        return f"✅ {state}"
    elif state_lower in warning_states:
        return f"⚠️ {state}"
    elif state_lower in error_states:
        return f"❌ {state}"
    else:
        return f"❓ {state}"


def calculate_catchup_time(lag_bytes: int, catchup_rate_mbps: float = 10.0) -> str:
    """
    Estimate time for replica to catch up based on lag and throughput.

    Args:
        lag_bytes: Current replication lag in bytes
        catchup_rate_mbps: Estimated catchup rate in MB/s (default: 10)

    Returns:
        Human-readable catchup time estimate
    """
    if lag_bytes <= 0:
        return "0s (caught up)"

    lag_mb = lag_bytes / (1024 * 1024)
    seconds = lag_mb / catchup_rate_mbps

    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes}m"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"


def build_error_result(
    check_name: str,
    error: str,
    builder: Optional[CheckContentBuilder] = None
) -> Tuple[str, Dict]:
    """
    Build standardized error result for Patroni checks.

    Args:
        check_name: Name of the check (for findings key)
        error: Error message
        builder: Optional CheckContentBuilder instance

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    if builder is None:
        builder = CheckContentBuilder()

    builder.error(f"❌ Check failed: {error}")

    findings = {
        check_name: {
            'status': 'error',
            'error': error,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    }

    return builder.build(), findings


def parse_patroni_member_info(member: Dict) -> Dict:
    """
    Parse Patroni member information into standardized format.

    Handles different formats from /cluster and /patroni endpoints.

    Args:
        member: Member dictionary from Patroni API

    Returns:
        Standardized member info dictionary
    """
    return {
        'name': member.get('name', member.get('member', 'unknown')),
        'host': member.get('host', 'unknown'),
        'port': member.get('port', 5432),
        'role': member.get('role', 'unknown'),
        'state': member.get('state', 'unknown'),
        'timeline': member.get('timeline'),
        'lag': member.get('lag', 0),
        'tags': member.get('tags', {}),
        'api_url': member.get('api_url', '')
    }


def get_health_emoji(is_healthy: bool) -> str:
    """
    Get health status emoji.

    Args:
        is_healthy: Boolean health status

    Returns:
        Emoji string
    """
    return "✅" if is_healthy else "❌"


def create_summary_table(
    connector,
    cluster_name: str,
    total_nodes: int,
    leader_name: Optional[str],
    replica_count: int,
    health_score: int,
    additional_rows: Optional[List[Dict]] = None
) -> List[Dict]:
    """
    Create standardized summary table for Patroni checks.

    Args:
        connector: PostgreSQL connector
        cluster_name: Name of the cluster
        total_nodes: Total number of nodes
        leader_name: Name of leader node
        replica_count: Number of replicas
        health_score: Overall health score (0-100)
        additional_rows: Additional rows to include

    Returns:
        List of dictionaries for table formatting
    """
    rows = [
        {'Attribute': 'Cluster Name', 'Value': cluster_name},
        {'Attribute': 'Total Nodes', 'Value': str(total_nodes)},
    ]

    if leader_name:
        rows.append({'Attribute': 'Leader', 'Value': leader_name})

    rows.extend([
        {'Attribute': 'Replicas', 'Value': str(replica_count)},
        {'Attribute': 'Health Score', 'Value': f"{health_score}/100"}
    ])

    if additional_rows:
        rows.extend(additional_rows)

    return rows

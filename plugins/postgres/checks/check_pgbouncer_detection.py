"""
PgBouncer Detection Check

Detects if PgBouncer is present in the PostgreSQL connection path and
determines the level of access available for monitoring.

Self-Opt-In Behavior:
- Skips silently if PgBouncer not detected AND no explicit config
- Runs full detection if PgBouncer found OR user provided config

Detection Methods:
1. Direct admin console connection (primary method for self-hosted)
2. Connection pattern analysis from pg_stat_activity (indirect)
3. Port scanning (supplementary)

Outputs:
- Detection status (detected/not detected)
- Detection method used
- Version information
- Access level (full admin / limited / none)
- Configuration details
"""

import logging
from datetime import datetime
from typing import Dict, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.pgbouncer_helpers import skip_if_not_pgbouncer
from plugins.postgres.utils.pgbouncer_client import test_pgbouncer_connection

logger = logging.getLogger(__name__)


def check_pgbouncer_detection(connector, settings: Dict) -> Tuple[str, Dict]:
    """
    Detect PgBouncer in the connection path.

    Args:
        connector: PostgreSQL connector
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    # Self-opt-in: Skip if PgBouncer not detected/configured
    skip_result = skip_if_not_pgbouncer(settings)
    if skip_result:
        return skip_result

    builder = CheckContentBuilder()
    builder.h3("PgBouncer Detection")

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Multi-layered detection
        detection_result = _detect_pgbouncer(connector, settings)

        # Build output
        _build_detection_output(builder, detection_result, settings)

        # Build findings for trend storage
        findings = {
            'pgbouncer_detection': {
                'status': 'success',
                'timestamp': timestamp,
                'detected': detection_result.get('detected', False),
                'method': detection_result.get('method'),
                'version': detection_result.get('version'),
                'access_level': detection_result.get('access_level'),
                'host': detection_result.get('host'),
                'port': detection_result.get('port'),
                'details': detection_result.get('details', {})
            }
        }

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Failed to detect PgBouncer: {e}", exc_info=True)
        builder.critical_issue(
            "PgBouncer Detection Failed",
            [f"Detection failed: {str(e)}"]
        )

        findings = {
            'pgbouncer_detection': {
                'status': 'error',
                'timestamp': timestamp,
                'detected': False,
                'error': str(e)
            }
        }

        return builder.build(), findings


def _detect_pgbouncer(connector, settings: Dict) -> Dict:
    """
    Attempt to detect PgBouncer using multiple methods.

    Args:
        connector: PostgreSQL connector
        settings: Configuration dictionary

    Returns:
        Detection result dictionary
    """
    # Layer 1: Direct admin console connection (primary for self-hosted)
    admin_result = _detect_via_admin_console(settings)
    if admin_result.get('detected'):
        return admin_result

    # Layer 2: Connection pattern analysis
    pattern_result = _detect_via_connection_patterns(connector)
    if pattern_result.get('detected'):
        return pattern_result

    # Layer 3: Port scanning (supplementary)
    port_result = _detect_via_port_scan(settings)
    if port_result.get('detected'):
        return port_result

    # Not detected
    return {
        'detected': False,
        'method': 'none',
        'message': 'PgBouncer not detected',
        'attempted_methods': ['admin_console', 'connection_patterns', 'port_scan']
    }


def _detect_via_admin_console(settings: Dict) -> Dict:
    """
    Attempt detection via PgBouncer admin console.

    This is the primary detection method for self-hosted environments.

    Args:
        settings: Configuration dictionary

    Returns:
        Detection result dictionary
    """
    # Determine connection parameters
    pgbouncer_host = settings.get('pgbouncer_host') or settings.get('host')
    pgbouncer_port = settings.get('pgbouncer_port', 6432)
    pgbouncer_user = settings.get('pgbouncer_admin_user') or settings.get('user')
    pgbouncer_password = settings.get('pgbouncer_admin_password') or settings.get('password')
    timeout = settings.get('pgbouncer_timeout', 5)

    if not pgbouncer_host:
        return {'detected': False, 'method': 'admin_console', 'error': 'No host configured'}

    logger.info(f"Attempting PgBouncer detection on {pgbouncer_host}:{pgbouncer_port}")

    try:
        success, result = test_pgbouncer_connection(
            pgbouncer_host,
            pgbouncer_port,
            pgbouncer_user,
            pgbouncer_password,
            timeout
        )

        if success and result.get('detected'):
            return {
                'detected': True,
                'method': 'admin_console',
                'access_level': 'full_admin',
                'version': result.get('version'),
                'host': pgbouncer_host,
                'port': pgbouncer_port,
                'details': {
                    'admin_access': True,
                    'connection_successful': True
                }
            }
        else:
            logger.debug(f"Admin console detection failed: {result.get('error')}")
            return {
                'detected': False,
                'method': 'admin_console',
                'error': result.get('error'),
                'attempted_host': pgbouncer_host,
                'attempted_port': pgbouncer_port
            }

    except Exception as e:
        logger.debug(f"Admin console detection error: {e}")
        return {
            'detected': False,
            'method': 'admin_console',
            'error': str(e)
        }


def _detect_via_connection_patterns(connector) -> Dict:
    """
    Attempt detection by analyzing connection patterns in pg_stat_activity.

    Indicators of PgBouncer:
    - Limited number of source IPs (pooler hosts)
    - Application names containing "pgbouncer"
    - Connection age patterns (transaction pooling)

    Args:
        connector: PostgreSQL connector

    Returns:
        Detection result dictionary
    """
    try:
        cursor = connector.connection.cursor()

        # Look for pgbouncer in application names
        cursor.execute("""
            SELECT
                count(*) as conn_count,
                count(DISTINCT application_name) as app_count,
                count(DISTINCT client_addr) as ip_count
            FROM pg_stat_activity
            WHERE application_name ILIKE '%pgbouncer%'
               OR application_name ILIKE '%bouncer%'
        """)

        result = cursor.fetchone()
        if result and result[0] > 0:  # Found connections with pgbouncer in name
            return {
                'detected': True,
                'method': 'connection_patterns',
                'access_level': 'indirect',
                'details': {
                    'pgbouncer_connections': result[0],
                    'indicator': 'application_name contains pgbouncer'
                },
                'message': 'PgBouncer detected via connection analysis (limited monitoring available)'
            }

        # Look for pooling patterns (many clients, few source IPs)
        cursor.execute("""
            SELECT
                count(*) as total_connections,
                count(DISTINCT client_addr) as unique_ips
            FROM pg_stat_activity
            WHERE client_addr IS NOT NULL
              AND state != 'idle'
        """)

        result = cursor.fetchone()
        if result and result[0] > 10 and result[1] <= 3:
            # Many connections from few IPs might indicate pooling
            return {
                'detected': True,
                'method': 'connection_patterns',
                'access_level': 'indirect',
                'confidence': 'medium',
                'details': {
                    'total_connections': result[0],
                    'unique_source_ips': result[1],
                    'indicator': 'Connection pooling pattern detected'
                },
                'message': 'Possible connection pooler detected (might be PgBouncer, needs verification)'
            }

        cursor.close()
        return {'detected': False, 'method': 'connection_patterns'}

    except Exception as e:
        logger.debug(f"Connection pattern analysis failed: {e}")
        return {'detected': False, 'method': 'connection_patterns', 'error': str(e)}


def _detect_via_port_scan(settings: Dict) -> Dict:
    """
    Attempt detection by checking if PgBouncer port is open.

    This is a supplementary method - port being open doesn't confirm PgBouncer.

    Args:
        settings: Configuration dictionary

    Returns:
        Detection result dictionary
    """
    import socket

    pgbouncer_host = settings.get('pgbouncer_host') or settings.get('host')
    pgbouncer_port = settings.get('pgbouncer_port', 6432)
    timeout = settings.get('pgbouncer_timeout', 3)

    if not pgbouncer_host:
        return {'detected': False, 'method': 'port_scan'}

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((pgbouncer_host, pgbouncer_port))
        sock.close()

        if result == 0:
            # Port is open, but we can't confirm it's PgBouncer
            return {
                'detected': True,
                'method': 'port_scan',
                'access_level': 'unknown',
                'confidence': 'low',
                'host': pgbouncer_host,
                'port': pgbouncer_port,
                'details': {
                    'port_open': True
                },
                'message': f'Port {pgbouncer_port} is open (likely PgBouncer, but not confirmed)'
            }
        else:
            return {'detected': False, 'method': 'port_scan', 'port_closed': True}

    except Exception as e:
        logger.debug(f"Port scan failed: {e}")
        return {'detected': False, 'method': 'port_scan', 'error': str(e)}


def _build_detection_output(builder: CheckContentBuilder, detection: Dict, settings: Dict):
    """
    Build AsciiDoc output for PgBouncer detection results.

    Args:
        builder: CheckContentBuilder instance
        detection: Detection result dictionary
        settings: Configuration dictionary
    """
    detected = detection.get('detected', False)
    method = detection.get('method', 'unknown')

    builder.text("*Detection Status*")
    builder.blank()

    if detected:
        # Successfully detected
        version = detection.get('version', 'Unknown')
        access_level = detection.get('access_level', 'unknown')
        host = detection.get('host', 'N/A')
        port = detection.get('port', 'N/A')

        builder.text(f"Status: **Detected**")
        builder.text(f"Detection Method: `{method}`")

        if version and version != 'Unknown':
            builder.text(f"Version: `{version}`")

        if host != 'N/A':
            builder.text(f"Location: `{host}:{port}`")

        builder.text(f"Access Level: `{access_level}`")
        builder.blank()

        # Access level explanation
        if access_level == 'full_admin':
            builder.note("**Full Admin Access Available**\n\nComplete PgBouncer monitoring is enabled. All statistics, configuration, and health metrics are accessible.")
        elif access_level == 'indirect':
            builder.warning("**Limited Monitoring Available**\n\nPgBouncer detected indirectly. Full statistics require admin console access.\n\nTo enable full monitoring, configure PgBouncer admin credentials in your config file.")
        elif access_level == 'unknown':
            confidence = detection.get('confidence', 'low')
            builder.warning(f"**Detection Uncertain (Confidence: {confidence})**\n\n{detection.get('message', 'PgBouncer may be present but could not be confirmed.')}\n\nVerify PgBouncer installation and configure admin access for full monitoring.")

        # Additional details
        details = detection.get('details', {})
        if details:
            builder.text("*Detection Details*")
            builder.blank()
            for key, value in details.items():
                builder.text(f"• {key.replace('_', ' ').title()}: {value}")
            builder.blank()

    else:
        # Not detected - user must have explicit config (otherwise would have skipped)
        builder.text("Status: **Not Detected**")
        builder.blank()

        # Show what was attempted
        attempted = detection.get('attempted_methods', [])
        error_msg = detection.get('error', '')

        builder.warning(f"**PgBouncer Configuration Provided but Not Detected**\n\nAttempted detection methods: {', '.join(attempted)}\n\n{f'Last error: {error_msg}' if error_msg else 'No errors reported.'}")
        builder.blank()

        # Configuration guidance
        _add_configuration_guidance(builder, settings)


def _add_configuration_guidance(builder: CheckContentBuilder, settings: Dict):
    """
    Add configuration guidance for PgBouncer setup.

    Args:
        builder: CheckContentBuilder instance
        settings: Configuration dictionary
    """
    builder.text("*PgBouncer Configuration Guide*")
    builder.blank()

    builder.text("Verify these settings in your configuration file:")
    builder.blank()

    current_host = settings.get('pgbouncer_host') or settings.get('host', '<host>')
    current_port = settings.get('pgbouncer_port', 6432)
    current_user = settings.get('pgbouncer_admin_user') or settings.get('user', '<user>')

    builder.text("```yaml")
    builder.text(f"pgbouncer_host: {current_host}          # PgBouncer host")
    builder.text(f"pgbouncer_port: {current_port}                   # PgBouncer port (default: 6432)")
    builder.text(f"pgbouncer_admin_user: {current_user}        # Admin user")
    builder.text("pgbouncer_admin_password: <password>  # Admin password")
    builder.text("```")
    builder.blank()

    builder.text("*Testing PgBouncer Admin Console Access*")
    builder.blank()
    builder.text("To test admin console access manually:")
    builder.blank()
    builder.text("```bash")
    builder.text(f"psql -h {current_host} -p {current_port} -U {current_user} -d pgbouncer")
    builder.text("SHOW VERSION;")
    builder.text("SHOW POOLS;")
    builder.text("```")
    builder.blank()

    builder.tip("**Common Issues:**\n\n• Wrong port (default is 6432, not 5432)\n• User lacks admin privileges in pgbouncer.ini\n• PgBouncer not running\n• Firewall blocking connections\n• Wrong auth_type in pgbouncer.ini (try 'trust' or 'md5')")

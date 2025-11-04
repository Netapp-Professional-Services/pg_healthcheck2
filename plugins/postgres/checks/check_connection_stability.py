"""
Connection Stability Health Check

Monitors and reports on database connection health during health check execution.

Critical Issues Detected:
- Frequent reconnections (indicates unstable connection)
- Connection failures
- SSL/TLS issues
- Proxy (PgBouncer/HAProxy) connection problems

This check tracks how many times the connection was lost and had to be reestablished
during health check execution, which can indicate serious infrastructure problems.
"""

import logging
from datetime import datetime
from typing import Dict, Tuple
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def check_connection_stability(connector, settings: Dict) -> Tuple[str, Dict]:
    """
    Monitor connection stability during health check execution.

    Args:
        connector: PostgreSQL connector
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Connection Stability Analysis")

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        reconnection_count = connector.reconnection_count
        failures = connector.connection_failures

        # Determine severity
        if reconnection_count == 0:
            severity = 'healthy'
            health_score = 100
        elif reconnection_count <= 2:
            severity = 'warning'
            health_score = 80
        elif reconnection_count <= 10:
            severity = 'critical'
            health_score = 40
        else:
            severity = 'critical'
            health_score = 10

        # Build summary
        builder.text(f"*Connection Health Score*: {health_score}/100")
        builder.blank()
        builder.text(f"*Reconnections During Health Check*: {reconnection_count}")
        builder.blank()

        # Status message
        if reconnection_count == 0:
            builder.note("Connection remained stable throughout health check execution. No reconnections required.")
        elif reconnection_count <= 2:
            builder.warning(f"**Minor Connection Instability Detected**\\n\\nThe database connection was lost {reconnection_count} time(s) during health check execution and had to be reestablished.\\n\\n*This may indicate intermittent network issues or proxy instability.*")
        elif reconnection_count <= 10:
            builder.critical(f"**Significant Connection Instability Detected**\\n\\nThe database connection was lost **{reconnection_count} times** during health check execution.\\n\\n*This indicates a serious infrastructure problem that requires immediate investigation.*")
        else:
            builder.critical(f"**SEVERE Connection Instability Detected**\\n\\nThe database connection was lost **{reconnection_count} times** during health check execution.\\n\\n*This indicates critical infrastructure failure. The connection is completely unstable and may cause application failures.*")

        builder.blank()

        # Detailed analysis for issues
        if reconnection_count > 0:
            builder.text("*Connection Stability Issues*")
            builder.blank()

            # Check if connecting via proxy
            is_pgbouncer = bool(settings.get('pgbouncer_host'))
            proxy_type = "PgBouncer" if is_pgbouncer else "proxy"

            if is_pgbouncer:
                builder.text(f"**Detected Configuration**: Connecting via {proxy_type}")
                builder.text(f"   • Proxy Host: {settings.get('host')}:{settings.get('port')}")
                builder.text(f"   • Proxy Type: PgBouncer")
                builder.blank()

                builder.text("**Common PgBouncer Connection Failure Causes:**")
                builder.blank()
                builder.text("1. **SSL/TLS Issues**")
                builder.text("   • PgBouncer-to-PostgreSQL SSL connection failures")
                builder.text("   • Certificate validation problems")
                builder.text("   • SSL protocol mismatch")
                builder.blank()
                builder.text("2. **Backend PostgreSQL Issues**")
                builder.text("   • PostgreSQL server closing connections unexpectedly")
                builder.text("   • statement_timeout triggering")
                builder.text("   • max_connections exhaustion")
                builder.blank()
                builder.text("3. **Network Issues**")
                builder.text("   • Network instability between PgBouncer and PostgreSQL")
                builder.text("   • Firewall dropping idle connections")
                builder.text("   • DNS resolution problems")
                builder.blank()
                builder.text("4. **PgBouncer Configuration**")
                builder.text("   • server_idle_timeout too low")
                builder.text("   • query_timeout too restrictive")
                builder.text("   • Pool exhaustion")
                builder.blank()

                # Action items
                builder.text("**Immediate Investigation Steps:**")
                builder.blank()
                builder.text("1. Check PgBouncer logs:")
                builder.text("   ```bash")
                builder.text(f"   ssh {settings.get('host')} 'sudo tail -100 /var/log/pgbouncer/pgbouncer.log | grep -E \"close|disconnect|error\"'")
                builder.text("   ```")
                builder.blank()
                builder.text("2. Check PostgreSQL logs for connection errors:")
                builder.text("   ```bash")
                builder.text("   # Check logs on backend PostgreSQL servers")
                builder.text("   sudo tail -100 /var/log/postgresql/postgresql-*.log | grep -iE 'fatal|error|disconnect'")
                builder.text("   ```")
                builder.blank()
                builder.text("3. Verify PgBouncer SSL configuration:")
                builder.text("   ```bash")
                builder.text("   grep -E 'server_tls|client_tls' /etc/pgbouncer/pgbouncer.ini")
                builder.text("   ```")
                builder.blank()
                builder.text("4. Check PgBouncer pool status:")
                builder.text("   ```bash")
                builder.text(f"   psql -h {settings.get('host')} -p {settings.get('port')} -U pgbouncer -d pgbouncer -c 'SHOW POOLS;'")
                builder.text("   ```")
                builder.blank()
            else:
                builder.text("**Common Direct Connection Failure Causes:**")
                builder.blank()
                builder.text("• PostgreSQL server instability")
                builder.text("• Network issues")
                builder.text("• statement_timeout or connection timeout settings")
                builder.text("• max_connections exhaustion")
                builder.blank()

            # Show failure details if any
            if failures:
                builder.text("*Reconnection Failure Details*")
                builder.blank()
                failure_table = []
                for failure in failures[:10]:  # Show up to 10 failures
                    failure_table.append({
                        'Attempt': failure['attempt'],
                        'Timestamp': failure['timestamp'],
                        'Error': failure['error'][:80] + '...' if len(failure['error']) > 80 else failure['error']
                    })
                if failure_table:
                    builder.table(failure_table)
                    builder.blank()

        # Recommendations
        if reconnection_count > 0:
            builder.text("*Recommendations*")
            builder.blank()

            if reconnection_count > 10:
                builder.text("**CRITICAL - Immediate Action Required:**")
                builder.blank()
                builder.text("1. **Investigate infrastructure immediately** - connection is failing constantly")
                builder.text("2. **Check proxy/pooler logs** for backend connection failures")
                builder.text("3. **Verify network stability** between all connection hops")
                builder.text("4. **Review SSL/TLS configuration** if using encrypted connections")
                builder.text("5. **Consider temporarily bypassing proxy** to isolate the issue")
                builder.blank()
            elif reconnection_count > 2:
                builder.text("**High Priority:**")
                builder.blank()
                builder.text("1. Review proxy and database server logs for connection errors")
                builder.text("2. Check SSL/TLS configuration and certificate validity")
                builder.text("3. Verify network stability between connection hops")
                builder.text("4. Review timeout settings (connection_timeout, statement_timeout)")
                builder.blank()
            else:
                builder.text("**Low Priority:**")
                builder.blank()
                builder.text("1. Monitor for recurring connection issues")
                builder.text("2. Review logs if problem persists")
                builder.blank()

        # Build findings
        findings = {
            'connection_stability': {
                'status': 'success',
                'data': {
                    'timestamp': timestamp,
                    'health_score': health_score,
                    'severity': severity,
                    'reconnection_count': reconnection_count,
                    'connection_failures': failures,
                    'via_proxy': bool(settings.get('pgbouncer_host')),
                    'proxy_type': 'pgbouncer' if settings.get('pgbouncer_host') else None
                }
            }
        }

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Failed to check connection stability: {e}", exc_info=True)
        builder.error(f"Connection stability check failed: {str(e)}")

        findings = {
            'connection_stability': {
                'status': 'error',
                'timestamp': timestamp,
                'error': str(e)
            }
        }

        return builder.build(), findings

"""
PgBouncer Health Monitoring Check

Monitors PgBouncer pool health, performance, and identifies issues.

Self-Opt-In Behavior:
- Skips silently if PgBouncer not detected AND no explicit config
- Runs full monitoring if PgBouncer found OR user provided config

Critical Metrics:
- Pool utilization and exhaustion
- Client wait queues (cl_waiting)
- Wait times (maxwait)
- Connection balance (clients vs servers)
- Transaction/query performance

Common Issues Detected:
- Pool exhaustion (clients waiting for connections)
- Undersized pools
- High wait times
- Connection imbalance
- Performance degradation

Outputs actionable recommendations with specific configuration changes.
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.pgbouncer_helpers import skip_if_not_pgbouncer
from plugins.postgres.utils.pgbouncer_client import PgBouncerClient

logger = logging.getLogger(__name__)


def check_pgbouncer_health(connector, settings: Dict) -> Tuple[str, Dict]:
    """
    Monitor PgBouncer health and performance.

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
    builder.h3("PgBouncer Health Status")

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Collect health metrics
        health_data = _collect_health_metrics(settings)

        if not health_data.get('success'):
            builder.error(f"Could not collect PgBouncer health metrics: {health_data.get('error')}")

            findings = {
                'pgbouncer_health': {
                    'status': 'error',
                    'timestamp': timestamp,
                    'error': health_data.get('error')
                }
            }
            return builder.build(), findings

        # Analyze health
        analysis = _analyze_health(health_data)

        # Check if any queries failed through PgBouncer and used fallback
        fallback_stats = connector.get_fallback_stats()
        if fallback_stats['count'] > 0:
            analysis['fallback_events'] = fallback_stats
            # Reduce health score for fallback usage
            analysis['health_score'] = max(0, analysis['health_score'] - (fallback_stats['count'] * 10))
            # Add critical issue
            analysis['issues'].insert(0, {
                'type': 'pgbouncer_query_failures',
                'severity': 'critical',
                'count': fallback_stats['count'],
                'message': f"{fallback_stats['count']} queries failed through PgBouncer but succeeded via direct connection",
                'recommendation': "PgBouncer is not functioning correctly. Investigate PgBouncer logs and connection pool status immediately."
            })

        # Check for connection reconnections (indicates PgBouncer connection instability)
        reconnection_count = connector.reconnection_count
        if reconnection_count > 0:
            analysis['reconnection_events'] = {
                'count': reconnection_count,
                'failures': connector.connection_failures
            }

            # Determine severity based on reconnection count
            if reconnection_count >= 20:
                severity = 'critical'
                score_penalty = 90  # Severely unstable
            elif reconnection_count >= 10:
                severity = 'critical'
                score_penalty = 60  # Very unstable
            elif reconnection_count >= 5:
                severity = 'high'
                score_penalty = 40  # Significantly unstable
            else:
                severity = 'warning'
                score_penalty = 20  # Minor instability

            # Reduce health score
            analysis['health_score'] = max(0, analysis['health_score'] - score_penalty)

            # Add critical issue
            analysis['issues'].insert(0, {
                'type': 'pgbouncer_connection_instability',
                'severity': severity,
                'count': reconnection_count,
                'message': f"PgBouncer connection dropped {reconnection_count} times during health check execution",
                'recommendation': "Critical PgBouncer connection instability detected. Check PgBouncer logs, verify backend PostgreSQL server health, and investigate SSL/TLS configuration and network stability."
            })

        # Build output with actionable advice
        _build_health_output(builder, health_data, analysis)

        # Build findings for trend storage
        findings = {
            'pgbouncer_health': {
                'status': 'success',
                'data': {
                    'timestamp': timestamp,
                    'health_score': analysis['health_score'],
                    'pools': health_data.get('pools', []),
                    'stats': health_data.get('stats', []),
                    'config': health_data.get('config', {}),
                    'analysis': analysis,
                    'version': health_data.get('version')
                }
            }
        }

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Failed to check PgBouncer health: {e}", exc_info=True)
        builder.error(f"Health check failed: {str(e)}")

        findings = {
            'pgbouncer_health': {
                'status': 'error',
                'timestamp': timestamp,
                'error': str(e)
            }
        }

        return builder.build(), findings


# Removed _should_skip_health_check - now using skip_if_not_pgbouncer() from pgbouncer_helpers


def _collect_health_metrics(settings: Dict) -> Dict:
    """
    Collect comprehensive health metrics from PgBouncer.

    Args:
        settings: Configuration dictionary

    Returns:
        Dictionary with collected metrics
    """
    pgbouncer_host = settings.get('pgbouncer_host') or settings.get('host')
    pgbouncer_port = settings.get('pgbouncer_port', 6432)
    pgbouncer_user = settings.get('pgbouncer_admin_user') or settings.get('user')
    pgbouncer_password = settings.get('pgbouncer_admin_password') or settings.get('password')
    timeout = settings.get('pgbouncer_timeout', 5)

    client = PgBouncerClient(
        pgbouncer_host,
        pgbouncer_port,
        pgbouncer_user,
        pgbouncer_password,
        timeout=timeout
    )

    try:
        # Get comprehensive status
        status = client.get_comprehensive_status()

        if status.get('errors'):
            logger.warning(f"Some PgBouncer metrics could not be collected: {status['errors']}")

        return {
            'success': True,
            'version': status.get('version'),
            'config': status.get('config', {}),
            'stats': status.get('stats', []),
            'pools': status.get('pools', []),
            'databases': status.get('databases', []),
            'clients_count': status.get('clients_count', 0),
            'servers_count': status.get('servers_count', 0),
            'lists': status.get('lists', []),
            'collection_errors': status.get('errors', [])
        }

    except Exception as e:
        logger.error(f"Could not collect PgBouncer metrics: {e}")
        return {'success': False, 'error': str(e)}

    finally:
        client.close()


def _analyze_health(health_data: Dict) -> Dict:
    """
    Analyze PgBouncer health and identify issues.

    Args:
        health_data: Collected health metrics

    Returns:
        Analysis dictionary with health score and issues
    """
    issues = []
    health_score = 100
    pools = health_data.get('pools') or []
    config = health_data.get('config') or {}
    stats = health_data.get('stats') or []

    # Get configuration values (handle None config gracefully)
    pool_mode = config.get('pool_mode', 'session') if config else 'session'
    default_pool_size = int(config.get('default_pool_size', 20)) if config else 20
    reserve_pool_size = int(config.get('reserve_pool_size', 0)) if config else 0
    max_client_conn = int(config.get('max_client_conn', 100)) if config else 100

    # Analyze each pool
    for pool in pools:
        database = pool.get('database', 'unknown')
        user = pool.get('user', 'unknown')
        cl_waiting = int(pool.get('cl_waiting', 0))
        cl_active = int(pool.get('cl_active', 0))
        sv_active = int(pool.get('sv_active', 0))
        sv_idle = int(pool.get('sv_idle', 0))
        sv_used = int(pool.get('sv_used', 0))
        maxwait = float(pool.get('maxwait', 0))
        pool_mode_override = pool.get('pool_mode')

        # Critical: Pool exhaustion (clients waiting)
        if cl_waiting > 0:
            severity = 'critical' if cl_waiting > 10 else 'high' if cl_waiting > 5 else 'warning'
            issues.append({
                'type': 'pool_exhaustion',
                'severity': severity,
                'database': database,
                'user': user,
                'cl_waiting': cl_waiting,
                'message': f"{cl_waiting} client(s) waiting for connections in pool {database}/{user}",
                'recommendation': f"URGENT: Increase pool size. Current pool appears undersized. Consider increasing default_pool_size from {default_pool_size} to {default_pool_size + 10}."
            })
            health_score -= 20 if severity == 'critical' else 15 if severity == 'high' else 10

        # Critical: High wait time
        if maxwait > 5.0:  # 5 seconds
            issues.append({
                'type': 'high_wait_time',
                'severity': 'critical',
                'database': database,
                'user': user,
                'maxwait': maxwait,
                'message': f"Clients waiting {maxwait:.2f} seconds for connections in pool {database}/{user}",
                'recommendation': "URGENT: This indicates severe pool exhaustion or slow query execution. Increase pool size immediately and investigate slow queries."
            })
            health_score -= 25

        # High utilization (potential issue)
        total_server_conn = sv_active + sv_idle + sv_used
        if default_pool_size > 0:
            utilization = total_server_conn / default_pool_size
            if utilization > 0.9:  # 90% utilization
                issues.append({
                    'type': 'high_pool_utilization',
                    'severity': 'warning',
                    'database': database,
                    'user': user,
                    'utilization': utilization,
                    'message': f"Pool {database}/{user} at {utilization*100:.0f}% utilization",
                    'recommendation': f"Pool is near capacity. Consider increasing pool size before exhaustion occurs. Add reserve_pool_size = {max(5, default_pool_size // 4)} for burst capacity."
                })
                health_score -= 10

        # Connection imbalance
        total_clients = cl_active + cl_waiting
        if total_clients > total_server_conn * 10 and total_server_conn > 0:
            issues.append({
                'type': 'connection_imbalance',
                'severity': 'info',
                'database': database,
                'user': user,
                'ratio': total_clients / total_server_conn if total_server_conn > 0 else 0,
                'message': f"{total_clients} clients but only {total_server_conn} server connections in pool {database}/{user}",
                'recommendation': f"High client-to-server ratio ({total_clients}:{total_server_conn}). This is normal for transaction pooling but monitor for wait times."
            })

    # Configuration issues
    if reserve_pool_size == 0 and max_client_conn > default_pool_size * 5:
        issues.append({
            'type': 'no_reserve_pool',
            'severity': 'warning',
            'message': "No reserve pool configured",
            'recommendation': f"Add reserve_pool_size = {max(5, default_pool_size // 2)} to handle traffic bursts without exhausting pools."
        })
        health_score -= 10

    # Pool mode considerations
    if pool_mode == 'session':
        issues.append({
            'type': 'pool_mode_session',
            'severity': 'info',
            'message': "Pool mode is 'session' - least efficient but most compatible",
            'recommendation': "If your application is stateless (no prepared statements, temp tables, or session variables), consider switching to 'transaction' mode for better connection reuse."
        })

    return {
        'health_score': max(0, health_score),
        'issues': issues,
        'pool_count': len(pools),
        'total_waiting_clients': sum(int(p.get('cl_waiting', 0)) for p in pools),
        'max_wait_time': max((float(p.get('maxwait', 0)) for p in pools), default=0)
    }


def _build_health_output(
    builder: CheckContentBuilder,
    health_data: Dict,
    analysis: Dict
):
    """
    Build AsciiDoc output for PgBouncer health with actionable advice.

    Args:
        builder: CheckContentBuilder instance
        health_data: Collected health metrics
        analysis: Analysis results
    """
    health_score = analysis['health_score']
    pools = health_data.get('pools', [])
    config = health_data.get('config', {})
    version = health_data.get('version', 'Unknown')

    # Health Score
    builder.text(f"*PgBouncer Version*: {version}")
    builder.blank()
    builder.text("*Overall Health Score*")
    builder.blank()
    builder.text(f"Score: *{health_score}/100*")
    builder.blank()

    if health_score >= 90:
        builder.note("PgBouncer is healthy and operating normally.")
    elif health_score >= 70:
        builder.warning("PgBouncer has minor issues that should be investigated.")
    else:
        builder.critical("**PgBouncer has serious issues requiring immediate attention.**\n\nPool exhaustion or high wait times can cause application failures and performance degradation.")

    builder.blank()

    # Pool Statistics
    if pools:
        builder.text("*Pool Status*")
        builder.blank()

        # Get pool size for utilization calculation
        default_pool_size = int(config.get('default_pool_size', 20)) if config else 20

        pool_table = []
        for pool in pools:
            database = pool.get('database', 'unknown')
            user = pool.get('user', 'unknown')
            cl_active = pool.get('cl_active', 0)
            cl_waiting = pool.get('cl_waiting', 0)
            sv_active = pool.get('sv_active', 0)
            sv_idle = pool.get('sv_idle', 0)
            sv_used = pool.get('sv_used', 0)
            maxwait = pool.get('maxwait', 0)

            # Calculate utilization (% of pool size in use)
            total_server = sv_active + sv_idle + sv_used
            utilization = f"{int(total_server / default_pool_size * 100)}%" if default_pool_size > 0 else "N/A"

            # Wait status
            wait_status = "OK"
            if cl_waiting > 10:
                wait_status = "CRITICAL"
            elif cl_waiting > 0:
                wait_status = "WARNING"

            pool_table.append({
                'Database': database,
                'User': user,
                'Clients': f"{cl_active}",
                'Waiting': f"{cl_waiting}",
                'Servers Active': f"{sv_active}",
                'Servers Idle': f"{sv_idle}",
                'Utilization': utilization,
                'Max Wait': f"{maxwait:.1f}s",
                'Status': wait_status
            })

        builder.table(pool_table)
        builder.blank()

    # Configuration Summary
    builder.text("*Configuration*")
    builder.blank()

    config_items = [
        {'Parameter': 'Pool Mode', 'Value': config.get('pool_mode', 'unknown')},
        {'Parameter': 'Default Pool Size', 'Value': config.get('default_pool_size', 'unknown')},
        {'Parameter': 'Reserve Pool Size', 'Value': config.get('reserve_pool_size', '0')},
        {'Parameter': 'Max Client Connections', 'Value': config.get('max_client_conn', 'unknown')},
        {'Parameter': 'Server Idle Timeout', 'Value': f"{config.get('server_idle_timeout', 'unknown')}s"},
    ]

    builder.table(config_items)
    builder.blank()

    # Fallback Events Section (if any)
    fallback_events = analysis.get('fallback_events')
    if fallback_events and fallback_events['count'] > 0:
        builder.text("*🚨 PgBouncer Connection Failures*")
        builder.blank()
        builder.critical(f"**{fallback_events['count']} queries failed through PgBouncer** but succeeded via direct connection.\\n\\nThis indicates PgBouncer is NOT functioning correctly. Your application would experience failures if direct fallback was not configured.")
        builder.blank()

        builder.text("*Failed Queries:*")
        builder.blank()
        for i, event in enumerate(fallback_events['queries'][:5], 1):  # Show first 5
            builder.text(f"{i}. Query: `{event['query']}`")
            builder.text(f"   Error: {event['primary_error']}")
            builder.text(f"   Time: {event['timestamp']}")
            builder.blank()

        if fallback_events['count'] > 5:
            builder.text(f"... and {fallback_events['count'] - 5} more failed queries")
            builder.blank()

    # Reconnection Events Section (if any)
    reconnection_events = analysis.get('reconnection_events')
    if reconnection_events and reconnection_events['count'] > 0:
        reconnection_count = reconnection_events['count']
        builder.text("*🚨 PgBouncer Connection Instability*")
        builder.blank()

        # Severity-appropriate message
        if reconnection_count >= 20:
            builder.critical(f"**SEVERE: PgBouncer connection dropped {reconnection_count} times** during health check execution.\\n\\nThis indicates critical PgBouncer infrastructure failure. The connection is completely unstable and will cause application failures.")
        elif reconnection_count >= 10:
            builder.critical(f"**PgBouncer connection dropped {reconnection_count} times** during health check execution.\\n\\nThis indicates significant PgBouncer connection instability requiring immediate investigation.")
        elif reconnection_count >= 5:
            builder.warning(f"**PgBouncer connection dropped {reconnection_count} times** during health check execution.\\n\\nThis indicates notable PgBouncer connection instability.")
        else:
            builder.warning(f"**PgBouncer connection dropped {reconnection_count} times** during health check execution.\\n\\nMinor connection instability detected.")

        builder.blank()

        # Show failure details if available
        failures = reconnection_events.get('failures', [])
        if failures:
            builder.text("*Reconnection Failure Details:*")
            builder.blank()
            for i, failure in enumerate(failures[:5], 1):  # Show first 5
                builder.text(f"{i}. Attempt: {failure.get('attempt', 'N/A')}")
                builder.text(f"   Time: {failure.get('timestamp', 'N/A')}")
                error_msg = failure.get('error', 'Unknown error')
                if len(error_msg) > 100:
                    error_msg = error_msg[:100] + '...'
                builder.text(f"   Error: {error_msg}")
                builder.blank()

            if len(failures) > 5:
                builder.text(f"... and {len(failures) - 5} more reconnection failures")
                builder.blank()

        builder.text("*Common Causes of PgBouncer Connection Instability:*")
        builder.blank()
        builder.text("1. SSL/TLS issues between PgBouncer and PostgreSQL backend")
        builder.text("2. Backend PostgreSQL server closing connections unexpectedly")
        builder.text("3. Network instability or firewall dropping connections")
        builder.text("4. PgBouncer configuration issues (timeouts too aggressive)")
        builder.text("5. max_connections exhaustion on backend PostgreSQL")
        builder.blank()

    # Issues and Recommendations
    issues = analysis.get('issues', [])
    if issues:
        # Group by severity
        critical_issues = [i for i in issues if i.get('severity') == 'critical']
        high_issues = [i for i in issues if i.get('severity') == 'high']
        warning_issues = [i for i in issues if i.get('severity') == 'warning']
        info_issues = [i for i in issues if i.get('severity') == 'info']

        if critical_issues:
            builder.text("*🔴 Critical Issues*")
            builder.blank()
            for issue in critical_issues:
                builder.text(f"*{issue['type'].replace('_', ' ').title()}*")
                builder.text(f"   {issue['message']}")
                builder.text(f"   *Action*: {issue['recommendation']}")
                builder.blank()

        if high_issues:
            builder.text("*🟡 High Priority Issues*")
            builder.blank()
            for issue in high_issues:
                builder.text(f"*{issue['type'].replace('_', ' ').title()}*")
                builder.text(f"   {issue['message']}")
                builder.text(f"   *Action*: {issue['recommendation']}")
                builder.blank()

        if warning_issues:
            builder.text("*⚠️  Warnings*")
            builder.blank()
            for issue in warning_issues:
                builder.text(f"*{issue['type'].replace('_', ' ').title()}*")
                builder.text(f"   {issue['message']}")
                builder.text(f"   *Recommendation*: {issue['recommendation']}")
                builder.blank()

        if info_issues:
            builder.text("*ℹ️  Informational*")
            builder.blank()
            for issue in info_issues:
                builder.text(f"• {issue['message']}")
                if issue.get('recommendation'):
                    builder.text(f"  {issue['recommendation']}")
            builder.blank()

    else:
        builder.note("No issues detected. PgBouncer is operating optimally.")

    # Actionable guidance
    if health_score < 90:
        builder.text("*How to Fix Issues*")
        builder.blank()
        builder.text("To modify PgBouncer configuration:")
        builder.blank()
        builder.text("1. Edit `/etc/pgbouncer/pgbouncer.ini`")
        builder.text("2. Update parameters in the `[pgbouncer]` section")
        builder.text("3. Reload configuration:")
        builder.text("   ```bash")
        builder.text("   sudo systemctl reload pgbouncer")
        builder.text("   # or")
        builder.text("   psql -h localhost -p 6432 -U pgbouncer -d pgbouncer -c 'RELOAD'")
        builder.text("   ```")
        builder.blank()

    # Best practices
    builder.text("*PgBouncer Best Practices*")
    builder.blank()
    builder.tip("**Connection Pooling Tips:**\n\n• Monitor pool utilization regularly\n• Set reserve_pool_size for traffic bursts\n• Use transaction mode when application supports it\n• Keep server_idle_timeout reasonable (600s default)\n• Monitor client wait queues (cl_waiting should always be 0)\n• Pool size formula: (max_connections * 0.75) / number_of_app_servers")

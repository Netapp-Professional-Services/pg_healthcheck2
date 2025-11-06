"""
Kafka File Descriptor Usage Check (Prometheus - Instaclustr)

Monitors file descriptor usage from Instaclustr Prometheus endpoints.
File descriptor exhaustion is a common cause of Kafka broker crashes.

Health Check: prometheus_file_descriptors
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Metrics:
- ic_node_filedescriptoropencount - Current open file descriptors
- ic_node_filedescriptorlimit - Maximum file descriptor limit

CRITICAL IMPORTANCE:
- Kafka uses FDs for: log files, network connections, internal files
- FD exhaustion causes immediate broker crash
- Common with many partitions (2 FDs per partition) + many connections
- No graceful degradation - instant failure when limit reached
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10). High priority - FD exhaustion kills brokers."""
    return 9


def check_prometheus_file_descriptors(connector, settings):
    """
    Check file descriptor usage via Prometheus (Instaclustr managed service).

    Monitors:
    - Open file descriptor count
    - File descriptor limit
    - Usage percentage

    Thresholds:
    - WARNING: > 70% of limit
    - CRITICAL: > 85% of limit

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("File Descriptor Usage (Prometheus)")

    if not settings.get('instaclustr_prometheus_enabled'):
        findings = {
            'status': 'skipped',
            'reason': 'Prometheus monitoring not enabled',
            'data': [],
            'metadata': {
                'source': 'prometheus',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }
        return builder.build(), findings

    try:
        from plugins.common.prometheus_client import get_instaclustr_client

        client = get_instaclustr_client(
            cluster_id=settings['instaclustr_cluster_id'],
            username=settings['instaclustr_prometheus_username'],
            api_key=settings['instaclustr_prometheus_api_key'],
            prometheus_base_url=settings.get('instaclustr_prometheus_base_url')
        )

        all_metrics = client.scrape_all_nodes()

        if not all_metrics:
            builder.error("❌ No metrics available from Prometheus")
            findings = {
                'status': 'error',
                'error_message': 'No metrics available from Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        fd_open_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_filedescriptoropencount$')
        fd_limit_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_filedescriptorlimit$')

        if not fd_open_metrics or not fd_limit_metrics:
            builder.error("❌ File descriptor metrics not found")
            findings = {
                'status': 'error',
                'error_message': 'File descriptor metrics not found in Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Get thresholds
        warning_pct = settings.get('kafka_fd_warning_pct', 70)
        critical_pct = settings.get('kafka_fd_critical_pct', 85)

        # Process metrics by broker
        broker_data = {}

        for metric in fd_open_metrics:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')

            if node_id not in broker_data:
                broker_data[node_id] = {
                    'node_id': node_id,
                    'public_ip': target_labels.get('PublicIp', 'unknown'),
                    'rack': target_labels.get('Rack', 'unknown'),
                    'datacenter': target_labels.get('ClusterDataCenterName', 'unknown')
                }

            broker_data[node_id]['fd_open'] = int(metric['value'])

        for metric in fd_limit_metrics:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')

            if node_id in broker_data:
                broker_data[node_id]['fd_limit'] = int(metric['value'])

        if not broker_data:
            builder.error("❌ No broker data available")
            findings = {
                'status': 'error',
                'error_message': 'No broker data available',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Calculate percentages and identify issues
        node_data = list(broker_data.values())
        critical_fd = []
        warning_fd = []

        for broker in node_data:
            fd_open = broker.get('fd_open', 0)
            fd_limit = broker.get('fd_limit', 1)
            fd_pct = (fd_open / fd_limit * 100) if fd_limit > 0 else 0
            broker['fd_usage_pct'] = round(fd_pct, 1)

            if fd_pct >= critical_pct:
                critical_fd.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'fd_open': fd_open,
                    'fd_limit': fd_limit,
                    'fd_usage_pct': round(fd_pct, 1)
                })
            elif fd_pct >= warning_pct:
                warning_fd.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'fd_open': fd_open,
                    'fd_limit': fd_limit,
                    'fd_usage_pct': round(fd_pct, 1)
                })

        # Determine overall status
        if critical_fd:
            status = 'critical'
            severity = 10
            message = f"🔴 CRITICAL: {len(critical_fd)} broker(s) near FD exhaustion (>{critical_pct}%)"
        elif warning_fd:
            status = 'warning'
            severity = 7
            message = f"⚠️  {len(warning_fd)} broker(s) with high FD usage (>{warning_pct}%)"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ File descriptor usage healthy across {len(node_data)} brokers"

        # Build structured findings
        avg_fd_pct = sum(b['fd_usage_pct'] for b in node_data) / len(node_data)

        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_fd': {
                'status': status,
                'data': node_data,
                'metadata': {
                    'source': 'prometheus',
                    'metrics': ['filedescriptoropencount', 'filedescriptorlimit'],
                    'broker_count': len(node_data)
                }
            },
            'cluster_aggregate': {
                'avg_fd_usage_pct': round(avg_fd_pct, 1),
                'brokers_critical': len(critical_fd),
                'brokers_warning': len(warning_fd),
                'broker_count': len(node_data),
                'thresholds': {
                    'warning_pct': warning_pct,
                    'critical_pct': critical_pct
                }
            }
        }

        if critical_fd:
            findings['critical_fd_usage'] = {
                'count': len(critical_fd),
                'brokers': critical_fd,
                'recommendation': 'URGENT: Broker near FD exhaustion - increase ulimit or reduce connections/partitions'
            }

        if warning_fd:
            findings['warning_fd_usage'] = {
                'count': len(warning_fd),
                'brokers': warning_fd,
                'recommendation': 'Monitor FD usage - plan to increase ulimit'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*🚨 CRITICAL: FILE DESCRIPTOR EXHAUSTION IMMINENT 🚨*")
            builder.blank()
            builder.text("When FD limit is reached, broker will crash instantly with 'Too many open files' error.")
            builder.text("This is one of the most common causes of Kafka outages.")
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*")
        builder.text(f"- Average FD Usage: {round(avg_fd_pct, 1)}%")
        builder.text(f"- Brokers at Risk: {len(critical_fd)} critical, {len(warning_fd)} warning")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.blank()

        builder.text("*Per-Broker FD Usage:*")
        for broker in sorted(node_data, key=lambda x: x['fd_usage_pct'], reverse=True):
            status_icon = "🔴" if broker['fd_usage_pct'] >= critical_pct else "⚠️" if broker['fd_usage_pct'] >= warning_pct else "✅"
            builder.text(
                f"{status_icon} Broker {broker['node_id'][:8]}: "
                f"{broker['fd_open']:,}/{broker['fd_limit']:,} FDs ({broker['fd_usage_pct']}%)"
            )
        builder.blank()

        if critical_fd or warning_fd:
            recommendations = {
                "critical" if critical_fd else "high": [
                    "🚨 FILE DESCRIPTOR EXHAUSTION PREVENTION",
                    "Immediate Actions:",
                    "  1. Increase OS file descriptor limit: ulimit -n 100000",
                    "  2. Make permanent: Add to /etc/security/limits.conf:",
                    "     kafka soft nofile 100000",
                    "     kafka hard nofile 100000",
                    "  3. Restart broker (requires downtime) or wait for next restart",
                    "  4. Monitor FD growth rate",
                    "",
                    "FD Usage Breakdown (Kafka typically uses):",
                    "  • ~2 FDs per partition (log segment files)",
                    "  • 1 FD per active network connection",
                    "  • Internal files (state, indexes, temp files)",
                    "  • Example: 1000 partitions + 1000 connections ≈ 3000 FDs minimum",
                    "",
                    "Reduce FD Usage:",
                    "  • Reduce partition count (rebalance or delete unused topics)",
                    "  • Limit max.connections.per.ip",
                    "  • Enable socket.send.buffer.bytes and socket.receive.buffer.bytes tuning",
                    "  • Review if idle connections can be closed",
                    "",
                    "Typical Limits:",
                    "  • Default OS limit: 1024 (too low for Kafka!)",
                    "  • Recommended production: 100,000 - 1,000,000",
                    "  • Instaclustr default: Usually 1,048,576",
                    "",
                    "Check current limits on broker:",
                    "  cat /proc/<kafka-pid>/limits | grep 'open files'",
                    "  lsof -p <kafka-pid> | wc -l  # actual usage"
                ],
                "general": [
                    "Prevention Best Practices:",
                    "  • Always set ulimit -n to at least 100,000 for Kafka",
                    "  • Monitor FD usage as % of limit, not absolute count",
                    "  • Alert at 70% (warning) and 85% (critical)",
                    "  • Plan for 2x current usage as buffer",
                    "  • Consider: (2 × num_partitions) + (1.5 × max_connections) = min FD limit"
                ]
            }

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus file descriptor check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {
                'source': 'prometheus',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }
        return builder.build(), findings

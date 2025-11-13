"""
Kafka File Descriptor Usage Check (Unified Adaptive)

Monitors file descriptor usage using adaptive collection strategy.
File descriptor exhaustion is a common cause of Kafka broker crashes.

Health Check: prometheus_file_descriptors
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- ic_node_filedescriptoropencount / kafka_server_kafkaserver_filedescriptoropencount
- ic_node_filedescriptorlimit (Instaclustr only, calculated from OS for others)

CRITICAL IMPORTANCE:
- Kafka uses FDs for: log files, network connections, internal files
- FD exhaustion causes immediate broker crash
- Common with many partitions (2 FDs per partition) + many connections
- No graceful degradation - instant failure when limit reached
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10). High priority - FD exhaustion kills brokers."""
    return 9


def check_prometheus_file_descriptors(connector, settings):
    """
    Check file descriptor usage via adaptive collection strategy.

    Monitors:
    - Open file descriptor count
    - File descriptor limit (when available)
    - Usage percentage

    Thresholds:
    - WARNING: > 70% of limit
    - CRITICAL: > 85% of limit

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("File Descriptor Usage (Prometheus)")

    try:
        # Get metric definition
        fd_metric_def = get_metric_definition('file_descriptors')
        if not fd_metric_def:
            builder.error("❌ File descriptor metric definition not found")
            findings = {
                'status': 'error',
                'error_message': 'Metric definition not found',
                'data': [],
                'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Collect FD usage metric adaptively
        fd_result = collect_metric_adaptive(fd_metric_def, connector, settings)

        if not fd_result:
            builder.warning(
                "⚠️ Could not collect file descriptor metrics\n\n"
                "*Tried collection methods:*\n"
                "1. Instaclustr Prometheus API - Not configured or unavailable\n"
                "2. Local Prometheus JMX exporter - Not found or SSH unavailable\n"
                "3. Standard JMX - Not available or SSH unavailable\n\n"
                "*To enable monitoring, configure one of:*\n"
                "• Instaclustr Prometheus: Set `instaclustr_prometheus_enabled: true`\n"
                "• Local Prometheus exporter: Ensure JMX exporter running on brokers\n"
                "• Standard JMX: Enable JMX on port 9999 and configure SSH access"
            )
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect file descriptor metrics using any method',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = fd_result.get('method')
        node_metrics = fd_result.get('node_metrics', {})
        metadata = fd_result.get('metadata', {})

        if not node_metrics:
            builder.error("❌ No broker data available")
            findings = {
                'status': 'error',
                'error_message': 'No broker data available',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Get thresholds
        warning_pct = settings.get('kafka_fd_warning_pct', 70)
        critical_pct = settings.get('kafka_fd_critical_pct', 85)

        # Process broker data
        # NOTE: file_descriptors metric already returns usage percentage from metric_collection_strategies
        node_data = []
        critical_fd = []
        warning_fd = []

        for node_host, fd_usage_pct in node_metrics.items():
            broker_entry = {
                'node_id': node_host,
                'host': node_host,
                'fd_usage_pct': round(fd_usage_pct, 1)
            }
            node_data.append(broker_entry)

            if fd_usage_pct >= critical_pct:
                critical_fd.append(broker_entry)
            elif fd_usage_pct >= warning_pct:
                warning_fd.append(broker_entry)

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

        # Calculate cluster aggregate
        avg_fd_pct = fd_result.get('cluster_avg', 0)

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_fd': node_data,
                'cluster_aggregate': {
                    'avg_fd_usage_pct': round(avg_fd_pct, 1),
                    'brokers_critical': len(critical_fd),
                    'brokers_warning': len(warning_fd),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'warning_pct': warning_pct,
                        'critical_pct': critical_pct
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['file_descriptor_usage_pct'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        if critical_fd:
            findings['data']['critical_fd_usage'] = {
                'count': len(critical_fd),
                'brokers': critical_fd,
                'recommendation': 'URGENT: Broker near FD exhaustion - increase ulimit or reduce connections/partitions'
            }

        if warning_fd:
            findings['data']['warning_fd_usage'] = {
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
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker FD Usage:*")
        for broker in sorted(node_data, key=lambda x: x['fd_usage_pct'], reverse=True):
            status_icon = "🔴" if broker['fd_usage_pct'] >= critical_pct else "⚠️" if broker['fd_usage_pct'] >= warning_pct else "✅"
            builder.text(
                f"{status_icon} Broker {broker['node_id']}: {broker['fd_usage_pct']}% usage"
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
        logger.error(f"File descriptor check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

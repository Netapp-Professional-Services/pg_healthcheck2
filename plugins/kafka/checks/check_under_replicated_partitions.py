"""
Kafka Under-Replicated Partitions Check (Unified Adaptive)

Priority: 10 (CRITICAL - Data Loss Risk)

Monitors under-replicated partitions using best available collection method:
1. Instaclustr Prometheus API (if enabled)
2. Local Prometheus JMX exporter via SSH (if available)
3. Standard JMX via SSH (fallback)

Under-replicated partitions are partitions where one or more replicas are not
in-sync with the leader. This indicates replication failures and means the
cluster is ONE BROKER FAILURE away from data loss.

Replaces both:
- prometheus_under_replicated.py (Instaclustr Prometheus)
- Previous JMX-only version
"""

from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, get_metric_collection_error_message
from plugins.common.metric_collection_strategies import (
    collect_metric_adaptive,
    get_collection_method_description
)
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition


def run_under_replicated_check(connector, settings):
    """
    Check for under-replicated partitions using adaptive collection.

    This is a CRITICAL check (Priority 10) that detects replication failures.
    Under-replicated partitions mean the cluster is one failure away from data loss.

    Returns:
        tuple: (adoc_content: str, structured_findings: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Under-Replicated Partitions")

    # Get metric definition
    metric_def = get_metric_definition('under_replicated_partitions')
    if not metric_def:
        builder.error("❌ Metric definition not found")
        return builder.build(), {'status': 'error', 'reason': 'no_metric_definition'}

    # Collect metric using adaptive strategy
    data = collect_metric_adaptive(metric_def, connector, settings)

    if not data:
        error_msg = get_metric_collection_error_message(connector, "under-replicated partitions metric")
        builder.warning(error_msg)

        return builder.build(), {
            'status': 'skipped',
            'reason': 'no_collection_method_available'
        }

    # Extract metrics
    node_metrics = data.get('node_metrics', {})
    cluster_total = data.get('cluster_total', 0)
    node_count = data.get('node_count', 0)
    method = data.get('method')

    # Get thresholds
    thresholds = metric_def.get('thresholds', {})
    warning_threshold = thresholds.get('warning', 0)
    critical_threshold = thresholds.get('critical', 10)

    # Determine severity
    if cluster_total >= critical_threshold:
        status = 'critical'
        severity = 10
    elif cluster_total > warning_threshold:
        status = 'warning'
        severity = 7
    else:
        status = 'healthy'
        severity = 0

    # Build AsciiDoc output
    if status == 'critical':
        builder.critical(f"🚨 CRITICAL: {int(cluster_total)} under-replicated partition(s) detected!")
        builder.blank()
        builder.text("*Why This Is Critical:*")
        builder.text("Under-replicated partitions are ONE BROKER FAILURE away from data loss.")
        builder.text("If the leader broker fails, data that was not replicated will be PERMANENTLY LOST.")
        builder.blank()
    elif status == 'warning':
        builder.warning(f"⚠️ WARNING: {int(cluster_total)} under-replicated partition(s) detected")
        builder.blank()
    else:
        builder.success(f"✅ No under-replicated partitions ({node_count} nodes checked)")
        builder.blank()

    # Cluster summary
    builder.text("*Cluster Summary:*")
    builder.text(f"- Total Under-Replicated Partitions: {int(cluster_total)}")
    builder.text(f"- Nodes with URPs: {sum(1 for v in node_metrics.values() if v > 0)}/{node_count}")
    builder.text(f"- Warning Threshold: >{warning_threshold}")
    builder.text(f"- Critical Threshold: ≥{critical_threshold}")
    builder.text(f"- Collection Method: {get_collection_method_description(method)}")
    builder.blank()

    # Per-node breakdown
    if node_metrics:
        builder.text("*Per-Node Breakdown:*")
        for node_id, urp_count in sorted(node_metrics.items(), key=lambda x: x[1], reverse=True):
            status_icon = "🔴" if urp_count >= critical_threshold else "⚠️" if urp_count > warning_threshold else "✅"
            builder.text(f"{status_icon} {node_id}: {int(urp_count)} under-replicated partition(s)")
        builder.blank()

    # Recommendations for critical/warning
    if status in ['critical', 'warning']:
        builder.text("*Immediate Actions:*")
        builder.text("1. **Check broker health** - Are all brokers running and reachable?")
        builder.text("2. **Check network connectivity** - Can brokers communicate with each other?")
        builder.text("3. **Review broker logs** - Look for replication errors or resource issues")
        builder.text("4. **Check disk space** - Full disks prevent replication")
        builder.text("5. **Monitor ISR (In-Sync Replicas)** - Check ISR shrink rate")
        builder.blank()

        builder.text("*Common Causes:*")
        builder.text("• Broker down or unreachable")
        builder.text("• Network issues between brokers")
        builder.text("• Disk full on follower brokers")
        builder.text("• Slow disks causing replication lag")
        builder.text("• Resource exhaustion (CPU, memory)")
        builder.text("• Configuration issues (replica.lag.time.max.ms too low)")
        builder.blank()

        builder.text("*How to Investigate:*")
        builder.text("```bash")
        builder.text("# Check which partitions are under-replicated")
        builder.text("/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \\")
        builder.text("  --describe --under-replicated-partitions")
        builder.text("")
        builder.text("# Check ISR for specific topic")
        builder.text("/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \\")
        builder.text("  --describe --topic YOUR_TOPIC")
        builder.text("```")

    # Build structured findings for rules engine
    structured_data = {
        'status': status,
        'severity': severity,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'collection_method': method,
        'data': {
            # Fields for rules engine
            'total_urp': int(cluster_total),
            'nodes_with_urp': sum(1 for v in node_metrics.values() if v > 0),
            'node_count': node_count,
            'warning_threshold': warning_threshold,
            'critical_threshold': critical_threshold,
            # Metadata
            'node_metrics': {str(k): float(v) for k, v in node_metrics.items()},
            'collection_metadata': data.get('metadata', {})
        }
    }

    return builder.build(), structured_data

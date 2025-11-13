"""
Kafka Offline Partitions Check (Unified Adaptive)

Priority: 10 (CRITICAL - Data Unavailable)

Monitors offline partitions using best available collection method:
1. Instaclustr Prometheus API (if enabled)
2. Local Prometheus JMX exporter via SSH (if available)
3. Standard JMX via SSH (fallback)

Offline partitions are partitions with NO in-sync replicas - meaning data is
COMPLETELY UNAVAILABLE. This is more severe than under-replicated partitions.

Note: This is a controller-only metric. Only the controller broker reports this value.

Replaces both:
- prometheus_offline_partitions.py (Instaclustr Prometheus)
- Previous JMX-only version
"""

from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import (
    collect_metric_adaptive,
    get_collection_method_description
)
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition


def run_offline_partitions_check(connector, settings):
    """
    Check for offline partitions using adaptive collection.

    This is a CRITICAL check (Priority 10) that detects complete data unavailability.
    Offline partitions mean NO replicas are in-sync - data is inaccessible.

    Note: Controller-only metric in ZooKeeper. For KRaft, uses per-broker alternative.

    Returns:
        tuple: (adoc_content: str, structured_findings: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Offline Partitions")

    # For KRaft, use offline_replica_count (per-broker metric that works)
    if connector.is_kraft_mode():
        builder.note("ℹ️ Using KRaft-compatible per-broker metric (offline_replica_count)")
        builder.blank()
        metric_def = get_metric_definition('offline_replica_count')
    else:
        # ZooKeeper mode - use controller metric
        metric_def = get_metric_definition('offline_partitions')

    if not metric_def:
        builder.error("❌ Metric definition not found")
        return builder.build(), {'status': 'error', 'reason': 'no_metric_definition'}

    # Collect metric using adaptive strategy
    data = collect_metric_adaptive(metric_def, connector, settings)

    if not data:
        builder.warning("⚠️ Could not collect offline partitions metric")
        builder.blank()
        builder.text("*Tried collection methods:*")
        builder.text("1. Instaclustr Prometheus API - Not configured or unavailable")
        builder.text("2. Local Prometheus JMX exporter - Not found or SSH unavailable")
        builder.text("3. Standard JMX - Not available or SSH unavailable")
        builder.blank()
        builder.text("*Note:* This is a controller-only metric - only the controller broker reports it.")
        builder.blank()
        builder.text("*To enable monitoring, configure one of:*")
        builder.text("• Instaclustr Prometheus: Set `instaclustr_prometheus_enabled: true`")
        builder.text("• Local Prometheus exporter: Ensure JMX exporter running on brokers")
        builder.text("• Standard JMX: Enable JMX on port 9999 and configure SSH access")

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
    critical_threshold = thresholds.get('critical', 0)

    # Determine severity
    # Any offline partitions is CRITICAL
    if cluster_total > critical_threshold:
        status = 'critical'
        severity = 10
    else:
        status = 'healthy'
        severity = 0

    # Find controller host (node with non-zero value, or first node)
    controller_host = None
    for node_id, value in node_metrics.items():
        if value >= 0:  # Controller reports this metric
            controller_host = node_id
            break

    # Build AsciiDoc output
    if status == 'critical':
        builder.critical(f"🔴 CRITICAL: {int(cluster_total)} offline partition(s) - data completely unavailable!")
        builder.blank()
        builder.text("*🚨 SEVERE DATA OUTAGE 🚨*")
        builder.blank()
        builder.text("*What This Means:*")
        builder.text("Offline partitions have ZERO in-sync replicas. This means:")
        builder.text("• Data in these partitions is COMPLETELY INACCESSIBLE")
        builder.text("• Producers cannot write to these partitions")
        builder.text("• Consumers cannot read from these partitions")
        builder.text("• This is worse than under-replicated - it's a complete outage")
        builder.blank()
    else:
        builder.success(f"✅ No offline partitions ({node_count} nodes checked)")
        builder.blank()

    # Cluster summary
    builder.text("*Status:*")
    builder.text(f"- Offline Partitions: {int(cluster_total)}")
    if controller_host:
        builder.text(f"- Controller Node: {controller_host}")
    builder.text(f"- Nodes Checked: {node_count}")
    builder.text(f"- Collection Method: {get_collection_method_description(method)}")
    builder.blank()

    # Per-node breakdown (if multiple nodes)
    if len(node_metrics) > 1:
        builder.text("*Per-Node Values:*")
        for node_id, value in sorted(node_metrics.items()):
            if value >= 0:
                builder.text(f"• {node_id}: {int(value)} offline partition(s)")
        builder.blank()

    # Recommendations for critical
    if status == 'critical':
        builder.text("*IMMEDIATE ACTIONS REQUIRED:*")
        builder.text("")
        builder.text("1. **Identify offline partitions:**")
        builder.text("   ```bash")
        builder.text("   /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \\")
        builder.text("     --describe --unavailable-partitions")
        builder.text("   ```")
        builder.blank()

        builder.text("2. **Check broker status:**")
        builder.text("   ```bash")
        builder.text("   # Are all brokers running?")
        builder.text("   ps aux | grep kafka")
        builder.text("")
        builder.text("   # Check broker logs for errors")
        builder.text("   tail -100 /var/log/kafka/server.log | grep -i error")
        builder.text("   ```")
        builder.blank()

        builder.text("3. **Review cluster metadata:**")
        builder.text("   ```bash")
        builder.text("   /opt/kafka/bin/kafka-broker-api-versions.sh \\")
        builder.text("     --bootstrap-server localhost:9092")
        builder.text("   ```")
        builder.blank()

        builder.text("*Common Causes of Offline Partitions:*")
        builder.text("• **All replica brokers are down** - Check if specific brokers crashed")
        builder.text("• **Network partition** - Brokers can't communicate")
        builder.text("• **Disk failures** - All replicas lost their data")
        builder.text("• **Insufficient replicas** - min.insync.replicas > available replicas")
        builder.text("• **ZooKeeper/KRaft issues** - Controller can't manage partitions")
        builder.blank()

        builder.text("*Recovery Steps:*")
        builder.text("1. Restart down brokers if they crashed")
        builder.text("2. Check and fix network connectivity")
        builder.text("3. Verify disk health on replica brokers")
        builder.text("4. If data is lost, consider unclean leader election (DATA LOSS RISK):")
        builder.text("   ```bash")
        builder.text("   /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server localhost:9092 \\")
        builder.text("     --election-type UNCLEAN --all-topic-partitions")
        builder.text("   ```")
        builder.text("   ⚠️ WARNING: Unclean leader election WILL CAUSE DATA LOSS")

    # Build structured findings for rules engine
    structured_data = {
        'status': status,
        'severity': severity,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'collection_method': method,
        'data': {
            # Fields for rules engine
            'offline_count': int(cluster_total),
            'controller_host': controller_host,
            'node_count': node_count,
            'critical_threshold': critical_threshold,
            # Metadata
            'node_metrics': {str(k): float(v) for k, v in node_metrics.items()},
            'collection_metadata': data.get('metadata', {})
        }
    }

    return builder.build(), structured_data

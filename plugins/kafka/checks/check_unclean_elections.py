"""
Kafka Unclean Leader Elections Check (Unified Adaptive)

Priority: 10 (CRITICAL - Data Loss Event)

Monitors unclean leader elections using best available collection method:
1. Instaclustr Prometheus API (if enabled)
2. Local Prometheus JMX exporter via SSH (if available)
3. Standard JMX via SSH (fallback)

An unclean leader election occurs when a broker is elected leader WITH LESS DATA
than other replicas. This results in PERMANENT DATA LOSS for messages that were
not replicated.

Note: This is a controller-only metric. Only the controller broker reports this value.
The metric is cumulative since broker start.

Replaces both:
- prometheus_unclean_elections.py (Instaclustr Prometheus)
- Previous JMX-only version
"""

from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, get_metric_collection_error_message
from plugins.common.metric_collection_strategies import (
    collect_metric_adaptive,
    get_collection_method_description
)
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition


def run_unclean_elections_check(connector, settings):
    """
    Check for unclean leader elections using adaptive collection.

    This is a CRITICAL check (Priority 10) that detects DATA LOSS EVENTS.
    Unclean leader elections mean data that was written but not replicated
    has been PERMANENTLY LOST.

    Note: This metric is only available on the controller broker in ZooKeeper mode.
    In KRaft mode (Kafka 3.x+), this metric does not exist as the election
    mechanism is fundamentally different.

    Returns:
        tuple: (adoc_content: str, structured_findings: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Unclean Leader Elections")

    # Check if cluster is in KRaft mode
    if connector.is_kraft_mode():
        builder.note("ℹ️ This check is not applicable to KRaft mode (Kafka 3.x+)")
        builder.blank()
        builder.text("*Why:*")
        builder.text("• KRaft uses a different controller election mechanism than ZooKeeper")
        builder.text("• The concept of 'unclean leader elections' applies to partition leader elections")
        builder.text("• In KRaft, controller elections use Raft consensus (always clean)")
        builder.text("• Partition leader elections still exist but are tracked differently")
        builder.blank()
        builder.text("*KRaft Leader Election:*")
        builder.text("• KRaft maintains a quorum-based controller cluster")
        builder.text("• Leader elections for partitions still follow ISR rules")
        builder.text("• Monitor ISR shrinks and partition health instead")
        builder.text("• The `unclean.leader.election.enable` config still exists for partitions")
        builder.blank()
        builder.text("*Recommended Monitoring for KRaft:*")
        builder.text("• ISR shrink rate - Detects replication lag issues")
        builder.text("• Under-replicated partitions - Identifies replication problems")
        builder.text("• Offline partitions - Critical availability issues")

        return builder.build(), {
            'status': 'skipped',
            'reason': 'kraft_mode',
            'kafka_mode': 'kraft',
            'message': 'Unclean leader elections metric not available in KRaft mode'
        }

    # Get metric definition
    metric_def = get_metric_definition('unclean_leader_elections')
    if not metric_def:
        builder.error("❌ Metric definition not found")
        return builder.build(), {'status': 'error', 'reason': 'no_metric_definition'}

    # Collect metric using adaptive strategy
    data = collect_metric_adaptive(metric_def, connector, settings)

    if not data:
        error_msg = get_metric_collection_error_message(connector, "unclean leader elections metric")
        error_msg += "\n\n*Note:* This is a controller-only metric - only the controller broker reports it."
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
    critical_threshold = thresholds.get('critical', 0)

    # Determine severity
    # Any unclean elections is CRITICAL (indicates past data loss)
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
        builder.critical(f"🚨 DATA LOSS EVENT DETECTED 🚨")
        builder.blank()
        builder.text(f"*{int(cluster_total)} unclean leader election(s) have occurred since broker start*")
        builder.blank()
        builder.text("*What This Means:*")
        builder.text("An unclean leader election occurs when:")
        builder.text("• A partition leader fails")
        builder.text("• NO in-sync replicas (ISR) are available")
        builder.text("• Kafka elects a leader WITH LESS DATA than the failed leader had")
        builder.text("• Messages that were written to the old leader but NOT replicated are PERMANENTLY LOST")
        builder.blank()
        builder.text("*Why This Happened:*")
        builder.text("• Likely cause: All ISR members became unavailable simultaneously")
        builder.text("• Config setting: unclean.leader.election.enable=true (allows data loss for availability)")
        builder.text("• Alternative: If false, partitions would have gone offline instead (no data loss, but unavailable)")
        builder.blank()
    else:
        builder.success(f"✅ No unclean leader elections ({node_count} nodes checked)")
        builder.blank()
        builder.text("This is expected in healthy production. Unclean elections indicate past data loss events.")
        builder.blank()

    # Cluster summary
    builder.text("*Status:*")
    builder.text(f"- Unclean Leader Elections (cumulative): {int(cluster_total)}")
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
                builder.text(f"• {node_id}: {int(value)} unclean election(s)")
        builder.blank()

    # Configuration info
    builder.text("*Configuration:*")
    builder.text("• `unclean.leader.election.enable` - Controls whether unclean elections are allowed")
    builder.text("• **true** = Prioritize availability over data consistency (allows data loss)")
    builder.text("• **false** = Prioritize data consistency over availability (partitions go offline)")
    builder.blank()

    # Recommendations for critical
    if status == 'critical':
        builder.text("*POST-INCIDENT ACTIONS:*")
        builder.blank()
        builder.text("1. **Identify affected partitions** (requires broker logs):")
        builder.text("   ```bash")
        builder.text("   # Search for unclean election events in logs")
        builder.text("   grep -i 'unclean leader election' /var/log/kafka/server.log")
        builder.text("   grep -i 'electing leader.*not in isr' /var/log/kafka/server.log")
        builder.text("   ```")
        builder.blank()

        builder.text("2. **Investigate root cause:**")
        builder.text("   • Why did all ISR members become unavailable?")
        builder.text("   • Network partition?")
        builder.text("   • Multiple broker failures?")
        builder.text("   • Configuration issue (min.insync.replicas)?")
        builder.blank()

        builder.text("3. **Review replication configuration:**")
        builder.text("   ```bash")
        builder.text("   /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \\")
        builder.text("     --describe --entity-type topics")
        builder.text("   ```")
        builder.text("   Check: min.insync.replicas, replication.factor")
        builder.blank()

        builder.text("4. **Consider changing unclean.leader.election.enable:**")
        builder.text("   • If data consistency is critical: Set to **false**")
        builder.text("   • If availability is critical: Keep as **true** (but accept data loss risk)")
        builder.text("   ```bash")
        builder.text("   /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \\")
        builder.text("     --alter --entity-type brokers --entity-default \\")
        builder.text("     --add-config unclean.leader.election.enable=false")
        builder.text("   ```")
        builder.blank()

        builder.text("*Prevention Strategies:*")
        builder.text("• Increase replication.factor (minimum 3)")
        builder.text("• Set min.insync.replicas appropriately (typically replication.factor - 1)")
        builder.text("• Use acks=all for producers to ensure replication")
        builder.text("• Monitor ISR shrinks to detect replication issues early")
        builder.text("• Ensure brokers are in different availability zones")
        builder.text("• Implement proper capacity planning to prevent cascading failures")

    # Build structured findings for rules engine
    structured_data = {
        'status': status,
        'severity': severity,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'collection_method': method,
        'data': {
            # Fields for rules engine
            'unclean_count': int(cluster_total),
            'controller_host': controller_host,
            'node_count': node_count,
            'critical_threshold': critical_threshold,
            # Metadata
            'node_metrics': {str(k): float(v) for k, v in node_metrics.items()},
            'collection_metadata': data.get('metadata', {})
        }
    }

    return builder.build(), structured_data

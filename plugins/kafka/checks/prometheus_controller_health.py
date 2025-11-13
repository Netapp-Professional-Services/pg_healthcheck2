"""
Kafka KRaft Controller Health Check (Unified Adaptive)

Monitors KRaft controller health using adaptive collection strategy.
KRaft (Kafka Raft) replaces ZooKeeper for metadata management starting in Kafka 3.x.

Health Check: prometheus_controller_health
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- active_controller_count - Should sum to 1 cluster-wide (exactly one active controller)
- fenced_broker_count - Brokers that are fenced (isolated from cluster)
- metadata_error_count - KRaft metadata errors

CRITICAL IMPORTANCE:
- Zero active controllers = cluster metadata operations unavailable
- Multiple active controllers = split-brain condition (catastrophic)
- Fenced brokers indicate network partitioning or broker failures
- Metadata errors can lead to cluster instability

NOTE: This check only applies to KRaft-mode clusters (Kafka 3.x+)
For ZooKeeper-based clusters, metrics will not be available.
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def check_prometheus_controller_health(connector, settings):
    """
    Check KRaft controller health via adaptive collection strategy.

    Monitors:
    - Active controller count (should be exactly 1 cluster-wide)
    - Fenced broker count (should be 0)
    - Metadata error count (should be 0)

    Thresholds:
    - CRITICAL: active_controller_count != 1 (cluster-wide sum)
    - CRITICAL: fenced_broker_count > 0
    - WARNING: metadata_error_count > 0

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Controller Health - KRaft (Prometheus)")

    try:
        # Get metric definitions
        active_controller_def = get_metric_definition('active_controller')
        fenced_broker_def = get_metric_definition('fenced_broker_count')
        metadata_error_def = get_metric_definition('metadata_error_count')

        # Collect metrics adaptively
        active_result = collect_metric_adaptive(active_controller_def, connector, settings) if active_controller_def else None
        fenced_result = collect_metric_adaptive(fenced_broker_def, connector, settings) if fenced_broker_def else None
        error_result = collect_metric_adaptive(metadata_error_def, connector, settings) if metadata_error_def else None

        # If none collected, check may not be applicable
        if not any([active_result, fenced_result, error_result]):
            builder.text("ℹ️  KRaft controller metrics not available")
            builder.blank()
            builder.text("*Note:* This check only applies to KRaft-mode clusters (Kafka 3.x+).")
            builder.text("If you're using ZooKeeper-based Kafka, this is expected.")
            findings = {
                'status': 'info',
                'message': 'KRaft controller metrics not available (cluster may use ZooKeeper)',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = (active_result or fenced_result or error_result).get('method')
        active_metrics = active_result.get('node_metrics', {}) if active_result else {}
        fenced_metrics = fenced_result.get('node_metrics', {}) if fenced_result else {}
        error_metrics = error_result.get('node_metrics', {}) if error_result else {}

        # Combine broker data
        all_hosts = set(active_metrics.keys()) | set(fenced_metrics.keys()) | set(error_metrics.keys())
        node_data = []

        for host in all_hosts:
            active_count = int(active_metrics.get(host, 0))
            fenced_count = int(fenced_metrics.get(host, 0))
            error_count = int(error_metrics.get(host, 0))

            broker_entry = {
                'node_id': host,
                'host': host,
                'active_controllers': active_count,
                'fenced_brokers': fenced_count,
                'metadata_errors': error_count
            }
            node_data.append(broker_entry)

        if not node_data:
            builder.text("ℹ️  No KRaft controller data available")
            findings = {
                'status': 'info',
                'message': 'No KRaft controller data',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Calculate cluster-wide totals
        total_active_controllers = sum(b['active_controllers'] for b in node_data)
        total_fenced_brokers = sum(b['fenced_brokers'] for b in node_data)
        total_metadata_errors = sum(b['metadata_errors'] for b in node_data)

        # Determine status based on controller health
        issues = []

        # CRITICAL: Wrong number of active controllers
        if total_active_controllers == 0:
            status = 'critical'
            severity = 10
            issues.append("NO ACTIVE CONTROLLER - metadata operations unavailable")
        elif total_active_controllers > 1:
            status = 'critical'
            severity = 10
            issues.append(f"SPLIT BRAIN: {total_active_controllers} active controllers (should be exactly 1)")
        elif total_fenced_brokers > 0:
            status = 'critical'
            severity = 9
            issues.append(f"{total_fenced_brokers} fenced broker(s)")
        elif total_metadata_errors > 0:
            status = 'warning'
            severity = 6
            issues.append(f"{total_metadata_errors} metadata error(s)")
        else:
            status = 'healthy'
            severity = 0
            issues.append("KRaft controller healthy")

        message = " - ".join(issues)

        # Add status emoji
        if status == 'critical':
            message = "🔴 " + message
        elif status == 'warning':
            message = "⚠️  " + message
        else:
            message = "✅ " + message

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_controller': node_data,
                'cluster_aggregate': {
                    'total_active_controllers': total_active_controllers,
                    'total_fenced_brokers': total_fenced_brokers,
                    'total_metadata_errors': total_metadata_errors,
                    'broker_count': len(node_data),
                    'expected_active_controllers': 1
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['active_controller', 'fenced_broker_count', 'metadata_error_count'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            if total_active_controllers == 0:
                builder.text("*🚨 CRITICAL: No Active Controller 🚨*")
                builder.text("Without an active controller, the cluster cannot:")
                builder.text("  • Create/delete topics")
                builder.text("  • Assign partitions")
                builder.text("  • Elect partition leaders")
                builder.text("  • Process metadata changes")
                builder.blank()
            elif total_active_controllers > 1:
                builder.text("*🚨 CRITICAL: Split-Brain Condition 🚨*")
                builder.text(f"Multiple active controllers detected ({total_active_controllers}).")
                builder.text("This can lead to:")
                builder.text("  • Conflicting metadata updates")
                builder.text("  • Data inconsistency")
                builder.text("  • Cluster instability")
                builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*KRaft Controller Status:*")
        builder.text(f"- Active Controllers (cluster-wide): {total_active_controllers} (expected: 1)")
        builder.text(f"- Fenced Brokers: {total_fenced_brokers}")
        builder.text(f"- Metadata Errors: {total_metadata_errors}")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        # Show per-broker details if interesting
        if total_active_controllers != 1 or total_fenced_brokers > 0 or total_metadata_errors > 0:
            builder.text("*Per-Broker Controller Metrics:*")
            for broker in node_data:
                controller_indicator = "🟢 ACTIVE CONTROLLER" if broker['active_controllers'] > 0 else ""
                fenced_indicator = "🔴 FENCED" if broker['fenced_brokers'] > 0 else ""
                error_indicator = f"⚠️  {broker['metadata_errors']} errors" if broker['metadata_errors'] > 0 else ""

                indicators = " ".join(filter(None, [controller_indicator, fenced_indicator, error_indicator]))
                builder.text(f"- Broker {broker['node_id']}: {indicators if indicators else '✅ Healthy'}")
            builder.blank()

        # Add recommendations if issues found
        if status in ['critical', 'warning']:
            recommendations = {}

            if status == 'critical':
                if total_active_controllers == 0:
                    recommendations["critical"] = [
                        "No Active Controller - Immediate Actions:",
                        "  1. Check KRaft quorum status across controller nodes",
                        "  2. Review controller logs for errors",
                        "  3. Verify network connectivity between controller nodes",
                        "  4. Check if controller nodes are running",
                        "  5. Review KRaft metadata log for corruption",
                        "",
                        "KRaft requires a quorum of controller nodes to elect a leader.",
                        "If majority of controllers are down, cluster cannot function."
                    ]
                elif total_active_controllers > 1:
                    recommendations["critical"] = [
                        "Split-Brain Condition - Immediate Actions:",
                        "  1. STOP WRITES to cluster immediately if possible",
                        "  2. Identify network partition or timing issue",
                        "  3. Check KRaft quorum election logs",
                        "  4. Verify controller node clocks are synchronized (NTP)",
                        "  5. Contact Kafka support - this is a severe issue",
                        "",
                        "Split-brain can cause permanent data inconsistencies.",
                        "Do not restart brokers until root cause is identified."
                    ]
                elif total_fenced_brokers > 0:
                    recommendations["critical"] = [
                        "Fenced Brokers - Immediate Actions:",
                        "  1. Check network connectivity to fenced brokers",
                        "  2. Review fenced broker logs for errors",
                        "  3. Check if fenced brokers can reach controller",
                        "  4. Verify broker process is running",
                        "  5. Check for disk/resource issues on fenced brokers",
                        "",
                        "Fenced brokers are isolated from cluster:",
                        "  • Cannot serve client requests",
                        "  • Partitions may be under-replicated",
                        "  • Data loss risk if multiple brokers fenced"
                    ]

            if status == 'warning':
                recommendations["high"] = [
                    "Metadata Errors - Actions:",
                    "  1. Review KRaft metadata logs on controller nodes",
                    "  2. Check for disk corruption or I/O errors",
                    "  3. Monitor error count - increasing is concerning",
                    "  4. Review recent metadata operations (topic/partition changes)",
                    "  5. Consider restarting controller nodes one by one"
                ]

            recommendations["general"] = [
                "KRaft Controller Best Practices:",
                "  • Run 3 or 5 controller nodes (odd number for quorum)",
                "  • Separate controller nodes from broker nodes (dedicated controllers)",
                "  • Ensure low-latency network between controllers",
                "  • Monitor controller election metrics",
                "  • Keep controller nodes synchronized with NTP",
                "",
                "KRaft vs ZooKeeper:",
                "  • KRaft: Built-in metadata management (Kafka 3.x+)",
                "  • ZooKeeper: External metadata management (Kafka < 3.x)",
                "  • KRaft is simpler, faster, more scalable",
                "  • Migration from ZooKeeper to KRaft supported in 3.x",
                "",
                "Active Controller:",
                "  • Exactly 1 active controller required cluster-wide",
                "  • Controller manages all metadata operations",
                "  • Elected by KRaft quorum",
                "  • Failover automatic when controller fails"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Controller health check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

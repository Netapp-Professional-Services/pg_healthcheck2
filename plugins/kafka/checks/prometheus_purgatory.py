"""
Kafka Purgatory Size Check (Unified Adaptive)

Monitors produce and fetch purgatory sizes using adaptive collection strategy.
Large purgatory sizes indicate requests waiting for acknowledgment or data.

Health Check: prometheus_purgatory
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- ic_node_produce_purgatory_size / kafka_server_delayedoperationpurgatory_purgatorysize{delayedOperation="Produce"}
- ic_node_fetch_purgatory_size / kafka_server_delayedoperationpurgatory_purgatorysize{delayedOperation="Fetch"}

IMPORTANCE:
- High produce purgatory: Producers waiting for acks (acks=all with slow replicas)
- High fetch purgatory: Consumers/followers waiting for data (fetch.min.bytes not met)
- Can indicate replication lag or consumer configuration issues
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 6


def check_prometheus_purgatory(connector, settings):
    """
    Check purgatory sizes via adaptive collection strategy.

    Monitors:
    - Produce purgatory size (requests waiting for acks)
    - Fetch purgatory size (fetch requests waiting for data)

    Thresholds:
    - WARNING: > 100 requests
    - CRITICAL: > 500 requests

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Purgatory Size (Prometheus)")

    try:
        # Get metric definitions
        produce_purg_def = get_metric_definition('purgatory_produce')
        fetch_purg_def = get_metric_definition('purgatory_fetch')

        if not produce_purg_def or not fetch_purg_def:
            builder.error("❌ Purgatory metric definitions not found")
            findings = {
                'status': 'error',
                'error_message': 'Metric definitions not found',
                'data': [],
                'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Collect both metrics adaptively
        produce_result = collect_metric_adaptive(produce_purg_def, connector, settings)
        fetch_result = collect_metric_adaptive(fetch_purg_def, connector, settings)

        if not produce_result and not fetch_result:
            builder.warning(
                "⚠️ Could not collect purgatory metrics\n\n"
                "*Tried collection methods:*\n"
                "1. Instaclustr Prometheus API - Not configured or unavailable\n"
                "2. Local Prometheus JMX exporter - Not found or SSH unavailable\n"
                "3. Standard JMX - Not available or SSH unavailable"
            )
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect purgatory metrics using any method',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = produce_result.get('method') if produce_result else fetch_result.get('method')
        produce_metrics = produce_result.get('node_metrics', {}) if produce_result else {}
        fetch_metrics = fetch_result.get('node_metrics', {}) if fetch_result else {}

        # Get thresholds
        produce_warning = settings.get('kafka_produce_purgatory_warning', 100)
        produce_critical = settings.get('kafka_produce_purgatory_critical', 500)
        fetch_warning = settings.get('kafka_fetch_purgatory_warning', 100)
        fetch_critical = settings.get('kafka_fetch_purgatory_critical', 500)

        # Combine broker data
        all_hosts = set(produce_metrics.keys()) | set(fetch_metrics.keys())
        node_data = []
        critical_brokers = []
        warning_brokers = []

        for host in all_hosts:
            produce_size = produce_metrics.get(host, 0)
            fetch_size = fetch_metrics.get(host, 0)

            broker_entry = {
                'node_id': host,
                'host': host,
                'produce_purgatory_size': int(produce_size),
                'fetch_purgatory_size': int(fetch_size)
            }
            node_data.append(broker_entry)

            # Check thresholds
            if produce_size >= produce_critical or fetch_size >= fetch_critical:
                critical_brokers.append(broker_entry)
            elif produce_size >= produce_warning or fetch_size >= fetch_warning:
                warning_brokers.append(broker_entry)

        if not node_data:
            builder.error("❌ No broker data available")
            findings = {
                'status': 'error',
                'error_message': 'No broker data available',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Determine overall status
        if critical_brokers:
            status = 'critical'
            severity = 8
            message = f"🔴 {len(critical_brokers)} broker(s) with critical purgatory sizes"
        elif warning_brokers:
            status = 'warning'
            severity = 6
            message = f"⚠️  {len(warning_brokers)} broker(s) with high purgatory sizes"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ Purgatory sizes healthy across {len(node_data)} brokers"

        # Calculate cluster aggregates
        avg_produce = sum(b['produce_purgatory_size'] for b in node_data) / len(node_data)
        avg_fetch = sum(b['fetch_purgatory_size'] for b in node_data) / len(node_data)

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_purgatory': node_data,
                'cluster_aggregate': {
                    'avg_produce_purgatory_size': round(avg_produce, 1),
                    'avg_fetch_purgatory_size': round(avg_fetch, 1),
                    'brokers_critical': len(critical_brokers),
                    'brokers_warning': len(warning_brokers),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'produce_warning': produce_warning,
                        'produce_critical': produce_critical,
                        'fetch_warning': fetch_warning,
                        'fetch_critical': fetch_critical
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['produce_purgatory_size', 'fetch_purgatory_size'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Produce Purgatory: {round(avg_produce, 1)} requests")
        builder.text(f"- Avg Fetch Purgatory: {round(avg_fetch, 1)} requests")
        builder.text(f"- Brokers with Issues: {len(critical_brokers)} critical, {len(warning_brokers)} warning")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        if critical_brokers or warning_brokers:
            builder.text("*Brokers with High Purgatory Sizes:*")
            for broker in critical_brokers + warning_brokers:
                symbol = "🔴" if broker in critical_brokers else "⚠️"
                builder.text(
                    f"{symbol} Broker {broker['node_id']}: "
                    f"Produce {broker['produce_purgatory_size']}, Fetch {broker['fetch_purgatory_size']}"
                )
            builder.blank()

            recommendations = {
                "high": [
                    "Purgatory Size Issues:",
                    "",
                    "High Produce Purgatory:",
                    "  • Indicates producers waiting for acks (acks=all)",
                    "  • Check replication lag - slow replicas delay acks",
                    "  • Review min.insync.replicas setting",
                    "  • Consider reducing acks to 1 for lower latency (less durability)",
                    "",
                    "High Fetch Purgatory:",
                    "  • Consumers/followers waiting for fetch.min.bytes",
                    "  • Reduce fetch.min.bytes on consumers (trade bandwidth for latency)",
                    "  • Check if brokers are low-traffic (normal for idle clusters)",
                    "  • Ensure fetch.max.wait.ms is reasonable (default: 500ms)",
                    "",
                    "General Actions:",
                    "  • Monitor purgatory growth rate over time",
                    "  • Check for disk I/O bottlenecks causing delays",
                    "  • Review network latency between brokers"
                ],
                "general": [
                    "Typical healthy values:",
                    "  • Produce purgatory: < 100 requests",
                    "  • Fetch purgatory: < 100 requests (can be higher on idle clusters)"
                ]
            }
            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Purgatory check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

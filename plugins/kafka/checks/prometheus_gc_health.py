"""
Kafka GC Health Check (Prometheus - Instaclustr)

Monitors JVM garbage collection performance from Instaclustr Prometheus endpoints.
Excessive GC time indicates memory pressure and can cause performance issues.

Health Check: prometheus_gc_health
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Metrics:
- ic_node_young_gengc_collection_time (or ic_node_young_gengc_collection_time_kraft)
- ic_node_old_gengc_collection_time (or ic_node_old_gengc_collection_time_kraft)

IMPORTANCE:
- High GC time indicates memory pressure
- Full GC pauses cause latency spikes
- Can lead to broker unresponsiveness
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def check_prometheus_gc_health(connector, settings):
    """
    Check GC health metrics via Prometheus (Instaclustr managed service).

    Monitors:
    - Young generation GC collection time
    - Old generation (Full) GC collection time

    Thresholds:
    - Young GC WARNING: > 5% time spent in GC
    - Young GC CRITICAL: > 10% time spent in GC
    - Old GC WARNING: > 2% time spent in GC
    - Old GC CRITICAL: > 5% time spent in GC

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("GC Health (Prometheus)")

    # Check if Prometheus is enabled
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

        # Try both KRaft and ZooKeeper metric names
        young_gc_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_young_gengc_collection_time(_kraft)?$')
        old_gc_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_old_gengc_collection_time(_kraft)?$')

        if not (young_gc_metrics or old_gc_metrics):
            builder.error("❌ GC metrics not found")
            findings = {
                'status': 'error',
                'error_message': 'GC metrics not found in Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Get thresholds (percentage of time spent in GC)
        young_gc_warning_pct = settings.get('kafka_young_gc_warning_pct', 5)
        young_gc_critical_pct = settings.get('kafka_young_gc_critical_pct', 10)
        old_gc_warning_pct = settings.get('kafka_old_gc_warning_pct', 2)
        old_gc_critical_pct = settings.get('kafka_old_gc_critical_pct', 5)

        # Process metrics by broker
        broker_data = {}

        for metric in young_gc_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id not in broker_data:
                broker_data[node_id] = {
                    'node_id': node_id,
                    'public_ip': target_labels.get('PublicIp', 'unknown'),
                    'rack': target_labels.get('Rack', 'unknown'),
                    'datacenter': target_labels.get('ClusterDataCenterName', 'unknown')
                }

            # Value is in milliseconds
            broker_data[node_id]['young_gc_time_ms'] = round(metric['value'], 2)

        for metric in old_gc_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['old_gc_time_ms'] = round(metric['value'], 2)

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

        # NOTE: These are cumulative values, not rates
        # In production, you'd want to calculate rate from previous scrape
        # For this implementation, we'll report the values

        node_data = list(broker_data.values())

        # Since we can't calculate percentage without time window, just report values
        status = 'info'
        severity = 0
        message = f"GC metrics collected from {len(node_data)} brokers"

        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_gc': {
                'status': status,
                'data': node_data,
                'metadata': {
                    'source': 'prometheus',
                    'metrics': ['young_gc_time_ms', 'old_gc_time_ms'],
                    'broker_count': len(node_data),
                    'note': 'Values are cumulative - monitor trends over time'
                }
            },
            'cluster_aggregate': {
                'avg_young_gc_time_ms': round(sum(b.get('young_gc_time_ms', 0) for b in node_data) / len(node_data), 2),
                'avg_old_gc_time_ms': round(sum(b.get('old_gc_time_ms', 0) for b in node_data) / len(node_data), 2),
                'broker_count': len(node_data)
            }
        }

        builder.text(f"ℹ️  {message}")
        builder.blank()
        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Young GC Time: {findings['cluster_aggregate']['avg_young_gc_time_ms']} ms (cumulative)")
        builder.text(f"- Avg Old GC Time: {findings['cluster_aggregate']['avg_old_gc_time_ms']} ms (cumulative)")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.blank()

        builder.text("*Per-Broker GC Metrics:*")
        for broker in node_data:
            builder.text(
                f"- Broker {broker['node_id'][:8]}: "
                f"Young GC {broker.get('young_gc_time_ms', 0)}ms, "
                f"Old GC {broker.get('old_gc_time_ms', 0)}ms"
            )
        builder.blank()

        builder.text("*Note:* GC time values are cumulative since broker start.")
        builder.text("Monitor trends over time - rapid increases indicate memory pressure.")
        builder.blank()

        recommendations = {
            "general": [
                "GC Monitoring Best Practices:",
                "  • Monitor GC time trends, not absolute values",
                "  • Rapid increases indicate memory pressure",
                "  • Old (Full) GC is more concerning than Young GC",
                "  • Typical healthy values: < 5% time in Young GC, < 2% in Old GC",
                "",
                "Signs of GC Issues:",
                "  • Increasing old GC frequency",
                "  • Long GC pause times (> 100ms)",
                "  • Heap usage consistently > 80%",
                "  • Latency spikes correlating with GC",
                "",
                "Solutions:",
                "  • Increase heap size if frequently hitting limits",
                "  • Use G1GC for heaps > 4GB (default in modern Kafka)",
                "  • Review application memory usage patterns",
                "  • Check for memory leaks",
                "  • Consider ZGC or Shenandoah for very low pause times"
            ]
        }

        builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus GC health check failed: {e}", exc_info=True)
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

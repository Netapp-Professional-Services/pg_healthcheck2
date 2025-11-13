"""
Kafka Request Latency Check (Unified Adaptive)

Monitors produce/fetch request latencies using adaptive collection strategy.
High latency indicates broker overload or performance issues.

Health Check: prometheus_request_latency
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- produce_latency, consumer_fetch_latency, follower_fetch_latency

IMPORTANCE:
- High latency impacts client performance
- Indicates broker saturation or slow storage
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    return 8


def check_prometheus_request_latency(connector, settings):
    builder = CheckContentBuilder()
    builder.h3("Request Latency (Prometheus)")

    try:
        # Collect all three latency metrics
        produce_def = get_metric_definition('produce_latency')
        consumer_def = get_metric_definition('consumer_fetch_latency')
        follower_def = get_metric_definition('follower_fetch_latency')

        produce_result = collect_metric_adaptive(produce_def, connector, settings) if produce_def else None
        consumer_result = collect_metric_adaptive(consumer_def, connector, settings) if consumer_def else None
        follower_result = collect_metric_adaptive(follower_def, connector, settings) if follower_def else None

        if not any([produce_result, consumer_result, follower_result]):
            builder.warning("⚠️ Could not collect request latency metrics")
            return builder.build(), {'status': 'skipped', 'reason': 'Metrics unavailable', 'data': [], 'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}}

        method = (produce_result or consumer_result or follower_result).get('method')
        produce_metrics = produce_result.get('node_metrics', {}) if produce_result else {}
        consumer_metrics = consumer_result.get('node_metrics', {}) if consumer_result else {}
        follower_metrics = follower_result.get('node_metrics', {}) if follower_result else {}

        warning_threshold = settings.get('kafka_latency_warning_ms', 100)
        critical_threshold = settings.get('kafka_latency_critical_ms', 500)

        all_hosts = set(produce_metrics.keys()) | set(consumer_metrics.keys()) | set(follower_metrics.keys())
        node_data = []
        critical_brokers = []
        warning_brokers = []

        for host in all_hosts:
            produce_lat = produce_metrics.get(host, 0)
            consumer_lat = consumer_metrics.get(host, 0)
            follower_lat = follower_metrics.get(host, 0)
            max_lat = max(produce_lat, consumer_lat, follower_lat)

            broker_entry = {
                'node_id': host,
                'host': host,
                'produce_latency_ms': round(produce_lat, 2),
                'consumer_fetch_latency_ms': round(consumer_lat, 2),
                'follower_fetch_latency_ms': round(follower_lat, 2),
                'max_latency_ms': round(max_lat, 2)
            }
            node_data.append(broker_entry)

            if max_lat >= critical_threshold:
                critical_brokers.append(broker_entry)
            elif max_lat >= warning_threshold:
                warning_brokers.append(broker_entry)

        if critical_brokers:
            status, severity = 'critical', 9
            message = f"🔴 {len(critical_brokers)} broker(s) with critical latency (>{critical_threshold}ms)"
        elif warning_brokers:
            status, severity = 'warning', 7
            message = f"⚠️  {len(warning_brokers)} broker(s) with high latency (>{warning_threshold}ms)"
        else:
            status, severity = 'healthy', 0
            message = f"✅ Request latency healthy across {len(node_data)} brokers"

        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_latency': node_data,
                'cluster_aggregate': {
                    'avg_produce_latency_ms': round(sum(b['produce_latency_ms'] for b in node_data) / len(node_data), 2) if node_data else 0,
                    'avg_consumer_latency_ms': round(sum(b['consumer_fetch_latency_ms'] for b in node_data) / len(node_data), 2) if node_data else 0,
                    'avg_follower_latency_ms': round(sum(b['follower_fetch_latency_ms'] for b in node_data) / len(node_data), 2) if node_data else 0,
                    'broker_count': len(node_data)
                },
                'collection_method': method
            },
            'metadata': {'source': method, 'broker_count': len(node_data), 'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }

        if status == 'critical':
            builder.critical(message)
        elif status == 'warning':
            builder.warning(message)
        else:
            builder.success(message)

        builder.blank()
        builder.text(f"*Cluster Summary:* Avg Produce: {findings['data']['cluster_aggregate']['avg_produce_latency_ms']}ms, Consumer: {findings['data']['cluster_aggregate']['avg_consumer_latency_ms']}ms, Follower: {findings['data']['cluster_aggregate']['avg_follower_latency_ms']}ms")
        builder.text(f"- Collection Method: {method}")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Request latency check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}}

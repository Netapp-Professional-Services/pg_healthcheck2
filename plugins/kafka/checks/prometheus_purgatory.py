"""
Kafka Purgatory Size Check (Prometheus - Instaclustr)

Monitors produce and fetch purgatory sizes from Instaclustr Prometheus endpoints.
Large purgatory sizes indicate requests waiting for acknowledgment or data.

Health Check: prometheus_purgatory
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Metrics:
- ic_node_produce_purgatory_size
- ic_node_fetch_purgatory_size

IMPORTANCE:
- High produce purgatory: Producers waiting for acks (acks=all with slow replicas)
- High fetch purgatory: Consumers/followers waiting for data (fetch.min.bytes not met)
- Can indicate replication lag or consumer configuration issues
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 6


def check_prometheus_purgatory(connector, settings):
    """
    Check purgatory sizes via Prometheus (Instaclustr managed service).

    Monitors:
    - Produce purgatory size (requests waiting for acks)
    - Fetch purgatory size (fetch requests waiting for data)

    Thresholds:
    - Produce WARNING: > 100 requests
    - Produce CRITICAL: > 500 requests
    - Fetch WARNING: > 100 requests
    - Fetch CRITICAL: > 500 requests

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Purgatory Size (Prometheus)")

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
                'error_message': 'No metrics available',
                'data': [],
                'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        produce_purg_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_produce_purgatory_size$')
        fetch_purg_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_fetch_purgatory_size$')

        if not (produce_purg_metrics or fetch_purg_metrics):
            builder.text("ℹ️  Purgatory metrics not found (may not be available)")
            findings = {
                'status': 'info',
                'message': 'Purgatory metrics not available',
                'data': [],
                'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        produce_warning = settings.get('kafka_produce_purgatory_warning', 100)
        produce_critical = settings.get('kafka_produce_purgatory_critical', 500)
        fetch_warning = settings.get('kafka_fetch_purgatory_warning', 100)
        fetch_critical = settings.get('kafka_fetch_purgatory_critical', 500)

        broker_data = {}

        for metric in produce_purg_metrics:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id not in broker_data:
                broker_data[node_id] = {
                    'node_id': node_id,
                    'public_ip': target_labels.get('PublicIp', 'unknown'),
                    'rack': target_labels.get('Rack', 'unknown')
                }
            broker_data[node_id]['produce_purgatory'] = int(metric['value'])

        for metric in fetch_purg_metrics:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['fetch_purgatory'] = int(metric['value'])

        if not broker_data:
            builder.error("❌ No broker data available")
            findings = {'status': 'error', 'error_message': 'No broker data', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
            return builder.build(), findings

        node_data = list(broker_data.values())
        critical_produce = []
        warning_produce = []
        critical_fetch = []
        warning_fetch = []

        for broker in node_data:
            prod_purg = broker.get('produce_purgatory', 0)
            fetch_purg = broker.get('fetch_purgatory', 0)

            if prod_purg >= produce_critical:
                critical_produce.append({'node_id': broker['node_id'], 'public_ip': broker['public_ip'], 'produce_purgatory': prod_purg})
            elif prod_purg >= produce_warning:
                warning_produce.append({'node_id': broker['node_id'], 'public_ip': broker['public_ip'], 'produce_purgatory': prod_purg})

            if fetch_purg >= fetch_critical:
                critical_fetch.append({'node_id': broker['node_id'], 'public_ip': broker['public_ip'], 'fetch_purgatory': fetch_purg})
            elif fetch_purg >= fetch_warning:
                warning_fetch.append({'node_id': broker['node_id'], 'public_ip': broker['public_ip'], 'fetch_purgatory': fetch_purg})

        if critical_produce or critical_fetch:
            status = 'critical'
            severity = 9
            issues = []
            if critical_produce:
                issues.append(f"{len(critical_produce)} broker(s) with critical produce purgatory")
            if critical_fetch:
                issues.append(f"{len(critical_fetch)} broker(s) with critical fetch purgatory")
            message = " and ".join(issues)
        elif warning_produce or warning_fetch:
            status = 'warning'
            severity = 6
            issues = []
            if warning_produce:
                issues.append(f"{len(warning_produce)} broker(s) with high produce purgatory")
            if warning_fetch:
                issues.append(f"{len(warning_fetch)} broker(s) with high fetch purgatory")
            message = " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"Purgatory sizes healthy across {len(node_data)} brokers"

        avg_produce = sum(b.get('produce_purgatory', 0) for b in node_data) / len(node_data)
        avg_fetch = sum(b.get('fetch_purgatory', 0) for b in node_data) / len(node_data)

        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_purgatory': {
                'status': status,
                'data': node_data,
                'metadata': {'source': 'prometheus', 'metrics': ['produce_purgatory', 'fetch_purgatory'], 'broker_count': len(node_data)}
            },
            'cluster_aggregate': {
                'avg_produce_purgatory': round(avg_produce, 1),
                'avg_fetch_purgatory': round(avg_fetch, 1),
                'broker_count': len(node_data)
            }
        }

        if critical_produce:
            findings['critical_produce_purgatory'] = {'count': len(critical_produce), 'brokers': critical_produce, 'recommendation': 'High produce purgatory indicates slow replication or acks=all with lagging replicas'}
        if warning_produce:
            findings['warning_produce_purgatory'] = {'count': len(warning_produce), 'brokers': warning_produce, 'recommendation': 'Monitor produce purgatory - may indicate replication issues'}
        if critical_fetch:
            findings['critical_fetch_purgatory'] = {'count': len(critical_fetch), 'brokers': critical_fetch, 'recommendation': 'High fetch purgatory indicates consumers/followers waiting for data'}
        if warning_fetch:
            findings['warning_fetch_purgatory'] = {'count': len(warning_fetch), 'brokers': warning_fetch, 'recommendation': 'Monitor fetch purgatory - check consumer configuration'}

        if status == 'critical':
            builder.critical(f"⚠️  {message}")
        elif status == 'warning':
            builder.warning(f"⚠️  {message}")
        else:
            builder.success(f"✅ {message}")

        builder.blank()
        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Produce Purgatory: {round(avg_produce, 1)} requests")
        builder.text(f"- Avg Fetch Purgatory: {round(avg_fetch, 1)} requests")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.blank()

        if status in ['critical', 'warning']:
            recommendations = {
                "general": [
                    "Purgatory Explained:",
                    "  • Produce Purgatory: Requests waiting for acks (acks=all)",
                    "  • Fetch Purgatory: Fetch requests waiting for fetch.min.bytes",
                    "",
                    "High Produce Purgatory Causes:",
                    "  • Slow replica replication (check ISR health)",
                    "  • acks=all with lagging replicas",
                    "  • Network issues between brokers",
                    "",
                    "High Fetch Purgatory Causes:",
                    "  • fetch.min.bytes set too high for traffic rate",
                    "  • Low traffic periods with default fetch.min.bytes=1",
                    "  • fetch.max.wait.ms forcing waits",
                    "",
                    "Solutions:",
                    "  • Check replication health and ISR stability",
                    "  • Review producer acks configuration",
                    "  • Tune fetch.min.bytes and fetch.max.wait.ms for consumers"
                ]
            }
            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus purgatory check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

"""
Kafka Request Latency Check (Prometheus - Instaclustr)

Monitors request latency metrics from Instaclustr Prometheus endpoints.
Tracks produce, fetch consumer, and fetch follower request latencies.

Health Check: prometheus_request_latency
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Latency Metrics:
- Produce Request: Client write latency
- Fetch Consumer: Consumer read latency
- Fetch Follower: Replication latency

Metrics:
- ic_node_produce_request_time_milliseconds (mean, 99th percentile, max)
- ic_node_fetch_consumer_request_time_milliseconds
- ic_node_fetch_follower_request_time_milliseconds
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def check_prometheus_request_latency(connector, settings):
    """
    Check request latency metrics via Prometheus (Instaclustr managed service).

    Monitors:
    - Produce request latency (client writes)
    - Fetch consumer latency (client reads)
    - Fetch follower latency (replication)

    Thresholds:
    - Produce: WARNING > 100ms, CRITICAL > 500ms
    - Fetch Consumer: WARNING > 100ms, CRITICAL > 500ms
    - Fetch Follower: WARNING > 200ms, CRITICAL > 1000ms

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Request Latency (Prometheus)")

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
        # Import here to avoid dependency
        from plugins.common.prometheus_client import get_instaclustr_client

        # Get cached Prometheus client
        client = get_instaclustr_client(
            cluster_id=settings['instaclustr_cluster_id'],
            username=settings['instaclustr_prometheus_username'],
            api_key=settings['instaclustr_prometheus_api_key'],
            prometheus_base_url=settings.get('instaclustr_prometheus_base_url')
        )

        # Scrape all metrics
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

        # Extract latency metrics (looking for mean values)
        produce_latency_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^ic_node_produce_request_time_milliseconds$'
        )
        fetch_consumer_latency_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^ic_node_fetch_consumer_request_time_milliseconds$'
        )
        fetch_follower_latency_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^ic_node_fetch_follower_request_time_milliseconds$'
        )

        if not (produce_latency_metrics or fetch_consumer_latency_metrics):
            builder.error("❌ Request latency metrics not found")
            findings = {
                'status': 'error',
                'error_message': 'Request latency metrics not found in Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Get thresholds
        produce_warning = settings.get('kafka_produce_latency_warning_ms', 100)
        produce_critical = settings.get('kafka_produce_latency_critical_ms', 500)
        fetch_consumer_warning = settings.get('kafka_fetch_consumer_latency_warning_ms', 100)
        fetch_consumer_critical = settings.get('kafka_fetch_consumer_latency_critical_ms', 500)
        fetch_follower_warning = settings.get('kafka_fetch_follower_latency_warning_ms', 200)
        fetch_follower_critical = settings.get('kafka_fetch_follower_latency_critical_ms', 1000)

        # Process metrics by broker
        broker_data = {}

        # Process produce latency
        for metric in produce_latency_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))
            quantile = labels.get('quantile', 'mean')

            if node_id not in broker_data:
                broker_data[node_id] = {
                    'node_id': node_id,
                    'public_ip': target_labels.get('PublicIp', 'unknown'),
                    'rack': target_labels.get('Rack', 'unknown'),
                    'datacenter': target_labels.get('ClusterDataCenterName', 'unknown')
                }

            # Store by quantile/type
            if quantile == 'mean' or labels.get('type') == 'mean':
                broker_data[node_id]['produce_latency_ms'] = round(metric['value'], 2)

        # Process fetch consumer latency
        for metric in fetch_consumer_latency_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))
            quantile = labels.get('quantile', 'mean')

            if node_id in broker_data:
                if quantile == 'mean' or labels.get('type') == 'mean':
                    broker_data[node_id]['fetch_consumer_latency_ms'] = round(metric['value'], 2)

        # Process fetch follower latency
        for metric in fetch_follower_latency_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))
            quantile = labels.get('quantile', 'mean')

            if node_id in broker_data:
                if quantile == 'mean' or labels.get('type') == 'mean':
                    broker_data[node_id]['fetch_follower_latency_ms'] = round(metric['value'], 2)

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

        # Convert to list and identify issues
        node_data = list(broker_data.values())
        critical_produce = []
        warning_produce = []
        critical_fetch_consumer = []
        warning_fetch_consumer = []
        critical_fetch_follower = []
        warning_fetch_follower = []

        total_produce_latency = 0
        total_fetch_consumer_latency = 0
        total_fetch_follower_latency = 0
        produce_count = 0
        fetch_consumer_count = 0
        fetch_follower_count = 0

        for broker in node_data:
            produce_lat = broker.get('produce_latency_ms', 0)
            fetch_consumer_lat = broker.get('fetch_consumer_latency_ms', 0)
            fetch_follower_lat = broker.get('fetch_follower_latency_ms', 0)

            # Check produce latency
            if produce_lat > 0:
                total_produce_latency += produce_lat
                produce_count += 1

                if produce_lat >= produce_critical:
                    critical_produce.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
                        'produce_latency_ms': produce_lat
                    })
                elif produce_lat >= produce_warning:
                    warning_produce.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
                        'produce_latency_ms': produce_lat
                    })

            # Check fetch consumer latency
            if fetch_consumer_lat > 0:
                total_fetch_consumer_latency += fetch_consumer_lat
                fetch_consumer_count += 1

                if fetch_consumer_lat >= fetch_consumer_critical:
                    critical_fetch_consumer.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
                        'fetch_consumer_latency_ms': fetch_consumer_lat
                    })
                elif fetch_consumer_lat >= fetch_consumer_warning:
                    warning_fetch_consumer.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
                        'fetch_consumer_latency_ms': fetch_consumer_lat
                    })

            # Check fetch follower latency
            if fetch_follower_lat > 0:
                total_fetch_follower_latency += fetch_follower_lat
                fetch_follower_count += 1

                if fetch_follower_lat >= fetch_follower_critical:
                    critical_fetch_follower.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
                        'fetch_follower_latency_ms': fetch_follower_lat
                    })
                elif fetch_follower_lat >= fetch_follower_warning:
                    warning_fetch_follower.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
                        'fetch_follower_latency_ms': fetch_follower_lat
                    })

        # Determine overall status
        if critical_produce or critical_fetch_consumer or critical_fetch_follower:
            status = 'critical'
            severity = 10
            issues = []
            if critical_produce:
                issues.append(f"{len(critical_produce)} broker(s) with critical produce latency")
            if critical_fetch_consumer:
                issues.append(f"{len(critical_fetch_consumer)} broker(s) with critical consumer latency")
            if critical_fetch_follower:
                issues.append(f"{len(critical_fetch_follower)} broker(s) with critical replication latency")
            message = " and ".join(issues)
        elif warning_produce or warning_fetch_consumer or warning_fetch_follower:
            status = 'warning'
            severity = 7
            issues = []
            if warning_produce:
                issues.append(f"{len(warning_produce)} broker(s) with high produce latency")
            if warning_fetch_consumer:
                issues.append(f"{len(warning_fetch_consumer)} broker(s) with high consumer latency")
            if warning_fetch_follower:
                issues.append(f"{len(warning_fetch_follower)} broker(s) with high replication latency")
            message = " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"Request latencies are healthy across {len(node_data)} brokers"

        # Build structured findings
        avg_produce = total_produce_latency / produce_count if produce_count > 0 else 0
        avg_fetch_consumer = total_fetch_consumer_latency / fetch_consumer_count if fetch_consumer_count > 0 else 0
        avg_fetch_follower = total_fetch_follower_latency / fetch_follower_count if fetch_follower_count > 0 else 0

        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_latency': {
                'status': status,
                'data': node_data,
                'metadata': {
                    'source': 'prometheus',
                    'metrics': ['produce_latency_ms', 'fetch_consumer_latency_ms', 'fetch_follower_latency_ms'],
                    'broker_count': len(node_data)
                }
            },
            'cluster_aggregate': {
                'avg_produce_latency_ms': round(avg_produce, 2),
                'avg_fetch_consumer_latency_ms': round(avg_fetch_consumer, 2),
                'avg_fetch_follower_latency_ms': round(avg_fetch_follower, 2),
                'broker_count': len(node_data),
                'thresholds': {
                    'produce_warning_ms': produce_warning,
                    'produce_critical_ms': produce_critical,
                    'fetch_consumer_warning_ms': fetch_consumer_warning,
                    'fetch_consumer_critical_ms': fetch_consumer_critical,
                    'fetch_follower_warning_ms': fetch_follower_warning,
                    'fetch_follower_critical_ms': fetch_follower_critical
                }
            }
        }

        # Add issue details
        if critical_produce:
            findings['critical_produce_latency'] = {
                'count': len(critical_produce),
                'brokers': critical_produce,
                'recommendation': 'Investigate producer performance - writes are critically slow'
            }

        if warning_produce:
            findings['warning_produce_latency'] = {
                'count': len(warning_produce),
                'brokers': warning_produce,
                'recommendation': 'Monitor producer latency - performance degradation detected'
            }

        if critical_fetch_consumer:
            findings['critical_fetch_consumer_latency'] = {
                'count': len(critical_fetch_consumer),
                'brokers': critical_fetch_consumer,
                'recommendation': 'Investigate consumer fetch performance - reads are critically slow'
            }

        if warning_fetch_consumer:
            findings['warning_fetch_consumer_latency'] = {
                'count': len(warning_fetch_consumer),
                'brokers': warning_fetch_consumer,
                'recommendation': 'Monitor consumer fetch latency - performance degradation detected'
            }

        if critical_fetch_follower:
            findings['critical_fetch_follower_latency'] = {
                'count': len(critical_fetch_follower),
                'brokers': critical_fetch_follower,
                'recommendation': 'Investigate replication performance - followers are critically lagging'
            }

        if warning_fetch_follower:
            findings['warning_fetch_follower_latency'] = {
                'count': len(warning_fetch_follower),
                'brokers': warning_fetch_follower,
                'recommendation': 'Monitor replication latency - performance degradation detected'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(f"⚠️  {message}")
        elif status == 'warning':
            builder.warning(f"⚠️  {message}")
        else:
            builder.success(f"✅ {message}")

        builder.blank()
        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Produce Latency: {round(avg_produce, 2)}ms")
        builder.text(f"- Avg Consumer Fetch Latency: {round(avg_fetch_consumer, 2)}ms")
        builder.text(f"- Avg Replication Latency: {round(avg_fetch_follower, 2)}ms")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.blank()

        # Show critical issues
        for issue_list, issue_type, findings_key in [
            (critical_produce, "Produce", "critical_produce_latency"),
            (critical_fetch_consumer, "Consumer Fetch", "critical_fetch_consumer_latency"),
            (critical_fetch_follower, "Replication", "critical_fetch_follower_latency")
        ]:
            if issue_list:
                builder.text(f"*🔴 Critical {issue_type} Latency ({len(issue_list)}):*")
                for broker in issue_list:
                    latency_key = [k for k in broker.keys() if '_latency_ms' in k][0]
                    builder.text(
                        f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                        f"{broker[latency_key]}ms"
                    )
                builder.text(f"_Recommendation: {findings[findings_key]['recommendation']}_")
                builder.blank()

        # Show warnings
        for issue_list, issue_type, findings_key in [
            (warning_produce, "Produce", "warning_produce_latency"),
            (warning_fetch_consumer, "Consumer Fetch", "warning_fetch_consumer_latency"),
            (warning_fetch_follower, "Replication", "warning_fetch_follower_latency")
        ]:
            if issue_list:
                builder.text(f"*⚠️  High {issue_type} Latency ({len(issue_list)}):*")
                for broker in issue_list:
                    latency_key = [k for k in broker.keys() if '_latency_ms' in k][0]
                    builder.text(
                        f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                        f"{broker[latency_key]}ms"
                    )
                builder.text(f"_Recommendation: {findings[findings_key]['recommendation']}_")
                builder.blank()

        # Add recommendations if issues found
        if status in ['critical', 'warning']:
            recommendations = {}

            if status == 'critical':
                recommendations["critical"] = [
                    "Check broker resource utilization (CPU, disk I/O)",
                    "Review disk performance - slow disks cause high latency",
                    "Check for network issues or bandwidth saturation",
                    "Review log segment settings (segment.ms, segment.bytes)",
                    "Consider scaling up broker resources or adding brokers"
                ]

            if status == 'warning':
                recommendations["high"] = [
                    "Monitor latency trends over time",
                    "Check if latency correlates with traffic patterns",
                    "Review broker performance metrics",
                    "Ensure adequate resources for current workload"
                ]

            recommendations["general"] = [
                "Latency Best Practices:",
                "  • Monitor p99 latencies, not just averages",
                "  • Keep produce latency < 100ms for good user experience",
                "  • High replication latency can cause ISR shrinks",
                "  • Use SSDs for Kafka log directories",
                "  • Tune OS page cache and disk I/O scheduler",
                "",
                "Common Causes of High Latency:",
                "  • Disk I/O bottlenecks (slow disks, high utilization)",
                "  • CPU saturation",
                "  • Network congestion",
                "  • Large message batches",
                "  • Inefficient compression settings",
                "  • GC pauses"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus request latency check failed: {e}", exc_info=True)
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

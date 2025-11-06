"""
Kafka Broker Health Check (Prometheus - Instaclustr)

Monitors broker health metrics from Instaclustr Prometheus endpoints.
Uses ic_node_* metrics specific to Instaclustr's managed Kafka service.

Health Check: prometheus_broker_health
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Note: This check uses Instaclustr-specific metrics (ic_node_*).
For bare-metal Kafka with standard JMX metrics, use prometheus_jvm_heap.py instead.
"""

import logging
from typing import Dict
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def check_prometheus_broker_health(connector, settings):
    """
    Check broker health metrics via Prometheus (Instaclustr managed service).

    Monitors:
    - CPU utilization (ic_node_cpu_utilization)
    - Disk utilization (ic_node_disk_utilization)
    - Disk available (ic_node_disk_available)
    - Broker throughput (ic_node_broker_topic_bytes_in/out)

    Thresholds:
    - CPU: WARNING > 75%, CRITICAL > 90%
    - Disk: WARNING > 80%, CRITICAL > 90%

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        Structured findings with per-broker metrics and cluster aggregate
    """
    # Initialize builder
    builder = CheckContentBuilder()
    builder.h3("Broker Health Metrics (Prometheus)")

    # Check if Prometheus is enabled
    if not settings.get('instaclustr_prometheus_enabled'):
        findings = {
            'prometheus_broker_health': {
                'status': 'skipped',
                'reason': 'Prometheus monitoring not enabled',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
        }
        return builder.build(), findings

    try:
        # Import here to avoid dependency if not using Prometheus
        from plugins.common.prometheus_client import get_instaclustr_client

        # Get cached Prometheus client (avoids rate limiting)
        client = get_instaclustr_client(
            cluster_id=settings['instaclustr_cluster_id'],
            username=settings['instaclustr_prometheus_username'],
            api_key=settings['instaclustr_prometheus_api_key'],
            prometheus_base_url=settings.get('instaclustr_prometheus_base_url')
        )

        # Scrape all metrics from service discovery
        all_metrics = client.scrape_all_nodes()

        if not all_metrics:
            builder.error("❌ No metrics available from Prometheus")
            findings = {
                'prometheus_broker_health': {
                    'status': 'error',
                    'error_message': 'No metrics available from Prometheus',
                    'data': [],
                    'metadata': {
                        'source': 'prometheus',
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                }
            }
            return builder.build(), findings

        # Extract broker health metrics (Instaclustr-specific ic_node_* metrics)
        cpu_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_cpu_utilization$')
        disk_util_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_disk_utilization$')
        disk_avail_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_disk_available$')
        bytes_in_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_broker_topic_bytes_in$')
        bytes_out_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_broker_topic_bytes_out$')

        if not cpu_metrics and not disk_util_metrics:
            builder.error("❌ Broker health metrics not found")
            findings = {
                'prometheus_broker_health': {
                    'status': 'error',
                    'error_message': 'Broker health metrics not found in Prometheus',
                    'data': [],
                    'metadata': {
                        'source': 'prometheus',
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                }
            }
            return builder.build(), findings

        # Group by broker/node
        broker_data = {}

        # Process CPU metrics
        for metric in cpu_metrics:
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

            broker_data[node_id]['cpu_utilization'] = metric['value']

        # Process disk utilization metrics
        for metric in disk_util_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['disk_utilization'] = metric['value']

        # Process disk available metrics
        for metric in disk_avail_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['disk_available_gb'] = round(metric['value'] / (1024**3), 2)

        # Process throughput metrics
        for metric in bytes_in_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['bytes_in_per_sec'] = round(metric['value'], 2)

        for metric in bytes_out_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['bytes_out_per_sec'] = round(metric['value'], 2)

        # Build node data and identify issues
        node_data = []
        high_cpu_brokers = []
        critical_cpu_brokers = []
        high_disk_brokers = []
        critical_disk_brokers = []

        for node_id, data in broker_data.items():
            node_entry = {
                'node_id': node_id,
                'public_ip': data.get('public_ip', 'unknown'),
                'rack': data.get('rack', 'unknown'),
                'datacenter': data.get('datacenter', 'unknown'),
                'cpu_utilization': data.get('cpu_utilization', 0),
                'disk_utilization': data.get('disk_utilization', 0),
                'disk_available_gb': data.get('disk_available_gb', 0),
                'bytes_in_per_sec': data.get('bytes_in_per_sec', 0),
                'bytes_out_per_sec': data.get('bytes_out_per_sec', 0)
            }
            node_data.append(node_entry)

            # Check thresholds
            cpu = data.get('cpu_utilization', 0)
            disk = data.get('disk_utilization', 0)

            if cpu >= 90:
                critical_cpu_brokers.append({
                    'node_id': node_id,
                    'public_ip': data.get('public_ip', 'unknown'),
                    'cpu_percent': round(cpu, 2)
                })
            elif cpu >= 75:
                high_cpu_brokers.append({
                    'node_id': node_id,
                    'public_ip': data.get('public_ip', 'unknown'),
                    'cpu_percent': round(cpu, 2)
                })

            if disk >= 90:
                critical_disk_brokers.append({
                    'node_id': node_id,
                    'public_ip': data.get('public_ip', 'unknown'),
                    'disk_percent': round(disk, 2),
                    'disk_available_gb': data.get('disk_available_gb', 0)
                })
            elif disk >= 80:
                high_disk_brokers.append({
                    'node_id': node_id,
                    'public_ip': data.get('public_ip', 'unknown'),
                    'disk_percent': round(disk, 2),
                    'disk_available_gb': data.get('disk_available_gb', 0)
                })

        if not node_data:
            builder.error("❌ No broker data available")
            findings = {
                'prometheus_broker_health': {
                    'status': 'error',
                    'error_message': 'No broker data available',
                    'data': [],
                    'metadata': {
                        'source': 'prometheus',
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                }
            }
            return builder.build(), findings

        # Calculate cluster aggregates
        avg_cpu = sum(n['cpu_utilization'] for n in node_data) / len(node_data)
        avg_disk = sum(n['disk_utilization'] for n in node_data) / len(node_data)
        total_bytes_in = sum(n['bytes_in_per_sec'] for n in node_data)
        total_bytes_out = sum(n['bytes_out_per_sec'] for n in node_data)

        # Determine overall status
        if critical_cpu_brokers or critical_disk_brokers:
            status = 'critical'
            severity = 10
            issues = []
            if critical_cpu_brokers:
                issues.append(f"{len(critical_cpu_brokers)} broker(s) with critical CPU (>90%)")
            if critical_disk_brokers:
                issues.append(f"{len(critical_disk_brokers)} broker(s) with critical disk (>90%)")
            message = " and ".join(issues)
        elif high_cpu_brokers or high_disk_brokers:
            status = 'warning'
            severity = 7
            issues = []
            if high_cpu_brokers:
                issues.append(f"{len(high_cpu_brokers)} broker(s) with high CPU (>75%)")
            if high_disk_brokers:
                issues.append(f"{len(high_disk_brokers)} broker(s) with high disk (>80%)")
            message = " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"All {len(node_data)} brokers have healthy CPU and disk usage"

        # Build findings
        findings = {
            'prometheus_broker_health': {
                'status': status,
                'severity': severity,
                'message': message,
                'per_broker_metrics': {
                    'status': status,
                    'data': node_data,
                    'metadata': {
                        'source': 'prometheus',
                        'metric_type': 'instaclustr_managed',
                        'broker_count': len(node_data)
                    }
                },
                'cluster_aggregate': {
                    'avg_cpu_utilization': round(avg_cpu, 2),
                    'avg_disk_utilization': round(avg_disk, 2),
                    'total_bytes_in_per_sec': round(total_bytes_in, 2),
                    'total_bytes_out_per_sec': round(total_bytes_out, 2),
                    'broker_count': len(node_data)
                }
            }
        }

        # Add warnings if any
        if critical_cpu_brokers:
            findings['prometheus_broker_health']['critical_cpu_brokers'] = {
                'count': len(critical_cpu_brokers),
                'brokers': critical_cpu_brokers,
                'recommendation': 'Investigate CPU-intensive processes, consider scaling up instance types'
            }

        if high_cpu_brokers:
            findings['prometheus_broker_health']['high_cpu_brokers'] = {
                'count': len(high_cpu_brokers),
                'brokers': high_cpu_brokers,
                'recommendation': 'Monitor CPU usage, review workload distribution'
            }

        if critical_disk_brokers:
            findings['prometheus_broker_health']['critical_disk_brokers'] = {
                'count': len(critical_disk_brokers),
                'brokers': critical_disk_brokers,
                'recommendation': 'Urgently increase disk capacity or implement log retention policies'
            }

        if high_disk_brokers:
            findings['prometheus_broker_health']['high_disk_brokers'] = {
                'count': len(high_disk_brokers),
                'brokers': high_disk_brokers,
                'recommendation': 'Plan disk capacity expansion, review log retention settings'
            }

        # Generate AsciiDoc content
        if status == 'critical':
            builder.critical(f"⚠️  {message}")
        elif status == 'warning':
            builder.warning(f"⚠️  {message}")
        else:
            builder.success(f"✅ {message}")

        builder.blank()
        builder.para("*Cluster Summary:*")
        builder.para(f"- Average CPU: {round(avg_cpu, 2)}%")
        builder.para(f"- Average Disk: {round(avg_disk, 2)}%")
        builder.para(f"- Total Throughput In: {round(total_bytes_in / 1024 / 1024, 2)} MB/s")
        builder.para(f"- Total Throughput Out: {round(total_bytes_out / 1024 / 1024, 2)} MB/s")
        builder.para(f"- Brokers Monitored: {len(node_data)}")

        if critical_cpu_brokers:
            builder.blank()
            builder.para(f"*⚠️  Critical CPU Brokers ({len(critical_cpu_brokers)}):*")
            for broker in critical_cpu_brokers:
                builder.para(f"- Broker {broker['node_id']} ({broker.get('public_ip', 'unknown')}): {broker['cpu_percent']}% CPU")
            builder.para(f"_Recommendation: {findings['prometheus_broker_health']['critical_cpu_brokers']['recommendation']}_")

        if high_cpu_brokers:
            builder.blank()
            builder.para(f"*⚠️  High CPU Brokers ({len(high_cpu_brokers)}):*")
            for broker in high_cpu_brokers:
                builder.para(f"- Broker {broker['node_id']} ({broker.get('public_ip', 'unknown')}): {broker['cpu_percent']}% CPU")
            builder.para(f"_Recommendation: {findings['prometheus_broker_health']['high_cpu_brokers']['recommendation']}_")

        if critical_disk_brokers:
            builder.blank()
            builder.para(f"*⚠️  Critical Disk Brokers ({len(critical_disk_brokers)}):*")
            for broker in critical_disk_brokers:
                builder.para(f"- Broker {broker['node_id']} ({broker.get('public_ip', 'unknown')}): {broker['disk_percent']}% disk ({broker['disk_available_gb']} GB available)")
            builder.para(f"_Recommendation: {findings['prometheus_broker_health']['critical_disk_brokers']['recommendation']}_")

        if high_disk_brokers:
            builder.blank()
            builder.para(f"*⚠️  High Disk Brokers ({len(high_disk_brokers)}):*")
            for broker in high_disk_brokers:
                builder.para(f"- Broker {broker['node_id']} ({broker.get('public_ip', 'unknown')}): {broker['disk_percent']}% disk ({broker['disk_available_gb']} GB available)")
            builder.para(f"_Recommendation: {findings['prometheus_broker_health']['high_disk_brokers']['recommendation']}_")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus broker health check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'prometheus_broker_health': {
                'status': 'error',
                'error_message': str(e),
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
        }
        return builder.build(), findings

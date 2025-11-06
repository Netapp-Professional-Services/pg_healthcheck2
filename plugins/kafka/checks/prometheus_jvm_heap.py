"""
Kafka JVM Heap Check (Prometheus - Instaclustr)

Monitors JVM heap memory usage from Instaclustr Prometheus endpoints.
Uses ic_node_heap* metrics specific to Instaclustr's managed Kafka service.

Health Check: prometheus_jvm_heap
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Note: This check uses Instaclustr-specific metrics (ic_node_heap*).
For self-hosted Kafka with SSH access, use check_jvm_stats.py instead.
"""

import logging
from typing import Dict
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def check_prometheus_jvm_heap(connector, settings):
    """
    Check JVM heap memory usage via Prometheus (Instaclustr managed service).

    Monitors:
    - Heap memory used (ic_node_heapmemoryused_bytes)
    - Heap memory max (ic_node_heapmemorymax_bytes)
    - Heap usage percentage

    Thresholds:
    - WARNING: > 75% heap usage
    - CRITICAL: > 90% heap usage

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        Structured findings with per-broker heap metrics
    """
    # Initialize builder
    builder = CheckContentBuilder()
    builder.h3("JVM Heap Usage (Prometheus)")

    # Check if Prometheus is enabled
    if not settings.get('instaclustr_prometheus_enabled'):
        findings = {
            'prometheus_jvm_heap': {
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
                'prometheus_jvm_heap': {
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

        # Extract JVM heap metrics (Instaclustr-specific ic_node_heap* metrics)
        heap_used_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_heapmemoryused_bytes$')
        heap_max_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_heapmemorymax_bytes$')

        if not heap_used_metrics or not heap_max_metrics:
            builder.error("❌ JVM heap metrics not found")
            findings = {
                'prometheus_jvm_heap': {
                    'status': 'error',
                    'error_message': 'JVM heap metrics not found in Prometheus',
                    'data': [],
                    'metadata': {
                        'source': 'prometheus',
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                }
            }
            return builder.build(), findings

        # Get thresholds
        heap_warning = settings.get('kafka_jvm_heap_warning_percent', 75)
        heap_critical = settings.get('kafka_jvm_heap_critical_percent', 90)

        # Group by broker/node
        broker_data = {}

        # Process heap used metrics
        for metric in heap_used_metrics:
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

            broker_data[node_id]['heap_used_bytes'] = metric['value']

        # Process heap max metrics
        for metric in heap_max_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['heap_max_bytes'] = metric['value']

        # Calculate heap percentage and check thresholds
        critical_brokers = []
        warning_brokers = []
        healthy_brokers = []
        broker_list = []

        for node_id, data in broker_data.items():
            heap_used = data.get('heap_used_bytes', 0)
            heap_max = data.get('heap_max_bytes', 1)  # Avoid division by zero

            if heap_max > 0:
                heap_percent = (heap_used / heap_max) * 100
            else:
                heap_percent = 0

            # Convert to GB for display
            heap_used_gb = heap_used / (1024**3)
            heap_max_gb = heap_max / (1024**3)

            broker_info = {
                'node_id': node_id,
                'public_ip': data.get('public_ip', 'unknown'),
                'rack': data.get('rack', 'unknown'),
                'heap_used_gb': round(heap_used_gb, 2),
                'heap_max_gb': round(heap_max_gb, 2),
                'heap_percent': round(heap_percent, 1)
            }

            broker_list.append(broker_info)

            # Categorize by severity
            if heap_percent >= heap_critical:
                critical_brokers.append(broker_info)
            elif heap_percent >= heap_warning:
                warning_brokers.append(broker_info)
            else:
                healthy_brokers.append(broker_info)

        # === REPORT FINDINGS ===
        if critical_brokers:
            builder.critical_issue(
                f"🔴 Critical heap usage on {len(critical_brokers)} broker(s)",
                {
                    "Threshold": f">{heap_critical}%",
                    "Action Required": "Immediate investigation needed"
                }
            )
            builder.blank()

            for broker in critical_brokers:
                builder.para(
                    f"**Broker {broker['node_id']}** ({broker['public_ip']}): "
                    f"{broker['heap_percent']}% heap usage "
                    f"({broker['heap_used_gb']:.1f}GB / {broker['heap_max_gb']:.1f}GB)"
                )

            builder.blank()

        if warning_brokers:
            builder.warning(
                f"⚠️ High heap usage on {len(warning_brokers)} broker(s)"
            )
            builder.blank()

            for broker in warning_brokers:
                builder.para(
                    f"**Broker {broker['node_id']}** ({broker['public_ip']}): "
                    f"{broker['heap_percent']}% heap usage "
                    f"({broker['heap_used_gb']:.1f}GB / {broker['heap_max_gb']:.1f}GB)"
                )

            builder.blank()

        if healthy_brokers and not critical_brokers and not warning_brokers:
            builder.success(
                f"✅ All {len(healthy_brokers)} brokers have healthy heap usage"
            )
            builder.blank()

        # === SUMMARY TABLE ===
        if broker_list:
            # Calculate cluster-wide stats
            total_heap_used = sum(b['heap_used_gb'] for b in broker_list)
            total_heap_max = sum(b['heap_max_gb'] for b in broker_list)
            avg_heap_percent = sum(b['heap_percent'] for b in broker_list) / len(broker_list)

            builder.para("*Cluster Summary:*")
            builder.blank()
            builder.para(f"- Average Heap Usage: {avg_heap_percent:.1f}%")
            builder.para(f"- Total Heap Used: {total_heap_used:.1f} GB")
            builder.para(f"- Total Heap Max: {total_heap_max:.1f} GB")
            builder.para(f"- Brokers Monitored: {len(broker_list)}")
            builder.blank()

        # === RECOMMENDATIONS ===
        if critical_brokers or warning_brokers:
            recommendations = {}

            if critical_brokers:
                recommendations["critical"] = [
                    "Investigate high heap usage immediately - may indicate memory leak or undersized heap",
                    "Review application memory usage patterns and GC behavior",
                    "Consider increasing heap size if sustained high usage",
                    "Check for memory-intensive operations (large messages, excessive buffering)"
                ]

            if warning_brokers:
                recommendations["high"] = [
                    "Monitor heap trends to detect gradual memory growth",
                    "Review GC logs for excessive full garbage collections",
                    "Consider heap size optimization based on workload",
                    "Plan capacity upgrades if heap usage continues to grow"
                ]

            recommendations["general"] = [
                "Set appropriate -Xmx and -Xms JVM flags (typically 6-8GB for production)",
                "Monitor GC pause times in addition to heap usage",
                "Use G1GC or ZGC for better heap management on large heaps",
                "Keep heap usage below 75% for optimal performance",
                "Set up alerts for heap usage thresholds (warning: 75%, critical: 90%)"
            ]

            builder.recs(recommendations)

        # === STRUCTURED DATA ===
        findings = {
            'prometheus_jvm_heap': {
                'status': 'success',
                'brokers_checked': len(broker_list),
                'critical_count': len(critical_brokers),
                'warning_count': len(warning_brokers),
                'healthy_count': len(healthy_brokers),
                'average_heap_percent': round(avg_heap_percent, 1) if broker_list else 0,
                'total_heap_used_gb': round(total_heap_used, 2) if broker_list else 0,
                'total_heap_max_gb': round(total_heap_max, 2) if broker_list else 0,
                'thresholds': {
                    'heap_warning_percent': heap_warning,
                    'heap_critical_percent': heap_critical
                },
                'data': broker_list,
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
        }

    except Exception as e:
        import traceback
        logger.error(f"Prometheus JVM heap check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        findings = {
            'prometheus_jvm_heap': {
                'status': 'error',
                'details': str(e),
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
        }

    return builder.build(), findings

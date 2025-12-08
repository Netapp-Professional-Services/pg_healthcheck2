"""
Cassandra JVM Heap Usage Check (Prometheus)

Monitors JVM heap memory usage across all cluster nodes using Prometheus metrics.
Alerts on high heap usage that could lead to GC pressure or OOM errors.

Health Check: prometheus_jvm_heap
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true
"""

import logging
from typing import Dict, Tuple
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, require_prometheus

logger = logging.getLogger(__name__)


def check_prometheus_jvm_heap(connector, settings):
    """
    Check JVM heap usage across all Cassandra nodes via Prometheus.

    Thresholds:
    - WARNING: Heap usage > 75%
    - CRITICAL: Heap usage > 85%

    Args:
        connector: Database connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        Structured findings with per-node heap usage and cluster aggregate
    """
    # Initialize builder
    builder = CheckContentBuilder()
    builder.h3("JVM Heap Usage (Prometheus)")

    # Check if Prometheus is enabled
    prom_ok, skip_msg, skip_data = require_prometheus(settings, "JVM heap metrics")
    if not prom_ok:
        builder.add(skip_msg)
        return builder.build(), {'prometheus_jvm_heap': skip_data}

    try:
        # Import here to avoid dependency if not using Prometheus
        from plugins.common.prometheus_client import get_instaclustr_client

        # Get cached Prometheus client (avoids rate limiting)
        client = get_instaclustr_client(
            cluster_id=settings['instaclustr_cluster_id'],
            username=settings['instaclustr_prometheus_username'],
            api_key=settings['instaclustr_prometheus_api_key'],
            prometheus_base_url=settings['instaclustr_prometheus_base_url']
        )

        # Get JVM heap metrics
        heap_data = client.get_cassandra_jvm_heap()

        if heap_data.get('status') != 'success':
            error_msg = heap_data.get('error_message', 'Failed to retrieve heap metrics')
            builder.error(f"❌ {error_msg}")
            findings = {
                'prometheus_jvm_heap': {
                    'status': 'error',
                    'error_message': error_msg,
                    'data': [],
                    'metadata': heap_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Analyze heap usage
        node_data = heap_data.get('data', [])

        if not node_data:
            builder.error("❌ No heap data available")
            findings = {
                'prometheus_jvm_heap': {
                    'status': 'error',
                    'error_message': 'No heap data available',
                    'data': [],
                    'metadata': heap_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Calculate cluster aggregates
        total_heap_used = sum(node['heap_used'] for node in node_data)
        total_heap_max = sum(node['heap_max'] for node in node_data)
        cluster_heap_percent = (total_heap_used / total_heap_max * 100) if total_heap_max > 0 else 0

        # Find nodes with high heap usage
        high_heap_nodes = []
        critical_heap_nodes = []

        for node in node_data:
            heap_percent = node['heap_used_percent']

            if heap_percent >= 85:
                critical_heap_nodes.append({
                    'node_id': node['node_id'],
                    'public_ip': node.get('public_ip', 'unknown'),
                    'heap_percent': round(heap_percent, 2),
                    'heap_used_gb': round(node['heap_used'] / (1024**3), 2),
                    'heap_max_gb': round(node['heap_max'] / (1024**3), 2)
                })
            elif heap_percent >= 75:
                high_heap_nodes.append({
                    'node_id': node['node_id'],
                    'public_ip': node.get('public_ip', 'unknown'),
                    'heap_percent': round(heap_percent, 2),
                    'heap_used_gb': round(node['heap_used'] / (1024**3), 2),
                    'heap_max_gb': round(node['heap_max'] / (1024**3), 2)
                })

        # Determine overall status
        if critical_heap_nodes:
            status = 'critical'
            severity = 10
            message = f"{len(critical_heap_nodes)} node(s) with critical heap usage (>85%)"
        elif high_heap_nodes:
            status = 'warning'
            severity = 7
            message = f"{len(high_heap_nodes)} node(s) with high heap usage (>75%)"
        else:
            status = 'healthy'
            severity = 0
            message = f"All {len(node_data)} nodes have healthy heap usage (<75%)"

        # Build findings
        findings = {
            'prometheus_jvm_heap': {
                'status': status,
                'severity': severity,
                'message': message,
                'per_node_heap_usage': {
                    'status': status,
                    'data': node_data,
                    'metadata': {
                        'source': 'prometheus',
                        'metric': 'jvm_heap',
                        'node_count': len(node_data)
                    }
                },
                'cluster_aggregate': {
                    'total_heap_used_bytes': total_heap_used,
                    'total_heap_max_bytes': total_heap_max,
                    'total_heap_used_gb': round(total_heap_used / (1024**3), 2),
                    'total_heap_max_gb': round(total_heap_max / (1024**3), 2),
                    'cluster_heap_percent': round(cluster_heap_percent, 2),
                    'node_count': len(node_data)
                }
            }
        }

        # Add warnings if any
        if critical_heap_nodes:
            findings['prometheus_jvm_heap']['critical_nodes'] = {
                'count': len(critical_heap_nodes),
                'nodes': critical_heap_nodes,
                'recommendation': 'Investigate GC logs, consider heap tuning, check for memory leaks'
            }

        if high_heap_nodes:
            findings['prometheus_jvm_heap']['high_heap_nodes'] = {
                'count': len(high_heap_nodes),
                'nodes': high_heap_nodes,
                'recommendation': 'Monitor closely, review GC activity, consider preventive action'
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
        builder.para(f"- Total Heap: {findings['prometheus_jvm_heap']['cluster_aggregate']['total_heap_used_gb']} GB / {findings['prometheus_jvm_heap']['cluster_aggregate']['total_heap_max_gb']} GB")
        builder.para(f"- Cluster Usage: {findings['prometheus_jvm_heap']['cluster_aggregate']['cluster_heap_percent']}%")
        builder.para(f"- Nodes Monitored: {findings['prometheus_jvm_heap']['cluster_aggregate']['node_count']}")

        if critical_heap_nodes:
            builder.blank()
            builder.para(f"*⚠️  Critical Nodes ({len(critical_heap_nodes)}):*")
            for node in critical_heap_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['heap_percent']}% heap")
            builder.para(f"_Recommendation: {findings['prometheus_jvm_heap']['critical_nodes']['recommendation']}_")

        if high_heap_nodes:
            builder.blank()
            builder.para(f"*⚠️  High Heap Nodes ({len(high_heap_nodes)}):*")
            for node in high_heap_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['heap_percent']}% heap")
            builder.para(f"_Recommendation: {findings['prometheus_jvm_heap']['high_heap_nodes']['recommendation']}_")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus JVM heap check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'prometheus_jvm_heap': {
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

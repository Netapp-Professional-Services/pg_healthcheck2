"""
Cassandra Disk Usage Check (Prometheus)

Monitors disk utilization across all cluster nodes using Prometheus metrics.
Alerts on high disk usage that could lead to operational issues.

Health Check: prometheus_disk_usage
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true
"""

import logging
from typing import Dict
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def check_prometheus_disk_usage(connector, settings):
    """
    Check disk utilization across all Cassandra nodes via Prometheus.

    Thresholds:
    - WARNING: Disk usage > 70%
    - CRITICAL: Disk usage > 85%

    Args:
        settings: Configuration dictionary with Prometheus credentials
        connector: Not used (Prometheus client created directly)

    Returns:
        Structured findings with per-node disk usage and cluster aggregate
    """
    # Initialize builder
    builder = CheckContentBuilder()
    builder.h3("Disk Usage (Prometheus)")

    # Check if Prometheus is enabled
    if not settings.get('instaclustr_prometheus_enabled'):
        # Check skipped - Prometheus monitoring not enabled
        findings = {
            'prometheus_disk_usage': {
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
            prometheus_base_url=settings['instaclustr_prometheus_base_url']
        )

        # Get disk utilization metrics
        disk_data = client.get_cassandra_disk_usage()

        if disk_data.get('status') != 'success':
            error_msg = disk_data.get('error_message', 'Failed to retrieve disk metrics')
            builder.error(f"❌ {error_msg}")
            findings = {
                'prometheus_disk_usage': {
                    'status': 'error',
                    'error_message': error_msg,
                    'data': [],
                    'metadata': disk_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Analyze disk usage
        node_data = disk_data.get('data', [])

        if not node_data:
            builder.error("❌ No disk data available")
            findings = {
                'prometheus_disk_usage': {
                    'status': 'error',
                    'error_message': 'No disk data available',
                    'data': [],
                    'metadata': disk_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Calculate cluster aggregates
        avg_disk_percent = sum(node['value'] for node in node_data) / len(node_data)

        # Find nodes with high disk usage
        high_disk_nodes = []
        critical_disk_nodes = []

        for node in node_data:
            disk_percent = node['value']

            node_info = {
                'node_id': node['node_id'],
                'public_ip': node.get('public_ip', 'unknown'),
                'datacenter': node.get('datacenter', 'unknown'),
                'rack': node.get('rack', 'unknown'),
                'disk_percent': round(disk_percent, 2)
            }

            if disk_percent >= 85:
                critical_disk_nodes.append(node_info)
            elif disk_percent >= 70:
                high_disk_nodes.append(node_info)

        # Determine overall status
        if critical_disk_nodes:
            status = 'critical'
            severity = 10
            message = f"{len(critical_disk_nodes)} node(s) with critical disk usage (>85%)"
        elif high_disk_nodes:
            status = 'warning'
            severity = 7
            message = f"{len(high_disk_nodes)} node(s) with high disk usage (>70%)"
        else:
            status = 'healthy'
            severity = 0
            message = f"All {len(node_data)} nodes have healthy disk usage (<70%)"

        # Build findings
        findings = {
            'prometheus_disk_usage': {
                'status': status,
                'severity': severity,
                'message': message,
                'per_node_disk_usage': {
                    'status': status,
                    'data': node_data,
                    'metadata': {
                        'source': 'prometheus',
                        'metric': 'disk_utilization',
                        'node_count': len(node_data)
                    }
                },
                'cluster_aggregate': {
                    'average_disk_percent': round(avg_disk_percent, 2),
                    'node_count': len(node_data),
                    'max_disk_percent': round(max(node['value'] for node in node_data), 2),
                    'min_disk_percent': round(min(node['value'] for node in node_data), 2)
                }
            }
        }

        # Add warnings if any
        if critical_disk_nodes:
            findings['prometheus_disk_usage']['critical_nodes'] = {
                'count': len(critical_disk_nodes),
                'nodes': critical_disk_nodes,
                'recommendation': 'URGENT: Add capacity, run compaction, clean snapshots/backups, investigate data growth'
            }

        if high_disk_nodes:
            findings['prometheus_disk_usage']['high_disk_nodes'] = {
                'count': len(high_disk_nodes),
                'nodes': high_disk_nodes,
                'recommendation': 'Plan capacity increase, review compaction strategy, clean old snapshots'
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
        builder.para(f"- Average Disk: {findings['prometheus_disk_usage']['cluster_aggregate']['average_disk_percent']}%")
        builder.para(f"- Max Disk: {findings['prometheus_disk_usage']['cluster_aggregate']['max_disk_percent']}%")
        builder.para(f"- Min Disk: {findings['prometheus_disk_usage']['cluster_aggregate']['min_disk_percent']}%")
        builder.para(f"- Nodes Monitored: {findings['prometheus_disk_usage']['cluster_aggregate']['node_count']}")

        if critical_disk_nodes:
            builder.blank()
            builder.para(f"*⚠️  Critical Nodes ({len(critical_disk_nodes)}):*")
            for node in critical_disk_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['disk_percent']}% disk")
            builder.para(f"_Recommendation: {findings['prometheus_disk_usage']['critical_nodes']['recommendation']}_")

        if high_disk_nodes:
            builder.blank()
            builder.para(f"*⚠️  High Disk Nodes ({len(high_disk_nodes)}):*")
            for node in high_disk_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['disk_percent']}% disk")
            builder.para(f"_Recommendation: {findings['prometheus_disk_usage']['high_disk_nodes']['recommendation']}_")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus disk usage check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'prometheus_disk_usage': {
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

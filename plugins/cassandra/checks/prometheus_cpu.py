"""
Cassandra CPU Utilization Check (Prometheus)

Monitors CPU utilization across all cluster nodes using Prometheus metrics.
Alerts on high CPU usage that could indicate capacity constraints or performance issues.

Health Check: prometheus_cpu
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true
"""

import logging
from typing import Dict, Tuple
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, require_prometheus

logger = logging.getLogger(__name__)


def check_prometheus_cpu(connector, settings):
    """
    Check CPU utilization across all Cassandra nodes via Prometheus.

    Thresholds:
    - WARNING: CPU > 75%
    - CRITICAL: CPU > 90%

    Args:
        settings: Configuration dictionary with Prometheus credentials
        connector: Not used (Prometheus client created directly)

    Returns:
        Structured findings with per-node CPU usage and cluster average
    """
    # Initialize builder
    builder = CheckContentBuilder()
    builder.h3("CPU Utilization (Prometheus)")

    # Check if Prometheus is enabled
    prom_ok, skip_msg, skip_data = require_prometheus(settings, "CPU metrics")
    if not prom_ok:
        builder.add(skip_msg)
        return builder.build(), {'prometheus_cpu': skip_data}

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

        # Get CPU utilization metrics
        cpu_data = client.get_cassandra_cpu_utilization()

        if cpu_data.get('status') != 'success':
            error_msg = cpu_data.get('error_message', 'Failed to retrieve CPU metrics')
            builder.error(f"❌ {error_msg}")
            findings = {
                'prometheus_cpu': {
                    'status': 'error',
                    'error_message': error_msg,
                    'data': [],
                    'metadata': cpu_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Analyze CPU usage
        node_data = cpu_data.get('data', [])

        if not node_data:
            builder.error("❌ No CPU data available")
            findings = {
                'prometheus_cpu': {
                    'status': 'error',
                    'error_message': 'No CPU data available',
                    'data': [],
                    'metadata': cpu_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Calculate cluster aggregates
        avg_cpu_percent = sum(node['value'] for node in node_data) / len(node_data)

        # Find nodes with high CPU usage
        high_cpu_nodes = []
        critical_cpu_nodes = []

        for node in node_data:
            cpu_percent = node['value']

            node_info = {
                'node_id': node['node_id'],
                'public_ip': node.get('public_ip', 'unknown'),
                'datacenter': node.get('datacenter', 'unknown'),
                'rack': node.get('rack', 'unknown'),
                'cpu_percent': round(cpu_percent, 2)
            }

            if cpu_percent >= 90:
                critical_cpu_nodes.append(node_info)
            elif cpu_percent >= 75:
                high_cpu_nodes.append(node_info)

        # Determine overall status
        if critical_cpu_nodes:
            status = 'critical'
            severity = 10
            message = f"{len(critical_cpu_nodes)} node(s) with critical CPU usage (>90%)"
        elif high_cpu_nodes:
            status = 'warning'
            severity = 7
            message = f"{len(high_cpu_nodes)} node(s) with high CPU usage (>75%)"
        else:
            status = 'healthy'
            severity = 0
            message = f"All {len(node_data)} nodes have healthy CPU usage (<75%)"

        # Build findings
        findings = {
            'prometheus_cpu': {
                'status': status,
                'severity': severity,
                'message': message,
                'per_node_cpu_usage': {
                    'status': status,
                    'data': node_data,
                    'metadata': {
                        'source': 'prometheus',
                        'metric': 'cpu_utilization',
                        'unit': 'percent',
                        'node_count': len(node_data)
                    }
                },
                'cluster_aggregate': {
                    'average_cpu_percent': round(avg_cpu_percent, 2),
                    'max_cpu_percent': round(max(node['value'] for node in node_data), 2),
                    'min_cpu_percent': round(min(node['value'] for node in node_data), 2),
                    'node_count': len(node_data)
                }
            }
        }

        # Add warnings if any
        if critical_cpu_nodes:
            findings['prometheus_cpu']['critical_nodes'] = {
                'count': len(critical_cpu_nodes),
                'nodes': critical_cpu_nodes,
                'recommendation': 'URGENT: Critical CPU usage. Review query patterns, check for resource-intensive operations, consider scaling'
            }

        if high_cpu_nodes:
            findings['prometheus_cpu']['high_cpu_nodes'] = {
                'count': len(high_cpu_nodes),
                'nodes': high_cpu_nodes,
                'recommendation': 'Monitor CPU trends, review workload distribution, plan capacity if sustained'
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
        builder.para(f"- Average CPU: {findings['prometheus_cpu']['cluster_aggregate']['average_cpu_percent']}%")
        builder.para(f"- Max CPU: {findings['prometheus_cpu']['cluster_aggregate']['max_cpu_percent']}%")
        builder.para(f"- Min CPU: {findings['prometheus_cpu']['cluster_aggregate']['min_cpu_percent']}%")
        builder.para(f"- Nodes Monitored: {findings['prometheus_cpu']['cluster_aggregate']['node_count']}")

        if critical_cpu_nodes:
            builder.blank()
            builder.para(f"*⚠️  Critical Nodes ({len(critical_cpu_nodes)}):*")
            for node in critical_cpu_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['cpu_percent']}% CPU")
            builder.para(f"_Recommendation: {findings['prometheus_cpu']['critical_nodes']['recommendation']}_")

        if high_cpu_nodes:
            builder.blank()
            builder.para(f"*⚠️  High CPU Nodes ({len(high_cpu_nodes)}):*")
            for node in high_cpu_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['cpu_percent']}% CPU")
            builder.para(f"_Recommendation: {findings['prometheus_cpu']['high_cpu_nodes']['recommendation']}_")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus CPU check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'prometheus_cpu': {
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

"""
Cassandra Compaction Check (Prometheus)

Monitors pending compaction tasks across all cluster nodes using Prometheus metrics.
Alerts on compaction backlog that could indicate performance or capacity issues.

Health Check: prometheus_compaction
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true
"""

import logging
from typing import Dict
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, require_prometheus

logger = logging.getLogger(__name__)


def check_prometheus_compaction(connector, settings):
    """
    Check pending compaction tasks across all Cassandra nodes via Prometheus.

    Thresholds:
    - WARNING: Pending tasks > 5
    - CRITICAL: Pending tasks > 20

    Args:
        connector: Database connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        Structured findings with per-node pending compactions and cluster total
    """
    # Initialize builder
    builder = CheckContentBuilder()
    builder.h3("Compaction Pending Tasks (Prometheus)")

    # Check if Prometheus is enabled
    prom_ok, skip_msg, skip_data = require_prometheus(settings, "compaction metrics")
    if not prom_ok:
        builder.add(skip_msg)
        return builder.build(), {'prometheus_compaction': skip_data}

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

        # Get compaction metrics
        compaction_data = client.get_cassandra_compaction_pending()

        if compaction_data.get('status') != 'success':
            error_msg = compaction_data.get('error_message', 'Failed to retrieve compaction metrics')
            builder.error(f"❌ {error_msg}")
            findings = {
                'prometheus_compaction': {
                    'status': 'error',
                    'error_message': error_msg,
                    'data': [],
                    'metadata': compaction_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Analyze compaction data
        node_data = compaction_data.get('data', [])

        if not node_data:
            builder.error("❌ No compaction data available")
            findings = {
                'prometheus_compaction': {
                    'status': 'error',
                    'error_message': 'No compaction data available',
                    'data': [],
                    'metadata': compaction_data.get('metadata', {})
                }
            }
            return builder.build(), findings

        # Calculate cluster aggregates
        total_pending = sum(int(node['value']) for node in node_data)
        avg_pending = total_pending / len(node_data)

        # Find nodes with high pending compactions
        high_pending_nodes = []
        critical_pending_nodes = []

        for node in node_data:
            pending_count = int(node['value'])

            if pending_count > 0:  # Only track nodes with pending tasks
                node_info = {
                    'node_id': node['node_id'],
                    'public_ip': node.get('public_ip', 'unknown'),
                    'datacenter': node.get('datacenter', 'unknown'),
                    'rack': node.get('rack', 'unknown'),
                    'pending_tasks': pending_count
                }

                if pending_count > 20:
                    critical_pending_nodes.append(node_info)
                elif pending_count > 5:
                    high_pending_nodes.append(node_info)

        # Determine overall status
        if critical_pending_nodes:
            status = 'critical'
            severity = 10
            message = f"{len(critical_pending_nodes)} node(s) with critical compaction backlog (>20 tasks)"
        elif high_pending_nodes:
            status = 'warning'
            severity = 7
            message = f"{len(high_pending_nodes)} node(s) with high compaction backlog (>5 tasks)"
        elif total_pending > 0:
            status = 'healthy'
            severity = 0
            message = f"Compaction activity normal ({total_pending} total pending tasks)"
        else:
            status = 'healthy'
            severity = 0
            message = f"No pending compactions across {len(node_data)} nodes"

        # Build findings
        findings = {
            'prometheus_compaction': {
                'status': status,
                'severity': severity,
                'message': message,
                'per_node_compaction': {
                    'status': status,
                    'data': node_data,
                    'metadata': {
                        'source': 'prometheus',
                        'metric': 'compaction_pending',
                        'node_count': len(node_data)
                    }
                },
                'cluster_aggregate': {
                    'total_pending_tasks': total_pending,
                    'average_pending_tasks': round(avg_pending, 2),
                    'max_pending_tasks': max(int(node['value']) for node in node_data),
                    'nodes_with_pending': len([n for n in node_data if int(n['value']) > 0]),
                    'node_count': len(node_data)
                }
            }
        }

        # Add warnings if any
        if critical_pending_nodes:
            findings['prometheus_compaction']['critical_nodes'] = {
                'count': len(critical_pending_nodes),
                'nodes': critical_pending_nodes,
                'recommendation': 'URGENT: High compaction backlog. Check disk I/O, increase compaction throughput, investigate write load'
            }

        if high_pending_nodes:
            findings['prometheus_compaction']['high_pending_nodes'] = {
                'count': len(high_pending_nodes),
                'nodes': high_pending_nodes,
                'recommendation': 'Monitor compaction progress, review compaction strategy, check for large partitions'
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
        builder.para(f"- Total Pending: {findings['prometheus_compaction']['cluster_aggregate']['total_pending_tasks']} tasks")
        builder.para(f"- Average Pending: {findings['prometheus_compaction']['cluster_aggregate']['average_pending_tasks']} tasks/node")
        builder.para(f"- Max Pending: {findings['prometheus_compaction']['cluster_aggregate']['max_pending_tasks']} tasks")
        builder.para(f"- Nodes with Pending: {findings['prometheus_compaction']['cluster_aggregate']['nodes_with_pending']}/{findings['prometheus_compaction']['cluster_aggregate']['node_count']}")

        if critical_pending_nodes:
            builder.blank()
            builder.para(f"*⚠️  Critical Nodes ({len(critical_pending_nodes)}):*")
            for node in critical_pending_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['pending_tasks']} pending tasks")
            builder.para(f"_Recommendation: {findings['prometheus_compaction']['critical_nodes']['recommendation']}_")

        if high_pending_nodes:
            builder.blank()
            builder.para(f"*⚠️  High Pending Nodes ({len(high_pending_nodes)}):*")
            for node in high_pending_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['pending_tasks']} pending tasks")
            builder.para(f"_Recommendation: {findings['prometheus_compaction']['high_pending_nodes']['recommendation']}_")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus compaction check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'prometheus_compaction': {
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

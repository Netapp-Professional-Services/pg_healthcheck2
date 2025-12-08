"""
Cassandra Latency Check (Prometheus)

Monitors read and write latency (p95) across all cluster nodes using Prometheus metrics.
Alerts on degraded performance that could indicate capacity, tuning, or hardware issues.

Health Check: prometheus_latency
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true
"""

import logging
from typing import Dict
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, require_prometheus

logger = logging.getLogger(__name__)


def check_prometheus_latency(connector, settings):
    """
    Check read/write latency (p95) across all Cassandra nodes via Prometheus.

    Thresholds (p95):
    - Read WARNING: > 50ms, CRITICAL: > 100ms
    - Write WARNING: > 30ms, CRITICAL: > 75ms

    Args:
        connector: Database connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        Structured findings with per-node latency and cluster averages
    """
    # Initialize builder
    builder = CheckContentBuilder()
    builder.h3("Read/Write Latency P95 (Prometheus)")

    # Check if Prometheus is enabled
    prom_ok, skip_msg, skip_data = require_prometheus(settings, "latency metrics")
    if not prom_ok:
        builder.add(skip_msg)
        return builder.build(), {'prometheus_latency': skip_data}

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

        # Get read and write latency metrics
        read_latency_data = client.get_cassandra_read_latency()
        write_latency_data = client.get_cassandra_write_latency()

        # Check if we got data
        read_success = read_latency_data.get('status') == 'success'
        write_success = write_latency_data.get('status') == 'success'

        if not read_success and not write_success:
            builder.error("❌ Failed to retrieve latency metrics")
            findings = {
                'prometheus_latency': {
                    'status': 'error',
                    'error_message': 'Failed to retrieve latency metrics',
                    'data': [],
                    'metadata': {
                        'source': 'prometheus',
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                }
            }
            return builder.build(), findings

        # Process read latency
        read_nodes = read_latency_data.get('data', []) if read_success else []
        write_nodes = write_latency_data.get('data', []) if write_success else []

        # Convert microseconds to milliseconds
        for node in read_nodes:
            node['latency_ms'] = round(node['value'] / 1000, 2)

        for node in write_nodes:
            node['latency_ms'] = round(node['value'] / 1000, 2)

        # Analyze latency
        high_read_latency_nodes = []
        critical_read_latency_nodes = []
        high_write_latency_nodes = []
        critical_write_latency_nodes = []

        # Read latency thresholds (ms)
        READ_WARNING_MS = 50
        READ_CRITICAL_MS = 100

        # Write latency thresholds (ms)
        WRITE_WARNING_MS = 30
        WRITE_CRITICAL_MS = 75

        # Check read latency
        for node in read_nodes:
            latency_ms = node['latency_ms']

            node_info = {
                'node_id': node['node_id'],
                'public_ip': node.get('public_ip', 'unknown'),
                'datacenter': node.get('datacenter', 'unknown'),
                'latency_ms': latency_ms,
                'latency_us': node['value']
            }

            if latency_ms > READ_CRITICAL_MS:
                critical_read_latency_nodes.append(node_info)
            elif latency_ms > READ_WARNING_MS:
                high_read_latency_nodes.append(node_info)

        # Check write latency
        for node in write_nodes:
            latency_ms = node['latency_ms']

            node_info = {
                'node_id': node['node_id'],
                'public_ip': node.get('public_ip', 'unknown'),
                'datacenter': node.get('datacenter', 'unknown'),
                'latency_ms': latency_ms,
                'latency_us': node['value']
            }

            if latency_ms > WRITE_CRITICAL_MS:
                critical_write_latency_nodes.append(node_info)
            elif latency_ms > WRITE_WARNING_MS:
                high_write_latency_nodes.append(node_info)

        # Determine overall status
        critical_count = len(critical_read_latency_nodes) + len(critical_write_latency_nodes)
        warning_count = len(high_read_latency_nodes) + len(high_write_latency_nodes)

        if critical_count > 0:
            status = 'critical'
            severity = 10
            message = f"Critical latency detected on {critical_count} measurement(s)"
        elif warning_count > 0:
            status = 'warning'
            severity = 7
            message = f"High latency detected on {warning_count} measurement(s)"
        else:
            status = 'healthy'
            severity = 0
            message = f"Latency healthy across all nodes (p95 read <{READ_WARNING_MS}ms, write <{WRITE_WARNING_MS}ms)"

        # Calculate aggregates
        read_avg = sum(n['latency_ms'] for n in read_nodes) / len(read_nodes) if read_nodes else 0
        write_avg = sum(n['latency_ms'] for n in write_nodes) / len(write_nodes) if write_nodes else 0

        # Build findings
        findings = {
            'prometheus_latency': {
                'status': status,
                'severity': severity,
                'message': message,
                'read_latency_p95': {
                    'status': 'critical' if critical_read_latency_nodes else ('warning' if high_read_latency_nodes else 'healthy'),
                    'data': read_nodes,
                    'metadata': {
                        'source': 'prometheus',
                        'metric': 'read_latency_p95',
                        'unit': 'microseconds',
                        'node_count': len(read_nodes)
                    }
                },
                'write_latency_p95': {
                    'status': 'critical' if critical_write_latency_nodes else ('warning' if high_write_latency_nodes else 'healthy'),
                    'data': write_nodes,
                    'metadata': {
                        'source': 'prometheus',
                        'metric': 'write_latency_p95',
                        'unit': 'microseconds',
                        'node_count': len(write_nodes)
                    }
                },
                'cluster_aggregate': {
                    'average_read_latency_ms': round(read_avg, 2),
                    'average_write_latency_ms': round(write_avg, 2),
                    'max_read_latency_ms': round(max([n['latency_ms'] for n in read_nodes]), 2) if read_nodes else 0,
                    'max_write_latency_ms': round(max([n['latency_ms'] for n in write_nodes]), 2) if write_nodes else 0,
                    'thresholds': {
                        'read_warning_ms': READ_WARNING_MS,
                        'read_critical_ms': READ_CRITICAL_MS,
                        'write_warning_ms': WRITE_WARNING_MS,
                        'write_critical_ms': WRITE_CRITICAL_MS
                    }
                }
            }
        }

        # Add warnings if any
        if critical_read_latency_nodes:
            findings['prometheus_latency']['critical_read_latency'] = {
                'count': len(critical_read_latency_nodes),
                'nodes': critical_read_latency_nodes,
                'recommendation': f'URGENT: Read latency >{READ_CRITICAL_MS}ms. Check disk I/O, GC, table design, query patterns'
            }

        if high_read_latency_nodes:
            findings['prometheus_latency']['high_read_latency'] = {
                'count': len(high_read_latency_nodes),
                'nodes': high_read_latency_nodes,
                'recommendation': f'Read latency >{READ_WARNING_MS}ms. Monitor disk performance, review queries, check caching'
            }

        if critical_write_latency_nodes:
            findings['prometheus_latency']['critical_write_latency'] = {
                'count': len(critical_write_latency_nodes),
                'nodes': critical_write_latency_nodes,
                'recommendation': f'URGENT: Write latency >{WRITE_CRITICAL_MS}ms. Check disk I/O, compaction backlog, commit log sync'
            }

        if high_write_latency_nodes:
            findings['prometheus_latency']['high_write_latency'] = {
                'count': len(high_write_latency_nodes),
                'nodes': high_write_latency_nodes,
                'recommendation': f'Write latency >{WRITE_WARNING_MS}ms. Monitor disk performance, review compaction strategy'
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
        builder.para(f"- Avg Read Latency (p95): {findings['prometheus_latency']['cluster_aggregate']['average_read_latency_ms']} ms")
        builder.para(f"- Avg Write Latency (p95): {findings['prometheus_latency']['cluster_aggregate']['average_write_latency_ms']} ms")
        builder.para(f"- Max Read Latency: {findings['prometheus_latency']['cluster_aggregate']['max_read_latency_ms']} ms")
        builder.para(f"- Max Write Latency: {findings['prometheus_latency']['cluster_aggregate']['max_write_latency_ms']} ms")

        if critical_read_latency_nodes:
            builder.blank()
            builder.para(f"*⚠️  Critical Read Latency ({len(critical_read_latency_nodes)}):*")
            for node in critical_read_latency_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['latency_ms']} ms")
            builder.para(f"_Recommendation: {findings['prometheus_latency']['critical_read_latency']['recommendation']}_")

        if high_read_latency_nodes:
            builder.blank()
            builder.para(f"*⚠️  High Read Latency ({len(high_read_latency_nodes)}):*")
            for node in high_read_latency_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['latency_ms']} ms")
            builder.para(f"_Recommendation: {findings['prometheus_latency']['high_read_latency']['recommendation']}_")

        if critical_write_latency_nodes:
            builder.blank()
            builder.para(f"*⚠️  Critical Write Latency ({len(critical_write_latency_nodes)}):*")
            for node in critical_write_latency_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['latency_ms']} ms")
            builder.para(f"_Recommendation: {findings['prometheus_latency']['critical_write_latency']['recommendation']}_")

        if high_write_latency_nodes:
            builder.blank()
            builder.para(f"*⚠️  High Write Latency ({len(high_write_latency_nodes)}):*")
            for node in high_write_latency_nodes:
                builder.para(f"- Node {node['node_id']} ({node.get('public_ip', 'unknown')}): {node['latency_ms']} ms")
            builder.para(f"_Recommendation: {findings['prometheus_latency']['high_write_latency']['recommendation']}_")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus latency check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'prometheus_latency': {
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

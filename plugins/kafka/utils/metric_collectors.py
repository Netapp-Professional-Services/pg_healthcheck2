"""
Kafka Metric Collection Strategies

Provides unified interface for collecting Kafka metrics from multiple sources:
1. Instaclustr Prometheus API (external, HTTP-based, no SSH required)
2. Local Prometheus JMX Exporter (SSH + HTTP, typically port 7500)
3. Standard JMX (SSH + JMX, typically port 9999)

This allows checks to be adaptive and work across all Kafka deployment types.
"""

import re
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)


# ============================================================================
# Method 1: Instaclustr Prometheus API
# ============================================================================

def collect_from_instaclustr_prometheus(metric_name: str, connector, settings) -> Optional[Dict[str, Any]]:
    """
    Collect metrics from Instaclustr Prometheus API.

    Args:
        metric_name: Name of the metric to collect (e.g., 'under_replicated_partitions')
        connector: Kafka connector instance
        settings: Configuration settings

    Returns:
        Dictionary with metric data or None if not available
        {
            'method': 'instaclustr_prometheus',
            'broker_metrics': {broker_id: value, ...},
            'cluster_total': sum,
            'cluster_avg': average
        }
    """
    if not settings.get('instaclustr_prometheus_enabled'):
        return None

    try:
        from plugins.common.prometheus_client import get_instaclustr_client

        client = get_instaclustr_client(settings)
        if not client:
            return None

        # Map metric names to Instaclustr Prometheus metric names
        metric_map = {
            'under_replicated_partitions': 'ic_node_under_replicated_partitions',
            'offline_partitions': 'ic_node_offline_partitions_kraft',
            'unclean_elections': 'ic_node_unclean_leader_elections_kraft',
        }

        prom_metric = metric_map.get(metric_name)
        if not prom_metric:
            return None

        # Fetch metrics
        metrics = client.fetch_all_metrics()
        broker_metrics = {}

        for metric in metrics:
            if metric['name'] == prom_metric:
                broker_id = metric['labels'].get('instance', 'unknown')
                broker_metrics[broker_id] = float(metric['value'])

        if not broker_metrics:
            return None

        return {
            'method': 'instaclustr_prometheus',
            'broker_metrics': broker_metrics,
            'cluster_total': sum(broker_metrics.values()),
            'cluster_avg': sum(broker_metrics.values()) / len(broker_metrics),
            'broker_count': len(broker_metrics)
        }

    except Exception as e:
        logger.debug(f"Instaclustr Prometheus collection failed: {e}")
        return None


# ============================================================================
# Method 2: Local Prometheus JMX Exporter (via SSH)
# ============================================================================

def collect_from_local_prometheus(metric_name: str, ssh_client, port: int = 7500) -> Optional[float]:
    """
    Collect a single metric from local Prometheus JMX exporter via SSH.

    Args:
        metric_name: Prometheus metric name (e.g., 'kafka_server_replicamanager_underreplicatedpartitions')
        ssh_client: SSH client instance
        port: Prometheus exporter port (default: 7500)

    Returns:
        Metric value as float, or None if not available
    """
    try:
        # Query Prometheus metrics endpoint via curl
        cmd = f'curl -s http://localhost:{port}/metrics | grep "^{metric_name}\\s"'
        result = ssh_client.execute_command(cmd)

        if not result or not result.strip():
            return None

        # Parse metric line: "metric_name{labels} value"
        # Example: kafka_server_replicamanager_underreplicatedpartitions 0.0
        match = re.search(r'(\S+)\s+([\d.eE+-]+)', result.strip())
        if match:
            return float(match.group(2))

        return None

    except Exception as e:
        logger.debug(f"Local Prometheus collection failed: {e}")
        return None


def detect_prometheus_exporter_port(ssh_client) -> Optional[int]:
    """
    Auto-detect Prometheus JMX exporter port from Kafka process.

    Args:
        ssh_client: SSH client instance

    Returns:
        Port number or None if not found
    """
    try:
        # Look for javaagent with port configuration
        cmd = "ps aux | grep jmx_prometheus_javaagent | grep -v grep | grep -oP 'javaagent:[^=]+=\\K[0-9]+'"
        result = ssh_client.execute_command(cmd)

        if result and result.strip().isdigit():
            return int(result.strip())

        # Default port
        return 7500

    except Exception:
        return None


# ============================================================================
# Method 3: Standard JMX (via SSH)
# ============================================================================

def collect_from_jmx(mbean: str, attribute: str, ssh_client,
                     jmx_port: int = 9999, kafka_home: str = "/opt/kafka") -> Optional[float]:
    """
    Collect metric from standard JMX via SSH using Kafka's JmxTool.

    Args:
        mbean: JMX MBean name (e.g., 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions')
        attribute: Attribute to query (e.g., 'Value', 'Count')
        ssh_client: SSH client instance
        jmx_port: JMX port (default: 9999)
        kafka_home: Kafka installation directory

    Returns:
        Metric value as float, or None if not available
    """
    try:
        from plugins.kafka.utils.jmx_helper import query_jmx_via_kafka_tools

        result = query_jmx_via_kafka_tools(ssh_client, mbean, attribute, jmx_port, kafka_home)

        if result is not None:
            try:
                return float(result)
            except (ValueError, TypeError):
                return None

        return None

    except Exception as e:
        logger.debug(f"JMX collection failed: {e}")
        return None


# ============================================================================
# Unified Collection Interface
# ============================================================================

def collect_metric_adaptive(metric_name: str, connector, settings) -> Optional[Dict[str, Any]]:
    """
    Collect metric using best available method (waterfall approach).

    Tries methods in order:
    1. Instaclustr Prometheus API (external, no SSH)
    2. Local Prometheus JMX exporter via SSH (port 7500)
    3. Standard JMX via SSH (port 9999)

    Args:
        metric_name: Metric to collect (e.g., 'under_replicated_partitions')
        connector: Kafka connector instance
        settings: Configuration settings

    Returns:
        Dictionary with metric data:
        {
            'method': 'instaclustr_prometheus' | 'local_prometheus' | 'jmx',
            'broker_metrics': {broker_id/host: value, ...},
            'cluster_total': sum,
            'cluster_avg': average,
            'broker_count': count
        }
        Or None if no method worked
    """
    # Method 1: Try Instaclustr Prometheus API
    result = collect_from_instaclustr_prometheus(metric_name, connector, settings)
    if result:
        logger.info(f"Collected {metric_name} via Instaclustr Prometheus API")
        return result

    # Methods 2 & 3 require SSH
    from plugins.common.check_helpers import require_ssh
    available, _, _ = require_ssh(connector, f"{metric_name} collection")
    if not available:
        return None

    # Get SSH hosts
    ssh_hosts = connector.get_ssh_hosts()
    if not ssh_hosts:
        return None

    # Prepare for SSH-based collection
    broker_metrics = {}
    method_used = None

    # Metric name mappings
    prometheus_metric_map = {
        'under_replicated_partitions': 'kafka_server_replicamanager_underreplicatedpartitions',
        'offline_partitions': 'kafka_controller_kafkacontroller_offlinepartitionscount',
        'unclean_elections': 'kafka_controller_controllerstats_uncleanleaderelectionspersec_count',
    }

    jmx_mbean_map = {
        'under_replicated_partitions': {
            'mbean': 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions',
            'attribute': 'Value'
        },
        'offline_partitions': {
            'mbean': 'kafka.controller:type=KafkaController,name=OfflinePartitionsCount',
            'attribute': 'Value'
        },
        'unclean_elections': {
            'mbean': 'kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec',
            'attribute': 'Count'
        },
    }

    # Method 2: Try local Prometheus exporter
    prom_metric = prometheus_metric_map.get(metric_name)
    if prom_metric:
        for host in ssh_hosts:
            try:
                ssh_client = connector.ssh_manager.get_client(host)

                # Detect Prometheus port
                prom_port = detect_prometheus_exporter_port(ssh_client)
                if not prom_port:
                    continue

                value = collect_from_local_prometheus(prom_metric, ssh_client, prom_port)
                if value is not None:
                    broker_metrics[host] = value
                    method_used = 'local_prometheus'

            except Exception as e:
                logger.debug(f"Local Prometheus collection failed for {host}: {e}")
                continue

    # If we got data from local Prometheus, return it
    if broker_metrics and method_used == 'local_prometheus':
        logger.info(f"Collected {metric_name} via local Prometheus exporter")
        return {
            'method': 'local_prometheus',
            'broker_metrics': broker_metrics,
            'cluster_total': sum(broker_metrics.values()),
            'cluster_avg': sum(broker_metrics.values()) / len(broker_metrics) if broker_metrics else 0,
            'broker_count': len(broker_metrics)
        }

    # Method 3: Try standard JMX
    jmx_config = jmx_mbean_map.get(metric_name)
    if jmx_config:
        from plugins.kafka.utils.jmx_helper import detect_kafka_home, detect_jmx_port

        for host in ssh_hosts:
            try:
                ssh_client = connector.ssh_manager.get_client(host)

                # Auto-detect Kafka home and JMX port
                kafka_home = detect_kafka_home(ssh_client)
                if not kafka_home:
                    continue

                jmx_port = detect_jmx_port(ssh_client, kafka_home)

                value = collect_from_jmx(
                    jmx_config['mbean'],
                    jmx_config['attribute'],
                    ssh_client,
                    jmx_port,
                    kafka_home
                )

                if value is not None:
                    broker_metrics[host] = value
                    method_used = 'jmx'

            except Exception as e:
                logger.debug(f"JMX collection failed for {host}: {e}")
                continue

    # If we got data from JMX, return it
    if broker_metrics and method_used == 'jmx':
        logger.info(f"Collected {metric_name} via standard JMX")
        return {
            'method': 'jmx',
            'broker_metrics': broker_metrics,
            'cluster_total': sum(broker_metrics.values()),
            'cluster_avg': sum(broker_metrics.values()) / len(broker_metrics) if broker_metrics else 0,
            'broker_count': len(broker_metrics)
        }

    # Nothing worked
    logger.warning(f"Failed to collect {metric_name} using any method")
    return None


def get_collection_method_description(method: str) -> str:
    """
    Get human-readable description of collection method.

    Args:
        method: Method name ('instaclustr_prometheus', 'local_prometheus', 'jmx')

    Returns:
        Human-readable description
    """
    descriptions = {
        'instaclustr_prometheus': 'Instaclustr Prometheus API',
        'local_prometheus': 'Local Prometheus JMX Exporter (port 7500)',
        'jmx': 'Standard JMX (port 9999)'
    }
    return descriptions.get(method, 'Unknown method')

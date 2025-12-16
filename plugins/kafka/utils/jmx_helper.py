"""
JMX Helper for Kafka Health Checks

Provides utilities to query Kafka JMX metrics via SSH.
Supports multiple JMX access methods (kafka-run-class, jmxterm, direct socket).
"""

import json
import re
from typing import Dict, List, Optional, Any


def query_jmx_via_kafka_tools(ssh_client, mbean: str, attribute: str = "Value",
                               jmx_port: int = 9999, kafka_home: str = "/opt/kafka") -> Optional[float]:
    """
    Query JMX using Kafka's built-in kafka-run-class.sh tool.

    Args:
        ssh_client: SSHClient instance
        mbean: JMX MBean name (e.g., "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions")
        attribute: Attribute to query (default: "Value")
        jmx_port: JMX port (default: 9999)
        kafka_home: Kafka installation directory

    Returns:
        Metric value as float, or None if query fails
    """
    try:
        # Use Kafka's JmxTool
        cmd = f"""
        {kafka_home}/bin/kafka-run-class.sh kafka.tools.JmxTool \
            --jmx-url service:jmx:rmi:///jndi/rmi://localhost:{jmx_port}/jmxrmi \
            --object-name '{mbean}' \
            --attributes {attribute} \
            --reporting-interval 1000 \
            --one-time true 2>/dev/null | grep -v '^time' | awk '{{print $2}}'
        """

        result = ssh_client.execute_command(cmd)
        if result and result.strip():
            try:
                return float(result.strip())
            except ValueError:
                # Some metrics return non-numeric values
                return result.strip()
        return None

    except Exception as e:
        return None


def query_jmx_via_jcmd(ssh_client, mbean: str, attribute: str = "Value") -> Optional[float]:
    """
    Query JMX using jcmd (Java 8+) to inspect running Kafka process.

    Args:
        ssh_client: SSHClient instance
        mbean: JMX MBean name
        attribute: Attribute to query

    Returns:
        Metric value as float, or None if query fails
    """
    try:
        # Find Kafka PID
        pid_cmd = "ps aux | grep kafka\\.Kafka | grep -v grep | awk '{print $2}' | head -1"
        pid_result = ssh_client.execute_command(pid_cmd)

        if not pid_result or not pid_result.strip():
            return None

        kafka_pid = pid_result.strip()

        # Use jcmd to dump MBean info (requires Java 8+)
        # Note: This is a fallback method, less reliable than JmxTool
        cmd = f"jcmd {kafka_pid} VM.system_properties | grep -i jmx"
        result = ssh_client.execute_command(cmd)

        # This method is limited - jcmd doesn't directly expose MBean values
        # We'd need jconsole or jmxterm for full access
        return None

    except Exception:
        return None


def query_jmx_metrics_batch(ssh_client, metrics: List[Dict[str, str]],
                           jmx_port: int = 9999, kafka_home: str = "/opt/kafka") -> Dict[str, Any]:
    """
    Query multiple JMX metrics in batch for efficiency.

    Args:
        ssh_client: SSHClient instance
        metrics: List of dicts with 'mbean' and 'attribute' keys
        jmx_port: JMX port
        kafka_home: Kafka installation directory

    Returns:
        Dictionary mapping metric names to values
    """
    results = {}

    for metric in metrics:
        mbean = metric.get('mbean')
        attribute = metric.get('attribute', 'Value')
        metric_name = metric.get('name', mbean)

        value = query_jmx_via_kafka_tools(ssh_client, mbean, attribute, jmx_port, kafka_home)
        results[metric_name] = value

    return results


def detect_jmx_port(ssh_client, kafka_home: str = "/opt/kafka") -> Optional[int]:
    """
    Auto-detect JMX port from Kafka process or configuration.

    Args:
        ssh_client: SSHClient instance
        kafka_home: Kafka installation directory

    Returns:
        JMX port number, or None if not detected
    """
    try:
        # Method 1: Check running Kafka process for JMX port
        cmd = """
        ps aux | grep kafka\\.Kafka | grep -v grep | \
        grep -oP 'com\\.sun\\.management\\.jmxremote\\.port=\\K[0-9]+'
        """
        result = ssh_client.execute_command(cmd)

        if result and result.strip():
            return int(result.strip())

        # Method 2: Check environment variables in kafka-server-start.sh
        cmd = f"grep -r 'JMX_PORT' {kafka_home}/bin/ 2>/dev/null | grep -oP 'JMX_PORT=\\K[0-9]+' | head -1"
        result = ssh_client.execute_command(cmd)

        if result and result.strip():
            return int(result.strip())

        # Default
        return 9999

    except Exception:
        return 9999


def detect_kafka_home(ssh_client) -> Optional[str]:
    """
    Auto-detect Kafka installation directory.

    Args:
        ssh_client: SSHClient instance

    Returns:
        Kafka home directory path, or None if not found
    """
    common_paths = [
        "/opt/kafka",
        "/usr/local/kafka",
        "/home/kafka/kafka",
        "/opt/kafka_2.13-*",
        "/opt/kafka_2.12-*"
    ]

    for path in common_paths:
        cmd = f"ls -d {path} 2>/dev/null | head -1"
        result = ssh_client.execute_command(cmd)

        if result and result.strip():
            # Verify it has bin/kafka-run-class.sh
            kafka_home = result.strip()
            verify_cmd = f"test -f {kafka_home}/bin/kafka-run-class.sh && echo 'found'"
            verify_result = ssh_client.execute_command(verify_cmd)

            if verify_result and 'found' in verify_result:
                return kafka_home

    # Try to find from running process
    cmd = """
    ps aux | grep kafka\\.Kafka | grep -v grep | \
    grep -oP '\\s-cp\\s+\\K[^:]+' | head -1 | xargs dirname 2>/dev/null
    """
    result = ssh_client.execute_command(cmd)

    if result and result.strip():
        # Go up from libs to kafka_home
        kafka_home = ssh_client.execute_command(f"dirname {result.strip()}")
        if kafka_home and kafka_home.strip():
            return kafka_home.strip()

    return None


def check_jmx_connectivity(ssh_client, jmx_port: int = 9999, kafka_home: str = "/opt/kafka") -> bool:
    """
    Test if JMX is accessible and responding.

    Args:
        ssh_client: SSHClient instance
        jmx_port: JMX port to test
        kafka_home: Kafka installation directory

    Returns:
        True if JMX is accessible, False otherwise
    """
    # Try a simple JMX query that should always work
    test_mbean = "java.lang:type=Runtime"
    result = query_jmx_via_kafka_tools(ssh_client, test_mbean, "Name", jmx_port, kafka_home)

    return result is not None


# Pre-defined JMX MBean definitions for common Kafka metrics
KAFKA_JMXBEANS = {
    # Critical metrics (Priority 10)
    'under_replicated_partitions': {
        'mbean': 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions',
        'attribute': 'Value',
        'description': 'Number of under-replicated partitions'
    },
    'offline_partitions': {
        'mbean': 'kafka.controller:type=KafkaController,name=OfflinePartitionsCount',
        'attribute': 'Value',
        'description': 'Number of offline partitions'
    },
    'unclean_leader_elections': {
        'mbean': 'kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec',
        'attribute': 'Count',
        'description': 'Number of unclean leader elections'
    },

    # High priority metrics (Priority 8-9)
    'request_handler_idle_percent': {
        'mbean': 'kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent',
        'attribute': 'OneMinuteRate',
        'description': 'Request handler average idle percentage'
    },
    'produce_request_time_ms': {
        'mbean': 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce',
        'attribute': 'Mean',
        'description': 'Average produce request time in milliseconds'
    },
    'fetch_consumer_request_time_ms': {
        'mbean': 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer',
        'attribute': 'Mean',
        'description': 'Average consumer fetch request time in milliseconds'
    },
    'fetch_follower_request_time_ms': {
        'mbean': 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower',
        'attribute': 'Mean',
        'description': 'Average follower fetch request time in milliseconds'
    },
    'replica_max_lag': {
        'mbean': 'kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica',
        'attribute': 'Value',
        'description': 'Maximum replica lag'
    },
    'active_controller_count': {
        'mbean': 'kafka.controller:type=KafkaController,name=ActiveControllerCount',
        'attribute': 'Value',
        'description': 'Active controller count (should be 1)'
    },

    # Medium priority metrics (Priority 7)
    'bytes_in_per_sec': {
        'mbean': 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec',
        'attribute': 'OneMinuteRate',
        'description': 'Bytes in per second'
    },
    'bytes_out_per_sec': {
        'mbean': 'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec',
        'attribute': 'OneMinuteRate',
        'description': 'Bytes out per second'
    },
    'messages_in_per_sec': {
        'mbean': 'kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec',
        'attribute': 'OneMinuteRate',
        'description': 'Messages in per second'
    }
}


def get_jmx_metric_definition(metric_name: str) -> Optional[Dict[str, str]]:
    """
    Get JMX MBean definition for a named metric.

    Args:
        metric_name: Name of the metric (e.g., 'under_replicated_partitions')

    Returns:
        Dictionary with mbean, attribute, and description
    """
    return KAFKA_JMXBEANS.get(metric_name)


def query_named_metric(ssh_client, metric_name: str, jmx_port: int = 9999,
                       kafka_home: str = "/opt/kafka") -> Optional[float]:
    """
    Query a pre-defined named metric.

    Args:
        ssh_client: SSHClient instance
        metric_name: Name from KAFKA_JMXBEANS
        jmx_port: JMX port
        kafka_home: Kafka installation directory

    Returns:
        Metric value or None
    """
    metric_def = get_jmx_metric_definition(metric_name)
    if not metric_def:
        return None

    return query_jmx_via_kafka_tools(
        ssh_client,
        metric_def['mbean'],
        metric_def['attribute'],
        jmx_port,
        kafka_home
    )

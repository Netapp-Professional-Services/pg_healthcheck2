"""
Generic Metric Collection Framework

Provides unified interface for collecting metrics from distributed data systems
(Kafka, Cassandra, OpenSearch, ClickHouse, etc.) using multiple strategies:

1. Instaclustr Prometheus API (external, HTTP-based, no SSH required)
2. Local Prometheus Exporter (SSH + HTTP, typically port 7500)
3. Standard JMX (SSH + JMX, typically port 9999)

This framework is technology-agnostic - metric definitions are provided by
each technology plugin (e.g., kafka_metric_definitions.py).

Usage:
    from plugins.common.metric_collection_strategies import collect_metric_adaptive
    from plugins.kafka.utils.kafka_metric_definitions import KAFKA_METRICS

    metric_def = KAFKA_METRICS['under_replicated_partitions']
    data = collect_metric_adaptive(metric_def, connector, settings)
"""

import re
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


# ============================================================================
# Strategy 1: Instaclustr Prometheus API
# ============================================================================

def collect_from_instaclustr_prometheus(
    metric_def: Dict[str, Any],
    connector,
    settings: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Collect metrics from Instaclustr Prometheus API (external HTTP).

    Args:
        metric_def: Metric definition with 'instaclustr_prometheus' key
        connector: Technology connector instance
        settings: Configuration settings

    Returns:
        Dictionary with metric data:
        {
            'method': 'instaclustr_prometheus',
            'broker_metrics': {node_id: value, ...},
            'cluster_total': sum,
            'cluster_avg': average,
            'node_count': count
        }
        Or None if not available
    """
    if not settings.get('instaclustr_prometheus_enabled'):
        return None

    # Get Instaclustr metric name from definition
    instaclustr_metric = metric_def.get('instaclustr_prometheus')
    if not instaclustr_metric:
        return None

    try:
        from plugins.common.prometheus_client import get_instaclustr_client

        client = get_instaclustr_client(settings)
        if not client:
            return None

        # Fetch all metrics
        metrics = client.fetch_all_metrics()
        node_metrics = {}

        for metric in metrics:
            if metric['name'] == instaclustr_metric:
                node_id = metric['labels'].get('instance', 'unknown')
                node_metrics[node_id] = float(metric['value'])

        if not node_metrics:
            return None

        return {
            'method': 'instaclustr_prometheus',
            'node_metrics': node_metrics,
            'cluster_total': sum(node_metrics.values()),
            'cluster_avg': sum(node_metrics.values()) / len(node_metrics),
            'node_count': len(node_metrics),
            'metadata': {
                'source': 'instaclustr_api',
                'metric_name': instaclustr_metric
            }
        }

    except Exception as e:
        logger.debug(f"Instaclustr Prometheus collection failed: {e}")
        return None


# ============================================================================
# Strategy 2: Local Prometheus Exporter (via SSH)
# ============================================================================

def collect_from_local_prometheus_exporter(
    metric_def: Dict[str, Any],
    ssh_client,
    port: int = 7500
) -> Optional[float]:
    """
    Collect a single metric from local Prometheus exporter via SSH.

    Works with JMX exporters, node exporters, or custom Prometheus exporters
    running on localhost.

    Args:
        metric_def: Metric definition with 'local_prometheus' key
        ssh_client: SSH client instance
        port: Prometheus exporter port (default: 7500)

    Returns:
        Metric value as float, or None if not available
    """
    local_prom_config = metric_def.get('local_prometheus')
    if not local_prom_config:
        return None

    # Extract metric name from config
    if isinstance(local_prom_config, dict):
        local_metric = local_prom_config.get('metric')
    else:
        local_metric = local_prom_config

    if not local_metric:
        return None

    try:
        # Query Prometheus metrics endpoint via curl
        # Use grep to find exact metric (with optional labels)
        cmd = f'curl -s http://localhost:{port}/metrics 2>/dev/null | grep -E "^{local_metric}(\\{{|\\s)"'
        stdout, stderr, exit_code = ssh_client.execute_command(cmd)

        if not stdout or not stdout.strip() or exit_code != 0:
            return None

        # Parse metric line formats:
        # 1. Simple: "metric_name value"
        # 2. With labels: "metric_name{label1="value1"} value"
        # 3. With timestamp: "metric_name value timestamp"

        # Extract the numeric value (last or second-to-last token)
        tokens = stdout.strip().split()
        if len(tokens) >= 2:
            # Try second token (most common)
            try:
                return float(tokens[1])
            except ValueError:
                pass

            # Try last token
            try:
                return float(tokens[-1])
            except ValueError:
                pass

        # Fallback: regex extraction
        match = re.search(r'[\s}]([\d.eE+-]+)(?:\s|$)', stdout)
        if match:
            return float(match.group(1))

        return None

    except Exception as e:
        logger.debug(f"Local Prometheus collection failed: {e}")
        return None


def detect_prometheus_exporter_port(ssh_client, app_name: str = "jmx_prometheus") -> Optional[int]:
    """
    Auto-detect Prometheus exporter port from running process.

    Args:
        ssh_client: SSH client instance
        app_name: Application name hint (e.g., 'jmx_prometheus', 'kafka', 'cassandra')

    Returns:
        Port number or None if not found
    """
    try:
        # Look for javaagent with port configuration
        # Pattern: -javaagent:/path/to/jmx_prometheus_javaagent-VERSION.jar=PORT:/config.yml
        cmd = f"ps aux | grep {app_name} | grep -v grep | grep -oP 'javaagent:[^=]+=\\K[0-9]+' | head -1"
        stdout, stderr, exit_code = ssh_client.execute_command(cmd)

        if stdout and stdout.strip().isdigit():
            return int(stdout.strip())

        # Try alternative patterns
        cmd = f"ps aux | grep prometheus | grep -v grep | grep -oP ':\\K[0-9]+(?=:)' | head -1"
        stdout, stderr, exit_code = ssh_client.execute_command(cmd)

        if stdout and stdout.strip().isdigit():
            return int(stdout.strip())

        # Default port for JMX exporter
        return 7500

    except Exception:
        return None


# ============================================================================
# Strategy 3: Standard JMX (via SSH)
# ============================================================================

def collect_from_jmx(
    metric_def: Dict[str, Any],
    ssh_client,
    jmx_port: int = 9999,
    app_home: str = None
) -> Optional[float]:
    """
    Collect metric from standard JMX via SSH using application's JMX tools.

    Args:
        metric_def: Metric definition with 'jmx' key containing 'mbean' and 'attribute'
        ssh_client: SSH client instance
        jmx_port: JMX port (default: 9999)
        app_home: Application installation directory (e.g., /opt/kafka)

    Returns:
        Metric value as float, or None if not available
    """
    jmx_config = metric_def.get('jmx')
    if not jmx_config:
        return None

    mbean = jmx_config.get('mbean')
    attribute = jmx_config.get('attribute', 'Value')

    if not mbean:
        return None

    try:
        # Detect application type and use appropriate JMX tool
        # For now, use Kafka's JMX tool (most common pattern)
        from plugins.kafka.utils.jmx_helper import query_jmx_via_kafka_tools

        if not app_home:
            # Try to auto-detect
            app_home = detect_app_home(ssh_client)

        if not app_home:
            return None

        result = query_jmx_via_kafka_tools(ssh_client, mbean, attribute, jmx_port, app_home)

        if result is not None:
            try:
                return float(result)
            except (ValueError, TypeError):
                return None

        return None

    except Exception as e:
        logger.debug(f"JMX collection failed: {e}")
        return None


def detect_app_home(ssh_client) -> Optional[str]:
    """
    Auto-detect application installation directory.

    Tries common paths for Kafka, Cassandra, etc.

    Args:
        ssh_client: SSH client instance

    Returns:
        Application home directory or None
    """
    common_paths = [
        "/opt/kafka", "/usr/local/kafka", "/opt/kafka_*",
        "/opt/cassandra", "/usr/local/cassandra", "/opt/cassandra_*",
        "/opt/opensearch", "/usr/local/opensearch",
        "/opt/clickhouse", "/usr/local/clickhouse"
    ]

    for path in common_paths:
        cmd = f"ls -d {path} 2>/dev/null | head -1"
        stdout, stderr, exit_code = ssh_client.execute_command(cmd)

        if stdout and stdout.strip():
            return stdout.strip()

    return None


def detect_jmx_port(ssh_client) -> int:
    """
    Auto-detect JMX port from running process.

    Args:
        ssh_client: SSH client instance

    Returns:
        JMX port number (defaults to 9999)
    """
    try:
        # Look for JMX remote port in process args
        cmd = "ps aux | grep java | grep -v grep | grep -oP 'com\\.sun\\.management\\.jmxremote\\.port=\\K[0-9]+' | head -1"
        stdout, stderr, exit_code = ssh_client.execute_command(cmd)

        if stdout and stdout.strip().isdigit():
            return int(stdout.strip())

        # Default
        return 9999

    except Exception:
        return 9999


# ============================================================================
# Strategy Helper Functions
# ============================================================================

def _fetch_all_prometheus_metrics(connector, settings: Dict[str, Any]) -> Dict[str, str]:
    """
    Fetch ALL Prometheus metrics from all hosts ONCE and cache them.

    This dramatically improves performance by avoiding repeated SSH + curl calls.
    Instead of fetching metrics 15-20 times per health check run, we fetch once
    and parse locally.

    Returns:
        Dict mapping host -> full metrics output
    """
    # Check if connector has cache attribute
    if not hasattr(connector, '_prometheus_metrics_cache'):
        connector._prometheus_metrics_cache = {}

    # If cache is already populated, return it
    if connector._prometheus_metrics_cache:
        logger.debug(f"Using cached Prometheus metrics for {len(connector._prometheus_metrics_cache)} hosts")
        return connector._prometheus_metrics_cache

    # Fetch metrics from all hosts
    ssh_hosts = connector.get_ssh_hosts() if hasattr(connector, 'get_ssh_hosts') else []
    if not ssh_hosts:
        return {}

    logger.info(f"Fetching Prometheus metrics from {len(ssh_hosts)} hosts (will cache for all checks)...")

    for host in ssh_hosts:
        try:
            ssh_client = connector.get_ssh_manager(host)
            if not ssh_client:
                continue

            app_name = getattr(connector, 'technology_name', 'prometheus')
            prom_port = detect_prometheus_exporter_port(ssh_client, app_name)
            if not prom_port:
                logger.debug(f"No Prometheus port found for {host}")
                continue

            # Fetch ALL metrics once
            cmd = f'curl -s http://localhost:{prom_port}/metrics 2>/dev/null'
            stdout, stderr, exit_code = ssh_client.execute_command(cmd)

            if stdout and exit_code == 0:
                connector._prometheus_metrics_cache[host] = stdout
                logger.debug(f"Cached {len(stdout)} bytes of metrics from {host}")
            else:
                logger.debug(f"Failed to fetch metrics from {host}: exit_code={exit_code}")

        except Exception as e:
            logger.debug(f"Failed to fetch Prometheus metrics from {host}: {e}")
            continue

    logger.info(f"Cached Prometheus metrics from {len(connector._prometheus_metrics_cache)}/{len(ssh_hosts)} hosts")
    return connector._prometheus_metrics_cache


def _parse_metric_from_cache(metric_name: str, cached_metrics: str) -> Optional[float]:
    """
    Parse a specific metric from cached Prometheus metrics output.

    Args:
        metric_name: Name of the metric to extract (supports regex patterns)
        cached_metrics: Full metrics output from Prometheus endpoint

    Returns:
        Metric value as float, or None if not found
    """
    if not cached_metrics:
        return None

    try:
        # Look for the metric line (handle both simple and labeled metrics)
        # Pattern: metric_name value OR metric_name{labels} value
        pattern = f'^{metric_name}(\\{{|\\s)'

        for line in cached_metrics.split('\n'):
            if line.startswith('#'):
                continue

            if re.match(pattern, line):
                # Extract numeric value (last or second-to-last token)
                tokens = line.strip().split()
                if len(tokens) >= 2:
                    # Try second token (most common)
                    try:
                        return float(tokens[1])
                    except ValueError:
                        pass

                    # Try last token
                    try:
                        return float(tokens[-1])
                    except ValueError:
                        pass

                # Fallback: regex extraction
                match = re.search(r'[\s}]([\d.eE+-]+)(?:\s|$)', line)
                if match:
                    return float(match.group(1))

        return None

    except Exception as e:
        logger.debug(f"Failed to parse metric {metric_name}: {e}")
        return None


def _try_local_prometheus(metric_def: Dict[str, Any], connector, settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Try to collect metric using Local Prometheus via SSH (with caching)."""
    if not metric_def.get('local_prometheus'):
        return None

    # Fetch and cache all metrics from all hosts (only happens once)
    metrics_cache = _fetch_all_prometheus_metrics(connector, settings)
    if not metrics_cache:
        return None

    # Extract metric name from definition
    local_prom_config = metric_def.get('local_prometheus')
    if isinstance(local_prom_config, dict):
        local_metric = local_prom_config.get('metric')
    else:
        local_metric = local_prom_config

    if not local_metric:
        return None

    # Parse the specific metric from each host's cached data
    node_metrics = {}
    for host, cached_metrics in metrics_cache.items():
        value = _parse_metric_from_cache(local_metric, cached_metrics)
        if value is not None:
            node_metrics[host] = value

    if node_metrics:
        return {
            'method': 'local_prometheus',
            'node_metrics': node_metrics,
            'cluster_total': sum(node_metrics.values()),
            'cluster_avg': sum(node_metrics.values()) / len(node_metrics) if node_metrics else 0,
            'node_count': len(node_metrics),
            'metadata': {'cache_size': len(metrics_cache)}
        }
    return None


def _try_jmx(metric_def: Dict[str, Any], connector, settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Try to collect metric using JMX via SSH."""
    if not metric_def.get('jmx'):
        return None

    ssh_hosts = connector.get_ssh_hosts() if hasattr(connector, 'get_ssh_hosts') else []
    if not ssh_hosts:
        return None

    first_ssh_client = connector.get_ssh_manager(ssh_hosts[0])
    if not first_ssh_client:
        return None

    app_home = detect_app_home(first_ssh_client)
    jmx_port = detect_jmx_port(first_ssh_client)
    if not app_home:
        return None

    node_metrics = {}
    for host in ssh_hosts:
        try:
            ssh_client = connector.get_ssh_manager(host)
            if not ssh_client:
                continue

            value = collect_from_jmx(metric_def, ssh_client, jmx_port, app_home)
            if value is not None:
                node_metrics[host] = value
        except Exception as e:
            logger.debug(f"JMX collection failed for {host}: {e}")
            continue

    if node_metrics:
        return {
            'method': 'jmx',
            'node_metrics': node_metrics,
            'cluster_total': sum(node_metrics.values()),
            'cluster_avg': sum(node_metrics.values()) / len(node_metrics) if node_metrics else 0,
            'node_count': len(node_metrics),
            'metadata': {'jmx_port': jmx_port, 'app_home': app_home}
        }
    return None


# ============================================================================
# Unified Adaptive Collection
# ============================================================================

def collect_metric_adaptive(
    metric_def: Dict[str, Any],
    connector,
    settings: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Collect metric using connector's determined strategy, with fallback to alternatives.

    The connector determines the optimal collection strategy at connection time
    via _determine_metric_collection_strategy(). This function prioritizes that
    strategy, but will try alternatives if the metric doesn't support the primary
    strategy (e.g., metric has 'local_prometheus': None).

    Strategy Priority:
    1. Connector's determined strategy (from connection time)
    2. Fallback strategies (only if metric supports them)

    Args:
        metric_def: Metric definition dictionary with keys:
            - 'instaclustr_prometheus': Instaclustr metric name (optional)
            - 'local_prometheus': Local Prometheus metric name (optional)
            - 'jmx': Dict with 'mbean' and 'attribute' (optional)
        connector: Technology connector instance (must support SSH via SSHSupportMixin)
        settings: Configuration settings

    Returns:
        Dictionary with metric data:
        {
            'method': 'instaclustr_prometheus' | 'local_prometheus' | 'jmx',
            'node_metrics': {node_id/host: value, ...},
            'cluster_total': sum,
            'cluster_avg': average,
            'node_count': count,
            'metadata': {...}
        }
        Or None if collection failed via all available strategies

    Example:
        >>> from plugins.common.metric_collection_strategies import collect_metric_adaptive
        >>> from plugins.kafka.utils.kafka_metric_definitions import KAFKA_METRICS
        >>>
        >>> metric_def = KAFKA_METRICS['under_replicated_partitions']
        >>> data = collect_metric_adaptive(metric_def, connector, settings)
        >>> if data:
        >>>     print(f"Collected via {data['method']}: {data['cluster_total']}")
    """
    # Get the connector's determined strategy
    primary_strategy = getattr(connector, 'metric_collection_strategy', None)

    if not primary_strategy:
        logger.warning("Connector has no metric_collection_strategy set. Cannot collect metric.")
        return None

    # ONLY use the connector's determined strategy - no fallbacks
    # Fallbacks cause timeouts on unavailable methods (e.g., JMX without remote port)
    # If a metric doesn't support the primary strategy, it simply won't be collected
    strategy = primary_strategy

    if strategy not in ['instaclustr_prometheus', 'local_prometheus', 'jmx']:
        logger.warning(f"Unknown strategy: {strategy}")
        return None

    # Check if metric supports this strategy
    if strategy == 'instaclustr_prometheus':
        if not metric_def.get('instaclustr_prometheus'):
            logger.debug(f"Metric does not support {strategy} collection (not defined)")
            return None

        result = collect_from_instaclustr_prometheus(metric_def, connector, settings)
        if result:
            logger.debug(f"Collected metric via strategy '{strategy}'")
            return result

    elif strategy == 'local_prometheus':
        if not metric_def.get('local_prometheus'):
            logger.debug(f"Metric does not support {strategy} collection (not defined)")
            return None

        from plugins.common.check_helpers import require_ssh
        available, _, _ = require_ssh(connector, "metric collection")
        if not available:
            logger.debug(f"SSH not available for {strategy} collection")
            return None

        result = _try_local_prometheus(metric_def, connector, settings)
        if result:
            logger.debug(f"Collected metric via strategy '{strategy}'")
            return result

    elif strategy == 'jmx':
        if not metric_def.get('jmx'):
            logger.debug(f"Metric does not support {strategy} collection (not defined)")
            return None

        from plugins.common.check_helpers import require_ssh
        available, _, _ = require_ssh(connector, "metric collection")
        if not available:
            logger.debug(f"SSH not available for {strategy} collection")
            return None

        result = _try_jmx(metric_def, connector, settings)
        if result:
            logger.debug(f"Collected metric via strategy '{strategy}'")
            return result

    # If we reach here, collection failed
    logger.debug(f"Failed to collect metric via strategy '{strategy}'")
    return None


# ============================================================================
# Utility Functions
# ============================================================================

def get_collection_method_description(method: str) -> str:
    """
    Get human-readable description of collection method.

    Args:
        method: Method name ('instaclustr_prometheus', 'local_prometheus', 'jmx')

    Returns:
        Human-readable description
    """
    descriptions = {
        'instaclustr_prometheus': 'Instaclustr Prometheus API (external HTTP)',
        'local_prometheus': 'Local Prometheus Exporter via SSH (typically port 7500)',
        'jmx': 'Standard JMX via SSH (typically port 9999)'
    }
    return descriptions.get(method, f'Unknown method: {method}')


def get_supported_strategies(metric_def: Dict[str, Any]) -> list:
    """
    Get list of collection strategies supported by a metric definition.

    Args:
        metric_def: Metric definition dictionary

    Returns:
        List of strategy names that are configured
    """
    strategies = []
    if metric_def.get('instaclustr_prometheus'):
        strategies.append('instaclustr_prometheus')
    if metric_def.get('local_prometheus'):
        strategies.append('local_prometheus')
    if metric_def.get('jmx'):
        strategies.append('jmx')
    return strategies

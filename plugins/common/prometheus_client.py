"""
Prometheus Client for Managed Clusters

Reusable client for scraping Prometheus metrics from managed service providers
(Instaclustr, Aiven, Confluent, etc.)

Supports:
- Service discovery (Instaclustr-style)
- Prometheus text format parsing
- Per-node metric scraping
- Cross-node aggregation
- Structured data transformation

This client is designed to work with Instaclustr's Prometheus-compatible endpoints
but can be adapted for other providers.
"""

import logging
import requests
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

logger = logging.getLogger(__name__)


class PrometheusMetricsParser:
    """
    Parser for Prometheus text exposition format.

    Parses metrics like:
        # HELP ic_node_heapmemoryused_bytes Data type: value, Unit: B
        # TYPE ic_node_heapmemoryused_bytes gauge
        ic_node_heapmemoryused_bytes{nodeID="xxx",type="value",} 7.2472088E7
    """

    @staticmethod
    def parse_metrics(text: str) -> List[Dict]:
        """
        Parse Prometheus text format into structured data.

        Args:
            text: Raw Prometheus metrics text

        Returns:
            List of metric dictionaries with name, labels, value
        """
        metrics = []

        # Regex to match metric lines: metric_name{labels} value
        metric_pattern = re.compile(
            r'^([a-zA-Z_:][a-zA-Z0-9_:]*)'  # metric name
            r'(?:\{([^}]+)\})?'               # optional labels
            r'\s+'                             # whitespace
            r'([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)'  # value (with scientific notation)
        )

        for line in text.split('\n'):
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue

            match = metric_pattern.match(line)
            if match:
                metric_name = match.group(1)
                labels_str = match.group(2)
                value_str = match.group(3)

                # Parse labels
                labels = {}
                if labels_str:
                    # Parse label pairs: key="value",key="value"
                    label_pattern = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')
                    for label_match in label_pattern.finditer(labels_str):
                        labels[label_match.group(1)] = label_match.group(2)

                # Parse value (handle scientific notation)
                try:
                    value = float(value_str)
                except ValueError:
                    logger.warning(f"Could not parse value: {value_str}")
                    continue

                metrics.append({
                    'name': metric_name,
                    'labels': labels,
                    'value': value
                })

        return metrics


class PrometheusScraperClient:
    """
    Client for scraping Prometheus endpoints.

    Designed for managed service providers that expose raw Prometheus metrics
    endpoints rather than PromQL query APIs.
    """

    def __init__(self, base_url: str, username: str, api_key: str, timeout: int = 30):
        """
        Initialize Prometheus scraper client

        Args:
            base_url: Base URL for Prometheus endpoints
            username: API username
            api_key: API key or password
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (username, api_key)
        self.parser = PrometheusMetricsParser()

    def scrape_endpoint(self, url: str) -> List[Dict]:
        """
        Scrape metrics from an endpoint and parse them.

        Args:
            url: Full URL to scrape

        Returns:
            List of parsed metrics
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Parse Prometheus text format
            metrics = self.parser.parse_metrics(response.text)
            return metrics

        except requests.RequestException as e:
            logger.error(f"Failed to scrape {url}: {e}")
            raise

    def filter_metrics(self, metrics: List[Dict], name_pattern: str = None,
                      labels: Dict = None) -> List[Dict]:
        """
        Filter metrics by name pattern and/or labels.

        Args:
            metrics: List of parsed metrics
            name_pattern: Regex pattern to match metric names
            labels: Dict of label key/values that must match

        Returns:
            Filtered list of metrics
        """
        filtered = metrics

        # Filter by name pattern
        if name_pattern:
            pattern = re.compile(name_pattern)
            filtered = [m for m in filtered if pattern.match(m['name'])]

        # Filter by labels
        if labels:
            filtered = [
                m for m in filtered
                if all(m['labels'].get(k) == v for k, v in labels.items())
            ]

        return filtered

    def get_metric_value(self, metrics: List[Dict], metric_name: str,
                        labels: Dict = None) -> Optional[float]:
        """
        Get single metric value by name and labels.

        Args:
            metrics: List of parsed metrics
            metric_name: Exact metric name
            labels: Optional labels to match

        Returns:
            Metric value or None
        """
        filtered = self.filter_metrics(metrics, name_pattern=f'^{re.escape(metric_name)}$', labels=labels)

        if filtered:
            return filtered[0]['value']
        return None

    def aggregate_metrics(self, metrics: List[Dict], metric_name: str,
                         aggregation: str = 'sum') -> Optional[float]:
        """
        Aggregate metric values across multiple instances.

        Args:
            metrics: List of parsed metrics
            metric_name: Metric name to aggregate
            aggregation: Aggregation function ('sum', 'avg', 'min', 'max', 'count')

        Returns:
            Aggregated value
        """
        filtered = self.filter_metrics(metrics, name_pattern=f'^{re.escape(metric_name)}$')

        if not filtered:
            return None

        values = [m['value'] for m in filtered]

        if aggregation == 'sum':
            return sum(values)
        elif aggregation == 'avg':
            return sum(values) / len(values)
        elif aggregation == 'min':
            return min(values)
        elif aggregation == 'max':
            return max(values)
        elif aggregation == 'count':
            return len(values)
        else:
            raise ValueError(f"Unknown aggregation: {aggregation}")


class InstaclustrPrometheusClient(PrometheusScraperClient):
    """
    Specialized Prometheus client for Instaclustr managed clusters.

    Handles service discovery and per-node metrics scraping.
    """

    def __init__(self, cluster_id: str, username: str, api_key: str,
                 prometheus_base_url: str):
        """
        Initialize Instaclustr Prometheus client

        Args:
            cluster_id: Instaclustr cluster UUID
            username: Instaclustr API username
            api_key: Prometheus API key
            prometheus_base_url: Prometheus monitoring base URL
        """
        super().__init__(prometheus_base_url, username, api_key)
        self.cluster_id = cluster_id
        self._targets_cache = None
        self._metrics_cache = None  # Cache scraped metrics to avoid re-scraping

    def discover_targets(self) -> List[Dict]:
        """
        Discover cluster nodes via service discovery endpoint.

        Returns:
            List of target dictionaries with 'targets' and 'labels'
        """
        url = f"{self.base_url}/discovery/v1/{self.cluster_id}"

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            targets = response.json()

            self._targets_cache = targets
            return targets

        except requests.RequestException as e:
            logger.error(f"Service discovery failed: {e}")
            raise

    def scrape_all_nodes(self) -> List[Dict]:
        """
        Scrape metrics from all discovered nodes.

        Returns cached metrics if available to avoid rate limiting.

        Returns:
            Combined list of metrics from all nodes
        """
        # Return cached metrics if available
        if self._metrics_cache is not None:
            logger.debug("Returning cached metrics")
            return self._metrics_cache

        # Discover targets if not cached
        if not self._targets_cache:
            self.discover_targets()

        all_metrics = []

        for target in self._targets_cache:
            target_hosts = target.get('targets', [])
            target_labels = target.get('labels', {})

            for host in target_hosts:
                url = f"https://{host}/metrics/v2/query"

                try:
                    metrics = self.scrape_endpoint(url)

                    # Add target labels to each metric
                    for metric in metrics:
                        metric['target_labels'] = target_labels

                    all_metrics.extend(metrics)

                except Exception as e:
                    logger.warning(f"Failed to scrape {host}: {e}")
                    continue

        # Cache the scraped metrics for subsequent calls
        self._metrics_cache = all_metrics
        logger.debug(f"Cached {len(all_metrics)} metrics from all nodes")

        return all_metrics

    def to_structured_format(self, metrics: List[Dict], metric_name: str,
                            friendly_name: str, unit: str = '') -> Dict:
        """
        Transform scraped metrics to health check structured format.

        Args:
            metrics: List of parsed metrics
            metric_name: Metric name to extract
            friendly_name: Friendly name for output
            unit: Unit of measurement

        Returns:
            Structured data format compatible with health checks
        """
        timestamp = datetime.utcnow().isoformat() + 'Z'

        filtered = self.filter_metrics(metrics, name_pattern=f'^{re.escape(metric_name)}$')

        if not filtered:
            return {
                'status': 'error',
                'error_message': f'No metrics found for {metric_name}',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'metric_name': metric_name,
                    'query_timestamp': timestamp
                }
            }

        structured_data = []

        for metric in filtered:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})

            data_point = {
                'metric': friendly_name,
                'value': metric['value'],
                'timestamp': timestamp
            }

            # Add unit if provided
            if unit:
                data_point['unit'] = unit

            # Add node ID from labels
            if 'nodeID' in labels:
                data_point['node_id'] = labels['nodeID']

            # Add target labels (datacenter, rack, IPs, etc.)
            if 'Rack' in target_labels:
                data_point['rack'] = target_labels['Rack']
            if 'ClusterDataCenterName' in target_labels:
                data_point['datacenter'] = target_labels['ClusterDataCenterName']
            if 'PublicIp' in target_labels:
                data_point['public_ip'] = target_labels['PublicIp']
            if 'PrivateIp' in target_labels:
                data_point['private_ip'] = target_labels['PrivateIp']

            structured_data.append(data_point)

        return {
            'status': 'success',
            'data': structured_data,
            'metadata': {
                'source': 'prometheus',
                'metric_name': metric_name,
                'query_timestamp': timestamp,
                'result_count': len(structured_data)
            }
        }

    def test_connection(self) -> bool:
        """
        Test connection to Prometheus endpoint.

        Returns:
            True if connection successful
        """
        try:
            # Test service discovery
            targets = self.discover_targets()

            if not targets:
                logger.error("No targets discovered")
                return False

            # Try to scrape first target
            first_target = targets[0]
            target_hosts = first_target.get('targets', [])

            if not target_hosts:
                logger.error("No hosts in target")
                return False

            url = f"https://{target_hosts[0]}/metrics/v2/query"
            metrics = self.scrape_endpoint(url)

            return len(metrics) > 0

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    # Convenience methods for common Cassandra metrics

    def get_cassandra_jvm_heap(self) -> Dict:
        """Get Cassandra JVM heap usage metrics."""
        metrics = self.scrape_all_nodes()

        # Get heap used and max
        heap_used = self.to_structured_format(
            metrics,
            'ic_node_heapmemoryused_bytes',
            'heap_used',
            'bytes'
        )

        heap_max = self.to_structured_format(
            metrics,
            'ic_node_heapmemorymax_bytes',
            'heap_max',
            'bytes'
        )

        # Combine into single result
        combined_data = []

        # Merge heap used and max by node
        heap_used_by_node = {d['node_id']: d for d in heap_used.get('data', [])}
        heap_max_by_node = {d['node_id']: d for d in heap_max.get('data', [])}

        for node_id in heap_used_by_node.keys():
            if node_id in heap_max_by_node:
                used = heap_used_by_node[node_id]['value']
                max_heap = heap_max_by_node[node_id]['value']

                combined_data.append({
                    **heap_used_by_node[node_id],
                    'heap_used': used,
                    'heap_max': max_heap,
                    'heap_used_percent': (used / max_heap * 100) if max_heap > 0 else 0,
                    'metric': 'jvm_heap'
                })

        return {
            'status': 'success',
            'data': combined_data,
            'metadata': {
                'source': 'prometheus',
                'query_timestamp': datetime.utcnow().isoformat() + 'Z',
                'result_count': len(combined_data)
            }
        }

    def get_cassandra_disk_usage(self) -> Dict:
        """Get Cassandra disk usage metrics."""
        metrics = self.scrape_all_nodes()
        return self.to_structured_format(
            metrics,
            'ic_node_disk_utilization',
            'disk_utilization',
            'percent'
        )

    def get_cassandra_compaction_pending(self) -> Dict:
        """Get Cassandra pending compaction tasks."""
        metrics = self.scrape_all_nodes()
        return self.to_structured_format(
            metrics,
            'ic_node_compactions',
            'compaction_pending',
            'count'
        )

    def get_cassandra_read_latency(self) -> Dict:
        """Get Cassandra read latency metrics (95th percentile)."""
        metrics = self.scrape_all_nodes()
        return self.to_structured_format(
            metrics,
            'ic_node_client_request_read_v2_microseconds',
            'read_latency_p95',
            'microseconds'
        )

    def get_cassandra_write_latency(self) -> Dict:
        """Get Cassandra write latency metrics (95th percentile)."""
        metrics = self.scrape_all_nodes()
        return self.to_structured_format(
            metrics,
            'ic_node_client_request_write_microseconds',
            'write_latency_p95',
            'microseconds'
        )

    def get_cassandra_cpu_utilization(self) -> Dict:
        """Get Cassandra CPU utilization."""
        metrics = self.scrape_all_nodes()
        return self.to_structured_format(
            metrics,
            'ic_node_cpu_utilization',
            'cpu_utilization',
            'percent'
        )


# Convenience function for quick testing
def test_instaclustr_prometheus_connection(cluster_id: str, username: str,
                                          api_key: str, base_url: str) -> bool:
    """
    Test Instaclustr Prometheus connection

    Args:
        cluster_id: Instaclustr cluster ID
        username: API username
        api_key: API key
        base_url: Prometheus base URL

    Returns:
        True if connection successful
    """
    try:
        client = InstaclustrPrometheusClient(cluster_id, username, api_key, base_url)
        return client.test_connection()
    except Exception as e:
        logger.error(f"Prometheus connection test failed: {e}")
        return False


# ============================================================================
# Client Caching (to avoid rate limiting)
# ============================================================================

# Module-level client cache to avoid rate limiting
# Key is cluster_id, value is client instance
_client_cache: Dict[str, 'InstaclustrPrometheusClient'] = {}


def get_instaclustr_client(cluster_id: str, username: str, api_key: str,
                           prometheus_base_url: str) -> 'InstaclustrPrometheusClient':
    """
    Get or create a cached Instaclustr Prometheus client.

    This function implements client instance caching to avoid rate limiting.
    Multiple health checks can share the same client instance, which means
    they share the discovered targets cache and avoid redundant API calls.

    The cache is maintained for the lifetime of the Python process (e.g., one
    health check run). This is perfect for scenarios where multiple checks
    need to query the same Prometheus endpoint.

    Args:
        cluster_id: Instaclustr cluster UUID
        username: API username
        api_key: Prometheus API key
        prometheus_base_url: Prometheus base URL

    Returns:
        Cached or new InstaclustrPrometheusClient instance

    Example:
        # Multiple checks can use the same client
        client = get_instaclustr_client(cluster_id, username, api_key, base_url)
        heap_data = client.get_cassandra_jvm_heap()
        cpu_data = client.get_cassandra_cpu_usage()
        disk_data = client.get_cassandra_disk_usage()
        # Only one service discovery call is made, shared across all checks

    Note:
        - Cache persists for the Python process lifetime
        - Each unique cluster_id gets its own cached client
        - Targets are discovered once and reused
        - Prevents 429 rate limit errors from Instaclustr API
    """
    # Use cluster_id as cache key (sufficient for same-cluster checks)
    cache_key = cluster_id

    # Return cached client if available
    if cache_key in _client_cache:
        logger.debug(f"Reusing cached Prometheus client for cluster {cluster_id}")
        return _client_cache[cache_key]

    # Create new client and cache it
    logger.debug(f"Creating new Prometheus client for cluster {cluster_id}")
    client = InstaclustrPrometheusClient(
        cluster_id=cluster_id,
        username=username,
        api_key=api_key,
        prometheus_base_url=prometheus_base_url
    )

    _client_cache[cache_key] = client
    return client


def clear_client_cache():
    """
    Clear the module-level client cache.

    Useful for testing or when you want to force re-discovery of targets.
    This will cause the next call to get_instaclustr_client() to create
    a new client instance and perform fresh service discovery.
    """
    global _client_cache
    _client_cache = {}
    logger.debug("Cleared Prometheus client cache")

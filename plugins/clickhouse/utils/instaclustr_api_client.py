"""
Instaclustr Monitoring API Client for ClickHouse

Fetches operational metrics from Instaclustr Managed ClickHouse clusters
and transforms them into structured format for trend analysis.

API Documentation:
https://www.instaclustr.com/support/api-integrations/api-reference/monitoring-api/

Structured Data Requirements:
- All responses follow snapshot-compatible schema
- Status field always present (success/warning/error/skipped)
- Data always as array structure
- Timestamps on all data points (ISO 8601 format)
- Explicit units (MB, GB, ops/sec, ms, %)
- Consistent field names across snapshots
"""

import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class InstaclustrClickHouseAPIClient:
    """
    Client for Instaclustr ClickHouse Monitoring API

    Fetches metrics and transforms them into structured format
    compatible with health check snapshot requirements.
    """

    BASE_URL = "https://api.instaclustr.com/monitoring/v1"

    def __init__(self, cluster_id: str, username: str, api_key: str, timeout: int = 30):
        """
        Initialize Instaclustr API client for ClickHouse

        Args:
            cluster_id: Instaclustr cluster UUID
            username: Instaclustr username
            api_key: Instaclustr API key
            timeout: Request timeout in seconds
        """
        self.cluster_id = cluster_id
        self.username = username
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (username, api_key)

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make authenticated request to Instaclustr API

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dict

        Raises:
            requests.RequestException: On API errors
        """
        url = f"{self.BASE_URL}/clusters/{self.cluster_id}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Instaclustr API request failed: {e}")
            raise

    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO 8601 format"""
        return datetime.utcnow().isoformat() + 'Z'

    def _normalize_value(self, value: Any) -> Any:
        """
        Ensure consistent value types (int, float, not string)

        Trend analysis requires numeric types for calculations
        """
        if isinstance(value, str):
            try:
                return float(value) if '.' in value else int(value)
            except ValueError:
                return value
        return value

    def fetch_node_metrics(self) -> Dict:
        """
        Fetch node-level resource metrics (CPU, memory, load)

        Returns structured data:
        {
            "status": "success",
            "data": [
                {
                    "metric": "memory_used",
                    "value_mb": 15360,
                    "total_mb": 20480,
                    "percent_used": 75.0,
                    "threshold_warning": 75,
                    "threshold_critical": 85,
                    "timestamp": "2025-10-31T12:00:00Z"
                }
            ],
            "metadata": {
                "source": "instaclustr_api",
                "cluster_id": "...",
                "query_timestamp": "..."
            }
        }
        """
        try:
            # Fetch node metrics from API
            response = self._make_request("/nodes")

            timestamp = self._get_timestamp()

            # Initialize structured response
            structured_data = {
                'status': 'success',
                'data': [],
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': timestamp
                }
            }

            # Aggregate cluster-wide metrics
            total_memory_used = 0
            total_memory_total = 0
            total_cpu_usage = 0
            node_count = 0
            per_node_data = []

            for node in response.get('nodes', []):
                node_ip = node.get('privateIp', node.get('publicIp', 'unknown'))

                # Memory metrics
                memory_used_mb = self._normalize_value(node.get('memoryUsedMb', 0))
                memory_total_mb = self._normalize_value(node.get('memoryTotalMb', 0))
                memory_percent = round((memory_used_mb / memory_total_mb * 100), 1) if memory_total_mb > 0 else 0

                # CPU metrics
                cpu_usage = self._normalize_value(node.get('cpuUsagePercent', 0))

                # Load average
                load_avg = self._normalize_value(node.get('loadAverage1m', 0))

                total_memory_used += memory_used_mb
                total_memory_total += memory_total_mb
                total_cpu_usage += cpu_usage
                node_count += 1

                # Store per-node breakdown
                per_node_data.append({
                    'node': node_ip,
                    'memory_used_mb': memory_used_mb,
                    'memory_total_mb': memory_total_mb,
                    'memory_percent': memory_percent,
                    'cpu_percent': cpu_usage,
                    'load_average_1m': load_avg
                })

            # Cluster-wide aggregates
            avg_memory_percent = round((total_memory_used / total_memory_total * 100), 1) if total_memory_total > 0 else 0
            avg_cpu_percent = round((total_cpu_usage / node_count), 1) if node_count > 0 else 0

            # Memory usage metric
            structured_data['data'].append({
                'metric': 'memory_used',
                'value_mb': total_memory_used,
                'total_mb': total_memory_total,
                'percent_used': avg_memory_percent,
                'threshold_warning': 75,
                'threshold_critical': 85,
                'timestamp': timestamp,
                'aggregation': 'cluster_average',
                'node_count': node_count
            })

            # CPU usage metric
            structured_data['data'].append({
                'metric': 'cpu_usage',
                'percent': avg_cpu_percent,
                'threshold_warning': 80,
                'threshold_critical': 90,
                'timestamp': timestamp,
                'aggregation': 'cluster_average',
                'node_count': node_count
            })

            # Add per-node breakdown
            structured_data['per_node_data'] = per_node_data

            # Determine status based on thresholds
            if avg_memory_percent >= 85 or avg_cpu_percent >= 90:
                structured_data['status'] = 'error'
            elif avg_memory_percent >= 75 or avg_cpu_percent >= 80:
                structured_data['status'] = 'warning'

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch node metrics: {e}")
            return {
                'status': 'error',
                'data': [],
                'error_message': str(e),
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': self._get_timestamp()
                }
            }

    def fetch_disk_metrics(self) -> Dict:
        """
        Fetch disk utilization metrics

        Returns structured data with cluster aggregate and per-node breakdown:
        {
            "status": "success",
            "data": [
                {
                    "aggregation": "cluster_total",
                    "total_gb": 1000,
                    "used_gb": 650,
                    "free_gb": 350,
                    "percent_used": 65.0,
                    "timestamp": "..."
                }
            ],
            "per_node_breakdown": [
                {
                    "node": "54.211.151.192",
                    "total_gb": 333,
                    "used_gb": 217,
                    "free_gb": 116,
                    "percent_used": 65.2
                }
            ]
        }
        """
        try:
            response = self._make_request("/disk")

            timestamp = self._get_timestamp()

            structured_data = {
                'status': 'success',
                'data': [],
                'per_node_breakdown': [],
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': timestamp
                }
            }

            # Aggregate disk usage
            total_size_gb = 0
            total_used_gb = 0

            for node in response.get('nodes', []):
                node_ip = node.get('privateIp', node.get('publicIp', 'unknown'))

                size_gb = self._normalize_value(node.get('totalGb', 0))
                used_gb = self._normalize_value(node.get('usedGb', 0))
                free_gb = size_gb - used_gb
                percent_used = round((used_gb / size_gb * 100), 1) if size_gb > 0 else 0

                total_size_gb += size_gb
                total_used_gb += used_gb

                # Per-node data
                structured_data['per_node_breakdown'].append({
                    'node': node_ip,
                    'total_gb': size_gb,
                    'used_gb': used_gb,
                    'free_gb': free_gb,
                    'percent_used': percent_used
                })

            # Cluster aggregate
            cluster_percent = round((total_used_gb / total_size_gb * 100), 1) if total_size_gb > 0 else 0

            structured_data['data'].append({
                'aggregation': 'cluster_total',
                'total_gb': total_size_gb,
                'used_gb': total_used_gb,
                'free_gb': total_size_gb - total_used_gb,
                'percent_used': cluster_percent,
                'threshold_warning': 80,
                'threshold_critical': 90,
                'timestamp': timestamp
            })

            # Determine status
            if cluster_percent >= 90:
                structured_data['status'] = 'error'
            elif cluster_percent >= 80:
                structured_data['status'] = 'warning'

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch disk metrics: {e}")
            return {
                'status': 'error',
                'data': [],
                'error_message': str(e),
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': self._get_timestamp()
                }
            }

    def fetch_query_performance_metrics(self) -> Dict:
        """
        Fetch query performance metrics (throughput, latency)

        Returns structured data:
        {
            "status": "success",
            "data": [
                {
                    "metric": "queries_per_second",
                    "value": 1234.5,
                    "unit": "queries/sec",
                    "timestamp": "..."
                },
                {
                    "metric": "query_latency_p95",
                    "value_ms": 15.2,
                    "unit": "ms",
                    "timestamp": "..."
                }
            ]
        }
        """
        try:
            response = self._make_request("/performance")

            timestamp = self._get_timestamp()

            structured_data = {
                'status': 'success',
                'data': [],
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': timestamp
                }
            }

            # Query throughput
            queries_per_sec = self._normalize_value(response.get('queriesPerSecond', 0))
            structured_data['data'].append({
                'metric': 'queries_per_second',
                'value': queries_per_sec,
                'unit': 'queries/sec',
                'timestamp': timestamp
            })

            # Query latency percentiles
            latency = response.get('queryLatency', {})
            for percentile in ['p50', 'p95', 'p99']:
                value = self._normalize_value(latency.get(percentile, 0))
                structured_data['data'].append({
                    'metric': f'query_latency_{percentile}',
                    'value_ms': value,
                    'unit': 'ms',
                    'threshold_warning': 100 if percentile == 'p95' else 500,
                    'threshold_critical': 500 if percentile == 'p95' else 1000,
                    'timestamp': timestamp
                })

            # Insert throughput
            inserts_per_sec = self._normalize_value(response.get('insertsPerSecond', 0))
            structured_data['data'].append({
                'metric': 'inserts_per_second',
                'value': inserts_per_sec,
                'unit': 'inserts/sec',
                'timestamp': timestamp
            })

            # Check if latency thresholds exceeded
            p95_latency = latency.get('p95', 0)
            if p95_latency > 500:
                structured_data['status'] = 'error'
            elif p95_latency > 100:
                structured_data['status'] = 'warning'

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch query performance metrics: {e}")
            return {
                'status': 'error',
                'data': [],
                'error_message': str(e),
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': self._get_timestamp()
                }
            }

    def fetch_table_metrics(self) -> Dict:
        """
        Fetch table and merge metrics

        Returns structured data:
        {
            "status": "success",
            "data": [
                {
                    "metric": "active_merges",
                    "count": 5,
                    "threshold_warning": 10,
                    "threshold_critical": 20,
                    "timestamp": "..."
                },
                {
                    "metric": "total_parts",
                    "count": 150,
                    "threshold_warning": 200,
                    "threshold_critical": 300,
                    "timestamp": "..."
                }
            ]
        }
        """
        try:
            response = self._make_request("/tables")

            timestamp = self._get_timestamp()

            structured_data = {
                'status': 'success',
                'data': [],
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': timestamp
                }
            }

            # Active merges
            active_merges = self._normalize_value(response.get('activeMerges', 0))
            structured_data['data'].append({
                'metric': 'active_merges',
                'count': active_merges,
                'threshold_warning': 10,
                'threshold_critical': 20,
                'timestamp': timestamp
            })

            # Total parts count
            total_parts = self._normalize_value(response.get('totalParts', 0))
            structured_data['data'].append({
                'metric': 'total_parts',
                'count': total_parts,
                'threshold_warning': 200,
                'threshold_critical': 300,
                'timestamp': timestamp
            })

            # Table count
            table_count = self._normalize_value(response.get('tableCount', 0))
            structured_data['data'].append({
                'metric': 'table_count',
                'count': table_count,
                'timestamp': timestamp
            })

            # Determine status
            if active_merges >= 20 or total_parts >= 300:
                structured_data['status'] = 'error'
            elif active_merges >= 10 or total_parts >= 200:
                structured_data['status'] = 'warning'

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch table metrics: {e}")
            return {
                'status': 'error',
                'data': [],
                'error_message': str(e),
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': self._get_timestamp()
                }
            }

    def fetch_replication_metrics(self) -> Dict:
        """
        Fetch replication and cluster health metrics

        Returns structured data:
        {
            "status": "success",
            "data": [
                {
                    "metric": "replication_lag",
                    "max_lag_entries": 100,
                    "threshold_warning": 1000,
                    "threshold_critical": 10000,
                    "timestamp": "..."
                },
                {
                    "metric": "active_replicas",
                    "count": 3,
                    "total_replicas": 3,
                    "timestamp": "..."
                }
            ]
        }
        """
        try:
            response = self._make_request("/replication")

            timestamp = self._get_timestamp()

            structured_data = {
                'status': 'success',
                'data': [],
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': timestamp
                }
            }

            # Replication lag
            max_lag = self._normalize_value(response.get('maxReplicationLag', 0))
            structured_data['data'].append({
                'metric': 'replication_lag',
                'max_lag_entries': max_lag,
                'threshold_warning': 1000,
                'threshold_critical': 10000,
                'timestamp': timestamp
            })

            # Replica status
            active_replicas = self._normalize_value(response.get('activeReplicas', 0))
            total_replicas = self._normalize_value(response.get('totalReplicas', 0))
            structured_data['data'].append({
                'metric': 'active_replicas',
                'count': active_replicas,
                'total_replicas': total_replicas,
                'timestamp': timestamp
            })

            # Readonly replicas
            readonly_count = self._normalize_value(response.get('readonlyReplicas', 0))
            structured_data['data'].append({
                'metric': 'readonly_replicas',
                'count': readonly_count,
                'timestamp': timestamp
            })

            # Determine status
            if max_lag >= 10000 or readonly_count > 0 or active_replicas < total_replicas:
                structured_data['status'] = 'error'
            elif max_lag >= 1000:
                structured_data['status'] = 'warning'

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch replication metrics: {e}")
            return {
                'status': 'error',
                'data': [],
                'error_message': str(e),
                'metadata': {
                    'source': 'instaclustr_api',
                    'cluster_id': self.cluster_id,
                    'query_timestamp': self._get_timestamp()
                }
            }

    def fetch_all_metrics(self) -> Dict:
        """
        Fetch all available metrics

        Returns:
            Combined structured data from all metric endpoints
        """
        return {
            'node_metrics': self.fetch_node_metrics(),
            'disk_metrics': self.fetch_disk_metrics(),
            'query_performance_metrics': self.fetch_query_performance_metrics(),
            'table_metrics': self.fetch_table_metrics(),
            'replication_metrics': self.fetch_replication_metrics()
        }

    def validate_structured_data(self, check_data: Dict) -> bool:
        """
        Validate that check data follows snapshot requirements

        Ensures:
        - Status field present
        - Data is array
        - Timestamps present
        - Consistent field names

        Args:
            check_data: Structured check data

        Returns:
            True if valid

        Raises:
            ValueError: If validation fails
        """
        # Check has status
        if 'status' not in check_data:
            raise ValueError("Missing 'status' field")

        # Check has data array
        if 'data' not in check_data:
            raise ValueError("Missing 'data' field")

        if not isinstance(check_data['data'], list):
            raise ValueError("'data' must be an array")

        # Validate each data point
        for idx, data_point in enumerate(check_data['data']):
            if not isinstance(data_point, dict):
                raise ValueError(f"data[{idx}] must be an object")

            # Check for timestamp (add if missing)
            if 'timestamp' not in data_point:
                logger.warning(f"data[{idx}] missing timestamp - adding current time")
                data_point['timestamp'] = self._get_timestamp()

        return True

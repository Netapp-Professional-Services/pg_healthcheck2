"""
Instaclustr Monitoring API Client

Fetches operational metrics from Instaclustr Managed Cassandra clusters
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


class InstaclustrAPIClient:
    """
    Client for Instaclustr Monitoring API

    Fetches metrics and transforms them into structured format
    compatible with health check snapshot requirements.
    """

    BASE_URL = "https://api.instaclustr.com/monitoring/v1"

    def __init__(self, cluster_id: str, username: str, api_key: str, timeout: int = 30):
        """
        Initialize Instaclustr API client

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

    def fetch_jvm_metrics(self) -> Dict:
        """
        Fetch JVM heap and GC metrics

        Returns structured data:
        {
            "status": "success",
            "data": [
                {
                    "metric": "heap_used",
                    "value_mb": 15360,
                    "max_mb": 20480,
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
            # Fetch JVM metrics from API
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
            total_heap_used = 0
            total_heap_max = 0
            node_count = 0
            per_node_data = []

            for node in response.get('nodes', []):
                node_ip = node.get('privateIp', node.get('publicIp', 'unknown'))
                jvm_data = node.get('jvm', {})

                heap_used_mb = self._normalize_value(jvm_data.get('heapUsed', 0))
                heap_max_mb = self._normalize_value(jvm_data.get('heapMax', 0))

                total_heap_used += heap_used_mb
                total_heap_max += heap_max_mb
                node_count += 1

                # Store per-node breakdown
                per_node_data.append({
                    'node': node_ip,
                    'heap_used_mb': heap_used_mb,
                    'heap_max_mb': heap_max_mb,
                    'heap_percent': round((heap_used_mb / heap_max_mb * 100), 1) if heap_max_mb > 0 else 0
                })

            # Cluster-wide aggregate
            heap_percent = round((total_heap_used / total_heap_max * 100), 1) if total_heap_max > 0 else 0

            structured_data['data'].append({
                'metric': 'heap_used',
                'value_mb': total_heap_used,
                'max_mb': total_heap_max,
                'percent_used': heap_percent,
                'threshold_warning': 75,
                'threshold_critical': 85,
                'timestamp': timestamp,
                'aggregation': 'cluster_total',
                'node_count': node_count
            })

            # Add per-node breakdown
            structured_data['per_node_data'] = per_node_data

            # Determine status based on thresholds
            if heap_percent >= 85:
                structured_data['status'] = 'error'
            elif heap_percent >= 75:
                structured_data['status'] = 'warning'

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch JVM metrics: {e}")
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

    def fetch_compaction_metrics(self) -> Dict:
        """
        Fetch compaction metrics

        Returns structured data:
        {
            "status": "success",
            "data": [
                {
                    "metric": "pending_compaction_tasks",
                    "count": 5,
                    "threshold_warning": 10,
                    "threshold_critical": 20,
                    "timestamp": "..."
                },
                {
                    "metric": "compaction_throughput_mb_per_sec",
                    "value": 16.5,
                    "unit": "MB/s",
                    "timestamp": "..."
                }
            ]
        }
        """
        try:
            response = self._make_request("/compactions")

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

            # Pending compactions
            pending_count = self._normalize_value(response.get('pendingTasks', 0))
            structured_data['data'].append({
                'metric': 'pending_compaction_tasks',
                'count': pending_count,
                'threshold_warning': 10,
                'threshold_critical': 20,
                'timestamp': timestamp
            })

            # Compaction throughput
            throughput = self._normalize_value(response.get('throughputMbPerSec', 0))
            structured_data['data'].append({
                'metric': 'compaction_throughput_mb_per_sec',
                'value': throughput,
                'unit': 'MB/s',
                'timestamp': timestamp
            })

            # Determine status
            if pending_count >= 20:
                structured_data['status'] = 'error'
            elif pending_count >= 10:
                structured_data['status'] = 'warning'

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch compaction metrics: {e}")
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

    def fetch_performance_metrics(self) -> Dict:
        """
        Fetch read/write operations and latency metrics

        Returns structured data:
        {
            "status": "success",
            "data": [
                {
                    "metric": "reads_per_second",
                    "value": 1234.5,
                    "unit": "ops/sec",
                    "timestamp": "..."
                },
                {
                    "metric": "read_latency_p95",
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

            # Read operations
            reads_per_sec = self._normalize_value(response.get('readsPerSecond', 0))
            structured_data['data'].append({
                'metric': 'reads_per_second',
                'value': reads_per_sec,
                'unit': 'ops/sec',
                'timestamp': timestamp
            })

            # Write operations
            writes_per_sec = self._normalize_value(response.get('writesPerSecond', 0))
            structured_data['data'].append({
                'metric': 'writes_per_second',
                'value': writes_per_sec,
                'unit': 'ops/sec',
                'timestamp': timestamp
            })

            # Read latency percentiles
            read_latency = response.get('readLatency', {})
            for percentile in ['p50', 'p95', 'p99']:
                value = self._normalize_value(read_latency.get(percentile, 0))
                structured_data['data'].append({
                    'metric': f'read_latency_{percentile}',
                    'value_ms': value,
                    'unit': 'ms',
                    'timestamp': timestamp
                })

            # Write latency percentiles
            write_latency = response.get('writeLatency', {})
            for percentile in ['p50', 'p95', 'p99']:
                value = self._normalize_value(write_latency.get(percentile, 0))
                structured_data['data'].append({
                    'metric': f'write_latency_{percentile}',
                    'value_ms': value,
                    'unit': 'ms',
                    'timestamp': timestamp
                })

            return structured_data

        except Exception as e:
            logger.error(f"Failed to fetch performance metrics: {e}")
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
            'jvm_metrics': self.fetch_jvm_metrics(),
            'compaction_metrics': self.fetch_compaction_metrics(),
            'disk_metrics': self.fetch_disk_metrics(),
            'performance_metrics': self.fetch_performance_metrics()
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

"""
Instaclustr Performance Metrics Check

Fetches read/write operations and latency metrics from Instaclustr Monitoring API.

Monitors:
- Read/write operations per second
- Read/write latency percentiles (p50, p95, p99)

This check is only executed when:
1. Instaclustr environment is detected
2. API credentials are configured

Returns structured data compatible with trend analysis.
"""

import logging
from typing import Dict
from plugins.cassandra.utils.instaclustr_api_client import InstaclustrAPIClient

logger = logging.getLogger(__name__)


def check_instaclustr_performance_metrics(settings: Dict, connector=None) -> Dict:
    """
    Check performance metrics via Instaclustr Monitoring API

    Args:
        settings: Configuration settings containing:
            - cluster_id: Instaclustr cluster UUID
            - api_username: Instaclustr username
            - api_key: Instaclustr API key
        connector: Cassandra connector (optional)

    Returns:
        Structured findings:
        {
            "instaclustr_performance_metrics": {
                "read_operations": {
                    "status": "success",
                    "data": [
                        {
                            "metric": "reads_per_second",
                            "value": 1234.5,
                            "unit": "ops/sec",
                            "timestamp": "..."
                        }
                    ]
                },
                "write_operations": {...},
                "read_latency": {
                    "status": "success",
                    "data": [
                        {
                            "metric": "read_latency_p50",
                            "value_ms": 2.5,
                            "unit": "ms",
                            "timestamp": "..."
                        },
                        {
                            "metric": "read_latency_p95",
                            "value_ms": 15.2,
                            "unit": "ms",
                            "timestamp": "..."
                        }
                    ]
                },
                "write_latency": {...}
            }
        }
    """
    # Check if API credentials are configured
    cluster_id = settings.get('instaclustr_cluster_id')
    api_username = settings.get('instaclustr_api_username')
    api_key = settings.get('instaclustr_api_key')

    if not all([cluster_id, api_username, api_key]):
        return {
            'instaclustr_performance_metrics': {
                'read_operations': {
                    'status': 'skipped',
                    'reason': 'Instaclustr API credentials not configured',
                    'required_settings': [
                        'instaclustr_cluster_id',
                        'instaclustr_api_username',
                        'instaclustr_api_key'
                    ]
                },
                'write_operations': {
                    'status': 'skipped',
                    'reason': 'Instaclustr API credentials not configured'
                },
                'read_latency': {
                    'status': 'skipped',
                    'reason': 'Instaclustr API credentials not configured'
                },
                'write_latency': {
                    'status': 'skipped',
                    'reason': 'Instaclustr API credentials not configured'
                }
            }
        }

    try:
        # Initialize API client
        client = InstaclustrAPIClient(
            cluster_id=cluster_id,
            username=api_username,
            api_key=api_key,
            timeout=settings.get('api_timeout', 30)
        )

        # Fetch performance metrics
        perf_data = client.fetch_performance_metrics()

        # Validate structured data
        try:
            client.validate_structured_data(perf_data)
        except ValueError as e:
            logger.error(f"Performance metrics validation failed: {e}")
            perf_data['status'] = 'error'
            perf_data['validation_error'] = str(e)

        # Split into sub-checks for better organization
        read_ops_data = []
        write_ops_data = []
        read_latency_data = []
        write_latency_data = []

        for metric in perf_data.get('data', []):
            metric_name = metric.get('metric', '')

            if metric_name == 'reads_per_second':
                read_ops_data.append(metric)
            elif metric_name == 'writes_per_second':
                write_ops_data.append(metric)
            elif metric_name.startswith('read_latency_'):
                read_latency_data.append(metric)
            elif metric_name.startswith('write_latency_'):
                write_latency_data.append(metric)

        # Return in check format with separate sub-checks
        return {
            'instaclustr_performance_metrics': {
                'read_operations': {
                    'status': perf_data['status'],
                    'data': read_ops_data,
                    'metadata': perf_data.get('metadata', {})
                },
                'write_operations': {
                    'status': perf_data['status'],
                    'data': write_ops_data,
                    'metadata': perf_data.get('metadata', {})
                },
                'read_latency': {
                    'status': perf_data['status'],
                    'data': read_latency_data,
                    'metadata': perf_data.get('metadata', {})
                },
                'write_latency': {
                    'status': perf_data['status'],
                    'data': write_latency_data,
                    'metadata': perf_data.get('metadata', {})
                }
            }
        }

    except Exception as e:
        logger.error(f"Failed to fetch Instaclustr performance metrics: {e}")
        return {
            'instaclustr_performance_metrics': {
                'read_operations': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                },
                'write_operations': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                },
                'read_latency': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                },
                'write_latency': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                }
            }
        }


# Register check metadata
check_metadata = {
    'name': 'instaclustr_performance_metrics',
    'description': 'Read/write operations and latency metrics from Instaclustr API',
    'category': 'performance',
    'requires_api': True,
    'requires_ssh': False,
    'requires_cql': False
}

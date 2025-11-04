"""
Instaclustr Compaction Metrics Check

Fetches compaction metrics from Instaclustr Monitoring API.

Monitors:
- Pending compaction tasks
- Compaction throughput

This check is only executed when:
1. Instaclustr environment is detected
2. API credentials are configured

Returns structured data compatible with trend analysis.
"""

import logging
from typing import Dict
from plugins.cassandra.utils.instaclustr_api_client import InstaclustrAPIClient

logger = logging.getLogger(__name__)


def check_instaclustr_compaction_metrics(settings: Dict, connector=None) -> Dict:
    """
    Check compaction metrics via Instaclustr Monitoring API

    Args:
        settings: Configuration settings containing:
            - cluster_id: Instaclustr cluster UUID
            - api_username: Instaclustr username
            - api_key: Instaclustr API key
        connector: Cassandra connector (optional)

    Returns:
        Structured findings:
        {
            "instaclustr_compaction_metrics": {
                "pending_compactions": {
                    "status": "success",
                    "data": [
                        {
                            "metric": "pending_compaction_tasks",
                            "count": 5,
                            "threshold_warning": 10,
                            "threshold_critical": 20,
                            "timestamp": "..."
                        }
                    ]
                },
                "compaction_throughput": {
                    "status": "success",
                    "data": [
                        {
                            "metric": "compaction_throughput_mb_per_sec",
                            "value": 16.5,
                            "unit": "MB/s",
                            "timestamp": "..."
                        }
                    ]
                }
            }
        }
    """
    # Check if API credentials are configured
    cluster_id = settings.get('instaclustr_cluster_id')
    api_username = settings.get('instaclustr_api_username')
    api_key = settings.get('instaclustr_api_key')

    if not all([cluster_id, api_username, api_key]):
        return {
            'instaclustr_compaction_metrics': {
                'pending_compactions': {
                    'status': 'skipped',
                    'reason': 'Instaclustr API credentials not configured',
                    'required_settings': [
                        'instaclustr_cluster_id',
                        'instaclustr_api_username',
                        'instaclustr_api_key'
                    ]
                },
                'compaction_throughput': {
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

        # Fetch compaction metrics
        compaction_data = client.fetch_compaction_metrics()

        # Validate structured data
        try:
            client.validate_structured_data(compaction_data)
        except ValueError as e:
            logger.error(f"Compaction metrics validation failed: {e}")
            compaction_data['status'] = 'error'
            compaction_data['validation_error'] = str(e)

        # Split into sub-checks for better organization
        pending_data = []
        throughput_data = []

        for metric in compaction_data.get('data', []):
            if metric.get('metric') == 'pending_compaction_tasks':
                pending_data.append(metric)
            elif metric.get('metric') == 'compaction_throughput_mb_per_sec':
                throughput_data.append(metric)

        # Return in check format with separate sub-checks
        return {
            'instaclustr_compaction_metrics': {
                'pending_compactions': {
                    'status': compaction_data['status'],
                    'data': pending_data,
                    'metadata': compaction_data.get('metadata', {})
                },
                'compaction_throughput': {
                    'status': compaction_data['status'],
                    'data': throughput_data,
                    'metadata': compaction_data.get('metadata', {})
                }
            }
        }

    except Exception as e:
        logger.error(f"Failed to fetch Instaclustr compaction metrics: {e}")
        return {
            'instaclustr_compaction_metrics': {
                'pending_compactions': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                },
                'compaction_throughput': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                }
            }
        }


# Register check metadata
check_metadata = {
    'name': 'instaclustr_compaction_metrics',
    'description': 'Compaction metrics from Instaclustr API',
    'category': 'performance',
    'requires_api': True,
    'requires_ssh': False,
    'requires_cql': False
}

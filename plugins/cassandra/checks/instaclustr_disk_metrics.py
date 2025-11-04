"""
Instaclustr Disk Metrics Check

Fetches disk utilization metrics from Instaclustr Monitoring API.

Provides:
- Cluster-wide disk aggregate
- Per-node disk breakdown

This check is only executed when:
1. Instaclustr environment is detected
2. API credentials are configured

Returns structured data compatible with trend analysis.
"""

import logging
from typing import Dict
from plugins.cassandra.utils.instaclustr_api_client import InstaclustrAPIClient

logger = logging.getLogger(__name__)


def check_instaclustr_disk_metrics(settings: Dict, connector=None) -> Dict:
    """
    Check disk utilization via Instaclustr Monitoring API

    Args:
        settings: Configuration settings containing:
            - cluster_id: Instaclustr cluster UUID
            - api_username: Instaclustr username
            - api_key: Instaclustr API key
        connector: Cassandra connector (optional)

    Returns:
        Structured findings:
        {
            "instaclustr_disk_metrics": {
                "disk_utilization": {
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
            }
        }
    """
    # Check if API credentials are configured
    cluster_id = settings.get('instaclustr_cluster_id')
    api_username = settings.get('instaclustr_api_username')
    api_key = settings.get('instaclustr_api_key')

    if not all([cluster_id, api_username, api_key]):
        return {
            'instaclustr_disk_metrics': {
                'disk_utilization': {
                    'status': 'skipped',
                    'reason': 'Instaclustr API credentials not configured',
                    'required_settings': [
                        'instaclustr_cluster_id',
                        'instaclustr_api_username',
                        'instaclustr_api_key'
                    ]
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

        # Fetch disk metrics
        disk_data = client.fetch_disk_metrics()

        # Validate structured data
        try:
            client.validate_structured_data(disk_data)
        except ValueError as e:
            logger.error(f"Disk metrics validation failed: {e}")
            disk_data['status'] = 'error'
            disk_data['validation_error'] = str(e)

        # Return in check format
        return {
            'instaclustr_disk_metrics': {
                'disk_utilization': disk_data
            }
        }

    except Exception as e:
        logger.error(f"Failed to fetch Instaclustr disk metrics: {e}")
        return {
            'instaclustr_disk_metrics': {
                'disk_utilization': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                }
            }
        }


# Register check metadata
check_metadata = {
    'name': 'instaclustr_disk_metrics',
    'description': 'Disk utilization metrics from Instaclustr API',
    'category': 'capacity',
    'requires_api': True,
    'requires_ssh': False,
    'requires_cql': False
}

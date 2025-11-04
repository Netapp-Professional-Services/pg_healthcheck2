"""
Instaclustr JVM Metrics Check

Fetches JVM heap and GC metrics from Instaclustr Monitoring API.

This check is only executed when:
1. Instaclustr environment is detected
2. API credentials are configured (cluster_id, api_username, api_key)

Returns structured data compatible with trend analysis.
"""

import logging
from typing import Dict
from plugins.cassandra.utils.instaclustr_api_client import InstaclustrAPIClient

logger = logging.getLogger(__name__)


def check_instaclustr_jvm_metrics(settings: Dict, connector=None) -> Dict:
    """
    Check JVM heap usage via Instaclustr Monitoring API

    Args:
        settings: Configuration settings containing:
            - cluster_id: Instaclustr cluster UUID
            - api_username: Instaclustr username
            - api_key: Instaclustr API key
        connector: Cassandra connector (optional, for metadata)

    Returns:
        Structured findings:
        {
            "instaclustr_jvm_metrics": {
                "heap_usage": {
                    "status": "success",
                    "data": [
                        {
                            "metric": "heap_used",
                            "value_mb": 15360,
                            "max_mb": 20480,
                            "percent_used": 75.0,
                            "timestamp": "..."
                        }
                    ],
                    "per_node_data": [...]
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
            'instaclustr_jvm_metrics': {
                'heap_usage': {
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

        # Fetch JVM metrics
        jvm_data = client.fetch_jvm_metrics()

        # Validate structured data
        try:
            client.validate_structured_data(jvm_data)
        except ValueError as e:
            logger.error(f"JVM metrics validation failed: {e}")
            jvm_data['status'] = 'error'
            jvm_data['validation_error'] = str(e)

        # Return in check format
        return {
            'instaclustr_jvm_metrics': {
                'heap_usage': jvm_data
            }
        }

    except Exception as e:
        logger.error(f"Failed to fetch Instaclustr JVM metrics: {e}")
        return {
            'instaclustr_jvm_metrics': {
                'heap_usage': {
                    'status': 'error',
                    'error_message': str(e),
                    'data': []
                }
            }
        }


# Register check metadata
check_metadata = {
    'name': 'instaclustr_jvm_metrics',
    'description': 'JVM heap usage metrics from Instaclustr API',
    'category': 'performance',
    'requires_api': True,
    'requires_ssh': False,
    'requires_cql': False
}

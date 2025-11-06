"""
Instaclustr Node Metrics Check for ClickHouse

Fetches node-level resource metrics (CPU, memory, load) from Instaclustr API
and transforms them into snapshot-compatible structured format.

Requirements:
- Inst

aclustr API credentials configured
- instaclustr_cluster_id in config
"""

import logging
from typing import Dict, Tuple
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


# Check metadata for requirements
check_metadata = {
    'requires_api': True,
    'requires_ssh': False,
    'requires_connection': False,
    'description': 'Node resource metrics from Instaclustr API'
}


def get_weight():
    """Returns the importance score for this check."""
    return 8


def run_check_instaclustr_node_metrics(connector, settings) -> Tuple[str, Dict]:
    """
    Fetch node metrics from Instaclustr API

    Returns structured data compatible with trend analysis:
    {
        "instaclustr_node_metrics": {
            "memory_usage": {
                "status": "success",
                "data": [...],
                "metadata": {...}
            },
            "cpu_usage": {
                "status": "success",
                "data": [...],
                "metadata": {...}
            }
        }
    }

    Args:
        connector: ClickHouse connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("Instaclustr Node Metrics")
    builder.para("Node-level resource utilization metrics from Instaclustr Monitoring API.")

    # Check if API is configured
    cluster_id = settings.get('instaclustr_cluster_id')
    username = settings.get('instaclustr_api_username')
    api_key = settings.get('instaclustr_api_key')

    if not all([cluster_id, username, api_key]):
        builder.note(
            "ℹ️ **Instaclustr API Not Configured**\n\n"
            "To enable Instaclustr API metrics, configure:\n"
            "- instaclustr_cluster_id\n"
            "- instaclustr_api_username\n"
            "- instaclustr_api_key"
        )
        structured_data["instaclustr_node_metrics"] = {"status": "skipped", "data": []}
        return builder.build(), structured_data

    try:
        # Import API client
        from plugins.clickhouse.utils.instaclustr_api_client import InstaclustrClickHouseAPIClient

        # Create API client
        api_client = InstaclustrClickHouseAPIClient(
            cluster_id=cluster_id,
            username=username,
            api_key=api_key,
            timeout=settings.get('api_timeout', 30)
        )

        # Fetch node metrics
        metrics_response = api_client.fetch_node_metrics()

        # Validate structured data
        api_client.validate_structured_data(metrics_response)

        # Extract data for display
        if metrics_response.get('status') == 'success':
            builder.h4("Node Resource Summary")

            # Display cluster aggregates
            for data_point in metrics_response.get('data', []):
                metric_name = data_point.get('metric')

                if metric_name == 'memory_used':
                    memory_percent = data_point.get('percent_used', 0)
                    memory_mb = data_point.get('value_mb', 0)
                    total_mb = data_point.get('total_mb', 0)

                    if memory_percent >= 85:
                        builder.critical(
                            f"🔴 **Critical Memory Usage**: {memory_percent}% "
                            f"({memory_mb} MB / {total_mb} MB)"
                        )
                    elif memory_percent >= 75:
                        builder.warning(
                            f"⚠️ **High Memory Usage**: {memory_percent}% "
                            f"({memory_mb} MB / {total_mb} MB)"
                        )
                    else:
                        builder.note(
                            f"✅ **Memory Usage**: {memory_percent}% "
                            f"({memory_mb} MB / {total_mb} MB)"
                        )

                elif metric_name == 'cpu_usage':
                    cpu_percent = data_point.get('percent', 0)

                    if cpu_percent >= 90:
                        builder.critical(f"🔴 **Critical CPU Usage**: {cpu_percent}%")
                    elif cpu_percent >= 80:
                        builder.warning(f"⚠️ **High CPU Usage**: {cpu_percent}%")
                    else:
                        builder.note(f"✅ **CPU Usage**: {cpu_percent}%")

            # Display per-node breakdown
            per_node_data = metrics_response.get('per_node_data', [])
            if per_node_data:
                builder.h4("Per-Node Resource Breakdown")

                node_table = []
                for node in per_node_data:
                    node_table.append({
                        "Node": node.get('node'),
                        "Memory %": f"{node.get('memory_percent', 0):.1f}%",
                        "Memory Used": f"{node.get('memory_used_mb', 0)} MB",
                        "CPU %": f"{node.get('cpu_percent', 0):.1f}%",
                        "Load Avg": f"{node.get('load_average_1m', 0):.2f}"
                    })

                builder.table(node_table)

            builder.success("✅ Node metrics fetched successfully from Instaclustr API")

        else:
            # API returned error or warning
            error_msg = metrics_response.get('error_message', 'Unknown error')
            builder.error(f"Failed to fetch node metrics: {error_msg}")

        # Store structured data
        structured_data["instaclustr_node_metrics"] = metrics_response

    except ImportError:
        builder.error("Instaclustr API client not available")
        structured_data["instaclustr_node_metrics"] = {
            "status": "error",
            "data": [],
            "error_message": "API client not available"
        }

    except Exception as e:
        logger.error(f"Instaclustr node metrics check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["instaclustr_node_metrics"] = {
            "status": "error",
            "data": [],
            "error_message": str(e)
        }

    return builder.build(), structured_data

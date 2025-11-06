"""
ClickHouse System Metrics Check

Retrieves current system metrics snapshot for real-time monitoring.
Provides an overview of active operations and resource utilization.

Requirements:
- ClickHouse client access to system.metrics
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_node_metrics

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 7  # Medium-high priority


def run_system_metrics_check(connector, settings):
    """
    Retrieves a snapshot of current system metrics from system.metrics table.

    Args:
        connector: ClickHouse connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add check header
    builder.h3("System Metrics Snapshot")
    builder.para(
        "Real-time snapshot of current system activity and resource utilization from system.metrics table."
    )

    try:
        # Get all system metrics using qrylib
        query = qry_node_metrics.get_system_metrics_query(connector)
        metrics_result = connector.execute_query(query)

        if not metrics_result or len(metrics_result) == 0:
            builder.warning("No system metrics data available.")
            structured_data["system_metrics"] = {"status": "success", "count": 0}
            return builder.build(), structured_data

        # Convert to structured format
        metrics_data = []
        for row in metrics_result:
            metrics_data.append({
                'metric': row[0],
                'value': row[1],
                'description': row[2]
            })

        # Display summary of key metrics
        builder.h4("Key Operational Metrics")

        key_metrics = [
            'Query', 'Merge', 'PartMutation',
            'ReplicatedFetch', 'ReplicatedSend',
            'TCPConnection', 'HTTPConnection',
            'MemoryTracking', 'BackgroundPoolTask'
        ]

        key_metrics_table = []
        for metric_name in key_metrics:
            metric_data = next((m for m in metrics_data if m['metric'] == metric_name), None)
            if metric_data:
                key_metrics_table.append({
                    "Metric": metric_data['metric'],
                    "Current Value": str(metric_data['value']),
                    "Description": metric_data['description']
                })

        if key_metrics_table:
            builder.table(key_metrics_table)
        else:
            builder.para("No key metrics available.")

        builder.blank()

        # Display all metrics in expandable section if there are many
        total_metrics = len(metrics_data)
        builder.h4(f"All System Metrics ({total_metrics} total)")

        # Show first 50 metrics in table
        all_metrics_table = []
        for metric in metrics_data[:50]:
            all_metrics_table.append({
                "Metric": metric['metric'],
                "Value": str(metric['value']),
                "Description": metric['description'][:100] + "..." if len(metric['description']) > 100 else metric['description']
            })

        builder.table(all_metrics_table)

        if total_metrics > 50:
            builder.para(f"...and {total_metrics - 50} more metrics")

        builder.blank()

        # Add note about metrics
        builder.note(
            "**About System Metrics**\n\n"
            "The system.metrics table provides instantaneous metric values that can be calculated "
            "immediately. These metrics represent the current state of the system at the moment of query execution.\n\n"
            "For historical metric trends, query system.metric_log which contains periodic snapshots of these values."
        )

        # Structured data
        structured_data["system_metrics"] = {
            "status": "success",
            "count": total_metrics,
            "data": metrics_data
        }

    except Exception as e:
        logger.error(f"System metrics check failed: {e}", exc_info=True)
        builder.error(f"Could not retrieve system metrics: {e}")
        structured_data["system_metrics"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data

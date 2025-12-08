"""
OpenSearch cluster health check.

This check provides cluster-level health metrics including status, node counts,
shard allocation, and active operations.

Data source:
- Live clusters: GET /_cluster/health
- Imported diagnostics: cluster_health.json
"""

from plugins.common.check_helpers import CheckContentBuilder


def run_cluster_health_check(connector, settings):
    """
    Retrieves and formats the health status of the OpenSearch cluster.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("OpenSearch Cluster Health")

    try:
        health = connector.execute_query({"operation": "cluster_health"})

        if isinstance(health, dict) and "error" in health:
            builder.error(f"Could not retrieve cluster health: {health['error']}")
            return builder.build(), {
                "status": "error",
                "error": health['error']
            }

        # Extract key metrics
        cluster_status = health.get('status', 'unknown').lower()
        cluster_name = health.get('cluster_name', 'Unknown')
        node_count = health.get('number_of_nodes', 0)
        data_node_count = health.get('number_of_data_nodes', 0)
        active_shards = health.get('active_shards', 0)
        active_primary_shards = health.get('active_primary_shards', 0)
        unassigned_shards = health.get('unassigned_shards', 0)
        relocating_shards = health.get('relocating_shards', 0)
        initializing_shards = health.get('initializing_shards', 0)
        pending_tasks = health.get('number_of_pending_tasks', 0)
        active_shards_percent = health.get('active_shards_percent_as_number', 100.0)

        # Determine status severity and add appropriate message
        if cluster_status == 'green':
            builder.success(
                "**Cluster status is GREEN**\n\n"
                "All primary and replica shards are allocated. The cluster is fully operational."
            )
        elif cluster_status == 'yellow':
            builder.warning(
                "**Cluster status is YELLOW**\n\n"
                "All primary shards are allocated, but one or more replica shards are unassigned.\n"
                "The cluster is functional but at risk of data loss if a node fails."
            )
            builder.blank()
            builder.text("*Recommended Actions:*")
            builder.text("- Check if you have enough nodes for your replica configuration")
            builder.text("- Review shard allocation settings")
            builder.text("- Consider adding nodes if this is a production cluster")
        else:  # red or unknown
            builder.critical(
                "**Cluster status is RED**\n\n"
                "At least one primary shard is not allocated. The cluster is non-operational "
                "and some data is unavailable for search and indexing.\n\n"
                "**URGENT: Immediate action required!**"
            )
            builder.blank()
            builder.text("*Immediate Actions Required:*")
            builder.text("- Check cluster logs for errors")
            builder.text("- Verify all nodes are online and reachable")
            builder.text("- Review disk space on all nodes")
            builder.text("- Check for shard allocation failures")

        builder.blank()

        # Build health metrics table
        builder.h4("Cluster Health Metrics")

        table_data = [
            {"Metric": "Cluster Name", "Value": cluster_name},
            {"Metric": "Status", "Value": cluster_status.upper()},
            {"Metric": "Total Nodes", "Value": str(node_count)},
            {"Metric": "Data Nodes", "Value": str(data_node_count)},
            {"Metric": "Active Shards", "Value": str(active_shards)},
            {"Metric": "Active Primary Shards", "Value": str(active_primary_shards)},
            {"Metric": "Unassigned Shards", "Value": str(unassigned_shards)},
            {"Metric": "Relocating Shards", "Value": str(relocating_shards)},
            {"Metric": "Initializing Shards", "Value": str(initializing_shards)},
            {"Metric": "Pending Tasks", "Value": str(pending_tasks)},
            {"Metric": "Active Shards %", "Value": f"{active_shards_percent:.1f}%"},
        ]
        builder.table(table_data)

        # Build structured data for rules engine
        structured_data = {
            "status": "success",
            "data": {
                "cluster_name": cluster_name,
                "cluster_status": cluster_status,
                "is_green": cluster_status == 'green',
                "is_yellow": cluster_status == 'yellow',
                "is_red": cluster_status == 'red',
                "node_count": node_count,
                "data_node_count": data_node_count,
                "active_shards": active_shards,
                "active_primary_shards": active_primary_shards,
                "unassigned_shards": unassigned_shards,
                "relocating_shards": relocating_shards,
                "initializing_shards": initializing_shards,
                "pending_tasks": pending_tasks,
                "active_shards_percent": active_shards_percent,
                "has_unassigned_shards": unassigned_shards > 0,
                "has_pending_tasks": pending_tasks > 0,
                "has_relocating_shards": relocating_shards > 0,
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Could not retrieve cluster health: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

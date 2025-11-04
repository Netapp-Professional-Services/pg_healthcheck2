from plugins.common.check_helpers import CheckContentBuilder


def run_cluster_health_check(connector, settings):
    """
    Retrieves and formats the health status of the OpenSearch cluster.

    This check provides cluster-level health metrics including status, node counts,
    shard allocation, and active operations.
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add check header
    builder.h3("OpenSearch Cluster Health")

    try:
        # Use connector's execute_query method for standardized operation dispatch
        health = connector.execute_query({"operation": "cluster_health"})

        if "error" in health:
            builder.error(f"Could not retrieve cluster health: {health['error']}")
            structured_data["cluster_health"] = {"status": "error", "details": health['error']}
            return builder.build(), structured_data

        # Determine status severity and add appropriate message
        status = health.get('status', 'unknown').lower()

        if status == 'green':
            builder.note(
                "✅ **Cluster status is GREEN**\n\n"
                "All primary and replica shards are allocated. The cluster is fully operational."
            )
        elif status == 'yellow':
            builder.warning(
                "⚠️ **Cluster status is YELLOW**\n\n"
                "All primary shards are allocated, but one or more replica shards are unassigned.\n"
                "The cluster is functional but at risk of data loss if a node fails."
            )
            builder.recs([
                "Check if you have enough nodes for your replica configuration",
                "Review shard allocation settings",
                "Consider adding nodes if this is a production cluster"
            ], title="Recommended Actions")
        else:  # red or unknown
            builder.critical(
                "🔴 **Cluster status is RED**\n\n"
                "At least one primary shard is not allocated. The cluster is non-operational "
                "and some data is unavailable for search and indexing.\n\n"
                "**URGENT: Immediate action required!**"
            )
            builder.recs([
                "Check cluster logs for errors",
                "Verify all nodes are online and reachable",
                "Review disk space on all nodes",
                "Check for shard allocation failures"
            ], title="Immediate Actions Required")

        # Build health metrics table
        builder.h4("Cluster Health Metrics")

        # Build table data with priority ordering
        metrics_order = [
            ('cluster_name', 'Cluster Name'),
            ('status', 'Status'),
            ('number_of_nodes', 'Total Nodes'),
            ('number_of_data_nodes', 'Data Nodes'),
            ('active_primary_shards', 'Active Primary Shards'),
            ('active_shards', 'Active Shards'),
            ('relocating_shards', 'Relocating Shards'),
            ('initializing_shards', 'Initializing Shards'),
            ('unassigned_shards', 'Unassigned Shards'),
            ('delayed_unassigned_shards', 'Delayed Unassigned Shards'),
            ('number_of_pending_tasks', 'Pending Tasks'),
            ('number_of_in_flight_fetch', 'In-Flight Fetches'),
            ('task_max_waiting_in_queue_millis', 'Max Task Wait Time (ms)'),
            ('active_shards_percent_as_number', 'Active Shards %')
        ]

        # Convert to table format with visual indicators
        table_data = []
        for key, label in metrics_order:
            if key in health:
                value = health[key]
                # Add visual indicators for concerning values
                if key == 'unassigned_shards' and value > 0:
                    table_data.append({"Metric": label, "Value": f"**{value}** ⚠️"})
                elif key == 'status':
                    status_icon = {'green': '✅', 'yellow': '⚠️', 'red': '🔴'}.get(
                        str(value).lower(), '❓'
                    )
                    table_data.append({"Metric": label, "Value": f"**{value.upper()}** {status_icon}"})
                else:
                    table_data.append({"Metric": label, "Value": str(value)})

        # Add any remaining metrics not in the ordered list
        processed_keys = [m[0] for m in metrics_order]
        for key, value in health.items():
            if key not in processed_keys:
                readable_key = key.replace('_', ' ').title()
                table_data.append({"Metric": readable_key, "Value": str(value)})

        # Add table to builder
        if table_data:
            builder.table(table_data)

        structured_data["cluster_health"] = {"status": "success", "data": health}

    except Exception as e:
        builder.error(f"Could not retrieve cluster health: {e}")
        structured_data["cluster_health"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data

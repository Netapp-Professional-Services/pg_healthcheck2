"""
Watcher/Alerting status for Elasticsearch clusters.

This check analyzes the Watcher alerting system to identify:
- Watcher service state
- Watch count and execution status
- Queue and thread pool status
- Current and queued watches

Data source:
- Live clusters: GET /_watcher/stats
- Imported diagnostics: commercial/watcher_stats.json

Note: Watcher is an Elasticsearch X-Pack feature. OpenSearch uses
its own alerting plugin with different APIs.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check Watcher alerting system status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Watcher/Alerting Status")

    try:
        # Get watcher stats
        watcher_stats = connector.execute_query({"operation": "watcher_stats"})

        if isinstance(watcher_stats, dict) and "error" in watcher_stats:
            # Check if this is a license issue
            error_msg = str(watcher_stats.get("error", ""))
            if "license" in error_msg.lower():
                builder.note(
                    "Watcher is not available with the current license.\n\n"
                    "Watcher requires a Gold, Platinum, or Enterprise license."
                )
                return builder.build(), {
                    "status": "not_licensed",
                    "reason": "License does not include Watcher"
                }

            builder.note(
                "Watcher status is not available.\n\n"
                "This may indicate:\n"
                "- Watcher is not enabled\n"
                "- OpenSearch is being used (different alerting system)\n"
                "- Diagnostic export did not include watcher data"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": watcher_stats.get("error", "Unknown")
            }

        # Parse watcher stats
        manually_stopped = watcher_stats.get("manually_stopped", False)
        stats_list = watcher_stats.get("stats", [])

        if not stats_list:
            builder.note("No Watcher nodes found in cluster.")
            return builder.build(), {
                "status": "success",
                "data": {"watcher_enabled": False, "node_count": 0}
            }

        # Analyze per-node stats
        node_stats = []
        total_watches = 0
        total_queued = 0
        total_current = 0
        stopped_nodes = 0

        for stat in stats_list:
            node_id = stat.get("node_id", "Unknown")
            watcher_state = stat.get("watcher_state", "unknown")
            watch_count = stat.get("watch_count", 0)
            thread_pool = stat.get("execution_thread_pool", {})
            current_watches = stat.get("current_watches", [])
            queued_watches = stat.get("queued_watches", [])

            total_watches += watch_count
            total_queued += len(queued_watches)
            total_current += len(current_watches)

            if watcher_state != "started":
                stopped_nodes += 1

            node_stats.append({
                "node_id": node_id,
                "state": watcher_state,
                "watch_count": watch_count,
                "queue_size": thread_pool.get("queue_size", 0),
                "max_size": thread_pool.get("max_size", 0),
                "current_watches": len(current_watches),
                "queued_watches": len(queued_watches)
            })

        # Build report
        if manually_stopped:
            builder.warning(
                "**Watcher has been manually stopped**\n\n"
                "Alerting is disabled. Watches will not execute until Watcher is started."
            )
        elif stopped_nodes > 0:
            builder.warning(
                f"**{stopped_nodes} node(s) have Watcher in non-started state**\n\n"
                "Some nodes may not be executing watches."
            )
        else:
            all_started = all(n["state"] == "started" for n in node_stats)
            if all_started:
                builder.success("**Watcher is running on all nodes**")
            else:
                builder.note("Watcher status varies across nodes")

        builder.blank()

        # Summary
        builder.h4("Watcher Summary")

        summary_data = [
            {"Metric": "Total Watches Defined", "Value": str(total_watches)},
            {"Metric": "Watcher Nodes", "Value": str(len(node_stats))},
            {"Metric": "Currently Executing", "Value": str(total_current)},
            {"Metric": "Queued for Execution", "Value": str(total_queued)},
            {"Metric": "Manually Stopped", "Value": "Yes" if manually_stopped else "No"}
        ]
        builder.table(summary_data)
        builder.blank()

        # Per-node status
        if len(node_stats) > 0:
            builder.h4("Per-Node Status")

            node_table = []
            for node in node_stats:
                state = node["state"]
                if state == "started":
                    state_display = "✅ Started"
                elif state == "stopped":
                    state_display = "🔴 Stopped"
                else:
                    state_display = f"⚠️ {state}"

                node_table.append({
                    "Node": node["node_id"][:12] + "..." if len(node["node_id"]) > 15 else node["node_id"],
                    "State": state_display,
                    "Watches": str(node["watch_count"]),
                    "Executing": str(node["current_watches"]),
                    "Queued": str(node["queued_watches"])
                })
            builder.table(node_table)
            builder.blank()

        # Queue analysis
        if total_queued > 10:
            builder.warning(
                f"**{total_queued} watches queued for execution**\n\n"
                "High queue may indicate slow watch execution or insufficient capacity."
            )

        # Recommendations
        if manually_stopped or stopped_nodes > 0 or total_queued > 10:
            builder.text("*Recommended Actions:*")
            if manually_stopped:
                builder.text("1. Review why Watcher was stopped")
                builder.text("2. Start Watcher: POST /_watcher/_start")
            if stopped_nodes > 0:
                builder.text("1. Check node health for nodes with stopped Watcher")
            if total_queued > 10:
                builder.text("1. Review watch execution times")
                builder.text("2. Consider reducing watch frequency or complexity")
            builder.blank()

        if total_watches == 0:
            builder.text("*Note:* No watches are currently defined. Configure watches to enable alerting.")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "watcher_enabled": True,
                "node_count": len(node_stats),
                "total_watches": total_watches,
                "total_queued": total_queued,
                "total_current": total_current,
                "manually_stopped": manually_stopped,
                "stopped_node_count": stopped_nodes,
                "all_nodes_started": stopped_nodes == 0 and not manually_stopped,
                "has_queue_buildup": total_queued > 10,
                "has_watches": total_watches > 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check Watcher status: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

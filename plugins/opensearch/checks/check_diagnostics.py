"""
OpenSearch Diagnostic Queries

Advanced diagnostic checks for troubleshooting performance issues,
understanding cluster behavior, and identifying bottlenecks.
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 5


def run_check_diagnostics(connector, settings):
    """Run advanced diagnostic queries for troubleshooting."""
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("Advanced Diagnostics")
    builder.para(
        "Detailed diagnostic information for troubleshooting performance issues, "
        "identifying bottlenecks, and understanding cluster behavior."
    )

    try:
        # 1. Hot Threads - Shows CPU-consuming operations
        builder.h4("Hot Threads Analysis")
        builder.para("Identifies threads consuming the most CPU time (useful for performance troubleshooting).")

        try:
            # Get hot threads for all nodes
            hot_threads_result = connector.execute_query({"operation": "hot_threads"})

            if "error" not in hot_threads_result:
                # Parse hot threads output
                hot_threads_text = hot_threads_result.get("hot_threads", "")

                if hot_threads_text:
                    # Check if there are any hot threads
                    if "cpu usage" in hot_threads_text.lower() or "%" in hot_threads_text:
                        builder.warning("Hot threads detected - review output for CPU-intensive operations")
                        # Show first few lines
                        lines = hot_threads_text.split('\n')[:20]
                        builder.code('\n'.join(lines), language='text')
                    else:
                        builder.note("✅ No significant hot threads detected")
                else:
                    builder.note("Hot threads data not available")

                structured_data["hot_threads"] = {
                    "status": "success",
                    "has_hot_threads": "cpu usage" in hot_threads_text.lower()
                }
            else:
                builder.note("Hot threads API not available")
                structured_data["hot_threads"] = {"status": "unavailable"}
        except Exception as e:
            logger.warning(f"Could not retrieve hot threads: {e}")
            builder.note("Hot threads information unavailable")

        # 2. Pending Tasks Detail
        builder.h4("Pending Cluster Tasks Detail")
        builder.para("Shows cluster state update tasks waiting to be processed by the master node.")

        try:
            pending_tasks = connector.execute_query({"operation": "pending_tasks"})

            if "error" not in pending_tasks:
                tasks = pending_tasks.get("tasks", [])

                if tasks:
                    builder.warning(f"⚠️ {len(tasks)} pending task(s) detected")

                    task_summary = []
                    for task in tasks[:10]:  # Show first 10
                        task_summary.append({
                            "Priority": task.get("priority", "N/A"),
                            "Source": task.get("source", "N/A"),
                            "Time in Queue": f"{task.get('timeInQueueMillis', 0)}ms",
                            "Executing": "Yes" if task.get("executing", False) else "No"
                        })

                    builder.table(task_summary)

                    if len(tasks) > 10:
                        builder.note(f"... and {len(tasks) - 10} more pending tasks")
                else:
                    builder.note("✅ No pending cluster tasks")

                structured_data["pending_tasks"] = {
                    "status": "success",
                    "count": len(tasks),
                    "task_types": list(set([t.get("source", "unknown") for t in tasks]))
                }
            else:
                builder.note("Pending tasks information unavailable")
        except Exception as e:
            logger.warning(f"Could not retrieve pending tasks: {e}")

        # 3. Segment Information
        builder.h4("Index Segment Analysis")
        builder.para("Segment counts and merge activity - high segment counts can impact search performance.")

        try:
            segments = connector.execute_query({"operation": "cat_segments"})

            if "error" not in segments:
                segment_data = segments.get("segments", [])

                if segment_data:
                    # Aggregate by index
                    index_segments = {}
                    total_segments = 0

                    for seg in segment_data:
                        index_name = seg.get("index", "unknown")
                        if index_name not in index_segments:
                            index_segments[index_name] = 0
                        index_segments[index_name] += 1
                        total_segments += 1

                    # Show indices with high segment counts
                    high_segment_indices = []
                    for idx, count in index_segments.items():
                        if count > 100:  # More than 100 segments per index is high
                            high_segment_indices.append({
                                "Index": idx,
                                "Segment Count": count,
                                "Status": "⚠️ High" if count > 200 else "Review"
                            })

                    if high_segment_indices:
                        builder.warning(f"Indices with high segment counts detected")
                        builder.table(sorted(high_segment_indices, key=lambda x: x["Segment Count"], reverse=True)[:10])
                        builder.para(
                            "**Recommendation:** Consider force merging read-only indices to reduce segment count. "
                            "High segment counts can impact search performance."
                        )
                    else:
                        builder.note(f"✅ Segment counts healthy (total: {total_segments} across all indices)")

                    structured_data["segments"] = {
                        "status": "success",
                        "total_segments": total_segments,
                        "high_segment_indices": len(high_segment_indices)
                    }
                else:
                    builder.note("No segment data available")
            else:
                builder.note("Segment information unavailable")
        except Exception as e:
            logger.warning(f"Could not retrieve segment info: {e}")

        # 4. Recovery Status
        builder.h4("Shard Recovery Status")
        builder.para("Shows ongoing shard recovery operations (relocations, replications, snapshots).")

        try:
            recovery = connector.execute_query({"operation": "cat_recovery"})

            if "error" not in recovery:
                recovery_data = recovery.get("recovery", [])

                # Filter for active recoveries
                active_recoveries = [r for r in recovery_data if r.get("stage") != "done"]

                if active_recoveries:
                    builder.warning(f"⚠️ {len(active_recoveries)} active recovery operation(s)")

                    recovery_summary = []
                    for rec in active_recoveries[:10]:
                        recovery_summary.append({
                            "Index": rec.get("index", "N/A"),
                            "Shard": rec.get("shard", "N/A"),
                            "Stage": rec.get("stage", "N/A"),
                            "Type": rec.get("type", "N/A"),
                            "Source": rec.get("source_node", "N/A"),
                            "Target": rec.get("target_node", "N/A"),
                            "Progress": f"{rec.get('files_percent', '0')}%"
                        })

                    builder.table(recovery_summary)

                    if len(active_recoveries) > 10:
                        builder.note(f"... and {len(active_recoveries) - 10} more recoveries in progress")
                else:
                    builder.note("✅ No active shard recoveries")

                structured_data["recovery"] = {
                    "status": "success",
                    "active_recoveries": len(active_recoveries)
                }
            else:
                builder.note("Recovery status unavailable")
        except Exception as e:
            logger.warning(f"Could not retrieve recovery status: {e}")

        # 5. Task Management - Long-running tasks
        builder.h4("Long-Running Tasks")
        builder.para("Identifies tasks that have been running for an extended period.")

        try:
            tasks_result = connector.execute_query({"operation": "tasks"})

            if "error" not in tasks_result:
                nodes = tasks_result.get("nodes", {})
                long_running_tasks = []

                for node_id, node_data in nodes.items():
                    node_name = node_data.get("name", node_id)
                    tasks = node_data.get("tasks", {})

                    for task_id, task_data in tasks.items():
                        running_time_nanos = task_data.get("running_time_in_nanos", 0)
                        running_time_ms = running_time_nanos / 1_000_000

                        # Flag tasks running longer than 30 seconds
                        if running_time_ms > 30000:
                            long_running_tasks.append({
                                "Node": node_name,
                                "Action": task_data.get("action", "N/A"),
                                "Running Time": f"{running_time_ms / 1000:.1f}s",
                                "Description": task_data.get("description", "N/A")[:50]
                            })

                if long_running_tasks:
                    builder.warning(f"⚠️ {len(long_running_tasks)} long-running task(s) detected")
                    builder.table(sorted(long_running_tasks, key=lambda x: x["Running Time"], reverse=True)[:10])
                else:
                    builder.note("✅ No long-running tasks detected")

                structured_data["long_running_tasks"] = {
                    "status": "success",
                    "count": len(long_running_tasks)
                }
            else:
                builder.note("Task information unavailable")
        except Exception as e:
            logger.warning(f"Could not retrieve tasks: {e}")

        # 6. Plugin Information
        builder.h4("Installed Plugins")
        builder.para("Lists OpenSearch plugins installed on the cluster.")

        try:
            plugins = connector.execute_query({"operation": "cat_plugins"})

            if "error" not in plugins:
                plugin_data = plugins.get("plugins", [])

                if plugin_data:
                    # Group plugins by type
                    unique_plugins = set()
                    node_plugin_map = {}

                    for plugin in plugin_data:
                        plugin_name = plugin.get("component", "unknown")
                        unique_plugins.add(plugin_name)

                        if plugin_name not in node_plugin_map:
                            node_plugin_map[plugin_name] = []
                        node_plugin_map[plugin_name].append(plugin.get("node", "N/A"))

                    plugin_summary = []
                    for plugin_name in sorted(unique_plugins):
                        nodes_with_plugin = len(set(node_plugin_map[plugin_name]))
                        plugin_summary.append({
                            "Plugin": plugin_name,
                            "Nodes": nodes_with_plugin,
                            "Version": plugin_data[0].get("version", "N/A") if plugin_data else "N/A"
                        })

                    builder.table(plugin_summary)

                    structured_data["plugins"] = {
                        "status": "success",
                        "count": len(unique_plugins),
                        "plugins": list(unique_plugins)
                    }
                else:
                    builder.note("No plugins detected (or running OpenSearch core only)")
                    structured_data["plugins"] = {"status": "success", "count": 0}
            else:
                builder.note("Plugin information unavailable")
        except Exception as e:
            logger.warning(f"Could not retrieve plugin info: {e}")

        # 7. Field Data Statistics
        builder.h4("Field Data Memory Usage")
        builder.para("Memory used by field data structures - high usage can indicate inefficient queries.")

        try:
            node_stats = connector.execute_query({
                "operation": "node_stats",
                "metrics": ["indices"]
            })

            if "error" not in node_stats:
                nodes = node_stats.get("nodes", {})
                field_data_info = []
                total_field_data_bytes = 0

                for node_id, node_data in nodes.items():
                    node_name = node_data.get("name", node_id)
                    indices = node_data.get("indices", {})
                    fielddata = indices.get("fielddata", {})

                    memory_size_bytes = fielddata.get("memory_size_in_bytes", 0)
                    evictions = fielddata.get("evictions", 0)

                    total_field_data_bytes += memory_size_bytes

                    if memory_size_bytes > 0:
                        field_data_info.append({
                            "Node": node_name,
                            "Memory Used": f"{memory_size_bytes / (1024**3):.2f} GB",
                            "Evictions": evictions,
                            "Status": "⚠️ Review" if memory_size_bytes > 1_000_000_000 else "OK"
                        })

                if field_data_info:
                    builder.table(field_data_info)

                    if total_field_data_bytes > 5_000_000_000:  # > 5GB
                        builder.warning(
                            f"High field data usage detected ({total_field_data_bytes / (1024**3):.2f} GB total). "
                            "Consider using doc values or reviewing query patterns."
                        )
                else:
                    builder.note("✅ Field data usage is minimal")

                structured_data["field_data"] = {
                    "status": "success",
                    "total_bytes": total_field_data_bytes
                }
            else:
                builder.note("Field data statistics unavailable")
        except Exception as e:
            logger.warning(f"Could not retrieve field data stats: {e}")

        # Summary recommendations
        builder.h4("Diagnostic Summary")

        recs = {"high": [], "general": []}

        # Add contextual recommendations based on findings
        recs["general"].extend([
            "Review hot threads during high CPU periods to identify bottlenecks",
            "Monitor segment counts - force merge read-only indices if segments > 100",
            "Check recovery operations during high I/O periods",
            "Use tasks API to identify long-running operations during performance issues",
            "Review field data usage if heap pressure is high"
        ])

        builder.recs({"general": recs["general"]})

    except Exception as e:
        logger.error(f"Diagnostics check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["diagnostics"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data

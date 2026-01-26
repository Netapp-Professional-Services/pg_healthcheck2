from plugins.cassandra.utils.qrylib.qry_compaction_pending_tasks import get_compaction_pending_tasks_query
from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High: Significant operational issues


def run_compaction_pending_tasks(connector, settings):
    """
    Performs the health check analysis for pending compaction tasks.

    Uses CQL query against system_views.thread_pools for CompactionExecutor pending tasks.

    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)

    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    # Initialize with formatted header
    adoc_content = format_check_header(
        "Compaction Pending Tasks Analysis",
        "Checking for pending compaction tasks using system_views.thread_pools."
    )
    structured_data = {}

    # Execute check using safe helper (CQL query, no SSH required)
    query = get_compaction_pending_tasks_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Compaction pending tasks")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["compaction_stats"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data

    # Analyze results - CQL returns list of rows with 'pending_tasks' column
    pending_tasks = 0
    if isinstance(raw, list) and len(raw) > 0:
        pending_tasks = raw[0].get('pending_tasks', 0) if isinstance(raw[0], dict) else 0
    elif isinstance(raw, dict):
        pending_tasks = raw.get('pending_tasks', 0)

    # Determine status and provide recommendations
    if pending_tasks == 0:
        adoc_content.append(
            "[NOTE]\n====\n"
            "No pending compaction tasks. Compaction is current.\n"
            "====\n"
        )
        status = "success"
    else:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"**{pending_tasks} pending compaction tasks** detected. "
            "This may indicate a compaction backlog leading to "
            "performance issues and increased disk usage.\n"
            "====\n"
        )

        # Use helper to format recommendations
        recommendations = [
            "Monitor write throughput and consider reducing if application allows.",
            "Check disk I/O with 'iostat -x 5' to identify bottlenecks.",
            "Review compaction strategy for affected keyspaces - consider LeveledCompactionStrategy for read-heavy workloads.",
            "Increase concurrent_compactors in cassandra.yaml if CPU allows (default: number of disks)."
        ]
        adoc_content.extend(format_recommendations(recommendations))
        status = "warning"

    structured_data["compaction_stats"] = {
        "status": status,
        "pending_tasks": pending_tasks,
        "data": raw
    }

    return "\n".join(adoc_content), structured_data

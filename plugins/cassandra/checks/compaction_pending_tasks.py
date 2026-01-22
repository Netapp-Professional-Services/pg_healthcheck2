from plugins.cassandra.utils.qrylib.qry_compaction_pending_tasks import get_nodetool_compactionstats_query
from plugins.common.check_helpers import require_ssh, format_check_header, format_recommendations, safe_execute_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High: Significant operational issues


def run_compaction_pending_tasks(connector, settings):
    """
    Performs the health check analysis for pending compaction tasks.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    # Initialize with formatted header
    adoc_content = format_check_header(
        "Compaction Pending Tasks Analysis (Nodetool)",
        "Checking for pending compaction tasks using `nodetool compactionstats`.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability using helper
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["check_result"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute check using safe helper
    query = get_nodetool_compactionstats_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool compactionstats")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["compaction_stats"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Analyze results
    pending_tasks = raw.get('pending_tasks', 0) if isinstance(raw, dict) else 0
    active_compactions = raw.get('active_compactions', []) if isinstance(raw, dict) else []
    
    adoc_content.append(formatted)
    
    # Determine status and provide recommendations
    if pending_tasks == 0 and len(active_compactions) == 0:
        adoc_content.append(
            "[NOTE]\n====\n"
            "No pending or active compaction tasks. Compaction is current.\n"
            "====\n"
        )
        status = "success"
    else:
        if pending_tasks > 0:
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
        else:
            adoc_content.append(
                f"[NOTE]\n====\n"
                f"{len(active_compactions)} active compaction(s) in progress.\n"
                "====\n"
            )
            status = "success"
    
    structured_data["compaction_stats"] = {
        "status": status,
        "pending_tasks": pending_tasks,
        "active_compactions_count": len(active_compactions),
        "data": raw
    }
    
    return "\n".join(adoc_content), structured_data

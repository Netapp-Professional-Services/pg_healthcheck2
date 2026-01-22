from plugins.cassandra.utils.qrylib.qry_tombstone_metrics import get_tombstone_metrics_query
from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 6  # Medium - performance impact from high tombstones


def run_tombstone_metrics_check(connector, settings):
    """
    Analyzes tombstone metrics for tables exceeding thresholds.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Tombstone Metrics Analysis",
        "Checking table_metrics for high mean_tombstones or max_tombstones_per_slice."
    )
    structured_data = {}
    
    threshold = settings.get('tombstone_threshold', 1000)  # Configurable threshold
    
    # Check for Cassandra version compatibility (system_views.table_metrics requires Cassandra 4.0+)
    if not hasattr(connector, 'version_info') or connector.version_info.get('major_version', 0) < 4:
        adoc_content.append("[NOTE]\n====\nTombstone metrics check is skipped (requires Cassandra 4.0+).\n====\n")
        structured_data["tombstone_metrics"] = {"status": "skipped", "reason": "Cassandra version < 4.0"}
        return "\n".join(adoc_content), structured_data
    
    try:
        query = get_tombstone_metrics_query(connector)
        success, formatted, raw = safe_execute_query(connector, query, "Tombstone metrics query")
        
        if not success:
            adoc_content.append(formatted)
            structured_data["tombstone_metrics"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces in Python
        system_keyspaces = {'system', 'system_schema', 'system_traces', 
                           'system_auth', 'system_distributed', 'system_views'}
        user_tables = [table for table in raw 
                       if table.get('keyspace_name') not in system_keyspaces]
        
        if not user_tables:
            adoc_content.append("[NOTE]\n====\nNo user tables with metrics found.\n====\n")
            structured_data["tombstone_metrics"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Find tables exceeding threshold
        high_tombstone_tables = []
        for table in user_tables:
            mean_tombstones = table.get('mean_tombstones', 0)
            max_tombstones_per_slice = table.get('max_tombstones_per_slice', 0)
            if mean_tombstones > threshold or max_tombstones_per_slice > threshold:
                high_tombstone_tables.append({
                    'keyspace_name': table['keyspace_name'],
                    'table_name': table['table_name'],
                    'mean_tombstones': mean_tombstones,
                    'max_tombstones_per_slice': max_tombstones_per_slice
                })
        
        adoc_content.append(formatted)
        
        if high_tombstone_tables:
            adoc_content.append(
                f"[WARNING]\n====\n"
                f"**{len(high_tombstone_tables)} table(s)** exceed tombstone threshold of {threshold}.\n"
                "High tombstones can cause read performance degradation.\n"
                "====\n"
            )
            
            # Table of high tombstone tables
            adoc_content.append("\n==== High Tombstone Tables")
            adoc_content.append("|===\n|Keyspace|Table|Mean Tombstones|Max per Slice")
            for table in high_tombstone_tables:
                adoc_content.append(
                    f"|{table['keyspace_name']}|{table['table_name']}|{table['mean_tombstones']}|{table['max_tombstones_per_slice']}"
                )
            adoc_content.append("|===\n")
            
            recommendations = [
                "Review delete patterns in application code to reduce unnecessary deletes",
                "Consider enabling tombstone compaction options in table schema",
                "For high-delete workloads, tune gc_grace_seconds to a lower value (e.g., 1 day)",
                "Monitor with 'nodetool tablestats' for ongoing tombstone accumulation",
                "If severe, consider rewriting tables with 'sstableloader' to purge tombstones"
            ]
            adoc_content.extend(format_recommendations(recommendations))
            
            status_result = "warning"
        else:
            adoc_content.append(
                "[NOTE]\n====\n"
                f"All {len(user_tables)} user tables have tombstone metrics below threshold ({threshold}).\n"
                "====\n"
            )
            status_result = "success"
        
        structured_data["tombstone_metrics"] = {
            "status": status_result,
            "data": user_tables,
            "high_tombstone_count": len(high_tombstone_tables),
            "threshold_used": threshold
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nTombstone metrics check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["tombstone_metrics"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
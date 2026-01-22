from plugins.cassandra.utils.qrylib.qry_tombstone_metrics import get_tombstone_metrics_query
from plugins.common.check_helpers import require_ssh, format_check_header, format_recommendations, safe_execute_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - tombstones impact read performance


def run_tombstone_metrics(connector, settings):
    """
    Performs the health check analysis for tombstone metrics.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Tombstone Metrics Analysis (Nodetool)",
        "Checking for high tombstone counts across tables using nodetool cfstats or tablestats.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["check_result"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Determine version for parsing context
    major_version = getattr(connector, 'version_info', {}).get('major_version', 3) if hasattr(connector, 'version_info') else 3
    use_tablestats = major_version >= 3
    
    # Get appropriate query
    query = get_tombstone_metrics_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Tombstone metrics (nodetool)")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["tombstone_metrics"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Parse results - assuming list of dicts for tables
    tables = raw if isinstance(raw, list) else []
    
    if not tables:
        adoc_content.append("[NOTE]\n====\nNo table data returned.\n====\n")
        structured_data["tombstone_metrics"] = {"status": "success", "data": []}
        return "\n".join(adoc_content), structured_data
    
    # Identify problematic tables
    problematic_tables = []
    for table in tables:
        mean_tombstones = table.get('mean_tombstones', 0)
        max_tombstones_per_slice = table.get('max_tombstones_per_slice', 0)
        if mean_tombstones > 1000 or max_tombstones_per_slice > 100000:
            problematic_tables.append({
                'keyspace': table.get('keyspace', 'unknown'),
                'table': table.get('table', table.get('cfname', 'unknown')),
                'mean_tombstones': mean_tombstones,
                'max_tombstones_per_slice': max_tombstones_per_slice
            })
    
    # Generate report
    if problematic_tables:
        adoc_content.append("[WARNING]\n====\n")
        adoc_content.append(f"**{len(problematic_tables)} table(s)** with high tombstone counts detected.")
        adoc_content.append("High tombstones can cause read performance degradation and memory issues.")
        adoc_content.append("====\n")
        
        # Concise AsciiDoc table for problematic tables only
        adoc_content.append("==== Problematic Tables")
        adoc_content.append("|===\n|Keyspace|Table|Mean Tombstones|Max per Slice")
        for pt in problematic_tables:
            adoc_content.append(f"|{pt['keyspace']}|{pt['table']}|{pt['mean_tombstones']}|{pt['max_tombstones_per_slice']}")
        adoc_content.append("|===\n")
        
        adoc_content.append(formatted)
        
        recommendations = [
            "Consider enabling tombstone_compaction_interval for affected tables",
            "Review application delete patterns to reduce unnecessary tombstones",
            "Run targeted compaction: 'nodetool compact keyspace table'",
            "Monitor regularly with 'nodetool tablestats' or, if your version is older than 3, 'cfstats'",
            "For time-series data, evaluate TimeWindowCompactionStrategy (TWCS)"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        
        status_result = "warning"
    else:
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append(f"All {len(tables)} tables have acceptable tombstone levels.")
        adoc_content.append("====\n")
        adoc_content.append(formatted)
        status_result = "success"
    
    structured_data["tombstone_metrics"] = {
        "status": status_result,
        "data": tables,
        "problematic_count": len(problematic_tables),
        "use_tablestats": use_tablestats
    }
    
    return "\n".join(adoc_content), structured_data
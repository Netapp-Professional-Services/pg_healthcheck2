from plugins.cassandra.utils.qrylib.qry_gc_grace_seconds_audit import get_gc_grace_seconds_query
from plugins.cassandra.utils.keyspace_filter import filter_tables_by_keyspace
from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query, format_data_as_table


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High - impacts tombstone cleanup and storage


def run_gc_grace_seconds_audit(connector, settings):
    """
    Performs the health check analysis.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "GC Grace Seconds Audit",
        "Scanning all tables for inappropriate gc_grace_seconds settings ( > 3 days or 0 )."
    )
    structured_data = {}
    
    try:
        query = get_gc_grace_seconds_query(connector)
        success, formatted, raw = safe_execute_query(connector, query, "GC grace seconds query")
        
        if not success:
            adoc_content.append(formatted)
            structured_data["gc_grace_seconds"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces using centralized filter
        user_tables = filter_tables_by_keyspace(raw, settings)
        
        if not user_tables:
            adoc_content.append("[NOTE]\n====\nNo user tables found.\n====\n")
            structured_data["gc_grace_seconds"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Threshold: 3 days in seconds
        threshold = 3 * 24 * 3600  # 259200 seconds
        problematic_tables = []
        for table in user_tables:
            ks = table.get('keyspace_name', '')
            tbl = table.get('table_name', '')
            gc_grace = table.get('gc_grace_seconds', 864000)  # Default 10 days if null
            if gc_grace > threshold or gc_grace == 0:
                problematic_tables.append({
                    'keyspace': ks,
                    'table': tbl,
                    'gc_grace_seconds': gc_grace
                })

        # Format filtered data for display (only user tables)
        filtered_table = format_data_as_table(
            user_tables,
            columns=['keyspace_name', 'table_name', 'gc_grace_seconds']
        )
        adoc_content.append(filtered_table)
        
        if problematic_tables:
            adoc_content.append(
                f"[WARNING]\n====\n"
                f"**{len(problematic_tables)} table(s)** have gc_grace_seconds > 3 days or set to 0. "
                "This can lead to increased storage usage due to delayed tombstone cleanup and potential zombie reads.\n"
                "====\n"
            )
            
            # List problematic tables in a table
            adoc_content.append("\n==== Problematic Tables")
            adoc_content.append("|===\n|Keyspace|Table|GC Grace Seconds")
            for pt in problematic_tables:
                adoc_content.append(f"|{pt['keyspace']}|{pt['table']}|{pt['gc_grace_seconds']}s")
            adoc_content.append("|===\n")
            
            recommendations = [
                "Review and reduce gc_grace_seconds for affected tables to 1-2 days (86400-172800 seconds) unless specific retention needs exist.",
                "Execute: ALTER TABLE keyspace.table WITH gc_grace_seconds = 86400;",
                "After changes, monitor tombstone counts with 'nodetool tablestats' and consider running 'nodetool cleanup' if needed.",
                "For tables with gc_grace_seconds=0, set an appropriate value to enable tombstone expiration."
            ]
            adoc_content.extend(format_recommendations(recommendations))
            
            status_result = "warning"
        else:
            adoc_content.append(
                "[NOTE]\n====\n"
                f"All {len(user_tables)} user table(s) have appropriate gc_grace_seconds (<= 3 days and > 0).\n"
                "====\n"
            )
            status_result = "success"
        
        structured_data["gc_grace_seconds"] = {
            "status": status_result,
            "data": user_tables,
            "problematic_tables": problematic_tables,
            "problematic_count": len(problematic_tables)
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nGC grace seconds audit failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["gc_grace_seconds"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data

from plugins.cassandra.utils.qrylib.qry_row_cache import get_row_cache_query
from plugins.cassandra.utils.keyspace_filter import filter_tables_by_keyspace

from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query, format_data_as_table


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High - configuration best practice


def run_row_cache_check(connector, settings):
    """
    Analyzes row cache settings for all tables.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Row Cache Analysis",
        "Checking row cache settings across all user tables. Row cache is often discouraged in production due to memory concerns."
    )
    structured_data = {}
    
    try:
        query = get_row_cache_query(connector)
        success, formatted, raw = safe_execute_query(connector, query, "Row cache query")
        
        if not success:
            adoc_content.append(formatted)
            structured_data["row_cache"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces using centralized filter
        user_tables = filter_tables_by_keyspace(raw, settings)
        
        if not user_tables:
            adoc_content.append("[NOTE]\n====\nNo user tables found.\n====\n")
            structured_data["row_cache"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Find tables with row cache enabled
        tables_with_row_cache = []
        for table in user_tables:
            caching = table.get('caching', {})
            rows_per_partition = caching.get('rows_per_partition', 'NONE')
            # Row cache is disabled when rows_per_partition is 'NONE', 'None', or None
            if rows_per_partition not in ['NONE', 'None', None]:
                tables_with_row_cache.append({
                    'keyspace_name': table['keyspace_name'],
                    'table_name': table['table_name'],
                    'caching': caching
                })
        
        # Format filtered data for display (only user tables)
        filtered_table = format_data_as_table(
            user_tables,
            columns=['keyspace_name', 'table_name', 'caching']
        )

        if tables_with_row_cache:
            adoc_content.append(
                f"[WARNING]\n====\n"
                f"**{len(tables_with_row_cache)} table(s)** have row cache enabled. "
                "This can lead to high memory usage and is discouraged for production workloads with large partitions.\n"
                "====\n"
            )
            adoc_content.append(filtered_table)
            
            # List affected tables
            adoc_content.append("\n==== Tables with Row Cache Enabled")
            adoc_content.append("|===\n|Keyspace|Table|Caching Settings")
            for table in tables_with_row_cache:
                caching_str = ', '.join([f"{k}: {v}" for k, v in table['caching'].items()])
                adoc_content.append(f"|{table['keyspace_name']}|{table['table_name']}|{caching_str}")
            adoc_content.append("|===\n")
            
            recommendations = [
                "Review tables with row cache: ALTER TABLE keyspace.table WITH caching = {'keys': 'All', 'rows_per_partition': 'NONE'};",
                "Monitor memory usage after changes, as disabling row cache may increase read latency for hot partitions",
                "Consider application-level caching (e.g., Redis) for frequently accessed data instead of row cache",
                "Test changes in staging environment before production deployment"
            ]
            adoc_content.extend(format_recommendations(recommendations))
            
            status_result = "warning"
        else:
            adoc_content.append(
                "[NOTE]\n====\n"
                f"All {len(user_tables)} user table(s) have row cache disabled.\n"
                "====\n"
            )
            adoc_content.append(filtered_table)
            status_result = "success"
        
        structured_data["row_cache"] = {
            "status": status_result,
            "data": user_tables,
            "tables_with_row_cache": len(tables_with_row_cache),
            "affected_tables": tables_with_row_cache
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nRow cache check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["row_cache"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
from plugins.cassandra.utils.qrylib.qry_partition_size import get_partition_size_query
from plugins.cassandra.utils.keyspace_filter import filter_tables_by_keyspace
from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query, format_data_as_table

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - large partitions impact performance

def run_partition_size_check(connector, settings):
    """
    Performs the health check analysis.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Partition Size Analysis",
        "Checking for large partitions in table_metrics where max_partition_size exceeds 100MB (104857600 bytes)."
    )
    structured_data = {}
    
    # Check for Cassandra version before running a version-specific query
    if hasattr(connector, 'version_info') and connector.version_info.get('major_version', 0) >= 4:
        try:
            query = get_partition_size_query(connector)
            success, formatted, raw = safe_execute_query(connector, query, "Partition size query")
            
            if not success:
                adoc_content.append(formatted)
                structured_data["partition_sizes"] = {"status": "error", "data": raw}
                return "\n".join(adoc_content), structured_data
            
            # Filter out system keyspaces using centralized filter
            user_tables = filter_tables_by_keyspace(raw, settings)
            
            if not user_tables:
                adoc_content.append("[NOTE]\n====\nNo user tables found.\n====\n")
                structured_data["partition_sizes"] = {"status": "success", "data": []}
                return "\n".join(adoc_content), structured_data
            
            threshold_bytes = 100 * 1024 * 1024  # 100MB
            large_partitions = [t for t in user_tables
                                if t.get('max_partition_size', 0) > threshold_bytes]

            # Format filtered data for display (only user tables)
            filtered_table = format_data_as_table(
                user_tables,
                columns=['keyspace_name', 'table_name', 'max_partition_size']
            )

            if large_partitions:
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"**{len(large_partitions)} table(s)** with max_partition_size > 100MB detected. "
                    "Large partitions can cause performance degradation, increased memory usage, and compaction issues.\n"
                    "====\n"
                )
                adoc_content.append(filtered_table)

                recommendations = [
                    "Investigate tables with large partitions: review data model for wide partitions",
                    "Consider denormalizing or refactoring queries to avoid large partitions",
                    "Monitor partition sizes regularly and set alerts for growth",
                    "For affected tables, consider using SSTable tools to analyze partition distribution"
                ]
                adoc_content.extend(format_recommendations(recommendations))

                status_result = "warning"
            else:
                adoc_content.append(
                    "[NOTE]\n====\n"
                    f"All {len(user_tables)} user table(s) have max_partition_size <= 100MB.\n"
                    "====\n"
                )
                adoc_content.append(filtered_table)
                status_result = "success"
            
            structured_data["partition_sizes"] = {
                "status": status_result,
                "data": user_tables,
                "large_partition_count": len(large_partitions),
                "threshold_bytes": threshold_bytes
            }
            
        except Exception as e:
            error_msg = f"[ERROR]\n====\nPartition size check failed: {str(e)}\n====\n"
            adoc_content.append(error_msg)
            structured_data["partition_sizes"] = {"status": "error", "details": str(e)}
    else:
        adoc_content.append("[NOTE]\n====\nPartition size check is skipped (requires Cassandra 4.0+).\n====\n")
        structured_data["partition_sizes"] = {"status": "skipped", "reason": "version"}
    
    return "\n".join(adoc_content), structured_data
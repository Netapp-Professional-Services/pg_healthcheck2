from plugins.cassandra.utils.qrylib.qry_table_compression_settings import get_table_compression_query

from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 5  # Medium: Performance optimization recommendation


def run_table_compression_settings(connector, settings):
    """
    Analyzes table compression settings using CQL.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Table Compression Settings Analysis",
        "Checking compression configuration for all user tables in system_schema.tables."
    )
    structured_data = {}
    
    try:
        query = get_table_compression_query(connector)
        success, formatted, raw = safe_execute_query(connector, query, "Table compression query")
        
        if not success:
            adoc_content.append(formatted)
            structured_data["compression"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces in Python
        system_keyspaces = {'system', 'system_schema', 'system_traces', 
                           'system_auth', 'system_distributed', 'system_views'}
        user_tables = [t for t in raw 
                       if t.get('keyspace_name') not in system_keyspaces]
        
        if not user_tables:
            adoc_content.append("[NOTE]\n====\nNo user tables found.\n====\n")
            structured_data["compression"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Analyze compression settings
        tables_without_compression = []
        tables_not_lz4 = []
        for table in user_tables:
            compression = table.get('compression', {})
            keyspace = table['keyspace_name']
            table_name = table['table_name']

            if not compression or 'class' not in compression:
                tables_without_compression.append(f"{keyspace}.{table_name}")
            elif 'LZ4Compressor' not in compression.get('class', ''):
                tables_not_lz4.append(f"{keyspace}.{table_name} ({compression.get('class')})")
        
        # Report results
        adoc_content.append(formatted)
        
        issues_found = False
        if tables_without_compression:
            adoc_content.append(
                f"[WARNING]\n====\n"
                f"**{len(tables_without_compression)} table(s)** without compression enabled. "
                f"Compression improves storage efficiency and read performance.\n"
                "====\n"
            )
            adoc_content.append("\n==== Tables Without Compression")
            adoc_content.append("|===")
            adoc_content.append("|Table")
            for tbl in tables_without_compression:
                adoc_content.append(f"|{tbl}")
            adoc_content.append("|===\n")
            issues_found = True
        
        if tables_not_lz4:
            adoc_content.append(
                f"[WARNING]\n====\n"
                f"**{len(tables_not_lz4)} table(s)** using non-preferred compressor. "
                f"LZ4Compressor is recommended for optimal balance of speed and compression ratio.\n"
                "====\n"
            )
            adoc_content.append("\n==== Tables Using Non-LZ4 Compressor")
            adoc_content.append("|===")
            adoc_content.append("|Table (Compressor)")
            for tbl in tables_not_lz4:
                adoc_content.append(f"|{tbl}")
            adoc_content.append("|===\n")
            issues_found = True
        
        if not issues_found:
            adoc_content.append(
                "[NOTE]\n====\n"
                f"All {len(user_tables)} user table(s) have appropriate compression settings (LZ4Compressor enabled).\n"
                "====\n"
            )
            status_result = "success"
        else:
            status_result = "warning"
            
            recommendations = [
                "For tables without compression: ALTER TABLE keyspace.table WITH compression = {'class': 'LZ4Compressor'};",
                "For non-LZ4 tables: ALTER TABLE keyspace.table WITH compression = {'class': 'LZ4Compressor'};",
                "Consider chunk_length_in_kb: 64KB and crc_check_chance: 0.0 for production",
                "After altering, monitor compaction activity with 'nodetool compactionstats'",
                "LZ4 provides good CPU efficiency; Snappy is faster but less compression"
            ]
            adoc_content.extend(format_recommendations(recommendations))
        
        structured_data["compression"] = {
            "status": status_result,
            "data": user_tables,
            "tables_without_compression": len(tables_without_compression),
            "tables_not_lz4": len(tables_not_lz4),
            "total_user_tables": len(user_tables)
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCompression check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["compression"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
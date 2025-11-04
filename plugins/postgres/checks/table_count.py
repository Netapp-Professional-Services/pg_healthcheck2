from plugins.postgres.utils.qrylib.table_count_query import get_table_count_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 2  # Low: Informational check


def run_table_count(connector, settings):
    """
    Performs the table count health check analysis.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = []
    structured_data = {}
    
    adoc_content.append("=== Table Count Check")
    adoc_content.append("")
    
    try:
        query = get_table_count_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["table_count"] = {"status": "error", "data": raw}
        else:
            count = raw[0].get('count', 0) if raw else 0
            threshold = settings.get('table_count_threshold', 1000)
            
            adoc_content.append(f"The database currently has {count} user tables.")
            adoc_content.append("")
            
            if count > threshold:
                adoc_content.append("[WARNING]")
                adoc_content.append("====")
                adoc_content.append(f"High number of tables ({count}) exceeds the threshold of {threshold}. This may impact performance. Consider partitioning, archiving, or schema optimization.")
                adoc_content.append("====")
                adoc_content.append("")
                status = "warning"
            else:
                adoc_content.append("[NOTE]")
                adoc_content.append("====")
                adoc_content.append("Table count is within normal limits.")
                adoc_content.append("====")
                adoc_content.append("")
                status = "success"
            
            structured_data["table_count"] = {
                "status": status,
                "data": count,
                "threshold": threshold
            }
            
            adoc_content.append("==== Recommendations")
            adoc_content.append("[TIP]")
            adoc_content.append("====")
            adoc_content.append("* **Best Practice:** Regularly review table growth and implement archiving strategies for large datasets.")
            adoc_content.append("* **Remediation:** If count is high, investigate unused tables and consider dropping or partitioning them.")
            adoc_content.append("* **Monitoring:** Track table count over time to detect unusual growth patterns.")
            adoc_content.append("====")
            
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["table_count"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
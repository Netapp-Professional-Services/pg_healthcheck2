from plugins.postgres.utils.qrylib.table_count import get_table_count_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 2  # Low: Informational check


def run_table_count_check(connector, settings):
    """
    Performs the health check for table count.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = []
    structured_data = {}
    
    adoc_content.append("=== Table Count")
    adoc_content.append("")
    
    try:
        query = get_table_count_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["table_count"] = {"status": "error", "data": raw}
        else:
            count = raw[0].get('table_count', 0) if raw else 0
            threshold = settings.get('table_warning_threshold', 1000)
            
            adoc_content.append("==== Current Table Count")
            adoc_content.append("")
            
            if count > threshold:
                adoc_content.append("[WARNING]")
                adoc_content.append("====")
                adoc_content.append(f"**Action Required:** High number of tables detected ({count}). Consider schema optimization to improve performance.")
                adoc_content.append("====")
                adoc_content.append("")
            else:
                adoc_content.append("[NOTE]")
                adoc_content.append("====")
                adoc_content.append(f"Number of user tables: {count}")
                adoc_content.append("====")
                adoc_content.append("")
            
            adoc_content.append(formatted)
            
            adoc_content.append("\n==== Recommendations")
            adoc_content.append("[TIP]")
            adoc_content.append("====")
            adoc_content.append("* **Best Practice:** Regularly review table counts and archive or partition large schemas.")
            adoc_content.append("* **Remediation:** If count is high, investigate unused tables with pg_stat_user_tables.")
            adoc_content.append("* **Monitoring:** Set alerts for table count exceeding organizational thresholds.")
            adoc_content.append("====")
            
            structured_data["table_count"] = {"status": "success", "data": {"count": count}, "threshold": threshold}
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["table_count"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
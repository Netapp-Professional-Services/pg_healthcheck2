from plugins.kafka.utils.qrylib.topic_details_queries import get_topic_details_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 10  # Critical: Data loss risk


def run_under_replicated_partitions(connector, settings):
    """
    Performs the health check for under-replicated partitions.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = [
        "=== Under-Replicated Partitions",
        ""
    ]
    structured_data = {}
    
    try:
        query = get_topic_details_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            # Query execution failed
            adoc_content.append(formatted)
            structured_data["under_replicated"] = {"status": "error", "data": raw}
        elif not raw or all(t.get('under_replicated_partitions', 0) == 0 for t in raw):
            # No issues detected (healthy state)
            adoc_content.append("[NOTE]\n====\nAll partitions are fully replicated. No data loss risk detected.\n====\n")
            structured_data["under_replicated"] = {"status": "success", "data": []}
        else:
            # Issues found - provide critical warning and data
            under_replicated_count = sum(t.get('under_replicated_partitions', 0) for t in raw)
            adoc_content.append("[CRITICAL]\n====\n**Immediate Action Required:** {under_replicated_count} under-replicated partitions detected across topics. This poses a direct risk of data loss if a broker fails.\n====\n")
            adoc_content.append(formatted)
            structured_data["under_replicated"] = {"status": "success", "data": raw, "count": under_replicated_count}
            
            # Recommendations
            adoc_content.append("\n==== Recommendations")
            adoc_content.append("[TIP]\n====\n"
                                "* **Immediate Investigation:** Check broker status and logs for failures or network issues.\n"
                                "* **Remediation:** Ensure all brokers are healthy and replication is caught up. Restart failed brokers if necessary.\n"
                                "* **Prevention:** Monitor broker health proactively and maintain at least 3 replicas for fault tolerance.\n"
                                "====\n")
            
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["under_replicated"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data

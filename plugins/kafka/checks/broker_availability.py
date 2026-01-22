from plugins.kafka.utils.qrylib.cluster_metadata_queries import get_cluster_metadata_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8


def run_broker_availability(connector, settings):
    """
    Performs the health check analysis for broker availability and cluster connectivity.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = [
        "=== Broker Availability and Cluster Connectivity",
        ""
    ]
    structured_data = {}
    
    try:
        query = get_cluster_metadata_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            # Query execution failed
            adoc_content.append(formatted)
            structured_data["broker_availability"] = {"status": "error", "data": raw}
        else:
            brokers = raw.get('brokers', [])
            broker_count = len(brokers)
            expected_count = settings.get('expected_broker_count', 3)
            
            if broker_count == 0:
                adoc_content.append("[CRITICAL]\n====\n**Cluster Unreachable:** No active brokers detected. This indicates severe connectivity issues or complete cluster outage.\n====\n")
            elif broker_count < expected_count:
                adoc_content.append(f"[WARNING]\n====\n**Potential Outage:** Only {broker_count} active brokers found, expected at least {expected_count}. Investigate broker status and network connectivity.\n====\n")
            else:
                adoc_content.append(f"[NOTE]\n====\nCluster connectivity is healthy with {broker_count} active brokers.\n====\n")
            
            adoc_content.append(formatted)
            structured_data["broker_availability"] = {
                "status": "success",
                "data": raw,
                "broker_count": broker_count
            }
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["broker_availability"] = {"status": "error", "details": str(e)}
    
    adoc_content.append("\n==== Recommendations")
    adoc_content.append("[TIP]\n====\n"
                        "* **Broker Monitoring:** Check broker logs and JMX metrics for errors or restarts.\n"
                        "* **Network Verification:** Ensure all brokers can communicate via the advertised listeners.\n"
                        "* **Configuration:** Set 'expected_broker_count' in settings to match your cluster size.\n"
                        "====\n")
    
    return "\n".join(adoc_content), structured_data
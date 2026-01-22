from plugins.kafka.utils.qrylib.consumer_lag_queries import get_all_consumer_lag_query

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8

def run_consumer_lag(connector, settings):
    """
    Performs the health check analysis for consumer lag.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = ["=== Consumer Lag Analysis", ""]
    structured_data = {}
    
    warning_lag = settings.get('kafka_lag_warning', 1000)
    critical_lag = settings.get('kafka_lag_critical', 10000)
    
    try:
        query = get_all_consumer_lag_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["consumer_lag"] = {"status": "error", "data": raw}
        else:
            lags = raw.get('groups', []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
            
            high_critical = [lag for lag in lags if int(lag.get('lag', 0)) > critical_lag]
            high_warning = [lag for lag in lags if critical_lag >= int(lag.get('lag', 0)) > warning_lag]
            
            if high_critical:
                adoc_content.append("[CRITICAL]\n====\n"
                                  f"**Critical Lag Detected:** {len(high_critical)} "
                                  f"partitions have lag exceeding {critical_lag}\n"
                                  "====\n")
            elif high_warning:
                adoc_content.append("[WARNING]\n====\n"
                                  f"**High Lag Detected:** {len(high_warning)} "
                                  f"partitions have lag exceeding {warning_lag}\n"
                                  "====\n")
            else:
                adoc_content.append("[NOTE]\n====\n"
                                  "No significant consumer lag detected. System is healthy.\n"
                                  "====\n")
            
            adoc_content.append(formatted)
            structured_data["consumer_lag"] = {
                "status": "success",
                "data": lags,
                "critical_count": len(high_critical),
                "warning_count": len(high_warning)
            }
            
            if high_critical or high_warning:
                adoc_content.append("\n==== Recommendations")
                adoc_content.append("[TIP]\n====\n"
                                  "* **Best Practice:** Scale consumers if lag persists.\n"
                                  "* **Remediation:** Investigate stalled consumers and rebalancing.\n"
                                  "* **Monitoring:** Track lag trends over time.\n"
                                  "====\n")
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["consumer_lag"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
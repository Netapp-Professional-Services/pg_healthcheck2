from plugins.kafka.utils.qrylib.consumer_lag_queries import get_all_consumer_lag_query

def get_weight():
    return 8

def run_consumer_lag(connector, settings):
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
        elif raw and raw.get('group_lags'):
            critical_items = [item for item in raw['group_lags'] if item.get('lag', 0) > critical_lag]
            warning_items = [item for item in raw['group_lags'] if warning_lag < item.get('lag', 0) <= critical_lag]
            
            if critical_items:
                adoc_content.append("[CRITICAL]\n====\n**Critical Lag Detected:** {len(critical_items)} partitions have lag exceeding {critical_lag} messages.\n====\n")
            elif warning_items:
                adoc_content.append("[WARNING]\n====\n**High Lag Detected:** {len(warning_items)} partitions have lag exceeding {warning_lag} messages.\n====\n")
            else:
                adoc_content.append("[NOTE]\n====\nNo significant consumer lag detected.\n====\n")
            
            adoc_content.append(formatted)
            structured_data["consumer_lag"] = {"status": "success", "data": raw['group_lags'], "count": len(raw['group_lags'])}
            
            if critical_items or warning_items:
                adoc_content.append("\n==== Recommendations")
                adoc_content.append("[TIP]\n====\n* **Best Practice:** Monitor consumer performance and scale consumers if needed.\n* **Remediation:** Investigate consumer health and restart stalled consumers.\n* **Monitoring:** Track lag trends over time.\n====\n")
        else:
            adoc_content.append("[NOTE]\n====\nNo consumer groups or lag data available.\n====\n")
            structured_data["consumer_lag"] = {"status": "success", "data": []}
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["consumer_lag"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data

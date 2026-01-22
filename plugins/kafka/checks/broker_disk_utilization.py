from plugins.kafka.utils.qrylib.log_dirs_queries import get_log_dirs_query

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High: resource exhaustion risk


def run_broker_disk_utilization(connector, settings):
    """
    Performs the health check for Kafka broker disk utilization.
    
    Args:
        connector: Kafka connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = ["=== Broker Disk Utilization", ""]
    structured_data = {}
    
    try:
        query = get_log_dirs_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            # Query execution failed
            adoc_content.append(formatted)
            structured_data["disk_usage"] = {"status": "error", "data": raw}
        else:
            all_dirs = []
            log_dirs = raw.get('log_dirs', []) if raw else []
            for logdir in log_dirs:
                total = logdir.get('total', 0)
                usable = logdir.get('usable', 0)
                if total > 0:
                    used = total - usable
                    percent = (used / total) * 100
                    all_dirs.append({
                        'broker_id': logdir.get('broker_id', 'unknown'),
                        'log_dir': logdir.get('dir', 'unknown'),
                        'total_bytes': total,
                        'used_bytes': used,
                        'remaining_bytes': usable,
                        'utilization_percent': round(percent, 2)
                    })
            
            if not all_dirs:
                # No data (healthy or no access)
                adoc_content.append("[NOTE]\n====\nNo log directories data available. Ensure brokers are accessible and log dirs are configured.\n====\n")
                structured_data["disk_usage"] = {"status": "success", "data": []}
            else:
                # Analyze based on thresholds
                warning_percent = settings.get('kafka_disk_warning_percent', 80.0)
                critical_percent = settings.get('kafka_disk_critical_percent', 95.0)
                
                critical_dirs = [d for d in all_dirs if d['utilization_percent'] > critical_percent]
                warning_dirs = [d for d in all_dirs if warning_percent < d['utilization_percent'] <= critical_percent]
                
                if critical_dirs:
                    adoc_content.append(f"[CRITICAL]\n====\n**Storage Exhaustion Risk:** {len(critical_dirs)} log directories exceed {critical_percent}%% utilization. Immediate capacity expansion required to prevent service disruptions.\n====\n")
                elif warning_dirs:
                    adoc_content.append(f"[WARNING]\n====\n**High Storage Usage:** {len(warning_dirs)} log directories above {warning_percent}%% utilization. Plan for storage increase.\n====\n")
                else:
                    adoc_content.append("[NOTE]\n====\nAll log directories are below warning thresholds. System storage is healthy.\n====\n")
                
                # Add details table
                adoc_content.append("\n==== Log Directory Details")
                adoc_content.append("|===")
                adoc_content.append("| Broker ID | Log Directory | Total (GB) | Used (GB) | Utilization (%)")
                for d in all_dirs:
                    total_gb = round(d['total_bytes'] / (1024 ** 3), 2)
                    used_gb = round(d['used_bytes'] / (1024 ** 3), 2)
                    percent = d['utilization_percent']
                    broker_id = d['broker_id']
                    log_dir = d['log_dir']
                    adoc_content.append(f"| {broker_id} | {log_dir} | {total_gb} | {used_gb} | {percent}")
                adoc_content.append("|===\n")
                
                # Recommendations if issues found
                if critical_dirs or warning_dirs:
                    adoc_content.append("==== Recommendations")
                    adoc_content.append("[TIP]\n====\n* Increase disk capacity on affected brokers or migrate to larger volumes.\n* Tune log retention policies (e.g., decrease log.retention.bytes or log.retention.hours) for high-volume topics.\n* Implement monitoring for storage trends and automate alerts at 70% utilization.\n====\n")
                
                structured_data["disk_usage"] = {
                    "status": "success",
                    "data": all_dirs,
                    "critical_count": len(critical_dirs),
                    "warning_count": len(warning_dirs),
                    "total_dirs": len(all_dirs)
                }
    except Exception as e:
        error_msg = f"[ERROR]\n====\nDisk utilization check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["disk_usage"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
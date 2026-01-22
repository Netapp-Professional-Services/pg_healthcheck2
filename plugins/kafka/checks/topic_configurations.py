from plugins.kafka.utils.qrylib.describe_topics_queries import get_describe_topics_query
from plugins.kafka.utils.qrylib.topic_config_queries import get_topic_config_query

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8

def run_topic_configurations(connector, settings):
    """
    Performs the health check analysis for topic configurations.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = [
        "=== Topic Configuration Analysis",
        ""
    ]
    structured_data = {}
    
    try:
        # Get topic details for all topics
        describe_query = get_describe_topics_query(connector)
        formatted_desc, raw_describe = connector.execute_query(describe_query, return_raw=True)
        
        if "[ERROR]" in formatted_desc:
            adoc_content.append(formatted_desc)
            structured_data["describe"] = {"status": "error", "data": raw_describe}
        else:
            topics_details = raw_describe if raw_describe else []
            topic_map = {td.get('topic'): td for td in topics_details}
            all_topics = list(topic_map.keys())
            
            # Settings-based thresholds
            min_rep = settings.get('min_replication_factor', 3)
            max_parts = settings.get('max_partitions_per_topic', 200)
            min_ret_h = settings.get('min_retention_hours', 168)
            
            configs = {}
            issues_count = 0
            topic_data = []
            
            for topic in all_topics:
                if topic.startswith('__'):
                    continue  # Skip internal topics
                
                detail = topic_map.get(topic, {})
                rep_factor = detail.get('replication_factor', 1)
                parts = detail.get('partitions', 0)
                under_rep = detail.get('under_replicated_partitions', 0)
                
                # Get topic config
                config_query = get_topic_config_query(connector, topic_name=topic)
                formatted_config, raw_config = connector.execute_query(config_query, return_raw=True)
                
                topic_configs = {}
                if "[ERROR]" not in formatted_config and isinstance(raw_config, dict):
                    topic_configs = raw_config.get('configs', {})
                configs[topic] = topic_configs
                
                retention_ms_str = topic_configs.get('retention.ms', '604800000')  # Default 7 days
                try:
                    ret_ms = int(retention_ms_str) if retention_ms_str != '-1' else float('inf')
                except ValueError:
                    ret_ms = 0
                ret_hours = ret_ms / 3600000 if ret_ms != float('inf') else float('inf')
                
                topic_info = {
                    'topic': topic,
                    'replication_factor': rep_factor,
                    'partitions': parts,
                    'under_replicated_partitions': under_rep,
                    'retention_ms': retention_ms_str,
                    'retention_hours': ret_hours
                }
                topic_data.append(topic_info)
                
                # Generate warnings
                if under_rep > 0:
                    issues_count += 1
                    adoc_content.append(f"[CRITICAL]\n====\nUnder-replicated partitions in topic '{topic}': {under_rep}. Immediate action required to restore replication.\n====\n")
                elif rep_factor < min_rep:
                    issues_count += 1
                    adoc_content.append(f"[WARNING]\n====\nLow replication factor for '{topic}': {rep_factor} < {min_rep}. Risk of data loss.\n====\n")
                
                if parts > max_parts:
                    issues_count += 1
                    adoc_content.append(f"[WARNING]\n====\nExcessive partitions for '{topic}': {parts} > {max_parts}. May impact performance.\n====\n")
                
                if ret_hours < min_ret_h and ret_hours != float('inf'):
                    issues_count += 1
                    adoc_content.append(f"[WARNING]\n====\nInsufficient retention for '{topic}': {ret_hours:.1f}h < {min_ret_h}h. Data may expire too soon.\n====\n")
            
            if issues_count == 0:
                adoc_content.append("[NOTE]\n====\nAll user topics have proper configurations for performance and durability.\n====\n")
            
            adoc_content.append(formatted_desc)
            
            if issues_count > 0:
                adoc_content.append("\n==== Recommendations")
                adoc_content.append("[TIP]\n====\n"
                                  "* Use `kafka-topics.sh --alter` or `kafka-configs.sh --alter` to fix configurations.\n"
                                  "* Ensure replication factor >= 3 and retention >= 7 days for production.\n"
                                  "* Balance partitions based on throughput and consumer parallelism.\n"
                                  "* Regularly audit topic configs for security (e.g., enable compression if needed).\n"
                                  "====\n")
            
            structured_data["topics"] = {
                "status": "success",
                "data": topic_data,
                "issue_count": issues_count,
                "total_topics": len(topic_data)
            }
    except Exception as e:
        error_msg = f"[ERROR]\n====\nTopic configuration analysis failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["topics"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data

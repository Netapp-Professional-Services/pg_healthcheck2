from plugins.common.check_helpers import CheckContentBuilder
from plugins.kafka.utils.qrylib.isr_queries import get_describe_topics_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 9  # Critical for data consistency and replication health


def run_check_isr_health(connector, settings):
    """
    Performs the ISR health check analysis.
    
    Analyzes In-Sync Replica (ISR) status at the topic level to identify:
    - Topics with under-replicated partitions
    - Percentage of partitions affected per topic
    - Overall cluster replication health
    
    Note: This check analyzes topic-level aggregates. Use partition-level
    checks for detailed partition analysis.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    try:
        builder.h3("In-Sync Replicas (ISR) Health Check")
        
        query = get_describe_topics_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            # Query execution failed
            builder.add(formatted)
            structured_data["isr_health"] = {"status": "error", "data": raw}
            return builder.build(), structured_data
        
        # describe_topics returns: [{'topic': 'name', 'partitions': N, 'replication_factor': N, 'under_replicated_partitions': N}, ...]
        if not isinstance(raw, list):
            builder.error(f"Unexpected data format from ISR query: {type(raw)}")
            structured_data["isr_health"] = {"status": "error", "details": "Invalid data format"}
            return builder.build(), structured_data
        
        if len(raw) == 0:
            builder.note("No topics found in cluster.")
            structured_data["isr_health"] = {
                "status": "success",
                "summary": {
                    "total_topics": 0,
                    "total_partitions": 0,
                    "under_replicated_partitions": 0,
                    "topics_with_issues": 0,
                    "has_critical_issues": False,
                    "has_warning_issues": False
                },
                "affected_topics": [],
                "all_topics": []
            }
            return builder.build(), structured_data
        
        # === COLLECT FACTS: Analyze topic-level ISR data ===
        topics_with_issues = []
        
        total_topics = len(raw)
        total_partitions = 0
        total_under_replicated = 0
        
        for topic_data in raw:
            topic = topic_data.get('topic')
            partition_count = topic_data.get('partitions', 0)
            replication_factor = topic_data.get('replication_factor', 0)
            under_rep_count = topic_data.get('under_replicated_partitions', 0)
            
            total_partitions += partition_count
            total_under_replicated += under_rep_count
            
            # Track topics with issues
            if under_rep_count > 0:
                percent_affected = round((under_rep_count / partition_count * 100), 1) if partition_count > 0 else 0
                topics_with_issues.append({
                    'topic': topic,
                    'total_partitions': partition_count,
                    'replication_factor': replication_factor,
                    'under_replicated_count': under_rep_count,
                    'percent_under_replicated': percent_affected
                })
        
        # === INTERPRET FACTS: Report issues ===
        # Categorize by severity
        critical_topics = [t for t in topics_with_issues if t['percent_under_replicated'] > 50]
        warning_topics = [t for t in topics_with_issues if t['percent_under_replicated'] <= 50]
        
        if critical_topics:
            builder.critical(
                f"**Critical Replication Issues:** {len(critical_topics)} topic(s) have more than 50% of partitions under-replicated. "
                f"This poses a severe risk to data durability and availability."
            )
        
        if warning_topics:
            builder.warning(
                f"**Under-Replicated Partitions Detected:** {len(warning_topics)} topic(s) have some partitions under-replicated. "
                f"Total: {total_under_replicated} under-replicated partitions across cluster."
            )
        
        # === DETAILED ANALYSIS SECTIONS ===
        
        if topics_with_issues:
            builder.h4("Affected Topics")
            
            # Sort by severity (percent affected, then count)
            sorted_issues = sorted(topics_with_issues, 
                                  key=lambda x: (x['percent_under_replicated'], x['under_replicated_count']), 
                                  reverse=True)
            
            topic_rows = []
            for topic_info in sorted_issues:
                # Status indicator based on percentage
                if topic_info['percent_under_replicated'] > 50:
                    indicator = "🔴"
                elif topic_info['percent_under_replicated'] > 25:
                    indicator = "⚠️"
                else:
                    indicator = "⚠️"
                
                topic_rows.append({
                    "Status": indicator,
                    "Topic": topic_info['topic'],
                    "Total Partitions": topic_info['total_partitions'],
                    "Under-Replicated": topic_info['under_replicated_count'],
                    "% Affected": f"{topic_info['percent_under_replicated']}%",
                    "RF": topic_info['replication_factor']
                })
            
            builder.table(topic_rows)
        
        # Cluster summary
        builder.h4("Cluster Summary")
        
        healthy_topics = total_topics - len(topics_with_issues)
        summary_data = {
            "Total Topics": total_topics,
            "Healthy Topics": healthy_topics,
            "Topics with Issues": len(topics_with_issues),
            "Total Partitions": total_partitions,
            "Under-Replicated Partitions": total_under_replicated,
            "Cluster Health": f"{round((healthy_topics / total_topics * 100), 1)}%" if total_topics > 0 else "N/A"
        }
        builder.dict_table(summary_data, "Metric", "Value")
        
        # Recommendations
        if topics_with_issues:
            recommendations = {}
            
            if critical_topics:
                recommendations["critical"] = [
                    "**Immediate investigation required** - Majority of partitions out of sync",
                    "**Check broker health** - Verify all brokers are running and accessible",
                    "**Review broker logs** - Look for replication errors or disk issues",
                    "**Do NOT restart affected brokers** until replicas are back in sync",
                    "**Verify network connectivity** - Check for network issues between brokers"
                ]
            
            if warning_topics:
                if "high" not in recommendations:
                    recommendations["high"] = []
                recommendations["high"].extend([
                    "**Monitor replication lag** - Use kafka.server:type=ReplicaManager metrics",
                    "**Check for slow brokers** - Review I/O metrics, CPU, and GC pauses",
                    "**Verify broker capacity** - Ensure brokers have adequate resources",
                    "**Review retention policies** - High retention can cause replication delays"
                ])
            
            recommendations["general"] = [
                "Ensure all Kafka brokers are operational and can communicate",
                "Verify network connectivity and resolve any latency issues between brokers",
                "Monitor broker logs for replication errors or disk-related problems",
                "For production environments, maintain a replication factor of at least 3",
                "Set up alerts for under-replicated partitions (alert immediately at count > 0)",
                "Document runbook for under-replication response procedures",
                "Regularly test failover scenarios to verify replication health"
            ]
            
            builder.recs(recommendations)
        else:
            # No issues found
            builder.success(
                "All partitions have healthy ISR status.\n\n"
                "No under-replicated partitions detected across the cluster."
            )
        
        # === STRUCTURED DATA ===
        structured_data["isr_health"] = {
            "status": "success",
            "summary": {
                "total_topics": total_topics,
                "total_partitions": total_partitions,
                "under_replicated_partitions": total_under_replicated,
                "topics_with_issues": len(topics_with_issues),
                "critical_topics": len(critical_topics),
                "warning_topics": len(warning_topics),
                "has_critical_issues": len(critical_topics) > 0,
                "has_warning_issues": len(warning_topics) > 0,
                "cluster_health_percent": round((healthy_topics / total_topics * 100), 1) if total_topics > 0 else 100
            },
            "affected_topics": topics_with_issues,
            "all_topics": raw
        }
    
    except Exception as e:
        import traceback
        from logging import getLogger
        logger = getLogger(__name__)
        logger.error(f"ISR health check failed: {e}\n{traceback.format_exc()}")
        
        builder.error(f"ISR health check failed: {e}")
        structured_data["isr_health"] = {"status": "error", "details": str(e)}
    
    return builder.build(), structured_data

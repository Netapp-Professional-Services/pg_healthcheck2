from plugins.common.check_helpers import CheckContentBuilder
from plugins.kafka.utils.qrylib.check_storage_health_queries import get_describe_log_dirs_query

def get_weight():
    return 8

def run_check_storage_health(connector, settings):
    """
    Analyzes storage health across Kafka brokers.
    
    Collects log directory information and analyzes:
    - Total storage per broker
    - Large partitions (data skew)
    - Storage threshold violations
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    # Separate thresholds for partitions vs brokers
    warning_partition_gb = settings.get('storage_warning_partition_gb', 10)
    critical_partition_gb = settings.get('storage_critical_partition_gb', 50)
    warning_broker_gb = settings.get('storage_warning_broker_gb', 100)
    critical_broker_gb = settings.get('storage_critical_broker_gb', 500)
    
    # Convert to bytes
    warning_partition_bytes = warning_partition_gb * 1024 * 1024 * 1024
    critical_partition_bytes = critical_partition_gb * 1024 * 1024 * 1024
    warning_broker_bytes = warning_broker_gb * 1024 * 1024 * 1024
    critical_broker_bytes = critical_broker_gb * 1024 * 1024 * 1024
    
    try:
        builder.h3("Storage Health Analysis")
        
        # Get log directory information (includes broker IDs)
        query = get_describe_log_dirs_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        # Check for errors
        if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
            builder.add(formatted)
            structured_data["storage_health"] = {"status": "error", "data": raw}
            return builder.build(), structured_data
        
        # Validate data structure
        if not raw or not isinstance(raw, list):
            builder.note("No storage usage data available.")
            structured_data["storage_health"] = {
                "status": "success",
                "data": [],
                "message": "No log directory data returned"
            }
            return builder.build(), structured_data
        
        if len(raw) == 0:
            builder.note("No partitions found in cluster.")
            structured_data["storage_health"] = {"status": "success", "data": []}
            return builder.build(), structured_data
        
        # === COLLECT FACTS: Aggregate per broker, topic, and track large partitions ===
        broker_stats = {}
        topic_stats = {}
        large_partitions = []
        all_partition_data = []
        
        for entry in raw:
            broker_id = entry.get('broker_id')
            topic = entry.get('topic')
            partition = entry.get('partition')
            size_bytes = entry.get('size_bytes', 0)
            size_mb = size_bytes / (1024 * 1024)
            size_gb = size_bytes / (1024 * 1024 * 1024)
            
            # Track all partition data for structured output
            all_partition_data.append({
                'broker_id': broker_id,
                'topic': topic,
                'partition': partition,
                'size_bytes': size_bytes,
                'size_mb': round(size_mb, 2),
                'size_gb': round(size_gb, 2)
            })
            
            # Initialize broker stats if needed
            if broker_id not in broker_stats:
                broker_stats[broker_id] = {
                    'broker_id': broker_id,
                    'total_size_bytes': 0,
                    'total_size_gb': 0,
                    'partition_count': 0,
                    'topic_count': 0,
                    'topics': set(),
                    'largest_partition_mb': 0,
                    'largest_partition_topic': None
                }
            
            # Initialize topic stats if needed
            if topic not in topic_stats:
                topic_stats[topic] = {
                    'topic': topic,
                    'total_size_bytes': 0,
                    'total_size_gb': 0,
                    'partition_count': 0,
                    'brokers': set()
                }
            
            # Update broker totals
            broker_stats[broker_id]['total_size_bytes'] += size_bytes
            broker_stats[broker_id]['partition_count'] += 1
            broker_stats[broker_id]['topics'].add(topic)
            
            # Update topic totals
            topic_stats[topic]['total_size_bytes'] += size_bytes
            topic_stats[topic]['partition_count'] += 1
            topic_stats[topic]['brokers'].add(broker_id)
            
            if size_mb > broker_stats[broker_id]['largest_partition_mb']:
                broker_stats[broker_id]['largest_partition_mb'] = size_mb
                broker_stats[broker_id]['largest_partition_topic'] = f"{topic}-{partition}"
            
            # Track oversized partitions
            if size_bytes > critical_partition_bytes:
                large_partitions.append({
                    'broker_id': broker_id,
                    'topic': topic,
                    'partition': partition,
                    'size_bytes': size_bytes,
                    'size_gb': round(size_gb, 2),
                    'exceeds_critical_threshold': True
                })
            elif size_bytes > warning_partition_bytes:
                large_partitions.append({
                    'broker_id': broker_id,
                    'topic': topic,
                    'partition': partition,
                    'size_bytes': size_bytes,
                    'size_gb': round(size_gb, 2),
                    'exceeds_warning_threshold': True
                })
        
        # Convert broker totals to GB and count topics
        for broker_id in broker_stats:
            broker_stats[broker_id]['total_size_gb'] = round(
                broker_stats[broker_id]['total_size_bytes'] / (1024 * 1024 * 1024), 2
            )
            broker_stats[broker_id]['topic_count'] = len(broker_stats[broker_id]['topics'])
            # Convert set to list for JSON serialization
            broker_stats[broker_id]['topics'] = sorted(list(broker_stats[broker_id]['topics']))
        
        # Convert topic totals to GB
        for topic in topic_stats:
            topic_stats[topic]['total_size_gb'] = round(
                topic_stats[topic]['total_size_bytes'] / (1024 * 1024 * 1024), 2
            )
            topic_stats[topic]['brokers'] = sorted(list(topic_stats[topic]['brokers']))
        
        # === INTERPRET FACTS: Analyze and report issues ===
        issues_found = False
        broker_critical = []
        broker_warning = []
        
        # Check broker-level storage
        for broker_id, stats in broker_stats.items():
            total_gb = stats['total_size_gb']
            
            if stats['total_size_bytes'] > critical_broker_bytes:
                issues_found = True
                broker_critical.append(broker_id)
                builder.critical_issue(
                    f"Critical Storage Usage - Broker {broker_id}",
                    {
                        "Total Storage": f"{total_gb} GB (threshold: {critical_broker_gb} GB)",
                        "Partitions": stats['partition_count'],
                        "Topics": stats['topic_count'],
                        "Largest Partition": f"{round(stats['largest_partition_mb'] / 1024, 2)} GB ({stats['largest_partition_topic']})"
                    }
                )
            
            elif stats['total_size_bytes'] > warning_broker_bytes:
                issues_found = True
                broker_warning.append(broker_id)
                builder.warning_issue(
                    f"High Storage Usage - Broker {broker_id}",
                    {
                        "Total Storage": f"{total_gb} GB (threshold: {warning_broker_gb} GB)",
                        "Partitions": stats['partition_count'],
                        "Topics": stats['topic_count'],
                        "Largest Partition": f"{round(stats['largest_partition_mb'] / 1024, 2)} GB ({stats['largest_partition_topic']})"
                    }
                )
        
        # Check for large partitions
        if large_partitions:
            issues_found = True
            critical_count = sum(1 for p in large_partitions if p.get('exceeds_critical_threshold'))
            warning_count = sum(1 for p in large_partitions if p.get('exceeds_warning_threshold'))
            
            warning_msg = "**Large Partitions Detected:**\n\n"
            if critical_count > 0:
                warning_msg += f"* {critical_count} partition(s) exceed {critical_partition_gb} GB (critical)\n"
            if warning_count > 0:
                warning_msg += f"* {warning_count} partition(s) exceed {warning_partition_gb} GB (warning)\n"
            warning_msg += "\nThis may indicate data skew or retention issues."
            
            builder.warning(warning_msg)
            
            # Show top 10 largest
            builder.h4("Top 10 Largest Partitions")
            top_partitions = sorted(large_partitions, key=lambda x: x['size_bytes'], reverse=True)[:10]
            
            partition_rows = []
            for lp in top_partitions:
                severity = "🔴" if lp.get('exceeds_critical_threshold') else "⚠️"
                partition_rows.append([
                    severity,
                    lp['broker_id'],
                    lp['topic'],
                    lp['partition'],
                    f"{lp['size_gb']} GB"
                ])
            
            builder.table([
                {"Status": row[0], "Broker": row[1], "Topic": row[2], "Partition": row[3], "Size": row[4]}
                for row in partition_rows
            ])
            
            if len(large_partitions) > 10:
                builder.para(f"_... and {len(large_partitions) - 10} more large partitions_")
        
        # Show broker storage summary (concise)
        builder.h4("Broker Storage Summary")
        broker_rows = []
        for broker_id, stats in sorted(broker_stats.items()):
            indicator = ""
            if stats['total_size_bytes'] > critical_broker_bytes:
                indicator = "🔴"
            elif stats['total_size_bytes'] > warning_broker_bytes:
                indicator = "⚠️"
            
            broker_rows.append({
                "Status": indicator if indicator else "✅",
                "Broker": broker_id,
                "Total Storage": f"{stats['total_size_gb']} GB",
                "Partitions": stats['partition_count'],
                "Topics": stats['topic_count'],
                "Largest Partition": f"{round(stats['largest_partition_mb'] / 1024, 2)} GB"
            })
        
        builder.table(broker_rows)
        
        # Show top topics by storage (filter out internal topics unless large)
        builder.h4("Top 10 Topics by Storage")
        
        # Filter and sort topics
        user_topics = {k: v for k, v in topic_stats.items() 
                      if not k.startswith('__') or v['total_size_gb'] > 1.0}
        top_topics = sorted(user_topics.values(), key=lambda x: x['total_size_bytes'], reverse=True)[:10]
        
        topic_rows = []
        for topic in top_topics:
            topic_rows.append({
                "Topic": topic['topic'],
                "Total Storage": f"{topic['total_size_gb']} GB",
                "Partitions": topic['partition_count'],
                "Brokers": len(topic['brokers']),
                "Avg per Partition": f"{round(topic['total_size_gb'] / topic['partition_count'], 2)} GB"
            })
        
        builder.table(topic_rows)
        
        total_topics = len(topic_stats)
        if total_topics > 10:
            builder.para(f"_Showing top 10 of {total_topics} total topics_")
        
        # Recommendations
        if issues_found:
            recommendations = {}
            
            if broker_critical or broker_warning:
                recommendations["high"] = [
                    "**Review and adjust topic retention policies** to prevent excessive growth",
                    "**Enable log compaction** for key-based topics to reduce storage",
                    "**Consider adding storage capacity** or rebalancing partitions",
                    "**Set up disk space alerts** at 70% capacity"
                ]
            
            if large_partitions:
                if "high" not in recommendations:
                    recommendations["high"] = []
                recommendations["high"].extend([
                    "**Increase partition count** for topics with data skew",
                    "**Review partitioning key strategy** for even distribution",
                    "**Adjust retention settings** for high-volume topics"
                ])
            
            recommendations["general"] = [
                "Track storage growth trends over time",
                "Monitor partition size distribution across the cluster",
                "Alert on rapid storage increases (>20% in 24h)",
                "Consider topic archival strategies for historical data",
                "Document capacity planning procedures"
            ]
            
            builder.recs(recommendations)
        else:
            builder.success(
                "Storage usage is within healthy limits across all brokers.\n\n"
                f"All brokers are below {warning_broker_gb} GB threshold."
            )
        
        # === STRUCTURED DATA: Full details for machines ===
        broker_list = list(broker_stats.values())
        topic_list = list(topic_stats.values())
        
        # Summary statistics
        total_storage_bytes = sum(b['total_size_bytes'] for b in broker_list)
        total_storage_gb = round(total_storage_bytes / (1024 * 1024 * 1024), 2)
        avg_storage_per_broker = round(total_storage_gb / len(broker_list), 2) if broker_list else 0
        
        structured_data["storage_summary"] = {
            "status": "success",
            "total_storage_gb": total_storage_gb,
            "total_brokers": len(broker_list),
            "total_topics": len(topic_list),
            "total_partitions": sum(b['partition_count'] for b in broker_list),
            "avg_storage_per_broker_gb": avg_storage_per_broker,
            "brokers_critical": broker_critical,
            "brokers_warning": broker_warning,
            "large_partitions_count": len(large_partitions)
        }
        
        structured_data["broker_storage"] = {
            "status": "success",
            "total_brokers": len(broker_list),
            "brokers_critical": broker_critical,
            "brokers_warning": broker_warning,
            "thresholds": {
                "warning_broker_gb": warning_broker_gb,
                "critical_broker_gb": critical_broker_gb,
                "warning_partition_gb": warning_partition_gb,
                "critical_partition_gb": critical_partition_gb
            },
            "data": broker_list
        }
        
        structured_data["topic_storage"] = {
            "status": "success",
            "total_topics": len(topic_list),
            "data": sorted(topic_list, key=lambda x: x['total_size_bytes'], reverse=True)
        }
        
        structured_data["partition_storage"] = {
            "status": "success",
            "total_partitions": len(all_partition_data),
            "total_large_partitions": len(large_partitions),
            "critical_partitions": sum(1 for p in large_partitions if p.get('exceeds_critical_threshold')),
            "warning_partitions": sum(1 for p in large_partitions if p.get('exceeds_warning_threshold')),
            "large_partitions": large_partitions,
            "all_partitions": all_partition_data  # Full detail for analysis
        }
    
    except Exception as e:
        import traceback
        from logging import getLogger
        logger = getLogger(__name__)
        logger.error(f"Storage health check failed: {e}\n{traceback.format_exc()}")
        
        builder.error(f"Storage health check failed: {e}")
        structured_data["storage_health"] = {"status": "error", "details": str(e)}
    
    return builder.build(), structured_data

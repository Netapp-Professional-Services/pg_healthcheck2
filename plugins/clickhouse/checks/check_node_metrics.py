"""
ClickHouse Node Metrics Health Check

Monitors system metrics, resource utilization, and query execution across all nodes.
Equivalent to OpenSearch's node metrics check.

Requirements:
- ClickHouse client access to system tables
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_node_metrics

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 10  # High priority - core health monitoring


def run_check_node_metrics(connector, settings):
    """
    Monitor node-level health metrics including resource utilization and query performance.

    Args:
        connector: ClickHouse connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add check header
    builder.h3("Node Health Metrics")
    builder.para(
        "Comprehensive health monitoring of all ClickHouse nodes including resource utilization, "
        "query execution, and system performance metrics."
    )

    try:
        # 1. Get current metrics from system.metrics using qrylib
        metrics_query = qry_node_metrics.get_system_metrics_query(connector)
        current_metrics = connector.execute_query(metrics_query)

        # 2. Get asynchronous metrics (sampled periodically) using qrylib
        async_metrics_query = qry_node_metrics.get_async_metrics_query(connector)
        async_metrics = connector.execute_query(async_metrics_query)

        # 3. Get currently running queries using qrylib
        processes_query = qry_node_metrics.get_active_queries_query(connector)
        running_queries = connector.execute_query(processes_query)

        # 4. Get node summary metrics
        node_summary_query = qry_node_metrics.get_node_summary_query(connector)
        node_summary_result = connector.execute_query(node_summary_query)

        # 5. Process and analyze metrics
        metrics_dict = {}
        if current_metrics:
            for row in current_metrics:
                metrics_dict[row[0]] = {
                    'value': row[1],
                    'description': row[2]
                }

        async_metrics_dict = {}
        if async_metrics:
            for row in async_metrics:
                async_metrics_dict[row[0]] = row[1]

        # Get process summary from node summary query
        process_summary = {}
        if node_summary_result and len(node_summary_result) > 0:
            row = node_summary_result[0]
            process_summary = {
                'total_queries': row[6],  # active_queries
                'total_memory': row[7] if row[7] else 0,  # total_query_memory
                'avg_query_time': 0,
                'max_query_time': 0
            }

            # Calculate max query time from running queries list
            if running_queries:
                max_elapsed = max((q[2] for q in running_queries), default=0)
                avg_elapsed = sum(q[2] for q in running_queries) / len(running_queries) if running_queries else 0
                process_summary['max_query_time'] = max_elapsed
                process_summary['avg_query_time'] = avg_elapsed

        # 6. Check for issues
        critical_issues = []
        warnings = []

        # Memory pressure check
        memory_tracking = async_metrics_dict.get('MemoryTracking', 0)
        memory_total = async_metrics_dict.get('OSMemoryTotal', 1)
        memory_percent = (memory_tracking / memory_total * 100) if memory_total > 0 else 0

        memory_warning_threshold = settings.get('memory_warning_percent', 75)
        memory_critical_threshold = settings.get('memory_critical_percent', 85)

        if memory_percent >= memory_critical_threshold:
            critical_issues.append({
                'title': 'Critical Memory Usage',
                'details': {
                    'Memory Usage': f"{memory_percent:.1f}% ({memory_tracking / (1024**3):.2f} GB / {memory_total / (1024**3):.2f} GB)",
                    'Threshold': f"{memory_critical_threshold}%",
                    'Status': "🔴 CRITICAL"
                }
            })
        elif memory_percent >= memory_warning_threshold:
            warnings.append({
                'title': 'High Memory Usage',
                'details': {
                    'Memory Usage': f"{memory_percent:.1f}% ({memory_tracking / (1024**3):.2f} GB / {memory_total / (1024**3):.2f} GB)",
                    'Threshold': f"{memory_warning_threshold}%",
                    'Status': "⚠️ WARNING"
                }
            })

        # Check load average
        load_avg_1 = async_metrics_dict.get('LoadAverage1', 0)
        # Assuming typical server has 4+ cores; adjust as needed
        if load_avg_1 > 10:
            critical_issues.append({
                'title': 'High System Load',
                'details': {
                    'Load Average (1m)': f"{load_avg_1:.2f}",
                    'Status': "🔴 System overloaded"
                }
            })
        elif load_avg_1 > 5:
            warnings.append({
                'title': 'Elevated System Load',
                'details': {
                    'Load Average (1m)': f"{load_avg_1:.2f}",
                    'Status': "⚠️ Monitor closely"
                }
            })

        # Check for long-running queries
        max_query_time = process_summary.get('max_query_time', 0)
        if max_query_time > 300:  # 5 minutes
            warnings.append({
                'title': 'Long-Running Query Detected',
                'details': {
                    'Max Query Time': f"{max_query_time:.1f} seconds",
                    'Status': "⚠️ Check query optimization"
                }
            })

        # 7. Display issues
        if critical_issues:
            builder.h4("🔴 Critical Issues Detected")
            for issue in critical_issues:
                builder.critical_issue(issue['title'], issue['details'])

        if warnings:
            builder.h4("⚠️ Warnings")
            for warning in warnings:
                builder.warning_issue(warning['title'], warning['details'])

        # 8. Display current system metrics
        builder.h4("Real-Time System Metrics")

        # Key operational metrics
        key_metrics = [
            ('Query', 'Running Queries'),
            ('Merge', 'Active Merges'),
            ('PartMutation', 'Active Mutations'),
            ('ReplicatedFetch', 'Replication Fetches'),
            ('ReplicatedSend', 'Replication Sends'),
            ('TCPConnection', 'TCP Connections'),
            ('HTTPConnection', 'HTTP Connections')
        ]

        metrics_table = []
        for metric_name, display_name in key_metrics:
            if metric_name in metrics_dict:
                metrics_table.append({
                    "Metric": display_name,
                    "Value": str(metrics_dict[metric_name]['value']),
                    "Description": metrics_dict[metric_name]['description']
                })

        if metrics_table:
            builder.table(metrics_table)
        builder.blank()

        # 9. Display system resource metrics
        builder.h4("System Resources")

        # Calculate memory metrics
        memory_free = async_metrics_dict.get('OSMemoryFreeWithoutCached', 0)
        memory_cached = async_metrics_dict.get('OSMemoryCached', 0)
        memory_buffers = async_metrics_dict.get('OSMemoryBuffers', 0)
        memory_used = memory_total - memory_free

        resource_table = [
            {
                "Resource": "Total Memory",
                "Value": f"{memory_total / (1024**3):.2f} GB"
            },
            {
                "Resource": "Used Memory",
                "Value": f"{memory_used / (1024**3):.2f} GB ({(memory_used / memory_total * 100):.1f}%)"
            },
            {
                "Resource": "ClickHouse Memory",
                "Value": f"{memory_tracking / (1024**3):.2f} GB ({memory_percent:.1f}%)"
            },
            {
                "Resource": "Free Memory",
                "Value": f"{memory_free / (1024**3):.2f} GB"
            },
            {
                "Resource": "Cached Memory",
                "Value": f"{memory_cached / (1024**3):.2f} GB"
            },
            {
                "Resource": "Load Average (1m/5m/15m)",
                "Value": f"{async_metrics_dict.get('LoadAverage1', 0):.2f} / {async_metrics_dict.get('LoadAverage5', 0):.2f} / {async_metrics_dict.get('LoadAverage15', 0):.2f}"
            },
            {
                "Resource": "CPU System Time",
                "Value": f"{async_metrics_dict.get('OSSystemTimeNormalized', 0):.2f}"
            },
            {
                "Resource": "CPU User Time",
                "Value": f"{async_metrics_dict.get('OSUserTimeNormalized', 0):.2f}"
            }
        ]

        builder.table(resource_table)
        builder.blank()

        # 10. Display database/table statistics
        builder.h4("Database Statistics")

        stats_table = [
            {
                "Metric": "Total Databases",
                "Value": str(int(async_metrics_dict.get('NumberOfDatabases', 0)))
            },
            {
                "Metric": "Total Tables",
                "Value": str(int(async_metrics_dict.get('NumberOfTables', 0)))
            },
            {
                "Metric": "Total MergeTree Data",
                "Value": f"{async_metrics_dict.get('TotalBytesOfMergeTreeTables', 0) / (1024**3):.2f} GB"
            },
            {
                "Metric": "Total MergeTree Rows",
                "Value": f"{int(async_metrics_dict.get('TotalRowsOfMergeTreeTables', 0)):,}"
            }
        ]

        builder.table(stats_table)
        builder.blank()

        # 11. Display running queries summary
        builder.h4("Query Execution Summary")

        if process_summary.get('total_queries', 0) > 0:
            query_summary_table = [
                {
                    "Metric": "Active Queries",
                    "Value": str(process_summary['total_queries'])
                },
                {
                    "Metric": "Total Query Memory",
                    "Value": f"{process_summary['total_memory'] / (1024**3):.2f} GB"
                },
                {
                    "Metric": "Avg Query Time",
                    "Value": f"{process_summary['avg_query_time']:.2f} seconds"
                },
                {
                    "Metric": "Max Query Time",
                    "Value": f"{process_summary['max_query_time']:.2f} seconds"
                }
            ]
            builder.table(query_summary_table)
        else:
            builder.para("No queries currently running.")

        builder.blank()

        # 12. Display detailed running queries
        if running_queries and len(running_queries) > 0:
            builder.h4("Top Running Queries (by duration)")

            queries_table = []
            for row in running_queries:
                query_text = row[6]
                if len(query_text) > 80:
                    query_text = query_text[:77] + "..."

                queries_table.append({
                    "User": row[1],
                    "Duration (s)": f"{row[2]:.1f}",
                    "Rows Read": f"{row[3]:,}",
                    "Memory (MB)": f"{row[5] / (1024**2):.2f}",
                    "Query": query_text
                })

            builder.table(queries_table)
            builder.blank()

        # 13. Recommendations
        recommendations = _generate_recommendations(
            critical_issues,
            warnings,
            process_summary,
            memory_percent
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        else:
            builder.success("✅ All nodes are healthy. No issues detected.")

        # 14. Structured data
        structured_data["node_metrics"] = {
            "status": "success",
            "memory_percent": round(memory_percent, 1),
            "load_average_1m": round(load_avg_1, 2),
            "active_queries": process_summary.get('total_queries', 0),
            "total_tables": int(async_metrics_dict.get('NumberOfTables', 0)),
            "critical_issues": len(critical_issues),
            "warnings": len(warnings)
        }

    except Exception as e:
        logger.error(f"Node metrics check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["node_metrics"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _generate_recommendations(critical_issues, warnings, process_summary, memory_percent):
    """Generate recommendations based on node health."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    # Analyze issues
    has_memory_issues = any('Memory' in issue['title'] for issue in critical_issues + warnings)
    has_load_issues = any('Load' in issue['title'] for issue in critical_issues + warnings)
    has_query_issues = any('Query' in issue['title'] for issue in critical_issues + warnings)

    if has_memory_issues:
        if memory_percent >= 85:
            recs["critical"].extend([
                "Reduce memory usage immediately or add more RAM to the server",
                "Identify and terminate memory-intensive queries",
                "Review max_memory_usage settings for queries"
            ])
        recs["high"].extend([
            "Monitor query memory usage patterns",
            "Consider increasing server RAM or optimizing queries",
            "Review max_memory_usage_for_user and max_memory_usage_for_all_queries settings"
        ])

    if has_load_issues:
        recs["critical"].extend([
            "System load is high - investigate CPU-intensive operations",
            "Check for resource-intensive merges or queries",
            "Consider scaling horizontally by adding nodes"
        ])

    if has_query_issues:
        recs["high"].extend([
            "Optimize long-running queries using EXPLAIN and query_log",
            "Review query patterns and add appropriate indices",
            "Consider implementing query timeouts (max_execution_time)"
        ])

    # General best practices
    recs["general"].extend([
        "Monitor system.metrics and system.asynchronous_metrics regularly",
        "Set up alerting for memory usage (>75%), load average, and long-running queries",
        "Review system.query_log periodically to identify optimization opportunities",
        "Ensure background merges are not overwhelming the system",
        "Monitor replication lag if using replicated tables"
    ])

    return recs

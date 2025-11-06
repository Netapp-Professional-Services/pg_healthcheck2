"""
ClickHouse Query Performance Check

Monitors query execution performance, slow queries, and query patterns.
Enhanced with system.query_metric_log (ProfileEvents) for detailed bottleneck analysis.

MORE ACTIONABLE than instacollector:
- Identifies specific bottlenecks (I/O, CPU, locks, inefficient filtering)
- Provides targeted optimization recommendations
- Analyzes query efficiency ratios (not just "slow")

Requirements:
- ClickHouse client access to system.query_log
- ClickHouse client access to system.query_metric_log (for detailed metrics)
"""

import logging
from typing import Dict, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_query_log

logger = logging.getLogger(__name__)


# Check metadata
check_metadata = {
    'requires_api': False,
    'requires_ssh': False,
    'requires_connection': True,
    'description': 'Query performance analysis with detailed ProfileEvents bottleneck identification'
}


def get_weight():
    """Returns the importance score for this check."""
    return 8


def run_check_query_performance(connector, settings):
    """Monitor cluster query performance metrics."""
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("Query Performance Metrics")
    builder.para("Analysis of query execution performance, slow queries, and workload patterns.")

    try:
        # 1. Get query statistics from query_log using qrylib
        query_stats_query = qry_query_log.get_query_performance_summary_query(connector, hours=1)
        stats_result = connector.execute_query(query_stats_query)

        # 2. Get slow queries (>10 seconds) using qrylib
        slow_queries_query = qry_query_log.get_slow_queries_query(connector, threshold_seconds=10, limit=10)
        slow_queries = connector.execute_query(slow_queries_query)

        # 3. Get failed queries using qrylib
        failed_queries_query = qry_query_log.get_failed_queries_query(connector, hours=1, limit=10)
        failed_queries = connector.execute_query(failed_queries_query)

        # 4. Get query types distribution
        query_types_query = """
        SELECT
            multiIf(
                query LIKE 'SELECT%', 'SELECT',
                query LIKE 'INSERT%', 'INSERT',
                query LIKE 'ALTER%', 'ALTER',
                query LIKE 'CREATE%', 'CREATE',
                query LIKE 'DROP%', 'DROP',
                query LIKE 'OPTIMIZE%', 'OPTIMIZE',
                'OTHER'
            ) as query_type,
            count() as count,
            avg(query_duration_ms) as avg_duration_ms
        FROM system.query_log
        WHERE event_date >= today() - 1
          AND event_time >= now() - INTERVAL 1 HOUR
          AND type = 'QueryFinish'
        GROUP BY query_type
        ORDER BY count DESC
        """
        query_types = connector.execute_query(query_types_query)

        # 5. Get top queries by resource usage
        resource_intensive_query = """
        SELECT
            query,
            user,
            query_duration_ms,
            read_rows,
            memory_usage
        FROM system.query_log
        WHERE event_date >= today() - 1
          AND event_time >= now() - INTERVAL 1 HOUR
          AND type = 'QueryFinish'
        ORDER BY memory_usage DESC
        LIMIT 10
        """
        resource_intensive = connector.execute_query(resource_intensive_query)

        # 6. Process statistics
        stats = {}
        if stats_result and len(stats_result) > 0:
            row = stats_result[0]
            stats = {
                'total_queries': row[0],
                'successful_queries': row[1],
                'failed_queries': row[2],
                'avg_duration_ms': row[3] if row[3] else 0,
                'p95_duration_ms': row[4] if row[4] else 0,
                'p99_duration_ms': row[5] if row[5] else 0,
                'max_duration_ms': row[6] if row[6] else 0,
                'total_rows_read': row[7] if row[7] else 0,
                'total_bytes_read': row[8] if row[8] else 0,
                'total_rows_written': row[9] if row[9] else 0,
                'total_bytes_written': row[10] if row[10] else 0,
                'total_memory_used': row[11] if row[11] else 0
            }

        # 7. Check for performance issues
        critical_issues = []
        warnings = []

        # Check failure rate
        if stats.get('total_queries', 0) > 0:
            failure_rate = (stats['failed_queries'] / stats['total_queries']) * 100
            if failure_rate > 10:
                critical_issues.append({
                    'title': 'High Query Failure Rate',
                    'details': {
                        'Failure Rate': f"{failure_rate:.1f}%",
                        'Failed Queries': stats['failed_queries'],
                        'Total Queries': stats['total_queries'],
                        'Status': "🔴 Investigate query errors"
                    }
                })
            elif failure_rate > 5:
                warnings.append({
                    'title': 'Elevated Query Failure Rate',
                    'details': {
                        'Failure Rate': f"{failure_rate:.1f}%",
                        'Failed Queries': stats['failed_queries'],
                        'Status': "⚠️ Monitor closely"
                    }
                })

        # Check slow queries
        if slow_queries and len(slow_queries) > 0:
            warnings.append({
                'title': 'Slow Queries Detected',
                'details': {
                    'Count': len(slow_queries),
                    'Slowest Query': f"{slow_queries[0][0] / 1000:.1f} seconds",
                    'Status': "⚠️ Review query optimization"
                }
            })

        # Check p99 latency
        p99_threshold_ms = settings.get('p99_latency_threshold_ms', 5000)
        if stats.get('p99_duration_ms', 0) > p99_threshold_ms:
            warnings.append({
                'title': 'High P99 Query Latency',
                'details': {
                    'P99 Latency': f"{stats['p99_duration_ms'] / 1000:.2f} seconds",
                    'Threshold': f"{p99_threshold_ms / 1000:.2f} seconds",
                    'Status': "⚠️ Performance degradation"
                }
            })

        # 8. Display issues
        if critical_issues:
            builder.h4("🔴 Critical Issues Detected")
            for issue in critical_issues:
                builder.critical_issue(issue['title'], issue['details'])

        if warnings:
            builder.h4("⚠️ Warnings")
            for warning in warnings:
                builder.warning_issue(warning['title'], warning['details'])

        # 9. Performance summary
        builder.h4("Query Performance Summary (Last Hour)")

        if stats:
            perf_data = [
                {"Metric": "Total Queries", "Value": f"{stats['total_queries']:,}"},
                {"Metric": "Successful Queries", "Value": f"{stats['successful_queries']:,}"},
                {"Metric": "Failed Queries", "Value": f"{stats['failed_queries']:,}"},
                {"Metric": "Avg Query Time", "Value": f"{stats['avg_duration_ms'] / 1000:.3f} s"},
                {"Metric": "P95 Query Time", "Value": f"{stats['p95_duration_ms'] / 1000:.3f} s"},
                {"Metric": "P99 Query Time", "Value": f"{stats['p99_duration_ms'] / 1000:.3f} s"},
                {"Metric": "Max Query Time", "Value": f"{stats['max_duration_ms'] / 1000:.2f} s"},
                {"Metric": "Total Rows Read", "Value": f"{stats['total_rows_read']:,}"},
                {"Metric": "Total Data Read", "Value": f"{stats['total_bytes_read'] / (1024**3):.2f} GB"},
                {"Metric": "Total Memory Used", "Value": f"{stats['total_memory_used'] / (1024**3):.2f} GB"}
            ]
            builder.table(perf_data)
        else:
            builder.para("No query statistics available for the last hour.")

        builder.blank()

        # 10. Query types distribution
        if query_types and len(query_types) > 0:
            builder.h4("Query Workload Distribution")
            types_table = []
            for row in query_types:
                types_table.append({
                    "Query Type": row[0],
                    "Count": f"{row[1]:,}",
                    "Avg Duration (s)": f"{(row[2] / 1000 if row[2] else 0):.3f}"
                })
            builder.table(types_table)
            builder.blank()

        # 11. Display slow queries
        if slow_queries and len(slow_queries) > 0:
            builder.h4("Slow Queries (>10 seconds)")

            slow_table = []
            for row in slow_queries:
                query_text = row[1]
                if len(query_text) > 100:
                    query_text = query_text[:97] + "..."

                slow_table.append({
                    "Duration (s)": f"{row[0] / 1000:.1f}",
                    "User": row[2],
                    "Rows Read": f"{row[3]:,}",
                    "Memory (MB)": f"{row[6] / (1024**2):.2f}",
                    "Query": query_text
                })
            builder.table(slow_table)
            builder.blank()

        # 12. Display failed queries
        if failed_queries and len(failed_queries) > 0:
            builder.h4("Recent Failed Queries")

            failed_table = []
            for row in failed_queries:
                query_text = row[0]
                if len(query_text) > 80:
                    query_text = query_text[:77] + "..."

                exception = row[2]
                if len(exception) > 100:
                    exception = exception[:97] + "..."

                failed_table.append({
                    "User": row[1],
                    "Duration (s)": f"{row[3] / 1000:.2f}",
                    "Query": query_text,
                    "Error": exception
                })
            builder.table(failed_table)
            builder.blank()

        # 13. Display resource-intensive queries
        if resource_intensive and len(resource_intensive) > 0:
            builder.h4("Top Queries by Memory Usage")

            resource_table = []
            for row in resource_intensive:
                query_text = row[0]
                if len(query_text) > 80:
                    query_text = query_text[:77] + "..."

                resource_table.append({
                    "User": row[1],
                    "Duration (s)": f"{row[2] / 1000:.2f}",
                    "Rows Read": f"{row[3]:,}",
                    "Memory (GB)": f"{row[4] / (1024**3):.2f}",
                    "Query": query_text
                })
            builder.table(resource_table)
            builder.blank()

        # ============================================================================
        # DETAILED PROFILEEVENTS ANALYSIS (system.query_metric_log)
        # More actionable than instacollector - identifies specific bottlenecks
        # ============================================================================

        builder.h3("🔍 Detailed Query Bottleneck Analysis")
        builder.para(
            "Advanced ProfileEvents analysis from system.query_metric_log. "
            "Identifies specific performance bottlenecks beyond basic 'slow query' detection."
        )
        builder.blank()

        # Check if system.query_metric_log is available
        query_metric_log_available = qry_query_log.check_query_metric_log_available(connector)

        if not query_metric_log_available:
            builder.note(
                "**Note:** Detailed ProfileEvents analysis is not available. "
                "system.query_metric_log table is not enabled on this cluster. "
                "This is common in managed services (e.g., Instaclustr, ClickHouse Cloud). "
                "To enable on self-managed clusters, set log_query_threads=1 in server configuration."
            )
            builder.blank()

        detailed_analysis_available = False
        bottleneck_data = {}

        # 14. I/O Intensive Queries Analysis
        if query_metric_log_available:
            try:
                io_query = qry_query_log.get_io_intensive_queries_query(connector, hours=1, limit=5)
                io_result = connector.execute_query(io_query)

                if io_result and len(io_result) > 0:
                    detailed_analysis_available = True
                    builder.h4("I/O Intensive Queries")
                    builder.para(
                        "Queries with excessive disk I/O operations. "
                        "**Optimization focus:** Add indices, improve data filtering, use appropriate table engines."
                    )

                    io_table = []
                    io_data_list = []
                    for row in io_result:
                        query_text = row[10] if len(row) > 10 else "N/A"
                        if len(query_text) > 100:
                            query_text = query_text[:97] + "..."

                        io_table.append({
                            "Duration (s)": f"{row[3] / 1000:.1f}",
                            "File Opens": f"{row[4]:,}",
                            "Disk Reads": f"{row[5]:,}",
                            "Network Sent": row[6] if row[6] else "0 B",
                            "Rows Read": f"{row[8]:,}",
                            "Query": query_text
                        })

                        io_data_list.append({
                            'query_id': row[2],
                            'duration_ms': row[3],
                            'file_opens': row[4],
                            'disk_reads': row[5],
                            'rows_read': row[8]
                        })

                    builder.table(io_table)
                    builder.blank()

                    bottleneck_data['io_intensive'] = io_data_list
            except Exception as e:
                logger.debug(f"Could not analyze I/O intensive queries: {e}")

        # 15. CPU Intensive Queries Analysis
        if query_metric_log_available:
            try:
                cpu_query = qry_query_log.get_cpu_intensive_queries_query(connector, hours=1, limit=5)
                cpu_result = connector.execute_query(cpu_query)

                if cpu_result and len(cpu_result) > 0:
                    detailed_analysis_available = True
                    builder.h4("CPU Intensive Queries")
                    builder.para(
                        "Queries consuming significant CPU resources. "
                        "**Optimization focus:** Reduce computational complexity, optimize aggregations, use materialized views."
                    )

                    cpu_table = []
                    cpu_data_list = []
                    for row in cpu_result:
                        query_text = row[9] if len(row) > 9 else "N/A"
                        if len(query_text) > 100:
                            query_text = query_text[:97] + "..."

                        cpu_table.append({
                            "Duration (s)": f"{row[3] / 1000:.1f}",
                            "CPU Wait (s)": f"{row[4]:.2f}",
                            "User Time (s)": f"{row[5]:.2f}",
                            "System Time (s)": f"{row[6]:.2f}",
                            "Total CPU (s)": f"{row[7]:.2f}",
                            "Query": query_text
                        })

                        cpu_data_list.append({
                            'query_id': row[2],
                            'duration_ms': row[3],
                            'cpu_wait_seconds': row[4],
                            'user_time_seconds': row[5],
                            'system_time_seconds': row[6],
                            'total_cpu_seconds': row[7]
                        })

                    builder.table(cpu_table)
                    builder.blank()

                    bottleneck_data['cpu_intensive'] = cpu_data_list
            except Exception as e:
                logger.debug(f"Could not analyze CPU intensive queries: {e}")

        # 16. Lock Contention Analysis
        if query_metric_log_available:
            try:
                lock_query = qry_query_log.get_lock_contention_queries_query(connector, hours=1, limit=5)
                lock_result = connector.execute_query(lock_query)

                if lock_result and len(lock_result) > 0:
                    detailed_analysis_available = True
                    builder.h4("Lock Contention Queries")
                    builder.para(
                        "Queries experiencing high lock contention - indicates concurrency issues. "
                        "**Optimization focus:** Reduce table lock duration, batch updates, optimize concurrent access patterns."
                    )

                    lock_table = []
                    lock_data_list = []
                    for row in lock_result:
                        query_text = row[8] if len(row) > 8 else "N/A"
                        if len(query_text) > 100:
                            query_text = query_text[:97] + "..."

                        lock_table.append({
                            "Duration (s)": f"{row[3] / 1000:.1f}",
                            "Context Locks": f"{row[4]:,}",
                            "Read Locks": f"{row[5]:,}",
                            "Write Locks": f"{row[6]:,}",
                            "Query": query_text
                        })

                        lock_data_list.append({
                            'query_id': row[2],
                            'duration_ms': row[3],
                            'context_locks': row[4],
                            'read_locks': row[5],
                            'write_locks': row[6]
                        })

                    builder.table(lock_table)
                    builder.blank()

                    bottleneck_data['lock_contention'] = lock_data_list
            except Exception as e:
                logger.debug(f"Could not analyze lock contention: {e}")

        # 17. Inefficient Query Ratios (does not require query_metric_log)
        try:
            inefficient_query = qry_query_log.get_inefficient_queries_by_ratio_query(connector, hours=1, limit=5)
            inefficient_result = connector.execute_query(inefficient_query)

            if inefficient_result and len(inefficient_result) > 0:
                detailed_analysis_available = True
                builder.h4("Inefficient Query Patterns")
                builder.para(
                    "Queries with poor efficiency ratios (scanning far more data than returned). "
                    "**Optimization focus:** Add WHERE filters, create appropriate indices, use projections."
                )

                ineff_table = []
                ineff_data_list = []
                for row in inefficient_result:
                    query_text = row[11] if len(row) > 11 else "N/A"
                    if len(query_text) > 80:
                        query_text = query_text[:77] + "..."

                    ineff_table.append({
                        "Duration (s)": f"{row[3] / 1000:.1f}",
                        "Rows Scanned": f"{row[4]:,}",
                        "Rows Returned": f"{row[5]:,}",
                        "Scan Ratio": f"{row[6]:.1f}x",
                        "Data Read": row[7],
                        "Query": query_text
                    })

                    ineff_data_list.append({
                        'query_id': row[2],
                        'duration_ms': row[3],
                        'read_rows': row[4],
                        'result_rows': row[5],
                        'row_scan_ratio': row[6]
                    })

                builder.table(ineff_table)
                builder.blank()

                bottleneck_data['inefficient_queries'] = ineff_data_list
        except Exception as e:
            logger.debug(f"Could not analyze inefficient queries: {e}")

        # 18. Merge Intensive Queries
        if query_metric_log_available:
            try:
                merge_query = qry_query_log.get_merge_intensive_queries_query(connector, hours=1, limit=5)
                merge_result = connector.execute_query(merge_query)

                if merge_result and len(merge_result) > 0:
                    detailed_analysis_available = True
                    builder.h4("Merge Intensive Queries")
                    builder.para(
                        "Queries triggering excessive merge operations. "
                        "**Optimization focus:** Batch inserts, adjust merge settings, review partitioning strategy."
                    )

                    merge_table = []
                    merge_data_list = []
                    for row in merge_result:
                        query_text = row[9] if len(row) > 9 else "N/A"
                        if len(query_text) > 100:
                            query_text = query_text[:97] + "..."

                        merge_table.append({
                            "Duration (s)": f"{row[3] / 1000:.1f}",
                            "Merges": f"{row[4]:,}",
                            "Merged Rows": f"{row[5]:,}",
                            "Merged Data": row[6],
                            "Query": query_text
                        })

                        merge_data_list.append({
                            'query_id': row[2],
                            'duration_ms': row[3],
                            'merges': row[4],
                            'merged_rows': row[5]
                        })

                    builder.table(merge_table)
                    builder.blank()

                    bottleneck_data['merge_intensive'] = merge_data_list
            except Exception as e:
                logger.debug(f"Could not analyze merge intensive queries: {e}")

        if not detailed_analysis_available:
            builder.note(
                "**Note:** Detailed ProfileEvents analysis requires system.query_metric_log to be enabled. "
                "To enable, set log_query_threads=1 in server configuration. "
                "This provides much more actionable performance insights than basic query_log."
            )
            builder.blank()

        # 19. Enhanced Recommendations (including bottleneck-specific guidance)
        recs = _generate_performance_recommendations(
            critical_issues,
            warnings,
            stats,
            slow_queries,
            failed_queries,
            bottleneck_data
        )

        if recs["critical"] or recs["high"]:
            builder.recs(recs)
        elif stats.get('total_queries', 0) > 0:
            builder.success("✅ Query performance is healthy with no significant issues detected.")

        # 20. Enhanced Structured Data (includes bottleneck analysis for trends)
        structured_data["query_performance"] = {
            "status": "success",
            "data": [],
            "metadata": {
                "total_queries": stats.get('total_queries', 0),
                "failed_queries": stats.get('failed_queries', 0),
                "avg_duration_ms": round(stats.get('avg_duration_ms', 0), 2),
                "p99_duration_ms": round(stats.get('p99_duration_ms', 0), 2),
                "slow_queries_count": len(slow_queries) if slow_queries else 0,
                "critical_issues": len(critical_issues),
                "warnings": len(warnings),
                "timestamp": connector.get_current_timestamp()
            }
        }

        # Add bottleneck analysis to structured data for trend tracking
        if bottleneck_data:
            for bottleneck_type, data_list in bottleneck_data.items():
                structured_data[f"query_{bottleneck_type}"] = {
                    "status": "success",
                    "data": data_list,
                    "metadata": {
                        "count": len(data_list),
                        "timestamp": connector.get_current_timestamp()
                    }
                }

    except Exception as e:
        logger.error(f"Query performance check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["query_performance"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _generate_performance_recommendations(critical_issues, warnings, stats, slow_queries, failed_queries, bottleneck_data=None):
    """
    Generate actionable recommendations based on performance analysis.

    Enhanced with bottleneck-specific guidance from ProfileEvents analysis.
    More actionable than instacollector - provides targeted optimization steps.
    """
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    has_failures = stats.get('failed_queries', 0) > 0
    has_slow_queries = slow_queries and len(slow_queries) > 0

    if has_failures:
        failure_rate = (stats['failed_queries'] / stats['total_queries']) * 100 if stats['total_queries'] > 0 else 0
        if failure_rate > 10:
            recs["critical"].extend([
                f"High query failure rate ({failure_rate:.1f}%) requires immediate investigation",
                "Review failed queries in system.query_log for common error patterns",
                "Check for resource constraints (memory, disk space, connections)"
            ])

    if has_slow_queries:
        recs["high"].extend([
            "Optimize slow queries using EXPLAIN and query profiling",
            "Review table structures and add appropriate indices",
            "Consider materializing frequently-used aggregations",
            "Check if queries can benefit from pre-aggregated projections"
        ])

    if stats.get('p99_duration_ms', 0) > 5000:
        recs["high"].extend([
            f"P99 latency is high ({stats['p99_duration_ms'] / 1000:.2f}s) - review query patterns",
            "Consider using query result cache for repeated queries",
            "Review max_threads and max_execution_time settings"
        ])

    # Bottleneck-specific recommendations (MORE ACTIONABLE than instacollector)
    if bottleneck_data:
        if 'io_intensive' in bottleneck_data and len(bottleneck_data['io_intensive']) > 0:
            recs["high"].extend([
                f"⚠️ {len(bottleneck_data['io_intensive'])} I/O intensive queries detected - excessive disk operations",
                "Add missing indices to reduce full table scans",
                "Optimize WHERE clause filtering to reduce data read",
                "Consider using appropriate table engine (e.g., MergeTree with proper ORDER BY)",
                "Review partition pruning - ensure queries use partition key",
                "Check if SSD storage would benefit I/O-bound queries"
            ])

        if 'cpu_intensive' in bottleneck_data and len(bottleneck_data['cpu_intensive']) > 0:
            recs["high"].extend([
                f"⚠️ {len(bottleneck_data['cpu_intensive'])} CPU intensive queries detected - computational bottlenecks",
                "Optimize complex aggregations and GROUP BY operations",
                "Use materialized views for frequently-computed aggregations",
                "Consider pre-computing results for expensive calculations",
                "Review JOIN complexity - simplify where possible",
                "Check if projections can accelerate aggregation queries"
            ])

        if 'lock_contention' in bottleneck_data and len(bottleneck_data['lock_contention']) > 0:
            recs["high"].extend([
                f"⚠️ {len(bottleneck_data['lock_contention'])} queries experiencing lock contention - concurrency issues",
                "Batch UPDATE/DELETE operations to reduce lock duration",
                "Review concurrent write patterns to same tables",
                "Consider using ReplacingMergeTree for UPDATE-heavy workloads",
                "Optimize queries to hold locks for shorter duration",
                "Review max_concurrent_queries setting if needed"
            ])

        if 'inefficient_queries' in bottleneck_data and len(bottleneck_data['inefficient_queries']) > 0:
            recs["critical"].extend([
                f"🔴 {len(bottleneck_data['inefficient_queries'])} inefficient queries detected - poor filtering ratios",
                "Queries are scanning 1000x+ more data than returned",
                "Add WHERE clause filters to reduce data scanned",
                "Create secondary indices on frequently-filtered columns",
                "Use PREWHERE for early filtering on large tables",
                "Consider using data skipping indices",
                "Review query patterns - may need schema redesign"
            ])

        if 'merge_intensive' in bottleneck_data and len(bottleneck_data['merge_intensive']) > 0:
            recs["high"].extend([
                f"⚠️ {len(bottleneck_data['merge_intensive'])} queries triggering excessive merges",
                "Batch INSERT operations to reduce merge overhead",
                "Review background_pool_size setting for merge capacity",
                "Adjust merge settings (max_bytes_to_merge_at_max_space_in_pool)",
                "Consider async_insert=1 for high-frequency small inserts",
                "Review partition strategy - too many partitions increase merges"
            ])

    # General recommendations (enhanced with ProfileEvents guidance)
    recs["general"].extend([
        "Enable system.query_metric_log for detailed ProfileEvents analysis",
        "Use EXPLAIN PLAN to understand query execution paths",
        "Monitor query_log regularly for performance regressions",
        "Set max_execution_time to prevent runaway queries",
        "Consider enabling query_log sampling for high-traffic clusters",
        "Review and optimize JOIN operations - prefer INNER over CROSS joins",
        "Use appropriate data types to minimize memory usage",
        "Consider partitioning large tables for better query performance",
        "Test queries in development with realistic data volumes",
        "Use query_log statistics to identify optimization candidates"
    ])

    return recs

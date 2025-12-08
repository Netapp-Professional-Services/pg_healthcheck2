"""
OpenSearch Cluster Performance Check

Monitors search/indexing performance, thread pool health, and cache efficiency.
Uses REST API with optional CloudWatch metrics for AWS environments.
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 8


def run_check_cluster_performance(connector, settings):
    """
    Monitor cluster performance metrics.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)

    builder.h3("Cluster Performance Metrics")
    builder.para("Analysis of search/indexing performance, thread pool utilization, and cache hit ratios.")

    try:
        # Get node stats with performance metrics
        node_stats = connector.execute_query({
            "operation": "node_stats",
            "metrics": ["indices", "thread_pool", "jvm"]
        })

        if isinstance(node_stats, dict) and "error" in node_stats:
            builder.error(f"Could not retrieve performance metrics: {node_stats['error']}")
            return builder.build(), {
                "status": "error",
                "error": node_stats['error']
            }

        # Aggregate cluster-wide metrics
        total_search_count = 0
        total_search_time_ms = 0
        total_index_count = 0
        total_index_time_ms = 0
        total_query_cache_hits = 0
        total_query_cache_misses = 0
        total_request_cache_hits = 0
        total_request_cache_misses = 0
        thread_pool_issues = []

        for node_id, node_data in node_stats.get('nodes', {}).items():
            node_name = node_data.get('name', node_id)
            indices = node_data.get('indices', {})
            search = indices.get('search', {})
            indexing = indices.get('indexing', {})
            query_cache = indices.get('query_cache', {})
            request_cache = indices.get('request_cache', {})
            thread_pools = node_data.get('thread_pool', {})

            # Aggregate search stats
            total_search_count += search.get('query_total', 0)
            total_search_time_ms += search.get('query_time_in_millis', 0)

            # Aggregate indexing stats
            total_index_count += indexing.get('index_total', 0)
            total_index_time_ms += indexing.get('index_time_in_millis', 0)

            # Aggregate cache stats
            total_query_cache_hits += query_cache.get('hit_count', 0)
            total_query_cache_misses += query_cache.get('miss_count', 0)
            total_request_cache_hits += request_cache.get('hit_count', 0)
            total_request_cache_misses += request_cache.get('miss_count', 0)

            # Check thread pools for issues
            for pool_name in ['search', 'write', 'get', 'bulk', 'management']:
                pool = thread_pools.get(pool_name, {})
                rejected = pool.get('rejected', 0)
                queue = pool.get('queue', 0)

                if rejected > 0:
                    thread_pool_issues.append({
                        'node': node_name,
                        'pool': pool_name,
                        'rejected': rejected,
                        'queue': queue,
                        'severity': 'critical'
                    })
                elif queue > 100:  # High queue depth
                    thread_pool_issues.append({
                        'node': node_name,
                        'pool': pool_name,
                        'rejected': rejected,
                        'queue': queue,
                        'severity': 'warning'
                    })

        # Calculate averages
        avg_search_latency = (total_search_time_ms / total_search_count) if total_search_count > 0 else 0
        avg_index_latency = (total_index_time_ms / total_index_count) if total_index_count > 0 else 0

        # Calculate cache hit ratios
        total_query_cache = total_query_cache_hits + total_query_cache_misses
        query_cache_hit_ratio = (total_query_cache_hits / total_query_cache * 100) if total_query_cache > 0 else 0

        total_request_cache = total_request_cache_hits + total_request_cache_misses
        request_cache_hit_ratio = (total_request_cache_hits / total_request_cache * 100) if total_request_cache > 0 else 0

        # Display critical thread pool issues
        if thread_pool_issues:
            critical_issues = [i for i in thread_pool_issues if i['severity'] == 'critical']
            if critical_issues:
                builder.h4("🔴 Thread Pool Rejections Detected")
                for issue in critical_issues:
                    builder.critical_issue(
                        f"Thread Pool Rejections on {issue['node']}",
                        {
                            "Pool": issue['pool'],
                            "Rejected": issue['rejected'],
                            "Queue Depth": issue['queue'],
                            "Impact": "Requests are being rejected - cluster is overloaded"
                        }
                    )

            warning_issues = [i for i in thread_pool_issues if i['severity'] == 'warning']
            if warning_issues:
                builder.h4("⚠️ High Thread Pool Queue Depths")
                for issue in warning_issues[:5]:  # Show top 5
                    builder.warning_issue(
                        f"High Queue on {issue['node']}",
                        {
                            "Pool": issue['pool'],
                            "Queue Depth": issue['queue'],
                            "Status": "Approaching capacity"
                        }
                    )

        # Performance summary
        builder.h4("Cluster Performance Summary")
        perf_data = [
            {"Metric": "Total Search Queries", "Value": f"{total_search_count:,}"},
            {"Metric": "Avg Search Latency", "Value": f"{avg_search_latency:.2f} ms"},
            {"Metric": "Total Indexing Operations", "Value": f"{total_index_count:,}"},
            {"Metric": "Avg Indexing Latency", "Value": f"{avg_index_latency:.2f} ms"},
            {"Metric": "Query Cache Hit Ratio", "Value": f"{query_cache_hit_ratio:.1f}%"},
            {"Metric": "Request Cache Hit Ratio", "Value": f"{request_cache_hit_ratio:.1f}%"}
        ]
        builder.table(perf_data)

        # Add AWS CloudWatch metrics if available
        if connector.environment == 'aws' and connector.has_aws_support():
            builder.h4("AWS CloudWatch Performance Metrics")
            try:
                cw_metrics = connector.get_cloudwatch_metrics(
                    metric_names=['SearchRate', 'SearchLatency', 'IndexingRate', 'IndexingLatency'],
                    period=300,
                    hours_back=1
                )
                if cw_metrics:
                    builder.para("Recent CloudWatch metrics available for trend analysis.")
                else:
                    builder.para("CloudWatch metrics not available.")
            except Exception as e:
                logger.warning(f"Could not fetch CloudWatch metrics: {e}")
                builder.para("CloudWatch metrics not available.")

        # Recommendations
        recs = {"critical": [], "high": [], "general": []}

        if any(i['severity'] == 'critical' for i in thread_pool_issues):
            recs["critical"].extend([
                "Reduce load immediately or scale cluster by adding nodes",
                "Review and optimize resource-intensive queries",
                "Increase thread pool sizes if appropriate for your workload"
            ])

        if avg_search_latency > 1000:
            recs["high"].append(f"High search latency ({avg_search_latency:.0f}ms) - review query patterns and indices")

        if query_cache_hit_ratio < 50 and total_query_cache > 1000:
            recs["high"].append(f"Low query cache hit ratio ({query_cache_hit_ratio:.1f}%) - review query patterns")

        if avg_index_latency > 100:
            recs["high"].append(f"High indexing latency ({avg_index_latency:.0f}ms) - review bulk settings and refresh intervals")

        recs["general"].extend([
            "Monitor search and indexing latencies - set alerts for degradation",
            "Optimize slow queries using _profile API",
            "Consider increasing cache sizes if hit ratios are low",
            "Review thread pool settings based on workload characteristics"
        ])

        if recs["critical"] or recs["high"]:
            builder.recs(recs)
        else:
            builder.success("✅ Cluster performance is healthy with no significant issues detected.")

        # Return structured data with proper format for rules engine
        return builder.build(), {
            "status": "success",
            "data": {
                "search_queries": total_search_count,
                "avg_search_latency_ms": round(avg_search_latency, 2),
                "indexing_operations": total_index_count,
                "avg_indexing_latency_ms": round(avg_index_latency, 2),
                "query_cache_hit_ratio": round(query_cache_hit_ratio, 1),
                "request_cache_hit_ratio": round(request_cache_hit_ratio, 1),
                "thread_pool_issues": len(thread_pool_issues),
                "has_thread_pool_issues": len(thread_pool_issues) > 0,
                "has_critical_thread_pool_issues": any(i['severity'] == 'critical' for i in thread_pool_issues),
                "high_search_latency": avg_search_latency > 1000,
                "high_indexing_latency": avg_index_latency > 100,
                "low_query_cache_hit_ratio": query_cache_hit_ratio < 50 and total_query_cache > 1000,
            }
        }

    except Exception as e:
        logger.error(f"Performance check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

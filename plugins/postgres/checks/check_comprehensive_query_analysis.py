"""
Comprehensive Query Analysis Check

Provides a holistic view of query resource consumption for strategic
workload analysis and optimization prioritization.

This check differs from other pg_stat_statements checks by providing:
- TRUE CPU time as percentage of total cluster CPU
- Temporal context (stats reset time, frequency)
- Combined I/O, cache, and write analysis in one view
- User attribution for identifying application patterns
- Comprehensive metrics for optimization ROI calculation

Use this check to:
- Identify which queries dominate your workload
- Prioritize optimization efforts by cluster CPU impact
- Understand query frequency and temporal patterns
- Identify cache efficiency and I/O characteristics
- Attribute resource consumption to users/applications
"""

from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.output_formatters import AsciiDocFormatter
from plugins.postgres.utils.qrylib.comprehensive_query_analysis import (
    get_comprehensive_query_analysis_query
)


def get_weight():
    """
    Returns the importance score for this module.

    Weight: 7 (High priority strategic analysis)
    - Not as urgent as incident response (cpu_intensive_queries: 9)
    - Higher than basic query time analysis (top_queries_by_*: 3)
    - Provides unique cluster-wide perspective for optimization prioritization
    """
    return 7


def check_comprehensive_query_analysis(connector, settings):
    """
    Analyzes query resource consumption with comprehensive metrics.

    Returns a detailed view of the top CPU-consuming queries with:
    - Estimated CPU time (execution time minus I/O wait)
    - Percentage of total cluster CPU (for prioritization)
    - Execution frequency (calls per hour)
    - I/O analysis (wait time and percentage)
    - Cache hit rate
    - Temp and WAL write metrics
    - User attribution

    Args:
        connector: PostgresConnector instance
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Comprehensive Query Analysis")

    # Check if pg_stat_statements is enabled
    if not connector.has_pgstat:
        builder.note(
            "The `pg_stat_statements` extension is not enabled. "
            "This check cannot be performed.\n\n"
            "To enable: `CREATE EXTENSION pg_stat_statements;` and restart PostgreSQL."
        )
        findings = {
            'status': 'not_applicable',
            'reason': 'pg_stat_statements not enabled',
            'data': []
        }
        return builder.build(), findings

    builder.text(
        "Identifies queries that consume the most cluster-wide resources. "
        "This comprehensive view combines CPU usage, I/O patterns, cache efficiency, "
        "and execution frequency to help prioritize optimization efforts."
    )
    builder.blank()

    try:
        # Get the query
        query = get_comprehensive_query_analysis_query(connector)

        # Optional: Show query if requested
        if settings.get('show_qry') == 'true':
            builder.text("*Query:*")
            builder.text("[source,sql]")
            builder.text("----")
            builder.text(query.replace("%(limit)s", str(settings.get('row_limit', 5))))
            builder.text("----")
            builder.blank()

        # Execute query
        params = {'limit': settings.get('row_limit', 5)}
        formatted_result, raw_result = connector.execute_query(
            query,
            params=params,
            return_raw=True
        )

        if "[ERROR]" in formatted_result:
            builder.error(f"Query execution failed:\n{formatted_result}")
            findings = {
                'status': 'error',
                'error_message': 'Query execution failed',
                'data': []
            }
            return builder.build(), findings

        if not raw_result:
            builder.note(
                "No query statistics found in `pg_stat_statements`. "
                "This may indicate the extension was recently enabled or statistics were reset."
            )
            findings = {
                'status': 'success',
                'message': 'No queries found',
                'data': []
            }
            return builder.build(), findings

        # Display results with truncated queries for readability
        # (Full queries are preserved in structured findings)
        formatter = AsciiDocFormatter()
        truncated_display = formatter.format_table_with_truncation(
            raw_result,
            truncate_fields={'query': 120}
        )
        builder.text(truncated_display)
        builder.blank()

        # Analyze results for key insights
        total_queries = len(raw_result)

        # Configurable CPU threshold (default 5%, but can be lowered for stricter monitoring)
        cpu_threshold = settings.get('high_cpu_threshold_percent', 5.0)

        high_cpu_queries = [
            q for q in raw_result
            if q.get('percent_of_total_cluster_cpu', 0) and
            float(q['percent_of_total_cluster_cpu']) > cpu_threshold
        ]

        io_bound_queries = [
            q for q in raw_result
            if q.get('io_wait_percent_of_total', 0) and
            float(q['io_wait_percent_of_total']) > 20.0
        ]

        low_cache_queries = [
            q for q in raw_result
            if q.get('cache_hit_rate_percent') is not None and
            float(q.get('cache_hit_rate_percent', 100)) < 90.0
        ]

        temp_file_queries = [
            q for q in raw_result
            if q.get('total_temp_written_mb', 0) and
            float(q['total_temp_written_mb']) > 100.0
        ]

        # Add key insights
        builder.text("*Key Insights:*")
        builder.blank()

        if high_cpu_queries:
            builder.text(
                f"⚠️  **High CPU Impact**: {len(high_cpu_queries)} "
                f"{'query' if len(high_cpu_queries) == 1 else 'queries'} consuming "
                f">{cpu_threshold}% of total cluster CPU"
            )
            for q in high_cpu_queries[:3]:  # Show top 3
                cpu_pct = q.get('percent_of_total_cluster_cpu', 0)
                user = q.get('username', 'unknown')
                builder.text(f"  • {user}: {cpu_pct}% of cluster CPU")
        else:
            builder.text(f"✅ No single query dominates cluster CPU (all <{cpu_threshold}%)")

        builder.blank()

        if io_bound_queries:
            builder.text(
                f"💾 **I/O Bound Queries**: {len(io_bound_queries)} "
                f"{'query' if len(io_bound_queries) == 1 else 'queries'} spending "
                f">20% of time on I/O wait"
            )
            builder.text("  → Consider index optimization or increasing `work_mem`")
        else:
            builder.text("✅ Top queries are CPU-bound (minimal I/O wait)")

        builder.blank()

        if low_cache_queries:
            builder.text(
                f"📊 **Low Cache Hit Rate**: {len(low_cache_queries)} "
                f"{'query' if len(low_cache_queries) == 1 else 'queries'} with "
                f"<90% cache hit rate"
            )
            builder.text("  → May benefit from increased `shared_buffers` or index optimization")
        else:
            builder.text("✅ Good cache hit rates across top queries (>90%)")

        builder.blank()

        if temp_file_queries:
            builder.text(
                f"🗂️  **Temp File Usage**: {len(temp_file_queries)} "
                f"{'query' if len(temp_file_queries) == 1 else 'queries'} writing "
                f">100MB to temp files"
            )
            builder.text("  → Increase `work_mem` to avoid disk spills")
        else:
            builder.text("✅ Minimal temp file usage in top queries")

        builder.blank()

        # Add recommendations
        recommendations = {
            "high": []
        }

        if high_cpu_queries:
            recommendations["high"].extend([
                "Optimization Prioritization Strategy:",
                f"  1. Start with queries consuming >{cpu_threshold}% of cluster CPU",
                "  2. Calculate optimization ROI: CPU_impact × execution_frequency",
                "  3. Run EXPLAIN (ANALYZE, BUFFERS) on identified queries",
                "  4. Look for sequential scans, nested loops, or hash joins on large tables",
                ""
            ])

        if io_bound_queries or temp_file_queries:
            recommendations["high"].extend([
                "Memory Configuration Tuning:",
                "  • Run EXPLAIN (ANALYZE, BUFFERS) to verify disk spills",
                "  • Look for 'Sort Method: external merge Disk' in plan output",
                "  • Consider increasing work_mem for specific roles:",
                "    ALTER ROLE <username> SET work_mem = '64MB';",
                "  • Monitor temp file writes: pg_stat_database.temp_bytes",
                ""
            ])

        if low_cache_queries:
            recommendations["high"].extend([
                "Cache Optimization:",
                "  • Queries with <90% cache hit may need index tuning",
                "  • Verify index coverage: check for sequential scans in EXPLAIN output",
                "  • Consider partial indexes for frequently filtered queries",
                "  • Review shared_buffers size (typical: 25% of system RAM)",
                ""
            ])

        recommendations["general"] = [
            "Understanding the Metrics:",
            "  • percent_of_total_cluster_cpu: Query's share of ALL cluster CPU",
            "    → Use this to prioritize optimization efforts",
            "  • calls_per_hour: Execution frequency",
            "    → High-frequency queries have greater optimization ROI",
            "  • io_wait_percent_of_total: I/O wait as % of query time",
            "    → >20% suggests I/O-bound (index/storage optimization)",
            "    → <5% suggests CPU-bound (query rewrite/tuning)",
            "  • cache_hit_rate_percent: % of blocks found in cache",
            "    → >95% is excellent, 90-95% is good, <90% needs investigation",
            "",
            "Temporal Context:",
            "  • stats_collection_start_time: When statistics were last reset",
            "    → Fresh stats (< 1 day) may not represent typical workload",
            "    → Stale stats (> 30 days) may include obsolete queries",
            "  • Ideal collection window: 7-14 days for stable workload patterns",
            "",
            "Query Attribution:",
            "  • username: Database role executing the query",
            "    → Application roles help identify service ownership",
            "    → Monitoring tools (datadog, etc.) may appear in top consumers",
            "",
            "Next Steps:",
            f"  1. Identify queries with >{cpu_threshold}% cluster CPU impact",
            "  2. Review query frequency to calculate optimization value",
            "  3. Run EXPLAIN (ANALYZE, BUFFERS) on target queries",
            "  4. Create indexes for sequential scans on large tables",
            "  5. Rewrite complex queries (subqueries → CTEs, OR → UNION ALL)",
            "  6. Monitor results: compare before/after statistics"
        ]

        builder.recs(recommendations)

        # Build structured findings
        findings = {
            'status': 'success',
            'message': f'Analyzed top {total_queries} CPU-consuming queries',
            'data': {
                'queries': raw_result,
                'analysis': {
                    'total_queries_analyzed': total_queries,
                    'high_cpu_impact_count': len(high_cpu_queries),
                    'io_bound_count': len(io_bound_queries),
                    'low_cache_hit_count': len(low_cache_queries),
                    'temp_file_heavy_count': len(temp_file_queries)
                }
            },
            'metadata': {
                'check': 'comprehensive_query_analysis',
                'requires_extension': 'pg_stat_statements',
                'postgres_version': connector.version_info.get('version', 'unknown')
            }
        }

        # Add high-impact queries to findings for rule evaluation
        if high_cpu_queries:
            findings['data']['high_cpu_queries'] = high_cpu_queries

        if io_bound_queries:
            findings['data']['io_bound_queries'] = io_bound_queries

        if low_cache_queries:
            findings['data']['low_cache_queries'] = low_cache_queries

        if temp_file_queries:
            findings['data']['temp_file_queries'] = temp_file_queries

        return builder.build(), findings

    except Exception as e:
        builder.error(f"Failed during comprehensive query analysis: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': []
        }
        return builder.build(), findings

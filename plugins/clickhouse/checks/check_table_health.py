"""
ClickHouse Table Health Check

Monitors table health, part counts, merge operations, and storage efficiency.
Equivalent to OpenSearch's index health check.

Requirements:
- ClickHouse client access to system tables
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_table_stats

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 9  # High priority - table health is critical


def run_check_table_health(connector, settings):
    """
    Monitor table health, part distribution, and storage efficiency.

    Args:
        connector: ClickHouse connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add check header
    builder.h3("Table Health & Storage Analysis")
    builder.para(
        "Analysis of table health, part distribution, merge operations, and storage efficiency metrics."
    )

    try:
        # 1. Get table information using qrylib
        tables_query = qry_table_stats.get_table_summary_query(connector)
        tables_result = connector.execute_query(tables_query)

        if not tables_result:
            builder.note("No user tables found or all tables are non-MergeTree engines.")
            structured_data["table_health"] = {"status": "success", "tables": 0}
            return builder.build(), structured_data

        # 2. Get merge operations using qrylib
        merges_query = qry_table_stats.get_active_merges_query(connector)
        merges_result = connector.execute_query(merges_query)

        # 3. Get parts information for detailed analysis using qrylib
        parts_query = qry_table_stats.get_table_parts_query(connector)
        parts_result = connector.execute_query(parts_query)

        # 4. Analyze tables
        total_tables = len(tables_result)
        large_tables = []
        excessive_parts = []
        inactive_parts_tables = []

        for row in tables_result:
            table_info = {
                'database': row[0],
                'name': row[1],
                'engine': row[2],
                'rows': row[3] if row[3] else 0,
                'bytes': row[4] if row[4] else 0,
                'parts': row[5] if row[5] else 0,
                'active_parts': row[6] if row[6] else 0
            }

            # Check for large tables (>100GB)
            if table_info['bytes'] > 100 * 1024**3:
                large_tables.append(table_info)

            # Check for excessive parts (>100 parts may indicate merge issues)
            if table_info['active_parts'] > 100:
                excessive_parts.append(table_info)

        # Process parts information
        if parts_result:
            for row in parts_result:
                # Row schema: database, table, active_parts, total_rows, total_bytes,
                # size_readable, oldest_partition, newest_partition, active_count, inactive_count
                if row[9] > 0:  # inactive_count > 0
                    inactive_parts_tables.append({
                        'database': row[0],
                        'table': row[1],
                        'inactive_parts': row[9],
                        'total_parts': row[2]
                    })

        # 5. Display warnings for excessive parts
        if excessive_parts:
            builder.h4("⚠️ Tables with Excessive Parts")
            builder.warning(
                f"**{len(excessive_parts)} table(s) have high part counts**\n\n"
                "Excessive parts can impact query performance and indicate merge backlog or misconfiguration."
            )
            parts_table = []
            for table in excessive_parts[:10]:  # Show top 10
                parts_table.append({
                    "Database": table['database'],
                    "Table": table['name'],
                    "Active Parts": table['active_parts'],
                    "Total Size": f"{table['bytes'] / (1024**3):.2f} GB",
                    "Rows": f"{table['rows']:,}"
                })
            builder.table(parts_table)
            if len(excessive_parts) > 10:
                builder.para(f"...and {len(excessive_parts) - 10} more tables with excessive parts")
            builder.blank()

        # 6. Display inactive parts warning
        if inactive_parts_tables:
            builder.h4("⚠️ Tables with Inactive Parts")
            builder.warning(
                f"**{len(inactive_parts_tables)} table(s) have inactive parts**\n\n"
                "Inactive parts consume disk space and should be cleaned up."
            )
            inactive_table = []
            for table in inactive_parts_tables[:10]:
                inactive_table.append({
                    "Database": table['database'],
                    "Table": table['table'],
                    "Inactive Parts": table['inactive_parts'],
                    "Total Parts": table['total_parts']
                })
            builder.table(inactive_table)
            builder.blank()

        # 7. Display active merges
        if merges_result and len(merges_result) > 0:
            builder.h4("Active Merge Operations")
            builder.para(f"**{len(merges_result)} merge operation(s) currently in progress**")

            merges_table = []
            for row in merges_result:
                merges_table.append({
                    "Database": row[0],
                    "Table": row[1],
                    "Elapsed (s)": f"{row[2]:.1f}",
                    "Progress": f"{row[3] * 100:.1f}%",
                    "Parts": row[4],
                    "Size (GB)": f"{row[5] / (1024**3):.2f}",
                    "Memory (MB)": f"{row[6] / (1024**2):.2f}"
                })
            builder.table(merges_table)
            builder.blank()

        # 8. Summary statistics
        builder.h4("Table Statistics Summary")

        total_rows = sum(t[3] if t[3] else 0 for t in tables_result)
        total_bytes = sum(t[4] if t[4] else 0 for t in tables_result)

        summary_data = [
            {"Metric": "Total Tables", "Value": total_tables},
            {"Metric": "Total Rows", "Value": f"{total_rows:,}"},
            {"Metric": "Total Storage", "Value": f"{total_bytes / (1024**3):.2f} GB"},
            {"Metric": "Large Tables (>100GB)", "Value": f"{len(large_tables)}"},
            {"Metric": "Tables with Excessive Parts", "Value": f"{len(excessive_parts)} ⚠️" if excessive_parts else "0"},
            {"Metric": "Active Merges", "Value": f"{len(merges_result) if merges_result else 0}"}
        ]
        builder.table(summary_data)
        builder.blank()

        # 9. Top tables by size
        builder.h4("Top 10 Largest Tables")

        if tables_result:
            top_tables_data = []
            for row in tables_result[:10]:
                top_tables_data.append({
                    "Database": row[0],
                    "Table": row[1],
                    "Engine": row[2],
                    "Rows": f"{(row[3] if row[3] else 0):,}",
                    "Size (GB)": f"{(row[4] if row[4] else 0) / (1024**3):.2f}",
                    "Parts": row[6] if row[6] else 0
                })
            builder.table(top_tables_data)
        else:
            builder.para("No table data available.")
        builder.blank()

        # 10. Top tables by part count
        if parts_result:
            builder.h4("Top Tables by Part Count")
            parts_count_table = []
            for row in parts_result[:10]:
                parts_count_table.append({
                    "Database": row[0],
                    "Table": row[1],
                    "Parts": row[2],
                    "Inactive": row[6],
                    "Rows": f"{row[3]:,}",
                    "Size (GB)": f"{row[4] / (1024**3):.2f}"
                })
            builder.table(parts_count_table)
            builder.blank()

        # 11. Recommendations
        recommendations = _generate_table_recommendations(
            excessive_parts,
            large_tables,
            inactive_parts_tables,
            merges_result,
            total_tables
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        elif total_tables > 0:
            builder.success("✅ All tables are healthy with proper part management.")

        # 12. Structured data
        structured_data["table_health"] = {
            "status": "success",
            "total_tables": total_tables,
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "large_tables": len(large_tables),
            "excessive_parts": len(excessive_parts),
            "active_merges": len(merges_result) if merges_result else 0,
            "warnings": len(excessive_parts) + len(inactive_parts_tables)
        }

    except Exception as e:
        logger.error(f"Table health check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["table_health"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _generate_table_recommendations(excessive_parts, large_tables, inactive_parts_tables, merges_result, total_tables):
    """Generate recommendations based on table health analysis."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if excessive_parts:
        recs["high"].extend([
            f"{len(excessive_parts)} table(s) have excessive parts (>100) - review merge settings",
            "Consider running OPTIMIZE TABLE on tables with high part counts",
            "Check background_pool_size and max_bytes_to_merge_at_max_space_in_pool settings",
            "Review insert patterns - too many small inserts can create excessive parts"
        ])

    if inactive_parts_tables:
        recs["high"].extend([
            "Clean up inactive parts to free disk space",
            "Inactive parts may indicate failed mutations or ALTER operations",
            "Review system.mutations for stuck operations"
        ])

    if large_tables:
        recs["high"].extend([
            f"{len(large_tables)} table(s) exceed 100GB - consider partitioning strategies",
            "Review data retention policies for large tables",
            "Consider using TTL for automatic data expiration",
            "Evaluate if old data can be moved to cold storage or archived"
        ])

    if merges_result and len(merges_result) > 10:
        recs["high"].append(
            f"High number of concurrent merges ({len(merges_result)}) - may impact query performance"
        )

    # General recommendations
    recs["general"].extend([
        "Monitor part counts regularly - aim for <100 parts per partition",
        "Use appropriate partitioning keys for time-series data",
        "Set up scheduled OPTIMIZE TABLE for tables with frequent updates/deletes",
        "Monitor merge performance via system.merges table",
        "Configure merge_with_ttl_timeout for tables using TTL",
        "Review index_granularity settings for query performance vs storage tradeoff"
    ])

    if total_tables > 1000:
        recs["general"].append(
            f"Cluster has {total_tables} tables - consider database organization strategies"
        )

    return recs

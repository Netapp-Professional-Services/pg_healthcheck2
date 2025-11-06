"""
ClickHouse Disk Usage Check

Monitors disk space utilization, storage policies, and capacity planning.
Equivalent to OpenSearch's disk usage check.

Requirements:
- ClickHouse client access to system tables
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_disk_usage

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 9  # High priority - disk space is critical


def run_check_disk_usage(connector, settings):
    """
    Monitor disk space utilization and storage efficiency.

    Args:
        connector: ClickHouse connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add check header
    builder.h3("Disk Usage & Storage Analysis")
    builder.para(
        "Analysis of disk space utilization, storage policies, and capacity trends across all disks."
    )

    try:
        # 1. Get disk information from system.disks using qrylib
        disks_query = qry_disk_usage.get_disk_usage_query(connector)
        disks_result = connector.execute_query(disks_query)

        # 2. Get database storage breakdown using qrylib
        database_storage_query = qry_disk_usage.get_database_disk_usage_query(connector)
        db_storage = connector.execute_query(database_storage_query)

        # 3. Get table storage details using qrylib
        table_storage_query = qry_disk_usage.get_table_disk_usage_query(connector)
        table_storage = connector.execute_query(table_storage_query)

        # 4. Get storage summary using qrylib
        storage_summary_query = qry_disk_usage.get_storage_summary_query(connector)
        storage_summary = connector.execute_query(storage_summary_query)

        # Analyze disk usage
        critical_disks = []
        warning_disks = []
        disk_summary = []

        warning_threshold = settings.get('disk_warning_percent', 80)
        critical_threshold = settings.get('disk_critical_percent', 90)

        if disks_result:
            for row in disks_result:
                disk_name = row[0]
                path = row[1]
                free_space = row[2]
                total_space = row[3]
                keep_free = row[4]

                if total_space > 0:
                    used_space = total_space - free_space
                    used_percent = (used_space / total_space) * 100
                    available_after_keep_free = free_space - keep_free if keep_free else free_space

                    disk_info = {
                        'name': disk_name,
                        'path': path,
                        'total_gb': total_space / (1024**3),
                        'used_gb': used_space / (1024**3),
                        'free_gb': free_space / (1024**3),
                        'used_percent': used_percent,
                        'keep_free_gb': keep_free / (1024**3) if keep_free else 0
                    }

                    disk_summary.append(disk_info)

                    if used_percent >= critical_threshold:
                        critical_disks.append(disk_info)
                    elif used_percent >= warning_threshold:
                        warning_disks.append(disk_info)

        # 6. Display critical disk space issues
        if critical_disks:
            builder.h4("🔴 Critical Disk Space Issues")
            builder.critical(
                f"**{len(critical_disks)} disk(s) critically low on space**\n\n"
                "Immediate action required to prevent service disruption."
            )
            critical_table = []
            for disk in critical_disks:
                critical_table.append({
                    "Disk": disk['name'],
                    "Path": disk['path'],
                    "Usage": f"{disk['used_percent']:.1f}%",
                    "Used": f"{disk['used_gb']:.2f} GB",
                    "Free": f"{disk['free_gb']:.2f} GB",
                    "Total": f"{disk['total_gb']:.2f} GB"
                })
            builder.table(critical_table)
            builder.blank()

        if warning_disks:
            builder.h4("⚠️ Disk Space Warnings")
            builder.warning(
                f"**{len(warning_disks)} disk(s) approaching capacity**\n\n"
                "Monitor closely and plan for capacity expansion."
            )
            warning_table = []
            for disk in warning_disks:
                warning_table.append({
                    "Disk": disk['name'],
                    "Path": disk['path'],
                    "Usage": f"{disk['used_percent']:.1f}%",
                    "Free": f"{disk['free_gb']:.2f} GB"
                })
            builder.table(warning_table)
            builder.blank()

        # 7. Disk summary
        builder.h4("Disk Space Summary")

        if disk_summary:
            summary_table = []
            for disk in disk_summary:
                status = "✅"
                if disk['used_percent'] >= critical_threshold:
                    status = "🔴"
                elif disk['used_percent'] >= warning_threshold:
                    status = "⚠️"

                summary_table.append({
                    "Status": status,
                    "Disk": disk['name'],
                    "Path": disk['path'],
                    "Total": f"{disk['total_gb']:.2f} GB",
                    "Used": f"{disk['used_gb']:.2f} GB",
                    "Free": f"{disk['free_gb']:.2f} GB",
                    "Usage %": f"{disk['used_percent']:.1f}%"
                })
            builder.table(summary_table)
        else:
            builder.para("No disk information available.")

        builder.blank()

        # 8. Storage by disk distribution
        # TODO: Add query for storage_by_disk from system.parts grouped by disk_name
        # Currently skipped as query is not yet implemented
        storage_by_disk = []  # Placeholder until query is implemented
        if storage_by_disk and len(storage_by_disk) > 0:
            builder.h4("Data Distribution Across Disks")
            disk_dist_table = []
            for row in storage_by_disk:
                disk_dist_table.append({
                    "Disk": row[0],
                    "Data Size": f"{row[1] / (1024**3):.2f} GB",
                    "Parts": f"{row[2]:,}"
                })
            builder.table(disk_dist_table)
            builder.blank()

        # 9. Database storage breakdown
        if db_storage and len(db_storage) > 0:
            builder.h4("Storage by Database")

            # Row schema: database, table_count, total_bytes, size_readable, size_gb, total_rows, ...
            total_storage = sum(row[2] for row in db_storage if row[2])  # total_bytes
            db_table = []
            for row in db_storage:
                db_size_bytes = row[2] if row[2] else 0  # total_bytes
                percent_of_total = (db_size_bytes / total_storage * 100) if total_storage > 0 else 0

                db_table.append({
                    "Database": row[0],
                    "Size (GB)": f"{db_size_bytes / (1024**3):.2f}",
                    "% of Total": f"{percent_of_total:.1f}%",
                    "Tables": f"{row[1]:,}",  # table_count
                    "Rows": f"{row[5]:,}"  # total_rows
                })
            builder.table(db_table)
            builder.blank()

        # 10. Top tables by storage
        if table_storage and len(table_storage) > 0:
            builder.h4("Top 20 Tables by Storage")

            # Row schema: database, table, part_count, total_bytes, size_readable, size_gb, total_rows, ...
            table_table = []
            for row in table_storage:
                table_table.append({
                    "Database": row[0],
                    "Table": row[1],
                    "Size (GB)": f"{row[3] / (1024**3):.2f}",  # total_bytes
                    "Rows": f"{row[6]:,}",  # total_rows
                    "Parts": f"{row[2]:,}"  # part_count
                })
            builder.table(table_table)
            builder.blank()

        # 11. Calculate growth rate if possible
        # Note: This would require historical data, which we'd track over time
        # For now, provide capacity planning recommendations

        # 12. Recommendations
        recommendations = _generate_disk_recommendations(
            critical_disks,
            warning_disks,
            disk_summary,
            db_storage,
            table_storage
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        elif disk_summary:
            builder.success("✅ Disk space is healthy across all disks.")

        # 13. Structured data
        total_used_gb = sum(d['used_gb'] for d in disk_summary)
        total_capacity_gb = sum(d['total_gb'] for d in disk_summary)
        avg_usage_percent = (total_used_gb / total_capacity_gb * 100) if total_capacity_gb > 0 else 0

        structured_data["disk_usage"] = {
            "status": "success",
            "disks": len(disk_summary),
            "total_capacity_gb": round(total_capacity_gb, 2),
            "total_used_gb": round(total_used_gb, 2),
            "avg_usage_percent": round(avg_usage_percent, 1),
            "critical_disks": len(critical_disks),
            "warning_disks": len(warning_disks)
        }

    except Exception as e:
        logger.error(f"Disk usage check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["disk_usage"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _generate_disk_recommendations(critical_disks, warning_disks, disk_summary, db_storage, table_storage):
    """Generate recommendations based on disk usage analysis."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if critical_disks:
        recs["critical"].extend([
            f"{len(critical_disks)} disk(s) critically low on space - immediate action required",
            "Free up space by dropping old partitions or archiving data",
            "Review and clean up temporary files and old parts",
            "Consider adding storage capacity or scaling to additional nodes"
        ])

    if warning_disks:
        recs["high"].extend([
            f"{len(warning_disks)} disk(s) approaching capacity - plan for expansion",
            "Review data retention policies and implement TTL where appropriate",
            "Identify and archive or drop unused tables",
            "Monitor disk growth trends to predict capacity needs"
        ])

    if table_storage and len(table_storage) > 0:
        # Check if top table is disproportionately large
        largest_table_gb = table_storage[0][2] / (1024**3) if table_storage[0][2] else 0
        if largest_table_gb > 1000:  # >1TB
            recs["high"].append(
                f"Largest table is {largest_table_gb:.0f}GB - consider partitioning or sharding strategies"
            )

    # General recommendations
    recs["general"].extend([
        "Set up monitoring and alerting for disk space (>80% usage)",
        "Implement data lifecycle management with TTL for time-series data",
        "Use OPTIMIZE TABLE to reclaim space from deleted data",
        "Review compression codecs - use ZSTD for better compression ratios",
        "Configure multiple storage policies for hot/warm/cold data tiering",
        "Monitor keep_free_space settings to ensure ClickHouse can operate",
        "Use partitioning to make old data deletion more efficient",
        "Consider using S3 or object storage for cold data archival"
    ])

    if disk_summary and len(disk_summary) > 1:
        recs["general"].append(
            "With multiple disks, consider configuring storage policies for better space utilization"
        )

    return recs

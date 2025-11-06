"""
ClickHouse Backup Monitoring Check

Monitors backup operations from system.backups table for data protection validation.
Critical for ensuring disaster recovery capabilities.

Requirements:
- ClickHouse client access to system.backups
- Backup operations must be logged to system.backups
"""

import logging
from typing import Dict, Tuple
from datetime import datetime, timezone
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_backups

logger = logging.getLogger(__name__)


# Check metadata for requirements
check_metadata = {
    'requires_api': False,
    'requires_ssh': False,
    'requires_connection': True,
    'description': 'Backup monitoring and validation from system.backups'
}


def get_weight():
    """Returns the importance score for this check."""
    return 10  # Highest priority - backup validation is critical for data protection


def run_check_backups(connector, settings) -> Tuple[str, Dict]:
    """
    Monitor backup operations and validate backup health.

    Returns structured data compatible with trend analysis:
    {
        "backup_summary": {
            "status": "success",
            "data": [
                {
                    "name": "backup_20250115",
                    "status": "BACKUP_COMPLETE",
                    "start_time": "2025-01-15T10:00:00Z",
                    "duration_seconds": 300,
                    "total_size_bytes": 1073741824,
                    "compressed_size_bytes": 536870912
                }
            ],
            "metadata": {
                "total_backups": 10,
                "successful_backups": 9,
                "failed_backups": 1,
                "hours_since_last_backup": 2,
                "timestamp": "2025-01-15T12:00:00Z"
            }
        }
    }

    Args:
        connector: ClickHouse connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("Backup Monitoring & Validation")
    builder.para(
        "Analysis of backup operations from system.backups for data protection verification."
    )

    try:
        # Configuration thresholds
        max_backup_age_hours = settings.get('max_backup_age_hours', 24)
        backup_analysis_days = settings.get('backup_analysis_days', 7)

        # 1. Get backup summary using qrylib
        summary_query = qry_backups.get_backup_summary_query(connector, days=backup_analysis_days)
        summary_result = connector.execute_query(summary_query)

        # 2. Get recent backups using qrylib
        recent_query = qry_backups.get_recent_backups_query(connector, limit=10)
        recent_result = connector.execute_query(recent_query)

        # 3. Get failed backups using qrylib
        failed_query = qry_backups.get_failed_backups_query(connector, days=backup_analysis_days)
        failed_result = connector.execute_query(failed_query)

        # 4. Get backup age using qrylib
        age_query = qry_backups.get_backup_age_query(connector)
        age_result = connector.execute_query(age_query)

        # Check if backups table has any data
        if not recent_result or len(recent_result) == 0:
            builder.warning(
                "⚠️ **No Backup Data Available**\n\n"
                "The system.backups table is empty. Either:\n"
                "- No backups have been performed yet\n"
                "- Backup operations are not being logged to system.backups\n"
                "- The table is not accessible"
            )
            structured_data["backup_summary"] = {
                "status": "warning",
                "data": [],
                "metadata": {
                    "total_backups": 0,
                    "message": "No backup data available"
                }
            }
            return builder.build(), structured_data

        # Process summary statistics
        summary_stats = {}
        if summary_result and len(summary_result) > 0:
            row = summary_result[0]
            summary_stats = {
                'total_backups': row[0],
                'successful_backups': row[1],
                'failed_backups': row[2],
                'cancelled_backups': row[3],
                'total_backup_size': row[4] if row[4] else 0,
                'total_compressed_size': row[5] if row[5] else 0,
                'avg_duration_seconds': row[6] if row[6] else 0,
                'max_duration_seconds': row[7] if row[7] else 0,
                'oldest_backup_time': row[8],
                'newest_backup_time': row[9]
            }

        # Process recent backups
        recent_backups = []
        if recent_result:
            for row in recent_result:
                backup_info = {
                    'id': row[0],
                    'name': row[1],
                    'status': row[2],
                    'error': row[3] if len(row) > 3 else '',
                    'start_time': row[4].isoformat() if row[4] else None,
                    'end_time': row[5].isoformat() if row[5] else None,
                    'num_files': row[6] if len(row) > 6 else 0,
                    'total_size': row[7] if len(row) > 7 else 0,
                    'duration_seconds': (row[5] - row[4]).total_seconds() if row[5] and row[4] else 0
                }
                recent_backups.append(backup_info)

        # Process failed backups
        failed_backups = []
        if failed_result:
            for row in failed_result:
                failed_info = {
                    'id': row[0],
                    'name': row[1],
                    'status': row[2],
                    'error': row[3],
                    'start_time': row[4].isoformat() if row[4] else None
                }
                failed_backups.append(failed_info)

        # Calculate backup age
        hours_since_last_backup = None
        days_since_last_backup = None
        if age_result and len(age_result) > 0:
            hours_since_last_backup = age_result[0][4] if len(age_result[0]) > 4 else None
            days_since_last_backup = age_result[0][5] if len(age_result[0]) > 5 else None

        # Display backup health status
        critical_issues = []
        warning_issues = []

        if failed_backups:
            critical_issues.append(f"{len(failed_backups)} failed backup(s)")

        if hours_since_last_backup is not None and hours_since_last_backup > max_backup_age_hours:
            critical_issues.append(f"Last successful backup was {hours_since_last_backup} hours ago")

        if summary_stats.get('total_backups', 0) == 0:
            warning_issues.append("No backups recorded in analysis period")

        # Display status
        if critical_issues:
            builder.critical(
                "🔴 **Critical Backup Issues Detected**\n\n" +
                "\n".join(f"- {issue}" for issue in critical_issues)
            )
        elif warning_issues:
            builder.warning(
                "⚠️ **Backup Warnings**\n\n" +
                "\n".join(f"- {issue}" for issue in warning_issues)
            )
        else:
            builder.note("✅ **Backup Health is Good**")

        builder.blank()

        # Display backup summary
        if summary_stats:
            builder.h4("Backup Summary (Last {} Days)".format(backup_analysis_days))

            summary_table = [
                {"Metric": "Total Backups", "Value": f"{summary_stats['total_backups']:,}"},
                {"Metric": "Successful", "Value": f"{summary_stats['successful_backups']:,}"},
                {"Metric": "Failed", "Value": f"{summary_stats['failed_backups']:,} 🔴" if summary_stats['failed_backups'] > 0 else "0"},
                {"Metric": "Cancelled", "Value": f"{summary_stats['cancelled_backups']:,}"},
                {"Metric": "Total Backup Size", "Value": f"{summary_stats['total_backup_size'] / (1024**3):.2f} GB"},
                {"Metric": "Total Compressed Size", "Value": f"{summary_stats['total_compressed_size'] / (1024**3):.2f} GB"},
                {"Metric": "Avg Duration", "Value": f"{summary_stats['avg_duration_seconds']:.0f} seconds"},
                {"Metric": "Max Duration", "Value": f"{summary_stats['max_duration_seconds']:.0f} seconds"}
            ]

            if hours_since_last_backup is not None:
                summary_table.append({
                    "Metric": "Time Since Last Backup",
                    "Value": f"{hours_since_last_backup} hours ({days_since_last_backup} days)"
                })

            builder.table(summary_table)
            builder.blank()

        # Display failed backups if any
        if failed_backups:
            builder.h4("🔴 Failed Backups")
            builder.critical(
                f"**{len(failed_backups)} backup(s) failed in the last {backup_analysis_days} days**\n\n"
                "Investigate backup failures to ensure data protection."
            )

            failed_table = []
            for backup in failed_backups:
                failed_table.append({
                    "Name": backup['name'],
                    "Status": backup['status'],
                    "Start Time": backup['start_time'],
                    "Error": backup['error'][:100] + "..." if len(backup['error']) > 100 else backup['error']
                })
            builder.table(failed_table)
            builder.blank()

        # Display recent backups
        if recent_backups:
            builder.h4("Recent Backup Operations")

            recent_table = []
            for backup in recent_backups:
                status_icon = "✅" if backup['status'] == 'BACKUP_COMPLETE' else "🔴"
                recent_table.append({
                    "Status": status_icon,
                    "Name": backup['name'],
                    "Start Time": backup['start_time'],
                    "Duration": f"{backup['duration_seconds']:.0f}s",
                    "Size": f"{backup['total_size'] / (1024**3):.2f} GB",
                    "Files": f"{backup['num_files']:,}"
                })
            builder.table(recent_table)
            builder.blank()

        # Generate recommendations
        recommendations = _generate_backup_recommendations(
            summary_stats,
            failed_backups,
            hours_since_last_backup,
            max_backup_age_hours
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        else:
            builder.success("✅ Backups are operating normally - data protection is active")

        # Structured data for trend analysis
        structured_data["backup_summary"] = {
            "status": "success",
            "data": recent_backups,
            "metadata": {
                "total_backups": summary_stats.get('total_backups', 0),
                "successful_backups": summary_stats.get('successful_backups', 0),
                "failed_backups": summary_stats.get('failed_backups', 0),
                "cancelled_backups": summary_stats.get('cancelled_backups', 0),
                "hours_since_last_backup": hours_since_last_backup,
                "days_since_last_backup": days_since_last_backup,
                "total_backup_size_gb": summary_stats.get('total_backup_size', 0) / (1024**3) if summary_stats.get('total_backup_size') else 0,
                "avg_duration_seconds": summary_stats.get('avg_duration_seconds', 0),
                "timestamp": connector.get_current_timestamp()
            }
        }

        if failed_backups:
            structured_data["failed_backups"] = {
                "status": "success",
                "data": failed_backups,
                "metadata": {
                    "count": len(failed_backups),
                    "timestamp": connector.get_current_timestamp()
                }
            }

    except Exception as e:
        logger.error(f"Backup monitoring check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["backup_summary"] = {
            "status": "error",
            "data": [],
            "error_message": str(e)
        }

    return builder.build(), structured_data


def _generate_backup_recommendations(summary_stats, failed_backups, hours_since_last_backup, max_backup_age_hours):
    """Generate recommendations based on backup analysis."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if failed_backups:
        recs["critical"].extend([
            f"{len(failed_backups)} backup(s) failed - investigate backup errors immediately",
            "Review backup configuration and destination accessibility",
            "Check disk space on backup destination",
            "Verify backup user permissions and credentials",
            "Review ClickHouse server logs for backup failure details"
        ])

    if hours_since_last_backup is not None and hours_since_last_backup > max_backup_age_hours:
        recs["critical"].extend([
            f"Last successful backup was {hours_since_last_backup} hours ago (threshold: {max_backup_age_hours} hours)",
            "Verify backup schedule is running",
            "Check if backup process is stuck or disabled",
            "Ensure backup automation (cron, systemd timer) is active"
        ])

    if summary_stats.get('total_backups', 0) == 0:
        recs["high"].extend([
            "No backups found in the analysis period - data protection is at risk",
            "Configure and schedule regular backups immediately",
            "Use ClickHouse BACKUP command or backup tools",
            "Test backup and restore procedures"
        ])

    if summary_stats.get('failed_backups', 0) > summary_stats.get('successful_backups', 0):
        recs["high"].append(
            f"More failed backups ({summary_stats['failed_backups']}) than successful ({summary_stats['successful_backups']}) - backup reliability is compromised"
        )

    # General recommendations
    recs["general"].extend([
        "Implement regular backup schedule (daily minimum for production)",
        "Monitor backup completion and alert on failures",
        "Test backup restore procedures regularly",
        "Store backups in separate location from primary data",
        "Implement backup retention policy based on compliance requirements",
        "Monitor backup storage capacity and plan for growth",
        "Consider incremental backups for large datasets",
        "Document backup and restore procedures",
        "Set up automated backup verification",
        "Use backup compression to reduce storage costs"
    ])

    if summary_stats.get('avg_duration_seconds', 0) > 3600:  # >1 hour
        recs["general"].append(
            f"Average backup duration is {summary_stats['avg_duration_seconds']/3600:.1f} hours - consider optimization strategies"
        )

    return recs

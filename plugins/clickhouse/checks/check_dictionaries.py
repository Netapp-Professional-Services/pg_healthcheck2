"""
ClickHouse Dictionary Health Check

Monitors dictionary status, reload performance, and potential issues.

MORE ACTIONABLE than instacollector:
- Identifies specific dictionary failures with context
- Monitors reload performance and stale dictionaries
- Tracks memory consumption and capacity issues
- Provides health scoring for prioritization

Critical because dictionary failures cause query failures.

Requirements:
- ClickHouse client access to system.dictionaries
"""

import logging
from typing import Dict, Tuple
from datetime import datetime, timezone
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_dictionaries

logger = logging.getLogger(__name__)


# Check metadata
check_metadata = {
    'requires_api': False,
    'requires_ssh': False,
    'requires_connection': True,
    'description': 'Dictionary health monitoring with failure detection and performance analysis'
}


def get_weight():
    """Returns the importance score for this check."""
    return 7  # High priority - dictionary failures impact query reliability


def run_check_dictionaries(connector, settings) -> Tuple[str, Dict]:
    """
    Monitor ClickHouse dictionary health and performance.

    Returns structured data compatible with trend analysis:
    {
        "dictionary_status": {
            "status": "success",
            "data": [
                {
                    "database": "default",
                    "name": "user_dict",
                    "status": "LOADED",
                    "element_count": 1000000,
                    "memory_bytes": 104857600
                }
            ],
            "metadata": {
                "total_dictionaries": 10,
                "loaded_count": 9,
                "failed_count": 1,
                "total_memory_bytes": 1073741824,
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

    builder.h3("Dictionary Health & Performance")
    builder.para(
        "Analysis of ClickHouse dictionaries for status monitoring, "
        "reload performance, and resource consumption."
    )

    try:
        # Configuration thresholds
        max_reload_time_seconds = settings.get('max_dictionary_reload_seconds', 300)
        max_stale_hours = settings.get('max_dictionary_stale_hours', 24)
        large_dictionary_mb = settings.get('large_dictionary_mb', 100)

        # 1. Get dictionary summary
        summary_query = qry_dictionaries.get_dictionary_summary_query(connector)
        summary_result = connector.execute_query(summary_query)

        # 2. Get dictionary health scores
        health_query = qry_dictionaries.get_dictionary_health_score_query(connector)
        health_result = connector.execute_query(health_query)

        # 3. Get failed dictionaries
        failed_query = qry_dictionaries.get_failed_dictionaries_query(connector)
        failed_result = connector.execute_query(failed_query)

        # 4. Get stale dictionaries
        stale_query = qry_dictionaries.get_stale_dictionaries_query(connector, hours=max_stale_hours)
        stale_result = connector.execute_query(stale_query)

        # 5. Get dictionary memory usage
        memory_query = qry_dictionaries.get_dictionary_memory_usage_query(connector)
        memory_result = connector.execute_query(memory_query)

        # 6. Get dictionary types
        types_query = qry_dictionaries.get_dictionary_types_query(connector)
        types_result = connector.execute_query(types_query)

        # Process summary statistics
        summary_stats = {}
        if summary_result and len(summary_result) > 0:
            row = summary_result[0]
            summary_stats = {
                'total_dictionaries': row[0],
                'loaded_count': row[1],
                'loading_count': row[2],
                'failed_count': row[3],
                'exception_count': row[4],
                'total_memory_bytes': row[5] if row[5] else 0,
                'total_memory_readable': row[6] if row[6] else '0 B',
                'total_elements': row[7] if row[7] else 0,
                'avg_loading_duration_ms': row[8] if row[8] else 0,
                'max_loading_duration_ms': row[9] if row[9] else 0
            }

        # Check if any dictionaries exist
        if summary_stats.get('total_dictionaries', 0) == 0:
            builder.note(
                "**No dictionaries configured**\\n\\n"
                "No dictionary objects found in the system. This is normal if dictionaries are not used."
            )
            structured_data["dictionary_status"] = {
                "status": "success",
                "data": [],
                "metadata": {
                    "total_dictionaries": 0,
                    "message": "No dictionaries configured",
                    "timestamp": connector.get_current_timestamp()
                }
            }
            return builder.build(), structured_data

        # Analyze dictionary health
        critical_issues = []
        warnings = []

        if summary_stats.get('failed_count', 0) > 0:
            critical_issues.append(f"{summary_stats['failed_count']} dictionary(ies) failed to load")

        if summary_stats.get('exception_count', 0) > 0:
            warnings.append(f"{summary_stats['exception_count']} dictionary(ies) have recent exceptions")

        if stale_result and len(stale_result) > 0:
            warnings.append(f"{len(stale_result)} dictionary(ies) are stale (not updated in {max_stale_hours}+ hours)")

        # Display health status
        if critical_issues:
            builder.critical(
                "🔴 **Critical Dictionary Issues Detected**\\n\\n" +
                "\\n".join(f"- {issue}" for issue in critical_issues)
            )
        elif warnings:
            builder.warning(
                "⚠️ **Dictionary Warnings**\\n\\n" +
                "\\n".join(f"- {issue}" for issue in warnings)
            )
        else:
            builder.success("✅ **All Dictionaries Healthy**")

        builder.blank()

        # Display dictionary summary
        if summary_stats:
            builder.h4("Dictionary Summary")

            summary_table = [
                {"Metric": "Total Dictionaries", "Value": f"{summary_stats['total_dictionaries']:,}"},
                {"Metric": "Loaded", "Value": f"{summary_stats['loaded_count']:,} ✅" if summary_stats['failed_count'] == 0 else f"{summary_stats['loaded_count']:,}"},
                {"Metric": "Loading", "Value": f"{summary_stats['loading_count']:,}"},
                {"Metric": "Failed", "Value": f"{summary_stats['failed_count']:,} 🔴" if summary_stats['failed_count'] > 0 else "0"},
                {"Metric": "With Exceptions", "Value": f"{summary_stats['exception_count']:,}"},
                {"Metric": "Total Memory", "Value": summary_stats['total_memory_readable']},
                {"Metric": "Total Elements", "Value": f"{summary_stats['total_elements']:,}"},
                {"Metric": "Avg Load Time", "Value": f"{summary_stats['avg_loading_duration_ms']:.0f} ms"},
                {"Metric": "Max Load Time", "Value": f"{summary_stats['max_loading_duration_ms']:.0f} ms"}
            ]

            builder.table(summary_table)
            builder.blank()

        # Display failed dictionaries
        failed_dictionaries = []
        if failed_result and len(failed_result) > 0:
            builder.h4("🔴 Failed Dictionaries")
            builder.critical(
                f"**{len(failed_result)} dictionary(ies) failed to load**\\n\\n"
                "Dictionary failures cause queries to fail. Investigate immediately."
            )

            failed_table = []
            for row in failed_result:
                exception = row[5] if len(row) > 5 else ''
                if len(exception) > 100:
                    exception = exception[:97] + "..."

                failed_table.append({
                    "Database": row[0],
                    "Name": row[1],
                    "Status": row[2],
                    "Type": row[4],
                    "Elements": f"{row[9]:,}" if row[9] else "0",
                    "Error": exception
                })

                failed_dictionaries.append({
                    'database': row[0],
                    'name': row[1],
                    'status': row[2],
                    'type': row[4],
                    'element_count': row[9] if row[9] else 0,
                    'error': exception
                })

            builder.table(failed_table)
            builder.blank()

        # Display stale dictionaries
        stale_dictionaries = []
        if stale_result and len(stale_result) > 0:
            builder.h4("⚠️ Stale Dictionaries")
            builder.warning(
                f"**{len(stale_result)} dictionary(ies) not updated in {max_stale_hours}+ hours**\\n\\n"
                "Stale dictionaries may contain outdated data."
            )

            stale_table = []
            for row in stale_result:
                stale_table.append({
                    "Database": row[0],
                    "Name": row[1],
                    "Type": row[3],
                    "Last Update": row[5],
                    "Hours Ago": f"{row[6]}",
                    "Elements": f"{row[7]:,}"
                })

                stale_dictionaries.append({
                    'database': row[0],
                    'name': row[1],
                    'type': row[3],
                    'hours_since_update': row[6],
                    'element_count': row[7]
                })

            builder.table(stale_table)
            builder.blank()

        # Display dictionary types
        if types_result and len(types_result) > 0:
            builder.h4("Dictionary Type Distribution")

            types_table = []
            for row in types_result:
                types_table.append({
                    "Type": row[0],
                    "Count": f"{row[1]:,}",
                    "Loaded": f"{row[2]:,}",
                    "Failed": f"{row[3]:,}" if row[3] > 0 else "0",
                    "Memory": row[5],
                    "Elements": f"{row[6]:,}",
                    "Avg Load (s)": f"{row[7]:.1f}"
                })

            builder.table(types_table)
            builder.blank()

        # Display top memory consumers
        memory_consumers = []
        if memory_result and len(memory_result) > 0:
            builder.h4("Top Dictionary Memory Consumers")

            memory_table = []
            for row in memory_result[:10]:  # Top 10
                memory_table.append({
                    "Database": row[0],
                    "Name": row[1],
                    "Type": row[3],
                    "Elements": f"{row[4]:,}",
                    "Memory": row[6],
                    "Per Element": f"{row[7]:.0f} bytes" if row[7] > 0 else "N/A"
                })

                memory_consumers.append({
                    'database': row[0],
                    'name': row[1],
                    'type': row[3],
                    'element_count': row[4],
                    'memory_bytes': row[5]
                })

            builder.table(memory_table)
            builder.blank()

        # Display dictionary health scores
        healthy_dicts = []
        warning_dicts = []
        critical_dicts = []

        if health_result:
            for row in health_result:
                dict_info = {
                    'database': row[0],
                    'name': row[1],
                    'status': row[2],
                    'health_status': row[3],
                    'element_count': row[7],
                    'memory_used': row[8]
                }

                if row[3] == 'critical':
                    critical_dicts.append(dict_info)
                elif row[3] == 'warning':
                    warning_dicts.append(dict_info)
                else:
                    healthy_dicts.append(dict_info)

        # Generate recommendations
        recommendations = _generate_dictionary_recommendations(
            summary_stats,
            failed_dictionaries,
            stale_dictionaries,
            memory_consumers
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        else:
            builder.success("✅ Dictionary health is good - no issues detected")

        # Structured data for trend tracking
        all_dictionaries = []
        if health_result:
            for row in health_result:
                all_dictionaries.append({
                    'database': row[0],
                    'name': row[1],
                    'status': row[2],
                    'health_status': row[3],
                    'element_count': row[7] if row[7] else 0,
                    'memory_used': row[8] if row[8] else '0 B'
                })

        structured_data["dictionary_status"] = {
            "status": "success",
            "data": all_dictionaries,
            "metadata": {
                "total_dictionaries": summary_stats.get('total_dictionaries', 0),
                "loaded_count": summary_stats.get('loaded_count', 0),
                "failed_count": summary_stats.get('failed_count', 0),
                "exception_count": summary_stats.get('exception_count', 0),
                "total_memory_bytes": summary_stats.get('total_memory_bytes', 0),
                "total_elements": summary_stats.get('total_elements', 0),
                "healthy_count": len(healthy_dicts),
                "warning_count": len(warning_dicts),
                "critical_count": len(critical_dicts),
                "timestamp": connector.get_current_timestamp()
            }
        }

        if failed_dictionaries:
            structured_data["failed_dictionaries"] = {
                "status": "success",
                "data": failed_dictionaries,
                "metadata": {
                    "count": len(failed_dictionaries),
                    "timestamp": connector.get_current_timestamp()
                }
            }

        if stale_dictionaries:
            structured_data["stale_dictionaries"] = {
                "status": "success",
                "data": stale_dictionaries,
                "metadata": {
                    "count": len(stale_dictionaries),
                    "max_hours": max_stale_hours,
                    "timestamp": connector.get_current_timestamp()
                }
            }

    except Exception as e:
        logger.error(f"Dictionary health check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["dictionary_status"] = {
            "status": "error",
            "data": [],
            "error_message": str(e)
        }

    return builder.build(), structured_data


def _generate_dictionary_recommendations(summary_stats, failed_dicts, stale_dicts, memory_consumers):
    """Generate actionable dictionary recommendations."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if failed_dicts:
        recs["critical"].extend([
            f"{len(failed_dicts)} dictionary(ies) failed to load - queries using these dictionaries will fail",
            "Review error messages for each failed dictionary",
            "Check source system connectivity and credentials",
            "Verify dictionary SQL queries are valid",
            "Check for schema changes in source systems",
            "Review dictionary configuration syntax"
        ])

    if stale_dicts:
        recs["high"].extend([
            f"{len(stale_dicts)} dictionary(ies) contain stale data",
            "Verify dictionary reload schedule is configured correctly",
            "Check if source system is updating data",
            "Review dictionary lifetime settings",
            "Consider implementing monitoring alerts for stale dictionaries"
        ])

    if summary_stats.get('exception_count', 0) > 0:
        recs["high"].append(
            f"{summary_stats['exception_count']} dictionary(ies) have recent exceptions - review logs"
        )

    # Memory-based recommendations
    total_memory_gb = summary_stats.get('total_memory_bytes', 0) / (1024**3)
    if total_memory_gb > 10:
        recs["high"].extend([
            f"Dictionaries consuming {total_memory_gb:.1f} GB of memory",
            "Review dictionary sizes for optimization opportunities",
            "Consider using flat dictionaries for smaller datasets",
            "Implement dictionary partitioning for large datasets"
        ])

    # General best practices
    recs["general"].extend([
        "Monitor dictionary reload times to catch performance degradation",
        "Implement alerting for dictionary load failures",
        "Document dictionary data sources and dependencies",
        "Test dictionary reloads in staging before production",
        "Use appropriate dictionary types (flat, hashed, cache) for use case",
        "Set appropriate lifetime values for dictionary refresh",
        "Consider using SSD storage for dictionary data",
        "Monitor source system availability for external dictionaries",
        "Implement retry logic for transient dictionary load failures",
        "Document dictionary schema dependencies for change management"
    ])

    if memory_consumers and len(memory_consumers) > 5:
        recs["general"].append(
            "Multiple large dictionaries detected - review memory capacity planning"
        )

    return recs

"""
ClickHouse Error Tracking Check

Monitors errors from system.errors and system.error_log tables for problem diagnosis.
Critical for identifying issues that may impact cluster health and query execution.

Requirements:
- ClickHouse client access to system.errors
- Optional: system.error_log (if logging is enabled)
"""

import logging
from typing import Dict, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_errors

logger = logging.getLogger(__name__)


# Check metadata for requirements
check_metadata = {
    'requires_api': False,
    'requires_ssh': False,
    'requires_connection': True,
    'description': 'Error tracking and diagnostics from system.errors and system.error_log'
}


def get_weight():
    """Returns the importance score for this check."""
    return 10  # Highest priority - error tracking is critical for diagnostics


def _categorize_error(error_name):
    """
    Categorize an error by its name pattern for filtering and analysis.

    Args:
        error_name: Error name from system.errors

    Returns:
        str: Category name (memory, network, replication, corruption, keeper, query, resource, other)
    """
    error_upper = error_name.upper()

    if any(pattern in error_upper for pattern in ['MEMORY', 'ALLOCATE', 'OUT_OF_MEMORY']):
        return 'memory'
    elif any(pattern in error_upper for pattern in ['NETWORK', 'CONNECTION', 'SOCKET', 'TIMEOUT']):
        return 'network'
    elif any(pattern in error_upper for pattern in ['REPLICA', 'REPLICATION', 'QUORUM']):
        return 'replication'
    elif any(pattern in error_upper for pattern in ['CORRUPT', 'CHECKSUM', 'DAMAGED']):
        return 'corruption'
    elif any(pattern in error_upper for pattern in ['KEEPER', 'ZOOKEEPER', 'ZK_']):
        return 'keeper'
    elif any(pattern in error_upper for pattern in ['QUERY', 'SYNTAX', 'PARSE', 'UNKNOWN_IDENTIFIER', 'TYPE_MISMATCH']):
        return 'query'
    elif any(pattern in error_upper for pattern in ['TOO_MANY', 'LIMIT_EXCEEDED', 'QUOTA']):
        return 'resource'
    else:
        return 'other'


def _classify_severity(error_name, error_count, category):
    """
    Classify error severity based on name, count, and category.

    Args:
        error_name: Error name
        error_count: Number of occurrences
        category: Error category from _categorize_error()

    Returns:
        str: Severity level (critical, high, medium, low)
    """
    error_upper = error_name.upper()

    # Critical categories
    if category in ['corruption', 'keeper']:
        return 'critical'

    # Critical error patterns
    critical_patterns = [
        'CANNOT_ALLOCATE_MEMORY',
        'MEMORY_LIMIT_EXCEEDED',
        'CORRUPTED_DATA',
        'CHECKSUM_DOESNT_MATCH',
        'ALL_CONNECTION_TRIES_FAILED',
        'REPLICA_IS_NOT_IN_QUORUM',
        'NO_ZOOKEEPER',
        'READONLY'
    ]
    if any(pattern in error_upper for pattern in critical_patterns):
        return 'critical'

    # High severity with high count
    if error_count > 100 and category in ['memory', 'network', 'replication']:
        return 'high'

    # Medium severity
    if error_count > 10 or category in ['memory', 'network', 'replication', 'resource']:
        return 'medium'

    return 'low'


def run_check_errors(connector, settings) -> Tuple[str, Dict]:
    """
    Monitor errors and error trends for problem diagnosis.

    Returns structured data compatible with trend analysis:
    {
        "error_summary": {
            "status": "success",
            "data": [
                {
                    "name": "NETWORK_ERROR",
                    "code": 210,
                    "error_count": 5,
                    "last_error_time": "2025-01-15T10:30:00Z",
                    "last_error_message": "...",
                    "severity": "critical",
                    "category": "network",
                    "is_high_frequency": false
                }
            ],
            "metadata": {
                "total_error_types": 3,
                "total_error_count": 15,
                "critical_errors": 1,
                "timestamp": "2025-01-15T10:30:00Z"
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

    builder.h3("Error Tracking & Diagnostics")
    builder.para(
        "Analysis of errors from system.errors and system.error_log tables for problem identification."
    )

    try:
        # 1. Get error summary from system.errors
        errors_query = qry_errors.get_errors_summary_query(connector)
        errors_result = connector.execute_query(errors_query)

        # 2. Get critical errors
        critical_errors_query = qry_errors.get_critical_errors_query(connector)
        critical_errors_result = connector.execute_query(critical_errors_query)

        # 3. Try to get recent error log (may not be available if logging disabled)
        recent_errors_result = []
        try:
            recent_errors_query = qry_errors.get_recent_error_log_query(connector, hours=24)
            recent_errors_result = connector.execute_query(recent_errors_query)
        except Exception as e:
            logger.debug(f"system.error_log not available or logging disabled: {e}")

        # 4. Get top errors
        top_errors_query = qry_errors.get_top_errors_query(connector, limit=20)
        top_errors_result = connector.execute_query(top_errors_query)

        # Process results
        all_errors = []
        critical_errors = []
        total_error_count = 0

        if errors_result:
            for row in errors_result:
                error_name = row[0]
                error_count = row[2]

                # Pre-compute metadata for efficient rule filtering
                category = _categorize_error(error_name)
                severity = _classify_severity(error_name, error_count, category)

                error_info = {
                    'name': error_name,
                    'code': row[1],
                    'error_count': error_count,
                    'last_error_time': row[3].isoformat() if row[3] else None,
                    'last_error_message': row[4] if len(row) > 4 else '',
                    'last_error_trace': row[5] if len(row) > 5 else '',
                    'remote': row[6] if len(row) > 6 else False,
                    # Pre-computed metadata for rule filtering
                    'category': category,
                    'severity': severity,
                    'is_high_frequency': error_count > 100,
                    'is_critical': severity == 'critical'
                }
                all_errors.append(error_info)
                total_error_count += error_info['error_count']

        if critical_errors_result:
            for row in critical_errors_result:
                error_name = row[0]
                error_count = row[2]

                # Pre-compute metadata for efficient rule filtering
                category = _categorize_error(error_name)
                severity = _classify_severity(error_name, error_count, category)

                critical_error = {
                    'name': error_name,
                    'code': row[1],
                    'error_count': error_count,
                    'last_error_time': row[3].isoformat() if row[3] else None,
                    'last_error_message': row[4] if len(row) > 4 else '',
                    'last_error_trace': row[5] if len(row) > 5 else '',
                    # Pre-computed metadata for rule filtering
                    'category': category,
                    'severity': severity,
                    'is_high_frequency': error_count > 100,
                    'is_critical': True  # All critical_errors_result items are critical
                }
                critical_errors.append(critical_error)

                # Also add critical errors to all_errors if not already there
                # This ensures they can be matched by specific error type rules
                error_codes_in_all = {e['code'] for e in all_errors}
                if critical_error['code'] not in error_codes_in_all:
                    all_errors.append(critical_error.copy())
                    total_error_count += critical_error['error_count']

        # Display critical errors first
        if critical_errors:
            builder.h4("🔴 Critical Errors Detected")
            builder.critical(
                f"**{len(critical_errors)} critical error type(s) detected**\n\n"
                "These errors indicate serious issues requiring immediate attention."
            )

            critical_table = []
            for err in critical_errors[:10]:  # Show top 10
                critical_table.append({
                    "Error Name": err['name'],
                    "Code": str(err['code']),
                    "Count": f"{err['error_count']:,}",
                    "Last Occurrence": err['last_error_time'] if err['last_error_time'] else 'Unknown',
                    "Message": err['last_error_message'][:80] + "..." if len(err['last_error_message']) > 80 else err['last_error_message']
                })
            builder.table(critical_table)
            builder.blank()

        # Display error summary
        if all_errors:
            builder.h4("Error Summary")

            if total_error_count > 1000:
                builder.warning(
                    f"**High error volume detected: {total_error_count:,} total errors across {len(all_errors)} error types**"
                )
            else:
                builder.para(
                    f"**Total Errors:** {total_error_count:,} across {len(all_errors)} error type(s)"
                )
            builder.blank()

            # Top errors table
            if top_errors_result:
                builder.h4("Top 20 Most Frequent Errors")
                top_errors_table = []
                for row in top_errors_result:
                    top_errors_table.append({
                        "Error Name": row[0],
                        "Code": str(row[1]),
                        "Count": f"{row[2]:,}",
                        "Last Occurrence": row[3].isoformat() if row[3] else 'Unknown',
                        "Last Message": row[4][:100] + "..." if len(row[4]) > 100 else row[4]
                    })
                builder.table(top_errors_table)
                builder.blank()

            # Categorize errors by severity
            memory_errors = [e for e in all_errors if 'MEMORY' in e['name'] or 'ALLOCATE' in e['name']]
            network_errors = [e for e in all_errors if 'NETWORK' in e['name'] or 'CONNECTION' in e['name']]
            replication_errors = [e for e in all_errors if 'REPLICA' in e['name'] or 'REPLICATION' in e['name']]
            corruption_errors = [e for e in all_errors if 'CORRUPT' in e['name'] or 'CHECKSUM' in e['name']]

            # Display categorized errors
            if memory_errors:
                builder.h4("⚠️ Memory-Related Errors")
                memory_table = []
                for err in memory_errors:
                    memory_table.append({
                        "Error": err['name'],
                        "Count": f"{err['error_count']:,}",
                        "Last Occurrence": err['last_error_time'] if err['last_error_time'] else 'Unknown'
                    })
                builder.table(memory_table)
                builder.blank()

            if network_errors:
                builder.h4("⚠️ Network/Connection Errors")
                network_table = []
                for err in network_errors:
                    network_table.append({
                        "Error": err['name'],
                        "Count": f"{err['error_count']:,}",
                        "Last Occurrence": err['last_error_time'] if err['last_error_time'] else 'Unknown'
                    })
                builder.table(network_table)
                builder.blank()

            if replication_errors:
                builder.h4("⚠️ Replication Errors")
                replication_table = []
                for err in replication_errors:
                    replication_table.append({
                        "Error": err['name'],
                        "Count": f"{err['error_count']:,}",
                        "Last Occurrence": err['last_error_time'] if err['last_error_time'] else 'Unknown'
                    })
                builder.table(replication_table)
                builder.blank()

            if corruption_errors:
                builder.h4("🔴 Data Corruption Errors")
                builder.critical(
                    "**Data corruption errors detected - immediate investigation required**"
                )
                corruption_table = []
                for err in corruption_errors:
                    corruption_table.append({
                        "Error": err['name'],
                        "Count": f"{err['error_count']:,}",
                        "Last Occurrence": err['last_error_time'] if err['last_error_time'] else 'Unknown',
                        "Message": err['last_error_message'][:100] + "..." if len(err['last_error_message']) > 100 else err['last_error_message']
                    })
                builder.table(corruption_table)
                builder.blank()

        else:
            builder.success("✅ No errors recorded in system.errors")

        # Display recent error log if available
        if recent_errors_result and len(recent_errors_result) > 0:
            builder.h4("Recent Error Events (Last 24 Hours)")
            builder.para(f"Showing {len(recent_errors_result)} most recent error events from system.error_log")

            # Create code-to-name lookup from all_errors
            code_to_name = {err['code']: err['name'] for err in all_errors}

            recent_table = []
            for row in recent_errors_result[:20]:  # Show top 20
                # Row schema: event_date, event_time, code, value, remote
                error_code = row[2]
                error_name = code_to_name.get(error_code, f'Error {error_code}')

                recent_table.append({
                    "Time": row[1].isoformat() if row[1] else 'Unknown',
                    "Error Name": error_name,
                    "Code": str(error_code),
                    "Count": str(row[3]),
                    "Remote": "Yes" if row[4] else "No"
                })
            builder.table(recent_table)
            builder.blank()
        else:
            builder.note(
                "**Note:** system.error_log is not available or error logging is disabled. "
                "Enable error_log in ClickHouse configuration for detailed error tracking over time."
            )

        # Generate recommendations
        recommendations = _generate_error_recommendations(
            all_errors,
            critical_errors,
            memory_errors,
            network_errors,
            replication_errors,
            corruption_errors
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        elif all_errors:
            builder.note(
                "**Recommendation:** Monitor error trends over time and investigate recurring errors."
            )
        else:
            builder.success("✅ No errors detected - cluster is operating without recorded errors")

        # Structured data for trend analysis
        # Calculate counts by category and severity using pre-computed fields
        category_counts = {}
        severity_counts = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}

        for error in all_errors:
            category = error.get('category', 'other')
            severity = error.get('severity', 'low')
            category_counts[category] = category_counts.get(category, 0) + 1
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

        structured_data["error_summary"] = {
            "status": "success",
            "data": all_errors,
            "metadata": {
                "total_error_types": len(all_errors),
                "total_error_count": total_error_count,
                "critical_errors": len(critical_errors),
                # Legacy counts for backward compatibility
                "memory_errors": len(memory_errors),
                "network_errors": len(network_errors),
                "replication_errors": len(replication_errors),
                "corruption_errors": len(corruption_errors),
                # New category-based counts
                "category_counts": category_counts,
                "severity_counts": severity_counts,
                "high_frequency_errors": sum(1 for e in all_errors if e.get('is_high_frequency')),
                "timestamp": connector.get_current_timestamp()
            }
        }

        structured_data["critical_errors"] = {
            "status": "success",
            "data": critical_errors,
            "metadata": {
                "count": len(critical_errors),
                "timestamp": connector.get_current_timestamp()
            }
        }

    except Exception as e:
        logger.error(f"Error tracking check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["error_summary"] = {
            "status": "error",
            "data": [],
            "error_message": str(e)
        }

    return builder.build(), structured_data


def _generate_error_recommendations(
    all_errors, critical_errors, memory_errors, network_errors,
    replication_errors, corruption_errors
):
    """Generate recommendations based on error analysis."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if corruption_errors:
        recs["critical"].extend([
            f"{len(corruption_errors)} data corruption error(s) detected - immediate investigation required",
            "Run CHECK TABLE queries on affected tables",
            "Review system.part_log for merge or mutation failures",
            "Consider restoring affected partitions from backup if corruption is widespread",
            "Check hardware health - disk errors may indicate failing storage"
        ])

    if critical_errors:
        recs["critical"].append(
            f"{len(critical_errors)} critical error type(s) detected - review error messages and take action"
        )

    if memory_errors:
        recs["high"].extend([
            f"{len(memory_errors)} memory-related error(s) detected",
            "Review max_memory_usage and max_memory_usage_for_user settings",
            "Consider increasing available RAM or optimizing query memory consumption",
            "Check for memory-intensive queries using system.query_log",
            "Review max_bytes_before_external_sort and max_bytes_before_external_group_by settings"
        ])

    if network_errors:
        recs["high"].extend([
            f"{len(network_errors)} network/connection error(s) detected",
            "Check network connectivity between cluster nodes",
            "Review firewall rules and network latency",
            "Increase connection timeout settings if network is slow",
            "Monitor system.replication_queue for replication issues"
        ])

    if replication_errors:
        recs["high"].extend([
            f"{len(replication_errors)} replication error(s) detected",
            "Check ZooKeeper/ClickHouse Keeper connectivity and health",
            "Review system.replicas for read-only or expired session replicas",
            "Check system.replication_queue for stuck operations",
            "Verify quorum settings and replica availability"
        ])

    # General recommendations
    recs["general"].extend([
        "Enable system.error_log for detailed error tracking over time",
        "Set up monitoring and alerting for critical error types",
        "Review ClickHouse server logs for additional context on errors",
        "Investigate errors with high occurrence counts",
        "Document and track recurring errors for pattern analysis",
        "Consider increasing log_queries_min_type to capture more query details",
        "Review max_concurrent_queries if seeing TOO_MANY_SIMULTANEOUS_QUERIES",
        "Use system.query_log to correlate errors with specific queries"
    ])

    return recs

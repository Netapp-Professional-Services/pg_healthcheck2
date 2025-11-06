"""
ClickHouse Error Tracking Query Library

Centralized SQL query definitions for error monitoring and diagnostics.

Queries:
- get_errors_summary_query(): Current error counts from system.errors
- get_top_errors_query(): Most frequent errors
- get_recent_error_log_query(): Recent error events from system.error_log
- get_errors_by_code_query(): Error breakdown by error code
- get_error_trend_query(): Error occurrence trends over time
"""


def get_errors_summary_query(connector):
    """
    Returns query for error summary from system.errors.

    Shows all errors that have occurred with their counts and last occurrence.

    Returns:
        str: SQL query for error summary
    """
    return """
    SELECT
        name,
        code,
        value as error_count,
        last_error_time,
        last_error_message,
        last_error_trace,
        remote
    FROM system.errors
    WHERE value > 0
    ORDER BY value DESC, last_error_time DESC
    """


def get_top_errors_query(connector, limit=20):
    """
    Returns query for most frequent errors.

    Args:
        connector: ClickHouse connector instance
        limit: Maximum number of errors to return (default: 20)

    Returns:
        str: SQL query for top errors
    """
    return f"""
    SELECT
        name,
        code,
        value as error_count,
        last_error_time,
        last_error_message
    FROM system.errors
    WHERE value > 0
    ORDER BY value DESC
    LIMIT {limit}
    """


def get_recent_error_log_query(connector, hours=24, limit=100):
    """
    Returns query for recent error events from system.error_log.

    Note: ClickHouse 25.x removed the 'name' column from system.error_log.
    We only get code, value, and timestamps now.

    Args:
        connector: ClickHouse connector instance
        hours: Number of hours to look back (default: 24)
        limit: Maximum number of error events to return (default: 100)

    Returns:
        str: SQL query for recent error log
    """
    return f"""
    SELECT
        event_date,
        event_time,
        code,
        value,
        remote
    FROM system.error_log
    WHERE event_time >= now() - INTERVAL {hours} HOUR
    ORDER BY event_time DESC
    LIMIT {limit}
    """


def get_errors_by_code_query(connector):
    """
    Returns query for error breakdown by error code.

    Groups errors by their error code for pattern analysis.

    Returns:
        str: SQL query for errors grouped by code
    """
    return """
    SELECT
        code,
        name,
        value as total_occurrences,
        last_error_time,
        last_error_message
    FROM system.errors
    WHERE value > 0
    ORDER BY code, value DESC
    """


def get_error_trend_query(connector, hours=24):
    """
    Returns query for error occurrence trends over time.

    Requires system.error_log table to be enabled.

    Args:
        connector: ClickHouse connector instance
        hours: Number of hours to analyze (default: 24)

    Returns:
        str: SQL query for error trends
    """
    return f"""
    SELECT
        toStartOfHour(event_time) as hour,
        name,
        count() as error_count
    FROM system.error_log
    WHERE event_time >= now() - INTERVAL {hours} HOUR
    GROUP BY hour, name
    ORDER BY hour DESC, error_count DESC
    """


def get_critical_errors_query(connector):
    """
    Returns query for critical error types that require immediate attention.

    Filters for error codes that indicate serious issues:
    - Connection errors
    - Memory errors
    - Corruption errors
    - Replication errors

    Returns:
        str: SQL query for critical errors
    """
    critical_error_patterns = [
        'CANNOT_ALLOCATE_MEMORY',
        'MEMORY_LIMIT_EXCEEDED',
        'CORRUPTED_DATA',
        'CHECKSUM_DOESNT_MATCH',
        'REPLICA_IS_NOT_IN_QUORUM',
        'REPLICA_IS_ALREADY_ACTIVE',
        'REPLICA_IS_ALREADY_EXIST',
        'ALL_CONNECTION_TRIES_FAILED',
        'NETWORK_ERROR',
        'KEEPER_EXCEPTION',
        'ZOOKEEPER_EXCEPTION',
        'NO_ZOOKEEPER',
        'READONLY',
        'TOO_MANY_SIMULTANEOUS_QUERIES'
    ]

    # Create LIKE conditions for pattern matching
    like_conditions = " OR ".join([f"name LIKE '%{pattern}%'" for pattern in critical_error_patterns])

    return f"""
    SELECT
        name,
        code,
        value as error_count,
        last_error_time,
        last_error_message,
        last_error_trace
    FROM system.errors
    WHERE value > 0
      AND ({like_conditions})
    ORDER BY last_error_time DESC, value DESC
    """


def get_error_rate_query(connector, interval_minutes=5):
    """
    Returns query for error rate calculation over time intervals.

    Requires system.error_log to be enabled.

    Args:
        connector: ClickHouse connector instance
        interval_minutes: Time interval for rate calculation (default: 5)

    Returns:
        str: SQL query for error rates
    """
    return f"""
    SELECT
        toStartOfInterval(event_time, INTERVAL {interval_minutes} MINUTE) as interval_start,
        name,
        count() as error_count,
        count() / {interval_minutes} as errors_per_minute
    FROM system.error_log
    WHERE event_time >= now() - INTERVAL 1 HOUR
    GROUP BY interval_start, name
    ORDER BY interval_start DESC, error_count DESC
    """


def get_unique_error_messages_query(connector, hours=24):
    """
    Returns query for unique error messages in recent history.

    Helps identify new or unusual error patterns.

    Args:
        connector: ClickHouse connector instance
        hours: Number of hours to look back (default: 24)

    Returns:
        str: SQL query for unique error messages
    """
    return f"""
    SELECT
        name,
        last_error_message,
        value as occurrence_count,
        last_error_time
    FROM system.errors
    WHERE value > 0
      AND last_error_time >= now() - INTERVAL {hours} HOUR
    GROUP BY name, last_error_message, value, last_error_time
    ORDER BY last_error_time DESC
    """

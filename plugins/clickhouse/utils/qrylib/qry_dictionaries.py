"""
ClickHouse Dictionary Monitoring Query Library

Centralized SQL query definitions for dictionary health and performance monitoring.

Dictionaries are critical for:
- Enrichment queries (JOINs with external data sources)
- Lookup operations
- Query performance optimization

Dictionary failures can cause widespread query failures, making this monitoring essential.

Queries:
- get_dictionary_status_query(): Overall dictionary health and status
- get_failed_dictionaries_query(): Dictionaries with load failures
- get_dictionary_reload_times_query(): Dictionary reload performance
- get_dictionary_memory_usage_query(): Dictionary memory consumption
- get_dictionary_load_history_query(): Recent dictionary load operations
- get_dictionary_types_query(): Dictionary type distribution
"""


def get_dictionary_status_query(connector):
    """
    Returns query for dictionary status overview.

    Shows all dictionaries with their current state, load status, and basic metrics.
    This is the primary query for dictionary health monitoring.

    Returns:
        str: SQL query for dictionary status
    """
    return """
    SELECT
        database,
        name,
        status,
        origin,
        type,
        key,
        formatReadableSize(bytes_allocated) as memory_used,
        element_count,
        load_factor,
        loading_start_time,
        last_successful_update_time,
        loading_duration,
        last_exception
    FROM system.dictionaries
    ORDER BY database, name
    """


def get_failed_dictionaries_query(connector):
    """
    Returns query for failed or problematic dictionaries.

    Identifies dictionaries that:
    - Failed to load (status = FAILED or FAILED_AND_RELOADING)
    - Have recent exceptions
    - Are stuck loading

    Returns:
        str: SQL query for failed dictionaries
    """
    return """
    SELECT
        database,
        name,
        status,
        origin,
        type,
        last_exception,
        loading_start_time,
        last_successful_update_time,
        loading_duration,
        element_count,
        formatReadableSize(bytes_allocated) as memory_used
    FROM system.dictionaries
    WHERE status IN ('FAILED', 'FAILED_AND_RELOADING', 'LOADING')
       OR last_exception != ''
    ORDER BY loading_start_time DESC
    """


def get_dictionary_reload_times_query(connector, hours=24):
    """
    Returns query for dictionary reload performance analysis.

    Identifies dictionaries with slow reload times that may impact availability.

    Args:
        connector: ClickHouse connector instance
        hours: Time window for analysis (default: 24 hours)

    Returns:
        str: SQL query for dictionary reload times
    """
    return f"""
    SELECT
        database,
        name,
        status,
        loading_duration,
        loading_duration / 1000000.0 as loading_seconds,
        last_successful_update_time,
        element_count,
        formatReadableSize(bytes_allocated) as memory_used,
        type
    FROM system.dictionaries
    WHERE last_successful_update_time >= now() - INTERVAL {hours} HOUR
      AND loading_duration > 0
    ORDER BY loading_duration DESC
    LIMIT 50
    """


def get_dictionary_memory_usage_query(connector):
    """
    Returns query for dictionary memory consumption analysis.

    Identifies dictionaries consuming significant memory resources.
    Important for capacity planning and detecting memory bloat.

    Returns:
        str: SQL query for dictionary memory usage
    """
    return """
    SELECT
        database,
        name,
        status,
        type,
        element_count,
        bytes_allocated,
        formatReadableSize(bytes_allocated) as memory_readable,
        CASE
            WHEN element_count > 0 THEN bytes_allocated / element_count
            ELSE 0
        END as bytes_per_element,
        load_factor,
        last_successful_update_time
    FROM system.dictionaries
    WHERE bytes_allocated > 0
    ORDER BY bytes_allocated DESC
    LIMIT 50
    """


def get_dictionary_summary_query(connector):
    """
    Returns query for dictionary statistics summary.

    Provides aggregate metrics across all dictionaries:
    - Total count
    - Status distribution
    - Total memory usage
    - Failed dictionary count

    Returns:
        str: SQL query for dictionary summary
    """
    return """
    SELECT
        count() as total_dictionaries,
        countIf(status = 'LOADED') as loaded_count,
        countIf(status = 'LOADING') as loading_count,
        countIf(status IN ('FAILED', 'FAILED_AND_RELOADING')) as failed_count,
        countIf(last_exception != '') as exception_count,
        sum(bytes_allocated) as total_memory_bytes,
        formatReadableSize(sum(bytes_allocated)) as total_memory_readable,
        sum(element_count) as total_elements,
        avg(loading_duration) as avg_loading_duration_ms,
        max(loading_duration) as max_loading_duration_ms
    FROM system.dictionaries
    """


def get_dictionary_types_query(connector):
    """
    Returns query for dictionary type distribution.

    Shows which dictionary types are in use and their characteristics.
    Useful for understanding dictionary architecture.

    Returns:
        str: SQL query for dictionary types
    """
    return """
    SELECT
        type,
        count() as count,
        countIf(status = 'LOADED') as loaded_count,
        countIf(status IN ('FAILED', 'FAILED_AND_RELOADING')) as failed_count,
        sum(bytes_allocated) as total_memory,
        formatReadableSize(sum(bytes_allocated)) as total_memory_readable,
        sum(element_count) as total_elements,
        avg(loading_duration) / 1000.0 as avg_loading_seconds
    FROM system.dictionaries
    GROUP BY type
    ORDER BY count DESC
    """


def get_stale_dictionaries_query(connector, hours=24):
    """
    Returns query for stale dictionaries.

    Identifies dictionaries that haven't been updated recently.
    May indicate reload schedule issues or source data problems.

    Args:
        connector: ClickHouse connector instance
        hours: Staleness threshold in hours (default: 24 hours)

    Returns:
        str: SQL query for stale dictionaries
    """
    return f"""
    SELECT
        database,
        name,
        status,
        type,
        origin,
        last_successful_update_time,
        dateDiff('hour', last_successful_update_time, now()) as hours_since_update,
        element_count,
        formatReadableSize(bytes_allocated) as memory_used
    FROM system.dictionaries
    WHERE status = 'LOADED'
      AND last_successful_update_time < now() - INTERVAL {hours} HOUR
      AND last_successful_update_time != toDateTime('1970-01-01 00:00:00')
    ORDER BY hours_since_update DESC
    LIMIT 50
    """


def get_dictionary_sources_query(connector):
    """
    Returns query for dictionary source analysis.

    Shows which data sources are used by dictionaries.
    Helps identify external dependencies.

    Returns:
        str: SQL query for dictionary sources
    """
    return """
    SELECT
        origin,
        count() as dictionary_count,
        countIf(status = 'LOADED') as loaded_count,
        countIf(status IN ('FAILED', 'FAILED_AND_RELOADING')) as failed_count,
        countIf(last_exception != '') as with_exceptions
    FROM system.dictionaries
    GROUP BY origin
    ORDER BY dictionary_count DESC
    """


def get_large_dictionaries_query(connector, min_size_mb=100):
    """
    Returns query for large dictionaries.

    Identifies dictionaries consuming significant resources.
    Useful for capacity planning and optimization.

    Args:
        connector: ClickHouse connector instance
        min_size_mb: Minimum size threshold in MB (default: 100 MB)

    Returns:
        str: SQL query for large dictionaries
    """
    min_size_bytes = min_size_mb * 1024 * 1024

    return f"""
    SELECT
        database,
        name,
        status,
        type,
        bytes_allocated,
        formatReadableSize(bytes_allocated) as memory_readable,
        element_count,
        CASE
            WHEN element_count > 0 THEN formatReadableSize(bytes_allocated / element_count)
            ELSE 'N/A'
        END as bytes_per_element,
        loading_duration / 1000000.0 as loading_seconds,
        last_successful_update_time
    FROM system.dictionaries
    WHERE bytes_allocated >= {min_size_bytes}
    ORDER BY bytes_allocated DESC
    LIMIT 50
    """


def get_dictionary_health_score_query(connector):
    """
    Returns query for dictionary health scoring.

    Calculates a health score based on:
    - Load status (LOADED = healthy, FAILED = unhealthy)
    - Exception presence
    - Staleness (last update time)
    - Loading duration

    More actionable than simple status - provides prioritization.

    Returns:
        str: SQL query for dictionary health scoring
    """
    return """
    SELECT
        database,
        name,
        status,
        CASE
            WHEN status IN ('FAILED', 'FAILED_AND_RELOADING') THEN 'critical'
            WHEN last_exception != '' THEN 'warning'
            WHEN status = 'LOADING' AND dateDiff('minute', loading_start_time, now()) > 30 THEN 'warning'
            WHEN status = 'LOADED' AND dateDiff('hour', last_successful_update_time, now()) > 48 THEN 'warning'
            WHEN status = 'LOADED' THEN 'healthy'
            ELSE 'unknown'
        END as health_status,
        last_exception,
        loading_start_time,
        last_successful_update_time,
        dateDiff('hour', last_successful_update_time, now()) as hours_since_update,
        element_count,
        formatReadableSize(bytes_allocated) as memory_used
    FROM system.dictionaries
    ORDER BY
        CASE
            WHEN status IN ('FAILED', 'FAILED_AND_RELOADING') THEN 1
            WHEN last_exception != '' THEN 2
            WHEN status = 'LOADING' THEN 3
            ELSE 4
        END,
        database, name
    """

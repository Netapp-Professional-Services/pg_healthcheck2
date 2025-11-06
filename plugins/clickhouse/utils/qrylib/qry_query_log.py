"""
ClickHouse Query Log Queries

Queries for query performance analysis and monitoring.
"""


def check_query_metric_log_available(connector):
    """
    Check if system.query_metric_log is available.

    This table may not be enabled in managed services (e.g., Instaclustr)
    or if log_query_threads=0 in server configuration.

    Returns:
        bool: True if table exists and is accessible, False otherwise
    """
    try:
        check_query = """
        SELECT 1
        FROM system.tables
        WHERE database = 'system'
          AND name = 'query_metric_log'
        LIMIT 1
        """
        result = connector.execute_query(check_query)
        return result and len(result) > 0
    except Exception:
        return False


def get_query_performance_summary_query(connector, hours=24):
    """
    Returns query for query performance summary.

    Aggregates query statistics over the specified time period.
    """
    return f"""
    SELECT
        count() as total_queries,
        countIf(type = 'QueryFinish') as successful_queries,
        countIf(type = 'ExceptionWhileProcessing') as failed_queries,
        countIf(type = 'ExceptionBeforeStart') as failed_before_start,
        countIf(query_duration_ms > 10000) as slow_queries_count,
        avg(query_duration_ms) as avg_duration_ms,
        median(query_duration_ms) as median_duration_ms,
        quantile(0.95)(query_duration_ms) as p95_duration_ms,
        quantile(0.99)(query_duration_ms) as p99_duration_ms,
        max(query_duration_ms) as max_duration_ms,
        sum(read_rows) as total_rows_read,
        sum(read_bytes) as total_bytes_read,
        sum(written_rows) as total_rows_written,
        sum(written_bytes) as total_bytes_written,
        sum(memory_usage) as total_memory_used
    FROM system.query_log
    WHERE event_date >= today() - {hours // 24}
        AND event_time >= now() - INTERVAL {hours} HOUR
        AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
    """


def get_slow_queries_query(connector, threshold_seconds=10, limit=50):
    """
    Returns query for slow query identification.

    Lists queries exceeding the specified duration threshold.
    """
    threshold_ms = threshold_seconds * 1000

    return f"""
    SELECT
        type,
        event_time,
        query_duration_ms,
        query_duration_ms / 1000 as duration_seconds,
        user,
        query_id,
        read_rows,
        formatReadableSize(read_bytes) as read_size,
        written_rows,
        formatReadableSize(written_bytes) as written_size,
        result_rows,
        formatReadableSize(result_bytes) as result_size,
        memory_usage,
        substring(query, 1, 200) as query_preview,
        exception
    FROM system.query_log
    WHERE event_date >= today() - 1
        AND event_time >= now() - INTERVAL 24 HOUR
        AND query_duration_ms > {threshold_ms}
        AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
        AND query NOT LIKE '%system.query_log%'
    ORDER BY query_duration_ms DESC
    LIMIT {limit}
    """


def get_failed_queries_query(connector, hours=24, limit=100):
    """
    Returns query for failed query analysis.

    Lists queries that failed with exceptions.

    Returns columns in fixed positions for check code compatibility:
    - row[0] = query_preview (string)
    - row[1] = user (string)
    - row[2] = exception (string)
    - row[3] = query_duration_ms (numeric)
    """
    return f"""
    SELECT
        substring(query, 1, 300) as query_preview,
        user,
        exception,
        query_duration_ms
    FROM system.query_log
    WHERE event_date >= today() - {hours // 24}
        AND event_time >= now() - INTERVAL {hours} HOUR
        AND type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')
        AND query NOT LIKE '%system.query_log%'
    ORDER BY event_time DESC
    LIMIT {limit}
    """


def get_query_by_user_query(connector, hours=24):
    """
    Returns query for per-user query statistics.

    Aggregates query metrics grouped by user.
    """
    return f"""
    SELECT
        user,
        count() as total_queries,
        countIf(type = 'QueryFinish') as successful_queries,
        countIf(type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')) as failed_queries,
        avg(query_duration_ms) as avg_duration_ms,
        quantile(0.95)(query_duration_ms) as p95_duration_ms,
        sum(read_rows) as total_rows_read,
        formatReadableSize(sum(read_bytes)) as total_data_read,
        sum(memory_usage) as total_memory_used
    FROM system.query_log
    WHERE event_date >= today() - {hours // 24}
        AND event_time >= now() - INTERVAL {hours} HOUR
        AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
    GROUP BY user
    ORDER BY total_queries DESC
    """


def get_query_types_query(connector, hours=24):
    """
    Returns query for query type distribution.

    Categorizes queries by type (SELECT, INSERT, etc.).
    """
    return f"""
    SELECT
        query_kind,
        count() as query_count,
        avg(query_duration_ms) as avg_duration_ms,
        sum(read_rows) as total_rows_read,
        sum(written_rows) as total_rows_written
    FROM system.query_log
    WHERE event_date >= today() - {hours // 24}
        AND event_time >= now() - INTERVAL {hours} HOUR
        AND type = 'QueryFinish'
    GROUP BY query_kind
    ORDER BY query_count DESC
    """


def get_memory_intensive_queries_query(connector, limit=50):
    """
    Returns query for memory-intensive query identification.

    Lists queries with highest memory usage.
    """
    return f"""
    SELECT
        event_time,
        user,
        query_id,
        query_duration_ms,
        memory_usage,
        formatReadableSize(memory_usage) as memory_readable,
        peak_memory_usage,
        formatReadableSize(peak_memory_usage) as peak_memory_readable,
        read_rows,
        formatReadableSize(read_bytes) as read_size,
        substring(query, 1, 200) as query_preview
    FROM system.query_log
    WHERE event_date >= today() - 1
        AND event_time >= now() - INTERVAL 24 HOUR
        AND type = 'QueryFinish'
        AND query NOT LIKE '%system.query_log%'
    ORDER BY peak_memory_usage DESC
    LIMIT {limit}
    """


# ============================================================================
# QUERY METRIC LOG QUERIES (Detailed ProfileEvents Analysis)
# ============================================================================
# These queries provide granular execution metrics for query optimization
# More actionable than basic query_log - identifies specific bottlenecks


def get_query_profile_metrics_query(connector, query_id):
    """
    Returns detailed ProfileEvents for a specific query.

    Provides granular execution metrics including:
    - I/O operations (disk reads, network traffic)
    - CPU utilization (user time, system time)
    - Memory operations (allocations, reallocations)
    - Locking and contention
    - Query execution stages

    More actionable than instacollector - identifies specific bottlenecks.

    Args:
        connector: ClickHouse connector instance
        query_id: Query ID to analyze

    Returns:
        str: SQL query for detailed profile metrics
    """
    return f"""
    SELECT
        metric_name,
        metric_value,
        description
    FROM system.query_metric_log
    ARRAY JOIN
        ProfileEvents.Names AS metric_name,
        ProfileEvents.Values AS metric_value
    WHERE query_id = '{query_id}'
        AND metric_value > 0
    ORDER BY metric_value DESC
    LIMIT 100
    """


def get_io_intensive_queries_query(connector, hours=24, limit=20):
    """
    Returns queries with highest I/O operations.

    Identifies queries causing excessive disk reads or network traffic.
    More specific than "slow query" - pinpoints I/O bottlenecks.

    Key metrics:
    - FileOpen: Number of file opens
    - ReadBufferFromFileDescriptorRead: Actual disk reads
    - NetworkSendBytes: Network egress
    - NetworkReceiveBytes: Network ingress

    Args:
        connector: ClickHouse connector instance
        hours: Time window for analysis
        limit: Maximum queries to return

    Returns:
        str: SQL query for I/O intensive queries
    """
    return f"""
    WITH query_io AS (
        SELECT
            query_id,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'FileOpen')],
                  has(ProfileEvents.Names, 'FileOpen')) as file_opens,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'ReadBufferFromFileDescriptorRead')],
                  has(ProfileEvents.Names, 'ReadBufferFromFileDescriptorRead')) as disk_reads,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'NetworkSendBytes')],
                  has(ProfileEvents.Names, 'NetworkSendBytes')) as network_send_bytes,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'NetworkReceiveBytes')],
                  has(ProfileEvents.Names, 'NetworkReceiveBytes')) as network_recv_bytes
        FROM system.query_metric_log
        WHERE event_time >= now() - INTERVAL {hours} HOUR
        GROUP BY query_id
        HAVING file_opens > 0 OR disk_reads > 0
    )
    SELECT
        ql.event_time,
        ql.user,
        ql.query_id,
        ql.query_duration_ms,
        io.file_opens,
        io.disk_reads,
        formatReadableSize(io.network_send_bytes) as network_sent,
        formatReadableSize(io.network_recv_bytes) as network_received,
        ql.read_rows,
        formatReadableSize(ql.read_bytes) as data_read,
        substring(ql.query, 1, 150) as query_preview
    FROM system.query_log ql
    INNER JOIN query_io io ON ql.query_id = io.query_id
    WHERE ql.event_date >= today() - {hours // 24}
        AND ql.event_time >= now() - INTERVAL {hours} HOUR
        AND ql.type = 'QueryFinish'
        AND ql.query NOT LIKE '%system.query_log%'
    ORDER BY io.disk_reads DESC
    LIMIT {limit}
    """


def get_cpu_intensive_queries_query(connector, hours=24, limit=20):
    """
    Returns queries with highest CPU consumption.

    Identifies computational bottlenecks and CPU-bound queries.

    Key metrics:
    - OSCPUWaitMicroseconds: Time waiting for CPU
    - UserTimeMicroseconds: User-space CPU time
    - SystemTimeMicroseconds: Kernel-space CPU time

    Args:
        connector: ClickHouse connector instance
        hours: Time window for analysis
        limit: Maximum queries to return

    Returns:
        str: SQL query for CPU intensive queries
    """
    return f"""
    WITH query_cpu AS (
        SELECT
            query_id,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'OSCPUWaitMicroseconds')],
                  has(ProfileEvents.Names, 'OSCPUWaitMicroseconds')) as cpu_wait_us,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'UserTimeMicroseconds')],
                  has(ProfileEvents.Names, 'UserTimeMicroseconds')) as user_time_us,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'SystemTimeMicroseconds')],
                  has(ProfileEvents.Names, 'SystemTimeMicroseconds')) as system_time_us
        FROM system.query_metric_log
        WHERE event_time >= now() - INTERVAL {hours} HOUR
        GROUP BY query_id
        HAVING cpu_wait_us > 0 OR user_time_us > 0
    )
    SELECT
        ql.event_time,
        ql.user,
        ql.query_id,
        ql.query_duration_ms,
        cpu.cpu_wait_us / 1000000.0 as cpu_wait_seconds,
        cpu.user_time_us / 1000000.0 as user_time_seconds,
        cpu.system_time_us / 1000000.0 as system_time_seconds,
        (cpu.user_time_us + cpu.system_time_us) / 1000000.0 as total_cpu_seconds,
        ql.read_rows,
        substring(ql.query, 1, 150) as query_preview
    FROM system.query_log ql
    INNER JOIN query_cpu cpu ON ql.query_id = cpu.query_id
    WHERE ql.event_date >= today() - {hours // 24}
        AND ql.event_time >= now() - INTERVAL {hours} HOUR
        AND ql.type = 'QueryFinish'
        AND ql.query NOT LIKE '%system.query_log%'
    ORDER BY total_cpu_seconds DESC
    LIMIT {limit}
    """


def get_lock_contention_queries_query(connector, hours=24, limit=20):
    """
    Returns queries experiencing lock contention.

    Identifies queries waiting on locks - indicates concurrency issues.

    Key metrics:
    - ContextLocks: Context lock acquisitions
    - RWLockAcquiredReadLocks: Read lock acquisitions
    - RWLockAcquiredWriteLocks: Write lock acquisitions

    Args:
        connector: ClickHouse connector instance
        hours: Time window for analysis
        limit: Maximum queries to return

    Returns:
        str: SQL query for lock contention analysis
    """
    return f"""
    WITH query_locks AS (
        SELECT
            query_id,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'ContextLocks')],
                  has(ProfileEvents.Names, 'ContextLocks')) as context_locks,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'RWLockAcquiredReadLocks')],
                  has(ProfileEvents.Names, 'RWLockAcquiredReadLocks')) as read_locks,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'RWLockAcquiredWriteLocks')],
                  has(ProfileEvents.Names, 'RWLockAcquiredWriteLocks')) as write_locks
        FROM system.query_metric_log
        WHERE event_time >= now() - INTERVAL {hours} HOUR
        GROUP BY query_id
        HAVING context_locks > 1000 OR read_locks > 1000 OR write_locks > 100
    )
    SELECT
        ql.event_time,
        ql.user,
        ql.query_id,
        ql.query_duration_ms,
        locks.context_locks,
        locks.read_locks,
        locks.write_locks,
        ql.read_rows,
        substring(ql.query, 1, 150) as query_preview
    FROM system.query_log ql
    INNER JOIN query_locks locks ON ql.query_id = locks.query_id
    WHERE ql.event_date >= today() - {hours // 24}
        AND ql.event_time >= now() - INTERVAL {hours} HOUR
        AND ql.type = 'QueryFinish'
        AND ql.query NOT LIKE '%system.query_log%'
    ORDER BY locks.context_locks DESC
    LIMIT {limit}
    """


def get_merge_intensive_queries_query(connector, hours=24, limit=20):
    """
    Returns queries triggering excessive merges.

    Identifies INSERT/SELECT patterns causing merge overhead.

    Key metrics:
    - Merge: Number of merge operations initiated
    - MergedRows: Rows processed in merges
    - MergedUncompressedBytes: Data volume in merges

    Args:
        connector: ClickHouse connector instance
        hours: Time window for analysis
        limit: Maximum queries to return

    Returns:
        str: SQL query for merge-intensive queries
    """
    return f"""
    WITH query_merges AS (
        SELECT
            query_id,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'Merge')],
                  has(ProfileEvents.Names, 'Merge')) as merges,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'MergedRows')],
                  has(ProfileEvents.Names, 'MergedRows')) as merged_rows,
            sumIf(ProfileEvents.Values[indexOf(ProfileEvents.Names, 'MergedUncompressedBytes')],
                  has(ProfileEvents.Names, 'MergedUncompressedBytes')) as merged_bytes
        FROM system.query_metric_log
        WHERE event_time >= now() - INTERVAL {hours} HOUR
        GROUP BY query_id
        HAVING merges > 0
    )
    SELECT
        ql.event_time,
        ql.user,
        ql.query_id,
        ql.query_duration_ms,
        m.merges,
        m.merged_rows,
        formatReadableSize(m.merged_bytes) as merged_data,
        ql.written_rows,
        formatReadableSize(ql.written_bytes) as written_data,
        substring(ql.query, 1, 150) as query_preview
    FROM system.query_log ql
    INNER JOIN query_merges m ON ql.query_id = m.query_id
    WHERE ql.event_date >= today() - {hours // 24}
        AND ql.event_time >= now() - INTERVAL {hours} HOUR
        AND ql.type = 'QueryFinish'
        AND ql.query NOT LIKE '%system.query_log%'
    ORDER BY m.merges DESC
    LIMIT {limit}
    """


def get_inefficient_queries_by_ratio_query(connector, hours=24, limit=20):
    """
    Returns queries with poor efficiency ratios.

    Identifies queries that:
    - Read much more data than they return (inefficient filtering)
    - Use excessive memory per row processed
    - Have long duration relative to data processed

    More actionable than "slow query" - identifies WHY query is inefficient.

    Args:
        connector: ClickHouse connector instance
        hours: Time window for analysis
        limit: Maximum queries to return

    Returns:
        str: SQL query for efficiency analysis
    """
    return f"""
    SELECT
        event_time,
        user,
        query_id,
        query_duration_ms,
        read_rows,
        result_rows,
        CASE
            WHEN result_rows > 0 THEN toFloat64(read_rows) / toFloat64(result_rows)
            ELSE toFloat64(read_rows)
        END as row_scan_ratio,
        formatReadableSize(read_bytes) as data_read,
        formatReadableSize(result_bytes) as data_returned,
        CASE
            WHEN result_bytes > 0 THEN toFloat64(read_bytes) / toFloat64(result_bytes)
            ELSE toFloat64(read_bytes)
        END as data_scan_ratio,
        formatReadableSize(memory_usage) as memory_used,
        CASE
            WHEN read_rows > 0 THEN toFloat64(memory_usage) / toFloat64(read_rows)
            ELSE toFloat64(memory_usage)
        END as memory_per_row,
        substring(query, 1, 150) as query_preview
    FROM system.query_log
    WHERE event_date >= today() - {hours // 24}
        AND event_time >= now() - INTERVAL {hours} HOUR
        AND type = 'QueryFinish'
        AND query NOT LIKE '%system.query_log%'
        AND read_rows > 100000
        AND (
            (result_rows > 0 AND toFloat64(read_rows) / toFloat64(result_rows) > 1000) OR
            (result_bytes > 0 AND toFloat64(read_bytes) / toFloat64(result_bytes) > 1000) OR
            (read_rows > 0 AND toFloat64(memory_usage) / toFloat64(read_rows) > 10000)
        )
    ORDER BY row_scan_ratio DESC
    LIMIT {limit}
    """


def get_query_bottleneck_analysis_query(connector, query_id):
    """
    Returns comprehensive bottleneck analysis for a specific query.

    Provides actionable insights on what's slowing the query down:
    - I/O wait time
    - CPU wait time
    - Lock contention
    - Network latency
    - Memory pressure

    This is HIGHLY actionable - tells you exactly where to optimize.

    Args:
        connector: ClickHouse connector instance
        query_id: Query ID to analyze

    Returns:
        str: SQL query for bottleneck analysis
    """
    return f"""
    WITH profile_data AS (
        SELECT
            ProfileEvents.Names as metric_names,
            ProfileEvents.Values as metric_values
        FROM system.query_metric_log
        WHERE query_id = '{query_id}'
        LIMIT 1
    )
    SELECT
        'I/O Operations' as category,
        sumIf(metric_values[idx], metric_names[idx] = 'FileOpen') as file_opens,
        sumIf(metric_values[idx], metric_names[idx] = 'ReadBufferFromFileDescriptorRead') as disk_reads,
        sumIf(metric_values[idx], metric_names[idx] = 'OSReadBytes') as os_read_bytes,
        sumIf(metric_values[idx], metric_names[idx] = 'OSWriteBytes') as os_write_bytes,
        sumIf(metric_values[idx], metric_names[idx] = 'OSIOWaitMicroseconds') / 1000000.0 as io_wait_seconds
    FROM profile_data
    ARRAY JOIN arrayEnumerate(metric_names) as idx

    UNION ALL

    SELECT
        'CPU Usage' as category,
        sumIf(metric_values[idx], metric_names[idx] = 'UserTimeMicroseconds') / 1000000.0 as user_time_seconds,
        sumIf(metric_values[idx], metric_names[idx] = 'SystemTimeMicroseconds') / 1000000.0 as system_time_seconds,
        sumIf(metric_values[idx], metric_names[idx] = 'OSCPUWaitMicroseconds') / 1000000.0 as cpu_wait_seconds,
        NULL as unused1,
        NULL as unused2
    FROM profile_data
    ARRAY JOIN arrayEnumerate(metric_names) as idx

    UNION ALL

    SELECT
        'Memory Operations' as category,
        sumIf(metric_values[idx], metric_names[idx] = 'MemoryTrackerUsage') as memory_tracked,
        sumIf(metric_values[idx], metric_names[idx] = 'MemoryAllocations') as allocations,
        sumIf(metric_values[idx], metric_names[idx] = 'MemoryReallocations') as reallocations,
        NULL as unused1,
        NULL as unused2
    FROM profile_data
    ARRAY JOIN arrayEnumerate(metric_names) as idx

    UNION ALL

    SELECT
        'Lock Contention' as category,
        sumIf(metric_values[idx], metric_names[idx] = 'ContextLocks') as context_locks,
        sumIf(metric_values[idx], metric_names[idx] = 'RWLockAcquiredReadLocks') as read_locks,
        sumIf(metric_values[idx], metric_names[idx] = 'RWLockAcquiredWriteLocks') as write_locks,
        NULL as unused1,
        NULL as unused2
    FROM profile_data
    ARRAY JOIN arrayEnumerate(metric_names) as idx

    UNION ALL

    SELECT
        'Network Activity' as category,
        sumIf(metric_values[idx], metric_names[idx] = 'NetworkSendBytes') as bytes_sent,
        sumIf(metric_values[idx], metric_names[idx] = 'NetworkReceiveBytes') as bytes_received,
        NULL as unused1,
        NULL as unused2,
        NULL as unused3
    FROM profile_data
    ARRAY JOIN arrayEnumerate(metric_names) as idx
    """

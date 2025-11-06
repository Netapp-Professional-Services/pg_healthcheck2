"""
ClickHouse Node Metrics Queries

Queries for node-level resource monitoring and system metrics.
"""


def get_system_metrics_query(connector):
    """
    Returns query for real-time system metrics.

    Retrieves current metric values from system.metrics.
    """
    return """
    SELECT
        metric,
        value,
        description
    FROM system.metrics
    ORDER BY metric
    """


def get_async_metrics_query(connector):
    """
    Returns query for asynchronous system metrics.

    Includes memory, CPU, and system resource metrics.
    """
    return """
    SELECT
        metric,
        value
    FROM system.asynchronous_metrics
    WHERE metric IN (
        'MemoryResident',
        'MemoryVirtual',
        'MemoryCode',
        'MemoryDataAndStack',
        'MemoryShared',
        'MemoryResidentMax',
        'LoadAverage1',
        'LoadAverage5',
        'LoadAverage15',
        'OSSystemTimeNormalized',
        'OSUserTimeNormalized',
        'OSMemoryTotal',
        'OSMemoryAvailable',
        'OSMemoryFreePlusCached',
        'NumberOfLogicalCPUCores',
        'NumberOfPhysicalCPUCores',
        'Uptime'
    )
    ORDER BY metric
    """


def get_active_queries_query(connector):
    """
    Returns query for currently running queries.

    Monitors active query execution and resource usage.

    Note: query_duration_ms removed in ClickHouse 25.x, use elapsed instead.

    Version Support:
    - ClickHouse >= 25: Uses elapsed (time in seconds)
    - ClickHouse < 25: Uses query_duration_ms (time in milliseconds) converted to seconds

    Query returns columns in fixed positions for check code compatibility:
    - row[0] = query_id
    - row[1] = user
    - row[2] = elapsed (seconds)
    - row[3] = read_rows
    - row[4] = read_bytes
    - row[5] = memory_usage
    - row[6] = query
    """
    major_version = connector.version_info.get('major_version', 0)

    # ClickHouse 25.x removed query_duration_ms column
    if major_version >= 25:
        return """
        SELECT
            query_id,
            user,
            elapsed,
            read_rows,
            read_bytes,
            memory_usage,
            query
        FROM system.processes
        ORDER BY elapsed DESC
        """
    else:
        # ClickHouse < 25.x - use query_duration_ms converted to seconds
        return """
        SELECT
            query_id,
            user,
            query_duration_ms / 1000 as elapsed_seconds,
            read_rows,
            read_bytes,
            memory_usage,
            query
        FROM system.processes
        ORDER BY elapsed DESC
        """


def get_memory_tracking_query(connector):
    """
    Returns query for detailed memory tracking.

    Provides breakdown of memory usage by category.
    """
    return """
    SELECT
        metric,
        value
    FROM system.asynchronous_metrics
    WHERE metric LIKE '%Memory%'
    ORDER BY metric
    """


def get_node_summary_query(connector):
    """
    Returns query for node health summary.

    Aggregates key node metrics for quick health assessment.
    """
    return """
    SELECT
        (SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryAvailable') as memory_available,
        (SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal') as memory_total,
        (SELECT value FROM system.asynchronous_metrics WHERE metric = 'LoadAverage1') as load_average_1m,
        (SELECT value FROM system.asynchronous_metrics WHERE metric = 'LoadAverage5') as load_average_5m,
        (SELECT value FROM system.asynchronous_metrics WHERE metric = 'LoadAverage15') as load_average_15m,
        (SELECT value FROM system.asynchronous_metrics WHERE metric = 'Uptime') as uptime_seconds,
        (SELECT count() FROM system.processes) as active_queries,
        (SELECT sum(memory_usage) FROM system.processes) as total_query_memory
    """

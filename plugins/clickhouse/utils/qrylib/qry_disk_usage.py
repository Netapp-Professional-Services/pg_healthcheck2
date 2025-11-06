"""
ClickHouse Disk Usage Queries

Queries for disk space monitoring and storage analysis.
"""


def get_disk_usage_query(connector):
    """
    Returns query for disk usage statistics.

    Retrieves disk space usage across all configured disks.

    Returns columns in fixed positions for check code compatibility:
    - row[0] = disk_name (string)
    - row[1] = path (string)
    - row[2] = free_space (bytes, numeric)
    - row[3] = total_space (bytes, numeric)
    - row[4] = keep_free_space (bytes, numeric)
    """
    return """
    SELECT
        name as disk_name,
        path,
        free_space,
        total_space,
        keep_free_space
    FROM system.disks
    ORDER BY name
    """


def get_database_disk_usage_query(connector):
    """
    Returns query for per-database disk usage.

    Aggregates storage usage grouped by database.
    """
    return """
    SELECT
        database,
        count() as table_count,
        sum(bytes_on_disk) as total_bytes,
        formatReadableSize(sum(bytes_on_disk)) as size_readable,
        sum(bytes_on_disk) / (1024 * 1024 * 1024) as size_gb,
        sum(rows) as total_rows,
        sum(data_compressed_bytes) as compressed_bytes,
        sum(data_uncompressed_bytes) as uncompressed_bytes,
        round((sum(data_compressed_bytes) / sum(data_uncompressed_bytes)) * 100, 2) as compression_ratio
    FROM system.parts
    WHERE active = 1
        AND database NOT IN ('system', '_temporary_and_external_tables')
    GROUP BY database
    ORDER BY total_bytes DESC
    """


def get_table_disk_usage_query(connector, database=None):
    """
    Returns query for per-table disk usage.

    Detailed storage usage for tables, optionally filtered by database.
    """
    base_query = """
    SELECT
        database,
        table,
        count() as part_count,
        sum(bytes_on_disk) as total_bytes,
        formatReadableSize(sum(bytes_on_disk)) as size_readable,
        sum(bytes_on_disk) / (1024 * 1024 * 1024) as size_gb,
        sum(rows) as total_rows,
        formatReadableSize(sum(rows)) as rows_readable,
        sum(data_compressed_bytes) as compressed_bytes,
        sum(data_uncompressed_bytes) as uncompressed_bytes,
        round((sum(data_compressed_bytes) / nullIf(sum(data_uncompressed_bytes), 0)) * 100, 2) as compression_ratio,
        min(modification_time) as oldest_part,
        max(modification_time) as newest_part
    FROM system.parts
    WHERE active = 1
        AND database NOT IN ('system', '_temporary_and_external_tables')
    """

    if database:
        base_query += f"\n    AND database = '{database}'"

    base_query += """
    GROUP BY database, table
    ORDER BY total_bytes DESC
    """

    return base_query


def get_partition_disk_usage_query(connector, database, table):
    """
    Returns query for partition-level disk usage.

    Detailed storage breakdown by partition for a specific table.
    """
    return f"""
    SELECT
        partition,
        count() as part_count,
        sum(bytes_on_disk) as total_bytes,
        formatReadableSize(sum(bytes_on_disk)) as size_readable,
        sum(rows) as total_rows,
        min(min_date) as partition_start,
        max(max_date) as partition_end,
        min(modification_time) as oldest_part,
        max(modification_time) as newest_part
    FROM system.parts
    WHERE database = '{database}'
        AND table = '{table}'
        AND active = 1
    GROUP BY partition
    ORDER BY partition_start DESC
    """


def get_storage_summary_query(connector):
    """
    Returns query for overall storage summary.

    Provides cluster-wide storage statistics.
    """
    return """
    SELECT
        sum(total_space) / (1024 * 1024 * 1024) as total_capacity_gb,
        sum(free_space) / (1024 * 1024 * 1024) as total_free_gb,
        sum(total_space - free_space) / (1024 * 1024 * 1024) as total_used_gb,
        round((sum(total_space - free_space) / sum(total_space)) * 100, 2) as overall_used_percent,
        count() as disk_count
    FROM system.disks
    """


def get_largest_tables_query(connector, limit=20):
    """
    Returns query for largest tables by storage size.

    Identifies tables consuming the most disk space.
    """
    return f"""
    SELECT
        database,
        table,
        sum(bytes_on_disk) as total_bytes,
        formatReadableSize(sum(bytes_on_disk)) as size_readable,
        sum(rows) as total_rows,
        count() as part_count,
        (SELECT engine FROM system.tables WHERE database = parts.database AND name = parts.table LIMIT 1) as engine
    FROM system.parts
    WHERE active = 1
        AND database NOT IN ('system', '_temporary_and_external_tables')
    GROUP BY database, table
    ORDER BY total_bytes DESC
    LIMIT {limit}
    """


def get_old_partitions_query(connector, days_old=90):
    """
    Returns query for identifying old partitions.

    Lists partitions older than specified threshold for cleanup consideration.
    """
    return f"""
    SELECT
        database,
        table,
        partition,
        min(min_date) as partition_start,
        max(max_date) as partition_end,
        sum(bytes_on_disk) as total_bytes,
        formatReadableSize(sum(bytes_on_disk)) as size_readable,
        sum(rows) as total_rows,
        count() as part_count,
        dateDiff('day', max(max_date), today()) as days_old
    FROM system.parts
    WHERE active = 1
        AND database NOT IN ('system', '_temporary_and_external_tables')
        AND max_date < today() - INTERVAL {days_old} DAY
    GROUP BY database, table, partition
    ORDER BY days_old DESC
    LIMIT 100
    """

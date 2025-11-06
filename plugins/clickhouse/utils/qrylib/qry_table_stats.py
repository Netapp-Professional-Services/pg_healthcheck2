"""
ClickHouse Table Statistics Queries

Queries for table health, part management, and storage analysis.
"""


def get_table_summary_query(connector):
    """
    Returns query for table overview and statistics.

    Retrieves table metadata, storage size, and row counts.
    """
    return """
    SELECT
        database,
        name as table_name,
        engine,
        total_rows,
        total_bytes,
        formatReadableSize(total_bytes) as size_readable,
        lifetime_rows,
        lifetime_bytes
    FROM system.tables
    WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', '_temporary_and_external_tables')
        AND engine LIKE '%MergeTree%'
    ORDER BY total_bytes DESC
    """


def get_table_parts_query(connector):
    """
    Returns query for table parts and merge status.

    Monitors part counts, sizes, and merge backlog.
    """
    return """
    SELECT
        database,
        table,
        count() as active_parts,
        sum(rows) as total_rows,
        sum(bytes_on_disk) as total_bytes,
        formatReadableSize(sum(bytes_on_disk)) as size_readable,
        min(min_date) as oldest_partition,
        max(max_date) as newest_partition,
        countIf(active = 1) as active_count,
        countIf(active = 0) as inactive_count
    FROM system.parts
    WHERE database NOT IN ('system', '_temporary_and_external_tables')
    GROUP BY database, table
    HAVING active_parts > 0
    ORDER BY active_parts DESC
    """


def get_table_parts_detailed_query(connector, database=None, table=None):
    """
    Returns detailed query for specific table parts.

    Provides granular part-level information for troubleshooting.
    """
    base_query = """
    SELECT
        database,
        table,
        partition,
        name as part_name,
        active,
        rows,
        bytes_on_disk,
        formatReadableSize(bytes_on_disk) as size_readable,
        modification_time,
        min_date,
        max_date,
        level,
        primary_key_bytes_in_memory,
        marks
    FROM system.parts
    WHERE database NOT IN ('system', '_temporary_and_external_tables')
    """

    if database and table:
        base_query += f"\n    AND database = '{database}' AND table = '{table}'"

    base_query += "\nORDER BY database, table, modification_time DESC"

    return base_query


def get_active_merges_query(connector):
    """
    Returns query for active merge operations.

    Monitors ongoing merges and their progress.
    """
    return """
    SELECT
        database,
        table,
        elapsed,
        progress,
        num_parts,
        result_part_name,
        total_size_bytes_compressed,
        formatReadableSize(total_size_bytes_compressed) as size_readable,
        bytes_read_uncompressed,
        rows_read,
        bytes_written_uncompressed,
        rows_written,
        memory_usage
    FROM system.merges
    ORDER BY elapsed DESC
    """


def get_mutations_query(connector):
    """
    Returns query for table mutations.

    Tracks ALTER TABLE mutations and their progress.
    """
    return """
    SELECT
        database,
        table,
        mutation_id,
        command,
        create_time,
        parts_to_do_names,
        parts_to_do,
        is_done,
        latest_failed_part,
        latest_fail_time,
        latest_fail_reason
    FROM system.mutations
    WHERE is_done = 0 OR latest_fail_time > now() - INTERVAL 1 DAY
    ORDER BY create_time DESC
    LIMIT 100
    """


def get_detached_parts_query(connector):
    """
    Returns query for detached table parts.

    Identifies parts that have been detached (potential data loss indicator).
    """
    return """
    SELECT
        database,
        table,
        partition_id,
        name as part_name,
        disk,
        reason,
        min_block_number,
        max_block_number,
        level
    FROM system.detached_parts
    WHERE database NOT IN ('system', '_temporary_and_external_tables')
    ORDER BY database, table, name
    """

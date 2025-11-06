"""
ClickHouse Backups Monitoring Query Library

Centralized SQL query definitions for backup monitoring and validation.

Queries:
- get_recent_backups_query(): Recent backup operations from system.backups
- get_backup_summary_query(): Backup summary statistics
- get_failed_backups_query(): Failed backup operations
- get_backup_age_query(): Backup age analysis
"""


def get_recent_backups_query(connector, limit=20):
    """
    Returns query for recent backup operations.

    Args:
        connector: ClickHouse connector instance
        limit: Maximum number of backups to return (default: 20)

    Returns:
        str: SQL query for recent backups
    """
    return f"""
    SELECT
        id,
        name,
        status,
        error,
        start_time,
        end_time,
        num_files,
        total_size,
        num_entries,
        uncompressed_size,
        compressed_size,
        files_read,
        bytes_read
    FROM system.backups
    ORDER BY start_time DESC
    LIMIT {limit}
    """


def get_backup_summary_query(connector, days=7):
    """
    Returns query for backup summary statistics over time period.

    Args:
        connector: ClickHouse connector instance
        days: Number of days to analyze (default: 7)

    Returns:
        str: SQL query for backup summary
    """
    return f"""
    SELECT
        count() as total_backups,
        countIf(status = 'BACKUP_COMPLETE') as successful_backups,
        countIf(status = 'BACKUP_FAILED') as failed_backups,
        countIf(status = 'BACKUP_CANCELLED') as cancelled_backups,
        sum(total_size) as total_backup_size,
        sum(compressed_size) as total_compressed_size,
        avg(dateDiff('second', start_time, end_time)) as avg_duration_seconds,
        max(dateDiff('second', start_time, end_time)) as max_duration_seconds,
        min(start_time) as oldest_backup_time,
        max(start_time) as newest_backup_time
    FROM system.backups
    WHERE start_time >= now() - INTERVAL {days} DAY
    """


def get_failed_backups_query(connector, days=7):
    """
    Returns query for failed backup operations.

    Args:
        connector: ClickHouse connector instance
        days: Number of days to look back (default: 7)

    Returns:
        str: SQL query for failed backups
    """
    return f"""
    SELECT
        id,
        name,
        status,
        error,
        start_time,
        end_time,
        num_files,
        total_size
    FROM system.backups
    WHERE status IN ('BACKUP_FAILED', 'BACKUP_CANCELLED')
      AND start_time >= now() - INTERVAL {days} DAY
    ORDER BY start_time DESC
    """


def get_backup_age_query(connector):
    """
    Returns query for backup age analysis.

    Calculates how long ago the most recent successful backup was taken.

    Returns:
        str: SQL query for backup age
    """
    return """
    SELECT
        name,
        status,
        start_time,
        end_time,
        dateDiff('hour', end_time, now()) as hours_since_backup,
        dateDiff('day', end_time, now()) as days_since_backup,
        total_size,
        compressed_size,
        num_files
    FROM system.backups
    WHERE status = 'BACKUP_COMPLETE'
    ORDER BY end_time DESC
    LIMIT 10
    """


def get_backup_size_trend_query(connector, days=30):
    """
    Returns query for backup size trends over time.

    Args:
        connector: ClickHouse connector instance
        days: Number of days to analyze (default: 30)

    Returns:
        str: SQL query for backup size trends
    """
    return f"""
    SELECT
        toStartOfDay(start_time) as backup_date,
        count() as backups_count,
        sum(total_size) as total_size,
        sum(compressed_size) as total_compressed_size,
        avg(dateDiff('second', start_time, end_time)) as avg_duration_seconds
    FROM system.backups
    WHERE status = 'BACKUP_COMPLETE'
      AND start_time >= now() - INTERVAL {days} DAY
    GROUP BY backup_date
    ORDER BY backup_date DESC
    """


def get_backup_compression_ratio_query(connector):
    """
    Returns query for backup compression ratio analysis.

    Shows compression effectiveness for recent backups.

    Returns:
        str: SQL query for backup compression ratios
    """
    return """
    SELECT
        name,
        start_time,
        uncompressed_size,
        compressed_size,
        round((uncompressed_size - compressed_size) / uncompressed_size * 100, 2) as compression_ratio_percent,
        round(uncompressed_size / compressed_size, 2) as compression_factor
    FROM system.backups
    WHERE status = 'BACKUP_COMPLETE'
      AND uncompressed_size > 0
      AND compressed_size > 0
    ORDER BY start_time DESC
    LIMIT 20
    """


def get_longest_backups_query(connector, days=30, limit=10):
    """
    Returns query for longest-running backup operations.

    Identifies backups that took the most time to complete.

    Args:
        connector: ClickHouse connector instance
        days: Number of days to look back (default: 30)
        limit: Maximum number of backups to return (default: 10)

    Returns:
        str: SQL query for longest backups
    """
    return f"""
    SELECT
        name,
        start_time,
        end_time,
        dateDiff('second', start_time, end_time) as duration_seconds,
        status,
        total_size,
        num_files
    FROM system.backups
    WHERE start_time >= now() - INTERVAL {days} DAY
      AND end_time IS NOT NULL
    ORDER BY duration_seconds DESC
    LIMIT {limit}
    """


def get_backup_destination_summary_query(connector):
    """
    Returns query for backup destination analysis.

    Groups backups by destination to understand backup distribution.

    Returns:
        str: SQL query for backup destinations
    """
    return """
    SELECT
        substring(name, 1, position(name, '/')) as backup_destination,
        count() as backup_count,
        sum(total_size) as total_size,
        max(start_time) as last_backup_time,
        countIf(status = 'BACKUP_COMPLETE') as successful_count,
        countIf(status = 'BACKUP_FAILED') as failed_count
    FROM system.backups
    WHERE start_time >= now() - INTERVAL 30 DAY
    GROUP BY backup_destination
    ORDER BY backup_count DESC
    """

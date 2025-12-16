"""
Query library for comprehensive query analysis.

Provides a holistic view of query resource consumption including:
- True CPU time (execution time minus I/O wait)
- Percentage of total cluster CPU
- Temporal context (stats reset time, calls per hour)
- I/O analysis (absolute and percentage of query time)
- Cache hit rate per query
- Temp/WAL write analysis
- User attribution

This query is optimized for strategic workload analysis and optimization
prioritization, as opposed to incident response diagnostics.
"""

def get_comprehensive_query_analysis_query(connector):
    """
    Get comprehensive query analysis query based on PostgreSQL version.

    This query provides a holistic view of resource consumption patterns,
    ideal for identifying optimization opportunities and understanding
    workload characteristics.

    Version compatibility:
    - PG 13+: Full support (exec_time, I/O timing, WAL bytes)
    - PG 10-12: Limited I/O analysis (no I/O timing columns)
    - PG <10: Not supported (requires pg_stat_statements v1.8+)

    Args:
        connector: The PostgresConnector instance with version info

    Returns:
        str: SQL query for comprehensive query analysis
    """
    compatibility = connector.version_info

    # Handle column name differences between PG 13 and 14+
    if compatibility.get('is_pg14_or_newer'):
        exec_time_col = 'total_exec_time'
    else:
        exec_time_col = 'total_time'

    # I/O time columns - check both PG17+ (new) and legacy styles
    # These columns were added in PG 13, but pg_stat_statements must be updated
    if connector.has_pgstat_new_io_time:
        # PG 17+ has granular I/O timing columns
        io_time_expr = '(pss.shared_blk_read_time + pss.shared_blk_write_time + pss.local_blk_read_time + pss.local_blk_write_time + pss.temp_blk_read_time + pss.temp_blk_write_time)'
    elif connector.has_pgstat_legacy_io_time:
        # PG 13-16 has legacy I/O timing columns
        io_time_expr = '(pss.blk_read_time + pss.blk_write_time)'
    else:
        # PG <13 or track_io_timing = off
        io_time_expr = '0'

    # WAL bytes column added in PG 13
    if compatibility.get('major_version', 0) >= 13:
        wal_column = """
    -- Total WAL written in MB
    ROUND((pss.wal_bytes / 1024 / 1024)::numeric, 2) AS total_wal_written_mb,"""
    else:
        wal_column = """
    -- WAL bytes not available in PostgreSQL < 13
    NULL AS total_wal_written_mb,"""

    query = f"""
-- Comprehensive Query Analysis
-- Use a CTE to find the earliest non-null stats start time
WITH stats_start_time AS (
  SELECT
    COALESCE(
      (
        SELECT stats_reset
        FROM pg_stat_database
        WHERE datname = current_database()
      ),
      (
        SELECT stats_reset
        FROM pg_stat_bgwriter
      ),
      (
        SELECT pg_postmaster_start_time()
      )
    ) AS start_time
),
-- CTE to get cluster-wide totals for percentage calculations
cluster_totals AS (
  SELECT
    -- Total estimated CPU time used by all queries
    SUM(
      pss.{exec_time_col} - {io_time_expr}
    ) AS total_cluster_cpu_time_ms,
    -- Total hours since the last stats reset
    (
      NULLIF(
        EXTRACT(
          EPOCH
          FROM (now() - (SELECT start_time FROM stats_start_time))
        ),
        0
      ) / 3600
    ) AS total_hours_since_reset
  FROM pg_stat_statements pss
)
SELECT
  -- Who ran it
  r.rolname AS username,

  -- Frequency calculations
  (SELECT start_time FROM stats_start_time) AS stats_collection_start_time,
  pss.calls AS total_executions,
  ROUND(
    (pss.calls / ct.total_hours_since_reset)::numeric,
    2
  ) AS calls_per_hour,

  -- CPU calculations
  ROUND(
    (pss.{exec_time_col} - {io_time_expr})::numeric,
    2
  ) AS estimated_cpu_time_ms,
  ROUND(
    (
      (pss.{exec_time_col} - {io_time_expr}) / 3600000
    )::numeric,
    2
  ) AS estimated_cpu_time_hours,

  -- Percentage of total cluster CPU
  ROUND(
    (
      (pss.{exec_time_col} - {io_time_expr}) /
      NULLIF(ct.total_cluster_cpu_time_ms, 0) * 100
    )::numeric,
    2
  ) AS percent_of_total_cluster_cpu,

  -- I/O calculations
  ROUND(
    {io_time_expr}::numeric,
    2
  ) AS total_io_wait_time_ms,

  -- I/O as percentage of this query's total time
  ROUND(
    (
      {io_time_expr} /
      NULLIF(pss.{exec_time_col}, 0) * 100
    )::numeric,
    2
  ) AS io_wait_percent_of_total,

  -- Cache Hit Rate
  ROUND(
    (
      (pss.shared_blks_hit * 100) /
      NULLIF(pss.shared_blks_hit + pss.shared_blks_read, 0)
    )::numeric,
    2
  ) AS cache_hit_rate_percent,

  -- Rows
  pss.rows AS total_rows,

  -- Average rows per call
  ROUND(
    (pss.rows / NULLIF(pss.calls, 0))::numeric,
    2
  ) AS avg_rows_returned,

  -- Total temp written in MB (1 block = 8KB)
  ROUND(
    (pss.temp_blks_written * 8192 / 1024 / 1024)::numeric,
    2
  ) AS total_temp_written_mb,
  {wal_column}

  -- Query Text (full text with whitespace normalized)
  regexp_replace(pss.query, '\\s+', ' ', 'g') AS query
FROM
  pg_stat_statements pss
  JOIN pg_roles r ON pss.userid = r.oid
  CROSS JOIN cluster_totals ct
WHERE
  pss.calls > 0
ORDER BY
  estimated_cpu_time_ms DESC
LIMIT %(limit)s
"""

    return query

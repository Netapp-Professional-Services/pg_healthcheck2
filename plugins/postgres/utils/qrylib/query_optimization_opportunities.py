"""
Query library for query optimization opportunities analysis.

This module provides queries to:
1. Aggregate resource consumption by database user/service
2. Identify queries with missing indexes via sequential scan correlation
3. Extract full query text for detailed analysis
4. Suggest specific index candidates based on query patterns

Designed to complement comprehensive_query_analysis with actionable
optimization recommendations and team/service attribution.
"""


def get_user_resource_aggregation_query(connector):
    """
    Aggregate resource consumption by database user/service.

    Shows which users/services are consuming the most cluster resources,
    helping identify which teams to work with for optimization.

    Args:
        connector: The PostgresConnector instance with version info

    Returns:
        str: SQL query for user resource aggregation
    """
    compatibility = connector.version_info

    # Handle column name differences between PG 13 and 14+
    if compatibility.get('is_pg14_or_newer'):
        exec_time_col = 'total_exec_time'
    else:
        exec_time_col = 'total_time'

    # I/O time columns
    if connector.has_pgstat_new_io_time:
        io_time_expr = '(pss.shared_blk_read_time + pss.shared_blk_write_time + pss.local_blk_read_time + pss.local_blk_write_time + pss.temp_blk_read_time + pss.temp_blk_write_time)'
    elif connector.has_pgstat_legacy_io_time:
        io_time_expr = '(pss.blk_read_time + pss.blk_write_time)'
    else:
        io_time_expr = '0'

    query = f"""
-- User/Service Resource Aggregation
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
cluster_totals AS (
  SELECT
    SUM(pss.{exec_time_col} - {io_time_expr}) AS total_cluster_cpu_time_ms,
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
),
user_aggregates AS (
  SELECT
    r.rolname AS username,
    COUNT(*) AS query_count,
    SUM(pss.calls) AS total_executions,
    ROUND(
      (SUM(pss.{exec_time_col} - {io_time_expr}) /
       NULLIF((SELECT total_cluster_cpu_time_ms FROM cluster_totals), 0) * 100)::numeric,
      2
    ) AS percent_of_cluster_cpu,
    ROUND(
      (SUM(pss.calls) / NULLIF((SELECT total_hours_since_reset FROM cluster_totals), 0))::numeric,
      2
    ) AS avg_calls_per_hour,
    ROUND(
      (SUM(pss.{exec_time_col} - {io_time_expr}) / 1000)::numeric,
      2
    ) AS total_cpu_time_seconds,
    ROUND(
      (SUM({io_time_expr}) /
       NULLIF(SUM(pss.{exec_time_col}), 0) * 100)::numeric,
      2
    ) AS avg_io_wait_percent,
    ROUND(
      (SUM(pss.shared_blks_hit)::numeric * 100 /
       NULLIF(SUM(pss.shared_blks_hit) + SUM(pss.shared_blks_read), 0))::numeric,
      2
    ) AS avg_cache_hit_rate
  FROM pg_stat_statements pss
  JOIN pg_roles r ON pss.userid = r.oid
  WHERE pss.calls > 0
  GROUP BY r.rolname
)
SELECT
  username,
  query_count,
  total_executions,
  percent_of_cluster_cpu,
  avg_calls_per_hour,
  total_cpu_time_seconds,
  avg_io_wait_percent,
  avg_cache_hit_rate
FROM user_aggregates
WHERE percent_of_cluster_cpu > 1.0  -- Only show users consuming >1%% cluster CPU
ORDER BY percent_of_cluster_cpu DESC
LIMIT %(limit)s
"""

    return query


def get_query_with_seqscan_correlation_query(connector):
    """
    Get detailed query information correlated with sequential scan data.

    Joins pg_stat_statements with pg_stat_user_tables to identify queries
    that are likely causing sequential scans (missing indexes).

    Args:
        connector: The PostgresConnector instance with version info

    Returns:
        str: SQL query for query-seqscan correlation
    """
    compatibility = connector.version_info

    # Handle column name differences
    if compatibility.get('is_pg14_or_newer'):
        exec_time_col = 'total_exec_time'
    else:
        exec_time_col = 'total_time'

    # I/O time columns
    if connector.has_pgstat_new_io_time:
        io_time_expr = '(pss.shared_blk_read_time + pss.shared_blk_write_time + pss.local_blk_read_time + pss.local_blk_write_time + pss.temp_blk_read_time + pss.temp_blk_write_time)'
    elif connector.has_pgstat_legacy_io_time:
        io_time_expr = '(pss.blk_read_time + pss.blk_write_time)'
    else:
        io_time_expr = '0'

    query = f"""
-- Query details with full text and sequential scan correlation
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
cluster_totals AS (
  SELECT
    SUM(pss.{exec_time_col} - {io_time_expr}) AS total_cluster_cpu_time_ms,
    (
      NULLIF(
        EXTRACT(
          EPOCH
          FROM (now() - (SELECT start_time FROM stats_start_time))
        ),
        0
      ) / 3600
    ) AS hours_since_reset
  FROM pg_stat_statements pss
),
query_details AS (
  SELECT
    pss.queryid,
    r.rolname AS username,
    pss.query AS full_query_text,
    pss.calls AS total_executions,
    ROUND((pss.calls / ct.hours_since_reset)::numeric, 2) AS calls_per_hour,
    ROUND((pss.{exec_time_col} - {io_time_expr})::numeric, 2) AS cpu_time_ms,
    ROUND(((pss.{exec_time_col} - {io_time_expr}) / 3600000)::numeric, 2) AS cpu_time_hours,
    ROUND(
      ((pss.{exec_time_col} - {io_time_expr}) /
       NULLIF(ct.total_cluster_cpu_time_ms, 0) * 100)::numeric,
      2
    ) AS percent_of_cluster_cpu,
    ROUND((pss.{exec_time_col} / NULLIF(pss.calls, 0))::numeric, 2) AS avg_exec_time_ms,
    ROUND(({io_time_expr})::numeric, 2) AS io_wait_time_ms,
    ROUND(
      ({io_time_expr} / NULLIF(pss.{exec_time_col}, 0) * 100)::numeric,
      2
    ) AS io_wait_percent,
    ROUND(
      (pss.shared_blks_hit * 100 /
       NULLIF(pss.shared_blks_hit + pss.shared_blks_read, 0))::numeric,
      2
    ) AS cache_hit_rate_percent,
    ROUND((pss.temp_blks_written * 8192 / 1024 / 1024)::numeric, 2) AS temp_written_mb
  FROM pg_stat_statements pss
  JOIN pg_roles r ON pss.userid = r.oid
  CROSS JOIN cluster_totals ct
  WHERE pss.calls > 0
)
SELECT
  qd.queryid,
  qd.username,
  qd.full_query_text,
  qd.total_executions,
  qd.calls_per_hour,
  qd.cpu_time_ms,
  qd.cpu_time_hours,
  qd.percent_of_cluster_cpu,
  qd.avg_exec_time_ms,
  qd.io_wait_time_ms,
  qd.io_wait_percent,
  qd.cache_hit_rate_percent,
  qd.temp_written_mb
FROM query_details qd
WHERE qd.percent_of_cluster_cpu >= %(min_cpu_percent)s
ORDER BY qd.percent_of_cluster_cpu DESC
LIMIT %(limit)s
"""

    return query


def get_tables_with_high_seqscans_query():
    """
    Get tables with high sequential scan counts.

    This data is used to correlate with query patterns to identify
    missing index opportunities.

    Returns:
        str: SQL query for high sequential scan tables
    """
    query = """
-- Tables with high sequential scans (potential missing indexes)
SELECT
  schemaname,
  relname AS tablename,
  seq_scan,
  seq_tup_read,
  idx_scan,
  n_live_tup,
  CASE
    WHEN idx_scan > 0 THEN
      ROUND((seq_scan::numeric / (seq_scan + idx_scan) * 100), 2)
    ELSE
      100.0
  END AS seq_scan_percent,
  CASE
    WHEN n_live_tup > 0 THEN
      ROUND((seq_tup_read::numeric / seq_scan / NULLIF(n_live_tup, 0) * 100), 2)
    ELSE
      0
  END AS avg_rows_per_scan_percent
FROM pg_stat_user_tables
WHERE seq_scan > 1000  -- Only tables with significant sequential scans
  AND schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY seq_scan DESC
LIMIT %(limit)s
"""

    return query

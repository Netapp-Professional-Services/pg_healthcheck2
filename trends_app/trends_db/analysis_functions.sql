-- =====================================================
-- Database Health Intelligence Platform
-- Analysis Functions for Flask Application
-- =====================================================
--
-- Purpose: Stored procedures/functions that Flask app calls
--          instead of embedding SQL in Python code
--
-- Convention: All analysis functions check privileges internally
--             and return empty results if user lacks access
--
-- Usage from Python:
--   cursor.execute("SELECT * FROM get_migration_candidates(%s)", [limit])
--
-- Author: Perry Clark
-- Date: 2025-11-10
-- =====================================================

\echo 'Creating analysis functions for Flask integration...'

-- =====================================================
-- Function: get_migration_candidates
-- Purpose: Get high-frequency write workloads for migration
-- =====================================================

CREATE OR REPLACE FUNCTION public.get_migration_candidates(
  p_limit INTEGER DEFAULT 50,
  p_min_frequency INTEGER DEFAULT 50000,
  p_days_lookback INTEGER DEFAULT 30
)
RETURNS TABLE(
  company_name TEXT,
  company_id INTEGER,
  run_id INTEGER,
  run_timestamp TIMESTAMP WITH TIME ZONE,
  db_user TEXT,
  calls_per_hour NUMERIC,
  total_executions NUMERIC,
  cpu_percent NUMERIC,
  cache_hit_rate NUMERIC,
  query_preview TEXT,
  migration_priority TEXT,
  recommendation TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  -- Check privilege before returning data
  IF NOT (checkprivilege('ViewPostgreSQLAnalysis') OR
          checkprivilege('ViewAllTechnologies') OR
          checkprivilege('ViewMigrationPipeline')) THEN
    -- Return empty result if no privilege
    RETURN;
  END IF;

  RETURN QUERY
  SELECT
    co.company_name::TEXT,
    co.id,
    hcr.id,
    hcr.run_timestamp,
    (query_data->>'username')::TEXT,
    (query_data->>'calls_per_hour')::NUMERIC,
    (query_data->>'total_executions')::NUMERIC,
    (query_data->>'percent_of_total_cluster_cpu')::NUMERIC,
    (query_data->>'cache_hit_rate_percent')::NUMERIC,
    substring(query_data->>'query' from 1 for 80)::TEXT,
    CASE
      WHEN (query_data->>'calls_per_hour')::NUMERIC > 500000 THEN 'critical'
      WHEN (query_data->>'calls_per_hour')::NUMERIC > 200000 THEN 'high'
      WHEN (query_data->>'calls_per_hour')::NUMERIC > 100000 THEN 'medium'
      ELSE 'low'
    END::TEXT,
    CASE
      WHEN (query_data->>'calls_per_hour')::NUMERIC > 200000
      THEN '🚨 Strongly consider Kafka/Cassandra'
      WHEN (query_data->>'calls_per_hour')::NUMERIC > 100000
      THEN '⚠️  Consider Kafka migration'
      ELSE '✅ PostgreSQL appropriate'
    END::TEXT
  FROM public.health_check_runs hcr
  JOIN public.companies co ON (co.id = hcr.company_id)
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
  ) AS query_data
  WHERE
    hcr.db_technology = 'postgres'
    AND (
      query_data->>'query' ILIKE '%INSERT INTO%' OR
      query_data->>'query' ILIKE '%UPDATE %' OR
      query_data->>'query' ILIKE '%DELETE FROM%'
    )
    AND (query_data->>'calls_per_hour')::NUMERIC > p_min_frequency
    AND hcr.run_timestamp > NOW() - (p_days_lookback || ' days')::INTERVAL
  ORDER BY (query_data->>'calls_per_hour')::NUMERIC DESC
  LIMIT p_limit;
END;
$$;

COMMENT ON FUNCTION public.get_migration_candidates(INTEGER, INTEGER, INTEGER) IS
'Returns high-frequency write workloads that may benefit from Kafka/Cassandra migration.
Parameters: limit (default 50), min_frequency (default 50000), days_lookback (default 30).
Requires ViewPostgreSQLAnalysis, ViewAllTechnologies, or ViewMigrationPipeline privilege.';

-- =====================================================
-- Function: get_write_volume_trends
-- Purpose: Get write volume growth trends for a company
-- =====================================================

CREATE OR REPLACE FUNCTION public.get_write_volume_trends(
  p_company_id INTEGER,
  p_days_lookback INTEGER DEFAULT 90
)
RETURNS TABLE(
  check_date DATE,
  total_writes_per_hour NUMERIC,
  unique_write_queries BIGINT,
  avg_cpu_percent NUMERIC,
  max_single_query_rate NUMERIC
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  -- Check privilege
  IF NOT (checkprivilege('ViewPostgreSQLAnalysis') OR
          checkprivilege('ViewAllTechnologies')) THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT
    hcr.run_timestamp::DATE,
    SUM((query_data->>'calls_per_hour')::NUMERIC),
    COUNT(DISTINCT query_data->>'query'),
    AVG((query_data->>'percent_of_total_cluster_cpu')::NUMERIC),
    MAX((query_data->>'calls_per_hour')::NUMERIC)
  FROM public.health_check_runs hcr
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
  ) AS query_data
  WHERE
    hcr.company_id = p_company_id
    AND hcr.db_technology = 'postgres'
    AND (
      query_data->>'query' ILIKE '%INSERT INTO%' OR
      query_data->>'query' ILIKE '%UPDATE %' OR
      query_data->>'query' ILIKE '%DELETE FROM%'
    )
    AND hcr.run_timestamp > NOW() - (p_days_lookback || ' days')::INTERVAL
  GROUP BY hcr.run_timestamp::DATE
  ORDER BY hcr.run_timestamp::DATE DESC;
END;
$$;

COMMENT ON FUNCTION public.get_write_volume_trends(INTEGER, INTEGER) IS
'Returns daily write volume trends for a specific company.
Parameters: company_id, days_lookback (default 90).
Requires ViewPostgreSQLAnalysis or ViewAllTechnologies privilege.';

-- =====================================================
-- Function: get_migration_pipeline_summary
-- Purpose: Executive summary of migration opportunities
-- =====================================================

CREATE OR REPLACE FUNCTION public.get_migration_pipeline_summary()
RETURNS TABLE(
  total_opportunities INTEGER,
  critical_count INTEGER,
  high_count INTEGER,
  medium_count INTEGER,
  estimated_monthly_revenue TEXT,
  top_company TEXT,
  top_company_workload NUMERIC
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_critical INTEGER;
  v_high INTEGER;
  v_medium INTEGER;
  v_total INTEGER;
BEGIN
  -- Check privilege
  IF NOT (checkprivilege('ViewMigrationPipeline') OR
          checkprivilege('ViewAllTechnologies') OR
          checkprivilege('ViewExecutiveAnalysis')) THEN
    -- Return zeros if no privilege
    RETURN QUERY SELECT 0, 0, 0, 0, '$0'::TEXT, ''::TEXT, 0::NUMERIC;
    RETURN;
  END IF;

  -- Count opportunities by priority (grouped by company)
  -- Each company counts as ONE opportunity even if they have multiple high-frequency queries
  WITH company_priorities AS (
    SELECT
      company_id,
      company_name,
      MAX(calls_per_hour) as max_workload,
      MAX(
        CASE migration_priority
          WHEN 'critical' THEN 3
          WHEN 'high' THEN 2
          WHEN 'medium' THEN 1
          ELSE 0
        END
      ) as priority_rank
    FROM postgres_analysis.high_frequency_write_candidates
    GROUP BY company_id, company_name
  )
  SELECT
    COUNT(*) FILTER (WHERE priority_rank = 3),
    COUNT(*) FILTER (WHERE priority_rank = 2),
    COUNT(*) FILTER (WHERE priority_rank = 1),
    COUNT(*)
  INTO v_critical, v_high, v_medium, v_total
  FROM company_priorities;

  -- Return summary
  RETURN QUERY
  SELECT
    v_total,
    v_critical,
    v_high,
    v_medium,
    '$' || ((v_critical * 2000) + (v_high * 2000))::TEXT || '/month' as revenue,
    (SELECT company_name FROM (
      SELECT company_id, company_name, MAX(calls_per_hour) as max_workload
      FROM postgres_analysis.high_frequency_write_candidates
      GROUP BY company_id, company_name
      ORDER BY max_workload DESC
      LIMIT 1
    ) t)::TEXT,
    (SELECT max_workload FROM (
      SELECT company_id, MAX(calls_per_hour) as max_workload
      FROM postgres_analysis.high_frequency_write_candidates
      GROUP BY company_id
      ORDER BY max_workload DESC
      LIMIT 1
    ) t)::NUMERIC;
END;
$$;

COMMENT ON FUNCTION public.get_migration_pipeline_summary() IS
'Returns executive summary of technology migration opportunities and revenue potential.
Requires ViewMigrationPipeline, ViewAllTechnologies, or ViewExecutiveAnalysis privilege.';

-- =====================================================
-- Function: get_customer_technology_footprint
-- Purpose: Show what technologies each customer uses
-- =====================================================

CREATE OR REPLACE FUNCTION public.get_customer_technology_footprint(
  p_days_lookback INTEGER DEFAULT 90
)
RETURNS TABLE(
  company_name TEXT,
  company_id INTEGER,
  db_technology TEXT,
  health_check_count BIGINT,
  last_check TIMESTAMP WITH TIME ZONE,
  has_migration_opportunity BOOLEAN
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  -- Check privilege
  IF NOT (checkprivilege('ViewExecutiveAnalysis') OR
          checkprivilege('ViewAllTechnologies')) THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT
    co.company_name::TEXT,
    co.id,
    hcr.db_technology::TEXT,
    COUNT(DISTINCT hcr.id),
    MAX(hcr.run_timestamp),
    EXISTS(
      SELECT 1 FROM postgres_analysis.high_frequency_write_candidates hwc
      WHERE hwc.company_id = co.id
        AND hwc.migration_priority IN ('high', 'critical')
    )
  FROM public.companies co
  JOIN public.health_check_runs hcr ON (hcr.company_id = co.id)
  WHERE hcr.run_timestamp > NOW() - (p_days_lookback || ' days')::INTERVAL
  GROUP BY co.company_name, co.id, hcr.db_technology
  ORDER BY co.company_name, hcr.db_technology;
END;
$$;

COMMENT ON FUNCTION public.get_customer_technology_footprint(INTEGER) IS
'Shows which technologies each customer uses and identifies upsell opportunities.
Parameters: days_lookback (default 90).
Requires ViewExecutiveAnalysis or ViewAllTechnologies privilege.';

-- =====================================================
-- Function: get_query_details
-- Purpose: Get full details for a specific high-frequency query
-- =====================================================

CREATE OR REPLACE FUNCTION public.get_query_details(
  p_run_id INTEGER,
  p_query_text TEXT
)
RETURNS TABLE(
  username TEXT,
  calls_per_hour NUMERIC,
  total_executions NUMERIC,
  estimated_cpu_time_ms NUMERIC,
  cpu_percent NUMERIC,
  io_wait_ms NUMERIC,
  io_wait_percent NUMERIC,
  cache_hit_rate NUMERIC,
  avg_exec_time_ms NUMERIC,
  temp_written_mb NUMERIC,
  wal_written_mb NUMERIC,
  full_query TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  -- Check privilege
  IF NOT (checkprivilege('ViewPostgreSQLAnalysis') OR
          checkprivilege('ViewAllTechnologies')) THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT
    (query_data->>'username')::TEXT,
    (query_data->>'calls_per_hour')::NUMERIC,
    (query_data->>'total_executions')::NUMERIC,
    (query_data->>'estimated_cpu_time_ms')::NUMERIC,
    (query_data->>'percent_of_total_cluster_cpu')::NUMERIC,
    (query_data->>'total_io_wait_time_ms')::NUMERIC,
    (query_data->>'io_wait_percent_of_total')::NUMERIC,
    (query_data->>'cache_hit_rate_percent')::NUMERIC,
    ((query_data->>'estimated_cpu_time_ms')::NUMERIC / NULLIF((query_data->>'total_executions')::NUMERIC, 0)),
    (query_data->>'total_temp_written_mb')::NUMERIC,
    (query_data->>'total_wal_written_mb')::NUMERIC,
    (query_data->>'query')::TEXT
  FROM public.health_check_runs hcr
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
  ) AS query_data
  WHERE
    hcr.id = p_run_id
    AND query_data->>'query' LIKE '%' || p_query_text || '%'
  LIMIT 1;
END;
$$;

COMMENT ON FUNCTION public.get_query_details(INTEGER, TEXT) IS
'Returns detailed metrics for a specific query from a health check run.
Parameters: run_id, query_text (partial match).
Requires ViewPostgreSQLAnalysis or ViewAllTechnologies privilege.';

-- =====================================================
-- Function: get_consulting_opportunities_summary
-- Purpose: Executive summary of consulting engagement opportunities
-- =====================================================

CREATE OR REPLACE FUNCTION public.get_consulting_opportunities_summary()
RETURNS TABLE(
  total_opportunities INTEGER,
  total_companies INTEGER,
  critical_priority_count INTEGER,
  high_priority_count INTEGER,
  medium_priority_count INTEGER,
  total_triggered_rules INTEGER,
  estimated_revenue_range TEXT,
  top_opportunity_type TEXT,
  top_opportunity_count INTEGER
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_total INTEGER;
  v_companies INTEGER;
  v_critical INTEGER;
  v_high INTEGER;
  v_medium INTEGER;
  v_rules INTEGER;
BEGIN
  -- Check privilege
  IF NOT (checkprivilege('ViewConsultingOpportunities') OR
          checkprivilege('ViewAllTechnologies') OR
          checkprivilege('ViewExecutiveAnalysis')) THEN
    -- Return zeros if no privilege
    RETURN QUERY SELECT 0, 0, 0, 0, 0, 0, '$0'::TEXT, ''::TEXT, 0;
    RETURN;
  END IF;

  -- Get overall counts
  SELECT
    COUNT(*),
    COUNT(DISTINCT company_id),
    COUNT(*) FILTER (WHERE consulting_priority = 'critical'),
    COUNT(*) FILTER (WHERE consulting_priority = 'high'),
    COUNT(*) FILTER (WHERE consulting_priority = 'medium'),
    SUM(triggered_rule_count)
  INTO v_total, v_companies, v_critical, v_high, v_medium, v_rules
  FROM consulting_analysis.consulting_opportunities_from_rules;

  -- Return summary with estimated revenue
  RETURN QUERY
  SELECT
    v_total,
    v_companies,
    v_critical,
    v_high,
    v_medium,
    v_rules,
    CASE
      WHEN v_total = 0 THEN '$0'
      ELSE '$' || ((v_critical * 15000) + (v_high * 12000) + (v_medium * 8000))::TEXT || ' - $' ||
           ((v_critical * 25000) + (v_high * 18000) + (v_medium * 12000))::TEXT
    END::TEXT as revenue_range,
    (SELECT ecs.consulting_type FROM consulting_analysis.executive_consulting_summary ecs
     ORDER BY ecs.total_opportunities DESC LIMIT 1)::TEXT,
    (SELECT ecs.total_opportunities FROM consulting_analysis.executive_consulting_summary ecs
     ORDER BY ecs.total_opportunities DESC LIMIT 1)::INTEGER;
END;
$$;

COMMENT ON FUNCTION public.get_consulting_opportunities_summary() IS
'Returns executive summary of consulting engagement opportunities and estimated revenue.
Based on triggered rules (high/critical severity only).
Requires ViewConsultingOpportunities, ViewAllTechnologies, or ViewExecutiveAnalysis privilege.';

-- =====================================================
-- Grant execute permissions
-- =====================================================

GRANT EXECUTE ON FUNCTION public.get_migration_candidates(INTEGER, INTEGER, INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_write_volume_trends(INTEGER, INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_migration_pipeline_summary() TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_customer_technology_footprint(INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_query_details(INTEGER, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_consulting_opportunities_summary() TO PUBLIC;

\echo '✓ Analysis functions created and granted to PUBLIC'
\echo ''
\echo 'Available Functions:'
\echo '  - get_migration_candidates(limit, min_freq, days)'
\echo '  - get_write_volume_trends(company_id, days)'
\echo '  - get_migration_pipeline_summary()'
\echo '  - get_customer_technology_footprint(days)'
\echo '  - get_query_details(run_id, query_text)'
\echo '  - get_consulting_opportunities_summary()'
\echo ''
\echo 'Usage from Python:'
\echo '  cursor.execute("SELECT * FROM get_migration_candidates(%s, %s, %s)", [50, 50000, 30])'
\echo ''

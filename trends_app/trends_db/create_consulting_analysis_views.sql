-- =====================================================
-- Database Health Intelligence Platform
-- Consulting Engagement Opportunity Analysis
-- =====================================================
--
-- Purpose: Identify professional services and consulting opportunities
--          based on health check findings across all technologies
--
-- Consulting Opportunity Types:
--   - Query Optimization: Slow queries, missing indexes, seq scans
--   - Vacuum Tuning: Bloat detection, autovacuum configuration issues
--   - Performance Tuning: Configuration optimization, resource allocation
--   - Schema Design: Table structure issues, partitioning opportunities
--   - High Availability: Replication lag, failover readiness
--   - Capacity Planning: Growth trends, scaling recommendations
--   - Security Audits: SSL/TLS config, authentication, encryption
--
-- Usage:
--   psql -h <host> -p <port> -U <user> -d health_trends -f create_consulting_analysis_views.sql
--
-- Author: Perry Clark
-- Date: 2025-11-10
-- =====================================================

\echo 'Creating consulting engagement analysis views...'

-- =====================================================
-- Step 1: Create Privilege for Consulting Analysis
-- =====================================================

\echo 'Step 1: Creating consulting analysis privilege...'

SELECT createpriv('TRENDS', 'ViewConsultingOpportunities', 'Can view consulting engagement opportunities across all technologies');

\echo '✓ Privilege created'

-- =====================================================
-- Step 2: Create Consulting Analysis Schema
-- =====================================================

\echo 'Step 2: Creating consulting analysis schema...'

CREATE SCHEMA IF NOT EXISTS consulting_analysis;

COMMENT ON SCHEMA consulting_analysis IS 'Professional services and consulting engagement opportunity detection';

\echo '✓ Schema created'

-- =====================================================
-- Step 3: PostgreSQL Consulting Opportunities
-- =====================================================

\echo 'Step 3: Creating PostgreSQL consulting opportunity views...'

-- View: Query optimization consulting opportunities
CREATE OR REPLACE VIEW consulting_analysis.postgres_query_optimization_opps AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Count of slow queries
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
    ) AS query
    WHERE (query->>'avg_duration_ms')::numeric > 1000
  ) as slow_query_count,
  -- Count of queries with low cache hit rate
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
    ) AS query
    WHERE (query->>'cache_hit_rate_percent')::numeric < 90
  ) as low_cache_hit_count,
  -- Count of queries doing sequential scans
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_missing_indexes' -> 'data' -> 'tables_missing_indexes'
    ) AS table_data
  ) as missing_index_count,
  -- Overall CPU consumption from inefficient queries
  (
    SELECT SUM((query->>'percent_of_total_cluster_cpu')::numeric)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
    ) AS query
    WHERE (query->>'avg_duration_ms')::numeric > 500
  ) as inefficient_query_cpu_percent,
  -- Determine consulting priority
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
      ) AS query
      WHERE (query->>'avg_duration_ms')::numeric > 5000
    ) > 5 THEN 'critical'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
      ) AS query
      WHERE (query->>'avg_duration_ms')::numeric > 1000
    ) > 3 THEN 'high'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
      ) AS query
      WHERE (query->>'avg_duration_ms')::numeric > 500
    ) > 2 THEN 'medium'
    ELSE 'low'
  END as consulting_priority,
  -- Recommendation
  '🔧 Query Optimization Consulting: Identify and optimize slow queries, add missing indexes, improve query plans' as engagement_type,
  -- Estimated engagement value
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
      ) AS query
      WHERE (query->>'avg_duration_ms')::numeric > 5000
    ) > 5 THEN '$15,000-$25,000'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
      ) AS query
      WHERE (query->>'avg_duration_ms')::numeric > 1000
    ) > 3 THEN '$10,000-$15,000'
    ELSE '$5,000-$10,000'
  END as estimated_value
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'postgres'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    -- Has slow queries
    (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
      ) AS query
      WHERE (query->>'avg_duration_ms')::numeric > 500
    ) > 0
    OR
    -- Has missing indexes
    (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_missing_indexes' -> 'data' -> 'tables_missing_indexes'
      ) AS table_data
    ) > 0
  )
ORDER BY consulting_priority DESC, inefficient_query_cpu_percent DESC NULLS LAST;

COMMENT ON VIEW consulting_analysis.postgres_query_optimization_opps IS
'Identifies PostgreSQL databases with query performance issues requiring optimization consulting.
Detects slow queries, missing indexes, sequential scans, and low cache hit rates.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

-- View: Vacuum tuning consulting opportunities
CREATE OR REPLACE VIEW consulting_analysis.postgres_vacuum_tuning_opps AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Count of bloated tables
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
    ) AS bloat
    WHERE (bloat->>'bloat_percent')::numeric > 20
  ) as bloated_table_count,
  -- Total wasted space (GB)
  (
    SELECT SUM((bloat->>'wasted_gb')::numeric)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
    ) AS bloat
  ) as total_wasted_gb,
  -- Autovacuum configuration issues
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_autovacuum_config' -> 'data' -> 'issues_found'
  )::boolean as has_autovacuum_issues,
  -- Long-running transactions blocking vacuum
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_long_running_transactions' -> 'data' -> 'long_transactions'
    ) AS txn
    WHERE (txn->>'duration_hours')::numeric > 2
  ) as blocking_transaction_count,
  -- Determine consulting priority
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
      ) AS bloat
      WHERE (bloat->>'bloat_percent')::numeric > 50
    ) > 3 THEN 'critical'
    WHEN (
      SELECT SUM((bloat->>'wasted_gb')::numeric)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
      ) AS bloat
    ) > 100 THEN 'high'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
      ) AS bloat
      WHERE (bloat->>'bloat_percent')::numeric > 20
    ) > 1 THEN 'medium'
    ELSE 'low'
  END as consulting_priority,
  -- Recommendation
  '🔧 Vacuum & Bloat Tuning: Configure autovacuum, implement bloat reduction strategy, optimize maintenance windows' as engagement_type,
  -- Estimated engagement value
  CASE
    WHEN (
      SELECT SUM((bloat->>'wasted_gb')::numeric)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
      ) AS bloat
    ) > 100 THEN '$12,000-$20,000'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
      ) AS bloat
      WHERE (bloat->>'bloat_percent')::numeric > 20
    ) > 3 THEN '$8,000-$12,000'
    ELSE '$5,000-$8,000'
  END as estimated_value
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'postgres'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    -- Has bloated tables
    (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_table_bloat' -> 'data' -> 'bloated_tables'
      ) AS bloat
      WHERE (bloat->>'bloat_percent')::numeric > 20
    ) > 0
    OR
    -- Has autovacuum issues
    (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_autovacuum_config' -> 'data' -> 'issues_found'
    )::boolean = true
  )
ORDER BY consulting_priority DESC, total_wasted_gb DESC NULLS LAST;

COMMENT ON VIEW consulting_analysis.postgres_vacuum_tuning_opps IS
'Identifies PostgreSQL databases with vacuum/bloat issues requiring tuning consulting.
Detects table bloat, autovacuum configuration problems, and maintenance inefficiencies.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

-- View: Performance configuration tuning opportunities
CREATE OR REPLACE VIEW consulting_analysis.postgres_performance_tuning_opps AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Configuration issues
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_configuration_best_practices' -> 'data' -> 'recommendations'
    ) AS config
  ) as config_recommendation_count,
  -- Connection pool issues
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_connection_pooling' -> 'data' -> 'needs_pooling'
  )::boolean as needs_connection_pooling,
  -- Replication lag issues
  (
    SELECT MAX((replica->>'replication_lag_mb')::numeric)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_replication_status' -> 'data' -> 'replicas'
    ) AS replica
  ) as max_replication_lag_mb,
  -- Checkpoint tuning needs
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_checkpoint_tuning' -> 'data' -> 'needs_tuning'
  )::boolean as needs_checkpoint_tuning,
  -- Determine consulting priority
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_configuration_best_practices' -> 'data' -> 'recommendations'
      ) AS config
    ) > 10 THEN 'high'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_configuration_best_practices' -> 'data' -> 'recommendations'
      ) AS config
    ) > 5 THEN 'medium'
    ELSE 'low'
  END as consulting_priority,
  -- Recommendation
  '🔧 Performance Tuning: Optimize configuration parameters, implement connection pooling, tune checkpoints/WAL' as engagement_type,
  -- Estimated engagement value
  '$8,000-$15,000' as estimated_value
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'postgres'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    -- Has configuration recommendations
    (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_configuration_best_practices' -> 'data' -> 'recommendations'
      ) AS config
    ) > 3
    OR
    -- Needs connection pooling
    (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_connection_pooling' -> 'data' -> 'needs_pooling'
    )::boolean = true
  )
ORDER BY consulting_priority DESC, config_recommendation_count DESC NULLS LAST;

COMMENT ON VIEW consulting_analysis.postgres_performance_tuning_opps IS
'Identifies PostgreSQL databases with performance configuration issues.
Detects suboptimal settings, connection pool needs, checkpoint tuning opportunities.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

\echo '✓ PostgreSQL consulting views created'

-- =====================================================
-- Step 4: Kafka Consulting Opportunities
-- =====================================================

\echo 'Step 4: Creating Kafka consulting opportunity views...'

-- View: Kafka performance tuning opportunities
CREATE OR REPLACE VIEW consulting_analysis.kafka_performance_tuning_opps AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Partition imbalance issues
  (
    SELECT MAX((broker->>'partition_count')::numeric) - MIN((broker->>'partition_count')::numeric)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_partition_balance' -> 'data' -> 'brokers'
    ) AS broker
  ) as partition_imbalance,
  -- Under-replicated partitions
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'kafka_overview' -> 'data' -> 'under_replicated_partitions'
  )::integer as under_replicated_partitions,
  -- Topic configuration issues
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_topic_configuration' -> 'data' -> 'topics_with_issues'
    ) AS topic
  ) as topic_config_issue_count,
  -- GC pause issues
  (
    SELECT AVG((broker->>'max_gc_pause_ms')::numeric)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_gc_pauses' -> 'data' -> 'brokers'
    ) AS broker
  ) as avg_max_gc_pause_ms,
  -- Determine consulting priority
  CASE
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'kafka_overview' -> 'data' -> 'under_replicated_partitions'
    )::integer > 10 THEN 'critical'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_topic_configuration' -> 'data' -> 'topics_with_issues'
      ) AS topic
    ) > 5 THEN 'high'
    ELSE 'medium'
  END as consulting_priority,
  -- Recommendation
  '🔧 Kafka Performance Tuning: Rebalance partitions, optimize topic configs, tune JVM/GC settings' as engagement_type,
  -- Estimated engagement value
  '$10,000-$18,000' as estimated_value
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'kafka'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    -- Has under-replicated partitions
    (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'kafka_overview' -> 'data' -> 'under_replicated_partitions'
    )::integer > 0
    OR
    -- Has topic configuration issues
    (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_topic_configuration' -> 'data' -> 'topics_with_issues'
      ) AS topic
    ) > 0
  )
ORDER BY consulting_priority DESC, under_replicated_partitions DESC NULLS LAST;

COMMENT ON VIEW consulting_analysis.kafka_performance_tuning_opps IS
'Identifies Kafka clusters with performance/configuration issues requiring tuning consulting.
Detects partition imbalance, under-replication, topic config problems, GC issues.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

\echo '✓ Kafka consulting views created'

-- =====================================================
-- Step 5: Cassandra Consulting Opportunities
-- =====================================================

\echo 'Step 5: Creating Cassandra consulting opportunity views...'

-- View: Cassandra performance tuning opportunities
CREATE OR REPLACE VIEW consulting_analysis.cassandra_performance_tuning_opps AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Compaction strategy issues
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_compaction_strategy' -> 'data' -> 'tables_with_issues'
    ) AS table_data
  ) as compaction_issue_count,
  -- Read/write latency
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
  )::numeric as read_p99_ms,
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_read_write_latency' -> 'data' -> 'write_p99_ms'
  )::numeric as write_p99_ms,
  -- GC pressure
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_memory_pressure' -> 'data' -> 'heap_used_percent'
  )::numeric as heap_used_percent,
  -- Determine consulting priority
  CASE
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
    )::numeric > 100 THEN 'high'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_compaction_strategy' -> 'data' -> 'tables_with_issues'
      ) AS table_data
    ) > 3 THEN 'medium'
    ELSE 'low'
  END as consulting_priority,
  -- Recommendation
  '🔧 Cassandra Performance Tuning: Optimize compaction strategies, tune read/write paths, configure caching' as engagement_type,
  -- Estimated engagement value
  '$12,000-$20,000' as estimated_value
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'cassandra'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    -- Has compaction issues
    (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_compaction_strategy' -> 'data' -> 'tables_with_issues'
      ) AS table_data
    ) > 0
    OR
    -- Has high latency
    (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
    )::numeric > 50
  )
ORDER BY consulting_priority DESC, read_p99_ms DESC NULLS LAST;

COMMENT ON VIEW consulting_analysis.cassandra_performance_tuning_opps IS
'Identifies Cassandra clusters with performance issues requiring tuning consulting.
Detects compaction strategy problems, high latency, memory pressure.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

\echo '✓ Cassandra consulting views created'

-- =====================================================
-- Step 6: OpenSearch Consulting Opportunities
-- =====================================================

\echo 'Step 6: Creating OpenSearch consulting opportunity views...'

-- View: OpenSearch performance tuning opportunities
CREATE OR REPLACE VIEW consulting_analysis.opensearch_performance_tuning_opps AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Shard allocation issues
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_shard_allocation' -> 'data' -> 'unassigned_shards'
  )::integer as unassigned_shards,
  -- Total shard count
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_shard_allocation' -> 'data' -> 'total_shards'
  )::integer as total_shards,
  -- JVM heap usage
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_jvm_memory' -> 'data' -> 'heap_used_percent'
  )::numeric as heap_used_percent,
  -- Query performance issues
  (
    SELECT COUNT(*)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_slow_queries' -> 'data' -> 'slow_queries'
    ) AS query
  ) as slow_query_count,
  -- Determine consulting priority
  CASE
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_shard_allocation' -> 'data' -> 'unassigned_shards'
    )::integer > 10 THEN 'critical'
    WHEN (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_slow_queries' -> 'data' -> 'slow_queries'
      ) AS query
    ) > 5 THEN 'high'
    ELSE 'medium'
  END as consulting_priority,
  -- Recommendation
  '🔧 OpenSearch Performance Tuning: Optimize shard allocation, tune JVM heap, improve query performance' as engagement_type,
  -- Estimated engagement value
  '$10,000-$16,000' as estimated_value
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'opensearch'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    -- Has unassigned shards
    (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_shard_allocation' -> 'data' -> 'unassigned_shards'
    )::integer > 0
    OR
    -- Has slow queries
    (
      SELECT COUNT(*)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'check_slow_queries' -> 'data' -> 'slow_queries'
      ) AS query
    ) > 0
  )
ORDER BY consulting_priority DESC, unassigned_shards DESC NULLS LAST;

COMMENT ON VIEW consulting_analysis.opensearch_performance_tuning_opps IS
'Identifies OpenSearch clusters with performance issues requiring tuning consulting.
Detects shard allocation problems, JVM issues, slow queries.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

\echo '✓ OpenSearch consulting views created'

-- =====================================================
-- Step 7: Consolidated Consulting Pipeline View
-- =====================================================

\echo 'Step 7: Creating consolidated consulting pipeline view...'

-- View: All consulting opportunities across technologies
CREATE OR REPLACE VIEW consulting_analysis.all_consulting_opportunities AS
SELECT * FROM (
  -- PostgreSQL query optimization
  SELECT
    company_name,
    company_id,
    run_id,
    run_timestamp,
    'postgres' as technology,
    engagement_type,
    consulting_priority,
    estimated_value,
    slow_query_count::text || ' slow queries, ' ||
    missing_index_count::text || ' missing indexes' as details,
    CASE consulting_priority
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      ELSE 4
    END as priority_rank
  FROM consulting_analysis.postgres_query_optimization_opps

  UNION ALL

  -- PostgreSQL vacuum tuning
  SELECT
    company_name,
    company_id,
    run_id,
    run_timestamp,
    'postgres' as technology,
    engagement_type,
    consulting_priority,
    estimated_value,
    bloated_table_count::text || ' bloated tables, ' ||
    COALESCE(total_wasted_gb::text, '0') || ' GB wasted' as details,
    CASE consulting_priority
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      ELSE 4
    END as priority_rank
  FROM consulting_analysis.postgres_vacuum_tuning_opps

  UNION ALL

  -- PostgreSQL performance tuning
  SELECT
    company_name,
    company_id,
    run_id,
    run_timestamp,
    'postgres' as technology,
    engagement_type,
    consulting_priority,
    estimated_value,
    config_recommendation_count::text || ' config recommendations' as details,
    CASE consulting_priority
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      ELSE 4
    END as priority_rank
  FROM consulting_analysis.postgres_performance_tuning_opps

  UNION ALL

  -- Kafka performance tuning
  SELECT
    company_name,
    company_id,
    run_id,
    run_timestamp,
    'kafka' as technology,
    engagement_type,
    consulting_priority,
    estimated_value,
    under_replicated_partitions::text || ' under-replicated, ' ||
    topic_config_issue_count::text || ' topic issues' as details,
    CASE consulting_priority
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      ELSE 4
    END as priority_rank
  FROM consulting_analysis.kafka_performance_tuning_opps

  UNION ALL

  -- Cassandra performance tuning
  SELECT
    company_name,
    company_id,
    run_id,
    run_timestamp,
    'cassandra' as technology,
    engagement_type,
    consulting_priority,
    estimated_value,
    'Read p99: ' || COALESCE(read_p99_ms::text, 'N/A') || 'ms, ' ||
    compaction_issue_count::text || ' compaction issues' as details,
    CASE consulting_priority
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      ELSE 4
    END as priority_rank
  FROM consulting_analysis.cassandra_performance_tuning_opps

  UNION ALL

  -- OpenSearch performance tuning
  SELECT
    company_name,
    company_id,
    run_id,
    run_timestamp,
    'opensearch' as technology,
    engagement_type,
    consulting_priority,
    estimated_value,
    unassigned_shards::text || ' unassigned shards, ' ||
    slow_query_count::text || ' slow queries' as details,
    CASE consulting_priority
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      ELSE 4
    END as priority_rank
  FROM consulting_analysis.opensearch_performance_tuning_opps
) all_opps
ORDER BY priority_rank, run_timestamp DESC;

COMMENT ON VIEW consulting_analysis.all_consulting_opportunities IS
'Consolidated view of all consulting engagement opportunities across all database technologies.
Groups PostgreSQL, Kafka, Cassandra, and OpenSearch consulting needs in one place.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

\echo '✓ Consolidated consulting pipeline view created'

-- =====================================================
-- Step 8: Grant Privileges
-- =====================================================

\echo 'Step 8: Granting privileges...'

-- Grant usage on consulting_analysis schema
GRANT USAGE ON SCHEMA consulting_analysis TO trends_app;

-- Grant SELECT on all views to trends_app
GRANT SELECT ON ALL TABLES IN SCHEMA consulting_analysis TO trends_app;

\echo '✓ Privileges granted'

\echo ''
\echo '=========================================='
\echo 'Consulting Analysis Views Created Successfully!'
\echo '=========================================='
\echo ''
\echo 'Available Views:'
\echo '  - consulting_analysis.postgres_query_optimization_opps'
\echo '  - consulting_analysis.postgres_vacuum_tuning_opps'
\echo '  - consulting_analysis.postgres_performance_tuning_opps'
\echo '  - consulting_analysis.kafka_performance_tuning_opps'
\echo '  - consulting_analysis.cassandra_performance_tuning_opps'
\echo '  - consulting_analysis.opensearch_performance_tuning_opps'
\echo '  - consulting_analysis.all_consulting_opportunities'
\echo ''
\echo 'Required Privilege: ViewConsultingOpportunities or ViewAllTechnologies'
\echo ''

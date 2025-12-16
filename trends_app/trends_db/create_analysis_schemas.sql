-- =====================================================
-- Database Health Intelligence Platform
-- Analysis Schemas with Privilege-Based Access Control
-- =====================================================
--
-- Purpose: Create technology-specific analysis schemas with views
--          that use checkprivilege() for access control
--
-- Schemas Created:
--   - postgres_analysis: PostgreSQL-specific analysis
--   - kafka_analysis: Kafka-specific analysis
--   - cassandra_analysis: Cassandra-specific analysis
--   - executive_analysis: Cross-technology business intelligence
--
-- Usage:
--   psql -h <host> -p <port> -U <user> -d health_trends -f create_analysis_schemas.sql
--
-- Author: Perry Clark
-- Date: 2025-11-10
-- =====================================================

\echo 'Creating analysis schemas and privileges...'

-- =====================================================
-- Step 1: Create Privileges for Analysis Access
-- Using createpriv() function for idempotent privilege creation
-- =====================================================

\echo 'Step 1: Creating privileges...'

-- PostgreSQL analysis privileges
SELECT createpriv('TRENDS', 'ViewPostgreSQLAnalysis', 'Can view PostgreSQL analysis views and reports');
SELECT createpriv('TRENDS', 'ViewPostgreSQLMigrationOpps', 'Can view PostgreSQL Kafka/Cassandra migration opportunities');

-- Kafka analysis privileges
SELECT createpriv('TRENDS', 'ViewKafkaAnalysis', 'Can view Kafka analysis views and reports');
SELECT createpriv('TRENDS', 'ViewKafkaHealthTrends', 'Can view Kafka cluster health trends');

-- Cassandra analysis privileges
SELECT createpriv('TRENDS', 'ViewCassandraAnalysis', 'Can view Cassandra analysis views and reports');
SELECT createpriv('TRENDS', 'ViewCassandraHealthTrends', 'Can view Cassandra cluster health trends');

-- OpenSearch analysis privileges
SELECT createpriv('TRENDS', 'ViewOpenSearchAnalysis', 'Can view OpenSearch analysis views and reports');
SELECT createpriv('TRENDS', 'ViewOpenSearchHealthTrends', 'Can view OpenSearch cluster health trends');

-- Cross-technology privileges
SELECT createpriv('TRENDS', 'ViewExecutiveAnalysis', 'Can view executive cross-technology analysis and dashboards');
SELECT createpriv('TRENDS', 'ViewAllTechnologies', 'Can view analysis for all database technologies');
SELECT createpriv('TRENDS', 'ViewMigrationPipeline', 'Can view technology migration pipeline and revenue opportunities');
SELECT createpriv('TRENDS', 'ViewCustomerComparisons', 'Can view cross-customer comparison analytics');

\echo '✓ Privileges created'

-- =====================================================
-- Step 2: Create Analysis Schemas
-- =====================================================

\echo 'Step 2: Creating schemas...'

CREATE SCHEMA IF NOT EXISTS postgres_analysis;
CREATE SCHEMA IF NOT EXISTS kafka_analysis;
CREATE SCHEMA IF NOT EXISTS cassandra_analysis;
CREATE SCHEMA IF NOT EXISTS opensearch_analysis;
CREATE SCHEMA IF NOT EXISTS executive_analysis;

COMMENT ON SCHEMA postgres_analysis IS 'PostgreSQL-specific analysis views and functions';
COMMENT ON SCHEMA kafka_analysis IS 'Kafka-specific analysis views and functions';
COMMENT ON SCHEMA cassandra_analysis IS 'Cassandra-specific analysis views and functions';
COMMENT ON SCHEMA opensearch_analysis IS 'OpenSearch-specific analysis views and functions';
COMMENT ON SCHEMA executive_analysis IS 'Cross-technology executive analysis and business intelligence';

\echo '✓ Schemas created'

-- =====================================================
-- Step 3: Create PostgreSQL Analysis Views
-- =====================================================

\echo 'Step 3: Creating PostgreSQL analysis views...'

-- View: High-frequency write candidates for Kafka/Cassandra migration
CREATE OR REPLACE VIEW postgres_analysis.high_frequency_write_candidates AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  query_data->>'username' as db_user,
  (query_data->>'calls_per_hour')::numeric as calls_per_hour,
  (query_data->>'total_executions')::numeric as total_executions,
  (query_data->>'percent_of_total_cluster_cpu')::numeric as cpu_percent,
  (query_data->>'cache_hit_rate_percent')::numeric as cache_hit_rate,
  substring(query_data->>'query' from 1 for 80) as query_preview,
  query_data->>'query' as full_query,
  CASE
    WHEN (query_data->>'calls_per_hour')::numeric > 500000 THEN 'critical'
    WHEN (query_data->>'calls_per_hour')::numeric > 200000 THEN 'high'
    WHEN (query_data->>'calls_per_hour')::numeric > 100000 THEN 'medium'
    ELSE 'low'
  END as migration_priority,
  CASE
    WHEN (query_data->>'calls_per_hour')::numeric > 200000
    THEN '🚨 Strongly consider Kafka/Cassandra'
    WHEN (query_data->>'calls_per_hour')::numeric > 100000
    THEN '⚠️  Consider Kafka migration'
    ELSE '✅ PostgreSQL appropriate'
  END as recommendation
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
CROSS JOIN LATERAL jsonb_array_elements(
  CASE
    WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
    ELSE hcr.findings::jsonb
  END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
) AS query_data
WHERE
  -- Privilege check: user must have ViewPostgreSQLAnalysis or ViewAllTechnologies
  (checkprivilege('ViewPostgreSQLAnalysis') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'postgres'
  AND (
    query_data->>'query' ILIKE '%INSERT INTO%' OR
    query_data->>'query' ILIKE '%UPDATE %' OR
    query_data->>'query' ILIKE '%DELETE FROM%'
  )
  AND (query_data->>'calls_per_hour')::numeric > 50000
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days';

COMMENT ON VIEW postgres_analysis.high_frequency_write_candidates IS
'Identifies PostgreSQL workloads with high-frequency writes (>50K/hour) that may benefit from Kafka/Cassandra migration.
Requires ViewPostgreSQLAnalysis or ViewAllTechnologies privilege.';

-- View: Write volume growth trends per customer
CREATE OR REPLACE VIEW postgres_analysis.write_volume_growth_trends AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.run_timestamp::date as check_date,
  SUM((query_data->>'calls_per_hour')::numeric) as total_writes_per_hour,
  COUNT(DISTINCT query_data->>'query') as unique_write_queries,
  AVG((query_data->>'percent_of_total_cluster_cpu')::numeric) as avg_cpu_percent,
  MAX((query_data->>'calls_per_hour')::numeric) as max_single_query_rate
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
CROSS JOIN LATERAL jsonb_array_elements(
  CASE
    WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
    ELSE hcr.findings::jsonb
  END -> 'check_comprehensive_query_analysis' -> 'data' -> 'queries'
) AS query_data
WHERE
  (checkprivilege('ViewPostgreSQLAnalysis') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'postgres'
  AND (
    query_data->>'query' ILIKE '%INSERT INTO%' OR
    query_data->>'query' ILIKE '%UPDATE %' OR
    query_data->>'query' ILIKE '%DELETE FROM%'
  )
GROUP BY co.company_name, co.id, check_date
ORDER BY check_date DESC, total_writes_per_hour DESC;

COMMENT ON VIEW postgres_analysis.write_volume_growth_trends IS
'Daily aggregated write volume per customer. Use to identify growth trends and project capacity limits.
Requires ViewPostgreSQLAnalysis or ViewAllTechnologies privilege.';

\echo '✓ PostgreSQL analysis views created'

-- =====================================================
-- Step 4: Create Kafka Analysis Views
-- =====================================================

\echo 'Step 4: Creating Kafka analysis views...'

-- View: Kafka capacity upgrade candidates
CREATE OR REPLACE VIEW kafka_analysis.capacity_upgrade_candidates AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Extract CPU utilization from broker metrics
  (
    SELECT AVG((broker->>'cpu_percent')::numeric)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'prometheus_broker_health' -> 'data' -> 'brokers'
    ) AS broker
    WHERE broker->>'cpu_percent' IS NOT NULL
  ) as avg_cpu_percent,
  -- Extract disk utilization
  (
    SELECT AVG((broker->>'disk_used_percent')::numeric)
    FROM jsonb_array_elements(
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'kafka_overview' -> 'data' -> 'brokers'
    ) AS broker
    WHERE broker->>'disk_used_percent' IS NOT NULL
  ) as avg_disk_percent,
  -- Determine upgrade priority
  CASE
    WHEN (
      SELECT AVG((broker->>'cpu_percent')::numeric)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'prometheus_broker_health' -> 'data' -> 'brokers'
      ) AS broker
    ) > 80 THEN 'critical'
    WHEN (
      SELECT AVG((broker->>'cpu_percent')::numeric)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'prometheus_broker_health' -> 'data' -> 'brokers'
      ) AS broker
    ) > 70 THEN 'high'
    WHEN (
      SELECT AVG((broker->>'cpu_percent')::numeric)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'prometheus_broker_health' -> 'data' -> 'brokers'
      ) AS broker
    ) > 60 THEN 'medium'
    ELSE 'low'
  END as upgrade_priority,
  -- Recommendation
  CASE
    WHEN (
      SELECT AVG((broker->>'cpu_percent')::numeric)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'prometheus_broker_health' -> 'data' -> 'brokers'
      ) AS broker
    ) > 80 THEN '🚨 Urgent: Upgrade to larger instance types or add brokers'
    WHEN (
      SELECT AVG((broker->>'cpu_percent')::numeric)
      FROM jsonb_array_elements(
        CASE
          WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
          ELSE hcr.findings::jsonb
        END -> 'prometheus_broker_health' -> 'data' -> 'brokers'
      ) AS broker
    ) > 70 THEN '⚠️  Consider capacity upgrade soon'
    ELSE '✅ Capacity adequate'
  END as recommendation
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewKafkaAnalysis') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'kafka'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'prometheus_broker_health' -> 'data' -> 'brokers'
  ) IS NOT NULL;

COMMENT ON VIEW kafka_analysis.capacity_upgrade_candidates IS
'Identifies Kafka clusters with high CPU/disk utilization that may benefit from capacity upgrades.
Requires ViewKafkaAnalysis or ViewAllTechnologies privilege.';

\echo '✓ Kafka analysis views created'

-- =====================================================
-- Step 5: Create Cassandra Analysis Views
-- =====================================================

\echo 'Step 5: Creating Cassandra analysis views...'

-- View: Cassandra performance upgrade candidates
CREATE OR REPLACE VIEW cassandra_analysis.performance_upgrade_candidates AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Extract read latency (p99)
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
  )::numeric as read_p99_ms,
  -- Extract write latency (p99)
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_read_write_latency' -> 'data' -> 'write_p99_ms'
  )::numeric as write_p99_ms,
  -- Extract heap usage
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_memory_pressure' -> 'data' -> 'heap_used_percent'
  )::numeric as heap_used_percent,
  -- Determine upgrade priority
  CASE
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
    )::numeric > 100 THEN 'critical'
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
    )::numeric > 50 THEN 'high'
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
    )::numeric > 25 THEN 'medium'
    ELSE 'low'
  END as upgrade_priority,
  -- Recommendation
  CASE
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
    )::numeric > 100 THEN '🚨 Urgent: Upgrade to faster storage (NVMe) or add nodes'
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_read_write_latency' -> 'data' -> 'read_p99_ms'
    )::numeric > 50 THEN '⚠️  Consider performance upgrade or horizontal scaling'
    ELSE '✅ Performance adequate'
  END as recommendation
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewCassandraAnalysis') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'cassandra'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_read_write_latency' -> 'data'
  ) IS NOT NULL;

COMMENT ON VIEW cassandra_analysis.performance_upgrade_candidates IS
'Identifies Cassandra clusters with high latency that may benefit from performance upgrades.
Requires ViewCassandraAnalysis or ViewAllTechnologies privilege.';

\echo '✓ Cassandra analysis views created'

-- =====================================================
-- Step 6: Create OpenSearch Analysis Views
-- =====================================================

\echo 'Step 6: Creating OpenSearch analysis views...'

-- View: OpenSearch capacity upgrade candidates
CREATE OR REPLACE VIEW opensearch_analysis.capacity_upgrade_candidates AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.id as run_id,
  hcr.run_timestamp,
  hcr.target_host,
  hcr.target_port,
  -- Extract JVM heap usage
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_jvm_memory' -> 'data' -> 'heap_used_percent'
  )::numeric as heap_used_percent,
  -- Extract disk usage
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_disk_usage' -> 'data' -> 'disk_used_percent'
  )::numeric as disk_used_percent,
  -- Extract shard count
  (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_shard_allocation' -> 'data' -> 'total_shards'
  )::integer as total_shards,
  -- Determine upgrade priority
  CASE
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_jvm_memory' -> 'data' -> 'heap_used_percent'
    )::numeric > 85 THEN 'critical'
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_jvm_memory' -> 'data' -> 'heap_used_percent'
    )::numeric > 75 THEN 'high'
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_jvm_memory' -> 'data' -> 'heap_used_percent'
    )::numeric > 65 THEN 'medium'
    ELSE 'low'
  END as upgrade_priority,
  -- Recommendation
  CASE
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_jvm_memory' -> 'data' -> 'heap_used_percent'
    )::numeric > 85 THEN '🚨 Urgent: Increase heap size or add data nodes'
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_jvm_memory' -> 'data' -> 'heap_used_percent'
    )::numeric > 75 THEN '⚠️  Consider memory upgrade or horizontal scaling'
    WHEN (
      CASE
        WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
        ELSE hcr.findings::jsonb
      END -> 'check_disk_usage' -> 'data' -> 'disk_used_percent'
    )::numeric > 80 THEN '⚠️  Disk capacity approaching limits - add storage or nodes'
    ELSE '✅ Capacity adequate'
  END as recommendation
FROM public.health_check_runs hcr
JOIN public.companies co ON (co.id = hcr.company_id)
WHERE
  (checkprivilege('ViewOpenSearchAnalysis') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.db_technology = 'opensearch'
  AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
  AND (
    CASE
      WHEN hcr.encryption_mode = 'pgcrypto' THEN public.decrypt_run_findings(hcr.id)
      ELSE hcr.findings::jsonb
    END -> 'check_jvm_memory' -> 'data'
  ) IS NOT NULL;

COMMENT ON VIEW opensearch_analysis.capacity_upgrade_candidates IS
'Identifies OpenSearch clusters with high heap/disk utilization that may benefit from capacity upgrades.
Requires ViewOpenSearchAnalysis or ViewAllTechnologies privilege.';

\echo '✓ OpenSearch analysis views created'

-- =====================================================
-- Step 7: Create Executive Analysis Views
-- =====================================================

\echo 'Step 7: Creating executive analysis views...'

-- View: Technology migration pipeline (sales/revenue opportunities)
CREATE OR REPLACE VIEW executive_analysis.technology_migration_pipeline AS
SELECT
  company_name,
  company_id,
  'postgres' as current_technology,
  'kafka' as recommended_technology,
  calls_per_hour as workload_metric,
  migration_priority,
  CASE migration_priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    ELSE 4
  END as priority_rank,
  -- Revenue estimates
  '$2,000/month' as kafka_cluster_cost,
  '$500-1,000/month' as postgres_savings,
  '$1,500-2,000/month' as net_new_revenue,
  -- Technical details
  cpu_percent as current_cpu_impact,
  total_executions,
  query_preview,
  recommendation
FROM postgres_analysis.high_frequency_write_candidates
WHERE
  (checkprivilege('ViewMigrationPipeline') OR checkprivilege('ViewAllTechnologies'))
  AND migration_priority IN ('high', 'critical')
ORDER BY priority_rank, workload_metric DESC;

COMMENT ON VIEW executive_analysis.technology_migration_pipeline IS
'Sales pipeline view showing Kafka/Cassandra upsell opportunities with estimated revenue.
Requires ViewMigrationPipeline or ViewAllTechnologies privilege.';

-- View: Customer technology footprint
CREATE OR REPLACE VIEW executive_analysis.customer_technology_footprint AS
SELECT
  co.company_name,
  co.id as company_id,
  hcr.db_technology,
  COUNT(DISTINCT hcr.id) as health_check_count,
  MAX(hcr.run_timestamp) as last_check,
  -- Check if customer has migration opportunities
  EXISTS(
    SELECT 1 FROM postgres_analysis.high_frequency_write_candidates hwc
    WHERE hwc.company_id = co.id
      AND hwc.migration_priority IN ('high', 'critical')
  ) as has_migration_opportunity
FROM public.companies co
JOIN public.health_check_runs hcr ON (hcr.company_id = co.id)
WHERE
  (checkprivilege('ViewExecutiveAnalysis') OR checkprivilege('ViewAllTechnologies'))
  AND hcr.run_timestamp > NOW() - INTERVAL '90 days'
GROUP BY co.company_name, co.id, hcr.db_technology
ORDER BY company_name, db_technology;

COMMENT ON VIEW executive_analysis.customer_technology_footprint IS
'Shows which technologies each customer uses and identifies upsell opportunities.
Requires ViewExecutiveAnalysis or ViewAllTechnologies privilege.';

\echo '✓ Executive analysis views created'

-- =====================================================
-- Step 5: Helper Functions
-- =====================================================

\echo 'Step 5: Creating helper functions...'

-- Function: Check if current user can access a specific analysis schema
CREATE OR REPLACE FUNCTION public.can_access_analysis_schema(schema_name TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  CASE schema_name
    WHEN 'postgres_analysis' THEN
      RETURN checkprivilege('ViewPostgreSQLAnalysis') OR checkprivilege('ViewAllTechnologies');
    WHEN 'kafka_analysis' THEN
      RETURN checkprivilege('ViewKafkaAnalysis') OR checkprivilege('ViewAllTechnologies');
    WHEN 'cassandra_analysis' THEN
      RETURN checkprivilege('ViewCassandraAnalysis') OR checkprivilege('ViewAllTechnologies');
    WHEN 'opensearch_analysis' THEN
      RETURN checkprivilege('ViewOpenSearchAnalysis') OR checkprivilege('ViewAllTechnologies');
    WHEN 'executive_analysis' THEN
      RETURN checkprivilege('ViewExecutiveAnalysis') OR checkprivilege('ViewAllTechnologies');
    ELSE
      RETURN FALSE;
  END CASE;
END;
$$;

COMMENT ON FUNCTION public.can_access_analysis_schema(TEXT) IS
'Helper function to check if current user can access a specific analysis schema.
Used by Flask app to determine which dashboards to show.';

-- Function: Get list of accessible analysis schemas for current user
CREATE OR REPLACE FUNCTION public.get_accessible_analysis_schemas()
RETURNS TABLE(schema_name TEXT, display_name TEXT)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY
  SELECT s.schema_name, s.display_name FROM (
    VALUES
      ('postgres_analysis'::text, 'PostgreSQL Analysis'::text),
      ('kafka_analysis'::text, 'Kafka Analysis'::text),
      ('cassandra_analysis'::text, 'Cassandra Analysis'::text),
      ('opensearch_analysis'::text, 'OpenSearch Analysis'::text),
      ('executive_analysis'::text, 'Executive Dashboard'::text)
  ) AS s(schema_name, display_name)
  WHERE can_access_analysis_schema(s.schema_name);
END;
$$;

COMMENT ON FUNCTION public.get_accessible_analysis_schemas() IS
'Returns list of analysis schemas the current user has access to.
Used to populate navigation menus in Flask app.';

\echo '✓ Helper functions created'

-- =====================================================
-- Step 6: Grant Basic Access to Schemas
-- (Views handle privilege checks internally)
-- =====================================================

\echo 'Step 6: Granting schema access...'

GRANT USAGE ON SCHEMA postgres_analysis TO PUBLIC;
GRANT USAGE ON SCHEMA kafka_analysis TO PUBLIC;
GRANT USAGE ON SCHEMA cassandra_analysis TO PUBLIC;
GRANT USAGE ON SCHEMA opensearch_analysis TO PUBLIC;
GRANT USAGE ON SCHEMA executive_analysis TO PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA postgres_analysis TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA kafka_analysis TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA cassandra_analysis TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA opensearch_analysis TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA executive_analysis TO PUBLIC;

-- Allow public to execute helper functions
GRANT EXECUTE ON FUNCTION public.can_access_analysis_schema(TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_accessible_analysis_schemas() TO PUBLIC;

\echo '✓ Schema access granted'

-- =====================================================
-- Verification Queries
-- =====================================================

\echo ''
\echo '======================='
\echo 'Installation Complete!'
\echo '======================='
\echo ''
\echo 'Verification:'
\echo ''

\echo 'TRENDS module privileges:'
SELECT priv_name, priv_descrip
FROM priv
WHERE priv_module = 'TRENDS'
ORDER BY priv_name;

\echo ''
\echo 'Schemas accessible to current user:'
SELECT * FROM get_accessible_analysis_schemas();

\echo ''
\echo 'Sample query - High-frequency write candidates (limited to 5):'
SELECT company_name, calls_per_hour, migration_priority, recommendation
FROM postgres_analysis.high_frequency_write_candidates
ORDER BY calls_per_hour DESC
LIMIT 5;

\echo ''
\echo 'Next Steps:'
\echo '1. Assign privileges to users/groups using grppriv or usrpriv tables'
\echo '2. Test access by connecting as different users'
\echo '3. Integrate with Flask app using database.py helper functions'
\echo '4. Add Kafka and Cassandra analysis views as needed'
\echo ''

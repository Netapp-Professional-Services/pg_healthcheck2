-- =====================================================
-- Database Health Intelligence Platform
-- Consulting Engagement Opportunity Analysis (Triggered Rules Approach)
-- =====================================================
--
-- Purpose: Identify professional services and consulting opportunities
--          based on TRIGGERED RULES (high/critical alerts)
--
-- Strategy: Use health_check_triggered_rules table as primary data source
--          - Proven issues that have already crossed severity thresholds
--          - Clear evidence of need for consulting engagement
--          - Customer is already aware of the problem
--
-- Consulting Opportunity Types:
--   - Query Optimization: Slow queries, missing indexes, seq scans
--   - Vacuum Tuning: Bloat detection, autovacuum configuration issues
--   - Performance Tuning: Configuration optimization, resource allocation
--   - Schema Design: Table structure issues, partitioning opportunities
--   - High Availability: Replication lag, failover readiness
--   - Capacity Planning: Growth trends, scaling recommendations
--
-- Usage:
--   psql -h <host> -p <port> -U <user> -d health_trends -f create_consulting_analysis_triggered_rules.sql
--
-- Author: Perry Clark
-- Date: 2025-11-10
-- =====================================================

\echo 'Creating consulting engagement analysis views (triggered rules approach)...'

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
-- Step 3: Helper View - Categorize Rules by Consulting Type
-- =====================================================

\echo 'Step 3: Creating helper view for rule categorization...'

-- Map rule patterns to consulting engagement types
CREATE OR REPLACE VIEW consulting_analysis.rule_to_consulting_type AS
SELECT
  'query_optimization' as consulting_type,
  '🔧 Query Optimization Consulting' as engagement_name,
  '$8,000-$20,000' as typical_engagement_value,
  ARRAY[
    'high_cpu_query',
    'slow_query',
    'missing_index',
    'sequential_scan',
    'low_cache_hit',
    'inefficient_query'
  ] as rule_patterns

UNION ALL

SELECT
  'vacuum_tuning' as consulting_type,
  '🔧 Vacuum & Bloat Tuning' as engagement_name,
  '$8,000-$15,000' as typical_engagement_value,
  ARRAY[
    'table_bloat',
    'index_bloat',
    'autovacuum',
    'vacuum_tuning',
    'bloat_detected'
  ] as rule_patterns

UNION ALL

SELECT
  'performance_tuning' as consulting_type,
  '🔧 Performance Configuration Tuning' as engagement_name,
  '$10,000-$18,000' as typical_engagement_value,
  ARRAY[
    'config_',
    'checkpoint',
    'wal_',
    'connection_pool',
    'replication_lag',
    'gc_pause',
    'jvm_heap'
  ] as rule_patterns

UNION ALL

SELECT
  'capacity_planning' as consulting_type,
  '🔧 Capacity Planning & Scaling Strategy' as engagement_name,
  '$12,000-$25,000' as typical_engagement_value,
  ARRAY[
    'high_cpu',
    'high_memory',
    'disk_space',
    'capacity',
    'growth_',
    'scaling'
  ] as rule_patterns

UNION ALL

SELECT
  'high_availability' as consulting_type,
  '🔧 High Availability & Disaster Recovery' as engagement_name,
  '$15,000-$30,000' as typical_engagement_value,
  ARRAY[
    'replication',
    'failover',
    'patroni',
    'under_replicated',
    'replica_lag',
    'backup'
  ] as rule_patterns

UNION ALL

SELECT
  'schema_design' as consulting_type,
  '🔧 Schema Design & Data Modeling' as engagement_name,
  '$10,000-$20,000' as typical_engagement_value,
  ARRAY[
    'partition',
    'table_design',
    'schema_',
    'compaction_strategy'
  ] as rule_patterns

UNION ALL

SELECT
  'cluster_optimization' as consulting_type,
  '🔧 Cluster Optimization & Tuning' as engagement_name,
  '$12,000-$22,000' as typical_engagement_value,
  ARRAY[
    'partition_balance',
    'shard_allocation',
    'topic_config',
    'broker_',
    'node_'
  ] as rule_patterns;

COMMENT ON VIEW consulting_analysis.rule_to_consulting_type IS
'Maps rule name patterns to consulting engagement types.
Used to categorize triggered rules into consulting opportunities.';

\echo '✓ Helper view created'

-- =====================================================
-- Step 4: Consulting Opportunities from Triggered Rules
-- =====================================================

\echo 'Step 4: Creating main consulting opportunities view...'

-- View: Consulting opportunities based on triggered rules
CREATE OR REPLACE VIEW consulting_analysis.consulting_opportunities_from_rules AS
WITH rule_consulting_mapping AS (
  -- Join triggered rules with consulting type mapping
  SELECT
    tr.run_id,
    tr.rule_config_name,
    tr.severity_level,
    tr.reasoning,
    tr.recommendations,
    tr.triggered_data,
    hcr.company_id,
    co.company_name,
    hcr.db_technology,
    hcr.target_host,
    hcr.target_port,
    hcr.run_timestamp,
    rct.consulting_type,
    rct.engagement_name,
    rct.typical_engagement_value
  FROM public.health_check_triggered_rules tr
  JOIN public.health_check_runs hcr ON (hcr.id = tr.run_id)
  JOIN public.companies co ON (co.id = hcr.company_id)
  CROSS JOIN consulting_analysis.rule_to_consulting_type rct
  WHERE
    -- Only high/critical rules justify consulting engagement
    tr.severity_level IN ('high', 'critical')
    -- Match rule name to consulting type patterns
    AND EXISTS (
      SELECT 1 FROM unnest(rct.rule_patterns) AS pattern
      WHERE tr.rule_config_name ILIKE '%' || pattern || '%'
    )
    -- Recent runs only (last 30 days)
    AND hcr.run_timestamp > NOW() - INTERVAL '30 days'
    -- Privilege check
    AND (checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies'))
),
aggregated_by_company_type AS (
  -- Aggregate rules by company and consulting type
  SELECT
    company_id,
    company_name,
    db_technology,
    consulting_type,
    engagement_name,
    typical_engagement_value,
    -- Count of triggered rules in this category
    COUNT(*) as triggered_rule_count,
    -- Count of critical vs high
    COUNT(*) FILTER (WHERE severity_level = 'critical') as critical_count,
    COUNT(*) FILTER (WHERE severity_level = 'high') as high_count,
    -- Latest run details
    MAX(run_timestamp) as latest_run_timestamp,
    MAX(run_id) as latest_run_id,
    MAX(target_host) as target_host,
    MAX(target_port) as target_port,
    -- Collect rule names
    array_agg(DISTINCT rule_config_name ORDER BY rule_config_name) as related_rules,
    -- Collect sample reasoning
    array_agg(DISTINCT reasoning ORDER BY reasoning) FILTER (WHERE reasoning IS NOT NULL) as sample_reasoning
  FROM rule_consulting_mapping
  GROUP BY
    company_id,
    company_name,
    db_technology,
    consulting_type,
    engagement_name,
    typical_engagement_value
)
SELECT
  company_name,
  company_id,
  latest_run_id as run_id,
  latest_run_timestamp as run_timestamp,
  target_host,
  target_port,
  db_technology,
  consulting_type,
  engagement_name,
  triggered_rule_count,
  critical_count,
  high_count,
  -- Determine consulting priority based on severity mix
  CASE
    WHEN critical_count >= 3 THEN 'critical'
    WHEN critical_count >= 1 THEN 'high'
    WHEN high_count >= 3 THEN 'medium'
    ELSE 'low'
  END as consulting_priority,
  typical_engagement_value as estimated_value,
  -- Details string
  triggered_rule_count::text || ' triggered rules (' ||
  critical_count::text || ' critical, ' ||
  high_count::text || ' high)' as issue_summary,
  related_rules,
  sample_reasoning
FROM aggregated_by_company_type
ORDER BY
  CASE
    WHEN critical_count >= 3 THEN 1
    WHEN critical_count >= 1 THEN 2
    WHEN high_count >= 3 THEN 3
    ELSE 4
  END,
  triggered_rule_count DESC,
  latest_run_timestamp DESC;

COMMENT ON VIEW consulting_analysis.consulting_opportunities_from_rules IS
'Identifies consulting engagement opportunities based on triggered rules (high/critical severity).
Groups triggered rules by company and consulting type (query optimization, vacuum tuning, etc.).
Provides clear evidence of need based on health check alerts.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

\echo '✓ Main consulting opportunities view created'

-- =====================================================
-- Step 5: Technology-Specific Consulting Views
-- =====================================================

\echo 'Step 5: Creating technology-specific consulting views...'

-- PostgreSQL consulting opportunities
CREATE OR REPLACE VIEW consulting_analysis.postgres_consulting_opportunities AS
SELECT
  company_name,
  company_id,
  run_id,
  run_timestamp,
  target_host,
  target_port,
  consulting_type,
  engagement_name,
  triggered_rule_count,
  critical_count,
  high_count,
  consulting_priority,
  estimated_value,
  issue_summary,
  related_rules,
  sample_reasoning
FROM consulting_analysis.consulting_opportunities_from_rules
WHERE
  db_technology = 'postgres'
  AND (checkprivilege('ViewPostgreSQLAnalysis') OR checkprivilege('ViewAllTechnologies'))
ORDER BY
  CASE consulting_priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    ELSE 4
  END,
  triggered_rule_count DESC;

COMMENT ON VIEW consulting_analysis.postgres_consulting_opportunities IS
'PostgreSQL-specific consulting opportunities from triggered rules.';

-- Kafka consulting opportunities
CREATE OR REPLACE VIEW consulting_analysis.kafka_consulting_opportunities AS
SELECT
  company_name,
  company_id,
  run_id,
  run_timestamp,
  target_host,
  target_port,
  consulting_type,
  engagement_name,
  triggered_rule_count,
  critical_count,
  high_count,
  consulting_priority,
  estimated_value,
  issue_summary,
  related_rules,
  sample_reasoning
FROM consulting_analysis.consulting_opportunities_from_rules
WHERE
  db_technology = 'kafka'
  AND (checkprivilege('ViewKafkaAnalysis') OR checkprivilege('ViewAllTechnologies'))
ORDER BY
  CASE consulting_priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    ELSE 4
  END,
  triggered_rule_count DESC;

COMMENT ON VIEW consulting_analysis.kafka_consulting_opportunities IS
'Kafka-specific consulting opportunities from triggered rules.';

-- Cassandra consulting opportunities
CREATE OR REPLACE VIEW consulting_analysis.cassandra_consulting_opportunities AS
SELECT
  company_name,
  company_id,
  run_id,
  run_timestamp,
  target_host,
  target_port,
  consulting_type,
  engagement_name,
  triggered_rule_count,
  critical_count,
  high_count,
  consulting_priority,
  estimated_value,
  issue_summary,
  related_rules,
  sample_reasoning
FROM consulting_analysis.consulting_opportunities_from_rules
WHERE
  db_technology = 'cassandra'
  AND (checkprivilege('ViewCassandraAnalysis') OR checkprivilege('ViewAllTechnologies'))
ORDER BY
  CASE consulting_priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    ELSE 4
  END,
  triggered_rule_count DESC;

COMMENT ON VIEW consulting_analysis.cassandra_consulting_opportunities IS
'Cassandra-specific consulting opportunities from triggered rules.';

-- OpenSearch consulting opportunities
CREATE OR REPLACE VIEW consulting_analysis.opensearch_consulting_opportunities AS
SELECT
  company_name,
  company_id,
  run_id,
  run_timestamp,
  target_host,
  target_port,
  consulting_type,
  engagement_name,
  triggered_rule_count,
  critical_count,
  high_count,
  consulting_priority,
  estimated_value,
  issue_summary,
  related_rules,
  sample_reasoning
FROM consulting_analysis.consulting_opportunities_from_rules
WHERE
  db_technology = 'opensearch'
  AND (checkprivilege('ViewOpenSearchAnalysis') OR checkprivilege('ViewAllTechnologies'))
ORDER BY
  CASE consulting_priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    ELSE 4
  END,
  triggered_rule_count DESC;

COMMENT ON VIEW consulting_analysis.opensearch_consulting_opportunities IS
'OpenSearch-specific consulting opportunities from triggered rules.';

\echo '✓ Technology-specific consulting views created'

-- =====================================================
-- Step 6: Executive Summary View
-- =====================================================

\echo 'Step 6: Creating executive summary view...'

-- Executive summary of consulting opportunities
CREATE OR REPLACE VIEW consulting_analysis.executive_consulting_summary AS
SELECT
  consulting_type,
  MAX(engagement_name) as engagement_name,
  COUNT(DISTINCT company_id) as company_count,
  COUNT(*) as total_opportunities,
  SUM(triggered_rule_count) as total_triggered_rules,
  SUM(critical_count) as total_critical_issues,
  SUM(high_count) as total_high_issues,
  -- Count by priority
  COUNT(*) FILTER (WHERE consulting_priority = 'critical') as critical_priority_count,
  COUNT(*) FILTER (WHERE consulting_priority = 'high') as high_priority_count,
  COUNT(*) FILTER (WHERE consulting_priority = 'medium') as medium_priority_count,
  -- Breakdown by technology
  COUNT(*) FILTER (WHERE db_technology = 'postgres') as postgres_count,
  COUNT(*) FILTER (WHERE db_technology = 'kafka') as kafka_count,
  COUNT(*) FILTER (WHERE db_technology = 'cassandra') as cassandra_count,
  COUNT(*) FILTER (WHERE db_technology = 'opensearch') as opensearch_count,
  -- Estimated revenue range (simplified)
  MAX(estimated_value) as typical_engagement_value
FROM consulting_analysis.consulting_opportunities_from_rules
WHERE
  checkprivilege('ViewConsultingOpportunities') OR checkprivilege('ViewAllTechnologies')
GROUP BY consulting_type
ORDER BY
  SUM(CASE consulting_priority
    WHEN 'critical' THEN 4
    WHEN 'high' THEN 3
    WHEN 'medium' THEN 2
    ELSE 1
  END) DESC,
  company_count DESC;

COMMENT ON VIEW consulting_analysis.executive_consulting_summary IS
'Executive-level summary of consulting opportunities grouped by engagement type.
Shows company counts, issue counts, and priority breakdown.
Requires ViewConsultingOpportunities or ViewAllTechnologies privilege.';

\echo '✓ Executive summary view created'

-- =====================================================
-- Step 7: Grant Privileges
-- =====================================================

\echo 'Step 7: Granting privileges...'

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
\echo 'Main Views:'
\echo '  - consulting_analysis.consulting_opportunities_from_rules (all opportunities)'
\echo '  - consulting_analysis.executive_consulting_summary (executive overview)'
\echo ''
\echo 'Technology-Specific Views:'
\echo '  - consulting_analysis.postgres_consulting_opportunities'
\echo '  - consulting_analysis.kafka_consulting_opportunities'
\echo '  - consulting_analysis.cassandra_consulting_opportunities'
\echo '  - consulting_analysis.opensearch_consulting_opportunities'
\echo ''
\echo 'Helper Views:'
\echo '  - consulting_analysis.rule_to_consulting_type (rule categorization)'
\echo ''
\echo 'Data Source: health_check_triggered_rules (high/critical severity only)'
\echo 'Required Privilege: ViewConsultingOpportunities or ViewAllTechnologies'
\echo ''

-- Migration: Add metadata columns to health_check_runs table
-- Description: Adds pre-computed metadata columns for efficient querying and analytics
-- Date: 2025-11-05
-- Purpose: Enable version-specific analysis, infrastructure correlation, and time-based queries

-- Add metadata columns (add separately to handle potential failures gracefully)
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS db_version TEXT;
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS db_version_major INTEGER;
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS db_version_minor INTEGER;
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS cluster_name TEXT;
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS node_count INTEGER;
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS infrastructure_metadata JSONB;
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS health_score FLOAT;

-- Add run_date as a regular column with a trigger to auto-populate
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS run_date DATE;

-- Create a function to auto-populate run_date from run_timestamp
CREATE OR REPLACE FUNCTION set_run_date()
RETURNS TRIGGER AS $$
BEGIN
  NEW.run_date := DATE(NEW.run_timestamp);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create trigger to auto-populate run_date on insert/update
DROP TRIGGER IF EXISTS trigger_set_run_date ON health_check_runs;
CREATE TRIGGER trigger_set_run_date
  BEFORE INSERT OR UPDATE OF run_timestamp ON health_check_runs
  FOR EACH ROW
  EXECUTE FUNCTION set_run_date();

-- Backfill run_date for existing rows
UPDATE health_check_runs SET run_date = DATE(run_timestamp) WHERE run_date IS NULL;

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_health_check_runs_version ON health_check_runs(db_technology, db_version);
CREATE INDEX IF NOT EXISTS idx_health_check_runs_version_major ON health_check_runs(db_technology, db_version_major, db_version_minor);
CREATE INDEX IF NOT EXISTS idx_health_check_runs_cluster ON health_check_runs(cluster_name);
CREATE INDEX IF NOT EXISTS idx_health_check_runs_run_date ON health_check_runs(run_date);
CREATE INDEX IF NOT EXISTS idx_health_check_runs_company_date ON health_check_runs(company_id, run_date);
CREATE INDEX IF NOT EXISTS idx_health_check_runs_tech_date ON health_check_runs(db_technology, run_date);
CREATE INDEX IF NOT EXISTS idx_health_check_runs_health_score ON health_check_runs(health_score) WHERE health_score IS NOT NULL;

-- Add GIN index for infrastructure_metadata JSONB queries
CREATE INDEX IF NOT EXISTS idx_health_check_runs_infrastructure_gin ON health_check_runs USING GIN (infrastructure_metadata);

-- Add composite index for version-specific analysis
CREATE INDEX IF NOT EXISTS idx_health_check_runs_tech_version_date ON health_check_runs(db_technology, db_version_major, db_version_minor, run_date);

-- Add column comments
COMMENT ON COLUMN health_check_runs.db_version IS 'Full database version string (e.g., "16.3", "4.1.5", "25.1.2.15")';
COMMENT ON COLUMN health_check_runs.db_version_major IS 'Major version number for version-specific analysis (e.g., 16, 4, 25)';
COMMENT ON COLUMN health_check_runs.db_version_minor IS 'Minor version number for version-specific analysis (e.g., 3, 1, 1)';
COMMENT ON COLUMN health_check_runs.cluster_name IS 'Name of the database cluster from configuration or auto-detected';
COMMENT ON COLUMN health_check_runs.node_count IS 'Number of nodes in the cluster for infrastructure correlation';
COMMENT ON COLUMN health_check_runs.infrastructure_metadata IS 'JSONB metadata about infrastructure: cloud provider, region, instance types, storage config, etc.';
COMMENT ON COLUMN health_check_runs.health_score IS 'Pre-computed overall health score (0.0-100.0) calculated from triggered rules';
COMMENT ON COLUMN health_check_runs.run_date IS 'Date portion of run_timestamp for efficient date-based partitioning and queries';

-- Grant permissions (adjust role names as needed)
GRANT SELECT, INSERT, UPDATE ON health_check_runs TO postgres;

-- Display migration summary
DO $$
BEGIN
  RAISE NOTICE '✅ Migration 05: Added metadata columns to health_check_runs';
  RAISE NOTICE '   - db_version, db_version_major, db_version_minor';
  RAISE NOTICE '   - cluster_name, node_count';
  RAISE NOTICE '   - infrastructure_metadata (JSONB)';
  RAISE NOTICE '   - health_score (FLOAT)';
  RAISE NOTICE '   - run_date (GENERATED DATE column)';
  RAISE NOTICE '   - Created 9 new indexes for efficient querying';
END $$;

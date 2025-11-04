-- Migration: Create health_check_triggered_rules table
-- Description: Stores which analysis rules were triggered during health check execution
-- Date: 2025-10-22

CREATE TABLE IF NOT EXISTS health_check_triggered_rules (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES health_check_runs(id) ON DELETE CASCADE,
    rule_config_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    severity_level TEXT NOT NULL CHECK (severity_level IN ('critical', 'high', 'medium', 'low', 'info')),
    severity_score INTEGER NOT NULL,
    reasoning TEXT,
    recommendations JSONB,
    triggered_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

-- Indexes for common query patterns
CREATE INDEX idx_triggered_rules_run_id ON health_check_triggered_rules(run_id);
CREATE INDEX idx_triggered_rules_severity ON health_check_triggered_rules(severity_level);
CREATE INDEX idx_triggered_rules_config ON health_check_triggered_rules(rule_config_name);
CREATE INDEX idx_triggered_rules_created_at ON health_check_triggered_rules(created_at);
CREATE INDEX idx_triggered_rules_lookup ON health_check_triggered_rules(run_id, rule_config_name, severity_level);

-- Add comment
COMMENT ON TABLE health_check_triggered_rules IS 'Stores analysis rules that were triggered during health check execution, enabling trend analysis without re-evaluating rules';
COMMENT ON COLUMN health_check_triggered_rules.rule_config_name IS 'The name of the rule configuration from analysis_rules.rules_json (e.g., high_cpu_iowait)';
COMMENT ON COLUMN health_check_triggered_rules.metric_name IS 'The full metric path that triggered the rule (e.g., check_iostat_io_data)';
COMMENT ON COLUMN health_check_triggered_rules.triggered_data IS 'The specific data row that triggered the rule, for auditing and detailed analysis';

-- Create a view for easy trend analysis
CREATE OR REPLACE VIEW rule_trends AS
SELECT 
    c.company_name,
    c.id as company_id,
    r.db_technology,
    r.run_timestamp,
    r.run_timestamp::date as run_date,
    tr.rule_config_name,
    tr.severity_level,
    tr.severity_score,
    tr.metric_name,
    tr.reasoning,
    tr.id as triggered_rule_id,
    r.id as run_id
FROM health_check_triggered_rules tr
JOIN health_check_runs r ON tr.run_id = r.id
JOIN companies c ON r.company_id = c.id;

COMMENT ON VIEW rule_trends IS 'Simplified view for querying rule trigger trends across companies and time periods';

-- Grant permissions (adjust role names as needed)
GRANT SELECT ON health_check_triggered_rules TO postgres;
GRANT INSERT ON health_check_triggered_rules TO postgres;
GRANT USAGE ON SEQUENCE health_check_triggered_rules_id_seq TO postgres;
GRANT SELECT ON rule_trends TO postgres;

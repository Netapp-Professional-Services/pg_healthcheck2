-- =====================================================
-- Function: get_all_reports()
-- Purpose: Retrieve all reports accessible to a user across all report types
--
-- This function aggregates reports from multiple sources:
-- 1. generated_ai_reports (AI-generated reports and trend analyses)
-- 2. uploaded_reports (manually uploaded reports)
-- 3. health_check_runs (point-in-time health check reports)
--
-- Parameters:
--   p_user_id: User ID to filter reports by access rights
--
-- Returns: JSONB array of report objects with structure:
--   [
--     {
--       "id": <report_id>,
--       "type": "generated|trend_analysis|uploaded|health_check",
--       "name": "<report name>",
--       "description": "<description>",
--       "annotations": "<annotations (for AI reports)>",
--       "timestamp": "<ISO timestamp>",
--       "target_host": "<host or company name>",
--       "db_name": "<database name or analysis period>",
--       "profile_name": "<AI profile, template, or 'Manual Upload'>",
--       "rule_set_name": "<rule set name if applicable>",
--       "template_name": "<template name if applicable>",
--       "company_name": "<company name>",
--       "db_technology": "<database technology (postgres, cassandra, etc.)>"
--     },
--     ...
--   ]
--
-- Usage Examples:
--   -- Get all reports for user 5
--   SELECT get_all_reports(5);
--
--   -- Pretty print for debugging
--   SELECT jsonb_pretty(get_all_reports(5));
--
-- =====================================================

CREATE OR REPLACE FUNCTION get_all_reports(
    p_user_id INT
)
RETURNS JSONB AS $$
DECLARE
    v_result JSONB;
BEGIN
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', id,
            'type', type,
            'name', name,
            'description', description,
            'annotations', annotations,
            'timestamp', timestamp,
            'target_host', target_host,
            'db_name', db_name,
            'profile_name', profile_name,
            'rule_set_name', rule_set_name,
            'template_name', template_name,
            'company_name', company_name,
            'db_technology', db_technology
        )
        ORDER BY timestamp DESC
    ), '[]'::jsonb)
    INTO v_result
    FROM (
        -- Generated AI reports and trend analyses
        SELECT
            gar.id,
            CASE
                WHEN gar.report_type = 'trend_analysis' THEN 'trend_analysis'
                ELSE 'generated'
            END AS type,
            gar.report_name AS name,
            gar.report_description AS description,
            gar.annotations,
            gar.generation_timestamp AS timestamp,
            COALESCE(hcr.target_host, c.company_name) AS target_host,
            COALESCE(
                hcr.target_db_name,
                CAST(gar.analysis_period_days AS TEXT) || ' day analysis'
            ) AS db_name,
            COALESCE(uap.profile_name, 'Trend Analysis') AS profile_name,
            ar.rule_set_name,
            pt.template_name,
            c.company_name,
            COALESCE(hcr.db_technology, 'N/A') AS db_technology
        FROM generated_ai_reports gar
        LEFT JOIN health_check_runs hcr ON gar.run_id = hcr.id
        LEFT JOIN companies c ON gar.company_id = c.id
        LEFT JOIN user_ai_profiles uap ON gar.ai_profile_id = uap.id
        LEFT JOIN analysis_rules ar ON gar.rule_set_id = ar.id
        LEFT JOIN prompt_templates pt ON gar.template_id = pt.id
        WHERE gar.generated_by_user_id = p_user_id

        UNION ALL

        -- Uploaded reports
        SELECT
            ur.id,
            'uploaded' AS type,
            ur.report_name AS name,
            ur.report_description AS description,
            NULL AS annotations,
            ur.upload_timestamp AS timestamp,
            'N/A' AS target_host,
            'N/A' AS db_name,
            'Manual Upload' AS profile_name,
            NULL AS rule_set_name,
            NULL AS template_name,
            'N/A' AS company_name,
            'N/A' AS db_technology
        FROM uploaded_reports ur
        WHERE ur.uploaded_by_user_id = p_user_id

        UNION ALL

        -- Health check runs (point-in-time reports)
        SELECT
            hcr2.id,
            'health_check' AS type,
            hcr2.prompt_template_name AS name,
            'Health check report for ' || hcr2.target_host AS description,
            NULL AS annotations,
            hcr2.run_timestamp AS timestamp,
            hcr2.target_host,
            hcr2.target_db_name AS db_name,
            'Health Check (' || hcr2.db_technology || ')' AS profile_name,
            NULL AS rule_set_name,
            hcr2.prompt_template_name AS template_name,
            c2.company_name,
            hcr2.db_technology
        FROM health_check_runs hcr2
        JOIN companies c2 ON hcr2.company_id = c2.id
        JOIN user_company_access uca ON c2.id = uca.company_id
        WHERE uca.user_id = p_user_id
          AND hcr2.report_adoc IS NOT NULL
    ) subquery;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE;

-- Add function comment for documentation
COMMENT ON FUNCTION get_all_reports(INT) IS
'Returns all reports accessible to a user across generated AI reports, uploaded reports, and health check runs. Returns JSONB array ordered by timestamp DESC.';

-- Example test queries:
/*
-- Test 1: Get all reports for user 5
SELECT get_all_reports(5);

-- Test 2: Pretty print for debugging
SELECT jsonb_pretty(get_all_reports(5));

-- Test 3: Filter by type in application layer
SELECT jsonb_pretty(
    (SELECT jsonb_agg(item)
     FROM jsonb_array_elements(get_all_reports(5)) item
     WHERE item->>'type' = 'health_check')
);

-- Test 4: Count reports by type
SELECT
    item->>'type' AS report_type,
    COUNT(*) AS count
FROM jsonb_array_elements(get_all_reports(5)) item
GROUP BY item->>'type'
ORDER BY count DESC;
*/

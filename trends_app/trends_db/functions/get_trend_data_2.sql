CREATE OR REPLACE FUNCTION get_trend_data(
    p_company_id INT,
    p_days INT
)
RETURNS JSONB AS $$
DECLARE
    v_company_info JSONB;
    v_summary_data JSONB;
    v_recurring_issues JSONB;
    v_health_scores JSONB;
    v_cross_tech JSONB;
BEGIN
    -- 1. Company info
    SELECT to_jsonb(t) INTO v_company_info
    FROM (
        SELECT id, company_name FROM companies WHERE id = p_company_id
    ) t;

    -- If company not found, return NULL (matches Python logic)
    IF v_company_info IS NULL THEN
        RETURN NULL;
    END IF;

    -- 2. Health check summary
    SELECT jsonb_build_object(
        'total_runs', COUNT(*),
        'technologies', COALESCE(ARRAY_AGG(DISTINCT db_technology ORDER BY db_technology) FILTER (WHERE db_technology IS NOT NULL), '{}'),
        'first_run', MIN(run_timestamp),
        'last_run', MAX(run_timestamp)
    ) INTO v_summary_data
    FROM health_check_runs
    WHERE company_id = p_company_id
      AND run_timestamp > NOW() - (p_days || ' days')::INTERVAL;

    -- 3. Recurring triggered rules
    SELECT COALESCE(jsonb_agg(t ORDER BY t.sort_key, t.occurrences DESC), '[]'::jsonb) INTO v_recurring_issues
    FROM (
        WITH latest_run AS (
          SELECT id FROM health_check_runs
          WHERE company_id = p_company_id
          ORDER BY run_timestamp DESC
          LIMIT 1
        )
        SELECT 
            htr.rule_config_name as rule_name,
            htr.severity_level as severity,
            COUNT(*) as occurrences,
            COUNT(DISTINCT htr.run_id) as run_appearances,
            MIN(hcr.run_timestamp) as first_seen,
            MAX(hcr.run_timestamp) as last_seen,
            COALESCE(ARRAY_AGG(DISTINCT htr.metric_name ORDER BY htr.metric_name) FILTER (WHERE htr.metric_name IS NOT NULL), '{}') as affected_modules,
            EXISTS(
                SELECT 1 FROM health_check_triggered_rules htr2
                WHERE htr2.run_id = (SELECT id FROM latest_run)
                  AND htr2.rule_config_name = htr.rule_config_name
            ) as in_latest_run,
            CASE htr.severity_level 
                 WHEN 'critical' THEN 1 
                 WHEN 'high' THEN 2 
                 WHEN 'medium' THEN 3 
                 ELSE 4
            END as sort_key
        FROM health_check_triggered_rules htr
        JOIN health_check_runs hcr ON htr.run_id = hcr.id
        WHERE hcr.company_id = p_company_id
          AND hcr.run_timestamp > NOW() - (p_days || ' days')::INTERVAL
        GROUP BY htr.rule_config_name, htr.severity_level
    ) t;

    -- 4. Calculate simple health scores
    SELECT COALESCE(jsonb_agg(t ORDER BY t.date), '[]'::jsonb) INTO v_health_scores
    FROM (
        SELECT 
            hcr.run_timestamp::date as date,
            hcr.id as run_id,
            100 - LEAST(100, (
                COALESCE(COUNT(*) FILTER (WHERE htr.severity_level = 'critical'), 0) * 20 +
                COALESCE(COUNT(*) FILTER (WHERE htr.severity_level = 'high'), 0) * 10 +
                COALESCE(COUNT(*) FILTER (WHERE htr.severity_level = 'medium'), 0) * 5
            )) as calculated_score
        FROM health_check_runs hcr
        LEFT JOIN health_check_triggered_rules htr ON htr.run_id = hcr.id
        WHERE hcr.company_id = p_company_id
          AND hcr.run_timestamp > NOW() - (p_days || ' days')::INTERVAL
        GROUP BY hcr.id, hcr.run_timestamp
    ) t;

    -- 5. Cross-technology correlation
    SELECT COALESCE(jsonb_agg(t ORDER BY t.total_triggered_rules DESC), '[]'::jsonb) INTO v_cross_tech
    FROM (
        SELECT 
            hcr.db_technology,
            COUNT(DISTINCT htr.rule_config_name) as unique_issues,
            COUNT(*) as total_triggered_rules,
            COUNT(*) FILTER (WHERE htr.severity_level = 'critical') as critical_count,
            COUNT(*) FILTER (WHERE htr.severity_level = 'high') as high_count,
            COUNT(*) FILTER (WHERE htr.severity_level = 'medium') as medium_count
        FROM health_check_triggered_rules htr
        JOIN health_check_runs hcr ON htr.run_id = hcr.id
        WHERE hcr.company_id = p_company_id
          AND hcr.run_timestamp > NOW() - (p_days || ' days')::INTERVAL
        GROUP BY hcr.db_technology
    ) t;

    -- Final Assembly
    RETURN jsonb_build_object(
        'company_info', v_company_info,
        'time_period', jsonb_build_object(
            'days', p_days,
            'first_run', v_summary_data -> 'first_run',
            'last_run', v_summary_data -> 'last_run'
        ),
        'summary', jsonb_build_object(
            'total_runs', v_summary_data -> 'total_runs',
            'technologies', v_summary_data -> 'technologies'
        ),
        'recurring_issues', v_recurring_issues,
        'health_score_trend', v_health_scores,
        'cross_technology_patterns', v_cross_tech
    );
END;
$$ LANGUAGE plpgsql STABLE;

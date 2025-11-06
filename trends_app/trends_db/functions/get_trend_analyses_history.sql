CREATE OR REPLACE FUNCTION get_trend_analyses_history(
    p_company_ids INT[], -- Matches 'companies.id' and 'generated_ai_reports.company_id' (integer)
    p_limit INT DEFAULT 50
)
RETURNS TABLE (
    id INT, -- Corrected: Matches 'generated_ai_reports.id' (integer)
    report_name TEXT, -- Matches 'generated_ai_reports.report_name' (text)
    created_at TIMESTAMPTZ, -- Matches 'generated_ai_reports.generation_timestamp' (timestamp with time zone)
    analysis_period_days INT, -- Matches 'generated_ai_reports.analysis_period_days' (integer)
    analysis_persona VARCHAR(50), -- Corrected: Matches 'generated_ai_reports.analysis_persona' (character varying(50))
    template_name TEXT, -- Prompt template used for the analysis
    company_name TEXT, -- Matches 'companies.company_name' (text)
    username TEXT -- Matches 'users.username' (text)
) AS $$
    SELECT
        gar.id,
        gar.report_name,
        gar.generation_timestamp, -- Renamed to 'created_at' by the RETURNS TABLE definition
        gar.analysis_period_days,
        gar.analysis_persona,
        pt.template_name,
        c.company_name,
        u.username
    FROM generated_ai_reports AS gar
    JOIN companies AS c ON gar.company_id = c.id
    JOIN users AS u ON gar.generated_by_user_id = u.id
    LEFT JOIN prompt_templates AS pt ON gar.template_id = pt.id
    WHERE gar.report_type = 'trend_analysis'
      AND gar.company_id = ANY(p_company_ids)
    ORDER BY gar.generation_timestamp DESC
    LIMIT p_limit;
$$ LANGUAGE sql STABLE;

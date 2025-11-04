CREATE OR REPLACE FUNCTION save_trend_analysis(
    p_user_id INT,
    p_company_id INT,
    p_analysis_period_days INT,
    p_persona VARCHAR(50),
    p_template_id INT,
    p_ai_content TEXT,
    p_profile_id INT
)
RETURNS INT AS $$
DECLARE
    v_company_name TEXT;
    v_report_name TEXT;
    new_analysis_id INT;
BEGIN
    -- Look up the company name to build the report name
    SELECT company_name INTO v_company_name
    FROM companies
    WHERE id = p_company_id;

    -- Construct the report name, just like in the original Python code
    v_report_name := 'Trend Analysis - ' || v_company_name || ' - ' || p_analysis_period_days || 'd';

    -- Insert the new report
    INSERT INTO generated_ai_reports (
        generated_by_user_id,
        report_name,
        template_id,
        ai_profile_id,
        report_content,
        report_type,
        company_id,
        analysis_period_days,
        analysis_persona
    )
    VALUES (
        p_user_id,
        v_report_name,
        p_template_id,
        p_profile_id,
        pgp_sym_encrypt(p_ai_content, get_encryption_key()), -- Encryption happens here
        'trend_analysis', -- Logic is centralized
        p_company_id,
        p_analysis_period_days,
        p_persona
    )
    RETURNING id INTO new_analysis_id; -- Get the newly created ID

    RETURN new_analysis_id;
END;
$$ LANGUAGE plpgsql VOLATILE;

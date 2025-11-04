CREATE OR REPLACE FUNCTION get_accessible_companies_list(
    p_company_ids INT[] -- Matches 'companies.id' (integer)
)
RETURNS TABLE (
    id INT,
    company_name TEXT -- Matches 'companies.company_name' (text)
) AS $$
    SELECT
        id,
        company_name
    FROM companies
    WHERE id = ANY(p_company_ids)
    ORDER BY company_name;
$$ LANGUAGE sql STABLE;

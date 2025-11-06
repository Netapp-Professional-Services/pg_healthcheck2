-- =====================================================
-- Function: get_unique_targets()
-- Purpose: Get distinct target combinations for dropdown filters
--
-- This function retrieves all unique target combinations (company:host:port:database)
-- that a user has access to. Used primarily for populating dropdown filters in the UI.
--
-- Parameters:
--   p_company_ids: Array of company IDs the user has access to (for security)
--
-- Returns: JSONB array of target strings with structure:
--   [
--     {"target": "company_name:target_host:target_port:target_db_name"},
--     {"target": "AcmeCorp:postgres-prod.example.com:5432:customers"},
--     {"target": "AcmeCorp:cassandra-cluster.example.com:9042:analytics"},
--     ...
--   ]
--
-- Usage Examples:
--   -- Get all targets for a user's accessible companies
--   SELECT get_unique_targets(ARRAY[1,2,3]);
--
--   -- Parse targets in application layer
--   SELECT jsonb_array_elements(get_unique_targets(ARRAY[1])) -> 'target' as target;
--
-- Security: Enforces company_id access control
-- Note: Returns formatted strings for easy dropdown population
--
-- Migration Strategy:
--   Phase 1 (Current): Query PostgreSQL health_check_runs table
--   Phase 2 (Future): Route to ClickHouse FDW for analytical queries
--
-- =====================================================

CREATE OR REPLACE FUNCTION get_unique_targets(
    p_company_ids INT[]
)
RETURNS JSONB AS $$
DECLARE
    v_result JSONB;
BEGIN
    -- Validate inputs
    IF p_company_ids IS NULL OR array_length(p_company_ids, 1) IS NULL THEN
        RETURN '[]'::jsonb;
    END IF;

    -- Fetch distinct target combinations with formatted output
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'target', c.company_name || ':' || hcr.target_host || ':' || hcr.target_port || ':' || hcr.target_db_name
        )
        ORDER BY c.company_name, hcr.target_host, hcr.target_port, hcr.target_db_name
    ), '[]'::jsonb)
    INTO v_result
    FROM (
        SELECT DISTINCT
            hcr.company_id,
            hcr.target_host,
            hcr.target_port,
            hcr.target_db_name
        FROM health_check_runs hcr
        WHERE hcr.company_id = ANY(p_company_ids)
    ) hcr
    JOIN companies c ON hcr.company_id = c.id;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

-- Add function comment
COMMENT ON FUNCTION get_unique_targets(INT[]) IS
'Get distinct target combinations for dropdown filters. Returns formatted strings: "company:host:port:database"';

-- Grant execution to trends app user
GRANT EXECUTE ON FUNCTION get_unique_targets(INT[]) TO postgres;

-- Example test queries:
/*
-- Test 1: Get all unique targets for accessible companies
SELECT jsonb_pretty(
    get_unique_targets(ARRAY[1])
);

-- Test 2: Verify access control (should return empty for non-existent company)
SELECT get_unique_targets(ARRAY[999]);

-- Test 3: Extract target strings as text
SELECT
    jsonb_array_elements(get_unique_targets(ARRAY[1])) ->> 'target' as target_string
ORDER BY target_string;

-- Test 4: Count unique targets
SELECT
    jsonb_array_length(get_unique_targets(ARRAY[1])) as unique_target_count;

-- Test 5: Performance check with multiple companies
EXPLAIN ANALYZE
SELECT get_unique_targets(ARRAY[1,2,3,4,5]);

-- Test 6: Compare with direct query (validation)
-- Direct query:
SELECT DISTINCT
    c.company_name || ':' || hcr.target_host || ':' || hcr.target_port || ':' || hcr.target_db_name as target
FROM health_check_runs hcr
JOIN companies c ON hcr.company_id = c.id
WHERE hcr.company_id = ANY(ARRAY[1])
ORDER BY target;

-- Stored procedure:
SELECT jsonb_array_elements(get_unique_targets(ARRAY[1])) ->> 'target' as target
ORDER BY target;
*/

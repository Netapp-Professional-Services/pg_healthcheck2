-- =====================================================
-- Function: get_health_check_runs()
-- Purpose: Fetch health check runs with filtering and user favorites
--
-- This function abstracts the query for retrieving health check runs,
-- enabling future migration to ClickHouse via FDW by providing a
-- single switchpoint for routing queries.
--
-- Parameters:
--   p_company_ids: Array of company IDs the user has access to
--   p_user_id: User ID for determining favorite status
--   p_company_name: Optional company name filter (part of target filter)
--   p_target_host: Optional target host filter
--   p_target_port: Optional target port filter
--   p_target_db_name: Optional target database name filter
--   p_start_date: Optional start date for filtering runs
--   p_end_date: Optional end date for filtering runs
--
-- Returns: JSONB array of run objects with structure:
--   [
--     {
--       "id": <run_id>,
--       "timestamp": "<ISO timestamp>",
--       "target": "<host>:<port> (<db_name>)",
--       "is_favorite": <boolean>
--     },
--     ...
--   ]
--
-- Usage Examples:
--   -- Get all runs for user
--   SELECT get_health_check_runs(ARRAY[1,2,3], 5);
--
--   -- Get runs with date filter
--   SELECT get_health_check_runs(ARRAY[1], 5, NULL, NULL, NULL, NULL, '2024-01-01', '2024-12-31');
--
--   -- Get runs for specific target
--   SELECT get_health_check_runs(ARRAY[1], 5, 'Acme Corp', '192.168.1.100', 5432, 'production');
--
-- Migration Strategy:
--   Phase 1 (Current): Query PostgreSQL health_check_runs table
--   Phase 2 (Future): Add logic to route to ClickHouse FDW based on config flag
--
-- =====================================================

CREATE OR REPLACE FUNCTION get_health_check_runs(
    p_company_ids INT[],
    p_user_id INT,
    p_company_name TEXT DEFAULT NULL,
    p_target_host TEXT DEFAULT NULL,
    p_target_port INT DEFAULT NULL,
    p_target_db_name TEXT DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
RETURNS JSONB AS $$
DECLARE
    v_result JSONB;
BEGIN
    -- Build and execute query with dynamic filters
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', id,
            'run_timestamp', run_timestamp,
            'target_host', target_host,
            'target_port', target_port,
            'target_db_name', target_db_name,
            'is_favorite', is_favorite
        )
        ORDER BY run_timestamp DESC
    ), '[]'::jsonb)
    INTO v_result
    FROM (
        SELECT
            hcr.id,
            hcr.run_timestamp,
            hcr.target_host,
            hcr.target_port,
            hcr.target_db_name,
            CASE WHEN ufr.user_id IS NOT NULL THEN true ELSE false END AS is_favorite
        FROM health_check_runs hcr
        JOIN companies c ON hcr.company_id = c.id
        LEFT JOIN user_favorite_runs ufr
            ON hcr.id = ufr.run_id AND ufr.user_id = p_user_id
        WHERE hcr.company_id = ANY(p_company_ids)
          -- Optional target filters
          AND (p_company_name IS NULL OR c.company_name = p_company_name)
          AND (p_target_host IS NULL OR hcr.target_host = p_target_host)
          AND (p_target_port IS NULL OR hcr.target_port = p_target_port)
          AND (p_target_db_name IS NULL OR hcr.target_db_name = p_target_db_name)
          -- Optional date filters
          AND (p_start_date IS NULL OR hcr.run_timestamp >= p_start_date)
          AND (p_end_date IS NULL OR hcr.run_timestamp < (p_end_date + INTERVAL '1 day'))
    ) subquery;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE;

-- Add function comment for documentation
COMMENT ON FUNCTION get_health_check_runs(INT[], INT, TEXT, TEXT, INT, TEXT, DATE, DATE) IS
'Abstraction layer for health check runs query. Returns JSONB array of runs with filtering and user favorite status. Designed for future ClickHouse migration via FDW.';

-- Example test queries:
/*
-- Test 1: Get all runs for companies 1,2,3 and user 5
SELECT get_health_check_runs(ARRAY[1,2,3], 5);

-- Test 2: Get runs with date range
SELECT get_health_check_runs(
    ARRAY[1],
    5,
    NULL, NULL, NULL, NULL,
    '2024-01-01'::date,
    '2024-12-31'::date
);

-- Test 3: Get runs for specific target
SELECT get_health_check_runs(
    ARRAY[1],
    5,
    'Acme Corp',
    '192.168.1.100',
    5432,
    'production',
    NULL,
    NULL
);

-- Test 4: Verify JSONB structure
SELECT jsonb_pretty(get_health_check_runs(ARRAY[1], 5));
*/

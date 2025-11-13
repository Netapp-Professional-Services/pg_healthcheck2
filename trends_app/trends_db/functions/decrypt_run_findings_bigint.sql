-- =====================================================
-- Function: decrypt_run_findings(BIGINT[])
-- Purpose: Overloaded version accepting BIGINT[] for compatibility
--
-- This overloaded version of decrypt_run_findings accepts BIGINT[] instead of INTEGER[]
-- to resolve type mismatch when called from get_runs_by_ids(BIGINT[], INT[]).
--
-- Background:
--   - get_runs_by_ids() accepts p_run_ids as BIGINT[]
--   - Original decrypt_run_findings() only accepted INTEGER[]
--   - PostgreSQL won't implicitly cast BIGINT[] to INTEGER[]
--   - This caused: "ERROR: function decrypt_run_findings(bigint[]) does not exist"
--
-- Solution:
--   - Create overloaded version that accepts BIGINT[]
--   - Safely cast to INTEGER[] and delegate to existing function
--   - Run IDs are within INTEGER range in practice (max ~2 billion)
--
-- Parameters:
--   p_run_ids: Array of run IDs as BIGINT[]
--
-- Returns: TABLE(run_id INTEGER, decrypted_findings JSONB)
--
-- Usage:
--   -- Called automatically by get_runs_by_ids
--   SELECT * FROM decrypt_run_findings(ARRAY[953, 957]::BIGINT[]);
--
-- Security: SECURITY DEFINER - runs with function owner privileges
-- =====================================================

CREATE OR REPLACE FUNCTION decrypt_run_findings(p_run_ids BIGINT[])
RETURNS TABLE(run_id INTEGER, decrypted_findings JSONB)
LANGUAGE plpgsql
STABLE SECURITY DEFINER
AS $$
BEGIN
    -- Convert BIGINT[] to INTEGER[] and delegate to existing function
    -- This is safe because run IDs are within INTEGER range in practice
    RETURN QUERY
    SELECT * FROM decrypt_run_findings(p_run_ids::INTEGER[]);
END;
$$;

-- Add function comment
COMMENT ON FUNCTION decrypt_run_findings(BIGINT[]) IS
'Overloaded version accepting BIGINT[] - delegates to INTEGER[] version for compatibility with get_runs_by_ids';

-- Grant execution to trends app user
GRANT EXECUTE ON FUNCTION decrypt_run_findings(BIGINT[]) TO postgres;

-- Verify function signatures
-- Expected result: 3 versions (INTEGER, INTEGER[], BIGINT[])
-- \df decrypt_run_findings

-- Test query (uncomment to test):
/*
-- Test 1: Verify it works with BIGINT[]
SELECT * FROM decrypt_run_findings(ARRAY[953, 957]::BIGINT[]);

-- Test 2: Verify get_runs_by_ids now works
SELECT jsonb_array_length(
    get_runs_by_ids(ARRAY[953, 957]::BIGINT[], ARRAY[559]::INTEGER[])
) as run_count;

-- Test 3: Verify both runs are returned
SELECT r->>'id' as run_id,
       r->>'run_timestamp' as timestamp,
       r->>'db_technology' as tech
FROM get_runs_by_ids(ARRAY[953, 957]::BIGINT[], ARRAY[559]::INTEGER[]),
     jsonb_array_elements(get_runs_by_ids) as r;
*/

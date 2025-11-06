-- =====================================================
-- Function: get_runs_by_ids()
-- Purpose: Fetch specific health check runs by IDs with access control
--
-- This function retrieves detailed information about specific health check runs,
-- including encrypted findings that need to be decrypted by the application layer.
-- Used for dashboard comparisons and detailed run analysis.
--
-- Parameters:
--   p_run_ids: Array of run IDs to fetch
--   p_company_ids: Array of company IDs the user has access to (for security)
--
-- Returns: JSONB array of run objects with structure:
--   [
--     {
--       "id": <run_id>,
--       "run_timestamp": "<ISO timestamp>",
--       "target_host": "<hostname>",
--       "target_port": <port>,
--       "target_db_name": "<database>",
--       "db_technology": "<technology>",
--       "encryption_mode": "<mode>",
--       "encrypted_data_key": "<key_if_kms>",
--       "decrypted_findings": <JSONB or encrypted string>,
--       "db_version": "<version>",
--       "cluster_name": "<cluster>",
--       "health_score": <score>
--     },
--     ...
--   ]
--
-- Usage Examples:
--   -- Get runs for comparison dashboard
--   SELECT get_runs_by_ids(ARRAY[123, 124], ARRAY[1,2,3]);
--
--   -- Single run details
--   SELECT get_runs_by_ids(ARRAY[456], ARRAY[1]);
--
-- Security: Enforces company_id access control
-- Note: Encryption handling remains in Python layer for security
--
-- Migration Strategy:
--   Phase 1 (Current): Query PostgreSQL health_check_runs table
--   Phase 2 (Future): Route to ClickHouse FDW for analytical queries
--
-- =====================================================

CREATE OR REPLACE FUNCTION get_runs_by_ids(
    p_run_ids BIGINT[],
    p_company_ids INT[]
)
RETURNS JSONB AS $$
DECLARE
    v_result JSONB;
BEGIN
    -- Validate inputs
    IF p_run_ids IS NULL OR array_length(p_run_ids, 1) IS NULL THEN
        RETURN '[]'::jsonb;
    END IF;

    IF p_company_ids IS NULL OR array_length(p_company_ids, 1) IS NULL THEN
        RETURN '[]'::jsonb;
    END IF;

    -- Fetch runs with decryption join
    -- Note: decrypt_run_findings() is an existing function that handles PGP decryption
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', hcr.id,
            'run_timestamp', hcr.run_timestamp,
            'target_host', hcr.target_host,
            'target_port', hcr.target_port,
            'target_db_name', hcr.target_db_name,
            'db_technology', hcr.db_technology,
            'encryption_mode', hcr.encryption_mode,
            'encrypted_data_key', hcr.encrypted_data_key,
            'decrypted_findings', decrypted.decrypted_findings,
            'db_version', hcr.db_version,
            'cluster_name', hcr.cluster_name,
            'node_count', hcr.node_count,
            'health_score', hcr.health_score
        )
        ORDER BY hcr.run_timestamp DESC
    ), '[]'::jsonb)
    INTO v_result
    FROM health_check_runs hcr
    JOIN decrypt_run_findings(p_run_ids) AS decrypted
        ON hcr.id = decrypted.run_id
    WHERE hcr.id = ANY(p_run_ids)
      AND hcr.company_id = ANY(p_company_ids);

    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

-- Add function comment
COMMENT ON FUNCTION get_runs_by_ids(BIGINT[], INT[]) IS
'Fetch specific health check runs by IDs with access control. Returns runs with decrypted findings (PGP) or encrypted data (KMS). KMS decryption handled by application layer.';

-- Grant execution to trends app user
GRANT EXECUTE ON FUNCTION get_runs_by_ids(BIGINT[], INT[]) TO postgres;

-- Example test queries:
/*
-- Test 1: Get two runs for comparison
SELECT jsonb_pretty(
    get_runs_by_ids(
        ARRAY[
            (SELECT id FROM health_check_runs ORDER BY id DESC LIMIT 1 OFFSET 0),
            (SELECT id FROM health_check_runs ORDER BY id DESC LIMIT 1 OFFSET 1)
        ],
        ARRAY[1]
    )
);

-- Test 2: Verify access control (should return empty for wrong company)
SELECT get_runs_by_ids(ARRAY[1], ARRAY[999]);

-- Test 3: Get single run details
SELECT get_runs_by_ids(
    ARRAY[(SELECT id FROM health_check_runs ORDER BY id DESC LIMIT 1)],
    ARRAY[1]
) -> 0;

-- Test 4: Performance check with multiple runs
EXPLAIN ANALYZE
SELECT get_runs_by_ids(
    ARRAY(SELECT id FROM health_check_runs ORDER BY id DESC LIMIT 10),
    ARRAY[1]
);

-- Test 5: Verify metadata columns are included
SELECT
    (result -> 0 ->> 'db_version') as db_version,
    (result -> 0 ->> 'cluster_name') as cluster_name,
    (result -> 0 ->> 'health_score') as health_score
FROM (
    SELECT get_runs_by_ids(
        ARRAY[(SELECT id FROM health_check_runs WHERE db_version IS NOT NULL ORDER BY id DESC LIMIT 1)],
        ARRAY[1]
    ) as result
) t;
*/

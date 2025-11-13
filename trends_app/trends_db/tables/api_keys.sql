-- API Keys table for external submission authentication
--
-- This table stores API keys that allow external health check tools to submit
-- data via the /api/submit-health-check endpoint.
--
-- Keys are hashed using PostgreSQL's pgcrypto crypt() function with bcrypt algorithm.

CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    key_name VARCHAR(255) NOT NULL,
    key_hash TEXT NOT NULL,  -- bcrypt hash of the actual API key
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    expires_at TIMESTAMP WITH TIME ZONE,  -- NULL means never expires
    last_used_at TIMESTAMP WITH TIME ZONE,
    usage_count BIGINT NOT NULL DEFAULT 0,
    notes TEXT,

    CONSTRAINT unique_key_name_per_company UNIQUE(company_id, key_name)
);

CREATE INDEX idx_api_keys_company_id ON api_keys(company_id);
CREATE INDEX idx_api_keys_is_active ON api_keys(is_active);
CREATE INDEX idx_api_keys_expires_at ON api_keys(expires_at);

-- Function to generate a new API key
-- Usage: SELECT generate_api_key(1, 'Production Server', 1);
CREATE OR REPLACE FUNCTION generate_api_key(
    p_company_id INTEGER,
    p_key_name VARCHAR,
    p_created_by_user_id INTEGER,
    p_expires_days INTEGER DEFAULT NULL
)
RETURNS TABLE(api_key TEXT, key_id INTEGER) AS $$
DECLARE
    v_api_key TEXT;
    v_key_id INTEGER;
    v_expires_at TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Generate random API key (64 characters, URL-safe base64)
    v_api_key := encode(gen_random_bytes(48), 'base64');
    v_api_key := replace(v_api_key, '+', '-');
    v_api_key := replace(v_api_key, '/', '_');
    v_api_key := replace(v_api_key, '=', '');

    -- Calculate expiration if specified
    IF p_expires_days IS NOT NULL THEN
        v_expires_at := NOW() + (p_expires_days || ' days')::INTERVAL;
    END IF;

    -- Insert with hashed key
    INSERT INTO api_keys (
        company_id, key_name, key_hash, created_by_user_id, expires_at
    ) VALUES (
        p_company_id, p_key_name, crypt(v_api_key, gen_salt('bf')),
        p_created_by_user_id, v_expires_at
    ) RETURNING id INTO v_key_id;

    -- Return the plain-text key (ONLY time it's visible!) and the key ID
    RETURN QUERY SELECT v_api_key, v_key_id;
END;
$$ LANGUAGE plpgsql;

-- Function to revoke an API key
CREATE OR REPLACE FUNCTION revoke_api_key(p_key_id INTEGER)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE api_keys SET is_active = false WHERE id = p_key_id;
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- Function to list active API keys for a company (without revealing actual keys)
CREATE OR REPLACE FUNCTION get_company_api_keys(p_company_id INTEGER)
RETURNS TABLE(
    key_id INTEGER,
    key_name VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    last_used_at TIMESTAMP WITH TIME ZONE,
    usage_count BIGINT,
    is_expired BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ak.id,
        ak.key_name,
        ak.is_active,
        ak.created_at,
        ak.expires_at,
        ak.last_used_at,
        ak.usage_count,
        (ak.expires_at IS NOT NULL AND ak.expires_at < NOW()) AS is_expired
    FROM api_keys ak
    WHERE ak.company_id = p_company_id
    ORDER BY ak.created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Add helpful comments
COMMENT ON TABLE api_keys IS 'API keys for external health check submission authentication';
COMMENT ON COLUMN api_keys.key_hash IS 'bcrypt hash of the API key - never store plain text';
COMMENT ON COLUMN api_keys.usage_count IS 'Incremented on each successful authentication';
COMMENT ON FUNCTION generate_api_key IS 'Generate new API key - returns plain text key ONCE';
COMMENT ON FUNCTION revoke_api_key IS 'Revoke an API key (sets is_active=false)';
COMMENT ON FUNCTION get_company_api_keys IS 'List all API keys for a company with metadata';

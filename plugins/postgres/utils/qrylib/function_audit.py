def get_security_definer_functions_query(connector):
    """
    Returns a query to find potentially insecure SECURITY DEFINER functions.
    These functions execute with the privileges of their owner, not the calling user.
    """
    return """
        SELECT
            p.proname AS function_name,
            n.nspname AS schema_name,
            pg_get_userbyid(p.proowner) AS owner
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE p.prosecdef IS TRUE
        ORDER BY schema_name, function_name
        LIMIT %(limit)s;
    """

def get_superuser_owned_functions_query(connector):
    """
    Returns a query to find functions owned by superusers. These should be reviewed
    to ensure they don't present a security risk.
    """
    return """
        SELECT
            p.proname AS function_name,
            n.nspname AS schema_name,
            pg_get_userbyid(p.proowner) AS owner
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_roles r ON r.oid = p.proowner
        WHERE r.rolsuper IS TRUE
        ORDER BY schema_name, function_name
        LIMIT %(limit)s;
    """

def get_function_volatility_query(connector):
    """
    Returns a query to find functions with a 'volatile' volatility setting,
    which can prevent query parallelization.
    """
    return """
        SELECT
            n.nspname as schema_name,
            p.proname as function_name,
            CASE p.provolatile
                WHEN 'i' THEN 'immutable'
                WHEN 's' THEN 'stable'
                WHEN 'v' THEN 'volatile'
            END as volatility
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND p.provolatile = 'v'
        ORDER BY 1, 2
        LIMIT %(limit)s;
    """


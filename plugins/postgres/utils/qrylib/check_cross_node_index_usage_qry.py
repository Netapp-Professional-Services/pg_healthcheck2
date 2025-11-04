"""
Query library for cross-node index usage analysis.

This module provides the SQL query for analyzing index usage patterns across
primary and replica nodes in a PostgreSQL cluster (especially Aurora).
"""

def get_cross_node_index_usage_query():
    """
    Get the SQL query for cross-node index usage analysis.

    This query gathers detailed index usage statistics from pg_stat_user_indexes,
    including scan counts, tuples read/fetched, and index size.

    Returns:
        str: SQL query for index usage analysis
    """
    return """
    SELECT
        schemaname,
        schemaname||'.'||relname AS table_name,
        indexrelname AS index_name,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
        pg_relation_size(indexrelid) AS index_size_bytes
    FROM pg_stat_user_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY pg_relation_size(indexrelid) DESC;
    """

def get_constraint_check_query():
    """
    Get the SQL query for checking if indexes support constraints.

    This helps identify indexes that cannot be safely dropped because they
    enforce primary keys, unique constraints, or foreign keys.

    Returns:
        str: SQL query for constraint analysis
    """
    return """
    SELECT
        conname AS constraint_name,
        conrelid::regclass AS table_name,
        contype AS constraint_type,
        pg_get_constraintdef(oid) AS constraint_definition
    FROM pg_constraint
    WHERE contype IN ('p', 'u', 'f')
    ORDER BY conrelid::regclass, conname;
    """

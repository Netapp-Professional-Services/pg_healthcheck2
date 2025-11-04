def get_table_count_query(connector):
    """
    Returns the query for counting user tables.
    
    Args:
        connector: Connector instance (may have version_info)
    
    Returns:
        str: Query in appropriate format for this database technology
    """
    # Standard query, no version-specific changes needed for table count
    return """
    SELECT COUNT(*) AS table_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
    """
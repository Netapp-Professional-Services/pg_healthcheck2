def get_table_count_query(connector):
    """
    Returns the query to count the number of user tables.
    
    Args:
        connector: Connector instance (may have version_info)
    
    Returns:
        str: Query in appropriate format for this database technology
    """
    # This query is standard across PostgreSQL versions
    return """
    SELECT COUNT(*) AS count
    FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
    """
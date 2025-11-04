"""
PostgreSQL Version Compatibility Module

This module provides version-aware functionality to construct the correct
SQL queries based on the version of the connected PostgreSQL database.
It relies on the version information fetched by the PostgresConnector.
"""

def get_pg_stat_statements_query(connector, query_type='standard'):
    """
    Get pg_stat_statements query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
        query_type (str): Type of query ('standard', 'write_activity', 'function_performance')
    
    Returns:
        str: SQL query appropriate for the PostgreSQL version
    """
    compatibility = connector.version_info
    
    if query_type == 'standard':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_exec_time DESC
            """
        else:
            return """
                SELECT query, calls, total_time, mean_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_time DESC
            """
    
    elif query_type == 'write_activity':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY rows DESC, wal_bytes DESC
            """
        else:
            return """
                SELECT query, calls, total_time, mean_time, rows,
                       temp_blks_written
                FROM pg_stat_statements
                ORDER BY rows DESC
            """
    
    elif query_type == 'function_performance':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
            """
        else:
            return """
                SELECT f.funcid::regproc AS function_name,
                       s.calls, s.total_time, s.self_time
                FROM pg_stat_statements s
                JOIN pg_proc f ON s.funcid = f.oid
                ORDER BY s.total_time DESC
            """
    
    else:
        return get_pg_stat_statements_query(connector, 'standard')

def get_vacuum_progress_query(connector):
    """
    Get vacuum progress query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
    
    Returns:
        str: SQL query for vacuum progress
    """
    compatibility = connector.version_info
    
    if compatibility.get('is_pg17_or_newer'):
        return """
            SELECT n.nspname||'.'||c.relname AS table_name, v.phase, 
                   v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed, 
                   v.index_vacuum_count, v.max_dead_tuple_bytes, v.dead_tuple_bytes, 
                   v.num_dead_item_ids
            FROM pg_stat_progress_vacuum v 
            JOIN pg_class c ON v.relid = c.oid 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE v.datname = %(database)s
        """
    else:
        return """
            SELECT n.nspname||'.'||c.relname AS table_name, v.phase, 
                   v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed, 
                   v.index_vacuum_count, v.num_dead_tuples
            FROM pg_stat_progress_vacuum v 
            JOIN pg_class c ON v.relid = c.oid 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE v.datname = %(database)s
        """

def get_monitoring_metrics_query(connector):
    """
    Get monitoring metrics query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
    
    Returns:
        str: SQL query for monitoring metrics
    """
    compatibility = connector.version_info
    
    if compatibility.get('is_pg14_or_newer'):
        return """
            SELECT 
                datname, numbackends, xact_commit, xact_rollback,
                blks_read, blks_hit, tup_returned, tup_fetched,
                tup_inserted, tup_updated, tup_deleted, temp_files,
                temp_bytes, deadlocks, blk_read_time, blk_write_time
            FROM pg_stat_database 
            WHERE datname = %(database)s
        """
    else:
        return """
            SELECT 
                datname, numbackends, xact_commit, xact_rollback,
                blks_read, blks_hit, tup_returned, tup_fetched,
                tup_inserted, tup_updated, tup_deleted, temp_files,
                temp_bytes, deadlocks
            FROM pg_stat_database 
            WHERE datname = %(database)s
        """

def get_version_warnings(connector):
    """
    Get warnings for version-specific limitations.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
    
    Returns:
        list: List of warning messages
    """
    compatibility = connector.version_info
    warnings = []
    
    if not compatibility.get('is_pg13_or_newer'):
        warnings.append(f"PostgreSQL {compatibility.get('version_string', 'Unknown')} is below the minimum supported version (13).")
    
    if compatibility.get('is_pg18_or_newer'):
        warnings.append(f"PostgreSQL {compatibility.get('version_string', 'Unknown')} is a development version. Not all features may be fully tested.")
    
    if compatibility.get('is_pg14') or (compatibility.get('is_pg14_or_newer') and not compatibility.get('is_pg15_or_newer')):
        warnings.append("PostgreSQL 14: pg_stat_statements no longer includes funcid. Function performance analysis will be limited.")
    
    return warnings

def get_blocking_query(connector):
    """
    Returns a SQL query to identify blocking sessions, tailored to the PostgreSQL version.
    
    Args:
        connector: The database connector object with version_info attribute.
    
    Returns:
        str: The SQL query string.
    """
    version_info = connector.version_info
    major_version = version_info.get('major_version', 0)  # Default to 0 if not available
    
    if major_version >= 10:
        # Query for PostgreSQL 10 and above
        query = """
        SELECT
            blocked_locks.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocking_locks.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query AS blocked_query,
            blocking_activity.query AS blocking_query
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted;
        """
    else:
        # Query for older PostgreSQL versions (adjust as needed for your environment)
        query = """
        -- Simplified query for PostgreSQL versions < 10
        SELECT
            blocked.pid AS blocked_pid,
            blocked.usename AS blocked_user,
            blocking.pid AS blocking_pid,
            blocking.usename AS blocking_user,
            blocked.query AS blocked_query,
            blocking.query AS blocking_query
        FROM pg_stat_activity blocked,
             pg_stat_activity blocking
        WHERE blocked.waiting = true
          AND blocked.locked_by = blocking.pid;
        """
    return query

def get_checkpoint_query(connector):
    """
    Returns the appropriate query for checkpoint analysis based on PG version.
    The 'pg_stat_bgwriter' view was expanded in PostgreSQL 15.
    """
    if connector.version_info.get('is_pg15_or_newer'):
        # PostgreSQL 15 and newer query
        return """
            SELECT
                checkpoints_timed,
                checkpoints_req,
                checkpoint_write_time,
                checkpoint_sync_time,
                checkpoint_proc_time,
                (checkpoints_timed + checkpoints_req) as total_checkpoints
            FROM pg_stat_bgwriter;
        """
    else:
        # Query for versions older than 15
        return """
            SELECT
                checkpoints_timed,
                checkpoints_req,
                checkpoint_write_time,
                checkpoint_sync_time,
                (checkpoints_timed + checkpoints_req) as total_checkpoints
            FROM pg_stat_bgwriter;
        """


def get_bgwriter_query(connector):
    """
    Returns the appropriate query for bgwriter/checkpointer analysis.
    The 'pg_stat_bgwriter' view was expanded in PostgreSQL 15 and the checkpointer
    stats were moved to their own view in 17.
    """
    if connector.version_info.get('is_pg17_or_newer'):
        return "SELECT * FROM pg_stat_checkpointer;"
    elif connector.version_info.get('is_pg15_or_newer'):
        return "SELECT *, (checkpoints_timed + checkpoints_req) as total_checkpoints FROM pg_stat_bgwriter;"
    else:
        return "SELECT *, (checkpoints_timed + checkpoints_req) as total_checkpoints FROM pg_stat_bgwriter;"

def get_available_extensions_query(connector):
    """
    Returns a query to find installed extensions that have available updates.
    The query is version-agnostic but is placed here for consistency.
    """
    return """
        SELECT name, default_version, installed_version
        FROM pg_available_extensions
        WHERE installed_version IS NOT NULL AND default_version <> installed_version;
    """

def get_high_insert_tables_query(connector):
    """
    Returns a query to identify tables with high rates of inserts, which
    can be candidates for tuning.
    """
    return """
        SELECT
            schemaname,
            relname,
            n_ins_since_vacuum AS inserts_since_last_vacuum
        FROM pg_stat_user_tables
        WHERE n_ins_since_vacuum > 100000 -- Threshold for high inserts
        ORDER BY n_ins_since_vacuum DESC
        LIMIT %(limit)s;
    """

def get_cache_hit_ratio_query(connector):
    """
    Returns a query to calculate the cache hit ratio for indexes and tables.
    A low cache hit ratio can indicate an undersized shared_buffers.
    """
    return """
        SELECT
            'index' AS object_type,
            SUM(heap_blks_read) AS heap_read,
            SUM(heap_blks_hit)  AS heap_hit,
            (SUM(heap_blks_hit) - SUM(heap_blks_read)) / SUM(heap_blks_hit) AS ratio
        FROM pg_statio_user_indexes
        UNION ALL
        SELECT
            'table' AS object_type,
            SUM(idx_blks_read) AS idx_read,
            SUM(idx_blks_hit)  AS idx_hit,
            (SUM(idx_blks_hit) - SUM(idx_blks_read)) / SUM(idx_blks_hit) AS ratio
        FROM pg_statio_user_tables;
    """

def get_vacuum_stats_query(connector):
    """
    Returns a query to find tables that may need vacuuming.
    """
    return """
        SELECT
            schemaname,
            relname,
            n_live_tup,
            n_dead_tup,
            last_autovacuum,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 10000 -- Threshold for dead tuples
        ORDER BY n_dead_tup DESC
        LIMIT %(limit)s;
    """


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

def get_transaction_wraparound_query(connector):
    """
    Returns a query to check for databases approaching transaction ID wraparound.
    The formula for calculating 'percent_towards_wraparound' is version-agnostic.
    """
    return """
        SELECT
            datname,
            age(datfrozenxid),
            current_setting('autovacuum_freeze_max_age')::float8,
            round(100 * age(datfrozenxid) / current_setting('autovacuum_freeze_max_age')::float8) as percent_towards_wraparound
        FROM pg_database
        ORDER BY age(datfrozenxid) DESC
        LIMIT %(limit)s;
    """

def get_inserted_tuples_query(connector):
    """
    Returns a query to find tables with the highest number of inserted tuples.
    This helps identify tables with high write activity.
    """
    return """
        SELECT
            schemaname,
            relname,
            n_tup_ins
        FROM pg_stat_user_tables
        WHERE n_tup_ins > 0
        ORDER BY n_tup_ins DESC
        LIMIT %(limit)s;
    """

def get_top_queries_by_io_time_query(connector):
    """
    Returns a query for top queries by I/O time, supporting multiple generations
    of pg_stat_statements column names.
    """
    if connector.has_pgstat_new_io_time:
        # PG17+ style: Sums up shared, local, and temp I/O times
        return """
            SELECT
                (COALESCE(shared_blk_read_time, 0) + COALESCE(shared_blk_write_time, 0) +
                 COALESCE(local_blk_read_time, 0) + COALESCE(local_blk_write_time, 0) +
                 COALESCE(temp_blk_read_time, 0) + COALESCE(temp_blk_write_time, 0)) as total_io_time,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_io_time DESC LIMIT %(limit)s;
        """
    elif connector.has_pgstat_legacy_io_time:
        # PG13-16 style
        return """
            SELECT
                (blk_read_time + blk_write_time) as total_io_time,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_io_time DESC LIMIT %(limit)s;
        """
    else:
        # Fallback for older versions or outdated extensions
        return """
            SELECT
                total_exec_time as total_exec_time_as_proxy_for_io,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_time DESC LIMIT %(limit)s;
        """

# Add this new function to your existing compatibility file

def get_cache_analysis_queries(connector):
    """
    Returns a dictionary of queries for cache analysis, adjusted for PG version.
    Handles the split of stats from pg_stat_bgwriter to pg_stat_checkpointer in PG17+.
    """
    queries = {
        "database_cache_hit_ratio": """
            SELECT
                datname,
                blks_hit,
                blks_read,
                round((blks_hit::float / NULLIF(blks_hit + blks_read, 0) * 100)::numeric, 2) AS hit_ratio_percent
            FROM pg_stat_database
            WHERE blks_read > 0 AND datname = %(database)s;
        """
    }
    
    # In PostgreSQL 17, checkpoint stats moved from pg_stat_bgwriter to pg_stat_checkpointer.
    # We now query both to get a complete picture.
    if connector.version_info.get('is_pg17_or_newer'):
        queries["bgwriter_buffer_statistics"] = "SELECT buffers_alloc, buffers_clean FROM pg_stat_bgwriter;"
        queries["checkpoint_buffer_statistics"] = "SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;"
    else:
        # For older versions, all stats are in pg_stat_bgwriter
        queries["buffer_cache_statistics"] = "SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;"

    return queries

def get_high_insert_tables_query(connector):
    """
    Returns a query to identify tables with a high rate of inserts.
    This query is version-agnostic but centralized for consistency.
    """
    return """
        SELECT
            schemaname || '.' || relname AS table_name,
            n_tup_ins,
            n_dead_tup,
            last_autovacuum
        FROM pg_stat_user_tables
        WHERE n_tup_ins > %(min_tup_ins_threshold)s
        ORDER BY n_tup_ins DESC
        LIMIT %(limit)s;
    """

def get_top_write_queries_query(connector):
    """
    Returns a version-aware query to find top write-intensive queries
    from pg_stat_statements.
    """
    # Sanitize query text for safe AsciiDoc table display
    query_select_prefix = "REPLACE(REPLACE(LEFT(query, 150), E'\\n', ' '), '|', ' ') || '...' AS query"

    if connector.version_info.get('is_pg14_or_newer'):
        return f"""
            SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows,
                   shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
            FROM pg_stat_statements
            ORDER BY wal_bytes DESC, shared_blks_written DESC
            LIMIT %(limit)s;
        """
    else:
        # For older versions, we rely on blocks written as the primary indicator
        return f"""
            SELECT {query_select_prefix}, calls, total_time AS total_exec_time, mean_time AS mean_exec_time, rows,
                   shared_blks_written, local_blks_written, temp_blks_written
            FROM pg_stat_statements
            ORDER BY shared_blks_written DESC, rows DESC
            LIMIT %(limit)s;
        """

# Used by foreign_key_audit.py
def get_all_foreign_keys_query(connector):
    """
    Returns a query to list all foreign key constraints in the database.
    This query is version-agnostic.
    """
    return """
        SELECT
            conname AS foreign_key_name,
            conrelid::regclass AS child_table,
            pg_get_constraintdef(oid) AS constraint_definition
        FROM pg_constraint
        WHERE contype = 'f'
        ORDER BY conrelid::regclass, conname
        LIMIT %(limit)s;
    """

def get_missing_fk_indexes_query(connector):
    """
    Returns a query to find foreign keys on child tables that are missing
    a corresponding index on the key column(s). This is a major cause of
    write amplification and locking issues.
    This query is version-aware for optimal performance.
    """
    # The core logic is stable across recent PostgreSQL versions, but centralizing it here
    # allows for future optimizations (e.g., for newer PG versions with improved catalog views).
    return """
        SELECT
            fk.conname AS foreign_key_name,
            n_child.nspname || '.' || fk_table.relname AS child_table,
            ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.conrelid AND attnum = ANY(fk.conkey)) AS fk_col_names,
            n_parent.nspname || '.' || pk_table.relname AS parent_table,
            ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.confrelid AND attnum = ANY(fk.confkey)) AS pk_col_names
        FROM
            pg_constraint fk
        JOIN pg_class fk_table ON fk_table.oid = fk.conrelid
        JOIN pg_namespace n_child ON n_child.oid = fk_table.relnamespace
        JOIN pg_class pk_table ON pk_table.oid = fk.confrelid
        JOIN pg_namespace n_parent ON n_parent.oid = pk_table.relnamespace
        WHERE
            fk.contype = 'f'
            AND NOT EXISTS (
                SELECT 1
                FROM pg_index i
                WHERE i.indrelid = fk.conrelid
                -- Ensure the leading columns of the index match the foreign key columns
                AND (i.indkey::int[] @> fk.conkey::int[] AND i.indkey::int[] <@ fk.conkey::int[])
            )
        ORDER BY
            child_table, foreign_key_name
        LIMIT %(limit)s;
    """

def get_fk_summary_query(connector):
    """
    Returns a query that provides a summary of foreign key health,
    counting total FKs and those missing an index.
    """
    return """
        SELECT
            COUNT(*) AS total_foreign_keys,
            COUNT(*) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pg_index i
                    WHERE i.indrelid = c.conrelid
                    AND (i.indkey::int[] @> c.conkey::int[] AND i.indkey::int[] <@ c.conkey::int[])
                )
            ) AS unindexed_foreign_keys
        FROM pg_constraint c
        WHERE c.contype = 'f';
    """


# ==== Used by replication_health.py ====


def get_physical_replication_query(connector):
    """
    Returns a version-aware query to check physical replication status.
    This relies on the connector's pre-fetched version information.
    """
    # This check correctly uses the pre-fetched version info from the connector
    if connector.version_info.get('is_pg10_or_newer'):
        return """
            SELECT usename, application_name, client_addr, state, sync_state,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) AS sent_lag_bytes,
                   write_lag, flush_lag, replay_lag
            FROM pg_stat_replication;
        """
    else:
        # Fallback for legacy versions older than 10
        return """
            SELECT usename, application_name, client_addr, state, sync_state,
                   pg_xlog_location_diff(pg_current_xlog_location(), sent_location) AS sent_lag_bytes
            FROM pg_stat_replication;
        """

def get_replication_slots_query(connector):
    """
    Returns a version-aware query to check all replication slots.
    """
    # This check also correctly uses the connector's version info
    if connector.version_info.get('is_pg10_or_newer'):
        lsn_diff_func = 'pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)'
    else:
        lsn_diff_func = 'pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn)'
#        lsn_diff_func = 'pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)'

    return f"""
        SELECT
            slot_name, plugin, slot_type, database, active,
            pg_size_pretty({lsn_diff_func}) AS replication_lag_size,
            wal_status, safe_wal_size
        FROM pg_replication_slots;
    """

def get_subscription_stats_query(connector):
    """
    Returns a query for logical replication subscription stats.
    """
    if connector.version_info.get('is_pg10_or_newer'):
        return "SELECT subname, received_lsn, last_msg_send_time, last_msg_receipt_time FROM pg_stat_subscription;"
    else:
        return None

def get_security_audit_query(connector):
    """
    Returns a query to audit user roles and password encryption methods.
    Checks for superuser status and MD5 password usage.
    """
    # In PostgreSQL 10+, the column is `rolpassword`. Before that, it was `passwd`.
    # For simplicity across modern versions, we can check if the string starts with 'md5'.
    password_check_column = "rolpassword"
    
    return f"""
        SELECT
            rolname AS user_name,
            rolsuper AS is_superuser,
            rolcreaterole AS can_create_roles,
            rolcreatedb AS can_create_db,
            {password_check_column} ~ 'md5' AS uses_md5_password
        FROM pg_authid
        ORDER BY rolsuper DESC, rolname;
    """

def get_ssl_stats_query(connector):
    """
    Returns a query to get statistics on SSL/TLS encrypted connections.
    """
    return """
        SELECT
            ssl,
            count(*) as connection_count
        FROM pg_stat_ssl
        JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid
        GROUP BY ssl;
    """

# Add these new functions to your existing compatibility file

def get_pk_exhaustion_summary_query(connector):
    """
    Returns a query that provides a high-level summary of primary keys that
    are integer-based, for AI analysis.
    """
    return """
        SELECT
            COUNT(*) AS total_integer_pks,
            COUNT(*) FILTER (WHERE a.atttypid = 21) AS smallint_pk_count,
            COUNT(*) FILTER (WHERE a.atttypid = 23) AS integer_pk_count
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_constraint con ON con.conrelid = c.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
        WHERE con.contype = 'p'
          AND a.atttypid IN (21, 23) -- smallint, integer
          AND n.nspname NOT IN ('information_schema', 'pg_catalog');
    """

def get_pk_exhaustion_details_query(connector):
    """
    Returns a query to find integer-based primary keys that are nearing
    their maximum value (exhaustion) by correctly finding the associated sequence.
    """
    # This query now uses pg_get_serial_sequence to reliably find the sequence name
    # and correctly uses a WHERE clause for filtering.
    return """
        WITH pk_info AS (
            SELECT
                n.nspname,
                c.relname,
                a.attname,
                a.atttypid,
                pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(c.relname), a.attname) AS seq_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_constraint con ON con.conrelid = c.oid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
            WHERE con.contype = 'p'
              AND a.atttypid IN (21, 23) -- smallint, integer
              AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        )
        SELECT
            pi.nspname AS table_schema,
            pi.relname AS table_name,
            pi.attname AS column_name,
            format_type(pi.atttypid, -1) AS data_type,
            s.last_value,
            CASE
                WHEN pi.atttypid = 21 THEN 32767
                WHEN pi.atttypid = 23 THEN 2147483647
            END as max_value,
            ROUND((s.last_value::numeric / (
                CASE
                    WHEN pi.atttypid = 21 THEN 32767
                    WHEN pi.atttypid = 23 THEN 2147483647
                END
            )::numeric) * 100, 2) AS percentage_used
        FROM pk_info pi
        JOIN pg_sequences s ON pi.seq_name = s.schemaname || '.' || s.sequencename
        WHERE s.last_value IS NOT NULL
          -- CORRECTED: Filtering logic moved from HAVING to WHERE
          AND (s.last_value::numeric / (
                CASE
                    WHEN pi.atttypid = 21 THEN 32767
                    WHEN pi.atttypid = 23 THEN 2147483647
                END
            )::numeric) > 0.80 -- Threshold for reporting (80%)
        ORDER BY percentage_used DESC;
    """

def get_object_counts_query(connector):
    """
    Returns a single, efficient query to count various database object types.
    This serves as both the detailed data and the AI summary.
    """
    return """
        SELECT
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS tables,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'i' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS indexes,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'S' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS sequences,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'v' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS views,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'm' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS materialized_views,
            (SELECT COUNT(*) FROM pg_proc WHERE pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS functions_procedures,
            (SELECT COUNT(*) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%%' AND nspname != 'information_schema') AS schemas,
            (SELECT COUNT(*) FROM pg_constraint WHERE contype = 'f') AS foreign_keys,
            (SELECT COUNT(*) FROM pg_class WHERE relispartition = true) AS partitions;
    """

def get_specialized_indexes_summary_query(connector):
    """
    Returns a query that provides a summary count of each specialized index type.
    """
    return """
        SELECT
            am.amname AS index_type,
            COUNT(*) AS count
        FROM pg_class i
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = i.relnamespace
        WHERE i.relkind = 'i'
          AND am.amname NOT IN ('btree')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY am.amname;
    """

def get_specialized_indexes_details_query(connector):
    """
    Returns a query to get detailed information about all non-B-Tree indexes.
    """
    return """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            i.relname AS index_name,
            am.amname AS index_type,
            pg_size_pretty(pg_relation_size(i.oid)) as index_size
        FROM pg_class c
        JOIN pg_index ix ON ix.indrelid = c.oid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE am.amname NOT IN ('btree')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY am.amname, n.nspname, c.relname, i.relname;
    """

def get_unused_indexes_query(connector):
    """
    Returns a query to find large, unused indexes. This is a key indicator of
    unnecessary write overhead and wasted space.
    """
    return """
        SELECT
            schemaname AS schema_name,
            relname AS table_name,
            indexrelname AS index_name,
            pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
            idx_scan AS index_scans
        FROM pg_stat_user_indexes
        WHERE idx_scan < 100 AND pg_relation_size(indexrelid) > 1048576 -- Scanned < 100 times and > 1MB
        ORDER BY pg_relation_size(indexrelid) DESC
        LIMIT %(limit)s;
    """

def get_duplicate_indexes_query(connector):
    """
    Returns a query to find indexes that are functionally duplicates of each other.
    """
    return """
        SELECT n.nspname || '.' || t.relname AS table_name,
        pg_size_pretty(SUM(pg_relation_size(pi.indexrelid))::bigint) AS total_wasted_size,
        array_agg(i.relname ORDER BY i.relname) AS redundant_indexes
        FROM pg_index AS pi
        JOIN pg_class AS i ON i.oid = pi.indexrelid
        JOIN pg_class AS t ON t.oid = pi.indrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND pi.indisprimary = false
        GROUP BY pi.indrelid, pi.indkey, pi.indclass, pi.indpred, n.nspname, t.relname
        HAVING COUNT(*) > 1 ORDER BY SUM(pg_relation_size(pi.indexrelid)) DESC LIMIT %(limit)s;
    """

def get_invalid_indexes_query(connector):
    """
    Returns a query to find invalid indexes that are unusable by the planner.
    """
    return """
        SELECT n.nspname AS schema_name, c.relname AS table_name, i.relname AS index_name
        FROM pg_class c, pg_index ix, pg_class i, pg_namespace n
        WHERE ix.indisvalid = false
          AND ix.indexrelid = i.oid
          AND i.relnamespace = c.relnamespace -- Simplified join condition
          AND c.oid = ix.indrelid
          AND i.relnamespace = n.oid;
    """

def get_object_inventory_query(connector):
    """
    Returns the SQL query to list all database objects.
    This query is version-agnostic but is centralized for consistency.
    """
    return """
    -- Tables, Views, Materialized Views, Sequences
    SELECT
        n.nspname AS schema_name,
        c.relname AS object_name,
        CASE c.relkind
            WHEN 'r' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'S' THEN 'SEQUENCE'
            WHEN 'f' THEN 'FOREIGN TABLE'
        END AS object_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'v', 'm', 'S', 'f')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

    UNION ALL

    -- Indexes
    SELECT
        n.nspname AS schema_name,
        c.relname AS object_name,
        'INDEX' AS object_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'i'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

    UNION ALL

    -- Functions and Procedures
    SELECT
        n.nspname AS schema_name,
        p.proname AS object_name,
        CASE p.prokind
            WHEN 'f' THEN 'FUNCTION'
            WHEN 'p' THEN 'PROCEDURE'
            WHEN 'a' THEN 'AGGREGATE FUNCTION'
            WHEN 'w' THEN 'WINDOW FUNCTION'
        END AS object_type
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

    ORDER BY schema_name, object_type, object_name;
    """

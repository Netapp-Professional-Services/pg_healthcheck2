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

    return f"""
        SELECT
            slot_name, plugin, slot_type, database, active,
            pg_size_pretty({lsn_diff_func}) AS replication_lag_size,
            wal_status, safe_wal_size
        FROM pg_replication_slots;
    """

def get_wal_receiver_query(connector):
    """
    Returns a query to check WAL receiver status on standby nodes.
    Shows incoming replication from the primary.
    """
    if connector.version_info.get('is_pg10_or_newer'):
        return """
            SELECT
                status,
                receive_start_lsn,
                receive_start_tli,
                written_lsn,
                flushed_lsn,
                received_tli,
                last_msg_send_time,
                last_msg_receipt_time,
                latest_end_lsn,
                latest_end_time,
                slot_name,
                sender_host,
                sender_port,
                conninfo,
                CASE
                    WHEN last_msg_receipt_time IS NULL THEN NULL
                    ELSE EXTRACT(EPOCH FROM (now() - last_msg_receipt_time))
                END AS last_msg_age_seconds
            FROM pg_stat_wal_receiver;
        """
    else:
        # Fallback for legacy versions (PG 9.x)
        return """
            SELECT
                status,
                receive_start_lsn,
                receive_start_tli,
                written_lsn,
                flushed_lsn,
                received_tli,
                last_msg_send_time,
                last_msg_receipt_time,
                latest_end_lsn,
                latest_end_time,
                slot_name,
                conninfo,
                CASE
                    WHEN last_msg_receipt_time IS NULL THEN NULL
                    ELSE EXTRACT(EPOCH FROM (now() - last_msg_receipt_time))
                END AS last_msg_age_seconds
            FROM pg_stat_wal_receiver;
        """


def get_subscription_stats_query(connector):
    """
    Returns a query for logical replication subscription stats.
    """
    if connector.version_info.get('is_pg10_or_newer'):
        return "SELECT subname, received_lsn, last_msg_send_time, last_msg_receipt_time FROM pg_stat_subscription;"
    else:
        return None

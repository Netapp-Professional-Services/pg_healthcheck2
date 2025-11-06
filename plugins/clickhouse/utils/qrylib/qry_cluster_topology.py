"""
ClickHouse Cluster Topology Queries

Queries for cluster discovery, node topology, and replica health.
"""


def get_cluster_topology_query(connector):
    """
    Returns query for cluster topology discovery.

    Retrieves cluster configuration from system.clusters including
    shard and replica information.
    """
    return """
    SELECT
        cluster,
        shard_num,
        replica_num,
        host_name,
        host_address,
        port,
        is_local,
        database_shard_name,
        database_replica_name
    FROM system.clusters
    ORDER BY cluster, shard_num, replica_num
    """


def get_replica_status_query(connector):
    """
    Returns query for replica health status.

    Monitors replication status, lag, and replica availability.
    """
    return """
    SELECT
        database,
        table,
        is_leader,
        is_readonly,
        is_session_expired,
        future_parts,
        parts_to_check,
        queue_size,
        inserts_in_queue,
        merges_in_queue,
        log_max_index,
        log_pointer,
        log_max_index - log_pointer as log_delay,
        absolute_delay,
        total_replicas,
        active_replicas
    FROM system.replicas
    WHERE database NOT IN ('system', '_temporary_and_external_tables')
    ORDER BY database, table
    """


def get_distributed_ddl_queue_query(connector):
    """
    Returns query for distributed DDL queue status.

    Monitors pending and failed DDL operations across the cluster.
    """
    return """
    SELECT
        entry,
        host_name,
        host_address,
        port,
        status,
        cluster,
        query,
        initiator_host,
        exception_text
    FROM system.distributed_ddl_queue
    WHERE status != 'Finished' OR exception_text != ''
    ORDER BY entry DESC
    LIMIT 100
    """


def get_cluster_health_summary_query(connector):
    """
    Returns query for overall cluster health summary.

    Aggregates replica status across all tables.
    """
    return """
    SELECT
        count() as total_replicated_tables,
        countIf(is_readonly = 1) as readonly_replicas,
        countIf(is_session_expired = 1) as expired_sessions,
        countIf(log_delay > 1000) as lagging_replicas,
        countIf(active_replicas < total_replicas) as tables_with_inactive_replicas,
        sum(queue_size) as total_queue_size,
        max(absolute_delay) as max_absolute_delay
    FROM system.replicas
    WHERE database NOT IN ('system', '_temporary_and_external_tables')
    """


def get_zookeeper_connection_query(connector):
    """
    Returns query for ZooKeeper/ClickHouse Keeper connection status.

    Monitors the health of the coordination service connection which is
    critical for replicated table operations.

    Note: This table may not exist if ClickHouse doesn't use ZooKeeper/Keeper.
    Note: zookeeper_path column removed in ClickHouse 25.x

    Version Support:
    - ClickHouse < 25: Includes zookeeper_path column
    - ClickHouse >= 25: Excludes zookeeper_path (removed)
    """
    major_version = connector.version_info.get('major_version', 0)

    # ClickHouse 25.x removed the zookeeper_path column
    if major_version >= 25:
        return """
        SELECT
            name,
            host,
            port,
            is_expired,
            session_uptime_elapsed_seconds
        FROM system.zookeeper_connection
        """
    else:
        # ClickHouse < 25.x includes zookeeper_path
        return """
        SELECT
            name,
            host,
            port,
            is_expired,
            session_uptime_elapsed_seconds,
            zookeeper_path
        FROM system.zookeeper_connection
        """

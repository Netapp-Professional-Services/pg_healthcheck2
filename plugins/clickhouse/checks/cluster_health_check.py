"""
ClickHouse Cluster Health Check

Monitors cluster topology, replica health, and overall cluster status.
Equivalent to OpenSearch's cluster health check.

Requirements:
- ClickHouse client access
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_cluster_topology

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 10  # Critical - cluster health is highest priority


def run_cluster_health_check(connector, settings):
    """
    Retrieves and formats the health status of the ClickHouse cluster.

    This check provides cluster-level health metrics including topology,
    replica status, and cluster configuration.

    Args:
        connector: ClickHouse connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add check header
    builder.h3("ClickHouse Cluster Health")
    builder.para(
        "Cluster topology, replica health, and configuration status across all nodes and shards."
    )

    try:
        # 1. Get cluster information using qrylib
        cluster_query = qry_cluster_topology.get_cluster_topology_query(connector)
        clusters_result = connector.execute_query(cluster_query)

        if not clusters_result:
            builder.warning("No cluster configuration found. This may be a standalone instance.")
            structured_data["cluster_health"] = {"status": "standalone", "details": "No cluster configured"}
            return builder.build(), structured_data

        # Parse cluster topology
        clusters = {}
        for row in clusters_result:
            cluster_name = row[0]
            if cluster_name not in clusters:
                clusters[cluster_name] = {
                    'shards': {},
                    'total_replicas': 0,
                    'total_shards': 0
                }

            shard_num = row[1]
            if shard_num not in clusters[cluster_name]['shards']:
                clusters[cluster_name]['shards'][shard_num] = []
                clusters[cluster_name]['total_shards'] += 1

            clusters[cluster_name]['shards'][shard_num].append({
                'replica_num': row[3],
                'host_name': row[4],
                'host_address': row[5],
                'port': row[6],
                'is_local': row[7],
                'user': row[8]
            })
            clusters[cluster_name]['total_replicas'] += 1

        # 2. Get replica status for replicated tables using qrylib
        replica_query = qry_cluster_topology.get_replica_status_query(connector)
        replicas_result = connector.execute_query(replica_query)

        # Process replica health
        total_replicas = len(replicas_result) if replicas_result else 0
        readonly_replicas = []
        session_expired = []
        lagging_replicas = []

        if replicas_result:
            for row in replicas_result:
                replica_info = {
                    'database': row[0],
                    'table': row[1],
                    'is_leader': row[2],
                    'is_readonly': row[3],
                    'is_session_expired': row[4],
                    'queue_size': row[7],
                    'log_delay': row[10],
                    'total_replicas': row[11],
                    'active_replicas': row[12]
                }

                if row[3]:  # is_readonly
                    readonly_replicas.append(replica_info)
                if row[4]:  # is_session_expired
                    session_expired.append(replica_info)
                if row[10] > 100:  # log_delay > 100
                    lagging_replicas.append(replica_info)

        # 3. Get ZooKeeper/Keeper connection status using qrylib
        zk_connection_data = []
        zk_expired_sessions = []
        try:
            zk_query = qry_cluster_topology.get_zookeeper_connection_query(connector)
            zk_result = connector.execute_query(zk_query)

            if zk_result:
                for row in zk_result:
                    zk_info = {
                        'name': row[0],
                        'host': row[1],
                        'port': row[2],
                        'is_expired': row[3],
                        'session_uptime': row[4]
                    }
                    # ClickHouse < 25.x includes zookeeper_path column
                    if len(row) > 5:
                        zk_info['zookeeper_path'] = row[5]

                    zk_connection_data.append(zk_info)
                    if row[3]:  # is_expired
                        zk_expired_sessions.append(zk_info)
        except Exception as e:
            logger.debug(f"ZooKeeper connection table not available or not configured: {e}")

        # 4. Display cluster status
        critical_issues = len(readonly_replicas) + len(session_expired) + len(zk_expired_sessions)
        warning_issues = len(lagging_replicas)

        if critical_issues > 0:
            builder.critical(
                "🔴 **Critical Issues Detected**\n\n"
                f"The cluster has {critical_issues} critical replica issue(s). "
                "Some replicas are not functioning properly."
            )
        elif warning_issues > 0:
            builder.warning(
                "⚠️ **Replication Lag Detected**\n\n"
                f"The cluster has {warning_issues} replicas with significant replication lag."
            )
        else:
            builder.note(
                "✅ **Cluster is Healthy**\n\n"
                "All replicas are operational and synchronized."
            )

        # 4. Display cluster topology
        builder.h4("Cluster Topology")

        for cluster_name, cluster_data in clusters.items():
            builder.para(f"**Cluster:** {cluster_name}")
            builder.para(f"- Total Shards: {cluster_data['total_shards']}")
            builder.para(f"- Total Replicas: {cluster_data['total_replicas']}")
            builder.blank()

            # Build topology table
            topology_table = []
            for shard_num in sorted(cluster_data['shards'].keys()):
                replicas = cluster_data['shards'][shard_num]
                for replica in replicas:
                    topology_table.append({
                        "Shard": shard_num,
                        "Replica": replica['replica_num'],
                        "Host": replica['host_name'],
                        "Address": f"{replica['host_address']}:{replica['port']}",
                        "Local": "✅" if replica['is_local'] else ""
                    })

            if topology_table:
                builder.table(topology_table)
            builder.blank()

        # 5. Display ZooKeeper/Keeper connection status
        if zk_connection_data:
            builder.h4("ZooKeeper/ClickHouse Keeper Connection")

            if zk_expired_sessions:
                builder.critical(
                    f"🔴 **{len(zk_expired_sessions)} ZooKeeper/Keeper session(s) expired**\n\n"
                    "Expired coordination service sessions will affect replication."
                )

            zk_table = []
            for zk in zk_connection_data:
                status = "🔴 Expired" if zk['is_expired'] else "✅ Active"
                uptime_hours = zk['session_uptime'] / 3600 if zk['session_uptime'] else 0

                table_row = {
                    "Status": status,
                    "Name": zk['name'],
                    "Host": zk['host'],
                    "Port": str(zk['port']),
                    "Uptime (hours)": f"{uptime_hours:.1f}"
                }

                # Only include ZK Path if available (ClickHouse < 25.x)
                if 'zookeeper_path' in zk:
                    zk_path = zk['zookeeper_path']
                    table_row["ZK Path"] = zk_path[:40] + "..." if len(zk_path) > 40 else zk_path

                zk_table.append(table_row)

            builder.table(zk_table)
            builder.blank()
        elif total_replicas > 0:
            builder.h4("ZooKeeper/ClickHouse Keeper Connection")
            builder.note(
                "ℹ️ ZooKeeper/Keeper connection information not available. "
                "This may indicate that system.zookeeper_connection table is not accessible."
            )
            builder.blank()

        # 6. Display critical replica issues
        if readonly_replicas:
            builder.h4("🔴 Read-Only Replicas")
            builder.critical(
                f"**{len(readonly_replicas)} replica(s) in read-only mode**\n\n"
                "Read-only replicas cannot accept writes and indicate synchronization issues."
            )
            readonly_table = []
            for replica in readonly_replicas:
                readonly_table.append({
                    "Database": replica['database'],
                    "Table": replica['table'],
                    "Is Leader": "✅" if replica['is_leader'] else "",
                    "Queue Size": replica['queue_size'],
                    "Active/Total": f"{replica['active_replicas']}/{replica['total_replicas']}"
                })
            builder.table(readonly_table)
            builder.blank()

        if session_expired:
            builder.h4("🔴 Session Expired Replicas")
            builder.critical(
                f"**{len(session_expired)} replica(s) with expired sessions**\n\n"
                "Expired sessions indicate connectivity issues with ZooKeeper/ClickHouse Keeper."
            )
            expired_table = []
            for replica in session_expired:
                expired_table.append({
                    "Database": replica['database'],
                    "Table": replica['table'],
                    "Queue Size": replica['queue_size'],
                    "Log Delay": replica['log_delay']
                })
            builder.table(expired_table)
            builder.blank()

        # 6. Display replication lag warnings
        if lagging_replicas:
            builder.h4("⚠️ Lagging Replicas")
            builder.warning(
                f"**{len(lagging_replicas)} replica(s) with replication lag**\n\n"
                "These replicas are behind in processing the replication log."
            )
            lag_table = []
            for replica in lagging_replicas[:10]:  # Show top 10
                lag_table.append({
                    "Database": replica['database'],
                    "Table": replica['table'],
                    "Log Delay": replica['log_delay'],
                    "Queue Size": replica['queue_size'],
                    "Active/Total": f"{replica['active_replicas']}/{replica['total_replicas']}"
                })
            builder.table(lag_table)
            if len(lagging_replicas) > 10:
                builder.para(f"...and {len(lagging_replicas) - 10} more lagging replicas")
            builder.blank()

        # 7. Replica summary
        if total_replicas > 0:
            builder.h4("Replication Summary")
            summary_data = [
                {"Metric": "Total Replicated Tables", "Value": total_replicas},
                {"Metric": "Read-Only Replicas", "Value": f"{len(readonly_replicas)} 🔴" if readonly_replicas else f"{len(readonly_replicas)}"},
                {"Metric": "Expired Sessions", "Value": f"{len(session_expired)} 🔴" if session_expired else f"{len(session_expired)}"},
                {"Metric": "Lagging Replicas", "Value": f"{len(lagging_replicas)} ⚠️" if lagging_replicas else f"{len(lagging_replicas)}"}
            ]
            builder.table(summary_data)
            builder.blank()

        # 8. Recommendations
        recommendations = _generate_recommendations(
            readonly_replicas,
            session_expired,
            lagging_replicas,
            zk_expired_sessions,
            total_replicas
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        elif total_replicas > 0:
            builder.success("✅ All replicas are healthy and synchronized.")
        else:
            builder.note(
                "ℹ️ **No Replicated Tables**\n\n"
                "This cluster does not have any replicated tables configured. "
                "Consider using ReplicatedMergeTree for high availability."
            )

        # 9. Structured data
        structured_data["cluster_health"] = {
            "status": "success",
            "clusters": len(clusters),
            "total_replicas": total_replicas,
            "readonly_replicas": len(readonly_replicas),
            "expired_sessions": len(session_expired),
            "lagging_replicas": len(lagging_replicas),
            "zk_connections": len(zk_connection_data),
            "zk_expired_sessions": len(zk_expired_sessions),
            "critical_issues": critical_issues,
            "warnings": warning_issues
        }

        # Include ZooKeeper connection data
        if zk_connection_data:
            structured_data["zookeeper_connection"] = {
                "status": "success",
                "data": [
                    {
                        "name": zk['name'],
                        "host": zk['host'],
                        "port": zk['port'],
                        "is_expired": zk['is_expired'],
                        "session_uptime_seconds": zk['session_uptime'],
                        # zookeeper_path may not exist in ClickHouse 25.x or managed services
                        "zookeeper_path": zk.get('zookeeper_path', 'N/A')
                    }
                    for zk in zk_connection_data
                ],
                "metadata": {
                    "total_connections": len(zk_connection_data),
                    "expired_sessions": len(zk_expired_sessions),
                    "timestamp": connector.get_current_timestamp()
                }
            }

    except Exception as e:
        logger.error(f"Cluster health check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["cluster_health"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _generate_recommendations(readonly_replicas, session_expired, lagging_replicas, zk_expired_sessions, total_replicas):
    """Generate recommendations based on cluster health."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if readonly_replicas:
        recs["critical"].extend([
            "Investigate read-only replicas immediately - check ZooKeeper/Keeper connectivity",
            "Review system.replication_queue for stuck operations",
            "Check disk space and permissions on read-only replica nodes"
        ])

    if session_expired:
        recs["critical"].extend([
            "Restore ZooKeeper/ClickHouse Keeper connectivity for expired sessions",
            "Check network connectivity between ClickHouse nodes and Keeper ensemble",
            "Review Keeper logs for session timeout issues"
        ])

    if zk_expired_sessions:
        recs["critical"].extend([
            f"{len(zk_expired_sessions)} ZooKeeper/Keeper session(s) expired - immediate action required",
            "Check ZooKeeper/ClickHouse Keeper service health and availability",
            "Verify network connectivity between ClickHouse and coordination service",
            "Review Keeper logs for session expiration causes",
            "Check if Keeper ensemble has sufficient resources and quorum"
        ])

    if lagging_replicas:
        recs["high"].extend([
            "Monitor replication lag - may indicate overloaded replicas or network issues",
            "Check if replica nodes have sufficient resources (CPU, memory, network bandwidth)",
            "Review system.replication_queue to identify bottlenecks"
        ])

    if total_replicas > 0:
        recs["general"].extend([
            "Regularly monitor system.replicas for early warning signs",
            "Set up alerting for is_readonly and is_session_expired flags",
            "Monitor log_delay and queue_size metrics for replication health",
            "Ensure adequate resources on all replica nodes for consistent performance"
        ])
    else:
        recs["general"].extend([
            "Consider using ReplicatedMergeTree engine for critical tables",
            "Configure at least 2 replicas per shard for high availability",
            "Set up ZooKeeper or ClickHouse Keeper for distributed coordination"
        ])

    return recs

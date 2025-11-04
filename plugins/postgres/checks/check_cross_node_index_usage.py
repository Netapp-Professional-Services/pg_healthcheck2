"""
Cross-Node Index Usage Analysis

This check analyzes index usage patterns across primary and replica nodes in
PostgreSQL clusters (especially Aurora) to identify:
1. Unused indexes that can be safely dropped (unused on all nodes)
2. Indexes used only on replicas (good - reads properly offloaded)
3. Indexes used on primary (warning - reads may be hitting writer)
4. Multi-node usage patterns (indexes used on multiple nodes)

This analysis is critical for Aurora and other read-replica architectures where
read traffic should ideally be directed to replicas, not the primary writer.
"""

from plugins.postgres.utils.qrylib.check_cross_node_index_usage_qry import (
    get_cross_node_index_usage_query,
    get_constraint_check_query
)

def get_weight():
    """
    Returns the importance score for this module.

    Cross-node index analysis is highly valuable for Aurora/replica setups
    as it can identify significant optimization opportunities and unused indexes
    that are safe to drop.
    """
    return 9

def run_check_cross_node_index_usage(connector, settings):
    """
    Performs cross-node index usage analysis across all cluster nodes.

    Args:
        connector: Database connector with cross-node query support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    adoc_content = []
    structured_data = {}

    # Check if connector supports cross-node queries
    if not hasattr(connector, 'supports_cross_node_queries') or not connector.supports_cross_node_queries():
        adoc_content.append("=== Cross-Node Index Usage Analysis")
        adoc_content.append("[NOTE]")
        adoc_content.append("====")
        adoc_content.append("Cross-node index usage analysis is not available for this database configuration.")
        adoc_content.append("This feature requires an Aurora or multi-node PostgreSQL cluster with `connect_to_replicas: true` in the configuration.")
        adoc_content.append("====\n")
        return "\n".join(adoc_content), {"status": "skipped", "reason": "cross_node_not_supported"}

    # Check if replicas are actually connected
    if not hasattr(connector, 'replica_conns') or not connector.replica_conns:
        adoc_content.append("=== Cross-Node Index Usage Analysis")
        adoc_content.append("[NOTE]")
        adoc_content.append("====")
        adoc_content.append("Cross-node index usage analysis requires connections to replica nodes.")
        adoc_content.append("Set `connect_to_replicas: true` in your configuration to enable this analysis.")
        adoc_content.append("====\n")
        return "\n".join(adoc_content), {"status": "skipped", "reason": "no_replicas_connected"}

    adoc_content.append("=== Cross-Node Index Usage Analysis")
    adoc_content.append("Analyzes index usage patterns across primary and replica nodes to identify optimization opportunities and unused indexes.\n")

    try:
        # Get cluster topology info
        topology_info = []
        if hasattr(connector, 'cluster_topology'):
            for node in connector.cluster_topology:
                if node['endpoint_type'] == 'instance':
                    topology_info.append(f"{node['role'].title()}: {node['host']}")

        if topology_info:
            adoc_content.append("==== Cluster Topology")
            adoc_content.append("[cols=\"1\",options=\"header\"]")
            adoc_content.append("|===")
            adoc_content.append("|Cluster Nodes")
            for info in topology_info:
                adoc_content.append(f"|{info}")
            adoc_content.append("|===\n")

        # Execute index usage query on all nodes
        index_usage_query = get_cross_node_index_usage_query()

        try:
            results = connector.execute_on_all_nodes(index_usage_query, include_replicas=True)
        except Exception as e:
            adoc_content.append(f"[ERROR]\n====\nFailed to execute cross-node query: {e}\n====\n")
            return "\n".join(adoc_content), {"status": "error", "error": str(e)}

        # Parse results
        primary_indexes = {}
        replica_indexes = {}

        # Process primary results
        if 'primary' in results:
            for row in results['primary']:
                schema, table_name, index_name, idx_scan, idx_tup_read, idx_tup_fetch, index_size, index_size_bytes = row
                primary_indexes[index_name] = {
                    'schema': schema,
                    'table_name': table_name,
                    'idx_scan': idx_scan or 0,
                    'idx_tup_read': idx_tup_read or 0,
                    'idx_tup_fetch': idx_tup_fetch or 0,
                    'index_size': index_size,
                    'index_size_bytes': index_size_bytes or 0
                }

        # Process replica results
        for node_host, node_results in results.items():
            if node_host == 'primary':
                continue
            for row in node_results:
                schema, table_name, index_name, idx_scan, idx_tup_read, idx_tup_fetch, index_size, index_size_bytes = row
                if index_name not in replica_indexes:
                    replica_indexes[index_name] = {
                        'schema': schema,
                        'table_name': table_name,
                        'total_scans': 0,
                        'nodes_used': [],
                        'index_size': index_size,
                        'index_size_bytes': index_size_bytes or 0
                    }
                if idx_scan and idx_scan > 0:
                    replica_indexes[index_name]['total_scans'] += idx_scan
                    replica_indexes[index_name]['nodes_used'].append(node_host)

        # Analyze patterns
        unused_everywhere = []
        used_only_on_replicas = []
        used_on_primary = []
        used_multi_node = []

        all_indexes = set(primary_indexes.keys())

        for index_name in all_indexes:
            primary_scans = primary_indexes.get(index_name, {}).get('idx_scan', 0)
            replica_scans = replica_indexes.get(index_name, {}).get('total_scans', 0)

            if primary_scans == 0 and replica_scans == 0:
                # Unused everywhere - candidate for removal
                unused_everywhere.append({
                    'index_name': index_name,
                    'table_name': primary_indexes[index_name]['table_name'],
                    'index_size': primary_indexes[index_name]['index_size'],
                    'index_size_bytes': primary_indexes[index_name]['index_size_bytes']
                })
            elif primary_scans == 0 and replica_scans > 0:
                # Used only on replicas - GOOD pattern
                used_only_on_replicas.append({
                    'index_name': index_name,
                    'table_name': primary_indexes[index_name]['table_name'],
                    'replica_scans': replica_scans,
                    'nodes_used': len(replica_indexes[index_name]['nodes_used']),
                    'index_size': primary_indexes[index_name]['index_size']
                })
            elif primary_scans > 0 and replica_scans == 0:
                # Used only on primary - WARNING
                used_on_primary.append({
                    'index_name': index_name,
                    'table_name': primary_indexes[index_name]['table_name'],
                    'primary_scans': primary_scans,
                    'index_size': primary_indexes[index_name]['index_size']
                })
            elif primary_scans > 0 and replica_scans > 0:
                # Used on both - may indicate suboptimal routing
                used_multi_node.append({
                    'index_name': index_name,
                    'table_name': primary_indexes[index_name]['table_name'],
                    'primary_scans': primary_scans,
                    'replica_scans': replica_scans,
                    'nodes_used': len(replica_indexes[index_name]['nodes_used']),
                    'index_size': primary_indexes[index_name]['index_size']
                })

        structured_data['unused_everywhere'] = unused_everywhere
        structured_data['used_only_on_replicas'] = used_only_on_replicas
        structured_data['used_on_primary'] = used_on_primary
        structured_data['used_multi_node'] = used_multi_node

        # Generate report sections

        # 1. Unused Indexes (can be dropped)
        adoc_content.append("==== Unused Indexes (All Nodes)")
        if unused_everywhere:
            total_wasted_bytes = sum(idx['index_size_bytes'] for idx in unused_everywhere)
            total_wasted_mb = total_wasted_bytes / (1024 * 1024)

            adoc_content.append("[IMPORTANT]")
            adoc_content.append("====")
            adoc_content.append(f"**Found {len(unused_everywhere)} indexes unused on all nodes.**")
            adoc_content.append(f"")
            adoc_content.append(f"These indexes are not used on the primary or any replica and can be safely dropped.")
            adoc_content.append(f"Potential storage savings: **{total_wasted_mb:.1f} MB**")
            adoc_content.append("====\n")

            adoc_content.append("[cols=\"2,2,1\",options=\"header\"]")
            adoc_content.append("|===")
            adoc_content.append("|Index Name|Table Name|Size")
            for idx in unused_everywhere[:settings.get('row_limit', 20)]:
                adoc_content.append(f"|`{idx['index_name']}`|{idx['table_name']}|{idx['index_size']}")
            adoc_content.append("|===\n")

            if len(unused_everywhere) > settings.get('row_limit', 20):
                adoc_content.append(f"_Showing {settings.get('row_limit', 20)} of {len(unused_everywhere)} unused indexes._\n")
        else:
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append("No unused indexes found across all nodes.")
            adoc_content.append("====\n")

        # 2. Replica-Only Indexes (GOOD pattern)
        adoc_content.append("==== Indexes Used Only on Replicas")
        if used_only_on_replicas:
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append(f"**Found {len(used_only_on_replicas)} indexes used only on read replicas.**")
            adoc_content.append("")
            adoc_content.append("This is the **optimal pattern** - read queries are properly offloaded to replicas.")
            adoc_content.append("====\n")

            adoc_content.append("[cols=\"2,2,1,1\",options=\"header\"]")
            adoc_content.append("|===")
            adoc_content.append("|Index Name|Table Name|Replica Scans|Replicas Using")
            for idx in used_only_on_replicas[:settings.get('row_limit', 20)]:
                adoc_content.append(f"|`{idx['index_name']}`|{idx['table_name']}|{idx['replica_scans']:,}|{idx['nodes_used']}")
            adoc_content.append("|===\n")

            if len(used_only_on_replicas) > settings.get('row_limit', 20):
                adoc_content.append(f"_Showing {settings.get('row_limit', 20)} of {len(used_only_on_replicas)} replica-only indexes._\n")
        else:
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append("No indexes found that are used exclusively on replicas.")
            adoc_content.append("====\n")

        # 3. Primary-Only Indexes (WARNING)
        adoc_content.append("==== Indexes Used Only on Primary")
        if used_on_primary:
            adoc_content.append("[WARNING]")
            adoc_content.append("====")
            adoc_content.append(f"**Found {len(used_on_primary)} indexes used only on the primary (writer) node.**")
            adoc_content.append("")
            adoc_content.append("This may indicate that read queries are hitting the primary instead of replicas.")
            adoc_content.append("Consider reviewing application connection strings and routing logic.")
            adoc_content.append("====\n")

            adoc_content.append("[cols=\"2,2,1\",options=\"header\"]")
            adoc_content.append("|===")
            adoc_content.append("|Index Name|Table Name|Primary Scans")
            for idx in used_on_primary[:settings.get('row_limit', 20)]:
                adoc_content.append(f"|`{idx['index_name']}`|{idx['table_name']}|{idx['primary_scans']:,}")
            adoc_content.append("|===\n")

            if len(used_on_primary) > settings.get('row_limit', 20):
                adoc_content.append(f"_Showing {settings.get('row_limit', 20)} of {len(used_on_primary)} primary-only indexes._\n")
        else:
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append("No indexes found that are used exclusively on the primary.")
            adoc_content.append("====\n")

        # 4. Multi-Node Indexes
        adoc_content.append("==== Indexes with Multi-Node Usage")
        if used_multi_node:
            adoc_content.append("[WARNING]")
            adoc_content.append("====")
            adoc_content.append(f"**Found {len(used_multi_node)} indexes used on both primary and replicas.**")
            adoc_content.append("")
            adoc_content.append("While some multi-node usage is expected, high primary usage may indicate suboptimal read routing.")
            adoc_content.append("Review which queries are hitting the primary and consider routing them to reader endpoints.")
            adoc_content.append("====\n")

            adoc_content.append("[cols=\"2,2,1,1,1\",options=\"header\"]")
            adoc_content.append("|===")
            adoc_content.append("|Index Name|Table Name|Primary Scans|Replica Scans|Replicas Using")
            for idx in used_multi_node[:settings.get('row_limit', 20)]:
                adoc_content.append(f"|`{idx['index_name']}`|{idx['table_name']}|{idx['primary_scans']:,}|{idx['replica_scans']:,}|{idx['nodes_used']}")
            adoc_content.append("|===\n")

            if len(used_multi_node) > settings.get('row_limit', 20):
                adoc_content.append(f"_Showing {settings.get('row_limit', 20)} of {len(used_multi_node)} multi-node indexes._\n")
        else:
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append("No indexes found with usage on both primary and replicas.")
            adoc_content.append("====\n")

        # Recommendations
        adoc_content.append("==== Recommendations")
        recommendations = []

        if unused_everywhere:
            recommendations.append(f"**Drop {len(unused_everywhere)} unused indexes** to free up storage and reduce write overhead. Use `DROP INDEX CONCURRENTLY` to avoid locking.")

        if used_only_on_replicas:
            recommendations.append(f"**Good pattern detected:** {len(used_only_on_replicas)} indexes are used exclusively on replicas, indicating proper read offloading.")

        if used_on_primary:
            recommendations.append(f"**Review read routing:** {len(used_on_primary)} indexes are used only on the primary. Consider routing read queries to replica endpoints.")

        if used_multi_node:
            recommendations.append(f"**Analyze multi-node usage:** {len(used_multi_node)} indexes are used on both primary and replicas. Review if reads should be redirected from primary to replicas.")

        if recommendations:
            for rec in recommendations:
                adoc_content.append(f"* {rec}")
        else:
            adoc_content.append("* All indexes show optimal usage patterns.")

        adoc_content.append("")
        structured_data['status'] = 'success'

    except Exception as e:
        adoc_content.append(f"\n[ERROR]")
        adoc_content.append("====")
        adoc_content.append(f"Error during cross-node index usage analysis: {e}")
        adoc_content.append("====\n")
        structured_data['status'] = 'error'
        structured_data['error'] = str(e)

    return "\n".join(adoc_content), structured_data

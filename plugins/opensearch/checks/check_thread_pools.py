"""
Thread pool analysis for Elasticsearch/OpenSearch clusters.

This check analyzes thread pool status to identify:
- Thread pool queue buildup
- Rejected requests
- Thread pool capacity issues
- Per-pool health status

Data source:
- Live clusters: GET /_cat/thread_pool
- Imported diagnostics: cat/cat_thread_pool.txt

Thread pool issues can indicate capacity problems or slow queries.
"""

from plugins.common.check_helpers import CheckContentBuilder


# Thread pools that are most critical to monitor
CRITICAL_POOLS = {
    "search": "Search query execution",
    "write": "Indexing/bulk operations",
    "get": "Get document operations",
    "bulk": "Bulk indexing (legacy)",
    "index": "Single document indexing (legacy)"
}

IMPORTANT_POOLS = {
    "analyze": "Text analysis",
    "fetch_shard_started": "Shard startup",
    "flush": "Index flush operations",
    "force_merge": "Segment merge operations",
    "generic": "Generic operations",
    "listener": "Transport listener",
    "management": "Cluster management",
    "refresh": "Index refresh",
    "snapshot": "Snapshot operations",
    "warmer": "Index warming"
}


def run(connector, settings):
    """
    Check thread pool status across cluster.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Thread Pool Status")

    try:
        # Get thread pool data
        thread_pool_data = connector.execute_query({"operation": "cat_thread_pool"})

        if isinstance(thread_pool_data, dict) and "error" in thread_pool_data:
            builder.note(
                "Thread pool information is not available.\n\n"
                "This may indicate the diagnostic export did not include cat/cat_thread_pool.txt"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": thread_pool_data.get("error", "Unknown")
            }

        if not thread_pool_data or not isinstance(thread_pool_data, list):
            builder.note("No thread pool data available.")
            return builder.build(), {
                "status": "success",
                "data": {"pool_count": 0}
            }

        # Analyze thread pools
        pools_by_node = {}
        pools_with_rejections = []
        pools_with_queued = []
        all_pools = set()

        for entry in thread_pool_data:
            node = entry.get("node_name", entry.get("node", "Unknown"))
            pool = entry.get("name", "Unknown")
            active = int(entry.get("active", 0))
            queue = int(entry.get("queue", 0))
            rejected = int(entry.get("rejected", 0))

            all_pools.add(pool)

            if node not in pools_by_node:
                pools_by_node[node] = []

            pool_info = {
                "name": pool,
                "active": active,
                "queue": queue,
                "rejected": rejected,
                "is_critical": pool in CRITICAL_POOLS
            }
            pools_by_node[node].append(pool_info)

            if rejected > 0:
                pools_with_rejections.append({
                    "node": node,
                    "pool": pool,
                    "rejected": rejected,
                    "is_critical": pool in CRITICAL_POOLS
                })

            if queue > 0:
                pools_with_queued.append({
                    "node": node,
                    "pool": pool,
                    "queue": queue,
                    "is_critical": pool in CRITICAL_POOLS
                })

        # Build report
        total_rejections = sum(p["rejected"] for p in pools_with_rejections)
        critical_rejections = [p for p in pools_with_rejections if p["is_critical"]]

        if critical_rejections:
            builder.critical(
                f"**{len(critical_rejections)} critical thread pool(s) have rejections**\n\n"
                "Rejected requests indicate the cluster cannot keep up with the request rate."
            )
        elif pools_with_rejections:
            builder.warning(
                f"**{len(pools_with_rejections)} thread pool(s) have rejections**\n\n"
                "Some requests have been rejected due to queue overflow."
            )
        else:
            builder.success("**No thread pool rejections detected**")

        builder.blank()

        # Critical pools table
        builder.h4("Critical Thread Pools")

        critical_table = []
        for node, pools in pools_by_node.items():
            for pool in pools:
                if pool["name"] in CRITICAL_POOLS:
                    status = "✅"
                    if pool["rejected"] > 0:
                        status = "🔴 Rejections"
                    elif pool["queue"] > 10:
                        status = "⚠️ Queued"

                    critical_table.append({
                        "Node": node,
                        "Pool": pool["name"],
                        "Active": str(pool["active"]),
                        "Queue": str(pool["queue"]),
                        "Rejected": str(pool["rejected"]),
                        "Status": status
                    })

        if critical_table:
            builder.table(critical_table)
        else:
            builder.text("No critical pool data available.")
        builder.blank()

        # Rejections detail
        if pools_with_rejections:
            builder.h4("Pools with Rejections")

            # Sort by rejection count
            sorted_rejections = sorted(pools_with_rejections, key=lambda x: x["rejected"], reverse=True)

            rej_table = []
            for p in sorted_rejections[:15]:
                pool_desc = CRITICAL_POOLS.get(p["pool"], IMPORTANT_POOLS.get(p["pool"], ""))
                rej_table.append({
                    "Node": p["node"],
                    "Pool": p["pool"],
                    "Rejected": str(p["rejected"]),
                    "Description": pool_desc
                })
            builder.table(rej_table)

            if len(sorted_rejections) > 15:
                builder.text(f"_... and {len(sorted_rejections) - 15} more pools with rejections_")
            builder.blank()

        # Queue buildup
        significant_queues = [p for p in pools_with_queued if p["queue"] > 5]
        if significant_queues:
            builder.h4("Pools with Queue Buildup")

            sorted_queues = sorted(significant_queues, key=lambda x: x["queue"], reverse=True)

            queue_table = []
            for p in sorted_queues[:10]:
                queue_table.append({
                    "Node": p["node"],
                    "Pool": p["pool"],
                    "Queue Size": str(p["queue"]),
                    "Critical": "Yes" if p["is_critical"] else "No"
                })
            builder.table(queue_table)
            builder.blank()

        # Recommendations
        if pools_with_rejections or significant_queues:
            builder.text("*Recommended Actions:*")
            if any(p["pool"] == "search" for p in pools_with_rejections):
                builder.text("- **Search rejections:** Add search thread pool capacity or optimize queries")
            if any(p["pool"] == "write" for p in pools_with_rejections):
                builder.text("- **Write rejections:** Reduce indexing rate or add more nodes")
            if any(p["pool"] == "bulk" or p["pool"] == "index" for p in pools_with_rejections):
                builder.text("- **Indexing rejections:** Use smaller bulk sizes or add indexing capacity")
            builder.text("- Monitor cluster during peak hours to identify patterns")
            builder.text("- Consider scaling horizontally by adding more nodes")
            builder.blank()

        builder.text("*Thread Pool Reference:*")
        builder.text("- **search:** Handles search queries")
        builder.text("- **write:** Handles indexing, update, delete operations")
        builder.text("- **get:** Handles get-by-ID operations")
        builder.text("- **snapshot:** Handles snapshot/restore operations")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "node_count": len(pools_by_node),
                "pool_count": len(all_pools),
                "total_rejections": total_rejections,
                "pools_with_rejections": len(pools_with_rejections),
                "critical_pools_with_rejections": len(critical_rejections),
                "pools_with_queue_buildup": len(significant_queues),
                "has_rejections": total_rejections > 0,
                "has_critical_rejections": len(critical_rejections) > 0,
                "has_queue_buildup": len(significant_queues) > 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to analyze thread pools: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

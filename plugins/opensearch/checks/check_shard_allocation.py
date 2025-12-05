"""
Shard allocation analysis for Elasticsearch/OpenSearch clusters.

This check analyzes shard allocation to identify:
- Unassigned shards and reasons for non-allocation
- Disk-based allocation issues
- Node capacity and distribution
- Allocation filtering rules
- Shard recovery status and throttling

Data source:
- Live clusters: GET /_cluster/allocation/explain, GET /_cat/allocation, GET /_cat/recovery
- Imported diagnostics: allocation_explain.json, allocation.json, cat/cat_allocation.txt,
                        cat/cat_recovery.txt, recovery.json

Shard allocation issues are a common cause of cluster problems.
"""

import logging

logger = logging.getLogger(__name__)

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check shard allocation status and issues.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Shard Allocation Analysis")

    try:
        # Get allocation data
        allocation_explain = connector.execute_query({"operation": "allocation_explain"})
        allocation_data = connector.execute_query({"operation": "allocation"})
        cat_allocation = connector.execute_query({"operation": "cat_allocation"})

        explain_available = not (isinstance(allocation_explain, dict) and "error" in allocation_explain)
        alloc_available = not (isinstance(allocation_data, dict) and "error" in allocation_data)
        cat_available = isinstance(cat_allocation, list) and len(cat_allocation) > 0

        # Parse cat allocation for disk usage per node
        node_allocations = []
        if cat_available:
            for node in cat_allocation:
                node_allocations.append({
                    "node": node.get("node", "Unknown"),
                    "shards": int(node.get("shards", 0)),
                    "disk_indices": node.get("disk.indices", "0b"),
                    "disk_used": node.get("disk.used", "0b"),
                    "disk_avail": node.get("disk.avail", "0b"),
                    "disk_total": node.get("disk.total", "0b"),
                    "disk_percent": int(node.get("disk.percent", 0)),
                    "host": node.get("host", ""),
                    "ip": node.get("ip", "")
                })

        # Check for allocation explain (unassigned shards)
        has_unassigned = False
        unassigned_reason = None
        unassigned_details = {}

        if explain_available and allocation_explain:
            # If we get a valid response, there's an unassigned shard
            if "index" in allocation_explain:
                has_unassigned = True
                unassigned_details = {
                    "index": allocation_explain.get("index"),
                    "shard": allocation_explain.get("shard"),
                    "primary": allocation_explain.get("primary", False),
                    "current_state": allocation_explain.get("current_state"),
                    "unassigned_info": allocation_explain.get("unassigned_info", {})
                }
                unassigned_reason = unassigned_details["unassigned_info"].get("reason", "Unknown")

        # Build report
        if node_allocations:
            builder.h4("Node Disk Allocation")

            # Check for disk pressure
            high_disk_nodes = [n for n in node_allocations if n["disk_percent"] > 80]
            critical_disk_nodes = [n for n in node_allocations if n["disk_percent"] > 90]

            if critical_disk_nodes:
                builder.critical(
                    f"**{len(critical_disk_nodes)} node(s) with critical disk usage (>90%)**\n\n"
                    "Elasticsearch will stop allocating shards to nodes above the high watermark (default 90%)."
                )
            elif high_disk_nodes:
                builder.warning(
                    f"**{len(high_disk_nodes)} node(s) with high disk usage (>80%)**\n\n"
                    "Approaching disk watermarks. Elasticsearch may start relocating shards."
                )
            else:
                builder.success("**Disk allocation healthy across all nodes**")

            builder.blank()

            # Allocation table
            alloc_table = []
            for node in node_allocations:
                disk_pct = node["disk_percent"]
                if disk_pct > 90:
                    status = f"{disk_pct}% 🔴"
                elif disk_pct > 80:
                    status = f"{disk_pct}% ⚠️"
                else:
                    status = f"{disk_pct}% ✅"

                alloc_table.append({
                    "Node": node["node"],
                    "Shards": str(node["shards"]),
                    "Index Data": node["disk_indices"],
                    "Disk Used": node["disk_used"],
                    "Disk Available": node["disk_avail"],
                    "Disk %": status
                })
            builder.table(alloc_table)
            builder.blank()

        # Unassigned shard analysis
        if has_unassigned:
            builder.h4("Unassigned Shard Analysis")
            builder.critical(
                f"**Unassigned shard detected**\n\n"
                f"Index: {unassigned_details.get('index')}, "
                f"Shard: {unassigned_details.get('shard')}, "
                f"Primary: {unassigned_details.get('primary')}"
            )

            reason_explanations = {
                "INDEX_CREATED": "Shard created with index but not yet allocated",
                "CLUSTER_RECOVERED": "Shard from recovered cluster state",
                "INDEX_REOPENED": "Shard from reopened index",
                "DANGLING_INDEX_IMPORTED": "Shard from imported dangling index",
                "NEW_INDEX_RESTORED": "Shard from restored snapshot",
                "EXISTING_INDEX_RESTORED": "Shard from restored snapshot into existing index",
                "REPLICA_ADDED": "Replica shard added",
                "ALLOCATION_FAILED": "Previous allocation attempt failed",
                "NODE_LEFT": "Shard's node left the cluster",
                "REROUTE_CANCELLED": "Allocation was cancelled",
                "REINITIALIZED": "Shard was reinitialized",
                "REALLOCATED_REPLICA": "Replica reallocated",
                "PRIMARY_FAILED": "Primary shard failed",
                "FORCED_EMPTY_PRIMARY": "Forced empty primary allocation",
                "MANUAL_ALLOCATION": "Manual allocation command"
            }

            builder.text(f"**Reason:** {unassigned_reason}")
            if unassigned_reason in reason_explanations:
                builder.text(f"**Explanation:** {reason_explanations[unassigned_reason]}")
            builder.blank()
        else:
            if explain_available:
                builder.h4("Shard Assignment Status")
                builder.success("**All shards are assigned**")
                builder.blank()

        # Shard distribution analysis
        if node_allocations and len(node_allocations) > 1:
            shard_counts = [n["shards"] for n in node_allocations]
            if shard_counts:
                min_shards = min(shard_counts)
                max_shards = max(shard_counts)
                imbalance = max_shards - min_shards

                builder.h4("Shard Distribution")
                if imbalance > 10:
                    builder.warning(
                        f"**Shard imbalance detected** (difference: {imbalance} shards)\n\n"
                        f"Min: {min_shards}, Max: {max_shards}"
                    )
                else:
                    builder.text(f"Shard distribution is balanced (min: {min_shards}, max: {max_shards})")
                builder.blank()

        # Recommendations
        if has_unassigned or any(n["disk_percent"] > 80 for n in node_allocations):
            builder.text("*Recommended Actions:*")
            if has_unassigned:
                builder.text("1. Check cluster allocation explain API for detailed reasons")
                builder.text("2. Review cluster reroute API to manually allocate shards")
                builder.text("3. Check node availability and disk space")
            if any(n["disk_percent"] > 80 for n in node_allocations):
                builder.text("1. Add more disk capacity or add nodes")
                builder.text("2. Delete old indices or use ILM to manage retention")
                builder.text("3. Consider using index lifecycle management")
            builder.blank()

        builder.text("*Disk Watermark Reference:*")
        builder.text("- Low watermark (default 85%): Stop allocating new shards")
        builder.text("- High watermark (default 90%): Start relocating shards away")
        builder.text("- Flood stage (default 95%): Enforce read-only index blocks")
        builder.blank()

        # Analyze shard recovery status
        recovery_analysis = _analyze_recovery(connector, builder)

        # Build structured data
        high_disk_count = len([n for n in node_allocations if n["disk_percent"] > 80])
        critical_disk_count = len([n for n in node_allocations if n["disk_percent"] > 90])
        max_disk_pct = max([n["disk_percent"] for n in node_allocations]) if node_allocations else 0

        structured_data = {
            "status": "success",
            "data": {
                "node_count": len(node_allocations),
                "total_shards": sum(n["shards"] for n in node_allocations),
                "high_disk_usage_count": high_disk_count,
                "critical_disk_usage_count": critical_disk_count,
                "max_disk_percent": max_disk_pct,
                "has_unassigned_shards": has_unassigned,
                "unassigned_reason": unassigned_reason,
                "has_disk_pressure": high_disk_count > 0,
                "has_critical_disk": critical_disk_count > 0,
                # Recovery analysis signals
                "active_recoveries": recovery_analysis.get("active_count", 0),
                "slow_recoveries": recovery_analysis.get("slow_count", 0),
                "has_active_recovery": recovery_analysis.get("active_count", 0) > 0,
                "has_slow_recovery": recovery_analysis.get("slow_count", 0) > 0,
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to analyze shard allocation: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _analyze_recovery(connector, builder):
    """
    Analyze shard recovery status to identify slow or stuck recoveries.

    Returns:
        dict: Recovery analysis summary
    """
    result = {
        "active_count": 0,
        "slow_count": 0,
        "recoveries": []
    }

    try:
        # Try cat_recovery first
        cat_recovery = connector.execute_query({"operation": "cat_recovery"})

        if isinstance(cat_recovery, list) and cat_recovery:
            active_recoveries = []
            slow_recoveries = []

            for rec in cat_recovery:
                # Skip completed recoveries
                stage = rec.get("stage", "")
                if stage.lower() == "done":
                    continue

                index = rec.get("index", rec.get("i", "unknown"))
                shard = rec.get("shard", rec.get("s", "0"))
                source = rec.get("source_node", rec.get("snode", ""))
                target = rec.get("target_node", rec.get("tnode", ""))
                bytes_total = rec.get("bytes_total", rec.get("bt", "0"))
                bytes_recovered = rec.get("bytes_recovered", rec.get("br", "0"))
                time_str = rec.get("time", rec.get("t", "0"))

                recovery_info = {
                    "index": index,
                    "shard": shard,
                    "stage": stage,
                    "source": source,
                    "target": target,
                    "bytes_total": bytes_total,
                    "bytes_recovered": bytes_recovered,
                    "time": time_str
                }

                active_recoveries.append(recovery_info)

                # Check if recovery is slow (> 30 minutes)
                time_ms = _parse_time_to_ms(time_str)
                if time_ms > 30 * 60 * 1000:  # 30 minutes
                    slow_recoveries.append(recovery_info)

            result["active_count"] = len(active_recoveries)
            result["slow_count"] = len(slow_recoveries)
            result["recoveries"] = active_recoveries

            # Report active recoveries
            if active_recoveries:
                builder.h4("Active Shard Recoveries")

                if slow_recoveries:
                    builder.warning(
                        f"⚠️ **{len(slow_recoveries)} slow recoveries detected (>30 min)**\n\n"
                        "Slow recoveries may indicate I/O bottlenecks, network issues, or recovery throttling."
                    )
                else:
                    builder.note(f"**{len(active_recoveries)} shard recovery operation(s) in progress**")

                builder.blank()

                recovery_table = []
                for rec in active_recoveries[:10]:
                    recovery_table.append({
                        "Index": rec["index"][:30],
                        "Shard": rec["shard"],
                        "Stage": rec["stage"],
                        "Progress": f"{rec['bytes_recovered']}/{rec['bytes_total']}",
                        "Time": rec["time"],
                        "Target Node": rec["target"][:15] if rec["target"] else "N/A"
                    })
                builder.table(recovery_table)

                if len(active_recoveries) > 10:
                    builder.text(f"_...and {len(active_recoveries) - 10} more recoveries in progress_")
                builder.blank()

                if slow_recoveries:
                    builder.text("*Recommendations for slow recovery:*")
                    builder.text("1. Check node I/O and network throughput")
                    builder.text("2. Review recovery throttling settings:")
                    builder.text("   - `indices.recovery.max_bytes_per_sec` (default 40mb)")
                    builder.text("   - `cluster.routing.allocation.node_concurrent_recoveries`")
                    builder.text("3. Ensure source and target nodes are healthy")
                    builder.text("4. Consider temporarily increasing recovery bandwidth")
                    builder.blank()

        # Also try recovery.json for more detail
        recovery_json = connector.execute_query({"operation": "recovery"})
        if recovery_json and isinstance(recovery_json, dict) and "error" not in recovery_json:
            # recovery.json is keyed by index name
            for index_name, index_recoveries in recovery_json.items():
                if isinstance(index_recoveries, dict) and "shards" in index_recoveries:
                    for shard_recovery in index_recoveries["shards"]:
                        stage = shard_recovery.get("stage", "")
                        if stage.lower() != "done":
                            result["active_count"] += 1

    except Exception as e:
        logger.warning(f"Could not analyze recovery status: {e}")

    return result


def _parse_time_to_ms(time_str):
    """Parse time string like '5.2m' or '1.5h' to milliseconds."""
    if not time_str:
        return 0

    time_str = str(time_str).lower().strip()

    try:
        if time_str.endswith("h"):
            return float(time_str[:-1]) * 60 * 60 * 1000
        elif time_str.endswith("m"):
            return float(time_str[:-1]) * 60 * 1000
        elif time_str.endswith("s"):
            return float(time_str[:-1]) * 1000
        elif time_str.endswith("ms"):
            return float(time_str[:-2])
        else:
            return float(time_str)
    except (ValueError, TypeError):
        return 0

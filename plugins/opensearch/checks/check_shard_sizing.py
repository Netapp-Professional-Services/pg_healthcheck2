"""
Shard Sizing Analysis

Analyzes shard sizes and counts to identify:
- Oversized shards (>50GB) that impact recovery and rebalancing
- Undersized shards (<1GB) that waste cluster resources
- Too many shards per node (memory pressure)
- Uneven shard distribution

Data source:
- Live clusters: GET /_cat/shards, GET /_cat/indices
- Imported diagnostics: cat/cat_shards.txt, cat/cat_indices.txt, indices_stats.json

Best Practices:
- Target shard size: 10-50GB
- Max shards per node: ~20 per GB of heap (e.g., 600 for 30GB heap)
- Avoid > 1000 shards per node in any case

Why This Matters:
- Oversized shards:
  - Slow recovery (can take hours)
  - Expensive rebalancing operations
  - Risk of incomplete searches
- Too many small shards:
  - Wasted memory (each shard ~10MB overhead)
  - Slower queries (more shards to search)
  - Cluster state bloat
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


# Size thresholds in bytes
GB = 1024 * 1024 * 1024
SHARD_SIZE_MIN = 1 * GB       # 1 GB - below this is "small"
SHARD_SIZE_TARGET_MIN = 10 * GB  # 10 GB - ideal minimum
SHARD_SIZE_TARGET_MAX = 50 * GB  # 50 GB - ideal maximum
SHARD_SIZE_CRITICAL = 65 * GB    # 65 GB - critical threshold


def get_weight():
    """Returns the importance score for this module."""
    return 8  # Important for cluster performance


def run(connector, settings):
    """
    Analyze shard sizes and distribution for optimization opportunities.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Shard Sizing Analysis")

    try:
        # Get shard data from multiple sources
        shard_data = _get_shard_data(connector)
        node_data = _get_node_data(connector)

        if not shard_data:
            builder.note(
                "Shard data is not available.\n\n"
                "This may indicate:\n"
                "- No indices/shards exist in the cluster\n"
                "- Diagnostic export did not include shard data"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "no_shard_data"
            }

        # Analyze shard sizes
        analysis = _analyze_shards(shard_data, node_data)

        # Build summary
        builder.h4("Shard Overview")
        summary = [
            {"Metric": "Total Shards", "Value": str(analysis["total_shards"])},
            {"Metric": "Primary Shards", "Value": str(analysis["primary_count"])},
            {"Metric": "Replica Shards", "Value": str(analysis["replica_count"])},
            {"Metric": "Average Shard Size", "Value": _format_size(analysis["avg_shard_size"])},
            {"Metric": "Largest Shard", "Value": _format_size(analysis["max_shard_size"])},
        ]
        builder.table(summary)
        builder.blank()

        # Critical: Oversized shards
        if analysis["oversized_shards"]:
            builder.critical(
                f"🔴 **{len(analysis['oversized_shards'])} shards exceed 50GB**\n\n"
                "Oversized shards significantly impact:\n"
                "- Shard recovery time (can take hours)\n"
                "- Rebalancing performance\n"
                "- Memory pressure during merges"
            )
            builder.blank()

            oversized_table = []
            for shard in sorted(analysis["oversized_shards"], key=lambda x: x["size_bytes"], reverse=True)[:10]:
                oversized_table.append({
                    "Index": shard["index"][:35],
                    "Shard": shard["shard"],
                    "Type": "Primary" if shard["primary"] else "Replica",
                    "Size": _format_size(shard["size_bytes"]),
                    "Node": shard.get("node", "N/A")[:20]
                })
            builder.table(oversized_table)

            if len(analysis["oversized_shards"]) > 10:
                builder.text(f"_...and {len(analysis['oversized_shards']) - 10} more oversized shards_")
            builder.blank()

        # Warning: Very small shards
        if analysis["tiny_shard_indices"]:
            builder.warning(
                f"⚠️ **{len(analysis['tiny_shard_indices'])} indices have very small shards (<1GB)**\n\n"
                "Many small shards waste resources:\n"
                "- Each shard uses ~10-50MB memory overhead\n"
                "- More shards = slower searches\n"
                "- Cluster state bloat"
            )
            builder.blank()

            tiny_table = []
            for idx in analysis["tiny_shard_indices"][:10]:
                tiny_table.append({
                    "Index": idx["index"][:35],
                    "Shards": idx["shard_count"],
                    "Avg Size": _format_size(idx["avg_size"]),
                    "Total Size": _format_size(idx["total_size"]),
                    "Recommendation": idx["recommendation"]
                })
            builder.table(tiny_table)

            if len(analysis["tiny_shard_indices"]) > 10:
                builder.text(f"_...and {len(analysis['tiny_shard_indices']) - 10} more indices with tiny shards_")
            builder.blank()

        # Shards per node analysis
        if analysis["node_shard_counts"]:
            builder.h4("Shards Per Node")

            max_shards_per_node = max(analysis["node_shard_counts"].values()) if analysis["node_shard_counts"] else 0
            avg_shards_per_node = sum(analysis["node_shard_counts"].values()) / len(analysis["node_shard_counts"]) if analysis["node_shard_counts"] else 0

            if max_shards_per_node > 1000:
                builder.critical(
                    f"🔴 **{max_shards_per_node} shards on a single node exceeds recommended limit**\n\n"
                    "More than 1000 shards per node can cause:\n"
                    "- High heap memory usage\n"
                    "- Slow cluster state updates\n"
                    "- Increased GC pressure"
                )
            elif max_shards_per_node > 600:
                builder.warning(
                    f"⚠️ **{max_shards_per_node} shards per node is high**\n\n"
                    "Consider reducing shard count or adding nodes."
                )
            else:
                builder.success(f"✅ Shard distribution is healthy (max {max_shards_per_node} per node)")

            builder.blank()

            node_table = []
            for node, count in sorted(analysis["node_shard_counts"].items(), key=lambda x: x[1], reverse=True)[:10]:
                status = "⚠️" if count > 600 else "✅"
                node_table.append({
                    "Node": node[:30],
                    "Shard Count": count,
                    "Status": status
                })
            builder.table(node_table)
            builder.blank()

        # All healthy
        if not analysis["oversized_shards"] and not analysis["tiny_shard_indices"]:
            builder.success("✅ **Shard sizes are within recommended ranges**")
            builder.blank()

        # Recommendations
        if analysis["oversized_shards"] or analysis["tiny_shard_indices"]:
            builder.text("*Shard Sizing Best Practices:*")
            builder.text("- **Target size**: 10-50GB per shard")
            builder.text("- **Rollover strategy**: Use ILM to roll indices at target size")
            builder.text("- **Index patterns**: Configure appropriate primary shard count at index creation")
            builder.text("- **Shrink API**: Reduce shard count on closed indices")
            builder.text("- **Reindex**: Split or combine indices to achieve target shard sizes")
            builder.blank()

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "total_shards": analysis["total_shards"],
                "primary_count": analysis["primary_count"],
                "replica_count": analysis["replica_count"],
                "avg_shard_size_bytes": analysis["avg_shard_size"],
                "max_shard_size_bytes": analysis["max_shard_size"],
                "oversized_shard_count": len(analysis["oversized_shards"]),
                "tiny_shard_index_count": len(analysis["tiny_shard_indices"]),
                "max_shards_per_node": max(analysis["node_shard_counts"].values()) if analysis["node_shard_counts"] else 0,
                # Signals for rules engine
                "has_oversized_shards": len(analysis["oversized_shards"]) > 0,
                "has_too_many_small_shards": len(analysis["tiny_shard_indices"]) > 5,
                "has_shard_imbalance": analysis.get("shard_imbalance", False),
                "shard_optimization_opportunity": (
                    len(analysis["oversized_shards"]) > 0 or
                    len(analysis["tiny_shard_indices"]) > 10
                ),
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Failed to analyze shard sizing: {e}")
        builder.error(f"Failed to analyze shard sizing: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _get_shard_data(connector):
    """Get shard information from available sources."""
    shards = []

    # Try cat_shards first
    cat_shards = connector.execute_query({"operation": "cat_shards"})
    if cat_shards and isinstance(cat_shards, list):
        for shard in cat_shards:
            size_str = shard.get("store") or shard.get("size", "0")
            shards.append({
                "index": shard.get("index", shard.get("i", "unknown")),
                "shard": shard.get("shard", shard.get("s", "0")),
                "primary": shard.get("prirep", shard.get("p", "r")) == "p",
                "state": shard.get("state", "UNKNOWN"),
                "size_bytes": _parse_size(size_str),
                "node": shard.get("node", shard.get("n", ""))
            })
    elif isinstance(cat_shards, dict) and "error" not in cat_shards:
        # Handle dict format
        pass

    # If no cat_shards, try indices_stats
    if not shards:
        indices_stats = connector.execute_query({"operation": "index_stats"})
        if indices_stats and isinstance(indices_stats, dict) and "indices" in indices_stats:
            for idx_name, idx_data in indices_stats.get("indices", {}).items():
                primaries = idx_data.get("primaries", {})
                total = idx_data.get("total", {})

                store = primaries.get("store", {})
                primary_size = store.get("size_in_bytes", 0)

                # Get shard count from _shards or assume 1
                shards_info = idx_data.get("_shards", {})
                total_shards = shards_info.get("total", 1)
                primary_count = max(1, total_shards // 2)  # Rough estimate

                # Create approximate shard entries
                avg_primary_size = primary_size // primary_count if primary_count > 0 else primary_size
                for i in range(primary_count):
                    shards.append({
                        "index": idx_name,
                        "shard": str(i),
                        "primary": True,
                        "state": "STARTED",
                        "size_bytes": avg_primary_size,
                        "node": "unknown"
                    })

    return shards


def _get_node_data(connector):
    """Get node information for shard-per-node analysis."""
    nodes = {}

    cat_nodes = connector.execute_query({"operation": "cat_nodes"})
    if cat_nodes and isinstance(cat_nodes, list):
        for node in cat_nodes:
            name = node.get("name") or node.get("n", "unknown")
            nodes[name] = {
                "name": name,
                "heap": node.get("heap.max") or node.get("hm", "unknown")
            }

    return nodes


def _analyze_shards(shards, nodes):
    """Analyze shard sizes and distribution."""
    analysis = {
        "total_shards": len(shards),
        "primary_count": 0,
        "replica_count": 0,
        "avg_shard_size": 0,
        "max_shard_size": 0,
        "oversized_shards": [],
        "tiny_shard_indices": [],
        "node_shard_counts": {},
        "shard_imbalance": False
    }

    if not shards:
        return analysis

    # Aggregate by index
    index_shards = {}
    total_size = 0

    for shard in shards:
        # Count primaries/replicas
        if shard["primary"]:
            analysis["primary_count"] += 1
        else:
            analysis["replica_count"] += 1

        # Track sizes
        size = shard.get("size_bytes", 0)
        total_size += size

        if size > analysis["max_shard_size"]:
            analysis["max_shard_size"] = size

        # Check for oversized
        if size > SHARD_SIZE_TARGET_MAX:
            analysis["oversized_shards"].append(shard)

        # Aggregate by index
        idx = shard["index"]
        if idx not in index_shards:
            index_shards[idx] = {"shards": [], "total_size": 0}
        index_shards[idx]["shards"].append(shard)
        index_shards[idx]["total_size"] += size

        # Count shards per node
        node = shard.get("node", "unknown")
        if node and node != "unknown":
            analysis["node_shard_counts"][node] = analysis["node_shard_counts"].get(node, 0) + 1

    # Calculate average
    analysis["avg_shard_size"] = total_size // len(shards) if shards else 0

    # Find indices with tiny shards
    for idx_name, idx_data in index_shards.items():
        if idx_name.startswith("."):  # Skip system indices
            continue

        shard_count = len(idx_data["shards"])
        total_size = idx_data["total_size"]
        avg_size = total_size // shard_count if shard_count > 0 else 0

        # Flag indices with small average shard size and multiple shards
        if avg_size < SHARD_SIZE_MIN and shard_count > 1:
            # Calculate recommended shard count
            recommended = max(1, total_size // SHARD_SIZE_TARGET_MIN)
            recommendation = f"Consider {recommended} shard(s)" if recommended < shard_count else "OK"

            analysis["tiny_shard_indices"].append({
                "index": idx_name,
                "shard_count": shard_count,
                "avg_size": avg_size,
                "total_size": total_size,
                "recommendation": recommendation
            })

    # Check for shard imbalance
    if analysis["node_shard_counts"]:
        counts = list(analysis["node_shard_counts"].values())
        if counts:
            max_count = max(counts)
            min_count = min(counts)
            if max_count > 0 and (max_count - min_count) / max_count > 0.3:
                analysis["shard_imbalance"] = True

    return analysis


def _parse_size(size_str):
    """Parse size string (e.g., '10gb', '500mb') to bytes."""
    if not size_str or size_str == "-":
        return 0

    size_str = str(size_str).lower().strip()

    try:
        if size_str.endswith("tb"):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024 * 1024)
        elif size_str.endswith("gb"):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
        elif size_str.endswith("mb"):
            return int(float(size_str[:-2]) * 1024 * 1024)
        elif size_str.endswith("kb"):
            return int(float(size_str[:-2]) * 1024)
        elif size_str.endswith("b"):
            return int(float(size_str[:-1]))
        else:
            return int(float(size_str))
    except (ValueError, TypeError):
        return 0


def _format_size(size_bytes):
    """Format bytes to human readable string."""
    if size_bytes >= 1024 * 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024 * 1024):.1f} TB"
    elif size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    elif size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes} B"

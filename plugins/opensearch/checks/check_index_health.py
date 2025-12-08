"""
OpenSearch Index Health Check

Monitors index health status, shard allocation, and storage efficiency.
Pure REST API check - works in all modes (AWS/self-hosted).

Requirements:
- REST API access only
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 9  # High priority - index health is critical


def run_check_index_health(connector, settings):
    """
    Monitor index health, shard allocation, and storage efficiency.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)

    builder.h3("Index Health & Shard Distribution")
    builder.para(
        "Analysis of index health status, shard allocation across nodes, and storage efficiency metrics."
    )

    try:
        # 1. Get index information
        indices = connector.execute_query({"operation": "cat_indices"})

        if isinstance(indices, dict) and "error" in indices:
            builder.error(f"Could not retrieve index information: {indices['error']}")
            return builder.build(), {
                "status": "error",
                "error": indices['error']
            }

        # 2. Get shard allocation details
        shards = connector.execute_query({"operation": "cat_shards"})

        # 3. Analyze indices
        total_indices = len(indices) if isinstance(indices, list) else 0
        red_indices = []
        yellow_indices = []
        green_indices = []
        large_indices = []
        unassigned_shards_by_index = {}

        # Process each index
        for index in indices if isinstance(indices, list) else []:
            index_name = index.get('index', 'Unknown')
            health = index.get('health', 'unknown').lower()
            docs_count = int(index.get('docs.count', 0) or 0)
            store_size = index.get('store.size', '0b')
            pri_shards = int(index.get('pri', 0) or 0)
            replica_shards = int(index.get('rep', 0) or 0)

            index_info = {
                'name': index_name,
                'health': health,
                'docs': docs_count,
                'size': store_size,
                'primary_shards': pri_shards,
                'replicas': replica_shards
            }

            if health == 'red':
                red_indices.append(index_info)
            elif health == 'yellow':
                yellow_indices.append(index_info)
            else:
                green_indices.append(index_info)

            # Check for large indices (>100GB or >1B docs)
            if _is_large_index(store_size, docs_count):
                large_indices.append(index_info)

        # Count unassigned shards
        unassigned_count = 0
        if isinstance(shards, list):
            for shard in shards:
                if shard.get('state') == 'UNASSIGNED':
                    unassigned_count += 1
                    index_name = shard.get('index', 'Unknown')
                    unassigned_shards_by_index[index_name] = unassigned_shards_by_index.get(index_name, 0) + 1

        # 4. Display critical issues first
        if red_indices:
            builder.h4("🔴 Critical: Red Indices")
            builder.critical(
                f"**{len(red_indices)} index(es) in RED state**\n\n"
                "Red status indicates at least one primary shard is not allocated. "
                "Data is unavailable and the cluster cannot accept writes to these indices."
            )
            _build_index_table(builder, red_indices)
            builder.blank()

        if unassigned_count > 0:
            builder.h4("⚠️ Unassigned Shards Detected")
            builder.warning(
                f"**{unassigned_count} shard(s) are currently unassigned**\n\n"
                "Unassigned shards may indicate insufficient nodes, disk space issues, "
                "or shard allocation problems."
            )

            if unassigned_shards_by_index:
                unassigned_table = [
                    {"Index": idx, "Unassigned Shards": count}
                    for idx, count in sorted(unassigned_shards_by_index.items(), key=lambda x: x[1], reverse=True)[:10]
                ]
                builder.table(unassigned_table)
            builder.blank()

        if yellow_indices:
            builder.h4("⚠️ Yellow Indices")
            builder.warning(
                f"**{len(yellow_indices)} index(es) in YELLOW state**\n\n"
                "Yellow status means all primary shards are allocated but one or more replica shards are not. "
                "The cluster is functional but at risk of data loss if a node fails."
            )
            _build_index_table(builder, yellow_indices[:10])  # Show top 10
            if len(yellow_indices) > 10:
                builder.para(f"...and {len(yellow_indices) - 10} more yellow indices")
            builder.blank()

        # 5. Summary statistics
        builder.h4("Cluster Index Summary")
        summary_data = [
            {"Metric": "Total Indices", "Value": total_indices},
            {"Metric": "Green Indices", "Value": f"{len(green_indices)} ✅"},
            {"Metric": "Yellow Indices", "Value": f"{len(yellow_indices)} ⚠️" if yellow_indices else f"{len(yellow_indices)}"},
            {"Metric": "Red Indices", "Value": f"{len(red_indices)} 🔴" if red_indices else f"{len(red_indices)}"},
            {"Metric": "Unassigned Shards", "Value": f"{unassigned_count} ⚠️" if unassigned_count > 0 else f"{unassigned_count}"}
        ]
        builder.table(summary_data)
        builder.blank()

        # 6. Large indices analysis
        if large_indices:
            builder.h4("Large Indices (>100GB or >1B documents)")
            builder.para(
                "These indices may benefit from optimization, archiving, or redistribution strategies."
            )
            _build_index_table(builder, large_indices)
            builder.blank()

        # 7. Top indices by size
        builder.h4("Top 10 Largest Indices")
        sorted_indices = sorted(
            [idx for idx in indices if isinstance(indices, list)],
            key=lambda x: _parse_size_to_bytes(x.get('store.size', '0b')),
            reverse=True
        )[:10]

        if sorted_indices:
            top_indices_data = []
            for idx in sorted_indices:
                top_indices_data.append({
                    "Index": idx.get('index', 'Unknown'),
                    "Health": _health_icon(idx.get('health', 'unknown')),
                    "Size": idx.get('store.size', 'N/A'),
                    "Documents": f"{int(idx.get('docs.count', 0) or 0):,}",
                    "Primary Shards": idx.get('pri', 'N/A'),
                    "Replicas": idx.get('rep', 'N/A')
                })
            builder.table(top_indices_data)
        else:
            builder.para("No index data available.")

        # 8. Shard distribution analysis
        builder.h4("Shard Distribution Analysis")
        if isinstance(shards, list) and shards:
            _analyze_shard_distribution(builder, shards)
        else:
            builder.para("Shard distribution data not available.")

        # 9. Recommendations
        recommendations = _generate_index_recommendations(
            red_indices,
            yellow_indices,
            large_indices,
            unassigned_count,
            total_indices
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        elif total_indices > 0:
            builder.success("✅ All indices are healthy with proper shard allocation.")

        # 10. Return structured data with proper format for rules engine
        return builder.build(), {
            "status": "success",
            "data": {
                "total_indices": total_indices,
                "green_indices": len(green_indices),
                "yellow_indices": len(yellow_indices),
                "red_indices": len(red_indices),
                "unassigned_shards": unassigned_count,
                "large_indices": len(large_indices),
                "critical_issues": len(red_indices) + (1 if unassigned_count > 0 else 0),
                "has_red_indices": len(red_indices) > 0,
                "has_yellow_indices": len(yellow_indices) > 0,
                "has_unassigned_shards": unassigned_count > 0,
                "has_large_indices": len(large_indices) > 0,
                "all_healthy": len(red_indices) == 0 and len(yellow_indices) == 0,
            }
        }

    except Exception as e:
        logger.error(f"Index health check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _is_large_index(size_str, docs_count):
    """Determine if an index is considered large."""
    size_bytes = _parse_size_to_bytes(size_str)
    # >100GB or >1 billion documents
    return size_bytes > (100 * 1024**3) or docs_count > 1_000_000_000


def _parse_size_to_bytes(size_str):
    """Convert size string (e.g., '10.5gb') to bytes."""
    if not size_str or size_str == 'N/A':
        return 0

    size_str = str(size_str).lower().strip()

    # Extract number and unit
    import re
    match = re.match(r'([0-9.]+)\s*([a-z]+)?', size_str)
    if not match:
        return 0

    value = float(match.group(1))
    unit = match.group(2) if match.group(2) else 'b'

    multipliers = {
        'b': 1,
        'kb': 1024,
        'mb': 1024**2,
        'gb': 1024**3,
        'tb': 1024**4,
        'pb': 1024**5
    }

    return int(value * multipliers.get(unit, 1))


def _health_icon(health_status):
    """Return icon for health status."""
    health = str(health_status).lower()
    icons = {
        'green': '✅ green',
        'yellow': '⚠️ yellow',
        'red': '🔴 red'
    }
    return icons.get(health, f'❓ {health}')


def _build_index_table(builder, index_list):
    """Build a table for index information."""
    if not index_list:
        return

    table_data = []
    for idx in index_list:
        table_data.append({
            "Index": idx['name'],
            "Health": _health_icon(idx['health']),
            "Documents": f"{idx['docs']:,}",
            "Size": idx['size'],
            "Primary Shards": idx['primary_shards'],
            "Replicas": idx['replicas']
        })

    builder.table(table_data)


def _analyze_shard_distribution(builder, shards):
    """Analyze shard distribution across nodes."""
    # Count shards per node
    shards_per_node = {}
    shard_states = {}

    for shard in shards:
        node = shard.get('node', 'UNASSIGNED')
        state = shard.get('state', 'UNKNOWN')

        shards_per_node[node] = shards_per_node.get(node, 0) + 1
        shard_states[state] = shard_states.get(state, 0) + 1

    # Build distribution table
    if shards_per_node:
        distribution_data = []
        for node, count in sorted(shards_per_node.items(), key=lambda x: x[1], reverse=True):
            if node != 'UNASSIGNED':
                distribution_data.append({
                    "Node": node,
                    "Shard Count": count
                })

        if distribution_data:
            builder.para("**Shards per Node:**")
            builder.table(distribution_data)

    # Show shard states
    if shard_states:
        builder.para("\n**Shard States:**")
        states_data = [
            {"State": state, "Count": count}
            for state, count in sorted(shard_states.items(), key=lambda x: x[1], reverse=True)
        ]
        builder.table(states_data)


def _generate_index_recommendations(red_indices, yellow_indices, large_indices, unassigned_count, total_indices):
    """Generate recommendations based on index health analysis."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if red_indices:
        recs["critical"].append(
            "Investigate and resolve red indices immediately - data is currently unavailable"
        )
        recs["critical"].append(
            "Check cluster logs for shard allocation errors and disk space issues"
        )
        recs["critical"].append(
            "Use 'GET _cluster/allocation/explain' API to understand why shards are unassigned"
        )

    if unassigned_count > 0:
        recs["critical"].append(
            "Resolve unassigned shards by addressing node availability, disk space, or allocation settings"
        )
        recs["high"].append(
            "Review shard allocation awareness and filtering settings"
        )

    if yellow_indices:
        recs["high"].append(
            "Investigate yellow indices - may indicate insufficient replica nodes"
        )
        recs["high"].append(
            "Consider adding nodes if running single-node cluster with replicas configured"
        )
        recs["high"].append(
            "For development environments, consider setting number_of_replicas to 0"
        )

    if large_indices:
        recs["high"].append(
            "Review large indices for optimization opportunities (force merge, shrink API)"
        )
        recs["high"].append(
            "Consider implementing Index Lifecycle Management (ILM) policies for data retention"
        )
        recs["high"].append(
            "Evaluate moving old data to warm/cold tiers if using hot-warm-cold architecture"
        )

    if total_indices > 1000:
        recs["general"].append(
            f"Cluster has {total_indices} indices - consider consolidating to reduce overhead"
        )
        recs["general"].append(
            "Too many indices can impact cluster state size and performance"
        )

    # General recommendations
    recs["general"].append(
        "Monitor index growth and implement automated deletion/archival policies"
    )
    recs["general"].append(
        "Set up alerting for index health status changes (green→yellow→red)"
    )
    recs["general"].append(
        "Review number of shards per index - over-sharding impacts performance"
    )

    return recs

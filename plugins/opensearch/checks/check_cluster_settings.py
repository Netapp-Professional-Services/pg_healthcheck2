"""
OpenSearch Cluster Settings Audit

Audits critical cluster settings for production readiness.
Pure REST API check with AWS-specific validations when applicable.
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 7


def run_check_cluster_settings(connector, settings):
    """
    Audit cluster settings for production readiness.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)

    builder.h3("Cluster Settings Audit")
    builder.para("Review of critical cluster configuration settings and production readiness.")

    try:
        # Get cluster settings
        cluster_settings = connector.execute_query({"operation": "cluster_stats"})

        if isinstance(cluster_settings, dict) and "error" in cluster_settings:
            builder.error(f"Could not retrieve cluster settings: {cluster_settings['error']}")
            return builder.build(), {
                "status": "error",
                "error": cluster_settings['error']
            }

        # Get cluster state for additional info
        cluster_health = connector.execute_query({"operation": "cluster_health"})

        issues = {"critical": [], "warnings": [], "info": []}

        # Analyze key settings
        nodes = cluster_settings.get('nodes', {})
        indices = cluster_settings.get('indices', {})

        node_count = nodes.get('count', {}).get('total', 0)
        data_node_count = nodes.get('count', {}).get('data', 0)
        master_node_count = nodes.get('count', {}).get('master', 0)

        # Check 1: Node counts
        builder.h4("Cluster Topology")
        topology_data = [
            {"Setting": "Total Nodes", "Value": node_count},
            {"Setting": "Data Nodes", "Value": data_node_count},
            {"Setting": "Master-Eligible Nodes", "Value": master_node_count}
        ]
        builder.table(topology_data)

        if master_node_count < 3 and connector.environment != 'aws':
            issues["warnings"].append(
                "Less than 3 master-eligible nodes - risk of split-brain. Recommended: 3+ master nodes"
            )

        if data_node_count < 2:
            issues["warnings"].append(
                "Single data node detected - no redundancy. Recommended: 2+ data nodes"
            )

        # Check 2: Shard counts
        builder.h4("Shard Configuration")
        total_shards = cluster_health.get('active_shards', 0)
        primary_shards = cluster_health.get('active_primary_shards', 0)
        unassigned_shards = cluster_health.get('unassigned_shards', 0)

        if data_node_count > 0:
            shards_per_node = total_shards / data_node_count
        else:
            shards_per_node = 0

        shard_data = [
            {"Metric": "Total Shards", "Value": total_shards},
            {"Metric": "Primary Shards", "Value": primary_shards},
            {"Metric": "Replica Shards", "Value": total_shards - primary_shards},
            {"Metric": "Unassigned Shards", "Value": f"{unassigned_shards} ⚠️" if unassigned_shards > 0 else unassigned_shards},
            {"Metric": "Shards per Node (avg)", "Value": f"{shards_per_node:.1f}"}
        ]
        builder.table(shard_data)

        if shards_per_node > 1000:
            issues["critical"].append(
                f"High shard count per node ({shards_per_node:.0f}). Recommended: <1000 shards per node"
            )
        elif shards_per_node > 600:
            issues["warnings"].append(
                f"Elevated shard count per node ({shards_per_node:.0f}). Consider consolidating indices"
            )

        # Check 3: Index count
        index_count = indices.get('count', 0)
        builder.h4("Index Statistics")
        index_stats = [
            {"Metric": "Total Indices", "Value": index_count},
            {"Metric": "Total Documents", "Value": f"{indices.get('docs', {}).get('count', 0):,}"},
            {"Metric": "Total Store Size", "Value": _format_bytes(indices.get('store', {}).get('size_in_bytes', 0))}
        ]
        builder.table(index_stats)

        if index_count > 1000:
            issues["warnings"].append(
                f"Large number of indices ({index_count}). Consider consolidating or implementing ISM"
            )

        # Check 4: AWS-specific settings
        if connector.environment == 'aws':
            builder.h4("AWS OpenSearch Service Configuration")
            builder.para(
                "AWS OpenSearch Service manages many settings automatically. "
                "Review the following via AWS Console:\n\n"
                "* Auto-Tune status (recommended: enabled)\n"
                "* Dedicated master nodes (recommended for production)\n"
                "* Multi-AZ deployment (recommended: enabled)\n"
                "* Automated snapshots (recommended: enabled)\n"
                "* VPC configuration and security groups"
            )

        # Check 5: Production readiness recommendations
        builder.h4("Production Readiness Checklist")

        readiness_items = []
        if connector.environment == 'self_hosted':
            readiness_items.extend([
                {"Check": "Master Nodes", "Status": "✅ OK" if master_node_count >= 3 else "⚠️ Review", "Recommendation": "3+ master-eligible nodes"},
                {"Check": "Data Node Redundancy", "Status": "✅ OK" if data_node_count >= 2 else "⚠️ Review", "Recommendation": "2+ data nodes minimum"},
                {"Check": "Shards per Node", "Status": "✅ OK" if shards_per_node < 600 else "⚠️ Review", "Recommendation": "< 1000 shards/node"},
            ])
        elif connector.environment == 'aws':
            readiness_items.extend([
                {"Check": "Multi-AZ", "Status": "Review in Console", "Recommendation": "Enable for HA"},
                {"Check": "Dedicated Masters", "Status": "Review in Console", "Recommendation": "Enable for production"},
                {"Check": "Auto-Tune", "Status": "Review in Console", "Recommendation": "Enable for optimization"},
            ])

        readiness_items.extend([
            {"Check": "Index Count", "Status": "✅ OK" if index_count < 500 else "ℹ️ Monitor", "Recommendation": "< 500 indices ideal"},
            {"Check": "Unassigned Shards", "Status": "✅ OK" if unassigned_shards == 0 else "🔴 Action Required", "Recommendation": "0 unassigned shards"},
        ])

        builder.table(readiness_items)

        # Display issues
        if issues["critical"]:
            builder.h4("🔴 Critical Configuration Issues")
            for issue in issues["critical"]:
                builder.critical(issue)

        if issues["warnings"]:
            builder.h4("⚠️ Configuration Warnings")
            for warning in issues["warnings"]:
                builder.warning(warning)

        # Recommendations
        recs = {"high": [], "general": []}

        if issues["critical"] or issues["warnings"]:
            recs["high"].extend([
                "Review and optimize shard allocation strategy",
                "Implement Index State Management (ISM) for lifecycle policies",
                "Consider cluster topology changes for better resilience"
            ])

        recs["general"].extend([
            "Regularly review cluster settings as workload changes",
            "Monitor shard count trends - plan consolidation if growing",
            "Set up automated snapshots for disaster recovery",
            "Review and tune JVM heap sizes (typically 50% of RAM, max 32GB)",
            "Implement monitoring and alerting on key metrics"
        ])

        if recs["high"] or (not issues["critical"] and not issues["warnings"]):
            if issues["critical"] or issues["warnings"]:
                builder.recs(recs)
            else:
                builder.success("✅ Cluster settings are well-configured for production use.")
                builder.recs({"general": recs["general"]})

        # Return structured data with proper format for rules engine
        return builder.build(), {
            "status": "success",
            "data": {
                "node_count": node_count,
                "data_node_count": data_node_count,
                "master_node_count": master_node_count,
                "index_count": index_count,
                "total_shards": total_shards,
                "unassigned_shards": unassigned_shards,
                "shards_per_node": round(shards_per_node, 1),
                "critical_issues": len(issues["critical"]),
                "warnings": len(issues["warnings"]),
                "has_critical_issues": len(issues["critical"]) > 0,
                "has_warnings": len(issues["warnings"]) > 0,
                "insufficient_master_nodes": master_node_count < 3,
                "single_data_node": data_node_count < 2,
                "high_shard_count": shards_per_node > 1000,
                "elevated_shard_count": shards_per_node > 600,
            }
        }

    except Exception as e:
        logger.error(f"Settings audit failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _format_bytes(bytes_value):
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"

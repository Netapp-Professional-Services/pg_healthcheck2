"""
Remote Clusters Check

Analyzes remote cluster configurations for cross-cluster search and replication.
Identifies multi-cluster architecture gaps and CCR/CCS opportunities.

Data source:
- Live clusters: GET /_remote/info
- Imported diagnostics: remote_cluster_info.json

Sales Opportunities:
- Multi-datacenter replication setup (CCR)
- Cross-cluster search configuration
- Disaster recovery architecture
- Geographic data distribution consulting
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check remote cluster configurations.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Remote Cluster Configuration")

    try:
        # Get remote cluster info
        remote_data = connector.execute_query({"operation": "remote_info"})

        if isinstance(remote_data, dict) and "error" in remote_data:
            builder.note(
                "Remote cluster information is not available.\n\n"
                "This may indicate the remote cluster feature is not configured."
            )
            return builder.build(), {
                "status": "unavailable",
                "data": {"reason": remote_data.get("error", "Unknown")}
            }

        # Count configured remote clusters
        # Remote cluster info returns a dict with cluster names as keys
        remote_clusters = {k: v for k, v in remote_data.items()
                         if isinstance(v, dict) and k not in ['error', 'status']}
        cluster_count = len(remote_clusters)

        if cluster_count > 0:
            builder.h4("Configured Remote Clusters")

            cluster_table = []
            connected_count = 0
            disconnected_count = 0

            for cluster_name, cluster_info in remote_clusters.items():
                connected = cluster_info.get("connected", False)
                mode = cluster_info.get("mode", "unknown")
                seeds = cluster_info.get("seeds", [])
                num_nodes = cluster_info.get("num_nodes_connected", 0)
                max_connections = cluster_info.get("max_connections_per_cluster", 3)
                skip_unavailable = cluster_info.get("skip_unavailable", False)

                if connected:
                    connected_count += 1
                    status = "✅ Connected"
                else:
                    disconnected_count += 1
                    status = "❌ Disconnected"

                cluster_table.append({
                    "Cluster": cluster_name,
                    "Status": status,
                    "Mode": mode,
                    "Connected Nodes": str(num_nodes),
                    "Max Connections": str(max_connections),
                    "Skip Unavailable": "Yes" if skip_unavailable else "No"
                })

            builder.table(cluster_table)
            builder.blank()

            # Connection issues
            if disconnected_count > 0:
                builder.warning(
                    f"**{disconnected_count} Remote Cluster(s) Disconnected**\n\n"
                    "Disconnected remote clusters may indicate network issues, "
                    "credential problems, or cluster unavailability."
                )
                builder.blank()

            # Architecture analysis
            builder.h4("Multi-Cluster Architecture")

            # Check for CCR indicators
            ccr_data = connector.execute_query({"operation": "ccr_stats"})
            ccr_configured = False
            if isinstance(ccr_data, dict) and "error" not in ccr_data:
                follower_indices = ccr_data.get("follow_stats", {}).get("indices", [])
                ccr_configured = len(follower_indices) > 0

            arch_table = [
                {"Feature": "Remote Clusters Configured", "Status": f"{cluster_count} cluster(s)"},
                {"Feature": "Connected Clusters", "Status": f"{connected_count} of {cluster_count}"},
                {"Feature": "Cross-Cluster Search (CCS)", "Status": "✅ Available" if connected_count > 0 else "❌ No connections"},
                {"Feature": "Cross-Cluster Replication (CCR)", "Status": "✅ Active" if ccr_configured else "⚠️ Not configured"},
            ]
            builder.table(arch_table)
            builder.blank()

            # Recommendations based on configuration
            recs = {"high": [], "general": []}

            if disconnected_count > 0:
                recs["high"].append("Investigate and restore disconnected remote cluster connections")

            if not ccr_configured and cluster_count > 0:
                recs["high"].append(
                    "Configure Cross-Cluster Replication (CCR) for disaster recovery - "
                    "remote clusters are available but not being used for replication"
                )

            recs["general"].extend([
                "Review remote cluster security settings and credentials",
                "Consider geographic distribution for improved latency",
                "Implement CCR for critical indices requiring DR protection",
                "Monitor cross-cluster search performance"
            ])

            builder.recs(recs)

        else:
            # No remote clusters - opportunity for multi-cluster architecture
            builder.note(
                "No remote clusters are configured.\n\n"
                "Consider the following multi-cluster capabilities:"
            )
            builder.blank()

            builder.h4("Multi-Cluster Opportunities")

            opportunities = [
                {
                    "Feature": "Cross-Cluster Search (CCS)",
                    "Benefit": "Search across multiple clusters as if they were one",
                    "Use Case": "Federated search, multi-region deployments"
                },
                {
                    "Feature": "Cross-Cluster Replication (CCR)",
                    "Benefit": "Real-time replication to remote clusters",
                    "Use Case": "Disaster recovery, geographic redundancy"
                },
                {
                    "Feature": "Active-Passive DR",
                    "Benefit": "Failover capability to secondary cluster",
                    "Use Case": "Business continuity, compliance requirements"
                },
                {
                    "Feature": "Active-Active Deployment",
                    "Benefit": "Read traffic distribution across regions",
                    "Use Case": "Global applications, reduced latency"
                }
            ]
            builder.table(opportunities)
            builder.blank()

            builder.para(
                "**Note:** Cross-Cluster Replication (CCR) requires a Platinum or Enterprise license. "
                "Cross-Cluster Search (CCS) is available with all license tiers."
            )

        # Build structured data for rules engine and sales queries
        return builder.build(), {
            "status": "success",
            "data": {
                "remote_cluster_count": cluster_count,
                "connected_cluster_count": connected_count if cluster_count > 0 else 0,
                "disconnected_cluster_count": disconnected_count if cluster_count > 0 else 0,
                "has_remote_clusters": cluster_count > 0,
                "has_disconnected_clusters": disconnected_count > 0 if cluster_count > 0 else False,
                "ccr_configured": ccr_configured if cluster_count > 0 else False,
                # Sales signals
                "multi_cluster_opportunity": cluster_count == 0,
                "ccr_upsell_opportunity": cluster_count > 0 and not (ccr_configured if cluster_count > 0 else False),
                "dr_architecture_gap": cluster_count == 0,
                "connection_issues": disconnected_count > 0 if cluster_count > 0 else False,
            }
        }

    except Exception as e:
        builder.error(f"Failed to check remote clusters: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

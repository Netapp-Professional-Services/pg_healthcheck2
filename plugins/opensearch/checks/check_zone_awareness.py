"""
Zone Awareness Check

Analyzes cluster topology for high availability configuration including
zone/rack awareness, shard allocation, and geographic distribution.

Data source:
- Live clusters: GET /_cat/nodeattrs, GET /_cluster/settings
- Imported diagnostics: cat/cat_nodeattrs.txt, cluster_settings.json

Sales Opportunities:
- Multi-AZ architecture consulting
- High availability improvements
- Disaster recovery planning
- Geographic distribution optimization
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check zone awareness and HA configuration.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Zone Awareness & High Availability")

    try:
        # Get node attributes
        nodeattrs_data = connector.execute_query({"operation": "cat_nodeattrs"})

        # Get cluster settings
        settings_data = connector.execute_query({"operation": "cluster_settings"})

        # Get nodes info for count
        nodes_data = connector.execute_query({"operation": "nodes"})

        # Parse node count
        node_count = 0
        if isinstance(nodes_data, dict) and "nodes" in nodes_data:
            node_count = len(nodes_data.get("nodes", {}))
        elif isinstance(nodes_data, dict) and "_nodes" in nodes_data:
            node_count = nodes_data.get("_nodes", {}).get("total", 0)

        # Analyze node attributes for zone awareness
        zone_attrs = {}  # node -> zone mapping
        rack_attrs = {}  # node -> rack mapping
        region_attrs = {}  # node -> region mapping
        all_node_attrs = {}

        if isinstance(nodeattrs_data, dict) and "nodeattrs" in nodeattrs_data:
            for attr in nodeattrs_data.get("nodeattrs", []):
                node_name = attr.get("node", "unknown")
                attr_name = attr.get("attr", "")
                attr_value = attr.get("value", "")

                if node_name not in all_node_attrs:
                    all_node_attrs[node_name] = {}
                all_node_attrs[node_name][attr_name] = attr_value

                # Track zone-related attributes
                if "zone" in attr_name.lower() or attr_name == "availability_zone":
                    zone_attrs[node_name] = attr_value
                elif "rack" in attr_name.lower():
                    rack_attrs[node_name] = attr_value
                elif "region" in attr_name.lower():
                    region_attrs[node_name] = attr_value

        # Check cluster settings for awareness configuration
        awareness_enabled = False
        awareness_attributes = []
        forced_awareness = False

        if isinstance(settings_data, dict):
            # Check persistent and transient settings
            for setting_type in ["persistent", "transient", "defaults"]:
                type_settings = settings_data.get(setting_type, {})
                cluster_settings = type_settings.get("cluster", {})
                routing = cluster_settings.get("routing", {})
                allocation = routing.get("allocation", {})
                awareness = allocation.get("awareness", {})

                if awareness.get("attributes"):
                    awareness_enabled = True
                    attrs = awareness.get("attributes", "")
                    if isinstance(attrs, str):
                        awareness_attributes = [a.strip() for a in attrs.split(",")]
                    elif isinstance(attrs, list):
                        awareness_attributes = attrs

                if awareness.get("force", {}).get("zone", {}).get("values"):
                    forced_awareness = True

        # Calculate zone distribution
        unique_zones = set(zone_attrs.values()) if zone_attrs else set()
        unique_racks = set(rack_attrs.values()) if rack_attrs else set()
        unique_regions = set(region_attrs.values()) if region_attrs else set()

        zone_count = len(unique_zones)
        rack_count = len(unique_racks)
        region_count = len(unique_regions)

        # Display topology analysis
        builder.h4("Cluster Topology")

        topology_table = [
            {"Metric": "Total Nodes", "Value": str(node_count)},
            {"Metric": "Unique Zones/AZs", "Value": str(zone_count) if zone_count > 0 else "Not configured"},
            {"Metric": "Unique Racks", "Value": str(rack_count) if rack_count > 0 else "Not configured"},
            {"Metric": "Unique Regions", "Value": str(region_count) if region_count > 0 else "Not configured"},
        ]
        builder.table(topology_table)
        builder.blank()

        # Zone awareness configuration status
        builder.h4("Zone Awareness Configuration")

        awareness_table = [
            {"Setting": "Zone Awareness Enabled", "Status": "✅ Yes" if awareness_enabled else "❌ No"},
            {"Setting": "Awareness Attributes", "Status": ", ".join(awareness_attributes) if awareness_attributes else "None"},
            {"Setting": "Forced Awareness", "Status": "✅ Enabled" if forced_awareness else "❌ Disabled"},
        ]
        builder.table(awareness_table)
        builder.blank()

        # Node distribution by zone
        if zone_attrs:
            builder.h4("Node Distribution by Zone")

            zone_distribution = {}
            for node, zone in zone_attrs.items():
                if zone not in zone_distribution:
                    zone_distribution[zone] = []
                zone_distribution[zone].append(node)

            dist_table = []
            for zone, nodes in sorted(zone_distribution.items()):
                dist_table.append({
                    "Zone/AZ": zone,
                    "Nodes": str(len(nodes)),
                    "Node Names": ", ".join(nodes[:3]) + ("..." if len(nodes) > 3 else "")
                })

            builder.table(dist_table)
            builder.blank()

            # Check for imbalanced distribution
            node_counts = [len(nodes) for nodes in zone_distribution.values()]
            if node_counts:
                max_nodes = max(node_counts)
                min_nodes = min(node_counts)
                if max_nodes > min_nodes * 2:
                    builder.warning(
                        "**Imbalanced Zone Distribution**\n\n"
                        f"Node distribution across zones is uneven (max: {max_nodes}, min: {min_nodes}). "
                        "This can impact fault tolerance and performance."
                    )
                    builder.blank()

        # HA Assessment
        builder.h4("High Availability Assessment")

        ha_issues = []
        ha_score = 100

        # Single zone = high risk
        if zone_count <= 1 and node_count > 1:
            ha_issues.append({
                "Issue": "Single Zone Deployment",
                "Risk": "Critical",
                "Impact": "Complete cluster failure if zone becomes unavailable"
            })
            ha_score -= 40

        # No awareness enabled with multiple zones
        if zone_count > 1 and not awareness_enabled:
            ha_issues.append({
                "Issue": "Zone Awareness Disabled",
                "Risk": "High",
                "Impact": "Primary and replica shards may be placed in same zone"
            })
            ha_score -= 25

        # No forced awareness
        if awareness_enabled and not forced_awareness and zone_count > 1:
            ha_issues.append({
                "Issue": "Forced Awareness Not Enabled",
                "Risk": "Medium",
                "Impact": "Cluster may allocate shards in unavailable zones during failures"
            })
            ha_score -= 15

        # Small cluster without redundancy
        if node_count < 3:
            ha_issues.append({
                "Issue": "Insufficient Nodes",
                "Risk": "High",
                "Impact": f"Only {node_count} node(s) - no quorum for master election"
            })
            ha_score -= 20

        if ha_issues:
            builder.table(ha_issues)
            builder.blank()
        else:
            builder.success("✅ High availability configuration appears healthy.")
            builder.blank()

        # Recommendations
        recs = {"critical": [], "high": [], "general": []}

        if zone_count <= 1 and node_count > 1:
            recs["critical"].append(
                "Deploy nodes across multiple availability zones for fault tolerance"
            )

        if zone_count > 1 and not awareness_enabled:
            recs["high"].append(
                "Enable zone awareness to ensure replicas are placed in different zones: "
                "PUT /_cluster/settings {\"persistent\": {\"cluster.routing.allocation.awareness.attributes\": \"zone\"}}"
            )

        if awareness_enabled and not forced_awareness:
            recs["high"].append(
                "Enable forced zone awareness to prevent shard allocation in unavailable zones"
            )

        if node_count < 3:
            recs["high"].append(
                "Add at least 3 nodes for proper master election quorum"
            )

        recs["general"].extend([
            "Regularly review node distribution across zones",
            "Consider dedicated master nodes for large clusters",
            "Implement monitoring for zone-level failures",
            "Test failover procedures periodically"
        ])

        if recs["critical"] or recs["high"]:
            builder.recs(recs)
        else:
            builder.recs({"general": recs["general"]})

        # Build structured data for rules engine and sales queries
        return builder.build(), {
            "status": "success",
            "data": {
                "node_count": node_count,
                "zone_count": zone_count,
                "rack_count": rack_count,
                "region_count": region_count,
                "awareness_enabled": awareness_enabled,
                "forced_awareness": forced_awareness,
                "awareness_attributes": awareness_attributes,
                "ha_score": max(0, ha_score),
                "ha_issue_count": len(ha_issues),
                # Sales signals
                "single_zone_deployment": zone_count <= 1 and node_count > 1,
                "zone_awareness_gap": zone_count > 1 and not awareness_enabled,
                "ha_consulting_opportunity": len(ha_issues) > 0,
                "multi_az_upgrade_opportunity": zone_count <= 1,
                "insufficient_nodes": node_count < 3,
            }
        }

    except Exception as e:
        builder.error(f"Failed to check zone awareness: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

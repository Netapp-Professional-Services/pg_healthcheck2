"""
Enrich policy status for Elasticsearch/OpenSearch clusters.

This check analyzes enrich policies to identify:
- Policy inventory and configuration
- Policy execution status
- Resource usage

Data source:
- Live clusters: GET /_enrich/policy, GET /_enrich/_stats
- Imported diagnostics: commercial/enrich_policies.json, commercial/enrich_stats.json

Enrich policies are used to add data from one index to documents being indexed.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check enrich policy status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Enrich Policies")

    try:
        # Get enrich data
        enrich_policies = connector.execute_query({"operation": "enrich_policies"})
        enrich_stats = connector.execute_query({"operation": "enrich_stats"})

        policies_available = not (isinstance(enrich_policies, dict) and "error" in enrich_policies)
        stats_available = not (isinstance(enrich_stats, dict) and "error" in enrich_stats)

        if not policies_available:
            builder.note(
                "Enrich policy information is not available.\n\n"
                "This may indicate enrich policies are not configured or diagnostic data is missing."
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": enrich_policies.get("error", "Unknown") if isinstance(enrich_policies, dict) else "Unknown"
            }

        policies_list = enrich_policies.get("policies", [])

        if not policies_list:
            builder.note(
                "**No enrich policies configured**\n\n"
                "Enrich policies are used to add data from lookup indices to documents during indexing. "
                "No action required if not using enrich."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "policy_count": 0,
                    "has_policies": False
                }
            }

        # Parse stats
        executing_policies = []
        coordinator_stats = []

        if stats_available and enrich_stats:
            executing_policies = enrich_stats.get("executing_policies", [])
            coordinator_stats = enrich_stats.get("coordinator_stats", [])

        # Analyze policies
        analyzed_policies = []

        for policy_entry in policies_list:
            config = policy_entry.get("config", {})

            # Policy can be match or geo_match type
            policy_type = None
            policy_config = {}

            if "match" in config:
                policy_type = "match"
                policy_config = config["match"]
            elif "geo_match" in config:
                policy_type = "geo_match"
                policy_config = config["geo_match"]
            else:
                policy_type = "unknown"
                policy_config = config

            policy_info = {
                "name": policy_config.get("name", "Unknown"),
                "type": policy_type,
                "indices": policy_config.get("indices", []),
                "match_field": policy_config.get("match_field", "N/A"),
                "enrich_fields": policy_config.get("enrich_fields", [])
            }
            analyzed_policies.append(policy_info)

        # Build report
        builder.h4("Enrich Policy Summary")

        summary_data = [
            {"Metric": "Total Policies", "Value": str(len(analyzed_policies))},
            {"Metric": "Currently Executing", "Value": str(len(executing_policies))},
            {"Metric": "Coordinator Nodes", "Value": str(len(coordinator_stats))}
        ]
        builder.table(summary_data)
        builder.blank()

        # Policy details
        builder.h4("Policy Details")

        policy_table = []
        for policy in analyzed_policies:
            indices_str = ", ".join(policy["indices"][:2])
            if len(policy["indices"]) > 2:
                indices_str += f" (+{len(policy['indices']) - 2} more)"

            fields_str = ", ".join(policy["enrich_fields"][:3])
            if len(policy["enrich_fields"]) > 3:
                fields_str += f" (+{len(policy['enrich_fields']) - 3} more)"

            policy_table.append({
                "Policy": policy["name"],
                "Type": policy["type"],
                "Source Indices": indices_str,
                "Match Field": policy["match_field"],
                "Enrich Fields": fields_str
            })
        builder.table(policy_table)
        builder.blank()

        # Coordinator stats
        if coordinator_stats:
            builder.h4("Coordinator Statistics")

            total_executions = sum(s.get("executed_searches_total", 0) for s in coordinator_stats)
            total_remote = sum(s.get("remote_requests_total", 0) for s in coordinator_stats)

            coord_summary = [
                {"Metric": "Total Executed Searches", "Value": f"{total_executions:,}"},
                {"Metric": "Total Remote Requests", "Value": f"{total_remote:,}"}
            ]
            builder.table(coord_summary)
            builder.blank()

        # Currently executing
        if executing_policies:
            builder.h4("Currently Executing")
            for exec_policy in executing_policies:
                task = exec_policy.get("task", {})
                builder.text(f"- **{exec_policy.get('name', 'Unknown')}**: Task {task.get('id', 'N/A')}")
            builder.blank()

        # Recommendations
        builder.text("*Enrich Best Practices:*")
        builder.text("- Keep enrich indices small (< 1GB) for best performance")
        builder.text("- Execute enrich policy after source index updates")
        builder.text("- Monitor coordinator queue for bottlenecks")
        builder.text("- Consider using smaller batch sizes if seeing memory pressure")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "policy_count": len(analyzed_policies),
                "executing_count": len(executing_policies),
                "coordinator_count": len(coordinator_stats),
                "has_policies": len(analyzed_policies) > 0,
                "has_executing": len(executing_policies) > 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check enrich policies: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

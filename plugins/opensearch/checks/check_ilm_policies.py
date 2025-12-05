"""
Index Lifecycle Management (ILM) / Index State Management (ISM) policy audit.

This check analyzes ILM policies (Elasticsearch) or ISM policies (OpenSearch)
to identify:
- Missing lifecycle policies for data retention
- Policies without delete phases (unbounded growth)
- Misconfigured rollover settings
- Policies that may cause storage issues

Data source:
- Live clusters: GET /_ilm/policy API (Elasticsearch) or GET /_plugins/_ism/policies (OpenSearch)
- Imported diagnostics: commercial/ilm_policies.json

Note: OpenSearch uses ISM (Index State Management) instead of ILM. The concepts
are similar but the API and terminology differ. This check handles both.
"""

from datetime import datetime, timezone
from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Audit ILM/ISM policies for best practices.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Index Lifecycle Management (ILM) Policy Audit")

    try:
        # Get ILM policy data
        policies_data = connector.execute_query({"operation": "ilm_policies"})

        if isinstance(policies_data, dict) and "error" in policies_data:
            builder.note(
                "ILM/ISM policy information is not available.\n\n"
                "This may indicate:\n"
                "- ILM is not enabled (Elasticsearch basic license or higher required)\n"
                "- OpenSearch ISM is not configured\n"
                "- Diagnostic export did not include ilm_policies.json"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": policies_data.get("error", "Unknown")
            }

        # Handle empty response
        if not policies_data:
            builder.warning(
                "⚠️ **No ILM policies defined**\n\n"
                "Without lifecycle policies, indices will grow indefinitely and "
                "consume storage until manually deleted.\n\n"
                "Consider implementing ILM policies for:\n"
                "- Log indices (delete after retention period)\n"
                "- Metrics indices (rollover + delete)\n"
                "- Time-series data (hot/warm/cold/delete tiers)"
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "total_policies": 0,
                    "policies_without_delete_count": 0,
                    "has_no_policies": True,
                }
            }

        # Analyze policies
        analyzed_policies = []
        policies_without_delete = []
        policies_with_issues = []
        system_policies = []  # Built-in policies
        user_policies = []    # User-defined policies

        # System policy prefixes (built-in policies we typically don't audit)
        system_prefixes = ['ilm-history', 'slm-history', 'watch-history', 'ml-', '.']

        for policy_name, policy_data in policies_data.items():
            policy_info = _analyze_policy(policy_name, policy_data)
            analyzed_policies.append(policy_info)

            # Categorize as system or user policy
            is_system = any(policy_name.startswith(prefix) for prefix in system_prefixes)
            if is_system:
                system_policies.append(policy_info)
            else:
                user_policies.append(policy_info)

            # Check for missing delete phase (excluding system policies)
            if not policy_info["has_delete_phase"] and not is_system:
                policies_without_delete.append(policy_info)

            # Check for other issues
            if policy_info["issues"] and not is_system:
                policies_with_issues.append(policy_info)

        # Build report
        builder.h4("Policy Summary")

        summary_data = [
            {"Metric": "Total Policies", "Value": str(len(analyzed_policies))},
            {"Metric": "User-Defined Policies", "Value": str(len(user_policies))},
            {"Metric": "System Policies", "Value": str(len(system_policies))},
            {"Metric": "Policies Without Delete Phase", "Value": str(len(policies_without_delete))},
        ]
        builder.table(summary_data)
        builder.blank()

        # Report issues
        if policies_without_delete:
            builder.warning(
                f"⚠️ **{len(policies_without_delete)} user policies have no delete phase**\n\n"
                "Indices managed by these policies will grow indefinitely."
            )
            _add_policy_table(builder, policies_without_delete, "Policies Without Delete Phase")
            builder.blank()

        if policies_with_issues:
            builder.warning(
                f"⚠️ **{len(policies_with_issues)} policies have configuration concerns**"
            )
            for policy in policies_with_issues:
                builder.text(f"*{policy['name']}:*")
                for issue in policy["issues"]:
                    builder.text(f"  - {issue}")
            builder.blank()

        # Show user policies
        if user_policies:
            builder.h4("User-Defined Policies")
            _add_policy_table(builder, user_policies, None)
            builder.blank()

        # Show system policies (collapsed/summary)
        if system_policies:
            builder.h4("System Policies")
            builder.text(f"_{len(system_policies)} system policies found (ilm-history, slm-history, etc.)_")
            builder.blank()

        # Recommendations
        if policies_without_delete or not user_policies:
            builder.text("*Recommendations:*")
            if not user_policies:
                builder.text("1. Create ILM policies for your data indices")
                builder.text("2. Common pattern: hot (7 days) → warm (30 days) → delete (90 days)")
            if policies_without_delete:
                builder.text("1. Add delete phase to policies for data with retention requirements")
                builder.text("2. Consider storage costs of indefinite retention")
            builder.text("")
            builder.text("*Example ILM Policy with Delete:*")
            builder.text("[source,json]")
            builder.text("----")
            builder.text('''{
  "policy": {
    "phases": {
      "hot": { "actions": { "rollover": { "max_size": "50gb", "max_age": "7d" }}},
      "delete": { "min_age": "90d", "actions": { "delete": {} }}
    }
  }
}''')
            builder.text("----")

        # Build structured data for rules engine
        # Note: Return flat structure - report_builder wraps with module name
        structured_data = {
            "status": "success",
            "data": {
                "total_policies": len(analyzed_policies),
                "user_policy_count": len(user_policies),
                "system_policy_count": len(system_policies),
                "policies_without_delete_count": len(policies_without_delete),
                "policies_with_issues_count": len(policies_with_issues),
                "has_no_policies": len(analyzed_policies) == 0,
                "has_no_user_policies": len(user_policies) == 0,
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to audit ILM policies: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _analyze_policy(policy_name, policy_data):
    """Analyze a single ILM policy for issues."""
    policy = policy_data.get("policy", {})
    phases = policy.get("phases", {})

    # Extract phase information
    has_hot = "hot" in phases
    has_warm = "warm" in phases
    has_cold = "cold" in phases
    has_frozen = "frozen" in phases
    has_delete = "delete" in phases

    # Build phase list string
    phase_list = []
    if has_hot:
        phase_list.append("hot")
    if has_warm:
        phase_list.append("warm")
    if has_cold:
        phase_list.append("cold")
    if has_frozen:
        phase_list.append("frozen")
    if has_delete:
        phase_list.append("delete")

    # Extract rollover config if present
    rollover_config = None
    if has_hot:
        hot_actions = phases.get("hot", {}).get("actions", {})
        rollover = hot_actions.get("rollover", {})
        if rollover:
            rollover_parts = []
            if "max_size" in rollover:
                rollover_parts.append(f"size: {rollover['max_size']}")
            if "max_age" in rollover:
                rollover_parts.append(f"age: {rollover['max_age']}")
            if "max_docs" in rollover:
                rollover_parts.append(f"docs: {rollover['max_docs']}")
            rollover_config = ", ".join(rollover_parts) if rollover_parts else "no conditions"

    # Extract delete timing
    delete_after = None
    if has_delete:
        delete_phase = phases.get("delete", {})
        min_age = delete_phase.get("min_age", "0")
        delete_after = min_age

    # Identify issues
    issues = []
    if has_hot and not rollover_config:
        issues.append("Hot phase has no rollover configuration")
    if has_hot and rollover_config == "no conditions":
        issues.append("Rollover has no size/age/docs conditions")

    return {
        "name": policy_name,
        "version": policy_data.get("version", 1),
        "modified_date": policy_data.get("modified_date", "Unknown"),
        "phases": " → ".join(phase_list) if phase_list else "none",
        "has_delete_phase": has_delete,
        "delete_after": delete_after,
        "rollover_config": rollover_config,
        "issues": issues
    }


def _add_policy_table(builder, policies, title):
    """Add a table of policies to the output."""
    if title:
        builder.h4(title)

    table_data = []
    for policy in policies:
        delete_info = policy.get("delete_after", "N/A")
        if policy.get("has_delete_phase"):
            delete_info = f"after {delete_info}"
        else:
            delete_info = "❌ Never"

        table_data.append({
            "Policy Name": policy["name"],
            "Phases": policy["phases"],
            "Rollover": policy.get("rollover_config") or "N/A",
            "Delete": delete_info
        })

    builder.table(table_data)

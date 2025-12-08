"""
Snapshot and Snapshot Lifecycle Management (SLM) policy audit.

This check analyzes backup configuration and execution to identify:
- Missing snapshot repositories (no backup configured)
- SLM policies without recent successful snapshots
- Failed snapshots that need attention
- Repository health and accessibility

Data source:
- Live clusters: GET /_snapshot, GET /_slm/policy, GET /_slm/stats
- Imported diagnostics: repositories.json, commercial/slm_policies.json, commercial/slm_stats.json

Backup validation is critical for disaster recovery readiness.
"""

from datetime import datetime, timezone
from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check snapshot repository and SLM policy status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Snapshot & Backup Status")

    try:
        # Get repository data
        repos_data = connector.execute_query({"operation": "repositories"})
        slm_policies = connector.execute_query({"operation": "slm_policies"})
        slm_stats = connector.execute_query({"operation": "slm_stats"})

        # Check if we have any repositories configured
        repos_available = not (isinstance(repos_data, dict) and "error" in repos_data)
        slm_available = not (isinstance(slm_policies, dict) and "error" in slm_policies)
        stats_available = not (isinstance(slm_stats, dict) and "error" in slm_stats)

        if not repos_available and not slm_available:
            builder.note(
                "Snapshot/SLM information is not available.\n\n"
                "This may indicate:\n"
                "- Snapshot repositories are not configured\n"
                "- SLM is not enabled (requires X-Pack)\n"
                "- Diagnostic export did not include snapshot data"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "Snapshot data not available"
            }

        # Analyze repositories
        repo_count = 0
        repo_list = []

        if repos_available and repos_data:
            for repo_name, repo_config in repos_data.items():
                repo_type = repo_config.get("type", "unknown")
                repo_settings = repo_config.get("settings", {})

                repo_info = {
                    "name": repo_name,
                    "type": repo_type,
                    "location": repo_settings.get("location", repo_settings.get("bucket", "N/A"))
                }
                repo_list.append(repo_info)
                repo_count += 1

        # Analyze SLM policies
        policy_count = 0
        policies_without_success = []
        policies_list = []

        if slm_available and slm_policies:
            for policy_name, policy_data in slm_policies.items():
                policy_info = _analyze_slm_policy(policy_name, policy_data)
                policies_list.append(policy_info)
                policy_count += 1

                if not policy_info.get("has_recent_success"):
                    policies_without_success.append(policy_info)

        # Analyze SLM stats
        stats_info = {}
        if stats_available and slm_stats:
            stats_info = {
                "total_snapshots_taken": slm_stats.get("total_snapshots_taken", 0),
                "total_snapshots_failed": slm_stats.get("total_snapshots_failed", 0),
                "total_snapshots_deleted": slm_stats.get("total_snapshots_deleted", 0),
                "retention_runs": slm_stats.get("retention_runs", 0),
                "retention_failed": slm_stats.get("retention_failed", 0)
            }

        # Build report
        if repo_count == 0 and policy_count == 0:
            builder.critical(
                "**No snapshot repositories or SLM policies configured**\n\n"
                "Without snapshots, data recovery after failure is not possible.\n"
                "This is a critical gap in disaster recovery preparedness."
            )
        elif repo_count == 0:
            builder.warning(
                "**No snapshot repositories configured**\n\n"
                "SLM policies exist but no repositories are configured. "
                "Snapshots cannot be created without a valid repository."
            )
        elif policy_count == 0:
            builder.warning(
                "**No SLM policies configured**\n\n"
                "Snapshot repositories exist but no automated backup policies are defined. "
                "Manual snapshots are required for backup."
            )
        else:
            builder.success(
                f"**{repo_count} repository(ies) and {policy_count} SLM policy(ies) configured**"
            )

        builder.blank()

        # Repository details
        if repo_list:
            builder.h4("Snapshot Repositories")
            table_data = []
            for repo in repo_list:
                table_data.append({
                    "Repository": repo["name"],
                    "Type": repo["type"],
                    "Location": repo["location"]
                })
            builder.table(table_data)
            builder.blank()

        # SLM policy details
        if policies_list:
            builder.h4("SLM Policies")
            table_data = []
            for policy in policies_list:
                status = "OK" if policy.get("has_recent_success") else "No recent success"
                table_data.append({
                    "Policy": policy["name"],
                    "Repository": policy.get("repository", "N/A"),
                    "Schedule": policy.get("schedule", "N/A"),
                    "Retention": policy.get("retention_str", "N/A"),
                    "Status": status
                })
            builder.table(table_data)
            builder.blank()

        # SLM stats summary
        if stats_info:
            builder.h4("Snapshot Statistics")
            stats_table = [
                {"Metric": "Total Snapshots Taken", "Value": str(stats_info.get("total_snapshots_taken", 0))},
                {"Metric": "Total Snapshots Failed", "Value": str(stats_info.get("total_snapshots_failed", 0))},
                {"Metric": "Total Snapshots Deleted", "Value": str(stats_info.get("total_snapshots_deleted", 0))},
                {"Metric": "Retention Runs", "Value": str(stats_info.get("retention_runs", 0))},
                {"Metric": "Retention Failures", "Value": str(stats_info.get("retention_failed", 0))}
            ]
            builder.table(stats_table)
            builder.blank()

            # Alert on failures
            if stats_info.get("total_snapshots_failed", 0) > 0:
                builder.warning(
                    f"**{stats_info['total_snapshots_failed']} snapshot failure(s) recorded**\n\n"
                    "Review snapshot logs to identify and resolve failure causes."
                )

        # Policies without recent success
        if policies_without_success:
            builder.warning(
                f"**{len(policies_without_success)} policy(ies) without recent successful snapshots**\n\n"
                "These policies may not be running correctly."
            )
            for policy in policies_without_success:
                last_failure = policy.get("last_failure_time", "Never recorded")
                builder.text(f"- **{policy['name']}**: Last failure: {last_failure}")
            builder.blank()

        # Recommendations
        if repo_count == 0 or policy_count == 0:
            builder.text("*Recommended Actions:*")
            if repo_count == 0:
                builder.text("1. Configure a snapshot repository (S3, GCS, Azure, or shared filesystem)")
                builder.text("   Example: PUT /_snapshot/my_backup { \"type\": \"s3\", \"settings\": { \"bucket\": \"my-bucket\" }}")
            if policy_count == 0:
                builder.text("1. Create SLM policies for automated backups")
                builder.text("   Recommended: Daily snapshots with 7-day retention minimum")
            builder.text("")
            builder.text("*Backup Best Practices:*")
            builder.text("- Test restore procedures regularly")
            builder.text("- Store snapshots in a different region/zone than primary data")
            builder.text("- Monitor snapshot success/failure via SLM stats")
            builder.text("- Implement retention policies to manage storage costs")

        # Calculate failure rate
        total_taken = stats_info.get("total_snapshots_taken", 0)
        total_failed = stats_info.get("total_snapshots_failed", 0)
        failure_rate = (total_failed / (total_taken + total_failed) * 100) if (total_taken + total_failed) > 0 else 0

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "repository_count": repo_count,
                "policy_count": policy_count,
                "has_repositories": repo_count > 0,
                "has_policies": policy_count > 0,
                "policies_without_success_count": len(policies_without_success),
                "total_snapshots_taken": stats_info.get("total_snapshots_taken", 0),
                "total_snapshots_failed": stats_info.get("total_snapshots_failed", 0),
                "snapshot_failure_rate": round(failure_rate, 2),
                "no_backup_configured": repo_count == 0 and policy_count == 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check snapshot status: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _analyze_slm_policy(policy_name, policy_data):
    """Analyze a single SLM policy."""
    policy = policy_data.get("policy", {})

    # Extract schedule
    schedule = policy.get("schedule", "Not configured")

    # Extract repository
    repository = policy.get("repository", "Not configured")

    # Extract retention settings
    retention = policy.get("retention", {})
    retention_parts = []
    if retention.get("expire_after"):
        retention_parts.append(f"expire: {retention['expire_after']}")
    if retention.get("min_count"):
        retention_parts.append(f"min: {retention['min_count']}")
    if retention.get("max_count"):
        retention_parts.append(f"max: {retention['max_count']}")
    retention_str = ", ".join(retention_parts) if retention_parts else "None"

    # Check last execution
    last_success = policy_data.get("last_success", {})
    last_failure = policy_data.get("last_failure", {})

    last_success_time = last_success.get("time") if last_success else None
    last_failure_time = last_failure.get("time") if last_failure else None

    # Determine if there's been a recent success
    has_recent_success = last_success_time is not None

    return {
        "name": policy_name,
        "repository": repository,
        "schedule": schedule,
        "retention_str": retention_str,
        "has_recent_success": has_recent_success,
        "last_success_time": last_success_time,
        "last_failure_time": last_failure_time,
        "next_execution": policy_data.get("next_execution")
    }

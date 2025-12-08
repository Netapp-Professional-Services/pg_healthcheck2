"""
Cross-Cluster Replication (CCR) status for Elasticsearch clusters.

This check analyzes CCR to identify:
- CCR availability and license status
- Follower indices and their replication status
- Replication lag
- Auto-follow patterns

Data source:
- Live clusters: GET /_ccr/stats, GET /_ccr/auto_follow
- Imported diagnostics: commercial/ccr_stats.json, commercial/ccr_autofollow_patterns.json

Note: CCR requires a Platinum or Enterprise license.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check Cross-Cluster Replication status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Cross-Cluster Replication (CCR)")

    try:
        # Get CCR data
        ccr_stats = connector.execute_query({"operation": "ccr_stats"})
        ccr_autofollow = connector.execute_query({"operation": "ccr_autofollow"})

        stats_available = not (isinstance(ccr_stats, dict) and "error" in ccr_stats)
        autofollow_available = not (isinstance(ccr_autofollow, dict) and "error" in ccr_autofollow)

        # Check for license issues
        if not stats_available:
            error_info = ccr_stats if isinstance(ccr_stats, dict) else {}
            error_msg = str(error_info.get("error", {}) if isinstance(error_info.get("error"), dict) else error_info.get("error", ""))
            reason = error_info.get("error", {}).get("reason", "") if isinstance(error_info.get("error"), dict) else ""

            if "license" in error_msg.lower() or "license" in reason.lower():
                builder.note(
                    "Cross-Cluster Replication is not available with the current license.\n\n"
                    "CCR requires a Platinum or Enterprise license."
                )
                return builder.build(), {
                    "status": "not_licensed",
                    "reason": "License does not include CCR"
                }

            builder.note(
                "CCR information is not available.\n\n"
                "This may indicate:\n"
                "- CCR is not configured\n"
                "- Remote clusters are not connected\n"
                "- Diagnostic data is missing"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "CCR data not available"
            }

        # Parse CCR stats
        follow_stats = ccr_stats.get("follow_stats", {})
        auto_follow_stats = ccr_stats.get("auto_follow_stats", {})

        follower_indices = follow_stats.get("indices", [])
        auto_follow_count = auto_follow_stats.get("number_of_successful_follow_indices", 0)
        auto_follow_failed = auto_follow_stats.get("number_of_failed_follow_indices", 0)
        auto_follow_remote_failures = auto_follow_stats.get("number_of_failed_remote_cluster_state_requests", 0)

        # Parse auto-follow patterns
        patterns = []
        if autofollow_available and ccr_autofollow:
            patterns = ccr_autofollow.get("patterns", [])

        # Analyze follower indices
        healthy_followers = []
        lagging_followers = []
        paused_followers = []

        for follower in follower_indices:
            index_name = follower.get("index", "Unknown")
            shards = follower.get("shards", [])

            for shard in shards:
                shard_id = shard.get("shard_id", 0)
                leader_index = shard.get("leader_index", "Unknown")
                leader_global_checkpoint = shard.get("leader_global_checkpoint", 0)
                follower_global_checkpoint = shard.get("follower_global_checkpoint", 0)
                lag_ops = leader_global_checkpoint - follower_global_checkpoint

                shard_info = {
                    "index": index_name,
                    "shard": shard_id,
                    "leader_index": leader_index,
                    "lag_ops": lag_ops,
                    "time_since_last_read_ms": shard.get("time_since_last_read_millis", 0)
                }

                if lag_ops > 1000:
                    lagging_followers.append(shard_info)
                else:
                    healthy_followers.append(shard_info)

        # Build report
        total_followers = len(follower_indices)

        if total_followers == 0 and len(patterns) == 0:
            builder.note(
                "**No CCR follower indices or auto-follow patterns configured**\n\n"
                "Cross-Cluster Replication is available but not in use."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "ccr_enabled": True,
                    "follower_count": 0,
                    "pattern_count": 0,
                    "has_ccr": False
                }
            }

        builder.h4("CCR Summary")

        summary_data = [
            {"Metric": "Follower Indices", "Value": str(total_followers)},
            {"Metric": "Auto-Follow Patterns", "Value": str(len(patterns))},
            {"Metric": "Successfully Auto-Followed", "Value": str(auto_follow_count)},
            {"Metric": "Failed Auto-Follows", "Value": str(auto_follow_failed)},
            {"Metric": "Remote Cluster Failures", "Value": str(auto_follow_remote_failures)}
        ]
        builder.table(summary_data)
        builder.blank()

        # Replication health
        if lagging_followers:
            builder.warning(
                f"**{len(lagging_followers)} follower shard(s) have significant lag**\n\n"
                "High replication lag may indicate network issues or leader cluster load."
            )
            lag_table = []
            for f in lagging_followers[:10]:
                lag_table.append({
                    "Follower Index": f["index"],
                    "Shard": str(f["shard"]),
                    "Leader Index": f["leader_index"],
                    "Lag (ops)": f"{f['lag_ops']:,}"
                })
            builder.table(lag_table)
            builder.blank()
        elif healthy_followers:
            builder.success(f"**All {len(healthy_followers)} follower shard(s) are in sync**")
            builder.blank()

        # Auto-follow failures
        if auto_follow_failed > 0 or auto_follow_remote_failures > 0:
            builder.warning(
                f"**Auto-follow failures detected**\n\n"
                f"Failed follows: {auto_follow_failed}, Remote failures: {auto_follow_remote_failures}"
            )
            builder.blank()

        # Auto-follow patterns
        if patterns:
            builder.h4("Auto-Follow Patterns")
            pattern_table = []
            for pattern in patterns:
                pattern_name = pattern.get("name", "Unknown")
                remote_cluster = pattern.get("remote_cluster", "Unknown")
                leader_patterns = pattern.get("leader_index_patterns", [])

                pattern_table.append({
                    "Pattern": pattern_name,
                    "Remote Cluster": remote_cluster,
                    "Index Patterns": ", ".join(leader_patterns[:3]) + ("..." if len(leader_patterns) > 3 else "")
                })
            builder.table(pattern_table)
            builder.blank()

        # Recommendations
        if lagging_followers or auto_follow_failed > 0:
            builder.text("*Recommended Actions:*")
            if lagging_followers:
                builder.text("1. Check network connectivity to leader cluster")
                builder.text("2. Review leader cluster load and performance")
                builder.text("3. Consider pausing non-critical replication during issues")
            if auto_follow_failed > 0:
                builder.text("1. Check auto-follow pattern configuration")
                builder.text("2. Verify remote cluster connectivity")
                builder.text("3. Review security permissions for CCR")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "ccr_enabled": True,
                "follower_count": total_followers,
                "pattern_count": len(patterns),
                "healthy_shard_count": len(healthy_followers),
                "lagging_shard_count": len(lagging_followers),
                "auto_follow_failed": auto_follow_failed,
                "remote_failures": auto_follow_remote_failures,
                "has_ccr": total_followers > 0 or len(patterns) > 0,
                "has_lag": len(lagging_followers) > 0,
                "has_failures": auto_follow_failed > 0 or auto_follow_remote_failures > 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check CCR status: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

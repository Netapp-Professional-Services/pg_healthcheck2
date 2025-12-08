"""
Rollup job status for Elasticsearch clusters.

This check analyzes rollup jobs to identify:
- Rollup job inventory and configuration
- Job execution status
- Failed or stopped jobs

Data source:
- Live clusters: GET /_rollup/job
- Imported diagnostics: commercial/rollup_jobs.json

Rollup jobs summarize historical data to reduce storage.
Note: Rollups are being deprecated in favor of downsampling in newer versions.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check rollup job status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Rollup Jobs")

    try:
        # Get rollup data
        rollup_jobs = connector.execute_query({"operation": "rollup_jobs"})

        if isinstance(rollup_jobs, dict) and "error" in rollup_jobs:
            error_msg = str(rollup_jobs.get("error", ""))
            if "license" in error_msg.lower():
                builder.note(
                    "Rollup jobs are not available with the current license.\n\n"
                    "Rollup requires a Basic license or higher."
                )
                return builder.build(), {
                    "status": "not_licensed",
                    "reason": "License does not include rollup"
                }

            builder.note(
                "Rollup job information is not available.\n\n"
                "This may indicate rollup is not configured or diagnostic data is missing."
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": rollup_jobs.get("error", "Unknown")
            }

        jobs_list = rollup_jobs.get("jobs", [])

        if not jobs_list:
            builder.note(
                "**No rollup jobs configured**\n\n"
                "Rollup jobs are used to summarize historical data. "
                "No action required if not using rollups.\n\n"
                "*Note:* Rollup is being deprecated in favor of downsampling in Elasticsearch 8.x+."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "job_count": 0,
                    "has_rollups": False
                }
            }

        # Analyze jobs
        analyzed_jobs = []
        started_jobs = []
        stopped_jobs = []
        failed_jobs = []

        for job_entry in jobs_list:
            config = job_entry.get("config", {})
            status = job_entry.get("status", {})
            stats = job_entry.get("stats", {})

            job_id = config.get("id", "Unknown")
            job_state = status.get("job_state", "unknown")
            index_pattern = config.get("index_pattern", "Unknown")
            rollup_index = config.get("rollup_index", "Unknown")

            job_info = {
                "id": job_id,
                "state": job_state,
                "index_pattern": index_pattern,
                "rollup_index": rollup_index,
                "documents_processed": stats.get("documents_processed", 0),
                "pages_processed": stats.get("pages_processed", 0),
                "rollups_indexed": stats.get("rollups_indexed", 0)
            }
            analyzed_jobs.append(job_info)

            if job_state == "started":
                started_jobs.append(job_info)
            elif job_state == "stopped":
                stopped_jobs.append(job_info)
            elif job_state in ["failed", "aborting"]:
                failed_jobs.append(job_info)

        # Build report
        builder.h4("Rollup Summary")

        summary_data = [
            {"Metric": "Total Rollup Jobs", "Value": str(len(analyzed_jobs))},
            {"Metric": "Started", "Value": str(len(started_jobs))},
            {"Metric": "Stopped", "Value": str(len(stopped_jobs))},
            {"Metric": "Failed", "Value": str(len(failed_jobs))}
        ]
        builder.table(summary_data)
        builder.blank()

        # Deprecation notice
        builder.note(
            "*Note:* Rollup jobs are deprecated in Elasticsearch 8.x. "
            "Consider migrating to downsampling for new use cases."
        )
        builder.blank()

        # Alert on issues
        if failed_jobs:
            builder.critical(
                f"**{len(failed_jobs)} rollup job(s) in FAILED state**"
            )
            for job in failed_jobs:
                builder.text(f"- **{job['id']}**: {job['index_pattern']} → {job['rollup_index']}")
            builder.blank()

        if stopped_jobs:
            builder.warning(
                f"**{len(stopped_jobs)} rollup job(s) are stopped**"
            )
            builder.blank()

        if not failed_jobs and not stopped_jobs and started_jobs:
            builder.success(f"**All {len(started_jobs)} rollup job(s) are running**")
            builder.blank()

        # Job details
        if analyzed_jobs:
            builder.h4("Rollup Job Details")

            job_table = []
            for job in analyzed_jobs:
                state = job["state"]
                if state == "started":
                    state_display = "✅ Running"
                elif state == "stopped":
                    state_display = "⚠️ Stopped"
                elif state in ["failed", "aborting"]:
                    state_display = "🔴 Failed"
                else:
                    state_display = state

                job_table.append({
                    "Job ID": job["id"],
                    "Source Pattern": job["index_pattern"],
                    "Rollup Index": job["rollup_index"],
                    "State": state_display,
                    "Docs Processed": f"{job['documents_processed']:,}"
                })
            builder.table(job_table)
            builder.blank()

        # Recommendations
        if failed_jobs or stopped_jobs:
            builder.text("*Recommended Actions:*")
            if failed_jobs:
                builder.text("1. Check rollup job errors: GET /_rollup/job/<job_id>")
                builder.text("2. Review source index availability")
                builder.text("3. Restart failed jobs after fixing issues")
            if stopped_jobs:
                builder.text("1. Start stopped jobs: POST /_rollup/job/<job_id>/_start")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "job_count": len(analyzed_jobs),
                "started_count": len(started_jobs),
                "stopped_count": len(stopped_jobs),
                "failed_count": len(failed_jobs),
                "has_rollups": len(analyzed_jobs) > 0,
                "has_failed": len(failed_jobs) > 0,
                "has_stopped": len(stopped_jobs) > 0,
                "all_healthy": len(failed_jobs) == 0 and len(stopped_jobs) == 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check rollup jobs: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

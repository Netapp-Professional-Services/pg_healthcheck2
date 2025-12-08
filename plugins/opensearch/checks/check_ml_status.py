"""
Machine Learning status for Elasticsearch clusters.

This check analyzes ML jobs and resources to identify:
- ML node capacity and configuration
- Anomaly detection job status
- Datafeed health
- ML resource usage

Data source:
- Live clusters: GET /_ml/info, GET /_ml/anomaly_detectors, GET /_ml/datafeeds
- Imported diagnostics: commercial/ml_info.json, commercial/ml_anomaly_detectors.json,
  commercial/ml_datafeeds.json

Note: ML is an X-Pack feature requiring a Platinum license or higher.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check Machine Learning status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Machine Learning Status")

    try:
        # Get ML data
        ml_info = connector.execute_query({"operation": "ml_info"})
        ml_jobs = connector.execute_query({"operation": "ml_anomaly_detectors"})
        ml_datafeeds = connector.execute_query({"operation": "ml_datafeeds"})

        info_available = not (isinstance(ml_info, dict) and "error" in ml_info)
        jobs_available = not (isinstance(ml_jobs, dict) and "error" in ml_jobs)
        feeds_available = not (isinstance(ml_datafeeds, dict) and "error" in ml_datafeeds)

        if not info_available and not jobs_available:
            error_msg = str(ml_info.get("error", "") if isinstance(ml_info, dict) else "")
            if "license" in error_msg.lower():
                builder.note(
                    "Machine Learning is not available with the current license.\n\n"
                    "ML requires a Platinum or Enterprise license."
                )
                return builder.build(), {
                    "status": "not_licensed",
                    "reason": "License does not include ML"
                }

            builder.note(
                "Machine Learning information is not available.\n\n"
                "This may indicate ML is not enabled or diagnostic data is missing."
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "ML data not available"
            }

        # Parse ML info
        upgrade_mode = False
        max_model_memory = "Unknown"
        native_version = "Unknown"

        if info_available and ml_info:
            defaults = ml_info.get("defaults", {})
            limits = ml_info.get("limits", {})
            native_code = ml_info.get("native_code", {})

            upgrade_mode = ml_info.get("upgrade_mode", False)
            max_model_memory = limits.get("effective_max_model_memory_limit", "Unknown")
            native_version = native_code.get("version", "Unknown")

        # Parse anomaly detectors
        job_count = 0
        jobs_list = []
        opened_jobs = []
        closed_jobs = []
        failed_jobs = []

        if jobs_available and ml_jobs:
            job_count = ml_jobs.get("count", 0)
            jobs_list = ml_jobs.get("jobs", [])

            for job in jobs_list:
                job_id = job.get("job_id", "Unknown")
                state = job.get("state", "unknown")
                model_memory = job.get("model_memory_limit", "Unknown")

                job_info = {
                    "id": job_id,
                    "state": state,
                    "model_memory": model_memory,
                    "description": job.get("description", "")
                }

                if state == "opened":
                    opened_jobs.append(job_info)
                elif state == "closed":
                    closed_jobs.append(job_info)
                elif state == "failed":
                    failed_jobs.append(job_info)

        # Parse datafeeds
        feed_count = 0
        feeds_list = []
        started_feeds = []
        stopped_feeds = []

        if feeds_available and ml_datafeeds:
            feed_count = ml_datafeeds.get("count", 0)
            feeds_list = ml_datafeeds.get("datafeeds", [])

            for feed in feeds_list:
                feed_id = feed.get("datafeed_id", "Unknown")
                state = feed.get("state", "unknown")
                job_id = feed.get("job_id", "Unknown")

                feed_info = {
                    "id": feed_id,
                    "state": state,
                    "job_id": job_id
                }

                if state == "started":
                    started_feeds.append(feed_info)
                else:
                    stopped_feeds.append(feed_info)

        # Build report
        if upgrade_mode:
            builder.warning(
                "**ML is in upgrade mode**\n\n"
                "Machine learning is temporarily disabled during upgrade."
            )
            builder.blank()

        builder.h4("ML Configuration")

        config_data = [
            {"Setting": "ML Native Version", "Value": native_version},
            {"Setting": "Max Model Memory", "Value": max_model_memory},
            {"Setting": "Upgrade Mode", "Value": "Yes" if upgrade_mode else "No"},
            {"Setting": "Total Jobs", "Value": str(job_count)},
            {"Setting": "Total Datafeeds", "Value": str(feed_count)}
        ]
        builder.table(config_data)
        builder.blank()

        # Job status
        if job_count > 0:
            builder.h4("Anomaly Detection Jobs")

            if failed_jobs:
                builder.critical(
                    f"**{len(failed_jobs)} job(s) in FAILED state**\n\n"
                    "Failed jobs are not processing data."
                )
                for job in failed_jobs:
                    builder.text(f"- **{job['id']}**")
                builder.blank()

            status_data = [
                {"Status": "Opened (Running)", "Count": str(len(opened_jobs))},
                {"Status": "Closed", "Count": str(len(closed_jobs))},
                {"Status": "Failed", "Count": str(len(failed_jobs))}
            ]
            builder.table(status_data)
            builder.blank()

            # Job details
            if jobs_list:
                job_table = []
                for job in jobs_list[:10]:
                    state = job.get("state", "unknown")
                    if state == "opened":
                        state_display = "✅ Running"
                    elif state == "closed":
                        state_display = "⚪ Closed"
                    elif state == "failed":
                        state_display = "🔴 Failed"
                    else:
                        state_display = state

                    job_table.append({
                        "Job ID": job.get("job_id", "Unknown"),
                        "State": state_display,
                        "Model Memory": job.get("model_memory_limit", "N/A"),
                        "Description": (job.get("description", "")[:30] + "...") if len(job.get("description", "")) > 30 else job.get("description", "")
                    })
                builder.table(job_table)

                if len(jobs_list) > 10:
                    builder.text(f"_... and {len(jobs_list) - 10} more jobs_")
                builder.blank()

        # Datafeed status
        if feed_count > 0:
            builder.h4("Datafeed Status")

            if stopped_feeds and len(opened_jobs) > 0:
                builder.warning(
                    f"**{len(stopped_feeds)} datafeed(s) are stopped**\n\n"
                    "Stopped datafeeds will not send data to their jobs."
                )
                builder.blank()

            feed_status_data = [
                {"Status": "Started", "Count": str(len(started_feeds))},
                {"Status": "Stopped", "Count": str(len(stopped_feeds))}
            ]
            builder.table(feed_status_data)
            builder.blank()

        if job_count == 0:
            builder.note(
                "**No ML jobs configured**\n\n"
                "Machine Learning is available but no anomaly detection jobs are defined."
            )

        # Recommendations
        if failed_jobs or (stopped_feeds and opened_jobs):
            builder.text("*Recommended Actions:*")
            if failed_jobs:
                builder.text("1. Check job error details: GET /_ml/anomaly_detectors/<job_id>")
                builder.text("2. Review model memory limits")
                builder.text("3. Restart failed jobs after fixing issues")
            if stopped_feeds and opened_jobs:
                builder.text("1. Start stopped datafeeds: POST /_ml/datafeeds/<feed_id>/_start")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "ml_available": True,
                "upgrade_mode": upgrade_mode,
                "job_count": job_count,
                "opened_job_count": len(opened_jobs),
                "closed_job_count": len(closed_jobs),
                "failed_job_count": len(failed_jobs),
                "datafeed_count": feed_count,
                "started_feed_count": len(started_feeds),
                "stopped_feed_count": len(stopped_feeds),
                "has_jobs": job_count > 0,
                "has_failed_jobs": len(failed_jobs) > 0,
                "has_stopped_feeds": len(stopped_feeds) > 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check ML status: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

"""
Transform job status for Elasticsearch/OpenSearch clusters.

This check analyzes transform jobs to identify:
- Transform job inventory and configuration
- Job execution status and health
- Failed or stopped transforms
- Transform resource usage

Data source:
- Live clusters: GET /_transform, GET /_transform/_stats
- Imported diagnostics: commercial/transform.json, commercial/transform_stats.json

Transforms continuously process and aggregate data from source indices.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check transform job status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Transform Jobs")

    try:
        # Get transform data
        transforms = connector.execute_query({"operation": "transform"})
        transform_stats = connector.execute_query({"operation": "transform_stats"})

        transforms_available = not (isinstance(transforms, dict) and "error" in transforms)
        stats_available = not (isinstance(transform_stats, dict) and "error" in transform_stats)

        if not transforms_available:
            error_msg = str(transforms.get("error", ""))
            if "license" in error_msg.lower():
                builder.note(
                    "Transforms are not available with the current license.\n\n"
                    "Transforms require a Basic license or higher."
                )
                return builder.build(), {
                    "status": "not_licensed",
                    "reason": "License does not include transforms"
                }

            builder.note(
                "Transform information is not available.\n\n"
                "This may indicate transforms are not configured or diagnostic data is missing."
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": transforms.get("error", "Unknown")
            }

        # Parse transforms
        transform_count = transforms.get("count", 0)
        transform_list = transforms.get("transforms", [])

        if transform_count == 0:
            builder.note(
                "**No transform jobs configured**\n\n"
                "Transforms are used to continuously aggregate and pivot data. "
                "No action required if transforms are not needed."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "transform_count": 0,
                    "has_transforms": False
                }
            }

        # Parse stats if available
        stats_map = {}
        if stats_available and transform_stats:
            stats_list = transform_stats.get("transforms", [])
            for stat in stats_list:
                transform_id = stat.get("id")
                if transform_id:
                    stats_map[transform_id] = stat

        # Analyze transforms
        analyzed_transforms = []
        failed_transforms = []
        stopped_transforms = []
        running_transforms = []

        for transform in transform_list:
            transform_id = transform.get("id", "Unknown")
            source = transform.get("source", {})
            dest = transform.get("dest", {})

            # Get stats for this transform
            stat = stats_map.get(transform_id, {})
            state = stat.get("state", "unknown")
            checkpointing = stat.get("checkpointing", {})
            stats_info = stat.get("stats", {})

            transform_info = {
                "id": transform_id,
                "source_indices": source.get("index", []),
                "dest_index": dest.get("index", "Unknown"),
                "state": state,
                "documents_processed": stats_info.get("documents_processed", 0),
                "pages_processed": stats_info.get("pages_processed", 0),
                "processing_time_ms": stats_info.get("processing_time_in_ms", 0),
                "checkpoint": checkpointing.get("last", {}).get("checkpoint", 0)
            }
            analyzed_transforms.append(transform_info)

            # Categorize by state
            if state == "failed":
                failed_transforms.append(transform_info)
            elif state == "stopped":
                stopped_transforms.append(transform_info)
            elif state in ["started", "indexing"]:
                running_transforms.append(transform_info)

        # Build report
        builder.h4("Transform Summary")

        summary_data = [
            {"Metric": "Total Transforms", "Value": str(transform_count)},
            {"Metric": "Running", "Value": str(len(running_transforms))},
            {"Metric": "Stopped", "Value": str(len(stopped_transforms))},
            {"Metric": "Failed", "Value": str(len(failed_transforms))}
        ]
        builder.table(summary_data)
        builder.blank()

        # Alert on issues
        if failed_transforms:
            builder.critical(
                f"**{len(failed_transforms)} transform(s) in FAILED state**\n\n"
                "Failed transforms are not processing data."
            )
            for t in failed_transforms:
                builder.text(f"- **{t['id']}**: {t['dest_index']}")
            builder.blank()

        if stopped_transforms:
            builder.warning(
                f"**{len(stopped_transforms)} transform(s) are stopped**\n\n"
                "Stopped transforms will not process new data."
            )
            builder.blank()

        if not failed_transforms and not stopped_transforms and running_transforms:
            builder.success(f"**All {len(running_transforms)} transform(s) are running**")
            builder.blank()

        # Transform details table
        if analyzed_transforms:
            builder.h4("Transform Details")

            detail_table = []
            for t in analyzed_transforms:
                state = t["state"]
                if state == "failed":
                    state_display = "🔴 Failed"
                elif state == "stopped":
                    state_display = "⚠️ Stopped"
                elif state in ["started", "indexing"]:
                    state_display = "✅ Running"
                else:
                    state_display = state

                detail_table.append({
                    "Transform ID": t["id"],
                    "Destination": t["dest_index"],
                    "State": state_display,
                    "Docs Processed": f"{t['documents_processed']:,}",
                    "Checkpoint": str(t["checkpoint"])
                })

            builder.table(detail_table)
            builder.blank()

        # Recommendations
        if failed_transforms or stopped_transforms:
            builder.text("*Recommended Actions:*")
            if failed_transforms:
                builder.text("1. Check transform error messages: GET /_transform/<id>/_stats")
                builder.text("2. Fix any source data or mapping issues")
                builder.text("3. Reset and restart failed transforms")
            if stopped_transforms:
                builder.text("1. Review why transforms were stopped")
                builder.text("2. Restart if needed: POST /_transform/<id>/_start")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "transform_count": transform_count,
                "running_count": len(running_transforms),
                "stopped_count": len(stopped_transforms),
                "failed_count": len(failed_transforms),
                "has_transforms": transform_count > 0,
                "has_failed": len(failed_transforms) > 0,
                "has_stopped": len(stopped_transforms) > 0,
                "all_healthy": len(failed_transforms) == 0 and len(stopped_transforms) == 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check transforms: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

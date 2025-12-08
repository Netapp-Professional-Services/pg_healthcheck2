"""
Data Stream analysis for Elasticsearch/OpenSearch clusters.

This check analyzes data streams to identify:
- Data stream inventory and configuration
- Backing index count and health
- Rollover status
- Storage usage

Data source:
- Live clusters: GET /_data_stream
- Imported diagnostics: commercial/data_stream.json

Data streams are used for time-series data like logs and metrics.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check data stream status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Data Streams")

    try:
        # Get data stream data
        data_streams = connector.execute_query({"operation": "data_stream"})

        if isinstance(data_streams, dict) and "error" in data_streams:
            builder.note(
                "Data stream information is not available.\n\n"
                "This may indicate data streams are not configured or diagnostic data is missing."
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": data_streams.get("error", "Unknown")
            }

        streams_list = data_streams.get("data_streams", [])

        if not streams_list:
            builder.note(
                "**No data streams configured**\n\n"
                "Data streams are used for time-series data with automatic rollover. "
                "No action required if not using data streams."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "stream_count": 0,
                    "has_data_streams": False
                }
            }

        # Analyze data streams
        analyzed_streams = []
        streams_many_indices = []
        hidden_streams = []

        for stream in streams_list:
            stream_name = stream.get("name", "Unknown")
            indices = stream.get("indices", [])
            timestamp_field = stream.get("timestamp_field", {}).get("name", "@timestamp")
            generation = stream.get("generation", 1)
            status = stream.get("status", "unknown")
            is_hidden = stream.get("hidden", False)
            template = stream.get("template", "Unknown")

            stream_info = {
                "name": stream_name,
                "index_count": len(indices),
                "generation": generation,
                "status": status,
                "hidden": is_hidden,
                "template": template,
                "timestamp_field": timestamp_field,
                "indices": indices
            }
            analyzed_streams.append(stream_info)

            # Flag streams with many backing indices
            if len(indices) > 50:
                streams_many_indices.append(stream_info)

            if is_hidden:
                hidden_streams.append(stream_info)

        # Build report
        builder.h4("Data Stream Summary")

        total_backing_indices = sum(s["index_count"] for s in analyzed_streams)
        summary_data = [
            {"Metric": "Total Data Streams", "Value": str(len(analyzed_streams))},
            {"Metric": "Total Backing Indices", "Value": str(total_backing_indices)},
            {"Metric": "Hidden Streams", "Value": str(len(hidden_streams))},
            {"Metric": "Streams with >50 Indices", "Value": str(len(streams_many_indices))}
        ]
        builder.table(summary_data)
        builder.blank()

        # Alert on streams with many indices
        if streams_many_indices:
            builder.warning(
                f"**{len(streams_many_indices)} data stream(s) have many backing indices**\n\n"
                "Consider reviewing ILM policies to manage index lifecycle."
            )
            for s in streams_many_indices:
                builder.text(f"- **{s['name']}**: {s['index_count']} indices")
            builder.blank()

        # Data stream details
        builder.h4("Data Stream Details")

        # Show user streams (non-hidden) first
        user_streams = [s for s in analyzed_streams if not s["hidden"]]
        if user_streams:
            stream_table = []
            for s in user_streams[:15]:
                status = s["status"]
                if status == "GREEN":
                    status_display = "✅ Green"
                elif status == "YELLOW":
                    status_display = "⚠️ Yellow"
                elif status == "RED":
                    status_display = "🔴 Red"
                else:
                    status_display = status

                stream_table.append({
                    "Data Stream": s["name"],
                    "Backing Indices": str(s["index_count"]),
                    "Generation": str(s["generation"]),
                    "Status": status_display,
                    "Template": s["template"]
                })
            builder.table(stream_table)

            if len(user_streams) > 15:
                builder.text(f"_... and {len(user_streams) - 15} more data streams_")
            builder.blank()

        # Hidden streams summary
        if hidden_streams:
            builder.h4("Hidden Data Streams")
            builder.text(f"_{len(hidden_streams)} hidden data stream(s) (system streams)_")
            builder.blank()

        # Health analysis
        yellow_streams = [s for s in analyzed_streams if s["status"] == "YELLOW"]
        red_streams = [s for s in analyzed_streams if s["status"] == "RED"]

        if red_streams:
            builder.critical(
                f"**{len(red_streams)} data stream(s) in RED state**\n\n"
                "Data streams with RED status have unassigned primary shards."
            )
            for s in red_streams:
                builder.text(f"- **{s['name']}**")
            builder.blank()

        if yellow_streams:
            builder.warning(
                f"**{len(yellow_streams)} data stream(s) in YELLOW state**\n\n"
                "Data streams with YELLOW status have unassigned replica shards."
            )
            builder.blank()

        # Recommendations
        if streams_many_indices or red_streams or yellow_streams:
            builder.text("*Recommended Actions:*")
            if streams_many_indices:
                builder.text("1. Review ILM policies for data streams with many indices")
                builder.text("2. Consider adding delete phase to reduce index count")
            if red_streams:
                builder.text("1. Check cluster shard allocation for RED streams")
                builder.text("2. Ensure nodes are available for primary shard allocation")
            if yellow_streams:
                builder.text("1. Add replica nodes to achieve full replication")
                builder.text("2. Or reduce replica count if single-node cluster")

        # Build structured data
        green_count = len([s for s in analyzed_streams if s["status"] == "GREEN"])
        structured_data = {
            "status": "success",
            "data": {
                "stream_count": len(analyzed_streams),
                "user_stream_count": len(user_streams),
                "hidden_stream_count": len(hidden_streams),
                "total_backing_indices": total_backing_indices,
                "green_count": green_count,
                "yellow_count": len(yellow_streams),
                "red_count": len(red_streams),
                "streams_with_many_indices": len(streams_many_indices),
                "has_data_streams": len(analyzed_streams) > 0,
                "has_issues": len(red_streams) > 0 or len(yellow_streams) > 0,
                "all_healthy": len(red_streams) == 0 and len(yellow_streams) == 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check data streams: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

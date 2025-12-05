"""
Ingest pipeline configuration audit for Elasticsearch/OpenSearch clusters.

This check analyzes ingest pipelines to identify:
- Pipeline inventory and configuration
- Processor types in use
- Potential performance concerns (heavy processors)
- Pipeline version information

Data source:
- Live clusters: GET /_ingest/pipeline
- Imported diagnostics: pipelines.json

Ingest pipelines can significantly impact indexing performance if not
properly configured.
"""

from plugins.common.check_helpers import CheckContentBuilder


# Processors that can be performance-intensive
HEAVY_PROCESSORS = {
    "grok": "CPU-intensive pattern matching",
    "script": "Dynamic scripting overhead",
    "enrich": "External lookup overhead",
    "inference": "ML model inference latency",
    "attachment": "Document parsing overhead",
    "foreach": "Loop processing overhead"
}


def run(connector, settings):
    """
    Audit ingest pipeline configuration.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Ingest Pipeline Configuration")

    try:
        # Get pipeline data
        pipelines_data = connector.execute_query({"operation": "pipelines"})

        if isinstance(pipelines_data, dict) and "error" in pipelines_data:
            builder.note(
                "Ingest pipeline information is not available.\n\n"
                "This may indicate:\n"
                "- No ingest pipelines are configured\n"
                "- Diagnostic export did not include pipelines.json"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": pipelines_data.get("error", "Unknown")
            }

        if not pipelines_data:
            builder.note(
                "**No ingest pipelines configured**\n\n"
                "Ingest pipelines are optional and used for data transformation "
                "during indexing. No action required if not needed."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "total_pipelines": 0,
                    "has_pipelines": False
                }
            }

        # Analyze pipelines
        analyzed_pipelines = []
        total_pipelines = 0
        system_pipelines = []
        user_pipelines = []
        pipelines_with_heavy_processors = []

        for pipeline_name, pipeline_data in pipelines_data.items():
            total_pipelines += 1
            pipeline_info = _analyze_pipeline(pipeline_name, pipeline_data)
            analyzed_pipelines.append(pipeline_info)

            # Categorize as system or user pipeline
            if pipeline_name.startswith("xpack_") or pipeline_name.startswith("."):
                system_pipelines.append(pipeline_info)
            else:
                user_pipelines.append(pipeline_info)

            # Check for heavy processors
            if pipeline_info["heavy_processors"]:
                pipelines_with_heavy_processors.append(pipeline_info)

        # Build report
        builder.h4("Pipeline Summary")

        summary_data = [
            {"Metric": "Total Pipelines", "Value": str(total_pipelines)},
            {"Metric": "User Pipelines", "Value": str(len(user_pipelines))},
            {"Metric": "System Pipelines", "Value": str(len(system_pipelines))},
            {"Metric": "Pipelines with Heavy Processors", "Value": str(len(pipelines_with_heavy_processors))}
        ]
        builder.table(summary_data)
        builder.blank()

        # Show user pipelines
        if user_pipelines:
            builder.h4("User-Defined Pipelines")
            pipeline_table = []
            for pipeline in user_pipelines:
                pipeline_table.append({
                    "Pipeline": pipeline["name"],
                    "Description": (pipeline["description"][:40] + "...") if len(pipeline.get("description", "")) > 40 else pipeline.get("description", "N/A"),
                    "Processors": str(pipeline["processor_count"]),
                    "Version": str(pipeline.get("version", "N/A"))
                })
            builder.table(pipeline_table)
            builder.blank()

        # Alert on heavy processors
        if pipelines_with_heavy_processors:
            builder.warning(
                f"**{len(pipelines_with_heavy_processors)} pipeline(s) use performance-intensive processors**\n\n"
                "These processors can impact indexing throughput if not optimized."
            )
            for pipeline in pipelines_with_heavy_processors:
                heavy_list = ", ".join([f"{p} ({HEAVY_PROCESSORS.get(p, 'heavy')})" for p in pipeline["heavy_processors"]])
                builder.text(f"- **{pipeline['name']}**: {heavy_list}")
            builder.blank()

        # Show processor distribution
        processor_counts = {}
        for pipeline in analyzed_pipelines:
            for processor in pipeline["processor_types"]:
                processor_counts[processor] = processor_counts.get(processor, 0) + 1

        if processor_counts:
            builder.h4("Processor Usage")
            processor_table = []
            for processor, count in sorted(processor_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                status = "Heavy" if processor in HEAVY_PROCESSORS else "Standard"
                processor_table.append({
                    "Processor Type": processor,
                    "Usage Count": str(count),
                    "Category": status
                })
            builder.table(processor_table)
            builder.blank()

        # Show system pipelines (summary only)
        if system_pipelines:
            builder.h4("System Pipelines")
            builder.text(f"_{len(system_pipelines)} system pipeline(s) found (xpack_monitoring, etc.)_")
            builder.blank()

        # Recommendations
        if pipelines_with_heavy_processors:
            builder.text("*Performance Optimization Tips:*")
            if any("grok" in p["heavy_processors"] for p in pipelines_with_heavy_processors):
                builder.text("- **Grok**: Pre-compile patterns, use specific patterns instead of generic ones")
            if any("script" in p["heavy_processors"] for p in pipelines_with_heavy_processors):
                builder.text("- **Script**: Cache compiled scripts, prefer painless over other languages")
            if any("enrich" in p["heavy_processors"] for p in pipelines_with_heavy_processors):
                builder.text("- **Enrich**: Ensure enrich indices are small and fit in memory")
            builder.text("- Monitor pipeline metrics via node stats API")
            builder.text("- Consider bulk indexing to amortize pipeline overhead")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "total_pipelines": total_pipelines,
                "user_pipeline_count": len(user_pipelines),
                "system_pipeline_count": len(system_pipelines),
                "heavy_processor_pipeline_count": len(pipelines_with_heavy_processors),
                "has_pipelines": total_pipelines > 0,
                "has_heavy_processors": len(pipelines_with_heavy_processors) > 0,
                "processor_types_used": list(processor_counts.keys())
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to audit ingest pipelines: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _analyze_pipeline(pipeline_name, pipeline_data):
    """Analyze a single ingest pipeline."""
    description = pipeline_data.get("description", "")
    version = pipeline_data.get("version")
    processors = pipeline_data.get("processors", [])

    # Extract processor types
    processor_types = []
    heavy_processors = []

    for processor in processors:
        if isinstance(processor, dict):
            for proc_type in processor.keys():
                processor_types.append(proc_type)
                if proc_type in HEAVY_PROCESSORS:
                    heavy_processors.append(proc_type)

    return {
        "name": pipeline_name,
        "description": description,
        "version": version,
        "processor_count": len(processors),
        "processor_types": processor_types,
        "heavy_processors": list(set(heavy_processors))
    }

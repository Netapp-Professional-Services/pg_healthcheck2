"""
Index template and component template audit for Elasticsearch/OpenSearch clusters.

This check analyzes templates to identify:
- Index template inventory and configuration
- Component template usage
- Template priority conflicts
- Managed vs custom templates
- Templates without ILM policies

Data source:
- Live clusters: GET /_index_template, GET /_component_template
- Imported diagnostics: index_templates.json, component_templates.json

Templates define how new indices are created and are critical for
consistent index configuration.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Audit index and component template configuration.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Index Template Configuration")

    try:
        # Get template data
        index_templates = connector.execute_query({"operation": "index_templates"})
        component_templates = connector.execute_query({"operation": "component_templates"})

        idx_available = not (isinstance(index_templates, dict) and "error" in index_templates)
        comp_available = not (isinstance(component_templates, dict) and "error" in component_templates)

        if not idx_available and not comp_available:
            builder.note(
                "Template information is not available.\n\n"
                "This may indicate:\n"
                "- No templates are configured\n"
                "- Diagnostic export did not include template data"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "Template data not available"
            }

        # Analyze index templates
        total_index_templates = 0
        system_templates = []
        user_templates = []
        templates_without_ilm = []
        data_stream_templates = []

        if idx_available and index_templates:
            templates_list = index_templates.get("index_templates", [])

            for template_entry in templates_list:
                template_name = template_entry.get("name", "unknown")
                template_data = template_entry.get("index_template", {})

                total_index_templates += 1
                template_info = _analyze_index_template(template_name, template_data)

                # Categorize
                if template_info["is_managed"]:
                    system_templates.append(template_info)
                else:
                    user_templates.append(template_info)

                # Check for data streams
                if template_info["is_data_stream"]:
                    data_stream_templates.append(template_info)

                # Check for missing ILM (user templates only)
                if not template_info["is_managed"] and not template_info["has_ilm"]:
                    templates_without_ilm.append(template_info)

        # Analyze component templates
        total_component_templates = 0
        component_list = []

        if comp_available and component_templates:
            components = component_templates.get("component_templates", [])

            for comp_entry in components:
                comp_name = comp_entry.get("name", "unknown")
                comp_data = comp_entry.get("component_template", {})

                total_component_templates += 1
                comp_info = _analyze_component_template(comp_name, comp_data)
                component_list.append(comp_info)

        # Build report
        builder.h4("Template Summary")

        summary_data = [
            {"Metric": "Total Index Templates", "Value": str(total_index_templates)},
            {"Metric": "User Index Templates", "Value": str(len(user_templates))},
            {"Metric": "System/Managed Templates", "Value": str(len(system_templates))},
            {"Metric": "Data Stream Templates", "Value": str(len(data_stream_templates))},
            {"Metric": "Component Templates", "Value": str(total_component_templates)},
            {"Metric": "Templates Without ILM", "Value": str(len(templates_without_ilm))}
        ]
        builder.table(summary_data)
        builder.blank()

        # Show user templates
        if user_templates:
            builder.h4("User-Defined Index Templates")
            template_table = []
            for template in user_templates:
                template_table.append({
                    "Template": template["name"],
                    "Patterns": ", ".join(template["patterns"][:2]) + ("..." if len(template["patterns"]) > 2 else ""),
                    "Priority": str(template["priority"]),
                    "ILM Policy": template.get("ilm_policy", "None"),
                    "Data Stream": "Yes" if template["is_data_stream"] else "No"
                })
            builder.table(template_table)
            builder.blank()

        # Alert on templates without ILM
        if templates_without_ilm:
            builder.warning(
                f"**{len(templates_without_ilm)} user template(s) without ILM policy**\n\n"
                "Indices created from these templates will not have lifecycle management. "
                "Consider adding ILM policies for automatic rollover and retention."
            )
            for template in templates_without_ilm:
                patterns = ", ".join(template["patterns"][:3])
                builder.text(f"- **{template['name']}**: {patterns}")
            builder.blank()

        # Show data stream templates
        if data_stream_templates:
            builder.h4("Data Stream Templates")
            builder.text("Templates configured for data streams (time-series data):")
            builder.blank()
            ds_table = []
            for template in data_stream_templates:
                ds_table.append({
                    "Template": template["name"],
                    "Patterns": ", ".join(template["patterns"][:2]),
                    "ILM Policy": template.get("ilm_policy", "None")
                })
            builder.table(ds_table)
            builder.blank()

        # Show component templates
        if component_list:
            builder.h4("Component Templates")
            comp_table = []
            for comp in component_list[:10]:
                comp_table.append({
                    "Component": comp["name"],
                    "Has Settings": "Yes" if comp["has_settings"] else "No",
                    "Has Mappings": "Yes" if comp["has_mappings"] else "No",
                    "Managed": "Yes" if comp["is_managed"] else "No"
                })
            builder.table(comp_table)
            if len(component_list) > 10:
                builder.text(f"_... and {len(component_list) - 10} more component templates_")
            builder.blank()

        # Show system templates (summary)
        if system_templates:
            builder.h4("System Templates")
            builder.text(f"_{len(system_templates)} system-managed template(s) (ilm-history, .watches, etc.)_")
            builder.blank()

        # Priority conflict check
        priority_map = {}
        for template in user_templates + system_templates:
            priority = template["priority"]
            for pattern in template["patterns"]:
                key = (pattern, priority)
                if key in priority_map:
                    # Potential conflict
                    builder.warning(
                        f"**Priority conflict detected**\n\n"
                        f"Pattern '{pattern}' has multiple templates at priority {priority}"
                    )
                    break
                priority_map[key] = template["name"]

        # Recommendations
        builder.text("*Template Best Practices:*")
        builder.text("- Use component templates for reusable settings and mappings")
        builder.text("- Set appropriate priorities to control template matching order")
        builder.text("- Always include ILM policies for data with retention requirements")
        builder.text("- Use data streams for append-only time-series data (logs, metrics)")
        builder.text("- Avoid overly broad patterns that may match unintended indices")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "total_index_templates": total_index_templates,
                "user_template_count": len(user_templates),
                "system_template_count": len(system_templates),
                "data_stream_template_count": len(data_stream_templates),
                "component_template_count": total_component_templates,
                "templates_without_ilm_count": len(templates_without_ilm),
                "has_templates": total_index_templates > 0,
                "has_user_templates": len(user_templates) > 0,
                "has_templates_without_ilm": len(templates_without_ilm) > 0
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to audit templates: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _analyze_index_template(template_name, template_data):
    """Analyze a single index template."""
    patterns = template_data.get("index_patterns", [])
    priority = template_data.get("priority", 0)
    composed_of = template_data.get("composed_of", [])
    meta = template_data.get("_meta", {})

    # Check if managed/system template
    is_managed = meta.get("managed", False) or template_name.startswith(".")

    # Check for data stream
    is_data_stream = "data_stream" in template_data

    # Extract template settings
    template_settings = template_data.get("template", {})
    settings = template_settings.get("settings", {})
    index_settings = settings.get("index", {})

    # Check for ILM policy
    lifecycle = index_settings.get("lifecycle", {})
    ilm_policy = lifecycle.get("name")
    has_ilm = ilm_policy is not None

    return {
        "name": template_name,
        "patterns": patterns,
        "priority": priority,
        "composed_of": composed_of,
        "is_managed": is_managed,
        "is_data_stream": is_data_stream,
        "has_ilm": has_ilm,
        "ilm_policy": ilm_policy
    }


def _analyze_component_template(comp_name, comp_data):
    """Analyze a single component template."""
    template = comp_data.get("template", {})
    meta = comp_data.get("_meta", {})

    has_settings = "settings" in template and bool(template["settings"])
    has_mappings = "mappings" in template and bool(template["mappings"])
    is_managed = meta.get("managed", False)

    return {
        "name": comp_name,
        "has_settings": has_settings,
        "has_mappings": has_mappings,
        "is_managed": is_managed,
        "version": comp_data.get("version")
    }

"""
Index Mapping Health Check - Field Explosion Detection

Analyzes index mappings to detect:
- Field explosion (too many fields per index)
- Deeply nested objects that impact performance
- Dynamic mapping that may cause uncontrolled field growth
- Mapping conflicts and type mismatches

Data source:
- Live clusters: GET /_mapping, GET /_cluster/state (metadata.indices)
- Imported diagnostics: mapping.json, cluster_state.json

Field Limits:
- Default limit: 1000 fields (index.mapping.total_fields.limit)
- Warning threshold: >500 fields
- Critical threshold: >1000 fields or approaching limit

Why This Matters:
- High field counts cause:
  - Increased memory usage (field data, mappings cache)
  - Slower query compilation
  - Cluster state bloat
  - Mapping update bottlenecks
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module."""
    return 7  # Important for cluster health


def run(connector, settings):
    """
    Analyze index mappings for field explosion and mapping health issues.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Index Mapping Health")

    try:
        # Try to get mapping data from multiple sources
        mappings = _get_mapping_data(connector)

        if not mappings:
            builder.note(
                "Mapping data is not available.\n\n"
                "This may indicate:\n"
                "- No indices exist in the cluster\n"
                "- Diagnostic export did not include mapping data\n"
                "- Permissions prevent accessing mapping information"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "no_mapping_data"
            }

        # Analyze mappings
        analysis = _analyze_mappings(mappings)

        # Build report
        total_indices = analysis["total_indices"]
        indices_analyzed = analysis["indices_analyzed"]

        builder.h4("Mapping Summary")
        summary = [
            {"Metric": "Total Indices", "Value": str(total_indices)},
            {"Metric": "Indices Analyzed", "Value": str(indices_analyzed)},
            {"Metric": "Indices with High Field Count", "Value": str(len(analysis["high_field_indices"]))},
            {"Metric": "Indices with Critical Field Count", "Value": str(len(analysis["critical_field_indices"]))},
            {"Metric": "Indices with Dynamic Mapping", "Value": str(analysis["dynamic_mapping_count"])},
        ]
        builder.table(summary)
        builder.blank()

        # Critical field explosion
        if analysis["critical_field_indices"]:
            builder.critical(
                f"🔴 **{len(analysis['critical_field_indices'])} indices have critical field counts (>1000)**\n\n"
                "These indices are at or near the default field limit and may fail to index new documents."
            )
            builder.blank()

            critical_table = []
            for idx in sorted(analysis["critical_field_indices"], key=lambda x: x["field_count"], reverse=True)[:10]:
                critical_table.append({
                    "Index": idx["name"][:40],
                    "Field Count": idx["field_count"],
                    "Limit": idx.get("limit", 1000),
                    "% Used": f"{idx['field_count'] / idx.get('limit', 1000) * 100:.0f}%",
                    "Nested Depth": idx.get("max_depth", "N/A")
                })
            builder.table(critical_table)

            if len(analysis["critical_field_indices"]) > 10:
                builder.text(f"_...and {len(analysis['critical_field_indices']) - 10} more critical indices_")
            builder.blank()

        # High field count warning
        if analysis["high_field_indices"]:
            builder.warning(
                f"⚠️ **{len(analysis['high_field_indices'])} indices have high field counts (500-1000)**\n\n"
                "These indices should be monitored and may benefit from mapping optimization."
            )
            builder.blank()

            high_table = []
            for idx in sorted(analysis["high_field_indices"], key=lambda x: x["field_count"], reverse=True)[:10]:
                high_table.append({
                    "Index": idx["name"][:40],
                    "Field Count": idx["field_count"],
                    "Dynamic": "Yes" if idx.get("has_dynamic") else "No"
                })
            builder.table(high_table)

            if len(analysis["high_field_indices"]) > 10:
                builder.text(f"_...and {len(analysis['high_field_indices']) - 10} more high-field indices_")
            builder.blank()

        # Dynamic mapping issues
        if analysis["dynamic_mapping_indices"]:
            builder.h4("Dynamic Mapping Analysis")
            builder.text(
                f"**{len(analysis['dynamic_mapping_indices'])} indices** have dynamic mapping enabled.\n\n"
                "Dynamic mapping automatically creates fields for new data but can lead to:\n"
                "- Uncontrolled field growth (field explosion)\n"
                "- Incorrect type inference\n"
                "- Inconsistent mappings across indices"
            )
            builder.blank()

        # If no issues found
        if not analysis["critical_field_indices"] and not analysis["high_field_indices"]:
            builder.success("✅ **All indices have healthy field counts**")
            builder.blank()

        # Recommendations
        if analysis["critical_field_indices"] or analysis["high_field_indices"]:
            builder.text("*Recommendations for Field Explosion:*")
            builder.text("1. **Flatten nested objects** - Reduce object hierarchy depth")
            builder.text("2. **Use flattened field type** - For dynamic key-value data")
            builder.text("3. **Disable dynamic mapping** - Set `dynamic: strict` or `dynamic: false`")
            builder.text("4. **Increase field limit** - Only if necessary: `index.mapping.total_fields.limit`")
            builder.text("5. **Consolidate fields** - Combine related fields into fewer structured fields")
            builder.text("6. **Use runtime fields** - For fields that don't need indexing")
            builder.blank()

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "total_indices": total_indices,
                "indices_analyzed": indices_analyzed,
                "critical_field_count": len(analysis["critical_field_indices"]),
                "high_field_count": len(analysis["high_field_indices"]),
                "dynamic_mapping_count": analysis["dynamic_mapping_count"],
                "max_field_count": analysis["max_field_count"],
                "avg_field_count": round(analysis["avg_field_count"], 1),
                # Signals for rules engine
                "has_field_explosion": len(analysis["critical_field_indices"]) > 0,
                "has_high_field_counts": len(analysis["high_field_indices"]) > 0,
                "has_dynamic_mapping_risk": analysis["dynamic_mapping_count"] > 5,
                "mapping_consulting_opportunity": (
                    len(analysis["critical_field_indices"]) > 0 or
                    len(analysis["high_field_indices"]) > 3
                ),
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Failed to analyze mapping health: {e}")
        builder.error(f"Failed to analyze mapping health: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _get_mapping_data(connector):
    """
    Get mapping data from available sources.

    Returns dict of {index_name: mapping_info} or None if unavailable.
    """
    mappings = {}

    # Try direct mapping API first
    mapping_data = connector.execute_query({"operation": "mappings"})
    if mapping_data and not isinstance(mapping_data, dict) or "error" not in mapping_data:
        if isinstance(mapping_data, dict) and mapping_data:
            mappings = mapping_data

    # If empty, try cluster_state for mapping info
    if not mappings:
        cluster_state = connector.execute_query({"operation": "cluster_state"})
        if cluster_state and isinstance(cluster_state, dict) and "error" not in cluster_state:
            metadata = cluster_state.get("metadata", {})
            indices = metadata.get("indices", {})
            for idx_name, idx_data in indices.items():
                if "mappings" in idx_data:
                    mappings[idx_name] = {"mappings": idx_data["mappings"]}

    # Also get index settings for field limits
    settings_data = connector.execute_query({"operation": "settings"})

    # Merge settings into mappings
    if settings_data and isinstance(settings_data, dict) and "error" not in settings_data:
        for idx_name, idx_settings in settings_data.items():
            if idx_name in mappings:
                mappings[idx_name]["settings"] = idx_settings
            elif idx_settings:
                mappings[idx_name] = {"settings": idx_settings}

    return mappings


def _analyze_mappings(mappings):
    """
    Analyze mappings for field explosion and other issues.
    """
    analysis = {
        "total_indices": len(mappings),
        "indices_analyzed": 0,
        "critical_field_indices": [],
        "high_field_indices": [],
        "dynamic_mapping_indices": [],
        "dynamic_mapping_count": 0,
        "max_field_count": 0,
        "avg_field_count": 0,
        "total_fields": 0
    }

    for index_name, index_data in mappings.items():
        # Skip system indices
        if index_name.startswith("."):
            continue

        analysis["indices_analyzed"] += 1

        # Get mapping definition
        mapping = index_data.get("mappings", {})

        # Handle different mapping formats (ES 6.x vs 7.x+)
        if "properties" in mapping:
            properties = mapping.get("properties", {})
        else:
            # ES 6.x format with type name
            for type_name, type_mapping in mapping.items():
                if isinstance(type_mapping, dict):
                    properties = type_mapping.get("properties", {})
                    break
            else:
                properties = {}

        # Count fields recursively
        field_count, max_depth = _count_fields(properties)
        analysis["total_fields"] += field_count

        if field_count > analysis["max_field_count"]:
            analysis["max_field_count"] = field_count

        # Get configured field limit
        settings = index_data.get("settings", {})
        index_settings = settings.get("index", {}) if isinstance(settings, dict) else {}
        mapping_settings = index_settings.get("mapping", {})
        total_fields_settings = mapping_settings.get("total_fields", {})
        field_limit = int(total_fields_settings.get("limit", 1000))

        # Check for dynamic mapping
        has_dynamic = mapping.get("dynamic", True) not in [False, "false", "strict"]

        index_info = {
            "name": index_name,
            "field_count": field_count,
            "limit": field_limit,
            "max_depth": max_depth,
            "has_dynamic": has_dynamic
        }

        # Categorize by severity
        if field_count >= 1000 or (field_limit and field_count >= field_limit * 0.9):
            analysis["critical_field_indices"].append(index_info)
        elif field_count >= 500:
            analysis["high_field_indices"].append(index_info)

        if has_dynamic:
            analysis["dynamic_mapping_indices"].append(index_info)
            analysis["dynamic_mapping_count"] += 1

    # Calculate average
    if analysis["indices_analyzed"] > 0:
        analysis["avg_field_count"] = analysis["total_fields"] / analysis["indices_analyzed"]

    return analysis


def _count_fields(properties, depth=0):
    """
    Recursively count fields in mapping properties.

    Returns:
        tuple: (field_count, max_depth)
    """
    if not properties or not isinstance(properties, dict):
        return 0, depth

    count = 0
    max_depth = depth

    for field_name, field_def in properties.items():
        count += 1

        if isinstance(field_def, dict):
            # Count nested properties
            nested_props = field_def.get("properties", {})
            if nested_props:
                nested_count, nested_depth = _count_fields(nested_props, depth + 1)
                count += nested_count
                max_depth = max(max_depth, nested_depth)

            # Count multi-fields
            fields = field_def.get("fields", {})
            if fields:
                count += len(fields)

    return count, max_depth

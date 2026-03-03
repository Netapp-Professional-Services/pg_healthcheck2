"""
Default Mapping Detection Check

Flags indices that do not have an explicit mapping by detecting the default
(dynamic) mapping pattern: most text fields are type "text" with a "keyword"
subfield, which is how OpenSearch/Elasticsearch maps string fields when no
explicit mapping is defined.

Data source:
- Live clusters: GET /_mapping (index=_all), GET /_cluster/state
- Imported diagnostics: mapping.json, cluster_state.json
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)

# OpenSearch-generated index name prefixes to exclude from default mapping check
OPENSEARCH_GENERATED_INDEX_PREFIXES = ("security-auditlog-", "top_queries-")


def _is_opensearch_generated(index_name):
    """Return True if the index is an OpenSearch-generated index we should ignore."""
    return index_name.startswith(OPENSEARCH_GENERATED_INDEX_PREFIXES)


def get_weight():
    """Returns the importance score for this module."""
    return 6


def run(connector, settings):
    """
    Detect indices likely using default (non-explicit) mapping.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Default Mapping Detection")

    min_text_fields = 2
    threshold = 0.8
    if isinstance(settings, dict):
        min_text_fields = settings.get("default_mapping", {}).get(
            "min_text_fields", min_text_fields
        )
        threshold = settings.get("default_mapping", {}).get(
            "threshold", threshold
        )

    try:
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
                "reason": "no_mapping_data",
            }

        analysis = _analyze_default_mapping(
            mappings,
            min_text_fields=min_text_fields,
            threshold=threshold,
        )

        default_indices = analysis["default_mapping_indices"]
        indices_analyzed = analysis["indices_analyzed"]

        builder.h4("Summary")
        builder.para(
            "Indices that rely on default (dynamic) mapping have most string fields "
            "created as type 'text' with a 'keyword' subfield. This can lead to "
            "inconsistent type inference, and mapping drift."
        )
        summary = [
            {"Metric": "Indices Analyzed", "Value": str(indices_analyzed)},
            {
                "Metric": "Indices with likely default mapping",
                "Value": str(len(default_indices)),
            },
        ]
        builder.table(summary)
        builder.blank()

        if default_indices:
            builder.warning(
                f"**{len(default_indices)} index(es)** appear to use default mapping "
                "(most text fields have the standard text+keyword subfield pattern). "
                "Consider defining explicit mappings or index templates for production indices."
            )
            builder.blank()

            table_rows = []
            for idx in sorted(
                default_indices,
                key=lambda x: (x["text_total"], x["text_with_keyword"]),
                reverse=True,
            )[:20]:
                pct = (
                    (idx["text_with_keyword"] / idx["text_total"] * 100)
                    if idx["text_total"]
                    else 0
                )
                table_rows.append({
                    "Index": idx["name"][:50],
                    "Text fields": idx["text_total"],
                    "With keyword subfield": idx["text_with_keyword"],
                    "% default-like": f"{pct:.0f}%",
                })
            builder.table(table_rows)
            if len(default_indices) > 20:
                builder.text(
                    f"_...and {len(default_indices) - 20} more indices_"
                )
            builder.blank()

            builder.text("*Recommendations:*")
            builder.text(
                "1. Define explicit mappings for production indices (field types, analyzers)."
            )
            builder.text(
                "2. Use index templates to enforce consistent mappings for new indices."
            )
            builder.text(
                "3. Consider disabling dynamic mapping (`dynamic: false` or `dynamic: strict`) where appropriate."
            )
            builder.blank()
        else:
            builder.success(
                "No indices were flagged as using default mapping, or no indices have enough text fields to assess."
            )
            builder.blank()

        structured_data = {
            "status": "success",
            "data": {
                "indices_analyzed": indices_analyzed,
                "default_mapping_indices": [
                    {
                        "name": idx["name"],
                        "text_total": idx["text_total"],
                        "text_with_keyword": idx["text_with_keyword"],
                    }
                    for idx in default_indices
                ],
                "has_default_mapping_risk": len(default_indices) > 0,
            },
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.exception("Failed to run default mapping check")
        builder.error(f"Failed to run default mapping check: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e),
        }


def _get_mapping_data(connector):
    """
    Get mapping data from available sources.

    Returns dict of {index_name: mapping_info}.
    """
    mappings = {}

    mapping_data = connector.execute_query({"operation": "mappings"})
    if mapping_data and isinstance(mapping_data, dict) and "error" not in mapping_data:
        if mapping_data:
            mappings = mapping_data

    if not mappings:
        cluster_state = connector.execute_query({"operation": "cluster_state"})
        if cluster_state and isinstance(cluster_state, dict) and "error" not in cluster_state:
            metadata = cluster_state.get("metadata", {})
            indices = metadata.get("indices", {})
            for idx_name, idx_data in indices.items():
                if "mappings" in idx_data:
                    mappings[idx_name] = {"mappings": idx_data["mappings"]}

    return mappings


def _get_properties(mapping):
    """Extract properties dict from mapping (OpenSearch / ES 7.x+)."""
    if not mapping or not isinstance(mapping, dict):
        return {}
    return mapping.get("properties", {})


def _count_text_and_keyword(properties):
    """
    Recursively count text fields and text fields with keyword subfield.

    Returns:
        tuple: (text_total, text_with_keyword)
    """
    if not properties or not isinstance(properties, dict):
        return 0, 0

    text_total = 0
    text_with_keyword = 0

    for _field_name, field_def in properties.items():
        if not isinstance(field_def, dict):
            continue

        if field_def.get("type") == "text":
            text_total += 1
            fields = field_def.get("fields", {})
            if isinstance(fields, dict):
                keyword_def = fields.get("keyword")
                if isinstance(keyword_def, dict) and keyword_def.get("type") == "keyword":
                    text_with_keyword += 1

        nested_props = field_def.get("properties", {})
        if nested_props:
            n_total, n_keyword = _count_text_and_keyword(nested_props)
            text_total += n_total
            text_with_keyword += n_keyword

    return text_total, text_with_keyword


def _analyze_default_mapping(mappings, min_text_fields=2, threshold=0.8):
    """
    Analyze mappings for default (text+keyword) pattern per index.

    Returns dict with default_mapping_indices, indices_analyzed.
    """
    analysis = {
        "indices_analyzed": 0,
        "default_mapping_indices": [],
    }

    for index_name, index_data in mappings.items():
        if index_name.startswith(".") or _is_opensearch_generated(index_name):
            continue

        analysis["indices_analyzed"] += 1
        mapping = index_data.get("mappings", {})
        properties = _get_properties(mapping)

        text_total, text_with_keyword = _count_text_and_keyword(properties)

        if text_total < min_text_fields:
            continue

        ratio = text_with_keyword / text_total if text_total else 0
        if ratio >= threshold:
            analysis["default_mapping_indices"].append({
                "name": index_name,
                "text_total": text_total,
                "text_with_keyword": text_with_keyword,
            })

    return analysis

"""
Non-Default Cluster Settings Check

Lists cluster settings that have been explicitly set (persistent or transient).
Informational/audit to show what has been customized.

Data source:
- Live clusters: GET /_cluster/settings
- Imported diagnostics: cluster_settings.json
"""

import logging
from typing import Dict, Any, List, Tuple

from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)

# Max rows to show in the table before adding "and N more"
MAX_TABLE_ROWS = 50


def _flatten_settings(nested_dict: Dict[str, Any], prefix: str = "") -> Dict[str, str]:
    """
    Recursively flatten nested dict to dotted keys. Values are stringified.
    OpenSearch cluster settings API returns nested structures and string values.
    """
    if not isinstance(nested_dict, dict):
        return {prefix.rstrip("."): str(nested_dict)} if prefix else {}
    result = {}
    for key, value in nested_dict.items():
        dotted = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict) and value:
            result.update(_flatten_settings(value, f"{dotted}."))
        else:
            result[dotted] = str(value) if value is not None else ""
    return result


def get_weight():
    """Returns the importance score for this check."""
    return 5


def run(connector, settings) -> Tuple[str, Dict[str, Any]]:
    """
    List cluster settings explicitly set (persistent or transient).

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Non-Default Cluster Settings")
    builder.para(
        "Settings explicitly set (persistent or transient). "
        "Persistent settings survive restarts; transient settings do not."
    )

    try:
        raw = connector.execute_query({"operation": "cluster_settings"})

        if isinstance(raw, dict) and "error" in raw:
            builder.error(f"Could not retrieve cluster settings: {raw['error']}")
            return builder.build(), {"status": "error", "error": raw["error"]}

        persistent_raw = raw.get("persistent") or {}
        transient_raw = raw.get("transient") or {}

        persistent_flat = _flatten_settings(persistent_raw)
        transient_flat = _flatten_settings(transient_raw)

        # All explicitly set settings (persistent or transient)
        override_keys = set(persistent_flat) | set(transient_flat)

        non_default: List[Dict[str, Any]] = []
        for key in sorted(override_keys):
            value = persistent_flat.get(key) or transient_flat.get(key)
            scope = "persistent" if key in persistent_flat else "transient"
            non_default.append({
                "setting": key,
                "current": value,
                "scope": scope,
            })

        if not non_default:
            builder.success(
                "No cluster settings are explicitly set. "
                "All settings are at their default (none in persistent or transient)."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "count": 0,
                    "non_default_settings": [],
                },
            }

        builder.h4("Explicitly set settings")
        builder.para(f"**{len(non_default)}** setting(s) in persistent or transient.")

        table_rows = []
        for item in non_default[:MAX_TABLE_ROWS]:
            current = item["current"] or ""
            table_rows.append({
                "Setting": item["setting"],
                "Value": current[:60] + ("…" if len(current) > 60 else ""),
                "Scope": item["scope"],
            })
        builder.table(table_rows)

        if len(non_default) > MAX_TABLE_ROWS:
            builder.para(f"_… and {len(non_default) - MAX_TABLE_ROWS} more._")

        return builder.build(), {
            "status": "success",
            "data": {
                "count": len(non_default),
                "non_default_settings": non_default,
            },
        }

    except Exception as e:
        logger.error(f"Non-default cluster settings check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        return builder.build(), {"status": "error", "error": str(e)}

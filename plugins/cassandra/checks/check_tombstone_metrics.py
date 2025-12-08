from plugins.common.check_helpers import require_ssh, CheckContentBuilder, safe_execute_query
from plugins.cassandra.utils.qrylib.qry_disk_space_per_keyspace import get_nodetool_tablestats_query
from plugins.cassandra.utils.keyspace_filter import KeyspaceFilter
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - tombstones impact read performance


def run_check_tombstone_metrics(connector, settings):
    """
    Analyzes tombstone metrics across all nodes using nodetool tablestats.

    Args:
        connector: Database connector with multi-host SSH support
        settings: Dictionary of configuration settings

    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add header
    builder.add_header(
        "Tombstone Metrics Analysis",
        "Checking for high tombstone counts across tables using `nodetool tablestats`.",
        requires_ssh=True
    )

    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        builder.add(skip_msg)
        structured_data["tombstone_metrics"] = skip_data
        return builder.build(), structured_data

    # Configurable thresholds
    mean_threshold = settings.get('tombstone_mean_threshold', 1000)
    max_threshold = settings.get('tombstone_max_threshold', 100000)

    # Execute check using query approach
    query = get_nodetool_tablestats_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool tablestats")

    if not success:
        builder.add(formatted)
        structured_data["tombstone_metrics"] = {"status": "error", "data": raw}
        return builder.build(), structured_data

    # Parse results - raw is a list of table dicts
    tables = raw if isinstance(raw, list) else []

    if not tables:
        builder.note("No table data returned from tablestats.")
        structured_data["tombstone_metrics"] = {"status": "success", "data": []}
        return builder.build(), structured_data

    # Filter user tables and find problematic ones
    # Use centralized keyspace filter for consistent filtering
    ks_filter = KeyspaceFilter(settings)

    all_tables = {}
    problematic_tables = set()

    for table_info in tables:
        keyspace = table_info.get('keyspace_name', 'unknown')
        table_name = table_info.get('table_name', 'unknown')

        # Skip excluded keyspaces (system + user-configured exclusions)
        if ks_filter.is_excluded(keyspace):
            continue

        # Extract tombstone metrics
        avg_tombstones = table_info.get('average_tombstones_per_slice_(last_five_minutes)', 0)
        max_tombstones = table_info.get('maximum_tombstones_per_slice_(last_five_minutes)', 0)

        # Try to convert to numeric if string
        try:
            avg_tombstones = float(avg_tombstones) if avg_tombstones not in ['NaN', None, ''] else 0
            max_tombstones = float(max_tombstones) if max_tombstones not in ['NaN', None, ''] else 0
        except (ValueError, TypeError):
            avg_tombstones = 0
            max_tombstones = 0

        key = (keyspace, table_name)
        all_tables[key] = {
            'avg_tombstones': avg_tombstones,
            'max_tombstones': max_tombstones
        }

        # Check thresholds
        if avg_tombstones > mean_threshold or max_tombstones > max_threshold:
            problematic_tables.add(key)

    # Display results

    if problematic_tables:
        builder.warning(
            f"**{len(problematic_tables)} table(s)** with high tombstone counts detected. "
            "High tombstones can cause read performance degradation and memory issues."
        )

        # Show problematic tables
        builder.h4("Tables with High Tombstone Counts")
        problem_data = []
        for keyspace, table_name in sorted(problematic_tables):
            table_data = all_tables.get((keyspace, table_name), {})
            avg = table_data.get('avg_tombstones', 0)
            max_val = table_data.get('max_tombstones', 0)

            problem_data.append({
                'Keyspace': keyspace,
                'Table': table_name,
                'Avg Tombstones': f"{avg:.0f}",
                'Max Tombstones per Slice': f"{max_val:.0f}"
            })

        builder.table(problem_data)

        builder.recs([
            "Review application delete patterns to reduce unnecessary tombstones",
            "Consider enabling tombstone_compaction_interval for affected tables",
            "For high-delete tables, tune gc_grace_seconds to a lower value (e.g., 1-2 days)",
            "Run targeted compaction: `nodetool compact keyspace table`",
            "For time-series data, evaluate TimeWindowCompactionStrategy (TWCS)",
            "Monitor regularly with `nodetool tablestats` or `nodetool tablehistograms`"
        ])
        status = "warning"
    elif all_tables:
        builder.note(
            f"All {len(all_tables)} user tables have acceptable tombstone levels "
            f"(avg < {mean_threshold}, max < {max_threshold})."
        )
        status = "success"
    else:
        builder.note("No user tables found or no tombstone data available.")
        status = "success"

    structured_data["tombstone_metrics"] = {
        "status": status,
        "total_tables": len(all_tables),
        "problematic_count": len(problematic_tables),
        "thresholds": {
            "mean": mean_threshold,
            "max": max_threshold
        }
    }

    return builder.build(), structured_data
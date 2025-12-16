# In plugins/cassandra/checks/disk_space_per_keyspace.py

from collections import defaultdict
import re
from plugins.cassandra.utils.qrylib.qry_disk_space_per_keyspace import get_nodetool_tablestats_query
from plugins.cassandra.utils.keyspace_filter import KeyspaceFilter
from plugins.common.check_helpers import require_ssh, format_check_header, format_recommendations, safe_execute_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 6  # Medium: Resource usage monitoring


def _parse_size(size_str):
    """
    Parse size string to bytes.

    Handles multiple formats:
    - Plain number: "12345" -> 12345 bytes
    - With unit: "1.23 GB" -> bytes
    - With unit (no space): "1.23GB" -> bytes

    Args:
        size_str: Size string or number

    Returns:
        float: Size in bytes
    """
    if not size_str or str(size_str).lower() in ['0', 'n/a', '', 'none']:
        return 0.0

    size_str = str(size_str).strip()

    try:
        # Try to parse as plain number (bytes)
        if size_str.isdigit():
            return float(size_str)

        # Try to parse as float (bytes)
        try:
            return float(size_str)
        except ValueError:
            pass

        # Extract number and unit
        num_match = re.search(r'([\d.]+)', size_str)
        if not num_match:
            return 0.0

        num = float(num_match.group(1))

        # Try to find unit
        unit_match = re.search(r'([a-z]+)', size_str.lower())
        if not unit_match:
            # No unit found, assume bytes
            return num

        unit = unit_match.group(1).lower()

        # Map unit to multiplier
        if unit.startswith('b') and len(unit) == 1:
            # Just 'b' for bytes
            return num
        elif unit.startswith('k'):
            return num * 1024
        elif unit.startswith('m'):
            return num * (1024 ** 2)
        elif unit.startswith('g'):
            return num * (1024 ** 3)
        elif unit.startswith('t'):
            return num * (1024 ** 4)
        else:
            # Unknown unit, assume bytes
            return num
    except (ValueError, IndexError, AttributeError):
        return 0.0


def run_disk_space_per_keyspace_check(connector, settings):
    """
    Performs the health check for disk space per keyspace using nodetool tablestats.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Disk Space per Keyspace and Table",
        "Analyzing disk usage across keyspaces and tables using `nodetool tablestats`. "
        "This check aggregates live disk space usage, excluding system keyspaces.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        # IMPORTANT: Return a valid structure even when skipped
        structured_data["disk_usage"] = {
            "status": "skipped",
            "reason": "SSH not configured",
            "max_percent": None,  # ← This is what the rule expects
            "total_live_gb": 0,
            "total_live_bytes": 0,
            "user_tables_count": 0
        }
        return "\n".join(adoc_content), structured_data
    
    # Execute check
    query = get_nodetool_tablestats_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool tablestats")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["disk_usage"] = {
            "status": "error", 
            "data": raw,
            "max_percent": None,
            "total_live_gb": 0,
            "total_live_bytes": 0
        }
        return "\n".join(adoc_content), structured_data
    
    # Assume raw is list of dicts with 'keyspace', 'table', 'space_used_live'
    tables = raw if isinstance(raw, list) else []
    
    if not tables:
        adoc_content.append("[NOTE]\n====\nNo table data returned from tablestats.\n====\n")
        structured_data["disk_usage"] = {
            "status": "success", 
            "data": [],
            "max_percent": 0,
            "total_live_gb": 0,
            "total_live_bytes": 0,
            "user_tables_count": 0
        }
        return "\n".join(adoc_content), structured_data
    
    # Filter system keyspaces using centralized filter
    ks_filter = KeyspaceFilter(settings)
    user_tables = [
        t for t in tables
        if not ks_filter.is_excluded(t.get('keyspace', ''))
    ]
    
    if not user_tables:
        adoc_content.append("[NOTE]\n====\nNo user tables found or all usage in system keyspaces.\n====\n")
        structured_data["disk_usage"] = {
            "status": "success", 
            "data": [], 
            "user_tables_count": 0,
            "max_percent": 0,
            "total_live_gb": 0,
            "total_live_bytes": 0
        }
        return "\n".join(adoc_content), structured_data
    
    # Aggregate live space per keyspace
    keyspace_usage = defaultdict(float)
    for t in user_tables:
        live_space_str = t.get('space_used_live', '0 bytes')
        bytes_size = _parse_size(live_space_str)
        ks = t.get('keyspace', 'unknown')
        keyspace_usage[ks] += bytes_size
    
    total_bytes = sum(keyspace_usage.values())
    if total_bytes == 0:
        adoc_content.append("[NOTE]\n====\nTotal user disk usage is 0 bytes.\n====\n")
        structured_data["disk_usage"] = {
            "status": "success", 
            "data": {}, 
            "total_live_bytes": 0,
            "total_live_gb": 0,
            "max_percent": 0,
            "user_tables_count": len(user_tables)
        }
        return "\n".join(adoc_content), structured_data
    
    # Prepare usage list
    usage_list = []
    for ks, bytes_ in keyspace_usage.items():
        gb = bytes_ / (1024 ** 3)
        percent = (bytes_ / total_bytes * 100) if total_bytes > 0 else 0
        usage_list.append({
            'keyspace': ks,
            'live_gb': round(gb, 2),
            'percent': round(percent, 2),
            'live_bytes': bytes_
        })
    
    # Sort by percentage descending
    usage_list.sort(key=lambda x: x['percent'], reverse=True)
    
    max_percent = usage_list[0]['percent'] if usage_list else 0
    
    if max_percent > 50:
        adoc_content.append(f"[WARNING]\n====\n"
                          f"Keyspace(s) with high disk usage detected (>{max_percent:.1f}% for top keyspace). "
                          f"This may indicate data skew or growth issues.\n====\n")
    else:
        adoc_content.append("[NOTE]\n====\n"
                          f"Disk usage distributed across {len(usage_list)} user keyspace(s). "
                          f"Total live space: {total_bytes / (1024**3):.2f} GB.\n====\n")

    # Summary table (using filtered user keyspaces only)
    adoc_content.append("\n==== Keyspace Disk Usage Summary")
    adoc_content.append("|===\n| Keyspace | Live Space (GB) | Percentage (%)")
    for u in usage_list:
        adoc_content.append(f"| {u['keyspace']} | {u['live_gb']} | {u['percent']}")
    adoc_content.append("|===\n")
    
    # Recommendations if high usage
    if max_percent > 50:
        recommendations = [
            "Review data distribution and consider sharding large keyspaces.",
            "Monitor keyspace growth and plan for capacity expansion.",
            "Archive or delete old/unused data if applicable.",
            "Evaluate adding nodes to the cluster for better load balancing."
        ]
        adoc_content.extend(format_recommendations(recommendations))
    
    structured_data["disk_usage"] = {
        "status": "success",
        "data": usage_list,
        "total_live_gb": round(total_bytes / (1024 ** 3), 2),
        "total_live_bytes": total_bytes,
        "user_tables_count": len(user_tables),
        "max_percent": max_percent  # ← This is what the rule needs
    }
    
    return "\n".join(adoc_content), structured_data

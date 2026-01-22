# In plugins/cassandra/checks/schema_version_consistency_check.py

from plugins.cassandra.utils.qrylib.qry_schema_version import get_nodetool_describecluster_query
from plugins.common.check_helpers import require_ssh, format_check_header, format_recommendations, safe_execute_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 9  # Critical - schema consistency affects data integrity


def run_schema_version_consistency_check(connector, settings):
    """
    Performs the schema version consistency analysis using nodetool describecluster.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Schema Version Consistency (Nodetool)",
        "Verifying that all nodes in the cluster agree on the schema version using `nodetool describecluster`.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["schema_versions"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute check
    query = get_nodetool_describecluster_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool describecluster")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["schema_versions"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Assume raw is a dict with 'schema_versions' key
    schema_versions = raw.get('schema_versions', []) if isinstance(raw, dict) else []
    
    if not schema_versions:
        adoc_content.append("[NOTE]\n====\nNo schema version information returned.\n====\n")
        structured_data["schema_versions"] = {"status": "success", "data": []}
        return "\n".join(adoc_content), structured_data
    
    # Extract versions
    all_versions = [sv.get('version') for sv in schema_versions if 'version' in sv]
    unique_versions = set(all_versions)
    
    total_nodes = sum(len(sv.get('endpoints', [])) for sv in schema_versions)
    
    if len(unique_versions) > 1:
        adoc_content.append("[CRITICAL]\n====\n"
                          f"Schema version inconsistency detected! {len(unique_versions)} different versions across {total_nodes} nodes.\n"
                          "This indicates divergent schema definitions, risking query failures and data inconsistencies.\n"
                          "====\n")
        adoc_content.append(formatted)
        
        recommendations = [
            "Identify nodes with outdated schema versions from the output above.",
            "On lagging nodes, run: `nodetool schema-pull`",
            "After syncing schema, run: `nodetool repair -full` to ensure data consistency.",
            "Investigate recent schema changes and ensure they propagate correctly."
        ]
        adoc_content.extend(format_recommendations(recommendations))
        
        inconsistent_count = len(unique_versions) - 1
        structured_data["schema_versions"] = {
            "status": "critical",
            "data": schema_versions,
            "unique_versions": list(unique_versions),
            "total_nodes": total_nodes,
            "inconsistent_count": inconsistent_count
        }
    else:
        version = unique_versions.pop() if unique_versions else "unknown"
        adoc_content.append(f"[NOTE]\n====\n"
                          f"All {total_nodes} nodes agree on schema version: {version}\n"
                          "====\n")
        adoc_content.append(formatted)
        
        structured_data["schema_versions"] = {
            "status": "success",
            "data": schema_versions,
            "unique_versions": list(unique_versions),
            "total_nodes": total_nodes,
            "inconsistent_count": 0
        }
    
    return "\n".join(adoc_content), structured_data

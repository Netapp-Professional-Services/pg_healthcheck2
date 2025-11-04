from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.common.parsers import NodetoolParser
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 9  # Critical - schema consistency affects data integrity


def run_schema_version_consistency_check(connector, settings):
    """
    Performs the schema version consistency analysis using nodetool describecluster.
    Runs on first available node to get cluster-wide schema information.

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
        "Schema Version Consistency (All Nodes)",
        "Verifying that all nodes in the cluster agree on the schema version using `nodetool describecluster`.",
        requires_ssh=True
    )

    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        builder.add(skip_msg)
        structured_data["schema_versions"] = skip_data
        return builder.build(), structured_data

    # Get SSH host to node mapping
    ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

    # Initialize parser
    parser = NodetoolParser()

    # Run describecluster on first available node (it reports cluster-wide info)
    ssh_hosts = list(connector.get_ssh_hosts())
    if not ssh_hosts:
        builder.warning("No SSH hosts configured.")
        structured_data["schema_versions"] = {"status": "error", "reason": "No SSH hosts"}
        return builder.build(), structured_data

    first_host = ssh_hosts[0]
    node_id = ssh_host_to_node.get(first_host, first_host)

    try:
        ssh_manager = connector.get_ssh_manager(first_host)
        if not ssh_manager:
            builder.warning(f"Could not get SSH manager for {first_host}")
            structured_data["schema_versions"] = {"status": "error", "reason": "No SSH manager"}
            return builder.build(), structured_data

        ssh_manager.ensure_connected()

        # Execute nodetool describecluster
        command = "nodetool describecluster"
        stdout, stderr, exit_code = ssh_manager.execute_command(command)

        if exit_code != 0:
            builder.warning(f"nodetool describecluster failed on {node_id}: {stderr}")
            structured_data["schema_versions"] = {"status": "error", "error": stderr}
            return builder.build(), structured_data

        # Parse output using NodetoolParser
        parsed = parser.parse('describecluster', stdout)
        schema_versions_list = parsed.get('schema_versions', [])

        if not schema_versions_list:
            builder.note("No schema version information returned.")
            structured_data["schema_versions"] = {"status": "success", "data": []}
            return builder.build(), structured_data

        # Extract versions
        all_versions = [sv.get('version') for sv in schema_versions_list if 'version' in sv]
        unique_versions = set(all_versions)
        total_nodes = sum(len(sv.get('endpoints', [])) for sv in schema_versions_list)

        if len(unique_versions) > 1:
            builder.critical(
                f"Schema version inconsistency detected! {len(unique_versions)} different versions across {total_nodes} nodes.\n"
                "This indicates divergent schema definitions, risking query failures and data inconsistencies."
            )

            # Show version breakdown
            builder.h4("Schema Version Breakdown")
            version_data = []
            for sv in schema_versions_list:
                version = sv.get('version')
                endpoints = sv.get('endpoints', [])
                version_data.append({
                    'Schema Version': version,
                    'Node Count': len(endpoints),
                    'Nodes': ', '.join(endpoints[:5]) + ('...' if len(endpoints) > 5 else '')
                })
            builder.table(version_data)

            builder.recs([
                "Identify nodes with outdated schema versions from the table above.",
                "On lagging nodes, run: `nodetool schema-pull`",
                "After syncing schema, run: `nodetool repair -full` to ensure data consistency.",
                "Investigate recent schema changes and ensure they propagate correctly."
            ])

            structured_data["schema_versions"] = {
                "status": "critical",
                "data": schema_versions_list,
                "unique_versions": list(unique_versions),
                "total_nodes": total_nodes,
                "inconsistent_count": len(unique_versions) - 1
            }
        else:
            version = unique_versions.pop() if unique_versions else "unknown"
            builder.note(f"All {total_nodes} nodes agree on schema version: {version}")

            structured_data["schema_versions"] = {
                "status": "success",
                "data": schema_versions_list,
                "unique_versions": list(unique_versions),
                "total_nodes": total_nodes,
                "inconsistent_count": 0
            }

    except Exception as e:
        logger.error(f"Failed to check schema versions: {e}")
        builder.warning(f"Failed to check schema versions: {e}")
        structured_data["schema_versions"] = {"status": "error", "error": str(e)}

    return builder.build(), structured_data

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 6  # Medium - potential disk waste and operational issues


def run_temporary_files_check(connector, settings):
    """
    Checks for temporary files in the Cassandra data directory across all nodes.

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
        "Temporary Files in Data Directory (All Nodes)",
        "Scanning for temporary files in Cassandra data directories using `find` command across all nodes.",
        requires_ssh=True
    )

    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        builder.add(skip_msg)
        structured_data["temp_files"] = skip_data
        return builder.build(), structured_data

    # Get SSH host to node mapping
    ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

    # Configurable data directory
    data_dir = settings.get('cassandra_data_dir', '/var/lib/cassandra/data')

    # Collect temp file data from all nodes
    all_node_data = []
    total_temp_files = 0
    nodes_with_temp_files = []
    errors = []

    for ssh_host in connector.get_ssh_hosts():
        node_id = ssh_host_to_node.get(ssh_host, ssh_host)

        try:
            ssh_manager = connector.get_ssh_manager(ssh_host)
            if not ssh_manager:
                continue

            ssh_manager.ensure_connected()

            # Execute find command to locate temp files
            command = f"find {data_dir} -name '*tmp*' -type f"
            stdout, stderr, exit_code = ssh_manager.execute_command(command)

            if exit_code != 0 and "No such file or directory" not in stderr:
                logger.warning(f"find command failed on {node_id}: {stderr}")
                errors.append({
                    'node': node_id,
                    'host': ssh_host,
                    'error': stderr
                })
                continue

            # Parse output
            temp_files = []
            if stdout.strip():
                lines = stdout.strip().split('\n')
                temp_files = [line.strip() for line in lines if line.strip() and not line.startswith('find:')]

            node_data = {
                'node': node_id,
                'host': ssh_host,
                'temp_file_count': len(temp_files),
                'temp_files': temp_files
            }
            all_node_data.append(node_data)

            total_temp_files += len(temp_files)

            if len(temp_files) > 0:
                nodes_with_temp_files.append(node_id)

        except Exception as e:
            logger.error(f"Failed to check temp files on {node_id}: {e}")
            errors.append({
                'node': node_id,
                'host': ssh_host,
                'error': str(e)
            })

    # Display results
    if errors:
        builder.warning(f"Could not check temp files on {len(errors)} node(s).")

    if all_node_data:
        builder.h4("Temporary Files Summary")
        builder.table([
            {
                'Node': d['node'],
                'Host': d['host'],
                'Temp Files Found': d['temp_file_count']
            }
            for d in all_node_data
        ])

    # Determine status and provide recommendations
    if total_temp_files == 0:
        builder.note(f"No temporary files found in {data_dir} across all nodes.")
        status = "success"
    else:
        builder.warning(
            f"**{total_temp_files} temporary file(s)** found across {len(nodes_with_temp_files)} node(s). "
            "These may indicate failed operations and consume disk space."
        )

        # Show detailed file listings for nodes with temp files (if not too many)
        for node_data in all_node_data:
            if node_data['temp_file_count'] > 0:
                temp_files = node_data['temp_files']
                builder.h4(f"Files on {node_data['node']}")

                if len(temp_files) <= 10:
                    for file_path in temp_files:
                        builder.add(f"* `{file_path}`")
                else:
                    builder.add(f"Found {len(temp_files)} files. First 10:")
                    for file_path in temp_files[:10]:
                        builder.add(f"* `{file_path}`")

        builder.recs([
            "Review and remove unnecessary temp files: `rm -f <file_path>` (ensure safe to delete)",
            "Investigate source: check `/var/log/cassandra/system.log` for failed compactions, repairs, or streaming",
            f"Monitor data directory regularly: consider automated cleanup of old temp files",
            "Check disk usage with `df -h {data_dir}` to assess impact",
            "If recurring, investigate compaction settings or application write patterns"
        ])
        status = "warning"

    structured_data["temp_files"] = {
        "status": status,
        "total_count": total_temp_files,
        "nodes_with_temp_files": nodes_with_temp_files,
        "node_data": all_node_data,
        "errors": errors
    }

    return builder.build(), structured_data

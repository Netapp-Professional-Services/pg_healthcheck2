from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.common.parsers import NodetoolParser
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High: Significant operational issues


def run_compaction_pending_tasks(connector, settings):
    """
    Performs the health check analysis for pending compaction tasks across all nodes.

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
        "Compaction Pending Tasks Analysis (All Nodes)",
        "Checking for pending compaction tasks using `nodetool compactionstats` across all nodes.",
        requires_ssh=True
    )

    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        builder.add(skip_msg)
        structured_data["check_result"] = skip_data
        return builder.build(), structured_data

    # Get SSH host to node mapping
    ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

    # Initialize parser
    parser = NodetoolParser()

    # Collect compaction data from all nodes
    all_node_data = []
    total_pending = 0
    total_active = 0
    nodes_with_backlog = []
    errors = []

    for ssh_host in connector.get_ssh_hosts():
        node_id = ssh_host_to_node.get(ssh_host, ssh_host)

        try:
            ssh_manager = connector.get_ssh_manager(ssh_host)
            if not ssh_manager:
                continue

            ssh_manager.ensure_connected()

            # Execute nodetool compactionstats
            command = "nodetool compactionstats"
            stdout, stderr, exit_code = ssh_manager.execute_command(command)

            if exit_code != 0:
                logger.warning(f"nodetool compactionstats failed on {node_id}: {stderr}")
                errors.append({
                    'node': node_id,
                    'host': ssh_host,
                    'error': stderr
                })
                continue

            # Parse output using NodetoolParser
            parsed = parser.parse('compactionstats', stdout)
            pending_tasks = parsed.get('pending_tasks', 0)
            active_compactions = parsed.get('active_compactions', [])

            node_data = {
                'node': node_id,
                'host': ssh_host,
                'pending_tasks': pending_tasks,
                'active_compactions': len(active_compactions)
            }
            all_node_data.append(node_data)

            total_pending += pending_tasks
            total_active += len(active_compactions)

            if pending_tasks > 0:
                nodes_with_backlog.append(node_id)

        except Exception as e:
            logger.error(f"Failed to collect compaction stats from {node_id}: {e}")
            errors.append({
                'node': node_id,
                'host': ssh_host,
                'error': str(e)
            })

    # Display results
    if errors:
        builder.warning(f"Could not collect compaction stats from {len(errors)} node(s).")

    if all_node_data:
        builder.h4("Compaction Status Summary")
        builder.table([
            {
                'Node': d['node'],
                'Host': d['host'],
                'Pending Tasks': d['pending_tasks'],
                'Active Compactions': d['active_compactions']
            }
            for d in all_node_data
        ])

    # Determine status and provide recommendations
    if total_pending == 0 and total_active == 0:
        builder.note("No pending or active compaction tasks across all nodes. Compaction is current.")
        status = "success"
    elif total_pending > 0:
        builder.warning(
            f"**{total_pending} total pending compaction tasks** detected across {len(nodes_with_backlog)} node(s). "
            "This may indicate a compaction backlog leading to performance issues and increased disk usage."
        )

        builder.recs([
            "Monitor write throughput and consider reducing if application allows.",
            "Check disk I/O with 'iostat -x 5' to identify bottlenecks.",
            "Review compaction strategy for affected keyspaces - consider LeveledCompactionStrategy for read-heavy workloads.",
            "Increase concurrent_compactors in cassandra.yaml if CPU allows (default: number of disks)."
        ])
        status = "warning"
    else:
        builder.note(f"{total_active} total active compaction(s) in progress across cluster.")
        status = "success"

    structured_data["compaction_stats"] = {
        "status": status,
        "total_pending_tasks": total_pending,
        "total_active_compactions": total_active,
        "nodes_with_backlog": nodes_with_backlog,
        "node_data": all_node_data,
        "errors": errors
    }

    return builder.build(), structured_data

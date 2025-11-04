"""
Memory usage check for Cassandra nodes.

Checks available memory across all nodes in the cluster via SSH.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def run_memory_usage_check(connector, settings):
    """
    Analyzes available memory on all Cassandra nodes using 'free -m' command.

    Args:
        connector: Cassandra connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Memory usage check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds
        threshold_mb = settings.get('cassandra_memory_threshold_mb', 512)
        threshold_percent = settings.get('cassandra_memory_threshold_percent', 10)

        builder.h3("Memory Usage Analysis (All Nodes)")
        builder.para("Checking available memory using `free -m` command.")
        builder.blank()

        # === CHECK ALL NODES ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_memory_data = []
        issues_found = False
        warning_nodes = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            node_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                ssh_manager = connector.get_ssh_manager(ssh_host)
                if not ssh_manager:
                    continue

                ssh_manager.ensure_connected()

                # Execute free -m command
                command = "free -m"
                stdout, stderr, exit_code = ssh_manager.execute_command(command)

                if exit_code != 0:
                    logger.warning(f"free command failed on {ssh_host}: {stderr}")
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': f"free command failed: {stderr}"
                    })
                    continue

                # Parse free output
                lines = stdout.strip().split('\n')
                mem_line = None
                for line in lines:
                    if line.startswith('Mem:'):
                        mem_line = line
                        break

                if not mem_line:
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': 'Could not parse free output'
                    })
                    continue

                parts = mem_line.split()
                if len(parts) >= 7:
                    total = int(parts[1])
                    used = int(parts[2])
                    free = int(parts[3])
                    available_mem = int(parts[6])
                else:
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': 'Unexpected free output format'
                    })
                    continue

                available_percent = round((available_mem / total * 100) if total > 0 else 0, 2)

                memory_info = {
                    'host': ssh_host,
                    'node_id': node_id,
                    'total_mb': total,
                    'used_mb': used,
                    'free_mb': free,
                    'available_mb': available_mem,
                    'available_percent': available_percent,
                    'low_memory': available_mem < threshold_mb or available_percent < threshold_percent
                }
                all_memory_data.append(memory_info)

                # === Check thresholds ===
                if memory_info['low_memory']:
                    issues_found = True
                    if node_id not in warning_nodes:
                        warning_nodes.append(node_id)

                    builder.warning_issue(
                        "Low Available Memory",
                        {
                            "Node": f"{node_id} ({ssh_host})",
                            "Available": f"{available_mem} MB ({available_percent}%)",
                            "Threshold": f"{threshold_mb} MB or {threshold_percent}%",
                            "Total Memory": f"{total} MB",
                            "Used": f"{used} MB"
                        }
                    )

            except Exception as e:
                logger.error(f"Error checking memory on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'node_id': node_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_memory_data:
            builder.h4("Memory Summary")

            table_lines = [
                "|===",
                "|Node|Host|Total (MB)|Used (MB)|Available (MB)|Available %"
            ]

            for mem in sorted(all_memory_data, key=lambda x: x['available_percent']):
                indicator = "⚠️ " if mem['low_memory'] else ""

                table_lines.append(
                    f"|{mem['node_id']}|{mem['host']}|{mem['total_mb']}|{mem['used_mb']}|"
                    f"{indicator}{mem['available_mb']}|{indicator}{mem['available_percent']}%"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check memory on {len(errors)} node(s):\n\n" +
                "\n".join([f"* Node {e['node_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            recommendations["high"] = [
                "**Review JVM heap:** Check `nodetool info` for heap usage and adjust if needed",
                "**Identify memory hogs:** Use `top` or `htop` to find processes consuming memory",
                "**Check for memory leaks:** Review Cassandra logs for OOM errors",
                "**Add more RAM:** Consider upgrading server memory"
            ]

            recommendations["general"] = [
                "Monitor memory usage trends over time",
                "Tune off-heap memory settings in cassandra.yaml",
                "Review concurrent read/write settings",
                "Consider reducing cache sizes if memory constrained",
                "Set up memory alerts at 20% available threshold"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Memory usage is healthy across all nodes.\n\n"
                f"All nodes have adequate available memory (above {threshold_mb} MB or {threshold_percent}%)."
            )

        # === STRUCTURED DATA ===
        structured_data["memory_usage"] = {
            "status": "success",
            "nodes_checked": len(connector.get_ssh_hosts()),
            "nodes_with_errors": len(set(e['node_id'] for e in errors)),
            "warning_nodes": warning_nodes,
            "thresholds": {
                "threshold_mb": threshold_mb,
                "threshold_percent": threshold_percent
            },
            "errors": errors,
            "data": all_memory_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Memory usage check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["memory_usage"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data
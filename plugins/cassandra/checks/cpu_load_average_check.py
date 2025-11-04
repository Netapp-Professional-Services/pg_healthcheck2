"""
CPU Load Average check for Cassandra nodes.

Checks CPU load averages across all nodes in the cluster via SSH.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import re
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def run_cpu_load_average_check(connector, settings):
    """
    Analyzes CPU load average on all Cassandra nodes using 'uptime' command.

    Args:
        connector: Cassandra connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "CPU load check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds
        warning_threshold = settings.get('cassandra_cpu_load_warning', 2.0)
        critical_threshold = settings.get('cassandra_cpu_load_critical', 5.0)

        builder.h3("CPU Load Average Analysis (All Nodes)")
        builder.para("Checking system load average using `uptime` command.")
        builder.blank()

        # === CHECK ALL NODES ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_load_data = []
        issues_found = False
        critical_nodes = []
        warning_nodes = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            node_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                ssh_manager = connector.get_ssh_manager(ssh_host)
                if not ssh_manager:
                    continue

                ssh_manager.ensure_connected()

                # Execute uptime command
                command = "uptime"
                stdout, stderr, exit_code = ssh_manager.execute_command(command)

                if exit_code != 0:
                    logger.warning(f"uptime command failed on {ssh_host}: {stderr}")
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': f"uptime command failed: {stderr}"
                    })
                    continue

                # Parse uptime output
                # Example: " 10:23:45 up 5 days,  3:21,  2 users,  load average: 0.52, 0.58, 0.59"
                load_match = re.search(r'load average:\s*([\d.]+),\s*([\d.]+),\s*([\d.]+)', stdout)

                if not load_match:
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': 'Could not parse load average from uptime output'
                    })
                    continue

                load1, load5, load15 = map(float, load_match.groups())
                max_load = max(load1, load5, load15)

                load_info = {
                    'host': ssh_host,
                    'node_id': node_id,
                    'load_1min': load1,
                    'load_5min': load5,
                    'load_15min': load15,
                    'max_load': max_load,
                    'exceeds_critical': max_load >= critical_threshold,
                    'exceeds_warning': max_load >= warning_threshold
                }
                all_load_data.append(load_info)

                # === Check thresholds ===
                if max_load >= critical_threshold:
                    issues_found = True
                    if node_id not in critical_nodes:
                        critical_nodes.append(node_id)

                    builder.critical_issue(
                        "Critical CPU Load",
                        {
                            "Node": f"{node_id} ({ssh_host})",
                            "Load 1min": f"{load1:.2f}",
                            "Load 5min": f"{load5:.2f}",
                            "Load 15min": f"{load15:.2f}",
                            "Max Load": f"{max_load:.2f}",
                            "Threshold": f"{critical_threshold:.2f}"
                        }
                    )
                    builder.para("**⚠️ High CPU load detected - immediate investigation required!**")
                    builder.blank()

                elif max_load >= warning_threshold:
                    issues_found = True
                    if node_id not in warning_nodes:
                        warning_nodes.append(node_id)

                    builder.warning_issue(
                        "Elevated CPU Load",
                        {
                            "Node": f"{node_id} ({ssh_host})",
                            "Load 1min": f"{load1:.2f}",
                            "Load 5min": f"{load5:.2f}",
                            "Load 15min": f"{load15:.2f}",
                            "Max Load": f"{max_load:.2f}",
                            "Threshold": f"{warning_threshold:.2f}"
                        }
                    )

            except Exception as e:
                logger.error(f"Error checking CPU load on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'node_id': node_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_load_data:
            builder.h4("CPU Load Summary")

            table_lines = [
                "|===",
                "|Node|Host|Load 1min|Load 5min|Load 15min|Max Load"
            ]

            for load in sorted(all_load_data, key=lambda x: x['max_load'], reverse=True):
                indicator = ""
                if load['exceeds_critical']:
                    indicator = "🔴 "
                elif load['exceeds_warning']:
                    indicator = "⚠️ "

                table_lines.append(
                    f"|{load['node_id']}|{load['host']}|{load['load_1min']:.2f}|"
                    f"{load['load_5min']:.2f}|{load['load_15min']:.2f}|{indicator}{load['max_load']:.2f}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check CPU load on {len(errors)} node(s):\n\n" +
                "\n".join([f"* Node {e['node_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_nodes:
                recommendations["critical"] = [
                    "**Identify CPU-intensive processes:** Use `top -c` or `htop` to find high CPU consumers",
                    "**Check Cassandra activity:** Run `nodetool tpstats` to identify thread pool bottlenecks",
                    "**Review compaction:** Use `nodetool compactionstats` - heavy compactions can spike CPU",
                    "**Analyze queries:** Check for inefficient CQL queries or full table scans",
                    "**Scale vertically:** Add more CPU cores or scale horizontally by adding nodes"
                ]

            if warning_nodes:
                recommendations["high"] = [
                    "**Monitor trends:** Track CPU usage over time to identify patterns",
                    "**Optimize compaction strategy:** Consider adjusting compaction settings for CPU-heavy tables",
                    "**Check for hot partitions:** Identify and resolve uneven data distribution",
                    "**Review query patterns:** Look for inefficient queries causing CPU spikes"
                ]

            recommendations["general"] = [
                "Set up CPU load alerts at 70% of CPU core count",
                "Monitor CPU usage correlation with query patterns",
                "Review Cassandra JMX metrics for CPU-related issues",
                "Consider load balancing across the cluster",
                "Regular performance testing to establish baselines"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"CPU load is healthy across all nodes.\n\n"
                f"All nodes have load averages below {warning_threshold:.2f}."
            )

        # === STRUCTURED DATA ===
        structured_data["cpu_load"] = {
            "status": "success",
            "nodes_checked": len(connector.get_ssh_hosts()),
            "nodes_with_errors": len(set(e['node_id'] for e in errors)),
            "critical_nodes": critical_nodes,
            "warning_nodes": warning_nodes,
            "thresholds": {
                "warning": warning_threshold,
                "critical": critical_threshold
            },
            "errors": errors,
            "data": all_load_data
        }

    except Exception as e:
        import traceback
        logger.error(f"CPU load check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["cpu_load"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data
"""
Data directory disk space check for Cassandra nodes.

Checks disk space for Cassandra data directories across all nodes
in the cluster via SSH.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def run_data_directory_disk_space_check(connector, settings):
    """
    Checks disk space for Cassandra data directories on all nodes via SSH.

    Args:
        connector: Cassandra connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Disk space check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds
        warning_percent = settings.get('cassandra_disk_warning_percent', 80)
        critical_percent = settings.get('cassandra_disk_critical_percent', 90)

        builder.h3("Data Directory Disk Space (All Nodes)")
        builder.para("Checking disk space for Cassandra data directory (`/var/lib/cassandra`).")
        builder.blank()

        # Standard Cassandra data directory
        data_dir = '/var/lib/cassandra'

        # === CHECK ALL NODES ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_disk_data = []
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

                # Check if directory exists
                check_cmd = f"test -d {data_dir} && echo 'EXISTS' || echo 'NOT_EXISTS'"
                check_out, _, _ = ssh_manager.execute_command(check_cmd)

                if 'NOT_EXISTS' in check_out:
                    logger.debug(f"Directory {data_dir} does not exist on {ssh_host}, skipping")
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'data_dir': data_dir,
                        'error': 'Directory does not exist'
                    })
                    continue

                # Execute df command
                command = f"df -h {data_dir}"
                stdout, stderr, exit_code = ssh_manager.execute_command(command)

                if exit_code != 0:
                    logger.warning(f"df command failed on {ssh_host}: {stderr}")
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'data_dir': data_dir,
                        'error': f"df command failed: {stderr}"
                    })
                    continue

                # Parse df output
                lines = stdout.strip().split('\n')
                for line in lines:
                    if line.startswith('Filesystem'):
                        continue  # Skip header

                    parts = line.split()
                    if len(parts) >= 6:
                        filesystem = parts[0]
                        size = parts[1]
                        used = parts[2]
                        available = parts[3]
                        use_percent_str = parts[4].rstrip('%')
                        mount_point = ' '.join(parts[5:])

                        try:
                            use_percent = int(use_percent_str)
                        except ValueError:
                            continue

                        disk_info = {
                            'host': ssh_host,
                            'node_id': node_id,
                            'data_dir': data_dir,
                            'filesystem': filesystem,
                            'mount_point': mount_point,
                            'size': size,
                            'used': used,
                            'available': available,
                            'use_percent': use_percent,
                            'exceeds_critical': use_percent >= critical_percent,
                            'exceeds_warning': use_percent >= warning_percent
                        }
                        all_disk_data.append(disk_info)

                        # === Check thresholds ===
                        if use_percent >= critical_percent:
                            issues_found = True
                            if node_id not in critical_nodes:
                                critical_nodes.append(node_id)

                            builder.critical_issue(
                                "Critical Disk Space",
                                {
                                    "Node": f"{node_id} ({ssh_host})",
                                    "Data Directory": data_dir,
                                    "Mount": mount_point,
                                    "Usage": f"{use_percent}% (threshold: {critical_percent}%)",
                                    "Available": f"{available} of {size}",
                                    "Filesystem": filesystem
                                }
                            )
                            builder.para("**⚠️ Immediate action required to prevent write failures!**")
                            builder.blank()

                        elif use_percent >= warning_percent:
                            issues_found = True
                            if node_id not in warning_nodes:
                                warning_nodes.append(node_id)

                            builder.warning_issue(
                                "High Disk Usage",
                                {
                                    "Node": f"{node_id} ({ssh_host})",
                                    "Data Directory": data_dir,
                                    "Mount": mount_point,
                                    "Usage": f"{use_percent}% (threshold: {warning_percent}%)",
                                    "Available": f"{available} of {size}"
                                }
                            )

            except Exception as e:
                logger.error(f"Error checking disk on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'node_id': node_id,
                    'data_dir': data_dir,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_disk_data:
            builder.h4("Disk Space Summary")

            table_lines = [
                "|===",
                "|Node|Host|Data Directory|Mount|Size|Used|Available|Usage %"
            ]

            for disk in sorted(all_disk_data, key=lambda x: x['use_percent'], reverse=True):
                usage_indicator = ""
                if disk['exceeds_critical']:
                    usage_indicator = "🔴 "
                elif disk['exceeds_warning']:
                    usage_indicator = "⚠️ "

                table_lines.append(
                    f"|{disk['node_id']}|{disk['host']}|{disk['data_dir']}|{disk['mount_point']}|"
                    f"{disk['size']}|{disk['used']}|{disk['available']}|{usage_indicator}{disk['use_percent']}%"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check disk space on {len(errors)} node(s):\n\n" +
                "\n".join([f"* Node {e['node_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_nodes:
                recommendations["critical"] = [
                    "**Run `nodetool clearsnapshot`:** Remove old snapshots immediately",
                    "**Check for failed compactions:** Use `nodetool compactionstats` to identify stuck compactions",
                    "**Clean up SSTables:** Run `nodetool cleanup` after decommissioning nodes",
                    "**Expand storage:** Add capacity or migrate to larger volumes urgently",
                    "**Emergency cleanup:** Find and remove temporary files: `find /var/lib/cassandra -name '*tmp*' -type f -mtime +7 -delete`"
                ]

            if warning_nodes:
                recommendations["high"] = [
                    "**Monitor growth rate:** Track disk usage trends daily",
                    "**Review retention:** Ensure TTL is set appropriately on tables",
                    "**Plan expansion:** Add storage capacity within 1-2 weeks",
                    "**Regular snapshots cleanup:** Schedule `nodetool clearsnapshot` regularly"
                ]

            recommendations["general"] = [
                "Set up disk space alerts at 70% threshold",
                "Monitor snapshot growth: `nodetool listsnapshots`",
                "Review compaction strategy for large tables",
                "Consider enabling compression on tables with high disk usage",
                "Regular maintenance: Run `nodetool cleanup` after topology changes"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Disk space is healthy across all nodes.\n\n"
                f"All data directories are below {warning_percent}% usage."
            )

        # === STRUCTURED DATA ===
        structured_data["disk_space"] = {
            "status": "success",
            "nodes_checked": len(connector.get_ssh_hosts()),
            "data_directory": data_dir,
            "nodes_with_errors": len(set(e['node_id'] for e in errors)),
            "critical_nodes": critical_nodes,
            "warning_nodes": warning_nodes,
            "thresholds": {
                "warning_percent": warning_percent,
                "critical_percent": critical_percent
            },
            "errors": errors,
            "data": all_disk_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Data directory disk space check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["disk_space"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data
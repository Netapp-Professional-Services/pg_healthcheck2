"""
Disk usage check for Cassandra nodes.

Checks disk usage for standard Cassandra data directories across all nodes
in the cluster via SSH.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.cassandra.utils.qrylib.log_dirs_queries import get_describe_log_dirs_query
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def run_check_disk_usage(connector, settings):
    """
    Checks disk usage on all Cassandra nodes via SSH.

    Checks standard Cassandra data directories for disk space usage
    across all nodes in the cluster.

    Args:
        connector: Cassandra connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Disk usage check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds
        warning_percent = settings.get('cassandra_disk_warning_percent', 75)
        critical_percent = settings.get('cassandra_disk_critical_percent', 90)

        builder.h3("Disk Usage (All Nodes)")
        
        # === STEP 1: DISCOVER DATA DIRECTORIES FROM NODETOOL ===
        # For Cassandra, use nodetool info to get data directory paths
        # Note: This is simpler than Kafka - we just check common Cassandra paths
        # The actual paths should be in cassandra.yaml, but we'll use common defaults

        # Try to get info from nodetool to discover actual paths
        node_data_dirs = {}

        try:
            log_dirs_query = get_describe_log_dirs_query(connector)
            _, nodetool_info_raw = connector.execute_query(log_dirs_query, return_raw=True)

            # Parse nodetool info output to find data directories
            # nodetool info output includes "Data File Locations: /var/lib/cassandra/data"
            # For now, use default paths per node
            # TODO: Parse nodetool info to extract actual paths

            logger.info("Using default Cassandra data directory paths")

        except Exception as e:
            logger.warning(f"Could not query nodetool info: {e}")

        # For each SSH host, use standard Cassandra paths
        # These are configurable but these are the defaults
        standard_cassandra_paths = [
            '/var/lib/cassandra/data',
            '/var/lib/cassandra/commitlog',
            '/var/lib/cassandra'  # Fallback
        ]

        builder.para("**Checking Cassandra Data Directory Disk Usage**")
        builder.blank()
        
        # === STEP 2: MAP SSH HOSTS TO NODES ===
        # For Cassandra, we use node IPs/hostnames instead of broker IDs
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

        # === STEP 3: CHECK DISK USAGE FOR EACH NODE'S DATA DIRECTORIES ===
        all_disk_data = []
        issues_found = False
        critical_nodes = []
        warning_nodes = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            node_id = ssh_host_to_node.get(ssh_host, ssh_host)  # Use host as fallback

            # Check each standard Cassandra directory
            for data_dir in standard_cassandra_paths:
                try:
                    ssh_manager = connector.get_ssh_manager(ssh_host)
                    if not ssh_manager:
                        continue
                    
                    ssh_manager.ensure_connected()

                    # Execute df command for specific directory
                    # First check if directory exists
                    check_cmd = f"test -d {data_dir} && echo 'EXISTS' || echo 'NOT_EXISTS'"
                    check_out, _, check_exit = ssh_manager.execute_command(check_cmd)

                    if 'NOT_EXISTS' in check_out:
                        logger.debug(f"Directory {data_dir} does not exist on {ssh_host}, skipping")
                        continue  # Skip non-existent directories silently

                    command = f"df -h {data_dir}"
                    stdout, stderr, exit_code = ssh_manager.execute_command(command)

                    if exit_code != 0:
                        logger.warning(f"df command failed on {ssh_host} for {data_dir}: {stderr}")
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

                            # === INTERPRET: Check thresholds ===
                            if use_percent >= critical_percent:
                                issues_found = True
                                if node_id not in critical_nodes:
                                    critical_nodes.append(node_id)

                                builder.critical_issue(
                                    "Critical Disk Usage",
                                    {
                                        "Node": f"{node_id} ({ssh_host})",
                                        "Data Directory": data_dir,
                                        "Mount": mount_point,
                                        "Usage": f"{use_percent}% (threshold: {critical_percent}%)",
                                        "Available": f"{available} of {size}",
                                        "Filesystem": filesystem
                                    }
                                )
                                builder.para("**⚠️ Immediate action required to prevent data loss!**")
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
                    logger.error(f"Error checking disk on {ssh_host} for {data_dir}: {e}")
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'data_dir': data_dir,
                        'error': str(e)
                    })
        
        # === STEP 4: SUMMARY TABLE ===
        if all_disk_data:
            builder.h4("Disk Usage Summary")
            
            # Build table manually since indicators are pre-calculated
            table_lines = [
                "|===",
                "|Node|Host|Data Directory|Mount|Size|Used|Available|Usage %"
            ]

            for disk in sorted(all_disk_data, key=lambda x: (x['node_id'], x['use_percent']), reverse=True):
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
        
        # === STEP 5: ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check disk usage for {len(errors)} location(s):\n\n" +
                "\n".join([f"* Node {e['node_id']} ({e['host']}): {e['data_dir']} - {e['error']}"
                          for e in errors])
            )

        # === STEP 6: RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_nodes:
                recommendations["critical"] = [
                    "**Free up space immediately:** Run `nodetool cleanup` to remove old data after decommissioning nodes",
                    "**Drop unnecessary tables/keyspaces:** Remove unused data",
                    "**Expand storage:** Add capacity or migrate to larger volumes immediately",
                    "**Check compaction:** Ensure compaction is running properly with `nodetool compactionstats`",
                    "**Emergency procedure:** Consider moving data directories to a larger volume"
                ]

            if warning_nodes:
                recommendations["high"] = [
                    "**Review TTL settings:** Ensure data expiration is configured appropriately",
                    "**Run compaction:** Use `nodetool compact` to reclaim space",
                    "**Plan storage expansion:** Add capacity within 1-2 weeks",
                    "**Monitor growth rate:** Track disk usage trends to predict when critical threshold will be reached"
                ]

            recommendations["general"] = [
                "Set up disk space alerts at 70% threshold for early warning",
                "Regularly run `nodetool cleanup` after topology changes",
                "Review and adjust table TTL settings where appropriate",
                "Monitor compaction activity - pending compactions can consume significant space",
                "Regular capacity planning reviews (monthly/quarterly)"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Disk usage is within healthy limits across all nodes.\n\n"
                f"All monitored data directories are below {warning_percent}% usage."
            )
        
        # === STEP 7: STRUCTURED DATA ===
        structured_data["disk_usage"] = {
            "status": "success",
            "nodes_checked": len(connector.get_ssh_hosts()),
            "directories_checked": standard_cassandra_paths,
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
        logger.error(f"Disk usage check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Disk usage check failed: {e}")
        structured_data["disk_usage"] = {
            "status": "error",
            "details": str(e)
        }
    
    return builder.build(), structured_data

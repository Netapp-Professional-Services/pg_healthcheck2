"""
Disk usage check for OpenSearch nodes.

Checks disk usage for OpenSearch data directories across all nodes.
Adaptive check supporting multiple modes:
- AWS OpenSearch: Uses CloudWatch disk metrics
- Self-hosted with SSH: Uses SSH df commands
- REST-only: Shows message that disk check requires SSH/AWS

Requirements:
- SSH access (for self-hosted) OR AWS credentials (for AWS OpenSearch)
"""

from plugins.common.check_helpers import require_ssh, require_aws, CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def run_check_disk_usage(connector, settings):
    """
    Checks disk usage on all OpenSearch nodes.

    Adaptive check that uses CloudWatch (AWS) or SSH (self-hosted).

    Args:
        connector: OpenSearch connector instance
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Detect mode and check requirements
    if connector.environment == 'aws':
        return _run_aws_disk_check(connector, settings, builder, structured_data)
    elif connector.environment == 'self_hosted':
        return _run_ssh_disk_check(connector, settings, builder, structured_data)
    else:
        builder.h3("Disk Usage Check")
        builder.note(
            "Disk usage monitoring requires either:\n\n"
            "* SSH access for self-hosted OpenSearch\n"
            "* AWS credentials for AWS OpenSearch Service\n\n"
            "Configure the appropriate access method in your settings."
        )
        structured_data["disk_usage"] = {"status": "skipped", "reason": "No monitoring method available"}
        return builder.build(), structured_data


def _run_aws_disk_check(connector, settings, builder, structured_data):
    """Run disk check using AWS CloudWatch metrics."""

    # Check AWS availability
    aws_ok, skip_msg, skip_data = require_aws(connector, "disk usage monitoring")
    if not aws_ok:
        return skip_msg, skip_data

    try:
        warning_percent = settings.get('opensearch_disk_warning_percent', 75)
        critical_percent = settings.get('opensearch_disk_critical_percent', 85)

        builder.h3("Disk Usage (AWS CloudWatch)")
        builder.para("Monitoring disk space usage via AWS CloudWatch metrics for OpenSearch Service domain.")
        builder.blank()

        # Fetch CloudWatch disk metrics
        metrics = connector.get_cloudwatch_metrics(
            metric_names=[
                'FreeStorageSpace',
                'ClusterUsedSpace',
                'SearchableDocuments'
            ],
            period=300,
            hours_back=1
        )

        if not metrics or 'FreeStorageSpace' not in metrics:
            builder.warning("Could not retrieve disk metrics from CloudWatch")
            structured_data["disk_usage"] = {"status": "error", "details": "CloudWatch metrics unavailable"}
            return builder.build(), structured_data

        # Get latest values
        free_space_data = metrics.get('FreeStorageSpace', [])
        used_space_data = metrics.get('ClusterUsedSpace', [])

        if not free_space_data:
            builder.warning("No disk usage data available from CloudWatch")
            structured_data["disk_usage"] = {"status": "no_data"}
            return builder.build(), structured_data

        # Get most recent datapoint
        latest_free = free_space_data[-1] if free_space_data else None
        latest_used = used_space_data[-1] if used_space_data else None

        if latest_free:
            free_gb = latest_free['Average'] / 1024  # Convert MB to GB

            # Estimate total and usage percent
            # AWS OpenSearch reports free space, need to calculate used
            if latest_used:
                used_gb = latest_used['Average'] / 1024
                total_gb = free_gb + used_gb
                use_percent = (used_gb / total_gb * 100) if total_gb > 0 else 0
            else:
                # Approximate if we don't have used space
                # Assume typical provisioned storage
                use_percent = 0
                total_gb = 0

            # Check thresholds
            issues_found = False

            if use_percent >= critical_percent:
                builder.critical_issue(
                    "Critical Disk Usage (AWS OpenSearch)",
                    {
                        "Usage": f"{use_percent:.1f}% (threshold: {critical_percent}%)",
                        "Free Space": f"{free_gb:.1f} GB",
                        "Status": "🔴 CRITICAL - Immediate action required"
                    }
                )
                issues_found = True

            elif use_percent >= warning_percent:
                builder.warning_issue(
                    "High Disk Usage (AWS OpenSearch)",
                    {
                        "Usage": f"{use_percent:.1f}% (threshold: {warning_percent}%)",
                        "Free Space": f"{free_gb:.1f} GB",
                        "Status": "⚠️ WARNING - Monitor closely"
                    }
                )
                issues_found = True

            # Display metrics table
            builder.h4("Current Disk Usage")
            metrics_table = []

            if latest_used and total_gb > 0:
                metrics_table.append({
                    "Metric": "Total Storage",
                    "Value": f"{total_gb:.1f} GB"
                })
                metrics_table.append({
                    "Metric": "Used Storage",
                    "Value": f"{used_gb:.1f} GB ({use_percent:.1f}%)"
                })

            metrics_table.append({
                "Metric": "Free Storage",
                "Value": f"{free_gb:.1f} GB"
            })

            builder.table(metrics_table)

            # Recommendations
            if issues_found:
                recs = {
                    "critical": [
                        "Scale up storage immediately via AWS Console or CLI",
                        "Delete old indices or enable Index State Management (ISM) policies",
                        "Consider enabling UltraWarm for older data if available"
                    ] if use_percent >= critical_percent else [],
                    "high": [
                        "Plan storage scaling within 1-2 weeks",
                        "Review index retention policies and implement ISM",
                        "Monitor storage growth rate trends"
                    ] if use_percent >= warning_percent else [],
                    "general": [
                        "Set up CloudWatch alarms for disk space at 70% threshold",
                        "Implement automated index lifecycle policies (ISM)",
                        "Consider cold storage tier for infrequently accessed data",
                        "Regular capacity planning based on data growth rate"
                    ]
                }
                builder.recs(recs)
            else:
                builder.success(
                    f"Disk usage is healthy with {free_gb:.1f} GB free space.\n\n"
                    f"Usage is below {warning_percent}% threshold."
                )

            structured_data["disk_usage"] = {
                "status": "success",
                "mode": "aws_cloudwatch",
                "free_gb": round(free_gb, 2),
                "used_gb": round(used_gb, 2) if latest_used else None,
                "total_gb": round(total_gb, 2) if total_gb > 0 else None,
                "use_percent": round(use_percent, 1),
                "critical_threshold": critical_percent,
                "warning_threshold": warning_percent,
                "issues_found": issues_found
            }

    except Exception as e:
        logger.error(f"AWS disk usage check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["disk_usage"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _run_ssh_disk_check(connector, settings, builder, structured_data):
    """Run disk check using SSH commands (similar to Cassandra pattern)."""

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Disk usage check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds
        warning_percent = settings.get('opensearch_disk_warning_percent', 75)
        critical_percent = settings.get('opensearch_disk_critical_percent', 85)

        builder.h3("Disk Usage (All Nodes)")

        # Standard OpenSearch data directory paths
        standard_opensearch_paths = [
            '/var/lib/opensearch',
            '/usr/share/opensearch/data',
            '/opt/opensearch/data',
            '/data/opensearch'  # Common custom path
        ]

        builder.para("**Checking OpenSearch Data Directory Disk Usage**")
        builder.blank()

        # Map SSH hosts to nodes
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

        # Check disk usage for each node's data directories
        all_disk_data = []
        issues_found = False
        critical_nodes = []
        warning_nodes = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            node_id = ssh_host_to_node.get(ssh_host, ssh_host)

            # Check each standard OpenSearch directory
            for data_dir in standard_opensearch_paths:
                try:
                    ssh_manager = connector.get_ssh_manager(ssh_host)
                    if not ssh_manager:
                        continue

                    ssh_manager.ensure_connected()

                    # Check if directory exists
                    check_cmd = f"test -d {data_dir} && echo 'EXISTS' || echo 'NOT_EXISTS'"
                    check_out, _, check_exit = ssh_manager.execute_command(check_cmd)

                    if 'NOT_EXISTS' in check_out:
                        logger.debug(f"Directory {data_dir} does not exist on {ssh_host}, skipping")
                        continue

                    # Execute df command
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

                            # Check thresholds
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

        # Summary table
        if all_disk_data:
            builder.h4("Disk Usage Summary")

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

        # Error summary
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check disk usage for {len(errors)} location(s):\n\n" +
                "\n".join([f"* Node {e['node_id']} ({e['host']}): {e['data_dir']} - {e['error']}"
                          for e in errors])
            )

        # Recommendations
        if issues_found:
            recommendations = {}

            if critical_nodes:
                recommendations["critical"] = [
                    "**Free up space immediately:** Delete old indices or snapshots",
                    "**Force merge indices:** Run force merge to reduce segment overhead",
                    "**Expand storage:** Add capacity or migrate to larger volumes immediately",
                    "**Review disk watermarks:** Check cluster.routing.allocation.disk.watermark.* settings",
                    "**Emergency procedure:** Increase disk.watermark.flood_stage if absolutely necessary"
                ]

            if warning_nodes:
                recommendations["high"] = [
                    "**Implement ISM policies:** Set up automated index lifecycle management",
                    "**Review retention:** Adjust index retention policies appropriately",
                    "**Plan storage expansion:** Add capacity within 1-2 weeks",
                    "**Force merge old indices:** Optimize read-only indices to save space",
                    "**Monitor growth rate:** Track disk usage trends to predict capacity needs"
                ]

            recommendations["general"] = [
                "Set up disk space alerts at 70% threshold for early warning",
                "Implement Index State Management (ISM) for automated lifecycle policies",
                "Regular capacity planning reviews (monthly/quarterly)",
                "Monitor segment count - high segment counts increase disk usage",
                "Consider snapshot and restore for archival purposes"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Disk usage is within healthy limits across all nodes.\n\n"
                f"All monitored data directories are below {warning_percent}% usage."
            )

        # Structured data
        structured_data["disk_usage"] = {
            "status": "success",
            "mode": "ssh",
            "nodes_checked": len(connector.get_ssh_hosts()),
            "directories_checked": standard_opensearch_paths,
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

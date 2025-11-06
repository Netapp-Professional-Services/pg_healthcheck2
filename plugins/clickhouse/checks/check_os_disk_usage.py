"""
ClickHouse OS-Level Disk Usage Check (SSH-based)

Checks OS-level disk usage for ClickHouse data directories via SSH.
Complements the SQL-based disk usage check (system.disks) with OS-level view.

Similar to Instacollector's df/du functionality.

Requirements:
- SSH access to ClickHouse nodes

Checks:
- Filesystem disk usage (df -h)
- ClickHouse data directory size (du)
- Disk usage trends per node
"""

import logging
import re
from plugins.common.check_helpers import require_ssh, CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 8  # High priority - disk space is critical


def run_check_os_disk_usage(connector, settings):
    """
    Check OS-level disk usage on all ClickHouse nodes via SSH.

    Args:
        connector: ClickHouse connector instance with SSH support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "OS disk usage check")
    if not available:
        return skip_msg, skip_data

    # Add check header
    builder.h3("OS-Level Disk Usage (SSH)")
    builder.para(
        "Filesystem and ClickHouse data directory disk usage collected via SSH. "
        "Provides OS-level perspective to complement system.disks SQL data."
    )

    try:
        ssh_hosts = connector.get_ssh_hosts()

        if not ssh_hosts:
            builder.warning("No SSH hosts configured.")
            structured_data["os_disk_usage"] = {"status": "skipped", "reason": "No SSH hosts"}
            return builder.build(), structured_data

        # Get thresholds
        warning_percent = settings.get('clickhouse_ssh_disk_warning_percent', 80)
        critical_percent = settings.get('clickhouse_ssh_disk_critical_percent', 90)

        # ClickHouse data directory paths (customizable)
        clickhouse_data_paths = settings.get('clickhouse_data_paths', [
            '/var/lib/clickhouse',
            '/var/lib/clickhouse/data',
            '/var/lib/clickhouse-keeper'
        ])

        all_node_data = []
        errors = []

        # Collect data from each node
        for ssh_host in ssh_hosts:
            node_data = _collect_node_disk_data(
                connector, ssh_host, clickhouse_data_paths
            )

            if 'error' in node_data:
                errors.append(f"{ssh_host}: {node_data['error']}")
            else:
                all_node_data.append(node_data)

        # Display results
        if all_node_data:
            _display_disk_usage(builder, all_node_data, warning_percent, critical_percent, settings)

        if errors:
            builder.h4("⚠️ Collection Errors")
            for error in errors:
                builder.para(f"• {error}")
            builder.blank()

        # Structured data
        structured_data["os_disk_usage"] = {
            "status": "success",
            "nodes_checked": len(all_node_data),
            "errors": len(errors),
            "disk_data": all_node_data
        }

    except Exception as e:
        logger.error(f"OS disk usage check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["os_disk_usage"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _collect_node_disk_data(connector, ssh_host, clickhouse_data_paths):
    """
    Collect disk usage data from a single node via SSH.

    Returns:
        dict: Disk usage data from the node
    """
    data = {
        'host': ssh_host,
        'node_id': connector.ssh_host_to_node.get(ssh_host, ssh_host)
    }

    try:
        ssh_manager = connector.get_ssh_manager(ssh_host)
        if not ssh_manager:
            return {'host': ssh_host, 'error': 'No SSH manager available'}

        ssh_manager.ensure_connected()

        # 1. Get filesystem disk usage (df -h)
        try:
            df_out, _, _ = ssh_manager.execute_command("df -h")
            data['filesystems'] = _parse_df_output(df_out)
        except Exception as e:
            logger.warning(f"Failed to get df output from {ssh_host}: {e}")
            data['filesystems'] = []

        # 2. Get ClickHouse data directory sizes
        data['clickhouse_dirs'] = []

        for data_path in clickhouse_data_paths:
            try:
                # Check if directory exists first
                check_cmd = f"test -d {data_path} && echo 'exists' || echo 'not_found'"
                check_out, _, _ = ssh_manager.execute_command(check_cmd)

                if 'exists' in check_out:
                    # Get directory size
                    du_cmd = f"du -sb {data_path} 2>/dev/null || echo '0 {data_path}'"
                    du_out, _, _ = ssh_manager.execute_command(du_cmd)

                    size_bytes = _parse_du_output(du_out)

                    data['clickhouse_dirs'].append({
                        'path': data_path,
                        'size_bytes': size_bytes,
                        'size_gb': size_bytes / (1024**3) if size_bytes else 0
                    })
                else:
                    logger.debug(f"Directory {data_path} not found on {ssh_host}")

            except Exception as e:
                logger.warning(f"Failed to get size of {data_path} on {ssh_host}: {e}")

        # 3. Get inode usage (can cause "no space" errors even with free disk space)
        try:
            inode_out, _, _ = ssh_manager.execute_command("df -i")
            data['inodes'] = _parse_df_inodes(inode_out)
        except Exception as e:
            logger.debug(f"Failed to get inode usage from {ssh_host}: {e}")
            data['inodes'] = []

    except Exception as e:
        return {'host': ssh_host, 'error': str(e)}

    return data


def _parse_df_output(output):
    """
    Parse df -h output.

    Returns:
        list: List of filesystem dicts
    """
    filesystems = []
    lines = output.strip().split('\n')

    for line in lines[1:]:  # Skip header
        parts = line.split()
        if len(parts) >= 6:
            try:
                # Parse usage percentage
                use_pct_str = parts[4].replace('%', '')
                use_pct = int(use_pct_str) if use_pct_str.isdigit() else 0

                filesystems.append({
                    'filesystem': parts[0],
                    'size': parts[1],
                    'used': parts[2],
                    'available': parts[3],
                    'use_percent': use_pct,
                    'mount_point': parts[5]
                })
            except (ValueError, IndexError) as e:
                logger.debug(f"Failed to parse df line: {line} - {e}")
                continue

    return filesystems


def _parse_df_inodes(output):
    """
    Parse df -i output for inode usage.

    Returns:
        list: List of inode usage dicts
    """
    inodes = []
    lines = output.strip().split('\n')

    for line in lines[1:]:  # Skip header
        parts = line.split()
        if len(parts) >= 6:
            try:
                # Parse inode usage percentage
                iuse_pct_str = parts[4].replace('%', '')
                iuse_pct = int(iuse_pct_str) if iuse_pct_str.isdigit() else 0

                inodes.append({
                    'filesystem': parts[0],
                    'inodes': parts[1],
                    'iused': parts[2],
                    'ifree': parts[3],
                    'iuse_percent': iuse_pct,
                    'mount_point': parts[5]
                })
            except (ValueError, IndexError) as e:
                logger.debug(f"Failed to parse df -i line: {line} - {e}")
                continue

    return inodes


def _parse_du_output(output):
    """
    Parse du -sb output.

    Returns:
        int: Size in bytes
    """
    # du -sb returns: "12345 /path/to/dir"
    match = re.match(r'(\d+)', output.strip())
    if match:
        return int(match.group(1))
    return 0


def _display_disk_usage(builder, all_node_data, warning_percent, critical_percent, settings):
    """Display collected disk usage data in the report."""

    # 1. Filesystem Disk Usage Summary
    builder.h4("Filesystem Disk Usage (All Nodes)")

    critical_filesystems = []
    warning_filesystems = []
    all_filesystems = []

    for node in all_node_data:
        for fs in node.get('filesystems', []):
            fs_entry = {
                'node': node['node_id'],
                'filesystem': fs['filesystem'],
                'mount_point': fs['mount_point'],
                'size': fs['size'],
                'used': fs['used'],
                'available': fs['available'],
                'use_percent': fs['use_percent']
            }

            all_filesystems.append(fs_entry)

            if fs['use_percent'] >= critical_percent:
                critical_filesystems.append(fs_entry)
            elif fs['use_percent'] >= warning_percent:
                warning_filesystems.append(fs_entry)

    # Show critical issues first
    if critical_filesystems:
        builder.critical(
            f"🔴 **{len(critical_filesystems)} filesystem(s) critically low on space (>{critical_percent}%)**\n\n"
            "Immediate action required to prevent service disruption."
        )
        crit_table = []
        for fs in critical_filesystems:
            crit_table.append({
                "Node": fs['node'],
                "Mount Point": fs['mount_point'],
                "Filesystem": fs['filesystem'][:30],
                "Size": fs['size'],
                "Used": fs['used'],
                "Available": fs['available'],
                "Use %": f"{fs['use_percent']}%"
            })
        builder.table(crit_table)
        builder.blank()

    if warning_filesystems:
        builder.warning(
            f"⚠️ **{len(warning_filesystems)} filesystem(s) approaching capacity (>{warning_percent}%)**\n\n"
            "Monitor closely and plan for capacity expansion."
        )
        warn_table = []
        for fs in warning_filesystems:
            warn_table.append({
                "Node": fs['node'],
                "Mount Point": fs['mount_point'],
                "Size": fs['size'],
                "Used": fs['used'],
                "Available": fs['available'],
                "Use %": f"{fs['use_percent']}%"
            })
        builder.table(warn_table)
        builder.blank()

    # All filesystems table
    if all_filesystems:
        fs_table = []
        for fs in sorted(all_filesystems, key=lambda x: x['use_percent'], reverse=True)[:20]:
            status = "✅"
            if fs['use_percent'] >= critical_percent:
                status = "🔴"
            elif fs['use_percent'] >= warning_percent:
                status = "⚠️"

            fs_table.append({
                "Status": status,
                "Node": fs['node'],
                "Mount Point": fs['mount_point'],
                "Size": fs['size'],
                "Used": fs['used'],
                "Free": fs['available'],
                "Use %": f"{fs['use_percent']}%"
            })

        builder.para("**Top Filesystems by Usage:**")
        builder.table(fs_table)
        builder.blank()

    # 2. ClickHouse Data Directory Sizes
    clickhouse_dirs_exist = any(
        len(node.get('clickhouse_dirs', [])) > 0 for node in all_node_data
    )

    if clickhouse_dirs_exist:
        builder.h4("ClickHouse Data Directory Sizes")

        dir_table = []
        for node in all_node_data:
            for dir_info in node.get('clickhouse_dirs', []):
                dir_table.append({
                    "Node": node['node_id'],
                    "Path": dir_info['path'],
                    "Size (GB)": f"{dir_info['size_gb']:.2f}",
                    "Size (TB)": f"{dir_info['size_gb']/1024:.3f}" if dir_info['size_gb'] > 100 else "-"
                })

        if dir_table:
            builder.table(dir_table)
        builder.blank()

    # 3. Inode Usage (if available)
    inodes_exist = any(len(node.get('inodes', [])) > 0 for node in all_node_data)

    if inodes_exist:
        builder.h4("Inode Usage")
        builder.para(
            "High inode usage can cause 'no space' errors even with free disk space available."
        )

        high_inode_usage = []

        inode_table = []
        for node in all_node_data:
            for inode in node.get('inodes', []):
                if inode['iuse_percent'] > 80:
                    high_inode_usage.append((node['node_id'], inode['mount_point'], inode['iuse_percent']))

                inode_table.append({
                    "Node": node['node_id'],
                    "Mount Point": inode['mount_point'],
                    "Inodes": inode['inodes'],
                    "Used": inode['iused'],
                    "Free": inode['ifree'],
                    "Use %": f"{inode['iuse_percent']}%"
                })

        if high_inode_usage:
            builder.warning(
                f"⚠️ **{len(high_inode_usage)} filesystem(s) with high inode usage (>80%)**\n\n"
                "Nodes: " + ", ".join(f"{n[0]}:{n[1]} ({n[2]}%)" for n in high_inode_usage)
            )
            builder.blank()

        # Show top filesystems by inode usage
        sorted_inodes = sorted(inode_table, key=lambda x: int(x['Use %'].replace('%', '')), reverse=True)
        builder.table(sorted_inodes[:10])
        builder.blank()

    # 4. Recommendations
    recommendations = _generate_recommendations(
        all_node_data, critical_filesystems, warning_filesystems, settings
    )

    if recommendations['critical'] or recommendations['high']:
        builder.recs(recommendations)
    else:
        builder.success("✅ All filesystems have healthy disk space.")


def _generate_recommendations(all_node_data, critical_filesystems, warning_filesystems, settings):
    """Generate recommendations based on disk usage analysis."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if critical_filesystems:
        recs["critical"].extend([
            f"{len(critical_filesystems)} filesystem(s) critically low on space - immediate action required",
            "Free up space by dropping old partitions, archiving data, or OPTIMIZE TABLE",
            "Review and clean up temporary files and old parts",
            "Consider adding storage capacity or scaling to additional nodes"
        ])

    if warning_filesystems:
        recs["high"].extend([
            f"{len(warning_filesystems)} filesystem(s) approaching capacity - plan for expansion",
            "Review data retention policies and implement TTL where appropriate",
            "Identify and archive or drop unused tables",
            "Monitor disk growth trends to predict capacity needs"
        ])

    # General recommendations
    recs["general"].extend([
        "Set up monitoring and alerting for disk space (>80% usage)",
        "Implement data lifecycle management with TTL for time-series data",
        "Use OPTIMIZE TABLE to reclaim space from deleted data",
        "Review compression codecs - use ZSTD for better compression ratios",
        "Configure multiple storage policies for hot/warm/cold data tiering",
        "Use partitioning to make old data deletion more efficient",
        "Consider using S3 or object storage for cold data archival",
        "Monitor inode usage on filesystems with many small files"
    ])

    return recs

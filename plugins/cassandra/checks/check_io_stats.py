"""
I/O statistics check for Cassandra.

Analyzes iostat output to detect:
- High disk utilization
- I/O wait times
- Read/write throughput
- Device performance issues

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def parse_iostat(content):
    """
    Parse iostat output.

    Example format:
        Linux 3.10.0-862.6.3.el7.x86_64 (mvno-cass01e)   12/12/2025   _x86_64_  (24 CPU)

        12/12/2025 12:23:14 AM
        avg-cpu:  %user   %nice %system %iowait  %steal   %idle
                   4.59    3.20    1.58    0.06    0.00   90.57

        Device:   rrqm/s wrqm/s  r/s   w/s  rMB/s  wMB/s avgrq-sz avgqu-sz await r_await w_await svctm %util
        sda       0.37   8.37  35.83 11.90  4.18   3.64  335.34    0.21    4.41   1.38   13.53   0.33  1.59

    Args:
        content: Raw iostat output

    Returns:
        dict: Parsed I/O statistics
    """
    result = {
        'cpu_stats': {},
        'devices': [],
        'timestamp': None,
        'hostname': None,
        'cpu_count': None
    }

    if not content:
        return result

    lines = content.strip().split('\n')

    # Parse header line for hostname and CPU count
    if lines:
        header_match = re.search(r'\((\S+)\).*\((\d+)\s*CPU\)', lines[0])
        if header_match:
            result['hostname'] = header_match.group(1)
            result['cpu_count'] = int(header_match.group(2))

    in_cpu_section = False
    in_device_section = False
    device_headers = []

    for line in lines:
        line = line.strip()

        # Detect CPU stats section
        if line.startswith('avg-cpu:'):
            in_cpu_section = True
            in_device_section = False
            continue

        # Parse CPU stats line
        if in_cpu_section and line and not line.startswith('Device'):
            parts = line.split()
            if len(parts) >= 6:
                try:
                    result['cpu_stats'] = {
                        'user': float(parts[0]),
                        'nice': float(parts[1]),
                        'system': float(parts[2]),
                        'iowait': float(parts[3]),
                        'steal': float(parts[4]),
                        'idle': float(parts[5])
                    }
                except (ValueError, IndexError):
                    pass
            in_cpu_section = False
            continue

        # Detect device stats section
        if line.startswith('Device'):
            in_device_section = True
            # Parse headers
            device_headers = line.split()[1:]  # Skip 'Device'
            continue

        # Parse device lines
        if in_device_section and line:
            parts = line.split()
            if len(parts) >= 2:
                device = {'device': parts[0]}

                # Map values to headers
                for i, header in enumerate(device_headers):
                    if i + 1 < len(parts):
                        try:
                            device[header.lower().replace('%', 'pct_')] = float(parts[i + 1])
                        except ValueError:
                            device[header.lower().replace('%', 'pct_')] = parts[i + 1]

                result['devices'].append(device)

    return result


def run_check_io_stats(connector, settings):
    """
    Analyze I/O statistics.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Disk I/O Statistics")
    builder.para("Analyzing disk I/O performance from `iostat`.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "I/O stats")
    if not available:
        return skip_msg, skip_data

    try:
        # Get iostat from nodes
        results = connector.execute_ssh_on_all_hosts(
            "iostat -x 2>/dev/null || cat io_stat.info 2>/dev/null",
            "Get iostat"
        )

        all_stats = []
        nodes_with_high_util = []
        nodes_with_high_iowait = []

        # Thresholds
        util_warning = settings.get('io_util_warning', 70)
        util_critical = settings.get('io_util_critical', 90)
        iowait_warning = settings.get('iowait_warning', 10)
        iowait_critical = settings.get('iowait_critical', 30)

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            stats = parse_iostat(result['output'])
            stats['node'] = node_ip
            all_stats.append(stats)

            # Check for high utilization
            max_util = 0
            for device in stats['devices']:
                util = device.get('pct_util', device.get('util', 0))
                if isinstance(util, (int, float)) and util > max_util:
                    max_util = util

            if max_util >= util_critical:
                nodes_with_high_util.append({'node': node_ip, 'util': max_util, 'severity': 'critical'})
            elif max_util >= util_warning:
                nodes_with_high_util.append({'node': node_ip, 'util': max_util, 'severity': 'warning'})

            # Check for high I/O wait
            iowait = stats['cpu_stats'].get('iowait', 0)
            if iowait >= iowait_critical:
                nodes_with_high_iowait.append({'node': node_ip, 'iowait': iowait, 'severity': 'critical'})
            elif iowait >= iowait_warning:
                nodes_with_high_iowait.append({'node': node_ip, 'iowait': iowait, 'severity': 'warning'})

        if not all_stats:
            builder.note("I/O statistics not available.")
            return builder.build(), {'status': 'no_data'}

        # Summary
        critical_nodes = [n for n in nodes_with_high_util if n['severity'] == 'critical']
        warning_nodes = [n for n in nodes_with_high_util if n['severity'] == 'warning']

        if critical_nodes:
            builder.critical(f"**{len(critical_nodes)} node(s) with critical disk utilization (>{util_critical}%)**")
        elif warning_nodes:
            builder.warning(f"**{len(warning_nodes)} node(s) with high disk utilization (>{util_warning}%)**")
        else:
            builder.success("Disk utilization is healthy across all nodes.")
        builder.blank()

        # I/O wait summary
        if nodes_with_high_iowait:
            iowait_critical_nodes = [n for n in nodes_with_high_iowait if n['severity'] == 'critical']
            if iowait_critical_nodes:
                builder.critical(f"**{len(iowait_critical_nodes)} node(s) with critical I/O wait (>{iowait_critical}%)**")
            else:
                builder.warning(f"**{len(nodes_with_high_iowait)} node(s) with elevated I/O wait**")
            builder.blank()

        # CPU stats (from first node as sample)
        if all_stats[0]['cpu_stats']:
            builder.h4("CPU Utilization (Sample Node)")
            cpu = all_stats[0]['cpu_stats']
            cpu_table = [
                {'Metric': 'User', 'Value': f"{cpu.get('user', 0):.1f}%"},
                {'Metric': 'System', 'Value': f"{cpu.get('system', 0):.1f}%"},
                {'Metric': 'I/O Wait', 'Value': f"{cpu.get('iowait', 0):.1f}%"},
                {'Metric': 'Idle', 'Value': f"{cpu.get('idle', 0):.1f}%"},
            ]
            builder.table(cpu_table)
            builder.blank()

        # Device stats
        all_devices = []
        for stats in all_stats:
            for device in stats['devices']:
                # Skip virtual devices typically
                if device['device'].startswith('loop') or device['device'].startswith('ram'):
                    continue
                device['node'] = stats['node']
                all_devices.append(device)

        if all_devices:
            builder.h4("Device Statistics")
            device_table = []
            for dev in all_devices[:20]:  # Limit to 20
                util = dev.get('pct_util', dev.get('util', 'N/A'))
                util_str = f"{util:.1f}%" if isinstance(util, (int, float)) else str(util)

                # Mark high utilization
                if isinstance(util, (int, float)):
                    if util >= util_critical:
                        util_str = f"🔴 {util_str}"
                    elif util >= util_warning:
                        util_str = f"⚠️ {util_str}"

                device_table.append({
                    'Node': dev['node'],
                    'Device': dev['device'],
                    'r/s': f"{dev.get('r/s', 0):.1f}",
                    'w/s': f"{dev.get('w/s', 0):.1f}",
                    'rMB/s': f"{dev.get('rmb/s', 0):.2f}",
                    'wMB/s': f"{dev.get('wmb/s', 0):.2f}",
                    'Util': util_str
                })
            builder.table(device_table)
            builder.blank()

        # Recommendations
        if nodes_with_high_util or nodes_with_high_iowait:
            builder.h4("Recommendations")
            builder.tip("**To address I/O performance issues:**")
            if nodes_with_high_util:
                builder.text("- **High disk utilization**: Consider faster storage (NVMe SSD) or spreading load")
                builder.text("- **Review compaction**: High compaction I/O may saturate disks")
            if nodes_with_high_iowait:
                builder.text("- **High I/O wait**: Applications are waiting for disk - storage is bottleneck")
                builder.text("- **Consider adding nodes**: Distribute I/O across more nodes")
            builder.text("- **Check for disk errors**: Run `dmesg | grep -i error` for disk issues")
            builder.blank()

        # Structured data
        max_util = max([n['util'] for n in nodes_with_high_util], default=0)
        max_iowait = max([n['iowait'] for n in nodes_with_high_iowait], default=0)

        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_stats),
                'nodes_with_high_util': len(nodes_with_high_util),
                'nodes_with_critical_util': len([n for n in nodes_with_high_util if n['severity'] == 'critical']),
                'nodes_with_high_iowait': len(nodes_with_high_iowait),
                'max_disk_util': round(max_util, 1),
                'max_iowait': round(max_iowait, 1),
                'has_io_issues': len(nodes_with_high_util) > 0 or len(nodes_with_high_iowait) > 0,
                'has_critical_io': any(n['severity'] == 'critical' for n in nodes_with_high_util + nodes_with_high_iowait)
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"I/O stats check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"I/O stats check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

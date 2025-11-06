"""
ClickHouse OS-Level System Metrics Check (SSH-based)

Collects system-level metrics from ClickHouse nodes via SSH.
Similar to Instacollector's node_collector.sh functionality.

Requirements:
- SSH access to ClickHouse nodes (ssh_hosts, ssh_user, ssh_key_file/ssh_password)

Collects:
- CPU information (/proc/cpuinfo)
- Memory information (/proc/meminfo)
- Load average
- File descriptors (ulimit)
- System settings (sysctl - kernel parameters)
- Disk I/O statistics (iostat)
"""

import logging
import re
from plugins.common.check_helpers import require_ssh, CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 7  # Important - OS-level metrics provide insight into node health


def run_check_os_system_metrics(connector, settings):
    """
    Collect OS-level system metrics from all ClickHouse nodes via SSH.

    Args:
        connector: ClickHouse connector instance with SSH support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "OS system metrics check")
    if not available:
        return skip_msg, skip_data

    # Add check header
    builder.h3("OS-Level System Metrics (SSH)")
    builder.para(
        "System-level metrics collected via SSH from all ClickHouse nodes. "
        "Provides insights into CPU, memory, load, and kernel parameters."
    )

    try:
        ssh_hosts = connector.get_ssh_hosts()

        if not ssh_hosts:
            builder.warning("No SSH hosts configured.")
            structured_data["os_system_metrics"] = {"status": "skipped", "reason": "No SSH hosts"}
            return builder.build(), structured_data

        all_node_metrics = []
        errors = []

        # Collect metrics from each node
        for ssh_host in ssh_hosts:
            node_metrics = _collect_node_metrics(connector, ssh_host)

            if 'error' in node_metrics:
                errors.append(f"{ssh_host}: {node_metrics['error']}")
            else:
                all_node_metrics.append(node_metrics)

        # Display results
        if all_node_metrics:
            _display_metrics(builder, all_node_metrics, settings)

        if errors:
            builder.h4("⚠️ Collection Errors")
            for error in errors:
                builder.para(f"• {error}")
            builder.blank()

        # Structured data
        structured_data["os_system_metrics"] = {
            "status": "success",
            "nodes_checked": len(all_node_metrics),
            "errors": len(errors),
            "metrics": all_node_metrics
        }

    except Exception as e:
        logger.error(f"OS system metrics check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["os_system_metrics"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _collect_node_metrics(connector, ssh_host):
    """
    Collect system metrics from a single node via SSH.

    Returns:
        dict: Metrics from the node
    """
    metrics = {
        'host': ssh_host,
        'node_id': connector.ssh_host_to_node.get(ssh_host, ssh_host)
    }

    try:
        ssh_manager = connector.get_ssh_manager(ssh_host)
        if not ssh_manager:
            return {'host': ssh_host, 'error': 'No SSH manager available'}

        ssh_manager.ensure_connected()

        # 1. CPU Information
        try:
            cpu_out, _, _ = ssh_manager.execute_command("cat /proc/cpuinfo")
            metrics['cpu'] = _parse_cpuinfo(cpu_out)
        except Exception as e:
            logger.warning(f"Failed to get CPU info from {ssh_host}: {e}")
            metrics['cpu'] = {'error': str(e)}

        # 2. Memory Information
        try:
            mem_out, _, _ = ssh_manager.execute_command("cat /proc/meminfo")
            metrics['memory'] = _parse_meminfo(mem_out)
        except Exception as e:
            logger.warning(f"Failed to get memory info from {ssh_host}: {e}")
            metrics['memory'] = {'error': str(e)}

        # 3. Load Average
        try:
            load_out, _, _ = ssh_manager.execute_command("cat /proc/loadavg")
            metrics['load_average'] = _parse_loadavg(load_out)
        except Exception as e:
            logger.warning(f"Failed to get load average from {ssh_host}: {e}")
            metrics['load_average'] = {'error': str(e)}

        # 4. File Descriptors
        try:
            fd_out, _, _ = ssh_manager.execute_command("ulimit -n")
            metrics['max_file_descriptors'] = int(fd_out.strip())
        except Exception as e:
            logger.warning(f"Failed to get file descriptors from {ssh_host}: {e}")
            metrics['max_file_descriptors'] = None

        # 5. Current file descriptor usage
        try:
            # Count open file descriptors for all clickhouse processes
            fd_count_out, _, _ = ssh_manager.execute_command(
                "lsof -p $(pgrep -d, clickhouse) 2>/dev/null | wc -l || echo 0"
            )
            metrics['current_file_descriptors'] = int(fd_count_out.strip())
        except Exception as e:
            logger.debug(f"Could not count file descriptors from {ssh_host}: {e}")
            metrics['current_file_descriptors'] = None

        # 6. Disk I/O stats (if iostat available)
        try:
            iostat_out, stderr, exit_code = ssh_manager.execute_command(
                "iostat -x 1 2 2>/dev/null || echo 'iostat not available'"
            )
            if exit_code == 0 and 'not available' not in iostat_out:
                metrics['io_stats'] = _parse_iostat(iostat_out)
            else:
                metrics['io_stats'] = None
        except Exception as e:
            logger.debug(f"iostat not available on {ssh_host}: {e}")
            metrics['io_stats'] = None

        # 7. Key kernel parameters
        try:
            sysctl_out, _, _ = ssh_manager.execute_command(
                "sysctl -n vm.swappiness vm.dirty_ratio vm.dirty_background_ratio "
                "net.core.somaxconn net.ipv4.tcp_max_syn_backlog 2>/dev/null || true"
            )
            metrics['kernel_params'] = _parse_sysctl(sysctl_out)
        except Exception as e:
            logger.debug(f"Could not get kernel params from {ssh_host}: {e}")
            metrics['kernel_params'] = None

    except Exception as e:
        return {'host': ssh_host, 'error': str(e)}

    return metrics


def _parse_cpuinfo(output):
    """Parse /proc/cpuinfo output."""
    cpu_count = output.count('processor')

    # Extract model name
    model_match = re.search(r'model name\s*:\s*(.+)', output)
    model_name = model_match.group(1).strip() if model_match else "Unknown"

    # Extract CPU MHz
    mhz_match = re.search(r'cpu MHz\s*:\s*([0-9.]+)', output)
    cpu_mhz = float(mhz_match.group(1)) if mhz_match else None

    # Extract cache size
    cache_match = re.search(r'cache size\s*:\s*(\d+)\s*KB', output)
    cache_kb = int(cache_match.group(1)) if cache_match else None

    return {
        'processors': cpu_count,
        'model': model_name,
        'mhz': cpu_mhz,
        'cache_kb': cache_kb
    }


def _parse_meminfo(output):
    """Parse /proc/meminfo output."""
    mem_data = {}

    for line in output.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            # Extract numeric value in KB
            match = re.match(r'(\d+)\s*kB', value)
            if match:
                mem_data[key] = int(match.group(1))

    # Calculate key metrics
    total_kb = mem_data.get('MemTotal', 0)
    free_kb = mem_data.get('MemFree', 0)
    available_kb = mem_data.get('MemAvailable', free_kb)
    buffers_kb = mem_data.get('Buffers', 0)
    cached_kb = mem_data.get('Cached', 0)

    used_kb = total_kb - free_kb - buffers_kb - cached_kb
    used_percent = (used_kb / total_kb * 100) if total_kb > 0 else 0

    return {
        'total_mb': total_kb / 1024,
        'used_mb': used_kb / 1024,
        'free_mb': free_kb / 1024,
        'available_mb': available_kb / 1024,
        'buffers_mb': buffers_kb / 1024,
        'cached_mb': cached_kb / 1024,
        'used_percent': used_percent
    }


def _parse_loadavg(output):
    """Parse /proc/loadavg output."""
    parts = output.strip().split()
    if len(parts) >= 3:
        return {
            'load_1min': float(parts[0]),
            'load_5min': float(parts[1]),
            'load_15min': float(parts[2])
        }
    return {}


def _parse_iostat(output):
    """Parse iostat output - extract avg wait time and utilization."""
    try:
        # Look for the second iteration (more accurate)
        lines = output.strip().split('\n')

        # Find device lines (skip headers)
        devices = []
        in_device_section = False

        for line in lines:
            if 'Device' in line and 'r/s' in line:
                in_device_section = True
                continue

            if in_device_section and line.strip():
                parts = line.split()
                if len(parts) >= 10:  # iostat -x has many columns
                    try:
                        device = parts[0]
                        util = float(parts[-1])  # Last column is usually %util
                        await_ms = float(parts[-2]) if len(parts) > 10 else None

                        devices.append({
                            'device': device,
                            'utilization_percent': util,
                            'await_ms': await_ms
                        })
                    except (ValueError, IndexError):
                        continue

        return devices if devices else None
    except Exception as e:
        logger.debug(f"Failed to parse iostat: {e}")
        return None


def _parse_sysctl(output):
    """Parse sysctl output."""
    lines = output.strip().split('\n')
    if len(lines) >= 5:
        return {
            'vm_swappiness': lines[0].strip(),
            'vm_dirty_ratio': lines[1].strip(),
            'vm_dirty_background_ratio': lines[2].strip(),
            'net_core_somaxconn': lines[3].strip(),
            'tcp_max_syn_backlog': lines[4].strip()
        }
    return {}


def _display_metrics(builder, all_node_metrics, settings):
    """Display collected metrics in the report."""

    # 1. CPU Summary
    builder.h4("CPU Information")
    cpu_table = []
    for node in all_node_metrics:
        cpu = node.get('cpu', {})
        if 'error' not in cpu:
            cpu_table.append({
                "Node": node['node_id'],
                "CPUs": cpu.get('processors', 'N/A'),
                "Model": cpu.get('model', 'Unknown')[:50],
                "MHz": f"{cpu.get('mhz', 0):.0f}" if cpu.get('mhz') else 'N/A',
                "Cache (KB)": cpu.get('cache_kb', 'N/A')
            })

    if cpu_table:
        builder.table(cpu_table)
    builder.blank()

    # 2. Memory Summary
    builder.h4("Memory Information")
    mem_table = []
    high_memory_usage = []

    mem_warning_threshold = settings.get('clickhouse_ssh_memory_warning_percent', 85)
    mem_critical_threshold = settings.get('clickhouse_ssh_memory_critical_percent', 95)

    for node in all_node_metrics:
        mem = node.get('memory', {})
        if 'error' not in mem:
            used_pct = mem.get('used_percent', 0)
            status = "✅"
            if used_pct >= mem_critical_threshold:
                status = "🔴"
                high_memory_usage.append((node['node_id'], used_pct, 'critical'))
            elif used_pct >= mem_warning_threshold:
                status = "⚠️"
                high_memory_usage.append((node['node_id'], used_pct, 'warning'))

            mem_table.append({
                "Status": status,
                "Node": node['node_id'],
                "Total (GB)": f"{mem.get('total_mb', 0) / 1024:.1f}",
                "Used (GB)": f"{mem.get('used_mb', 0) / 1024:.1f}",
                "Free (GB)": f"{mem.get('free_mb', 0) / 1024:.1f}",
                "Available (GB)": f"{mem.get('available_mb', 0) / 1024:.1f}",
                "Used %": f"{used_pct:.1f}%"
            })

    if mem_table:
        builder.table(mem_table)

    if high_memory_usage:
        builder.blank()
        critical_nodes = [n for n in high_memory_usage if n[2] == 'critical']
        warning_nodes = [n for n in high_memory_usage if n[2] == 'warning']

        if critical_nodes:
            builder.critical(
                f"🔴 **{len(critical_nodes)} node(s) with critically high memory usage (>{mem_critical_threshold}%)**\n\n"
                "Nodes: " + ", ".join(f"{n[0]} ({n[1]:.1f}%)" for n in critical_nodes)
            )

        if warning_nodes:
            builder.warning(
                f"⚠️ **{len(warning_nodes)} node(s) with high memory usage (>{mem_warning_threshold}%)**\n\n"
                "Nodes: " + ", ".join(f"{n[0]} ({n[1]:.1f}%)" for n in warning_nodes)
            )

    builder.blank()

    # 3. Load Average
    builder.h4("Load Average")
    load_table = []
    high_load_nodes = []

    for node in all_node_metrics:
        load = node.get('load_average', {})
        cpu = node.get('cpu', {})
        cpu_count = cpu.get('processors', 1) if 'error' not in cpu else 1

        if load:
            load_1 = load.get('load_1min', 0)
            load_5 = load.get('load_5min', 0)
            load_15 = load.get('load_15min', 0)

            # Flag if 5min load > CPU count (indicates sustained high load)
            status = "✅"
            if load_5 > cpu_count * 0.8:
                status = "⚠️"
                high_load_nodes.append((node['node_id'], load_5, cpu_count))

            load_table.append({
                "Status": status,
                "Node": node['node_id'],
                "CPUs": cpu_count,
                "1min": f"{load_1:.2f}",
                "5min": f"{load_5:.2f}",
                "15min": f"{load_15:.2f}",
                "Load/CPU": f"{load_5/cpu_count:.2f}" if cpu_count > 0 else "N/A"
            })

    if load_table:
        builder.table(load_table)

    if high_load_nodes:
        builder.blank()
        builder.warning(
            f"⚠️ **{len(high_load_nodes)} node(s) with sustained high load**\n\n"
            "Load > 80% of CPU count: " +
            ", ".join(f"{n[0]} ({n[1]:.2f}/{n[2]} CPUs)" for n in high_load_nodes)
        )

    builder.blank()

    # 4. File Descriptors
    builder.h4("File Descriptors")
    fd_table = []

    for node in all_node_metrics:
        max_fd = node.get('max_file_descriptors')
        current_fd = node.get('current_file_descriptors')

        if max_fd:
            status = "✅"
            usage_pct = (current_fd / max_fd * 100) if current_fd and max_fd else 0

            if usage_pct > 80:
                status = "⚠️"

            fd_table.append({
                "Status": status,
                "Node": node['node_id'],
                "Max FD": f"{max_fd:,}",
                "Current": f"{current_fd:,}" if current_fd else "N/A",
                "Usage %": f"{usage_pct:.1f}%" if current_fd else "N/A"
            })

    if fd_table:
        builder.table(fd_table)
    builder.blank()

    # 5. Key Kernel Parameters (if available)
    kernel_params_available = any(
        node.get('kernel_params') for node in all_node_metrics
    )

    if kernel_params_available:
        builder.h4("Key Kernel Parameters")
        param_table = []

        for node in all_node_metrics:
            params = node.get('kernel_params', {})
            if params:
                param_table.append({
                    "Node": node['node_id'],
                    "Swappiness": params.get('vm_swappiness', 'N/A'),
                    "Dirty Ratio": params.get('vm_dirty_ratio', 'N/A'),
                    "Dirty BG Ratio": params.get('vm_dirty_background_ratio', 'N/A'),
                    "SoMaxConn": params.get('net_core_somaxconn', 'N/A')
                })

        if param_table:
            builder.table(param_table)
        builder.blank()

    # 6. Recommendations
    recommendations = _generate_recommendations(all_node_metrics, settings)
    if recommendations['critical'] or recommendations['high']:
        builder.recs(recommendations)
    else:
        builder.success("✅ All nodes showing healthy OS-level metrics.")


def _generate_recommendations(all_node_metrics, settings):
    """Generate recommendations based on collected metrics."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    mem_critical_threshold = settings.get('clickhouse_ssh_memory_critical_percent', 95)
    mem_warning_threshold = settings.get('clickhouse_ssh_memory_warning_percent', 85)

    for node in all_node_metrics:
        node_id = node['node_id']

        # Memory issues
        mem = node.get('memory', {})
        if 'error' not in mem:
            used_pct = mem.get('used_percent', 0)
            if used_pct >= mem_critical_threshold:
                recs['critical'].append(
                    f"Node {node_id}: Memory usage critically high ({used_pct:.1f}%) - "
                    "investigate memory leaks or increase RAM"
                )
            elif used_pct >= mem_warning_threshold:
                recs['high'].append(
                    f"Node {node_id}: Memory usage high ({used_pct:.1f}%) - "
                    "monitor closely and consider increasing memory"
                )

        # Load issues
        load = node.get('load_average', {})
        cpu = node.get('cpu', {})
        cpu_count = cpu.get('processors', 1) if 'error' not in cpu else 1

        if load:
            load_5 = load.get('load_5min', 0)
            if load_5 > cpu_count:
                recs['high'].append(
                    f"Node {node_id}: Sustained high load ({load_5:.2f} with {cpu_count} CPUs) - "
                    "investigate query load or increase CPU resources"
                )

        # File descriptor usage
        max_fd = node.get('max_file_descriptors')
        current_fd = node.get('current_file_descriptors')
        if max_fd and current_fd:
            usage_pct = (current_fd / max_fd * 100)
            if usage_pct > 90:
                recs['high'].append(
                    f"Node {node_id}: File descriptor usage high ({usage_pct:.1f}%) - "
                    "increase ulimit -n setting"
                )

    # General recommendations
    recs['general'].extend([
        "Monitor memory usage trends to predict capacity needs",
        "Set up alerting for high memory usage (>85%) and load average",
        "Ensure file descriptor limits are adequate for concurrent connections",
        "Review kernel parameters for ClickHouse optimization",
        "Consider using cgroups to limit ClickHouse resource usage"
    ])

    return recs

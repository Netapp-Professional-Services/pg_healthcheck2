"""
I/O statistics check for Kafka brokers.

Monitors disk I/O performance including throughput, IOPS, utilization,
and I/O wait times across all broker nodes.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 6


def run_check_iostat(connector, settings):
    """
    Checks disk I/O performance on all Kafka brokers via SSH.
    
    Collects iostat metrics and analyzes disk utilization, I/O wait times,
    and throughput to identify performance bottlenecks.
    
    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds
    
    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    builder.h3("I/O Statistics (All Brokers)")
    
    available, skip_msg, skip_data = require_ssh(connector, "I/O statistics check")
    if not available:
        builder.add(skip_msg)
        return builder.build(), skip_data
    
    try:
        util_warning = settings.get('kafka_io_util_warning_percent', 80)
        util_critical = settings.get('kafka_io_util_critical_percent', 95)
        await_warning = settings.get('kafka_io_await_warning_ms', 20)
        await_critical = settings.get('kafka_io_await_critical_ms', 50)
        iowait_warning = settings.get('kafka_cpu_iowait_warning_percent', 10)
        iowait_critical = settings.get('kafka_cpu_iowait_critical_percent', 25)
        
        command = "iostat -x 1 2 | tail -n +3"
        
        results = connector.execute_ssh_on_all_hosts(command, "iostat check")
        
        all_io_data = []
        cpu_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []
        
        for result in results:
            host = result['host']
            broker_id = result['node_id']
            
            if not result['success']:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': result.get('error', 'Unknown error')
                })
                builder.warning(
                    f"Could not collect iostat on {host} (Broker {broker_id}): {result.get('error')}"
                )
                continue
            
            output = result['output'].strip()
            if not output:
                builder.warning(f"No iostat data returned from {host} (Broker {broker_id})")
                continue
            
            lines = output.split('\n')
            
            cpu_section_indices = []
            device_section_indices = []
            
            for i, line in enumerate(lines):
                if line.strip().startswith('avg-cpu:'):
                    cpu_section_indices.append(i)
                elif 'Device' in line and ('r/s' in line or 'rrqm/s' in line):
                    device_section_indices.append(i)
            
            last_cpu_start = cpu_section_indices[-1] if cpu_section_indices else -1
            last_device_start = device_section_indices[-1] if device_section_indices else -1
            
            broker_has_issues = False
            
            if last_cpu_start >= 0:
                for i in range(last_cpu_start + 1, min(last_cpu_start + 3, len(lines))):
                    line = lines[i].strip()
                    if not line or 'user' in line.lower():
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 6:
                        try:
                            cpu_info = {
                                'host': host,
                                'broker_id': broker_id,
                                'user': float(parts[0]),
                                'nice': float(parts[1]),
                                'system': float(parts[2]),
                                'iowait': float(parts[3]),
                                'steal': float(parts[4]),
                                'idle': float(parts[5])
                            }
                            cpu_data.append(cpu_info)
                            
                            if cpu_info['iowait'] >= iowait_critical:
                                issues_found = True
                                broker_has_issues = True
                                if broker_id not in critical_brokers:
                                    critical_brokers.append(broker_id)
                                
                                builder.critical_issue(
                                    "Critical CPU I/O Wait",
                                    {
                                        "Broker": f"{broker_id} ({host})",
                                        "I/O Wait": f"{cpu_info['iowait']:.1f}% (threshold: {iowait_critical}%)",
                                        "CPU Idle": f"{cpu_info['idle']:.1f}%",
                                        "Warning": "CPU spending excessive time waiting for disk I/O!"
                                    }
                                )
                            elif cpu_info['iowait'] >= iowait_warning:
                                issues_found = True
                                broker_has_issues = True
                                if broker_id not in warning_brokers:
                                    warning_brokers.append(broker_id)
                            
                            break
                        except (ValueError, IndexError) as e:
                            logger.debug(f"Could not parse CPU line: {line} - {e}")
            
            if last_device_start >= 0:
                for i in range(last_device_start + 1, len(lines)):
                    line = lines[i].strip()
                    if not line or 'Device' in line:
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 14:
                        try:
                            device = parts[0]
                            
                            io_info = {
                                'host': host,
                                'broker_id': broker_id,
                                'device': device,
                                'r_s': float(parts[3]),
                                'w_s': float(parts[4]),
                                'rkB_s': float(parts[5]),
                                'wkB_s': float(parts[6]),
                                'await': float(parts[9]),
                                'util': float(parts[13])
                            }
                            all_io_data.append(io_info)
                            
                            has_io_activity = (io_info['r_s'] + io_info['w_s']) > 0
                            
                            if io_info['util'] >= util_critical or (has_io_activity and io_info['await'] >= await_critical):
                                issues_found = True
                                broker_has_issues = True
                                if broker_id not in critical_brokers:
                                    critical_brokers.append(broker_id)
                                
                                builder.critical_issue(
                                    "Critical I/O Performance Issue",
                                    {
                                        "Broker": f"{broker_id} ({host})",
                                        "Device": device,
                                        "Utilization": f"{io_info['util']:.1f}% (threshold: {util_critical}%)",
                                        "Average Wait": f"{io_info['await']:.1f}ms (threshold: {await_critical}ms)",
                                        "Read/Write": f"{io_info['r_s']:.1f}/{io_info['w_s']:.1f} IOPS",
                                        "Throughput": f"{io_info['rkB_s']:.0f}/{io_info['wkB_s']:.0f} KB/s",
                                        "Warning": "Disk I/O bottleneck detected!"
                                    }
                                )
                            
                            elif io_info['util'] >= util_warning or (has_io_activity and io_info['await'] >= await_warning):
                                issues_found = True
                                broker_has_issues = True
                                if broker_id not in warning_brokers:
                                    warning_brokers.append(broker_id)
                                
                                builder.warning_issue(
                                    "High I/O Load",
                                    {
                                        "Broker": f"{broker_id} ({host})",
                                        "Device": device,
                                        "Utilization": f"{io_info['util']:.1f}% (threshold: {util_warning}%)",
                                        "Average Wait": f"{io_info['await']:.1f}ms (threshold: {await_warning}ms)",
                                        "Read/Write": f"{io_info['r_s']:.1f}/{io_info['w_s']:.1f} IOPS",
                                        "Throughput": f"{io_info['rkB_s']:.0f}/{io_info['wkB_s']:.0f} KB/s"
                                    }
                                )
                        
                        except (ValueError, IndexError) as e:
                            logger.debug(f"Could not parse device line: {line} - {e}")
        
        if cpu_data:
            builder.h4("CPU I/O Wait Summary")
            builder.text("|===")
            builder.text("|Broker|Host|User %|System %|I/O Wait %|Idle %")
            
            for cpu in sorted(cpu_data, key=lambda x: x['iowait'], reverse=True):
                indicator = ""
                if cpu['iowait'] >= iowait_critical:
                    indicator = "🔴 "
                elif cpu['iowait'] >= iowait_warning:
                    indicator = "⚠️ "
                
                builder.text(
                    f"|{cpu['broker_id']}|{cpu['host']}|"
                    f"{cpu['user']:.1f}|{cpu['system']:.1f}|"
                    f"{indicator}{cpu['iowait']:.1f}|{cpu['idle']:.1f}"
                )
            builder.text("|===")
            builder.blank()
        
        if all_io_data:
            builder.h4("Disk I/O Performance Summary")
            builder.text("|===")
            builder.text("|Broker|Host|Device|Read IOPS|Write IOPS|Read KB/s|Write KB/s|Await (ms)|Util %")
            
            for io in sorted(all_io_data, key=lambda x: x['util'], reverse=True):
                indicator = ""
                if io['util'] >= util_critical or io['await'] >= await_critical:
                    indicator = "🔴 "
                elif io['util'] >= util_warning or io['await'] >= await_warning:
                    indicator = "⚠️ "
                
                builder.text(
                    f"|{io['broker_id']}|{io['host']}|{io['device']}|"
                    f"{io['r_s']:.1f}|{io['w_s']:.1f}|"
                    f"{io['rkB_s']:.0f}|{io['wkB_s']:.0f}|"
                    f"{io['await']:.1f}|{indicator}{io['util']:.1f}"
                )
            builder.text("|===")
            builder.blank()
        
        if errors:
            builder.h4("Collection Errors")
            error_list = [f"Broker {e['broker_id']} ({e['host']}): {e['error']}" for e in errors]
            builder.warning(
                f"Could not collect iostat from {len(errors)} broker(s):\n\n" +
                "\n".join(f"* {e}" for e in error_list)
            )
        
        if issues_found:
            builder.recs({
                "critical": [
                    "**Upgrade storage:** Move to faster SSD/NVMe if using spinning disks",
                    "**Check disk health:** Run SMART diagnostics for failing drives",
                    "**Reduce load:** Review producer throughput and consumer lag",
                    "**Optimize flush settings:** Adjust log.flush.interval if too aggressive",
                    "**Add brokers:** Distribute load across more nodes"
                ] if critical_brokers else None,
                "high": [
                    "**Monitor trends:** Track I/O patterns over time",
                    "**Optimize batching:** Increase batch sizes to reduce I/O operations",
                    "**Review compaction:** Heavy compaction can increase I/O load",
                    "**Check RAID configuration:** Ensure proper RAID settings for performance"
                ] if warning_brokers else None,
                "general": [
                    "Use SSDs or NVMe for Kafka data directories (critical for performance)",
                    "Separate OS and Kafka data onto different disks when possible",
                    "Monitor disk queue depths and adjust I/O scheduler (deadline or noop for SSDs)",
                    "Set proper vm.swappiness (recommend 1 for Kafka)",
                    "Consider XFS or ext4 filesystems with proper mount options"
                ]
            })
        else:
            builder.success(
                f"✅ I/O performance is healthy across all brokers.\n\n"
                f"All disks show utilization below {util_warning}% and average wait times below {await_warning}ms."
            )
        
        structured_data["iostat"] = {
            "status": "success",
            "brokers_checked": len([r for r in results if r['success']]),
            "brokers_with_errors": len(errors),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "util_warning_percent": util_warning,
                "util_critical_percent": util_critical,
                "await_warning_ms": await_warning,
                "await_critical_ms": await_critical,
                "iowait_warning_percent": iowait_warning,
                "iowait_critical_percent": iowait_critical
            },
            "errors": errors,
            "cpu_data": cpu_data,
            "io_data": all_io_data
        }
        
        structured_data["iostat_summary"] = {
            "status": "success",
            "data": [{
                "total_brokers_checked": len([r for r in results if r['success']]),
                "critical_broker_count": len(critical_brokers),
                "warning_broker_count": len(warning_brokers),
                "critical_brokers": critical_brokers,
                "warning_brokers": warning_brokers,
                "has_cluster_wide_issue": len(critical_brokers) >= 2,
                "max_util": max([io['util'] for io in all_io_data], default=0),
                "max_await": max([io['await'] for io in all_io_data], default=0),
                "max_iowait": max([cpu['iowait'] for cpu in cpu_data], default=0)
            }]
        }
    
    except Exception as e:
        import traceback
        logger.error(f"iostat check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"I/O statistics check failed: {e}")
        structured_data["iostat"] = {
            "status": "error",
            "details": str(e)
        }
    
    return builder.build(), structured_data

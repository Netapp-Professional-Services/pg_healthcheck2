"""
Disk usage check for Kafka brokers.

Queries Kafka for actual log directory paths, then checks disk usage
for those directories via SSH. If the Admin API does not return log dirs
(e.g. restricted on managed clusters), reads log.dirs from server.properties
using kafka_config_file_path; only falls back to /data/kafka if both fail.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.kafka.utils.qrylib.log_dirs_queries import get_describe_log_dirs_query
import logging
import shlex

logger = logging.getLogger(__name__)


def _get_log_dirs_from_server_properties(connector, ssh_host):
    """
    Read log.dirs from server.properties on the given SSH host.
    Uses kafka_config_file_path from config. Returns list of paths, or None if unavailable.
    """
    path = connector.settings.get('kafka_config_file_path')
    if not path:
        return None
    ssh_manager = connector.get_ssh_manager(ssh_host)
    if not ssh_manager:
        return None
    path_quoted = shlex.quote(path)
    # log.dirs= can be comma-separated; strip whitespace per path
    cmd = f"grep -E '^log\\.dirs?=' {path_quoted} 2>/dev/null | cut -d= -f2- | tr ',' '\\n' | sed 's/^[ \\t]*//;s/[ \\t]*$//' | grep -v '^$'"
    try:
        stdout, stderr, exit_code = ssh_manager.execute_command(cmd, timeout=5)
        if exit_code != 0 or not stdout or not stdout.strip():
            return None
        dirs = [d.strip() for d in stdout.strip().split('\n') if d.strip()]
        return dirs if dirs else None
    except Exception as e:
        logger.debug(f"Could not read log.dirs from server.properties on {ssh_host}: {e}")
        return None


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def run_check_disk_usage(connector, settings):
    """
    Checks disk usage on all Kafka brokers via SSH.
    
    First queries Kafka to discover actual log directories configured
    on each broker, then uses SSH to check disk usage for those paths.
    
    Args:
        connector: Kafka connector with multi-host SSH support
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
        warning_percent = settings.get('kafka_disk_warning_percent', 75)
        critical_percent = settings.get('kafka_disk_critical_percent', 90)
        
        builder.h3("Disk Usage (All Brokers)")
        
        # === STEP 1: QUERY KAFKA FOR LOG DIRECTORIES ===
        log_dirs_query = get_describe_log_dirs_query(connector)
        _, log_dirs_raw = connector.execute_query(log_dirs_query, return_raw=True)
        
        if not log_dirs_raw or isinstance(log_dirs_raw, dict) and ('error' in log_dirs_raw or 'unavailable' in log_dirs_raw):
            builder.warning(
                "Could not retrieve log directory information from Kafka API.\n"
                "Will use log.dirs from server.properties per broker when available."
            )
            broker_log_dirs = {}
        else:
            # Extract unique log directories per broker
            broker_log_dirs = {}
            for entry in log_dirs_raw:
                broker_id = entry.get('broker_id')
                log_dir = entry.get('log_dir')
                
                if broker_id not in broker_log_dirs:
                    broker_log_dirs[broker_id] = set()
                
                if log_dir:
                    broker_log_dirs[broker_id].add(log_dir)
            
            # Convert sets to lists
            broker_log_dirs = {k: list(v) for k, v in broker_log_dirs.items()}
            
            logger.info(f"Discovered log directories: {broker_log_dirs}")
            
            if broker_log_dirs:
                builder.para("**Kafka Log Directories Discovered:**")
                for broker_id, dirs in sorted(broker_log_dirs.items()):
                    builder.text(f"* Broker {broker_id}: {', '.join(dirs)}")
                builder.blank()
        
        # === STEP 2: MAP BROKERS TO SSH HOSTS ===
        # We need to know which SSH host corresponds to which broker
        # The connector should have this mapping from connect()
        ssh_host_to_broker = getattr(connector, 'ssh_host_to_node', {})
        broker_to_ssh_host = {v: k for k, v in ssh_host_to_broker.items()}
        
        # === STEP 3: CHECK DISK USAGE FOR EACH BROKER'S LOG DIRS ===
        all_disk_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []
        
        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_broker.get(ssh_host, 'unknown')
            
            # Get log directories: from API first, then from server.properties (log.dirs), else default
            log_dirs = broker_log_dirs.get(broker_id) or []
            if not log_dirs:
                log_dirs = _get_log_dirs_from_server_properties(connector, ssh_host)
            if not log_dirs:
                log_dirs = ['/data/kafka']  # Last-resort fallback
            
            # Check each log directory
            for log_dir in log_dirs:
                try:
                    ssh_manager = connector.get_ssh_manager(ssh_host)
                    if not ssh_manager:
                        continue
                    
                    ssh_manager.ensure_connected()
                    
                    # Execute df command for specific directory
                    command = f"df -h {log_dir}"
                    stdout, stderr, exit_code = ssh_manager.execute_command(command)
                    
                    if exit_code != 0:
                        logger.warning(f"df command failed on {ssh_host} for {log_dir}: {stderr}")
                        errors.append({
                            'host': ssh_host,
                            'broker_id': broker_id,
                            'log_dir': log_dir,
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
                                'broker_id': broker_id,
                                'log_dir': log_dir,
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
                                if broker_id not in critical_brokers:
                                    critical_brokers.append(broker_id)
                                
                                builder.critical_issue(
                                    "Critical Disk Usage",
                                    {
                                        "Broker": f"{broker_id} ({ssh_host})",
                                        "Log Directory": log_dir,
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
                                if broker_id not in warning_brokers:
                                    warning_brokers.append(broker_id)
                                
                                builder.warning_issue(
                                    "High Disk Usage",
                                    {
                                        "Broker": f"{broker_id} ({ssh_host})",
                                        "Log Directory": log_dir,
                                        "Mount": mount_point,
                                        "Usage": f"{use_percent}% (threshold: {warning_percent}%)",
                                        "Available": f"{available} of {size}"
                                    }
                                )
                
                except Exception as e:
                    logger.error(f"Error checking disk on {ssh_host} for {log_dir}: {e}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'log_dir': log_dir,
                        'error': str(e)
                    })
        
        # === STEP 4: SUMMARY TABLE ===
        if all_disk_data:
            builder.h4("Disk Usage Summary")
            
            # Build table manually since indicators are pre-calculated
            table_lines = [
                "|===",
                "|Broker|Host|Log Directory|Mount|Size|Used|Available|Usage %"
            ]
            
            for disk in sorted(all_disk_data, key=lambda x: (x['broker_id'], x['use_percent']), reverse=True):
                usage_indicator = ""
                if disk['exceeds_critical']:
                    usage_indicator = "🔴 "
                elif disk['exceeds_warning']:
                    usage_indicator = "⚠️ "
                
                table_lines.append(
                    f"|{disk['broker_id']}|{disk['host']}|{disk['log_dir']}|{disk['mount_point']}|"
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
                "\n".join([f"* Broker {e['broker_id']} ({e['host']}): {e['log_dir']} - {e['error']}" 
                          for e in errors])
            )
        
        # === STEP 6: RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}
            
            if critical_brokers:
                recommendations["critical"] = [
                    "**Delete old log segments:** Review retention policies and manually clean up if needed",
                    "**Enable log compaction:** For compacted topics to reclaim space",
                    "**Expand storage:** Add capacity or migrate to larger volumes immediately",
                    "**Check partition balance:** Redistribute if imbalanced",
                    "**Emergency procedure:** Consider temporarily reducing replication factor (with caution)"
                ]
            
            if warning_brokers:
                recommendations["high"] = [
                    "**Review retention policies:** Adjust retention.ms and retention.bytes",
                    "**Identify large topics:** Use storage health check to find space hogs",
                    "**Plan storage expansion:** Add capacity within 1-2 weeks",
                    "**Monitor growth rate:** Predict when critical threshold will be reached"
                ]
            
            recommendations["general"] = [
                "Set up disk space alerts at 70% threshold for early warning",
                "Implement automated retention policy enforcement",
                "Regular capacity planning reviews (monthly/quarterly)",
                "Consider tiered storage for older data (Kafka 2.8+)",
                "Document runbook for emergency disk cleanup procedures"
            ]
            
            builder.recs(recommendations)
        else:
            builder.success(
                f"Disk usage is within healthy limits across all brokers.\n\n"
                f"All monitored log directories are below {warning_percent}% usage."
            )
        
        # === STEP 7: STRUCTURED DATA ===
        structured_data["disk_usage"] = {
            "status": "success",
            "brokers_checked": len(broker_to_ssh_host),
            "log_directories_discovered": {str(k): v for k, v in broker_log_dirs.items()},
            "brokers_with_errors": len(set(e['broker_id'] for e in errors)),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
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

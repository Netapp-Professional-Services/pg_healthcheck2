"""
System Limits Check for Kafka brokers.

Validates critical system limits and kernel parameters required for optimal Kafka performance:
- LimitMEMLOCK: Should be infinity (allows memory locking)
- LimitNOFILE: Should be >= 100000 (complements file descriptor check)
- LimitAS: Should be infinity (unlimited address space)
- LimitNPROC: Should be >= 32768 (process count limit)
- vm.swappiness: Should be 0 or 1 (minimize swap usage)
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.common.parsers import _safe_int
from plugins.kafka.utils.qrylib.system_limits_queries import (
    get_kafka_process_limits_query,
    get_swappiness_query
)
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8  # High priority - system limits are critical for Kafka performance


def _parse_limits_output(limits_output):
    """
    Parse /proc/<pid>/limits output into a dictionary.
    
    Example input:
    Limit                     Soft Limit           Hard Limit           Units
    Max cpu time              unlimited            unlimited            seconds
    Max file size             unlimited            unlimited            bytes
    Max data size             unlimited            unlimited            bytes
    Max stack size            8388608              unlimited            bytes
    Max core file size        0                    unlimited            bytes
    Max resident set          unlimited            unlimited            bytes
    Max processes             32768                32768                processes
    Max open files            100000               100000               files
    Max locked memory         65536                65536                bytes
    Max address space         unlimited            unlimited            bytes
    Max file locks            unlimited            unlimited            locks
    Max pending signals       123456               123456              signals
    Max msgqueue size         819200               819200              bytes
    Max nice priority         0                    0
    Max realtime priority     0                    0
    Max realtime timeout      unlimited            unlimited            us
    
    Returns:
        dict: Parsed limits with keys like 'Max processes', 'Max open files', etc.
    """
    limits = {}
    lines = limits_output.strip().split('\n')
    
    # Skip header line
    for line in lines[1:]:
        if not line.strip():
            continue
        
        # Parse line: "Max processes             32768                32768                processes"
        parts = line.split()
        if len(parts) >= 3:
            limit_name = ' '.join(parts[:-3])  # "Max processes"
            soft_limit = parts[-3]
            hard_limit = parts[-2]
            unit = parts[-1]
            
            # Normalize limit names
            limit_key = limit_name.strip()
            limits[limit_key] = {
                'soft': soft_limit,
                'hard': hard_limit,
                'unit': unit,
                'raw_line': line
            }
    
    return limits


def _parse_limit_value(value_str):
    """
    Parse a limit value string (e.g., "unlimited", "infinity", "32768") into a comparable value.
    
    Returns:
        tuple: (numeric_value, is_unlimited) where numeric_value is int or None, is_unlimited is bool
    """
    value_str = value_str.strip().lower()
    
    if value_str in ('unlimited', 'infinity'):
        return None, True
    
    try:
        return int(value_str), False
    except ValueError:
        return None, False


def _check_limit(limits, limit_name, min_value=None, must_be_unlimited=False):
    """
    Check if a limit meets requirements.
    
    Args:
        limits: Parsed limits dictionary
        limit_name: Name of the limit to check (e.g., "Max locked memory")
        min_value: Minimum numeric value required (None if not applicable)
        must_be_unlimited: If True, limit must be unlimited/infinity
    
    Returns:
        dict: {
            'status': 'ok' | 'warning' | 'critical',
            'soft_value': value or None,
            'hard_value': value or None,
            'soft_unlimited': bool,
            'hard_unlimited': bool,
            'message': str
        }
    """
    if limit_name not in limits:
        return {
            'status': 'critical',
            'soft_value': None,
            'hard_value': None,
            'soft_unlimited': False,
            'hard_unlimited': False,
            'message': f'Limit {limit_name} not found in limits output'
        }
    
    limit_info = limits[limit_name]
    soft_str = limit_info['soft']
    hard_str = limit_info['hard']
    
    soft_val, soft_unlimited = _parse_limit_value(soft_str)
    hard_val, hard_unlimited = _parse_limit_value(hard_str)
    
    # Check if must be unlimited
    if must_be_unlimited:
        if not (soft_unlimited and hard_unlimited):
            return {
                'status': 'critical',
                'soft_value': soft_val,
                'hard_value': hard_val,
                'soft_unlimited': soft_unlimited,
                'hard_unlimited': hard_unlimited,
                'message': f'Should be unlimited/infinity, but soft={soft_str}, hard={hard_str}'
            }
        return {
            'status': 'ok',
            'soft_value': None,
            'hard_value': None,
            'soft_unlimited': True,
            'hard_unlimited': True,
            'message': 'Correctly set to unlimited'
        }
    
    # Check minimum value
    if min_value is not None:
        # Use hard limit for comparison (soft can be lower)
        if hard_unlimited:
            return {
                'status': 'ok',
                'soft_value': soft_val,
                'hard_value': None,
                'soft_unlimited': soft_unlimited,
                'hard_unlimited': True,
                'message': f'Hard limit is unlimited (exceeds minimum {min_value})'
            }
        
        if hard_val is None or hard_val < min_value:
            return {
                'status': 'critical',
                'soft_value': soft_val,
                'hard_value': hard_val,
                'soft_unlimited': soft_unlimited,
                'hard_unlimited': hard_unlimited,
                'message': f'Hard limit {hard_str} is below minimum {min_value}'
            }
        
        # Check if soft limit is also adequate
        if soft_unlimited or (soft_val is not None and soft_val >= min_value):
            return {
                'status': 'ok',
                'soft_value': soft_val,
                'hard_value': hard_val,
                'soft_unlimited': soft_unlimited,
                'hard_unlimited': hard_unlimited,
                'message': f'Both limits meet minimum {min_value}'
            }
        else:
            return {
                'status': 'warning',
                'soft_value': soft_val,
                'hard_value': hard_val,
                'soft_unlimited': soft_unlimited,
                'hard_unlimited': hard_unlimited,
                'message': f'Hard limit OK ({hard_val} >= {min_value}), but soft limit {soft_str} is below minimum'
            }
    
    return {
        'status': 'ok',
        'soft_value': soft_val,
        'hard_value': hard_val,
        'soft_unlimited': soft_unlimited,
        'hard_unlimited': hard_unlimited,
        'message': 'Limit found'
    }


def run_system_limits_check(connector, settings):
    """
    Validates system limits and kernel parameters for Kafka brokers.
    
    Checks all brokers via SSH and validates:
    - LimitMEMLOCK: Should be infinity
    - LimitNOFILE: Should be >= 100000
    - LimitAS: Should be infinity
    - LimitNPROC: Should be >= 32768
    - vm.swappiness: Should be 0 or 1
    
    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds
    
    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "System limits check")
    if not available:
        return skip_msg, skip_data
    
    try:
        # Get thresholds from settings
        min_nofile = settings.get('kafka_min_nofile', 100000)
        min_nproc = settings.get('kafka_min_nproc', 32768)
        max_swappiness = settings.get('kafka_max_swappiness', 1)
        
        builder.h3("System Limits Validation (All Brokers)")
        builder.para(
            "Validating critical system limits and kernel parameters required for optimal Kafka performance. "
            "These limits control resource availability for the Kafka broker process."
        )
        builder.blank()
        
        # === CHECK ALL BROKERS ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_limits_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []
        
        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)
            broker_limits = {
                'host': ssh_host,
                'broker_id': broker_id,
                'limits': {},
                'swappiness': None,
                'issues': []
            }
            
            try:
                # === Get process limits ===
                limits_query = get_kafka_process_limits_query(connector)
                limits_formatted, limits_raw = connector.execute_query(limits_query, return_raw=True)
                
                if "[ERROR]" in limits_formatted or (isinstance(limits_raw, dict) and 'error' in limits_raw):
                    error_msg = limits_raw.get('error', 'Unknown error') if isinstance(limits_raw, dict) else limits_formatted
                    logger.warning(f"Failed to get limits on {ssh_host}: {error_msg}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': f"Could not read process limits: {error_msg}"
                    })
                    continue
                
                # Parse limits
                limits = _parse_limits_output(limits_raw if isinstance(limits_raw, str) else str(limits_raw))
                
                # === Check each required limit ===
                # Note: Limit names in /proc/<pid>/limits may vary slightly
                # Try multiple possible names for each limit
                limit_checks = [
                    {'names': ['Max locked memory', 'max locked memory'], 'must_be_unlimited': True, 'name': 'MEMLOCK'},
                    {'names': ['Max open files', 'max open files'], 'min_value': min_nofile, 'name': 'NOFILE'},
                    {'names': ['Max address space', 'max address space'], 'must_be_unlimited': True, 'name': 'AS'},
                    {'names': ['Max processes', 'max processes'], 'min_value': min_nproc, 'name': 'NPROC'}
                ]
                
                for check_config in limit_checks:
                    # Try to find the limit by trying different name variations
                    limit_name = None
                    for name_variant in check_config['names']:
                        if name_variant in limits:
                            limit_name = name_variant
                            break
                    
                    if not limit_name:
                        # Limit not found
                        broker_limits['limits'][check_config['name']] = {
                            'limit_name': check_config['names'][0],
                            'status': 'critical',
                            'soft': None,
                            'hard': None,
                            'soft_unlimited': False,
                            'hard_unlimited': False,
                            'message': f'Limit {check_config["names"][0]} not found in limits output'
                        }
                        issues_found = True
                        if broker_id not in critical_brokers:
                            critical_brokers.append(broker_id)
                        broker_limits['issues'].append({
                            'level': 'critical',
                            'limit': check_config['name'],
                            'message': f'Limit {check_config["names"][0]} not found'
                        })
                        continue
                    
                    check_result = _check_limit(
                        limits,
                        limit_name,
                        min_value=check_config.get('min_value'),
                        must_be_unlimited=check_config.get('must_be_unlimited', False)
                    )
                    
                    broker_limits['limits'][check_config['name']] = {
                        'limit_name': limit_name,
                        'status': check_result['status'],
                        'soft': check_result.get('soft_value'),
                        'hard': check_result.get('hard_value'),
                        'soft_unlimited': check_result.get('soft_unlimited', False),
                        'hard_unlimited': check_result.get('hard_unlimited', False),
                        'message': check_result['message']
                    }
                    
                    if check_result['status'] == 'critical':
                        issues_found = True
                        if broker_id not in critical_brokers:
                            critical_brokers.append(broker_id)
                        broker_limits['issues'].append({
                            'level': 'critical',
                            'limit': check_config['name'],
                            'message': check_result['message']
                        })
                    elif check_result['status'] == 'warning':
                        issues_found = True
                        if broker_id not in warning_brokers and broker_id not in critical_brokers:
                            warning_brokers.append(broker_id)
                        broker_limits['issues'].append({
                            'level': 'warning',
                            'limit': check_config['name'],
                            'message': check_result['message']
                        })
                
                # === Check swappiness ===
                swappiness_query = get_swappiness_query(connector)
                swappiness_formatted, swappiness_raw = connector.execute_query(swappiness_query, return_raw=True)
                
                if "[ERROR]" not in swappiness_formatted and not (isinstance(swappiness_raw, dict) and 'error' in swappiness_raw):
                    swappiness_val = _safe_int(swappiness_raw.strip() if isinstance(swappiness_raw, str) else str(swappiness_raw).strip())
                    broker_limits['swappiness'] = swappiness_val
                    
                    if swappiness_val is None or swappiness_val > max_swappiness:
                        issues_found = True
                        if broker_id not in critical_brokers:
                            if broker_id not in warning_brokers:
                                warning_brokers.append(broker_id)
                        broker_limits['issues'].append({
                            'level': 'warning' if swappiness_val <= 10 else 'critical',
                            'limit': 'swappiness',
                            'message': f'vm.swappiness is {swappiness_val}, should be 0 or 1 for Kafka'
                        })
                        if swappiness_val > 10:
                            if broker_id not in critical_brokers:
                                critical_brokers.append(broker_id)
                
                all_limits_data.append(broker_limits)
                
                # === Report issues for this broker ===
                if broker_limits['issues']:
                    critical_issues = [i for i in broker_limits['issues'] if i['level'] == 'critical']
                    warning_issues = [i for i in broker_limits['issues'] if i['level'] == 'warning']
                    
                    if critical_issues:
                        issue_details = {}
                        for issue in critical_issues:
                            limit_name = issue['limit']
                            if limit_name in broker_limits['limits']:
                                limit_info = broker_limits['limits'][limit_name]
                                if limit_info['soft_unlimited'] or limit_info['hard_unlimited']:
                                    value_str = f"soft={'unlimited' if limit_info['soft_unlimited'] else limit_info['soft']}, hard={'unlimited' if limit_info['hard_unlimited'] else limit_info['hard']}"
                                else:
                                    value_str = f"soft={limit_info['soft']}, hard={limit_info['hard']}"
                                issue_details[limit_name] = value_str
                            elif limit_name == 'swappiness':
                                issue_details['swappiness'] = str(broker_limits['swappiness'])
                        
                        builder.critical_issue(
                            f"Critical System Limits Issue - Broker {broker_id}",
                            {
                                "Host": ssh_host,
                                **issue_details
                            }
                        )
                        builder.blank()
                    
                    if warning_issues:
                        issue_details = {}
                        for issue in warning_issues:
                            limit_name = issue['limit']
                            if limit_name in broker_limits['limits']:
                                limit_info = broker_limits['limits'][limit_name]
                                if limit_info['soft_unlimited'] or limit_info['hard_unlimited']:
                                    value_str = f"soft={'unlimited' if limit_info['soft_unlimited'] else limit_info['soft']}, hard={'unlimited' if limit_info['hard_unlimited'] else limit_info['hard']}"
                                else:
                                    value_str = f"soft={limit_info['soft']}, hard={limit_info['hard']}"
                                issue_details[limit_name] = value_str
                            elif limit_name == 'swappiness':
                                issue_details['swappiness'] = str(broker_limits['swappiness'])
                        
                        builder.warning_issue(
                            f"System Limits Warning - Broker {broker_id}",
                            {
                                "Host": ssh_host,
                                **issue_details
                            }
                        )
                        builder.blank()
            
            except Exception as e:
                logger.error(f"Error checking system limits on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })
        
        # === SUMMARY TABLE ===
        if all_limits_data:
            builder.h4("System Limits Summary")
            
            table_lines = [
                "|===",
                "|Broker|Host|MEMLOCK|NOFILE|AS|NPROC|Swappiness|Status"
            ]
            
            for broker_data in sorted(all_limits_data, key=lambda x: (
                1 if any(i['level'] == 'critical' for i in x['issues']) else 0,
                1 if any(i['level'] == 'warning' for i in x['issues']) else 0
            ), reverse=True):
                broker_id = broker_data['broker_id']
                host = broker_data['host']
                
                # Format each limit
                memlock_status = "✅" if broker_data['limits'].get('MEMLOCK', {}).get('status') == 'ok' else "❌"
                nofile_status = "✅" if broker_data['limits'].get('NOFILE', {}).get('status') == 'ok' else "❌"
                as_status = "✅" if broker_data['limits'].get('AS', {}).get('status') == 'ok' else "❌"
                nproc_status = "✅" if broker_data['limits'].get('NPROC', {}).get('status') == 'ok' else "❌"
                swappiness_val = broker_data.get('swappiness', 'N/A')
                swappiness_status = "✅" if swappiness_val != 'N/A' and swappiness_val <= max_swappiness else "❌"
                
                # Overall status
                has_critical = any(i['level'] == 'critical' for i in broker_data['issues'])
                has_warning = any(i['level'] == 'warning' for i in broker_data['issues'])
                
                if has_critical:
                    overall_status = "🔴 Critical"
                elif has_warning:
                    overall_status = "⚠️ Warning"
                else:
                    overall_status = "✅ OK"
                
                table_lines.append(
                    f"|{broker_id}|{host}|{memlock_status}|{nofile_status}|{as_status}|{nproc_status}|{swappiness_val} {swappiness_status}|{overall_status}"
                )
            
            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()
        
        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check system limits on {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )
        
        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}
            
            if critical_brokers:
                recommendations["critical"] = [
                    "**IMMEDIATE ACTION REQUIRED:** Fix system limits on affected brokers",
                    "**Edit /etc/security/limits.conf** and add/update the following for the Kafka user:",
                    "  kafka soft memlock unlimited",
                    "  kafka hard memlock unlimited",
                    "  kafka soft nofile 100000",
                    "  kafka hard nofile 100000",
                    "  kafka soft as unlimited",
                    "  kafka hard as unlimited",
                    "  kafka soft nproc 32768",
                    "  kafka hard nproc 32768",
                    "",
                    "**Set vm.swappiness:** Add to /etc/sysctl.conf:",
                    "  vm.swappiness=1",
                    "  Then apply: sysctl -p",
                    "",
                    "**Restart Kafka broker** after making changes (limits require process restart)",
                    "**Verify changes:** Check /proc/<kafka-pid>/limits after restart"
                ]
            
            if warning_brokers:
                recommendations["high"] = [
                    "**Review system limits** on warning brokers",
                    "**Ensure consistency** across all brokers in the cluster",
                    "**Monitor** for any performance issues related to limits",
                    "**Plan maintenance window** to update limits if needed"
                ]
            
            recommendations["general"] = [
                "System limits are critical for Kafka performance and stability",
                "MEMLOCK=unlimited allows Kafka to lock memory pages (reduces GC pressure)",
                "NOFILE>=100000 prevents file descriptor exhaustion (Kafka uses many FDs)",
                "AS=unlimited allows Kafka to use virtual memory as needed",
                "NPROC>=32768 ensures sufficient process/thread capacity",
                "vm.swappiness=0 or 1 minimizes swap usage (swap is very slow for Kafka)",
                "All limits require broker restart to take effect",
                "Verify limits after restart: cat /proc/<kafka-pid>/limits"
            ]
            
            builder.recs(recommendations)
        else:
            builder.success(
                f"✅ All system limits are properly configured across all brokers.\n\n"
                f"All brokers meet the recommended limits for optimal Kafka performance."
            )
        
        # === STRUCTURED DATA ===
        structured_data["system_limits"] = {
            "status": "success",
            "brokers_checked": len(connector.get_ssh_hosts()),
            "brokers_with_errors": len(set(e['broker_id'] for e in errors)),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "min_nofile": min_nofile,
                "min_nproc": min_nproc,
                "max_swappiness": max_swappiness
            },
            "errors": errors,
            "data": all_limits_data
        }
    
    except Exception as e:
        import traceback
        logger.error(f"System limits check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["system_limits"] = {
            "status": "error",
            "details": str(e)
        }
    
    return builder.build(), structured_data

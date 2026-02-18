"""
Broker Configuration Audit check for Kafka brokers.

Audits server.properties configuration for best practices and potential issues.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.kafka.utils.qrylib.broker_config_queries import get_server_properties_query
import re
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def parse_properties(content):
    """Parse Java properties file format into dict."""
    props = {}
    for line in content.split('\n'):
        line = line.strip()
        # Skip comments and empty lines
        if not line or line.startswith('#'):
            continue
        # Parse key=value
        if '=' in line:
            key, value = line.split('=', 1)
            props[key.strip()] = value.strip()
    return props


def run_broker_config_check(connector, settings):
    """
    Audits Kafka broker configuration (server.properties) for best practices.

    Checks for common misconfigurations, suboptimal settings, and security issues.

    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Broker config audit")
    if not available:
        return skip_msg, skip_data

    try:
        builder.h3("Broker Configuration Audit (All Brokers)")
        builder.para("Analyzing server.properties for best practices and potential issues.")
        builder.blank()

        # === CHECK ALL BROKERS ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_config_data = []
        issues_found = False
        warning_brokers = []
        errors = []

        # Configuration checks to perform
        config_issues = []

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Execute query using path from config (kafka_config_file_path, mandatory when SSH is used)
                query = get_server_properties_query(connector)
                formatted, raw = connector.execute_query(query, return_raw=True)

                if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
                    error_msg = raw.get('error', 'Unknown error') if isinstance(raw, dict) else formatted
                    logger.warning(f"Could not read server.properties on {ssh_host}: {error_msg}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': f"Could not read server.properties: {error_msg}"
                    })
                    continue

                # Check if config was not found
                stdout = raw if isinstance(raw, str) else str(raw)
                if 'ERROR: server.properties not found' in stdout:
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': 'server.properties not found'
                    })
                    continue

                # Parse configuration
                props = parse_properties(stdout)

                broker_issues = []

                # === AUDIT CHECKS ===

                # 1. Log retention settings
                log_retention_hours = props.get('log.retention.hours', '')
                log_retention_ms = props.get('log.retention.ms', '')
                if log_retention_hours:
                    hours = int(log_retention_hours) if log_retention_hours.isdigit() else 0
                    if hours > 720:  # > 30 days
                        broker_issues.append({
                            'severity': 'info',
                            'setting': 'log.retention.hours',
                            'value': log_retention_hours,
                            'issue': f'Very long retention ({hours}h = {hours//24} days) may impact disk usage'
                        })
                elif not log_retention_ms:
                    broker_issues.append({
                        'severity': 'warning',
                        'setting': 'log.retention',
                        'value': 'not set',
                        'issue': 'No log retention policy configured (using default)'
                    })

                # 2. Replication factor defaults
                default_repl_factor = props.get('default.replication.factor', '')
                if default_repl_factor:
                    factor = int(default_repl_factor) if default_repl_factor.isdigit() else 0
                    if factor < 3:
                        broker_issues.append({
                            'severity': 'warning',
                            'setting': 'default.replication.factor',
                            'value': default_repl_factor,
                            'issue': 'Replication factor < 3 reduces fault tolerance'
                        })

                # 3. Min ISR setting
                min_isr = props.get('min.insync.replicas', '')
                if min_isr:
                    isr_val = int(min_isr) if min_isr.isdigit() else 0
                    if default_repl_factor and isr_val >= int(default_repl_factor):
                        broker_issues.append({
                            'severity': 'critical',
                            'setting': 'min.insync.replicas',
                            'value': min_isr,
                            'issue': f'min.insync.replicas ({isr_val}) >= replication.factor ({default_repl_factor}) will block writes'
                        })

                # 4. Number of network threads
                num_network_threads = props.get('num.network.threads', '')
                if num_network_threads:
                    threads = int(num_network_threads) if num_network_threads.isdigit() else 0
                    if threads < 3:
                        broker_issues.append({
                            'severity': 'info',
                            'setting': 'num.network.threads',
                            'value': num_network_threads,
                            'issue': 'Low network thread count may limit throughput'
                        })

                # 5. Number of I/O threads
                num_io_threads = props.get('num.io.threads', '')
                if num_io_threads:
                    threads = int(num_io_threads) if num_io_threads.isdigit() else 0
                    if threads < 8:
                        broker_issues.append({
                            'severity': 'info',
                            'setting': 'num.io.threads',
                            'value': num_io_threads,
                            'issue': 'Low I/O thread count may limit disk performance'
                        })

                # 6. Socket buffer sizes
                socket_send = props.get('socket.send.buffer.bytes', '')
                socket_recv = props.get('socket.receive.buffer.bytes', '')
                if socket_send:
                    send_val = int(socket_send) if socket_send.isdigit() else 0
                    if send_val < 102400:  # < 100KB
                        broker_issues.append({
                            'severity': 'info',
                            'setting': 'socket.send.buffer.bytes',
                            'value': socket_send,
                            'issue': 'Small send buffer may limit network throughput'
                        })

                # 7. Log segment size
                log_segment_bytes = props.get('log.segment.bytes', '')
                if log_segment_bytes:
                    segment_size = int(log_segment_bytes) if log_segment_bytes.isdigit() else 0
                    if segment_size < 536870912:  # < 512MB
                        broker_issues.append({
                            'severity': 'info',
                            'setting': 'log.segment.bytes',
                            'value': log_segment_bytes,
                            'issue': 'Small log segments may cause excessive file handle usage'
                        })

                # 8. Compression type
                compression_type = props.get('compression.type', 'producer')
                if compression_type == 'uncompressed':
                    broker_issues.append({
                        'severity': 'info',
                        'setting': 'compression.type',
                        'value': compression_type,
                        'issue': 'No compression enabled - may increase disk and network usage'
                    })

                # 9. Auto create topics
                auto_create = props.get('auto.create.topics.enable', '')
                if auto_create == 'true':
                    broker_issues.append({
                        'severity': 'warning',
                        'setting': 'auto.create.topics.enable',
                        'value': 'true',
                        'issue': 'Auto topic creation enabled - can lead to accidental topic creation'
                    })

                # 10. Unclean leader election
                unclean_election = props.get('unclean.leader.election.enable', '')
                if unclean_election == 'true':
                    broker_issues.append({
                        'severity': 'critical',
                        'setting': 'unclean.leader.election.enable',
                        'value': 'true',
                        'issue': 'Unclean leader election enabled - CAN CAUSE DATA LOSS'
                    })

                config_info = {
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'issues': broker_issues,
                    'critical_count': sum(1 for i in broker_issues if i['severity'] == 'critical'),
                    'warning_count': sum(1 for i in broker_issues if i['severity'] == 'warning'),
                    'info_count': sum(1 for i in broker_issues if i['severity'] == 'info'),
                    'key_settings': {
                        'log.retention.hours': props.get('log.retention.hours', 'not set'),
                        'default.replication.factor': props.get('default.replication.factor', 'not set'),
                        'min.insync.replicas': props.get('min.insync.replicas', 'not set'),
                        'num.network.threads': props.get('num.network.threads', 'not set'),
                        'num.io.threads': props.get('num.io.threads', 'not set')
                    }
                }
                all_config_data.append(config_info)

                # Report critical/warning issues
                critical_issues = [i for i in broker_issues if i['severity'] == 'critical']
                if critical_issues:
                    issues_found = True
                    if broker_id not in warning_brokers:
                        warning_brokers.append(broker_id)

                    for issue in critical_issues:
                        builder.critical_issue(
                            f"Critical Config Issue: {issue['setting']}",
                            {
                                "Broker": f"{broker_id} ({ssh_host})",
                                "Setting": issue['setting'],
                                "Value": issue['value'],
                                "Issue": issue['issue']
                            }
                        )

                warning_issues = [i for i in broker_issues if i['severity'] == 'warning']
                if warning_issues:
                    issues_found = True
                    if broker_id not in warning_brokers:
                        warning_brokers.append(broker_id)

                    for issue in warning_issues:
                        builder.warning_issue(
                            f"Config Warning: {issue['setting']}",
                            {
                                "Broker": f"{broker_id} ({ssh_host})",
                                "Setting": issue['setting'],
                                "Value": issue['value'],
                                "Issue": issue['issue']
                            }
                        )

            except Exception as e:
                logger.error(f"Error analyzing config on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_config_data:
            builder.h4("Configuration Issues Summary")

            table_lines = [
                "|===",
                "|Broker|Host|Critical|Warning|Info|Status"
            ]

            for config in sorted(all_config_data, key=lambda x: (x['critical_count'], x['warning_count']), reverse=True):
                if config['critical_count'] > 0:
                    indicator = "🔴"
                elif config['warning_count'] > 0:
                    indicator = "⚠️"
                elif config['info_count'] > 0:
                    indicator = "ℹ️"
                else:
                    indicator = "✅"

                table_lines.append(
                    f"|{config['broker_id']}|{config['host']}|{config['critical_count']}|"
                    f"{config['warning_count']}|{config['info_count']}|{indicator}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Config Access Errors")
            builder.warning(
                f"Could not audit config on {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {
                "critical": [
                    "**Review critical settings immediately:** Critical issues can cause data loss",
                    "**Test config changes in staging first:** Never change production configs without testing",
                    "**Document configuration decisions:** Maintain config change history",
                    "**Use configuration management:** Ansible, Chef, Puppet for consistency"
                ],
                "high": [
                    "**Establish configuration baselines:** Define standard configs for your environment",
                    "**Regular configuration audits:** Schedule periodic reviews",
                    "**Monitor configuration drift:** Detect unauthorized changes",
                    "**Benchmark performance:** Test impact of config changes"
                ],
                "general": [
                    "Maintain separate configs for dev/staging/prod environments",
                    "Use Kafka's dynamic configuration where possible",
                    "Review Kafka documentation for each setting",
                    "Consider using Confluent's configuration best practices",
                    "Test configuration changes during maintenance windows"
                ]
            }

            builder.recs(recommendations)
        else:
            builder.success(
                "No configuration issues found.\n\n"
                "All broker configurations appear properly configured."
            )

        # === STRUCTURED DATA ===
        structured_data["broker_config"] = {
            "status": "success",
            "brokers_checked": len(connector.get_ssh_hosts()),
            "brokers_with_errors": len(set(e['broker_id'] for e in errors)),
            "warning_brokers": warning_brokers,
            "errors": errors,
            "data": all_config_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Broker config audit failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["broker_config"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

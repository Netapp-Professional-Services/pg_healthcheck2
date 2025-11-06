"""
File Descriptor Usage check for Kafka brokers.

Checks file descriptor usage vs limits across all Kafka broker nodes via SSH.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.common.parsers import _safe_int
from plugins.kafka.utils.qrylib.file_descriptor_queries import (
    get_file_descriptor_limit_query,
    get_kafka_process_fd_query
)
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def run_file_descriptor_check(connector, settings):
    """
    Analyzes file descriptor usage for Kafka broker processes on all nodes.

    Checks both the system-level limits (ulimit -n) and actual usage by the
    Kafka process (lsof) to detect potential FD exhaustion.

    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "File descriptor check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds (percentage of limit)
        warning_threshold = settings.get('kafka_fd_warning', 70)
        critical_threshold = settings.get('kafka_fd_critical', 85)

        builder.h3("File Descriptor Usage Analysis (All Brokers)")
        builder.para("Checking Kafka process file descriptor usage against system limits.")
        builder.blank()

        # === CHECK ALL BROKERS ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_fd_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # === Get FD limit ===
                limit_query = get_file_descriptor_limit_query(connector)
                limit_formatted, limit_raw = connector.execute_query(limit_query, return_raw=True)

                if "[ERROR]" in limit_formatted or (isinstance(limit_raw, dict) and 'error' in limit_raw):
                    error_msg = limit_raw.get('error', 'Unknown error') if isinstance(limit_raw, dict) else limit_formatted
                    logger.warning(f"ulimit command failed on {ssh_host}: {error_msg}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': f"ulimit command failed: {error_msg}"
                    })
                    continue

                fd_limit = _safe_int(limit_raw.strip() if isinstance(limit_raw, str) else str(limit_raw).strip())

                if fd_limit == 0:
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': 'Could not parse file descriptor limit'
                    })
                    continue

                # === Get Kafka process FD usage ===
                usage_query = get_kafka_process_fd_query(connector)
                usage_formatted, usage_raw = connector.execute_query(usage_query, return_raw=True)

                if "[ERROR]" in usage_formatted or (isinstance(usage_raw, dict) and 'error' in usage_raw):
                    error_msg = usage_raw.get('error', 'Unknown error') if isinstance(usage_raw, dict) else usage_formatted
                    logger.warning(f"lsof command failed on {ssh_host}: {error_msg}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': f"lsof command failed: {error_msg}"
                    })
                    continue

                # Parse lsof output (includes header line, so subtract 1)
                fd_used_raw = _safe_int(usage_raw.strip() if isinstance(usage_raw, str) else str(usage_raw).strip())
                fd_used = max(0, fd_used_raw - 1) if fd_used_raw > 0 else 0

                # Calculate usage percentage
                usage_pct = (fd_used / fd_limit * 100) if fd_limit > 0 else 0

                fd_info = {
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'fd_limit': fd_limit,
                    'fd_used': fd_used,
                    'fd_available': fd_limit - fd_used,
                    'usage_pct': round(usage_pct, 2),
                    'exceeds_critical': usage_pct >= critical_threshold,
                    'exceeds_warning': usage_pct >= warning_threshold
                }
                all_fd_data.append(fd_info)

                # === Check thresholds ===
                if usage_pct >= critical_threshold:
                    issues_found = True
                    if broker_id not in critical_brokers:
                        critical_brokers.append(broker_id)

                    builder.critical_issue(
                        "Critical File Descriptor Usage",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "FD Limit": f"{fd_limit:,}",
                            "FD Used": f"{fd_used:,} ({usage_pct:.1f}%)",
                            "FD Available": f"{fd_info['fd_available']:,}",
                            "Threshold": f"{critical_threshold}%"
                        }
                    )
                    builder.para("**Critical FD exhaustion risk - immediate action required!**")
                    builder.blank()

                elif usage_pct >= warning_threshold:
                    issues_found = True
                    if broker_id not in warning_brokers:
                        warning_brokers.append(broker_id)

                    builder.warning_issue(
                        "High File Descriptor Usage",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "FD Limit": f"{fd_limit:,}",
                            "FD Used": f"{fd_used:,} ({usage_pct:.1f}%)",
                            "FD Available": f"{fd_info['fd_available']:,}",
                            "Threshold": f"{warning_threshold}%"
                        }
                    )

            except Exception as e:
                logger.error(f"Error checking file descriptors on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_fd_data:
            builder.h4("File Descriptor Summary")

            table_lines = [
                "|===",
                "|Broker|Host|Limit|Used|Available|Usage %|Status"
            ]

            for fd in sorted(all_fd_data, key=lambda x: x['usage_pct'], reverse=True):
                indicator = ""
                if fd['exceeds_critical']:
                    indicator = "🔴"
                elif fd['exceeds_warning']:
                    indicator = "⚠️"
                else:
                    indicator = "✅"

                table_lines.append(
                    f"|{fd['broker_id']}|{fd['host']}|{fd['fd_limit']:,}|"
                    f"{fd['fd_used']:,}|{fd['fd_available']:,}|{fd['usage_pct']:.1f}%|{indicator}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check file descriptors on {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_brokers:
                recommendations["critical"] = [
                    "**Increase FD limits immediately:** Edit `/etc/security/limits.conf` to raise `nofile` limits",
                    "**Check for connection leaks:** Review Kafka logs for unclosed connections",
                    "**Identify open file handles:** Use `lsof -p <kafka-pid> | head -100` to see what's open",
                    "**Review connection settings:** Check `num.network.threads` and `num.io.threads`",
                    "**Monitor network connections:** High connection count indicates client connection issues",
                    "**Restart broker if necessary:** After increasing limits, restart may be required"
                ]

            if warning_brokers:
                recommendations["high"] = [
                    "**Monitor FD usage trends:** Track growth over time to predict exhaustion",
                    "**Review producer/consumer connections:** Check for connection pooling misconfigurations",
                    "**Audit log file handles:** Excessive log segments can consume FDs",
                    "**Check socket buffer settings:** `socket.send.buffer.bytes` and `socket.receive.buffer.bytes`",
                    "**Review topic partition count:** More partitions = more file handles"
                ]

            recommendations["general"] = [
                "Set FD limits to at least 100,000 for Kafka brokers",
                "Monitor FD usage as part of regular health checks",
                "Alert when usage exceeds 60% of limit",
                "Review connection timeouts to prevent FD leaks",
                "Regular audit of client connection patterns",
                "Consider using connection pooling for clients"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"File descriptor usage is healthy across all brokers.\n\n"
                f"All brokers have FD usage below {warning_threshold}%."
            )

        # === STRUCTURED DATA ===
        structured_data["file_descriptors"] = {
            "status": "success",
            "brokers_checked": len(connector.get_ssh_hosts()),
            "brokers_with_errors": len(set(e['broker_id'] for e in errors)),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "warning": warning_threshold,
                "critical": critical_threshold
            },
            "errors": errors,
            "data": all_fd_data
        }

    except Exception as e:
        import traceback
        logger.error(f"File descriptor check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["file_descriptors"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

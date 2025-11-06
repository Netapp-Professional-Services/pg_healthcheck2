"""
Memory Usage check for Kafka brokers.

Checks system memory usage across all Kafka broker nodes via SSH.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.common.parsers import _parse_size_to_bytes, _safe_int
from plugins.kafka.utils.qrylib.memory_usage_queries import get_memory_usage_query
import re
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def run_memory_usage_check(connector, settings):
    """
    Analyzes system memory usage on all Kafka broker nodes using 'free -m' command.

    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Memory usage check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds (percentage of total memory)
        warning_threshold = settings.get('kafka_memory_warning', 80)
        critical_threshold = settings.get('kafka_memory_critical', 90)

        builder.h3("Memory Usage Analysis (All Brokers)")
        builder.para("Checking system memory usage using `free -m` command.")
        builder.blank()

        # === CHECK ALL BROKERS ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_memory_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Execute query via connector (uses qrylib)
                query = get_memory_usage_query(connector)
                formatted, raw = connector.execute_query(query, return_raw=True)

                if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
                    error_msg = raw.get('error', 'Unknown error') if isinstance(raw, dict) else formatted
                    logger.warning(f"free command failed on {ssh_host}: {error_msg}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': f"free command failed: {error_msg}"
                    })
                    continue

                # Parse free -m output
                # Example:
                #               total        used        free      shared  buff/cache   available
                # Mem:          15880        8234        1234         123        6412        7012
                stdout = raw if isinstance(raw, str) else str(raw)

                # Look for the Mem: line
                mem_match = re.search(r'Mem:\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+\s+(\d+)\s+(\d+)', stdout)

                if not mem_match:
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': 'Could not parse memory usage from free output'
                    })
                    continue

                total_mb = _safe_int(mem_match.group(1))
                used_mb = _safe_int(mem_match.group(2))
                free_mb = _safe_int(mem_match.group(3))
                buff_cache_mb = _safe_int(mem_match.group(4))
                available_mb = _safe_int(mem_match.group(5))

                # Calculate usage percentage (based on used vs total)
                usage_pct = (used_mb / total_mb * 100) if total_mb > 0 else 0
                available_pct = (available_mb / total_mb * 100) if total_mb > 0 else 0

                memory_info = {
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'total_mb': total_mb,
                    'used_mb': used_mb,
                    'free_mb': free_mb,
                    'buff_cache_mb': buff_cache_mb,
                    'available_mb': available_mb,
                    'usage_pct': round(usage_pct, 2),
                    'available_pct': round(available_pct, 2),
                    'exceeds_critical': usage_pct >= critical_threshold,
                    'exceeds_warning': usage_pct >= warning_threshold
                }
                all_memory_data.append(memory_info)

                # === Check thresholds ===
                if usage_pct >= critical_threshold:
                    issues_found = True
                    if broker_id not in critical_brokers:
                        critical_brokers.append(broker_id)

                    builder.critical_issue(
                        "Critical Memory Usage",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "Total Memory": f"{total_mb} MB",
                            "Used Memory": f"{used_mb} MB ({usage_pct:.1f}%)",
                            "Available Memory": f"{available_mb} MB ({available_pct:.1f}%)",
                            "Threshold": f"{critical_threshold}%"
                        }
                    )
                    builder.para("**Critical memory pressure detected - immediate action required!**")
                    builder.blank()

                elif usage_pct >= warning_threshold:
                    issues_found = True
                    if broker_id not in warning_brokers:
                        warning_brokers.append(broker_id)

                    builder.warning_issue(
                        "High Memory Usage",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "Total Memory": f"{total_mb} MB",
                            "Used Memory": f"{used_mb} MB ({usage_pct:.1f}%)",
                            "Available Memory": f"{available_mb} MB ({available_pct:.1f}%)",
                            "Threshold": f"{warning_threshold}%"
                        }
                    )

            except Exception as e:
                logger.error(f"Error checking memory usage on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_memory_data:
            builder.h4("Memory Usage Summary")

            table_lines = [
                "|===",
                "|Broker|Host|Total (MB)|Used (MB)|Available (MB)|Usage %|Status"
            ]

            for mem in sorted(all_memory_data, key=lambda x: x['usage_pct'], reverse=True):
                indicator = ""
                if mem['exceeds_critical']:
                    indicator = "🔴"
                elif mem['exceeds_warning']:
                    indicator = "⚠️"
                else:
                    indicator = "✅"

                table_lines.append(
                    f"|{mem['broker_id']}|{mem['host']}|{mem['total_mb']:,}|"
                    f"{mem['used_mb']:,}|{mem['available_mb']:,}|{mem['usage_pct']:.1f}%|{indicator}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check memory usage on {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_brokers:
                recommendations["critical"] = [
                    "**Identify memory-intensive processes:** Use `top` or `htop` to find high memory consumers",
                    "**Check Kafka heap settings:** Review `-Xmx` and `-Xms` JVM heap size settings",
                    "**Review page cache usage:** Kafka relies heavily on OS page cache for performance",
                    "**Check for memory leaks:** Review GC logs and heap dump for potential leaks",
                    "**Consider scaling:** Add more memory or distribute load across more brokers",
                    "**Monitor swap usage:** Check if system is swapping (kills Kafka performance)"
                ]

            if warning_brokers:
                recommendations["high"] = [
                    "**Monitor memory trends:** Track usage over time to identify growth patterns",
                    "**Review topic retention:** Excessive retention can increase page cache pressure",
                    "**Check producer batching:** Large batches increase memory usage",
                    "**Optimize consumer fetch sizes:** Large fetch sizes consume more memory",
                    "**Review replica fetcher settings:** Can impact memory consumption"
                ]

            recommendations["general"] = [
                "Set up memory usage alerts at 75% threshold",
                "Monitor heap vs off-heap memory usage",
                "Review JVM GC settings for optimal heap management",
                "Consider tuning socket buffer sizes to reduce memory pressure",
                "Regular memory profiling to establish baselines",
                "Ensure adequate swap space is available (but not actively used)"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Memory usage is healthy across all brokers.\n\n"
                f"All brokers have memory usage below {warning_threshold}%."
            )

        # === STRUCTURED DATA ===
        structured_data["memory_usage"] = {
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
            "data": all_memory_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Memory usage check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["memory_usage"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

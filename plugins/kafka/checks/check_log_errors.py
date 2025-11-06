"""
Log Error Analysis check for Kafka brokers.

Scans broker logs for ERROR, FATAL, and WARNING patterns across all nodes.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.kafka.utils.qrylib.log_file_queries import get_server_log_query
import re
import logging
from collections import Counter

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def run_log_errors_check(connector, settings):
    """
    Analyzes Kafka broker logs for ERROR, FATAL, and WARNING messages.

    Scans recent log entries (default: last 1000 lines) for error patterns,
    categorizes them, and reports severity across all brokers.

    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Log error analysis")
    if not available:
        return skip_msg, skip_data

    try:
        # Get settings
        num_lines = settings.get('kafka_log_lines_to_scan', 1000)
        fatal_threshold = settings.get('kafka_log_fatal_threshold', 1)
        error_threshold = settings.get('kafka_log_error_threshold', 10)
        warning_threshold = settings.get('kafka_log_warning_threshold', 50)

        builder.h3("Broker Log Error Analysis (All Brokers)")
        builder.para(f"Scanning last {num_lines} lines of server.log for errors and warnings.")
        builder.blank()

        # === CHECK ALL BROKERS ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_log_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Execute query via connector (uses qrylib)
                query = get_server_log_query(connector, num_lines=num_lines)
                formatted, raw = connector.execute_query(query, return_raw=True)

                if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
                    error_msg = raw.get('error', 'Unknown error') if isinstance(raw, dict) else formatted
                    logger.warning(f"Could not read server.log on {ssh_host}: {error_msg}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': f"Could not read server.log: {error_msg}"
                    })
                    continue

                # Check if log was not found
                stdout = raw if isinstance(raw, str) else str(raw)
                if 'ERROR: server.log not found' in stdout:
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': 'server.log not found'
                    })
                    continue

                # Parse log for error patterns
                log_lines = stdout.split('\n')

                fatal_count = 0
                error_count = 0
                warning_count = 0
                fatal_messages = []
                error_categories = Counter()
                warning_categories = Counter()

                for line in log_lines:
                    # Skip empty lines
                    if not line.strip():
                        continue

                    # Detect FATAL
                    if re.search(r'\b(FATAL|SEVERE)\b', line, re.IGNORECASE):
                        fatal_count += 1
                        # Extract brief message (first 100 chars after FATAL)
                        msg_match = re.search(r'(FATAL|SEVERE)[:\s]+(.*)', line, re.IGNORECASE)
                        if msg_match and len(fatal_messages) < 5:  # Keep first 5
                            fatal_messages.append(msg_match.group(2)[:100])

                    # Detect ERROR
                    elif re.search(r'\bERROR\b', line, re.IGNORECASE):
                        error_count += 1
                        # Categorize error
                        if 'Connection' in line or 'Socket' in line:
                            error_categories['Connection/Network'] += 1
                        elif 'Timeout' in line:
                            error_categories['Timeout'] += 1
                        elif 'Exception' in line:
                            error_categories['Exception'] += 1
                        elif 'I/O' in line or 'IOException' in line:
                            error_categories['I/O'] += 1
                        else:
                            error_categories['Other'] += 1

                    # Detect WARN
                    elif re.search(r'\bWARN(ING)?\b', line, re.IGNORECASE):
                        warning_count += 1
                        # Categorize warning
                        if 'ISR' in line or 'replica' in line.lower():
                            warning_categories['Replication'] += 1
                        elif 'lag' in line.lower():
                            warning_categories['Lag'] += 1
                        elif 'leader' in line.lower():
                            warning_categories['Leadership'] += 1
                        elif 'partition' in line.lower():
                            warning_categories['Partition'] += 1
                        else:
                            warning_categories['Other'] += 1

                log_info = {
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'fatal_count': fatal_count,
                    'error_count': error_count,
                    'warning_count': warning_count,
                    'fatal_messages': fatal_messages,
                    'error_categories': dict(error_categories),
                    'warning_categories': dict(warning_categories),
                    'exceeds_fatal': fatal_count >= fatal_threshold,
                    'exceeds_error': error_count >= error_threshold,
                    'exceeds_warning': warning_count >= warning_threshold
                }
                all_log_data.append(log_info)

                # === Check thresholds ===
                if fatal_count >= fatal_threshold:
                    issues_found = True
                    if broker_id not in critical_brokers:
                        critical_brokers.append(broker_id)

                    builder.critical_issue(
                        "FATAL Errors Detected",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "FATAL Count": str(fatal_count),
                            "Threshold": str(fatal_threshold)
                        }
                    )

                    # Show fatal messages
                    if fatal_messages:
                        builder.para("**Recent FATAL messages:**")
                        for msg in fatal_messages[:3]:  # Show top 3
                            builder.para(f"* {msg}")
                    builder.blank()

                elif error_count >= error_threshold:
                    issues_found = True
                    if broker_id not in warning_brokers:
                        warning_brokers.append(broker_id)

                    builder.warning_issue(
                        "High Error Count",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "ERROR Count": str(error_count),
                            "WARNING Count": str(warning_count),
                            "Threshold": str(error_threshold)
                        }
                    )

            except Exception as e:
                logger.error(f"Error analyzing logs on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_log_data:
            builder.h4("Log Error Summary")

            table_lines = [
                "|===",
                "|Broker|Host|FATAL|ERROR|WARNING|Status"
            ]

            for log in sorted(all_log_data, key=lambda x: (x['fatal_count'], x['error_count']), reverse=True):
                indicator = ""
                if log['exceeds_fatal']:
                    indicator = "🔴"
                elif log['exceeds_error']:
                    indicator = "⚠️"
                elif log['exceeds_warning']:
                    indicator = "⚠️"
                else:
                    indicator = "✅"

                table_lines.append(
                    f"|{log['broker_id']}|{log['host']}|{log['fatal_count']}|"
                    f"{log['error_count']}|{log['warning_count']}|{indicator}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

            # === ERROR CATEGORIES ===
            # Aggregate error categories across all brokers
            all_error_cats = Counter()
            all_warning_cats = Counter()

            for log in all_log_data:
                for cat, count in log['error_categories'].items():
                    all_error_cats[cat] += count
                for cat, count in log['warning_categories'].items():
                    all_warning_cats[cat] += count

            if all_error_cats:
                builder.h4("Error Categories (Cluster-wide)")
                cat_lines = ["|===", "|Category|Count"]
                for cat, count in all_error_cats.most_common():
                    cat_lines.append(f"|{cat}|{count}")
                cat_lines.append("|===")
                builder.add("\n".join(cat_lines))
                builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Log Access Errors")
            builder.warning(
                f"Could not analyze logs on {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_brokers:
                recommendations["critical"] = [
                    "**Investigate FATAL errors immediately:** FATAL errors indicate severe issues",
                    "**Check full server.log:** Review complete log context around FATAL messages",
                    "**Review controller.log:** Controller issues often cause cluster-wide problems",
                    "**Check state-change.log:** Leader election and ISR changes may indicate instability",
                    "**Monitor cluster health:** FATAL errors often precede broker failures"
                ]

            if warning_brokers:
                recommendations["high"] = [
                    "**Categorize errors:** Group errors by type to identify patterns",
                    "**Check for error trends:** Are errors increasing over time?",
                    "**Review recent changes:** Correlate errors with deployments or config changes",
                    "**Examine connection errors:** Network issues may indicate infrastructure problems",
                    "**Check timeout patterns:** May indicate performance degradation"
                ]

            recommendations["general"] = [
                "Set up centralized log aggregation (ELK, Splunk, etc.)",
                "Configure log rotation to prevent disk space issues",
                "Monitor log growth rate as indicator of cluster health",
                "Establish baseline error rates for alerting",
                "Regular log analysis to catch issues early",
                "Correlate log errors with metrics (latency, throughput, etc.)"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"No significant errors found in recent logs.\n\n"
                f"Scanned {num_lines} lines across all brokers."
            )

        # === STRUCTURED DATA ===
        structured_data["log_errors"] = {
            "status": "success",
            "brokers_checked": len(connector.get_ssh_hosts()),
            "brokers_with_errors": len(set(e['broker_id'] for e in errors)),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "fatal": fatal_threshold,
                "error": error_threshold,
                "warning": warning_threshold
            },
            "errors": errors,
            "data": all_log_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Log error analysis failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["log_errors"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

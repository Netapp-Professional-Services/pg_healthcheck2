"""
GC Pause Analysis check for Kafka brokers.

Analyzes GC logs for pause duration, frequency, and memory pressure indicators.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.common.parsers import _safe_float
from plugins.kafka.utils.qrylib.log_file_queries import get_gc_log_query
import re
import logging
from collections import Counter

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def run_gc_pauses_check(connector, settings):
    """
    Analyzes Kafka broker GC logs for pause duration and memory pressure.

    Scans GC logs for pause patterns, duration, and frequency to identify
    memory management issues.

    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "GC pause analysis")
    if not available:
        return skip_msg, skip_data

    try:
        # Get settings
        num_lines = settings.get('kafka_gc_log_lines_to_scan', 500)
        critical_pause_ms = settings.get('kafka_gc_pause_critical', 1000)  # 1s
        warning_pause_ms = settings.get('kafka_gc_pause_warning', 500)    # 500ms
        high_frequency_threshold = settings.get('kafka_gc_frequency_warning', 50)  # 50 GCs in sample

        builder.h3("GC Pause Analysis (All Brokers)")
        builder.para(f"Analyzing last {num_lines} lines of GC logs for pause duration and memory pressure.")
        builder.blank()

        # === CHECK ALL BROKERS ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_gc_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Execute query via connector (uses qrylib)
                query = get_gc_log_query(connector, num_lines=num_lines)
                formatted, raw = connector.execute_query(query, return_raw=True)

                if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
                    error_msg = raw.get('error', 'Unknown error') if isinstance(raw, dict) else formatted
                    logger.warning(f"Could not read GC log on {ssh_host}: {error_msg}")
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': f"Could not read GC log: {error_msg}"
                    })
                    continue

                # Check if log was not found
                stdout = raw if isinstance(raw, str) else str(raw)
                if 'GC log not found' in stdout:
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': 'GC log not found - JVM may not be configured to log GC'
                    })
                    continue

                # Parse GC log for pause patterns
                log_lines = stdout.split('\n')

                gc_pauses = []
                gc_types = Counter()
                total_pause_time_ms = 0

                for line in log_lines:
                    # Skip empty lines
                    if not line.strip():
                        continue

                    # Pattern 1: G1GC/ParallelGC format - look for pause time
                    # Example: [GC pause (G1 Evacuation Pause) 245M->156M(512M), 0.0234567 secs]
                    pause_match = re.search(r'pause.*?(\d+\.\d+)\s*secs?', line, re.IGNORECASE)
                    if pause_match:
                        pause_secs = _safe_float(pause_match.group(1))
                        pause_ms = pause_secs * 1000
                        gc_pauses.append(pause_ms)
                        total_pause_time_ms += pause_ms

                        # Identify GC type
                        if 'Full GC' in line or 'Full' in line:
                            gc_types['Full GC'] += 1
                        elif 'Young' in line or 'minor' in line.lower():
                            gc_types['Young GC'] += 1
                        elif 'Mixed' in line:
                            gc_types['Mixed GC'] += 1
                        else:
                            gc_types['GC'] += 1
                        continue

                    # Pattern 2: Alternative format with time in ms
                    # Example: GC(123) Pause Young (Normal) 123M->45M(512M) 123.456ms
                    ms_match = re.search(r'(\d+\.\d+)\s*ms', line, re.IGNORECASE)
                    if ms_match and ('pause' in line.lower() or 'gc' in line.lower()):
                        pause_ms = _safe_float(ms_match.group(1))
                        gc_pauses.append(pause_ms)
                        total_pause_time_ms += pause_ms

                        if 'Full' in line:
                            gc_types['Full GC'] += 1
                        elif 'Young' in line or 'Minor' in line:
                            gc_types['Young GC'] += 1
                        else:
                            gc_types['GC'] += 1

                # Calculate statistics
                gc_count = len(gc_pauses)

                if gc_count == 0:
                    errors.append({
                        'host': ssh_host,
                        'broker_id': broker_id,
                        'error': 'No GC pause data found in log - may need different parsing'
                    })
                    continue

                max_pause = max(gc_pauses) if gc_pauses else 0
                avg_pause = sum(gc_pauses) / gc_count if gc_count > 0 else 0
                p95_pause = sorted(gc_pauses)[int(gc_count * 0.95)] if gc_count > 0 else 0
                full_gc_count = gc_types.get('Full GC', 0)

                gc_info = {
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'gc_count': gc_count,
                    'max_pause_ms': round(max_pause, 2),
                    'avg_pause_ms': round(avg_pause, 2),
                    'p95_pause_ms': round(p95_pause, 2),
                    'total_pause_ms': round(total_pause_time_ms, 2),
                    'full_gc_count': full_gc_count,
                    'gc_types': dict(gc_types),
                    'exceeds_critical': max_pause >= critical_pause_ms,
                    'exceeds_warning': max_pause >= warning_pause_ms,
                    'high_frequency': gc_count >= high_frequency_threshold,
                    'has_full_gc': full_gc_count > 0
                }
                all_gc_data.append(gc_info)

                # === Check thresholds ===
                if max_pause >= critical_pause_ms:
                    issues_found = True
                    if broker_id not in critical_brokers:
                        critical_brokers.append(broker_id)

                    builder.critical_issue(
                        "Critical GC Pause Duration",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "Max Pause": f"{max_pause:.1f}ms",
                            "Avg Pause": f"{avg_pause:.1f}ms",
                            "P95 Pause": f"{p95_pause:.1f}ms",
                            "GC Count": str(gc_count),
                            "Full GCs": str(full_gc_count),
                            "Threshold": f"{critical_pause_ms}ms"
                        }
                    )
                    builder.para("**Long GC pauses cause message processing delays and can trigger timeouts!**")
                    builder.blank()

                elif max_pause >= warning_pause_ms or full_gc_count > 5:
                    issues_found = True
                    if broker_id not in warning_brokers:
                        warning_brokers.append(broker_id)

                    builder.warning_issue(
                        "High GC Pause Duration" if max_pause >= warning_pause_ms else "Excessive Full GCs",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "Max Pause": f"{max_pause:.1f}ms",
                            "Avg Pause": f"{avg_pause:.1f}ms",
                            "GC Count": str(gc_count),
                            "Full GCs": str(full_gc_count),
                            "Threshold": f"{warning_pause_ms}ms"
                        }
                    )

                # Check for high GC frequency
                if gc_count >= high_frequency_threshold and broker_id not in warning_brokers:
                    issues_found = True
                    warning_brokers.append(broker_id)
                    builder.warning_issue(
                        "High GC Frequency",
                        {
                            "Broker": f"{broker_id} ({ssh_host})",
                            "GC Count": f"{gc_count} in {num_lines} log lines",
                            "Full GCs": str(full_gc_count),
                            "Total Pause Time": f"{total_pause_time_ms:.1f}ms"
                        }
                    )

            except Exception as e:
                logger.error(f"Error analyzing GC log on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_gc_data:
            builder.h4("GC Pause Summary")

            table_lines = [
                "|===",
                "|Broker|Host|GC Count|Max Pause (ms)|Avg Pause (ms)|Full GCs|Status"
            ]

            for gc in sorted(all_gc_data, key=lambda x: x['max_pause_ms'], reverse=True):
                indicator = ""
                if gc['exceeds_critical']:
                    indicator = "🔴"
                elif gc['exceeds_warning'] or gc['has_full_gc']:
                    indicator = "⚠️"
                else:
                    indicator = "✅"

                table_lines.append(
                    f"|{gc['broker_id']}|{gc['host']}|{gc['gc_count']}|"
                    f"{gc['max_pause_ms']:.1f}|{gc['avg_pause_ms']:.1f}|{gc['full_gc_count']}|{indicator}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

            # === GC TYPE BREAKDOWN ===
            # Aggregate GC types across all brokers
            all_gc_types = Counter()
            for gc in all_gc_data:
                for gc_type, count in gc['gc_types'].items():
                    all_gc_types[gc_type] += count

            if all_gc_types:
                builder.h4("GC Type Distribution (Cluster-wide)")
                type_lines = ["|===", "|GC Type|Count"]
                for gc_type, count in all_gc_types.most_common():
                    type_lines.append(f"|{gc_type}|{count}")
                type_lines.append("|===")
                builder.add("\n".join(type_lines))
                builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("GC Log Access Errors")
            builder.warning(
                f"Could not analyze GC logs on {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_brokers:
                recommendations["critical"] = [
                    "**Review heap size settings:** GC pauses >1s indicate heap pressure",
                    "**Analyze heap dumps:** Use jmap/jhat to identify memory leaks",
                    "**Tune GC settings:** Consider G1GC with appropriate pause time goals",
                    "**Check for memory leaks:** Review producer/consumer client behaviors",
                    "**Monitor heap usage trends:** Use JMX or Prometheus for continuous monitoring",
                    "**Consider heap increase:** If consistently high usage, add more heap memory"
                ]

            if warning_brokers:
                recommendations["high"] = [
                    "**Optimize GC parameters:** Review -XX:MaxGCPauseMillis and other G1GC settings",
                    "**Reduce Full GCs:** Full GCs indicate heap exhaustion or fragmentation",
                    "**Review allocation rate:** High object creation rate causes frequent GCs",
                    "**Check page cache pressure:** OS page cache competes with JVM heap",
                    "**Monitor GC overhead:** GC should consume <5% of total time"
                ]

            recommendations["general"] = [
                "Enable GC logging with detailed timestamps for analysis",
                "Use G1GC for predictable pause times: -XX:+UseG1GC",
                "Set reasonable pause goals: -XX:MaxGCPauseMillis=200",
                "Monitor heap usage: -Xms and -Xmx should typically be equal",
                "Typical Kafka heap: 4-6GB (avoid >8GB unless necessary)",
                "Use GC log rotation to prevent disk filling",
                "Consider using GC analyzers (GCViewer, GCeasy) for detailed analysis"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"GC performance is healthy across all brokers.\n\n"
                f"All GC pauses are within acceptable thresholds."
            )

        # === STRUCTURED DATA ===
        structured_data["gc_pauses"] = {
            "status": "success",
            "brokers_checked": len(connector.get_ssh_hosts()),
            "brokers_with_errors": len(set(e['broker_id'] for e in errors)),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "warning_pause_ms": warning_pause_ms,
                "critical_pause_ms": critical_pause_ms,
                "high_frequency": high_frequency_threshold
            },
            "errors": errors,
            "data": all_gc_data
        }

    except Exception as e:
        import traceback
        logger.error(f"GC pause analysis failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["gc_pauses"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

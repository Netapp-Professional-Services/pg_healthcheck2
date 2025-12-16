"""
Kafka Log Flush Performance Check (Unified Adaptive)

Monitors log flush performance using adaptive collection strategy.
Log flush operations write data from page cache to disk - critical for durability.

Health Check: prometheus_log_flush
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- log_flush_rate - Flushes per second (writes from page cache to disk)
- log_flush_time - Average flush time in milliseconds

IMPORTANCE:
- High flush rate can indicate frequent small writes (inefficient)
- High flush time indicates slow disk I/O or disk saturation
- Flush performance directly impacts producer acknowledgment latency
- Excessive flushing reduces throughput and increases disk wear

NOTE: Kafka relies on OS page cache for performance.
Flushing too frequently defeats the purpose of page cache buffering.
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def check_prometheus_log_flush(connector, settings):
    """
    Check log flush performance via adaptive collection strategy.

    Monitors:
    - Log flush rate (flushes/sec)
    - Log flush time (milliseconds)

    Thresholds:
    - Flush time WARNING: > 50ms average
    - Flush time CRITICAL: > 100ms average
    - Flush rate WARNING: > 100/sec (may indicate config issue)

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Log Flush Performance (Prometheus)")

    try:
        # Get metric definitions
        flush_rate_def = get_metric_definition('log_flush_rate')
        flush_time_def = get_metric_definition('log_flush_time')

        # Collect metrics adaptively
        rate_result = collect_metric_adaptive(flush_rate_def, connector, settings) if flush_rate_def else None
        time_result = collect_metric_adaptive(flush_time_def, connector, settings) if flush_time_def else None

        if not rate_result and not time_result:
            builder.text("ℹ️  Log flush metrics not available")
            builder.blank()
            builder.text("*Note:* Log flush metrics may not be exposed by all Kafka versions.")
            findings = {
                'status': 'info',
                'message': 'Log flush metrics not available',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = (rate_result or time_result).get('method')
        rate_metrics = rate_result.get('node_metrics', {}) if rate_result else {}
        time_metrics = time_result.get('node_metrics', {}) if time_result else {}

        # Get thresholds
        flush_time_warning = settings.get('kafka_log_flush_time_warning_ms', 50)
        flush_time_critical = settings.get('kafka_log_flush_time_critical_ms', 100)
        flush_rate_warning = settings.get('kafka_log_flush_rate_warning', 100)

        # Combine broker data
        all_hosts = set(rate_metrics.keys()) | set(time_metrics.keys())
        node_data = []
        slow_flush_brokers = []
        critical_flush_brokers = []
        high_rate_brokers = []

        for host in all_hosts:
            flush_rate = rate_metrics.get(host, 0)
            flush_time = time_metrics.get(host, 0)

            broker_entry = {
                'node_id': host,
                'host': host,
                'flush_rate_per_sec': round(flush_rate, 2),
                'flush_time_ms': round(flush_time, 2)
            }
            node_data.append(broker_entry)

            # Check thresholds
            if flush_time >= flush_time_critical:
                critical_flush_brokers.append(broker_entry)
            elif flush_time >= flush_time_warning:
                slow_flush_brokers.append(broker_entry)

            if flush_rate >= flush_rate_warning:
                high_rate_brokers.append(broker_entry)

        if not node_data:
            builder.error("❌ No broker data available")
            findings = {
                'status': 'error',
                'error_message': 'No broker data available',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Determine overall status
        if critical_flush_brokers:
            status = 'critical'
            severity = 9
            message = f"🔴 {len(critical_flush_brokers)} broker(s) with critical flush time (>{flush_time_critical}ms)"
        elif slow_flush_brokers or high_rate_brokers:
            status = 'warning'
            severity = 6
            issues = []
            if slow_flush_brokers:
                issues.append(f"{len(slow_flush_brokers)} slow flush")
            if high_rate_brokers:
                issues.append(f"{len(high_rate_brokers)} high rate")
            message = f"⚠️  " + " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ Log flush performance healthy across {len(node_data)} brokers"

        # Calculate cluster aggregates
        avg_flush_rate = sum(b['flush_rate_per_sec'] for b in node_data) / len(node_data)
        avg_flush_time = sum(b['flush_time_ms'] for b in node_data) / len(node_data)

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_flush': node_data,
                'cluster_aggregate': {
                    'avg_flush_rate': round(avg_flush_rate, 2),
                    'avg_flush_time_ms': round(avg_flush_time, 2),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'flush_time_warning_ms': flush_time_warning,
                        'flush_time_critical_ms': flush_time_critical,
                        'flush_rate_warning': flush_rate_warning
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['log_flush_rate', 'log_flush_time'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Add issue details
        if critical_flush_brokers:
            findings['data']['critical_flush_time'] = {
                'count': len(critical_flush_brokers),
                'brokers': critical_flush_brokers,
                'recommendation': 'Critical disk I/O performance issues - investigate disk health and load'
            }

        if slow_flush_brokers:
            findings['data']['slow_flush_time'] = {
                'count': len(slow_flush_brokers),
                'brokers': slow_flush_brokers,
                'recommendation': 'Elevated flush times - monitor disk I/O and consider hardware upgrade'
            }

        if high_rate_brokers:
            findings['data']['high_flush_rate'] = {
                'count': len(high_rate_brokers),
                'brokers': high_rate_brokers,
                'recommendation': 'High flush rate may indicate suboptimal flush interval configuration'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
        elif status == 'warning':
            builder.warning(message)
        else:
            builder.success(message)

        builder.blank()
        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Flush Rate: {round(avg_flush_rate, 2)}/sec")
        builder.text(f"- Avg Flush Time: {round(avg_flush_time, 2)}ms")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker Flush Metrics:*")
        for broker in sorted(node_data, key=lambda x: x['flush_time_ms'], reverse=True):
            status_icon = "🔴" if broker['flush_time_ms'] >= flush_time_critical else "⚠️" if broker['flush_time_ms'] >= flush_time_warning else "✅"
            builder.text(
                f"{status_icon} Broker {broker['node_id']}: "
                f"Rate {broker['flush_rate_per_sec']}/s, "
                f"Time {broker['flush_time_ms']}ms"
            )
        builder.blank()

        # Show issues if any
        if critical_flush_brokers or slow_flush_brokers or high_rate_brokers:
            if critical_flush_brokers:
                builder.text(f"*🔴 Critical Flush Time ({len(critical_flush_brokers)}):*")
                for broker in critical_flush_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['flush_time_ms']}ms")
                builder.text(f"_Recommendation: {findings['data']['critical_flush_time']['recommendation']}_")
                builder.blank()

            if slow_flush_brokers:
                builder.text(f"*⚠️  Slow Flush Time ({len(slow_flush_brokers)}):*")
                for broker in slow_flush_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['flush_time_ms']}ms")
                builder.text(f"_Recommendation: {findings['data']['slow_flush_time']['recommendation']}_")
                builder.blank()

            if high_rate_brokers:
                builder.text(f"*⚠️  High Flush Rate ({len(high_rate_brokers)}):*")
                for broker in high_rate_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['flush_rate_per_sec']}/sec")
                builder.text(f"_Recommendation: {findings['data']['high_flush_rate']['recommendation']}_")
                builder.blank()

        # Add recommendations
        if status in ['critical', 'warning']:
            recommendations = {}

            if status == 'critical':
                recommendations["critical"] = [
                    "Critical Flush Performance - Immediate Actions:",
                    "  1. Check disk I/O metrics (iostat, iotop)",
                    "  2. Verify disk health (SMART status, RAID status)",
                    "  3. Check for disk space issues (df -h)",
                    "  4. Review other processes competing for disk I/O",
                    "  5. Consider moving to faster storage (SSD, NVMe)",
                    "",
                    "Disk I/O Troubleshooting:",
                    "  • Run: iostat -x 1 10 (check %util, await)",
                    "  • Check: iotop -o (find heavy I/O processes)",
                    "  • Verify: smartctl -a /dev/sdX (disk health)",
                    "  • Monitor: /proc/diskstats (disk statistics)"
                ]

            if status == 'warning':
                recommendations["high"] = [
                    "Monitor flush performance trends",
                    "Review flush interval configuration (log.flush.interval.messages, log.flush.interval.ms)",
                    "Check disk I/O capacity and utilization",
                    "Consider storage hardware upgrade if sustained"
                ]

            recommendations["general"] = [
                "Log Flush Configuration:",
                "  • log.flush.interval.messages: Messages before forced flush (default: none)",
                "  • log.flush.interval.ms: Time before forced flush (default: none)",
                "  • Default: Kafka relies on OS page cache, flushes only on fsync",
                "  • Recommendation: Let OS handle flushing (default settings)",
                "",
                "Flush Behavior:",
                "  • Producers with acks=all: Flush on every produce (durable, slower)",
                "  • Producers with acks=1: Flush only on leader (faster, less durable)",
                "  • Producers with acks=0: No flush waiting (fastest, least durable)",
                "  • log.flush.interval.* forces periodic flushes (reduces performance)",
                "",
                "Performance vs Durability Tradeoff:",
                "  • More flushing = More durable, Lower throughput",
                "  • Less flushing = Higher throughput, Risk of data loss",
                "  • Replication factor >= 3 provides durability without frequent flushing",
                "  • Page cache is very fast - let OS manage flushing",
                "",
                "Best Practices:",
                "  • Use replication (min.insync.replicas >= 2) instead of aggressive flushing",
                "  • Leave log.flush.interval.* unset (use defaults)",
                "  • Use fast local storage (SSD, NVMe)",
                "  • Separate data disks from OS disk",
                "  • Monitor disk I/O metrics continuously",
                "",
                "Storage Recommendations:",
                "  • Minimum: SATA SSD (500MB/s sequential write)",
                "  • Recommended: NVMe SSD (2-3GB/s sequential write)",
                "  • RAID 10 for redundancy if using spinning disks",
                "  • Dedicated disks for Kafka data (not shared with OS)",
                "  • XFS or ext4 filesystem (XFS preferred for Kafka)"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Log flush check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

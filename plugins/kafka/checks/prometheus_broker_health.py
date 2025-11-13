"""
Kafka Broker Health Check (Unified Adaptive)

Monitors broker resource health using adaptive collection strategy.
Tracks CPU, disk, and throughput metrics critical for broker stability.

Health Check: prometheus_broker_health
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- broker_cpu_utilization - CPU usage percentage
- broker_disk_utilization - Disk usage percentage
- broker_disk_available - Available disk space (bytes)
- broker_bytes_in - Incoming message bytes per second
- broker_bytes_out - Outgoing message bytes per second

CRITICAL IMPORTANCE:
- High CPU can cause broker unresponsiveness and cascading failures
- Disk full (100%) causes broker shutdown - catastrophic data loss
- Resource exhaustion impacts entire cluster stability
- Monitoring resource trends prevents outages
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 10  # Highest priority - resource health is critical


def check_prometheus_broker_health(connector, settings):
    """
    Check broker health metrics via adaptive collection strategy.

    Monitors:
    - CPU utilization
    - Disk utilization
    - Disk available space
    - Network throughput (bytes in/out)

    Thresholds:
    - CPU WARNING: > 75%
    - CPU CRITICAL: > 90%
    - Disk WARNING: > 80%
    - Disk CRITICAL: > 90%

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Broker Health Metrics (Prometheus)")

    try:
        # Get metric definitions
        cpu_def = get_metric_definition('broker_cpu_utilization')
        disk_util_def = get_metric_definition('broker_disk_utilization')
        disk_avail_def = get_metric_definition('broker_disk_available')
        bytes_in_def = get_metric_definition('broker_bytes_in')
        bytes_out_def = get_metric_definition('broker_bytes_out')

        # Collect metrics adaptively
        cpu_result = collect_metric_adaptive(cpu_def, connector, settings) if cpu_def else None
        disk_util_result = collect_metric_adaptive(disk_util_def, connector, settings) if disk_util_def else None
        disk_avail_result = collect_metric_adaptive(disk_avail_def, connector, settings) if disk_avail_def else None
        bytes_in_result = collect_metric_adaptive(bytes_in_def, connector, settings) if bytes_in_def else None
        bytes_out_result = collect_metric_adaptive(bytes_out_def, connector, settings) if bytes_out_def else None

        if not any([cpu_result, disk_util_result, disk_avail_result, bytes_in_result, bytes_out_result]):
            builder.warning(
                "⚠️ Could not collect broker health metrics\n\n"
                "*Tried collection methods:*\n"
                "1. Instaclustr Prometheus API - Not configured or unavailable\n"
                "2. Local Prometheus JMX exporter - Not found or SSH unavailable\n"
                "3. Standard JMX - Not available or SSH unavailable"
            )
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect broker health metrics using any method',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = (cpu_result or disk_util_result or disk_avail_result or bytes_in_result or bytes_out_result).get('method')
        cpu_metrics = cpu_result.get('node_metrics', {}) if cpu_result else {}
        disk_util_metrics = disk_util_result.get('node_metrics', {}) if disk_util_result else {}
        disk_avail_metrics = disk_avail_result.get('node_metrics', {}) if disk_avail_result else {}
        bytes_in_metrics = bytes_in_result.get('node_metrics', {}) if bytes_in_result else {}
        bytes_out_metrics = bytes_out_result.get('node_metrics', {}) if bytes_out_result else {}

        # Get thresholds
        cpu_warning = settings.get('kafka_broker_cpu_warning', 75)
        cpu_critical = settings.get('kafka_broker_cpu_critical', 90)
        disk_warning = settings.get('kafka_broker_disk_warning', 80)
        disk_critical = settings.get('kafka_broker_disk_critical', 90)

        # Combine broker data
        all_hosts = set(cpu_metrics.keys()) | set(disk_util_metrics.keys()) | set(disk_avail_metrics.keys()) | set(bytes_in_metrics.keys()) | set(bytes_out_metrics.keys())
        node_data = []
        critical_cpu_brokers = []
        high_cpu_brokers = []
        critical_disk_brokers = []
        high_disk_brokers = []

        for host in all_hosts:
            cpu = cpu_metrics.get(host, 0)
            disk_util = disk_util_metrics.get(host, 0)
            disk_avail = disk_avail_metrics.get(host, 0)
            bytes_in = bytes_in_metrics.get(host, 0)
            bytes_out = bytes_out_metrics.get(host, 0)

            broker_entry = {
                'node_id': host,
                'host': host,
                'cpu_utilization': round(cpu, 2),
                'disk_utilization': round(disk_util, 2),
                'disk_available_gb': round(disk_avail / (1024**3), 2) if disk_avail else 0,
                'bytes_in_per_sec': round(bytes_in, 2),
                'bytes_out_per_sec': round(bytes_out, 2),
                'throughput_mb_per_sec': round((bytes_in + bytes_out) / (1024**2), 2)
            }
            node_data.append(broker_entry)

            # Check CPU thresholds
            if cpu >= cpu_critical:
                critical_cpu_brokers.append(broker_entry)
            elif cpu >= cpu_warning:
                high_cpu_brokers.append(broker_entry)

            # Check disk thresholds
            if disk_util >= disk_critical:
                critical_disk_brokers.append(broker_entry)
            elif disk_util >= disk_warning:
                high_disk_brokers.append(broker_entry)

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
        if critical_cpu_brokers or critical_disk_brokers:
            status = 'critical'
            severity = 10
            issues = []
            if critical_cpu_brokers:
                issues.append(f"{len(critical_cpu_brokers)} broker(s) with critical CPU (>{cpu_critical}%)")
            if critical_disk_brokers:
                issues.append(f"{len(critical_disk_brokers)} broker(s) with critical disk (>{disk_critical}%)")
            message = "🔴 " + " and ".join(issues)
        elif high_cpu_brokers or high_disk_brokers:
            status = 'warning'
            severity = 7
            issues = []
            if high_cpu_brokers:
                issues.append(f"{len(high_cpu_brokers)} broker(s) with high CPU (>{cpu_warning}%)")
            if high_disk_brokers:
                issues.append(f"{len(high_disk_brokers)} broker(s) with high disk (>{disk_warning}%)")
            message = "⚠️  " + " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ All {len(node_data)} brokers have healthy CPU and disk usage"

        # Calculate cluster aggregates
        avg_cpu = sum(b['cpu_utilization'] for b in node_data) / len(node_data)
        avg_disk = sum(b['disk_utilization'] for b in node_data) / len(node_data)
        total_bytes_in = sum(b['bytes_in_per_sec'] for b in node_data)
        total_bytes_out = sum(b['bytes_out_per_sec'] for b in node_data)
        total_throughput = total_bytes_in + total_bytes_out

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_metrics': node_data,
                'cluster_aggregate': {
                    'avg_cpu_utilization': round(avg_cpu, 2),
                    'avg_disk_utilization': round(avg_disk, 2),
                    'total_bytes_in_per_sec': round(total_bytes_in, 2),
                    'total_bytes_out_per_sec': round(total_bytes_out, 2),
                    'total_throughput_mbps': round(total_throughput / (1024**2), 2),
                    'brokers_critical_cpu': len(critical_cpu_brokers),
                    'brokers_high_cpu': len(high_cpu_brokers),
                    'brokers_critical_disk': len(critical_disk_brokers),
                    'brokers_high_disk': len(high_disk_brokers),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'cpu_warning': cpu_warning,
                        'cpu_critical': cpu_critical,
                        'disk_warning': disk_warning,
                        'disk_critical': disk_critical
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['broker_cpu_utilization', 'broker_disk_utilization', 'broker_disk_available', 'broker_bytes_in', 'broker_bytes_out'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Add issue details
        if critical_cpu_brokers:
            findings['data']['critical_cpu_brokers'] = {
                'count': len(critical_cpu_brokers),
                'brokers': critical_cpu_brokers,
                'recommendation': 'CRITICAL: CPU exhaustion - broker may become unresponsive, investigate and scale immediately'
            }

        if high_cpu_brokers:
            findings['data']['high_cpu_brokers'] = {
                'count': len(high_cpu_brokers),
                'brokers': high_cpu_brokers,
                'recommendation': 'High CPU usage - monitor workload, review partition distribution, plan capacity increase'
            }

        if critical_disk_brokers:
            findings['data']['critical_disk_brokers'] = {
                'count': len(critical_disk_brokers),
                'brokers': critical_disk_brokers,
                'recommendation': 'CRITICAL: Disk near full - broker shutdown imminent, increase disk space NOW'
            }

        if high_disk_brokers:
            findings['data']['high_disk_brokers'] = {
                'count': len(high_disk_brokers),
                'brokers': high_disk_brokers,
                'recommendation': 'High disk usage - plan disk expansion, review log retention policies'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            if critical_disk_brokers:
                builder.text("*🚨 CRITICAL: Disk Space Exhaustion 🚨*")
                builder.text("Brokers at >90% disk will shut down when full.")
                builder.text("This causes IMMEDIATE data loss and cluster instability.")
                builder.blank()
            if critical_cpu_brokers:
                builder.text("*🚨 CRITICAL: CPU Exhaustion 🚨*")
                builder.text("High CPU causes broker unresponsiveness.")
                builder.text("Can trigger cascading failures across cluster.")
                builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*")
        builder.blank()
        builder.text(f"- Avg CPU Usage: {round(avg_cpu, 2)}%")
        builder.text(f"- Avg Disk Usage: {round(avg_disk, 2)}%")
        builder.text(f"- Total Throughput In: {round(total_bytes_in / (1024**2), 2)} MB/s")
        builder.text(f"- Total Throughput Out: {round(total_bytes_out / (1024**2), 2)} MB/s")
        builder.text(f"- Total Cluster Throughput: {round(total_throughput / (1024**2), 2)} MB/s")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker Resource Usage:*")
        builder.blank()
        for broker in sorted(node_data, key=lambda x: (x['cpu_utilization'] + x['disk_utilization']), reverse=True):
            cpu_icon = "🔴" if broker['cpu_utilization'] >= cpu_critical else "⚠️" if broker['cpu_utilization'] >= cpu_warning else "✅"
            disk_icon = "🔴" if broker['disk_utilization'] >= disk_critical else "⚠️" if broker['disk_utilization'] >= disk_warning else "✅"
            builder.text(
                f"Broker {broker['node_id']}:"
                f"{cpu_icon} CPU {broker['cpu_utilization']}%, "
                f"{disk_icon} Disk {broker['disk_utilization']}% ({broker['disk_available_gb']} GB free), "
                f"Throughput {broker['throughput_mb_per_sec']} MB/s\n\n"
            )
        builder.blank()

        # Show critical issues
        if critical_cpu_brokers or critical_disk_brokers or high_cpu_brokers or high_disk_brokers:
            if critical_cpu_brokers:
                builder.text(f"*🔴 Critical CPU Brokers ({len(critical_cpu_brokers)}):*")
                for broker in critical_cpu_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['cpu_utilization']}% CPU")
                builder.text(f"_Recommendation: {findings['data']['critical_cpu_brokers']['recommendation']}_")
                builder.blank()

            if critical_disk_brokers:
                builder.text(f"*🔴 Critical Disk Brokers ({len(critical_disk_brokers)}):*")
                for broker in critical_disk_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['disk_utilization']}% disk ({broker['disk_available_gb']} GB free)")
                builder.text(f"_Recommendation: {findings['data']['critical_disk_brokers']['recommendation']}_")
                builder.blank()

            if high_cpu_brokers:
                builder.text(f"*⚠️  High CPU Brokers ({len(high_cpu_brokers)}):*")
                for broker in high_cpu_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['cpu_utilization']}% CPU")
                builder.text(f"_Recommendation: {findings['data']['high_cpu_brokers']['recommendation']}_")
                builder.blank()

            if high_disk_brokers:
                builder.text(f"*⚠️  High Disk Brokers ({len(high_disk_brokers)}):*")
                for broker in high_disk_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['disk_utilization']}% disk ({broker['disk_available_gb']} GB free)")
                builder.text(f"_Recommendation: {findings['data']['high_disk_brokers']['recommendation']}_")
                builder.blank()

        # Add recommendations
        if status in ['critical', 'warning']:
            recommendations = {}

            if status == 'critical':
                if critical_disk_brokers:
                    recommendations["critical"] = [
                        "Disk Space Emergency - IMMEDIATE ACTIONS:",
                        "  1. CHECK DISK SPACE NOW: df -h /var/lib/kafka",
                        "  2. Reduce retention immediately: log.retention.hours=24",
                        "  3. Delete old log segments: kafka-delete-records.sh",
                        "  4. Add disk space (resize volume or add new disk)",
                        "  5. Monitor broker logs for disk space errors",
                        "",
                        "CRITICAL WARNING:",
                        "  • Broker SHUTS DOWN at 100% disk (data loss!)",
                        "  • Cannot write new messages when disk full",
                        "  • Log compaction fails without disk space",
                        "  • Replication breaks when disk full",
                        "",
                        "Quick Fixes:",
                        "  • Delete old topics: kafka-topics.sh --delete",
                        "  • Reduce retention: Edit log.retention.* settings",
                        "  • Clean log segments: kafka-log-cleaner.sh",
                        "  • Add disk volume immediately"
                    ]
                elif critical_cpu_brokers:
                    recommendations["critical"] = [
                        "CPU Exhaustion - Immediate Actions:",
                        "  1. Check CPU usage: top -H -p $(pgrep -f kafka)",
                        "  2. Identify hot threads: kafka-threads.sh",
                        "  3. Review partition count and leader distribution",
                        "  4. Check for consumer rebalances (expensive)",
                        "  5. Scale up instance type or add brokers",
                        "",
                        "Common Causes:",
                        "  • Too many partitions per broker (reduce to < 4000)",
                        "  • Excessive producer requests",
                        "  • Consumer rebalancing storms",
                        "  • Heavy compaction workload",
                        "  • Undersized broker instance"
                    ]

            if status == 'warning':
                recommendations["high"] = [
                    "Monitor resource trends - is usage growing?",
                    "Review broker capacity planning",
                    "Check partition balance (prometheus_partition_balance)",
                    "Review log retention policies",
                    "Plan capacity expansion if sustained high usage"
                ]

            recommendations["general"] = [
                "Resource Monitoring Best Practices:",
                "  • Alert at CPU > 75% (warning), > 90% (critical)",
                "  • Alert at Disk > 80% (warning), > 90% (critical)",
                "  • Monitor resource trends over time",
                "  • Plan capacity for 2x expected peak load",
                "",
                "CPU Optimization:",
                "  • Reduce partition count per broker (< 4000 recommended)",
                "  • Balance partition leaders across brokers",
                "  • Tune num.io.threads and num.network.threads",
                "  • Use compression to reduce CPU (lz4 recommended)",
                "  • Review producer/consumer batch sizes",
                "",
                "Disk Space Management:",
                "  • Set appropriate log retention: log.retention.hours",
                "  • Use log compaction for event sourcing topics",
                "  • Monitor disk growth rate (GB/day)",
                "  • Plan for 30% headroom (never exceed 80% long-term)",
                "  • Separate data disks from OS disk",
                "",
                "Capacity Planning:",
                "  • Monitor daily growth rate",
                "  • Plan for seasonal traffic spikes",
                "  • Size for 2-3x average load",
                "  • Keep 20-30% headroom for failover",
                "  • Review capacity quarterly",
                "",
                "Disk Space Recovery:",
                "  • Reduce log.retention.hours (default: 168 hours/7 days)",
                "  • Delete unused topics: kafka-topics.sh --delete",
                "  • Run log compaction: log.cleaner.enable=true",
                "  • Clean old log segments manually if needed",
                "  • Add disk capacity (resize or add volume)"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Broker health check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings


def get_weight():
    """Module priority weight (1-10). Highest priority - resource health is critical."""
    return 10

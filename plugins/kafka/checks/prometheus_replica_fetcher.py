"""
Kafka Replica Fetcher Health Check (Unified Adaptive)

Monitors replica fetcher threads using adaptive collection strategy.
Replica fetchers copy data from leader partitions to follower replicas.

Health Check: prometheus_replica_fetcher
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- replica_fetcher_failed_partitions - Partitions that failed to replicate
- replica_fetcher_max_lag - Maximum replication lag (records behind leader)
- replica_fetcher_min_fetch_rate - Minimum fetch rate (bytes/sec)

CRITICAL IMPORTANCE:
- Failed partitions indicate replication breakage (data loss risk)
- High lag means replicas are falling behind (under-replicated)
- Low fetch rate indicates network or broker performance issues
- Replica fetcher health directly impacts fault tolerance
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def check_prometheus_replica_fetcher(connector, settings):
    """
    Check replica fetcher health via adaptive collection strategy.

    Monitors:
    - Failed partition count
    - Maximum replication lag
    - Minimum fetch rate

    Thresholds:
    - WARNING: > 0 failed partitions
    - CRITICAL: > 10 failed partitions
    - WARNING: max_lag > 1000 records
    - WARNING: min_fetch_rate < 100 KB/s

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Replica Fetcher Health (Prometheus)")

    try:
        # Get metric definitions
        failed_parts_def = get_metric_definition('replica_fetcher_failed_partitions')
        max_lag_def = get_metric_definition('replica_fetcher_max_lag')
        min_fetch_rate_def = get_metric_definition('replica_fetcher_min_fetch_rate')

        # Collect metrics adaptively
        failed_result = collect_metric_adaptive(failed_parts_def, connector, settings) if failed_parts_def else None
        lag_result = collect_metric_adaptive(max_lag_def, connector, settings) if max_lag_def else None
        rate_result = collect_metric_adaptive(min_fetch_rate_def, connector, settings) if min_fetch_rate_def else None

        if not any([failed_result, lag_result, rate_result]):
            builder.text("ℹ️  Replica fetcher metrics not available")
            builder.blank()
            builder.text("*Note:* Replica fetcher metrics may not be exposed in all configurations.")
            findings = {
                'status': 'info',
                'message': 'Replica fetcher metrics not available',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = (failed_result or lag_result or rate_result).get('method')
        failed_metrics = failed_result.get('node_metrics', {}) if failed_result else {}
        lag_metrics = lag_result.get('node_metrics', {}) if lag_result else {}
        rate_metrics = rate_result.get('node_metrics', {}) if rate_result else {}

        # Get thresholds
        failed_warning = settings.get('kafka_replica_fetcher_failed_warning', 1)
        failed_critical = settings.get('kafka_replica_fetcher_failed_critical', 10)
        lag_warning = settings.get('kafka_replica_fetcher_lag_warning', 1000)
        rate_warning = settings.get('kafka_replica_fetcher_rate_warning_kbps', 100)

        # Combine broker data
        all_hosts = set(failed_metrics.keys()) | set(lag_metrics.keys()) | set(rate_metrics.keys())
        node_data = []
        critical_brokers = []
        warning_brokers = []

        for host in all_hosts:
            failed_parts = int(failed_metrics.get(host, 0))
            max_lag = lag_metrics.get(host, 0)
            min_fetch_rate = rate_metrics.get(host, 0)

            broker_entry = {
                'node_id': host,
                'host': host,
                'failed_partitions': failed_parts,
                'max_lag_records': round(max_lag, 2),
                'min_fetch_rate_bps': round(min_fetch_rate, 2),
                'min_fetch_rate_kbps': round(min_fetch_rate / 1024, 2) if min_fetch_rate else 0
            }
            node_data.append(broker_entry)

            # Check thresholds
            if failed_parts >= failed_critical:
                critical_brokers.append(broker_entry)
            elif (failed_parts >= failed_warning or
                  max_lag >= lag_warning or
                  # Only warn about slow fetch rate if there's ALSO lag (idle cluster = normal)
                  # Low fetch rate WITHOUT lag just means no replication activity (healthy idle state)
                  (max_lag > 0 and (min_fetch_rate / 1024) < rate_warning)):
                warning_brokers.append(broker_entry)

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
        if critical_brokers:
            status = 'critical'
            severity = 9
            message = f"🔴 {len(critical_brokers)} broker(s) with critical replica fetcher failures (>{failed_critical} failed partitions)"
        elif warning_brokers:
            status = 'warning'
            severity = 7
            message = f"⚠️  {len(warning_brokers)} broker(s) with replica fetcher issues"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ Replica fetchers healthy across {len(node_data)} brokers"

        # Calculate cluster aggregates
        total_failed = sum(b['failed_partitions'] for b in node_data)
        avg_max_lag = sum(b['max_lag_records'] for b in node_data) / len(node_data) if node_data else 0
        avg_min_rate = sum(b['min_fetch_rate_kbps'] for b in node_data) / len(node_data) if node_data else 0

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_fetcher': node_data,
                'cluster_aggregate': {
                    'total_failed_partitions': total_failed,
                    'avg_max_lag_records': round(avg_max_lag, 2),
                    'avg_min_fetch_rate_kbps': round(avg_min_rate, 2),
                    'brokers_critical': len(critical_brokers),
                    'brokers_warning': len(warning_brokers),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'failed_warning': failed_warning,
                        'failed_critical': failed_critical,
                        'lag_warning_records': lag_warning,
                        'rate_warning_kbps': rate_warning
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['replica_fetcher_failed_partitions', 'replica_fetcher_max_lag', 'replica_fetcher_min_fetch_rate'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Add issue details
        if critical_brokers:
            findings['data']['critical_replica_fetcher'] = {
                'count': len(critical_brokers),
                'brokers': critical_brokers,
                'recommendation': 'CRITICAL: Replica fetcher failures - partitions cannot replicate, data loss risk'
            }

        if warning_brokers:
            findings['data']['warning_replica_fetcher'] = {
                'count': len(warning_brokers),
                'brokers': warning_brokers,
                'recommendation': 'Replica fetcher issues detected - monitor replication health'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*🚨 CRITICAL: Replication Breakage Detected 🚨*")
            builder.text("Failed partitions cannot replicate data to followers.")
            builder.text("This reduces fault tolerance and increases data loss risk.")
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*")
        builder.text(f"- Total Failed Partitions: {total_failed}")
        builder.text(f"- Avg Max Lag: {round(avg_max_lag, 2)} records")
        builder.text(f"- Avg Min Fetch Rate: {round(avg_min_rate, 2)} KB/s")
        builder.text(f"- Brokers with Issues: {len(critical_brokers)} critical, {len(warning_brokers)} warning")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker Replica Fetcher Status:*")
        for broker in sorted(node_data, key=lambda x: x['failed_partitions'], reverse=True):
            status_icon = "🔴" if broker['failed_partitions'] >= failed_critical else "⚠️" if broker['failed_partitions'] >= failed_warning else "✅"
            builder.text(
                f"{status_icon} Broker {broker['node_id']}: "
                f"Failed {broker['failed_partitions']}, "
                f"Lag {broker['max_lag_records']} records, "
                f"Rate {broker['min_fetch_rate_kbps']} KB/s"
            )
        builder.blank()

        # Show critical issues if any
        if critical_brokers or warning_brokers:
            if critical_brokers:
                builder.text(f"*🔴 Critical Replica Fetcher Failures ({len(critical_brokers)}):*")
                for broker in critical_brokers:
                    builder.text(f"- Broker {broker['node_id']}: {broker['failed_partitions']} failed partitions")
                builder.text(f"_Recommendation: {findings['data']['critical_replica_fetcher']['recommendation']}_")
                builder.blank()

            if warning_brokers:
                builder.text(f"*⚠️  Replica Fetcher Issues ({len(warning_brokers)}):*")
                for broker in warning_brokers:
                    issues = []
                    if broker['failed_partitions'] > 0:
                        issues.append(f"{broker['failed_partitions']} failed")
                    if broker['max_lag_records'] >= lag_warning:
                        issues.append(f"lag {broker['max_lag_records']}")
                    # Only show slow rate warning if rate > 0 (0 KB/s on idle cluster is normal)
                    if broker['min_fetch_rate_kbps'] > 0 and broker['min_fetch_rate_kbps'] < rate_warning:
                        issues.append(f"slow rate {broker['min_fetch_rate_kbps']} KB/s")

                    # Only display this broker if it actually has issues to report
                    if issues:
                        builder.text(f"- Broker {broker['node_id']}: {', '.join(issues)}")

                # Only show recommendation if we displayed any brokers
                if any(
                    broker['failed_partitions'] > 0 or
                    broker['max_lag_records'] >= lag_warning or
                    (broker['min_fetch_rate_kbps'] > 0 and broker['min_fetch_rate_kbps'] < rate_warning)
                    for broker in warning_brokers
                ):
                    builder.text(f"_Recommendation: {findings['data']['warning_replica_fetcher']['recommendation']}_")
                builder.blank()

        # Add recommendations
        if status in ['critical', 'warning']:
            recommendations = {}

            if status == 'critical':
                recommendations["critical"] = [
                    "Replica Fetcher Failures - Immediate Actions:",
                    "  1. Check broker logs for replication errors",
                    "  2. Verify network connectivity between brokers",
                    "  3. Check replica.lag.time.max.ms configuration",
                    "  4. Review under-replicated partitions (prometheus_under_replicated_partitions)",
                    "  5. Verify broker health and resource availability",
                    "",
                    "Common Causes:",
                    "  • Network issues between brokers",
                    "  • Broker overload (CPU, disk I/O, memory)",
                    "  • Disk failures preventing replication",
                    "  • Firewall or security group blocking inter-broker traffic",
                    "  • Replication throttling set too low",
                    "",
                    "Data Loss Risk:",
                    "  • Failed partitions have NO replica copies",
                    "  • If leader broker fails, data can be lost",
                    "  • min.insync.replicas may prevent writes (good - prevents data loss)",
                    "  • Fix replication ASAP to restore fault tolerance"
                ]

            if status == 'warning':
                recommendations["high"] = [
                    "Monitor replica fetcher metrics continuously",
                    "Check network bandwidth between brokers",
                    "Review broker resource utilization (CPU, disk, network)",
                    "Investigate any failed partition replication",
                    "Consider increasing num.replica.fetchers if fetch rate is low"
                ]

            recommendations["general"] = [
                "Replica Fetcher Configuration:",
                "  • num.replica.fetchers: Fetcher threads per broker (default: 1)",
                "  • replica.fetch.max.bytes: Max bytes per fetch request (default: 1 MB)",
                "  • replica.fetch.wait.max.ms: Max wait time for fetch (default: 500ms)",
                "  • replica.lag.time.max.ms: Max time before follower out of ISR (default: 30s)",
                "",
                "How Replica Fetchers Work:",
                "  • Follower brokers fetch data from leader brokers",
                "  • Each fetcher thread handles multiple partitions",
                "  • Fetchers maintain replica synchronization",
                "  • If follower falls behind > replica.lag.time.max.ms, removed from ISR",
                "",
                "Performance Tuning:",
                "  • Increase num.replica.fetchers for higher throughput (2-4 recommended)",
                "  • Increase replica.fetch.max.bytes for larger messages",
                "  • Decrease replica.fetch.wait.max.ms for lower latency",
                "  • Ensure sufficient network bandwidth between brokers",
                "",
                "Monitoring Best Practices:",
                "  • Failed partitions should ALWAYS be 0",
                "  • Max lag should be < 1000 records in healthy clusters",
                "  • Min fetch rate varies by workload (monitor trends)",
                "  • Alert on any failed partitions immediately",
                "",
                "Troubleshooting Failed Partitions:",
                "  1. Check broker logs: grep 'replica fetcher' /var/log/kafka/server.log",
                "  2. Verify partition exists on leader",
                "  3. Check network connectivity: nc -zv leader_host 9092",
                "  4. Review broker resource usage (CPU, disk, network)",
                "  5. Check for disk errors or full disks"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Replica fetcher check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

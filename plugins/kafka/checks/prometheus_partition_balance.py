"""
Kafka Partition Balance Check (Unified Adaptive)

Monitors partition distribution across brokers using adaptive collection strategy.
Unbalanced partitions can cause performance issues and broker overload.

Health Check: prometheus_partition_balance
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- partition_count - Total partitions per broker (leaders + followers)
- leader_count - Leader partitions per broker

IMPORTANCE:
- Unbalanced leaders cause uneven load (leaders handle all reads/writes)
- Unbalanced partitions indicate poor distribution
- Too many partitions on one broker degrades performance
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, get_metric_collection_error_message
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def check_prometheus_partition_balance(connector, settings):
    """
    Check partition balance across brokers via adaptive collection strategy.

    Monitors:
    - Total partition count per broker
    - Leader partition count per broker
    - Distribution balance

    Thresholds:
    - Imbalance WARNING: > 20% deviation from average
    - Imbalance CRITICAL: > 40% deviation from average
    - Per-broker WARNING: > 1500 partitions
    - Per-broker CRITICAL: > 2000 partitions

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Partition Balance (Prometheus)")

    try:
        # Get metric definitions
        partition_count_def = get_metric_definition('partition_count')
        leader_count_def = get_metric_definition('leader_count')

        if not partition_count_def or not leader_count_def:
            builder.error("❌ Partition balance metric definitions not found")
            findings = {
                'status': 'error',
                'error_message': 'Metric definitions not found',
                'data': [],
                'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Collect both metrics adaptively
        partition_result = collect_metric_adaptive(partition_count_def, connector, settings)
        leader_result = collect_metric_adaptive(leader_count_def, connector, settings)

        if not partition_result and not leader_result:
            error_msg = get_metric_collection_error_message(connector, "partition balance metrics")
            builder.warning(error_msg)
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect partition balance metrics using any method',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = partition_result.get('method') if partition_result else leader_result.get('method')
        partition_metrics = partition_result.get('node_metrics', {}) if partition_result else {}
        leader_metrics = leader_result.get('node_metrics', {}) if leader_result else {}

        # Get thresholds
        imbalance_warning_pct = settings.get('kafka_partition_imbalance_warning_pct', 20)
        imbalance_critical_pct = settings.get('kafka_partition_imbalance_critical_pct', 40)
        per_broker_warning = settings.get('kafka_partition_per_broker_warning', 1500)
        per_broker_critical = settings.get('kafka_partition_per_broker_critical', 2000)

        # Combine broker data
        all_hosts = set(partition_metrics.keys()) | set(leader_metrics.keys())
        node_data = []
        overloaded_brokers = []
        critical_overload = []
        imbalanced_partitions = []
        critical_imbalance = []
        imbalanced_leaders = []
        critical_leader_imbalance = []

        for host in all_hosts:
            total_parts = int(partition_metrics.get(host, 0))
            leader_parts = int(leader_metrics.get(host, 0))
            follower_parts = total_parts - leader_parts

            broker_entry = {
                'node_id': host,
                'host': host,
                'total_partitions': total_parts,
                'leader_partitions': leader_parts,
                'follower_partitions': follower_parts
            }
            node_data.append(broker_entry)

        if not node_data:
            builder.error("❌ No broker data available")
            findings = {
                'status': 'error',
                'error_message': 'No broker data available',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Calculate statistics
        total_partitions_sum = sum(b['total_partitions'] for b in node_data)
        total_leaders_sum = sum(b['leader_partitions'] for b in node_data)
        avg_partitions = total_partitions_sum / len(node_data)
        avg_leaders = total_leaders_sum / len(node_data) if total_leaders_sum > 0 else 0

        # Identify issues
        for broker in node_data:
            total_parts = broker['total_partitions']
            leader_parts = broker['leader_partitions']

            # Check per-broker partition count
            if total_parts >= per_broker_critical:
                critical_overload.append({
                    'node_id': broker['node_id'],
                    'host': broker['host'],
                    'total_partitions': total_parts
                })
            elif total_parts >= per_broker_warning:
                overloaded_brokers.append({
                    'node_id': broker['node_id'],
                    'host': broker['host'],
                    'total_partitions': total_parts
                })

            # Check partition balance
            if avg_partitions > 0:
                deviation_pct = abs(total_parts - avg_partitions) / avg_partitions * 100
                broker['partition_deviation_pct'] = round(deviation_pct, 1)

                if deviation_pct >= imbalance_critical_pct:
                    critical_imbalance.append({
                        'node_id': broker['node_id'],
                        'host': broker['host'],
                        'total_partitions': total_parts,
                        'deviation_pct': round(deviation_pct, 1)
                    })
                elif deviation_pct >= imbalance_warning_pct:
                    imbalanced_partitions.append({
                        'node_id': broker['node_id'],
                        'host': broker['host'],
                        'total_partitions': total_parts,
                        'deviation_pct': round(deviation_pct, 1)
                    })

            # Check leader balance
            if avg_leaders > 0:
                leader_deviation_pct = abs(leader_parts - avg_leaders) / avg_leaders * 100
                broker['leader_deviation_pct'] = round(leader_deviation_pct, 1)

                if leader_deviation_pct >= imbalance_critical_pct:
                    critical_leader_imbalance.append({
                        'node_id': broker['node_id'],
                        'host': broker['host'],
                        'leader_partitions': leader_parts,
                        'deviation_pct': round(leader_deviation_pct, 1)
                    })
                elif leader_deviation_pct >= imbalance_warning_pct:
                    imbalanced_leaders.append({
                        'node_id': broker['node_id'],
                        'host': broker['host'],
                        'leader_partitions': leader_parts,
                        'deviation_pct': round(leader_deviation_pct, 1)
                    })

        # Determine overall status
        if critical_overload or critical_imbalance or critical_leader_imbalance:
            status = 'critical'
            severity = 9
            issues = []
            if critical_overload:
                issues.append(f"{len(critical_overload)} broker(s) critically overloaded")
            if critical_imbalance:
                issues.append(f"{len(critical_imbalance)} broker(s) critically imbalanced (partitions)")
            if critical_leader_imbalance:
                issues.append(f"{len(critical_leader_imbalance)} broker(s) critically imbalanced (leaders)")
            message = "🔴 " + " and ".join(issues)
        elif overloaded_brokers or imbalanced_partitions or imbalanced_leaders:
            status = 'warning'
            severity = 6
            issues = []
            if overloaded_brokers:
                issues.append(f"{len(overloaded_brokers)} broker(s) with high partition count")
            if imbalanced_partitions:
                issues.append(f"{len(imbalanced_partitions)} broker(s) with partition imbalance")
            if imbalanced_leaders:
                issues.append(f"{len(imbalanced_leaders)} broker(s) with leader imbalance")
            message = "⚠️  " + " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ Partitions well-balanced across {len(node_data)} brokers"

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_partitions': node_data,
                'cluster_aggregate': {
                    'total_partitions': total_partitions_sum,
                    'total_leaders': total_leaders_sum,
                    'avg_partitions_per_broker': round(avg_partitions, 1),
                    'avg_leaders_per_broker': round(avg_leaders, 1),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'imbalance_warning_pct': imbalance_warning_pct,
                        'imbalance_critical_pct': imbalance_critical_pct,
                        'per_broker_warning': per_broker_warning,
                        'per_broker_critical': per_broker_critical
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['partition_count', 'leader_count'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Add issue details
        if critical_overload:
            findings['data']['critical_overloaded_brokers'] = {
                'count': len(critical_overload),
                'brokers': critical_overload,
                'recommendation': 'Broker has too many partitions - performance degradation likely'
            }

        if overloaded_brokers:
            findings['data']['overloaded_brokers'] = {
                'count': len(overloaded_brokers),
                'brokers': overloaded_brokers,
                'recommendation': 'Monitor broker performance - partition count approaching limits'
            }

        if critical_imbalance:
            findings['data']['critical_partition_imbalance'] = {
                'count': len(critical_imbalance),
                'brokers': critical_imbalance,
                'recommendation': 'Severe partition imbalance - rebalance cluster immediately'
            }

        if imbalanced_partitions:
            findings['data']['partition_imbalance'] = {
                'count': len(imbalanced_partitions),
                'brokers': imbalanced_partitions,
                'recommendation': 'Partition distribution suboptimal - consider rebalancing'
            }

        if critical_leader_imbalance:
            findings['data']['critical_leader_imbalance'] = {
                'count': len(critical_leader_imbalance),
                'brokers': critical_leader_imbalance,
                'recommendation': 'Severe leader imbalance - some brokers handling disproportionate load'
            }

        if imbalanced_leaders:
            findings['data']['leader_imbalance'] = {
                'count': len(imbalanced_leaders),
                'brokers': imbalanced_leaders,
                'recommendation': 'Leader distribution suboptimal - run preferred leader election'
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
        builder.blank()
        builder.text(f"- Total Partitions: {total_partitions_sum:,}")
        builder.text(f"- Total Leaders: {total_leaders_sum:,}")
        builder.text(f"- Avg Partitions/Broker: {round(avg_partitions, 1)}")
        builder.text(f"- Avg Leaders/Broker: {round(avg_leaders, 1)}")
        builder.text(f"- Brokers: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        # Show per-broker breakdown
        builder.text("*Per-Broker Distribution:*")
        builder.blank()
        for broker in sorted(node_data, key=lambda x: x['total_partitions'], reverse=True):
            deviation_str = f" ({broker.get('partition_deviation_pct', 0):+.1f}%)" if 'partition_deviation_pct' in broker else ""
            builder.text(
                f"- Broker {broker['node_id']}:"
                f"{broker['total_partitions']} total "
                f"({broker['leader_partitions']} leaders, "
                f"{broker['follower_partitions']} followers){deviation_str}"
            )
        builder.blank()

        # Show issues
        issue_map = [
            (critical_overload, "🔴 Critically Overloaded", "critical_overloaded_brokers"),
            (overloaded_brokers, "⚠️  Overloaded", "overloaded_brokers"),
            (critical_imbalance, "🔴 Critical Partition Imbalance", "critical_partition_imbalance"),
            (imbalanced_partitions, "⚠️  Partition Imbalance", "partition_imbalance"),
            (critical_leader_imbalance, "🔴 Critical Leader Imbalance", "critical_leader_imbalance"),
            (imbalanced_leaders, "⚠️  Leader Imbalance", "leader_imbalance")
        ]

        for issue_list, issue_label, findings_key in issue_map:
            if issue_list:
                builder.text(f"*{issue_label} ({len(issue_list)}):*")
                builder.blank()
                for broker in issue_list:
                    if 'total_partitions' in broker:
                        builder.text(
                            f"- Broker {broker['node_id']}:\n\n "
                            f"* {broker['total_partitions']} partitions "
                            f"* ({broker.get('deviation_pct', 0):+.1f}% from avg)"
                        )
                    else:
                        builder.text(
                            f"- Broker {broker['node_id']}: "
                            f"{broker['leader_partitions']} leaders "
                            f"({broker.get('deviation_pct', 0):+.1f}% from avg)"
                        )
                builder.text(f"_Recommendation: {findings['data'][findings_key]['recommendation']}_")
                builder.blank()

        # Add recommendations if issues found
        if status in ['critical', 'warning']:
            recommendations = {}

            if status == 'critical':
                recommendations["critical"] = [
                    "Partition Imbalance - Immediate Actions:\n\n",
                    "  1. Generate rebalance plan: kafka-reassign-partitions.sh",
                    "  2. Review cluster capacity before rebalancing",
                    "  3. Execute rebalancing during maintenance window",
                    "  4. Monitor progress with kafka-reassign-partitions.sh --verify",
                    "  5. Run preferred leader election if leaders are imbalanced",
                    "",
                    "If brokers are critically overloaded:\n\n",
                    "  • Add more brokers to cluster",
                    "  • Reduce partition count for new topics",
                    "  • Review topic retention policies (delete old data)"
                ]

            if status == 'warning':
                recommendations["high"] = [
                    "Plan partition rebalancing during maintenance window",
                    "Monitor broker performance metrics for impact",
                    "Review partition assignment strategy",
                    "Consider rack-aware replica assignment"
                ]

            recommendations["general"] = [
                "Partition Balance Best Practices:\n\n",
                "  • Ideal: Even distribution of leaders and followers",
                "  • Leaders handle all read/write traffic (most important to balance)",
                "  • Followers only replicate (less critical to balance perfectly)",
                "  • Recommended: < 1000 partitions per broker for optimal performance",
                "  • Maximum: 2000-4000 partitions per broker (varies by hardware)",
                "",
                "Rebalancing Tools:\n\n",
                "  • kafka-reassign-partitions.sh - Manual reassignment",
                "  • Cruise Control - Automated continuous rebalancing",
                "  • kafka-preferred-replica-election.sh - Fix leader imbalance quickly",
                "",
                "Causes of Imbalance:\n\n",
                "  • Broker additions/removals without rebalancing",
                "  • Topic creation with non-round-robin assignment",
                "  • Preferred leader election not running",
                "  • Broker failures causing leader redistribution",
                "",
                "Impact of Imbalance:\n\n",
                "  • Hotspots on overloaded brokers",
                "  • Uneven resource utilization",
                "  • Degraded performance for some topics",
                "  • Increased latency for clients on overloaded brokers"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Partition balance check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

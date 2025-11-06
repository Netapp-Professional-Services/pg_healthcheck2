"""
Kafka Partition Balance Check (Prometheus - Instaclustr)

Monitors partition distribution across brokers to detect hotspots and imbalances.
Unbalanced partitions can cause performance issues and broker overload.

Health Check: prometheus_partition_balance
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Metrics:
- ic_node_partition_count - Total partitions per broker (leaders + followers)
- ic_node_leader_count - Leader partitions per broker

IMPORTANCE:
- Unbalanced leaders cause uneven load (leaders handle all reads/writes)
- Unbalanced partitions indicate poor distribution
- Too many partitions on one broker degrades performance
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def check_prometheus_partition_balance(connector, settings):
    """
    Check partition balance across brokers via Prometheus (Instaclustr managed service).

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
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Partition Balance (Prometheus)")

    # Check if Prometheus is enabled
    if not settings.get('instaclustr_prometheus_enabled'):
        findings = {
            'status': 'skipped',
            'reason': 'Prometheus monitoring not enabled',
            'data': [],
            'metadata': {
                'source': 'prometheus',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }
        return builder.build(), findings

    try:
        # Import here to avoid dependency
        from plugins.common.prometheus_client import get_instaclustr_client

        # Get cached Prometheus client
        client = get_instaclustr_client(
            cluster_id=settings['instaclustr_cluster_id'],
            username=settings['instaclustr_prometheus_username'],
            api_key=settings['instaclustr_prometheus_api_key'],
            prometheus_base_url=settings.get('instaclustr_prometheus_base_url')
        )

        # Scrape all metrics
        all_metrics = client.scrape_all_nodes()

        if not all_metrics:
            builder.error("❌ No metrics available from Prometheus")
            findings = {
                'status': 'error',
                'error_message': 'No metrics available from Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract partition metrics
        partition_count_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^ic_node_partition_count$'
        )
        leader_count_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^ic_node_leader_count$'
        )

        if not partition_count_metrics:
            builder.error("❌ Partition count metrics not found")
            findings = {
                'status': 'error',
                'error_message': 'Partition count metrics not found in Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Get thresholds
        imbalance_warning_pct = settings.get('kafka_partition_imbalance_warning_pct', 20)
        imbalance_critical_pct = settings.get('kafka_partition_imbalance_critical_pct', 40)
        per_broker_warning = settings.get('kafka_partition_per_broker_warning', 1500)
        per_broker_critical = settings.get('kafka_partition_per_broker_critical', 2000)

        # Process metrics by broker
        broker_data = {}

        # Process partition counts
        for metric in partition_count_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id not in broker_data:
                broker_data[node_id] = {
                    'node_id': node_id,
                    'public_ip': target_labels.get('PublicIp', 'unknown'),
                    'rack': target_labels.get('Rack', 'unknown'),
                    'datacenter': target_labels.get('ClusterDataCenterName', 'unknown')
                }

            broker_data[node_id]['total_partitions'] = int(metric['value'])

        # Process leader counts
        for metric in leader_count_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['leader_partitions'] = int(metric['value'])

        if not broker_data:
            builder.error("❌ No broker data available")
            findings = {
                'status': 'error',
                'error_message': 'No broker data available',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Calculate statistics
        node_data = list(broker_data.values())
        total_partitions_sum = sum(b['total_partitions'] for b in node_data)
        total_leaders_sum = sum(b.get('leader_partitions', 0) for b in node_data)
        avg_partitions = total_partitions_sum / len(node_data)
        avg_leaders = total_leaders_sum / len(node_data) if total_leaders_sum > 0 else 0

        # Identify issues
        overloaded_brokers = []
        critical_overload = []
        imbalanced_partitions = []
        critical_imbalance = []
        imbalanced_leaders = []
        critical_leader_imbalance = []

        for broker in node_data:
            total_parts = broker['total_partitions']
            leader_parts = broker.get('leader_partitions', 0)

            # Calculate follower count
            broker['follower_partitions'] = total_parts - leader_parts

            # Check per-broker partition count
            if total_parts >= per_broker_critical:
                critical_overload.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'total_partitions': total_parts
                })
            elif total_parts >= per_broker_warning:
                overloaded_brokers.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'total_partitions': total_parts
                })

            # Check partition balance
            if avg_partitions > 0:
                deviation_pct = abs(total_parts - avg_partitions) / avg_partitions * 100
                broker['partition_deviation_pct'] = round(deviation_pct, 1)

                if deviation_pct >= imbalance_critical_pct:
                    critical_imbalance.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
                        'total_partitions': total_parts,
                        'deviation_pct': round(deviation_pct, 1)
                    })
                elif deviation_pct >= imbalance_warning_pct:
                    imbalanced_partitions.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
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
                        'public_ip': broker['public_ip'],
                        'leader_partitions': leader_parts,
                        'deviation_pct': round(leader_deviation_pct, 1)
                    })
                elif leader_deviation_pct >= imbalance_warning_pct:
                    imbalanced_leaders.append({
                        'node_id': broker['node_id'],
                        'public_ip': broker['public_ip'],
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
            message = " and ".join(issues)
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
            message = " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"Partitions well-balanced across {len(node_data)} brokers"

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_partitions': {
                'status': status,
                'data': node_data,
                'metadata': {
                    'source': 'prometheus',
                    'metrics': ['partition_count', 'leader_count'],
                    'broker_count': len(node_data)
                }
            },
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
            }
        }

        # Add issue details
        if critical_overload:
            findings['critical_overloaded_brokers'] = {
                'count': len(critical_overload),
                'brokers': critical_overload,
                'recommendation': 'Broker has too many partitions - performance degradation likely'
            }

        if overloaded_brokers:
            findings['overloaded_brokers'] = {
                'count': len(overloaded_brokers),
                'brokers': overloaded_brokers,
                'recommendation': 'Monitor broker performance - partition count approaching limits'
            }

        if critical_imbalance:
            findings['critical_partition_imbalance'] = {
                'count': len(critical_imbalance),
                'brokers': critical_imbalance,
                'recommendation': 'Severe partition imbalance - rebalance cluster immediately'
            }

        if imbalanced_partitions:
            findings['partition_imbalance'] = {
                'count': len(imbalanced_partitions),
                'brokers': imbalanced_partitions,
                'recommendation': 'Partition distribution suboptimal - consider rebalancing'
            }

        if critical_leader_imbalance:
            findings['critical_leader_imbalance'] = {
                'count': len(critical_leader_imbalance),
                'brokers': critical_leader_imbalance,
                'recommendation': 'Severe leader imbalance - some brokers handling disproportionate load'
            }

        if imbalanced_leaders:
            findings['leader_imbalance'] = {
                'count': len(imbalanced_leaders),
                'brokers': imbalanced_leaders,
                'recommendation': 'Leader distribution suboptimal - run preferred leader election'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(f"⚠️  {message}")
        elif status == 'warning':
            builder.warning(f"⚠️  {message}")
        else:
            builder.success(f"✅ {message}")

        builder.blank()
        builder.text("*Cluster Summary:*")
        builder.text(f"- Total Partitions: {total_partitions_sum:,}")
        builder.text(f"- Total Leaders: {total_leaders_sum:,}")
        builder.text(f"- Avg Partitions/Broker: {round(avg_partitions, 1)}")
        builder.text(f"- Avg Leaders/Broker: {round(avg_leaders, 1)}")
        builder.text(f"- Brokers: {len(node_data)}")
        builder.blank()

        # Show per-broker breakdown
        builder.text("*Per-Broker Distribution:*")
        for broker in sorted(node_data, key=lambda x: x['total_partitions'], reverse=True):
            deviation_str = f" ({broker.get('partition_deviation_pct', 0):+.1f}%)" if 'partition_deviation_pct' in broker else ""
            builder.text(
                f"- Broker {broker['node_id'][:8]}: "
                f"{broker['total_partitions']} total "
                f"({broker.get('leader_partitions', 0)} leaders, "
                f"{broker.get('follower_partitions', 0)} followers){deviation_str}"
            )
        builder.blank()

        # Show issues
        for issue_list, issue_type, findings_key in [
            (critical_overload, "Critically Overloaded", "critical_overloaded_brokers"),
            (overloaded_brokers, "Overloaded", "overloaded_brokers"),
            (critical_imbalance, "Critical Partition Imbalance", "critical_partition_imbalance"),
            (imbalanced_partitions, "Partition Imbalance", "partition_imbalance"),
            (critical_leader_imbalance, "Critical Leader Imbalance", "critical_leader_imbalance"),
            (imbalanced_leaders, "Leader Imbalance", "leader_imbalance")
        ]:
            if issue_list:
                symbol = "🔴" if "Critical" in issue_type else "⚠️"
                builder.text(f"*{symbol} {issue_type} ({len(issue_list)}):*")
                for broker in issue_list:
                    if 'total_partitions' in broker:
                        builder.text(
                            f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                            f"{broker['total_partitions']} partitions "
                            f"({broker.get('deviation_pct', 0):+.1f}% from avg)"
                        )
                    else:
                        builder.text(
                            f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                            f"{broker['leader_partitions']} leaders "
                            f"({broker.get('deviation_pct', 0):+.1f}% from avg)"
                        )
                builder.text(f"_Recommendation: {findings[findings_key]['recommendation']}_")
                builder.blank()

        # Add recommendations if issues found
        if status in ['critical', 'warning']:
            recommendations = {}

            if status == 'critical':
                recommendations["critical"] = [
                    "Rebalance cluster immediately to distribute partitions evenly",
                    "Use kafka-reassign-partitions.sh to generate rebalance plan",
                    "Consider increasing cluster size if brokers are consistently overloaded",
                    "Review topic partition counts - may need to reduce for new topics",
                    "Run preferred leader election if leaders are imbalanced"
                ]

            if status == 'warning':
                recommendations["high"] = [
                    "Plan partition rebalancing during maintenance window",
                    "Monitor broker performance metrics for impact",
                    "Review partition assignment strategy",
                    "Consider rack-aware replica assignment"
                ]

            recommendations["general"] = [
                "Partition Balance Best Practices:",
                "  • Ideal: Even distribution of leaders and followers",
                "  • Leaders handle all read/write traffic (most important to balance)",
                "  • Followers only replicate (less critical to balance perfectly)",
                "  • Recommended: < 1000 partitions per broker for optimal performance",
                "  • Maximum: 2000-4000 partitions per broker (varies by hardware)",
                "",
                "Rebalancing Tools:",
                "  • kafka-reassign-partitions.sh - Manual reassignment",
                "  • Cruise Control - Automated continuous rebalancing",
                "  • kafka-preferred-replica-election.sh - Fix leader imbalance quickly",
                "",
                "Causes of Imbalance:",
                "  • Broker additions/removals without rebalancing",
                "  • Topic creation with non-round-robin assignment",
                "  • Preferred leader election not running",
                "  • Broker failures causing leader redistribution",
                "",
                "Impact of Imbalance:",
                "  • Hotspots on overloaded brokers",
                "  • Uneven resource utilization",
                "  • Degraded performance for some topics",
                "  • Increased latency for clients on overloaded brokers"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus partition balance check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {
                'source': 'prometheus',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }
        return builder.build(), findings

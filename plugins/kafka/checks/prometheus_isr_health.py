"""
Kafka ISR (In-Sync Replica) Health Check (Prometheus - Instaclustr)

Monitors ISR health metrics from Instaclustr Prometheus endpoints.
Tracks ISR shrink/expand rates and partitions under min ISR.

Health Check: prometheus_isr_health
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

ISR HEALTH INDICATORS:
- ISR Shrink Rate: Replicas being removed from ISR (bad - indicates replication problems)
- ISR Expand Rate: Replicas being added to ISR (good - indicates recovery)
- Under Min ISR: Partitions that don't have enough in-sync replicas (critical)

Metrics:
- ic_node_isr_shrink_rate
- ic_node_isr_expand_rate
- ic_node_under_min_isr_partitions
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def check_prometheus_isr_health(connector, settings):
    """
    Check ISR health metrics via Prometheus (Instaclustr managed service).

    Monitors:
    - ISR shrink rate (replicas leaving ISR - indicates problems)
    - ISR expand rate (replicas joining ISR - indicates recovery)
    - Partitions under min ISR (critical - not enough replicas)

    Thresholds:
    - ISR Shrink: WARNING > 1/sec, CRITICAL > 10/sec
    - Under Min ISR: WARNING > 0, CRITICAL > 5

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("ISR Health (Prometheus)")

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
        # Import here to avoid dependency if not using Prometheus
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

        # Extract ISR metrics
        isr_shrink_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_isr_shrink_rate$')
        isr_expand_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_isr_expand_rate$')
        under_min_isr_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_under_min_isr_partitions$')

        if not (isr_shrink_metrics or isr_expand_metrics or under_min_isr_metrics):
            builder.error("❌ ISR health metrics not found")
            findings = {
                'status': 'error',
                'error_message': 'ISR health metrics not found in Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Get thresholds
        shrink_warning = settings.get('kafka_isr_shrink_warning', 1.0)
        shrink_critical = settings.get('kafka_isr_shrink_critical', 10.0)
        under_min_isr_warning = settings.get('kafka_under_min_isr_warning', 0)
        under_min_isr_critical = settings.get('kafka_under_min_isr_critical', 5)

        # Process metrics by broker
        broker_data = {}

        # Process ISR shrink rate
        for metric in isr_shrink_metrics:
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

            broker_data[node_id]['isr_shrink_rate'] = round(metric['value'], 3)

        # Process ISR expand rate
        for metric in isr_expand_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['isr_expand_rate'] = round(metric['value'], 3)

        # Process under min ISR
        for metric in under_min_isr_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))

            if node_id in broker_data:
                broker_data[node_id]['under_min_isr_partitions'] = int(metric['value'])

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

        # Convert to list and identify issues
        node_data = list(broker_data.values())
        critical_shrink_brokers = []
        warning_shrink_brokers = []
        critical_under_min_isr = []
        warning_under_min_isr = []

        total_shrink_rate = 0
        total_expand_rate = 0
        total_under_min_isr = 0

        for broker in node_data:
            shrink_rate = broker.get('isr_shrink_rate', 0)
            expand_rate = broker.get('isr_expand_rate', 0)
            under_min_isr = broker.get('under_min_isr_partitions', 0)

            total_shrink_rate += shrink_rate
            total_expand_rate += expand_rate
            total_under_min_isr += under_min_isr

            # Check ISR shrink rate
            if shrink_rate >= shrink_critical:
                critical_shrink_brokers.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'isr_shrink_rate': shrink_rate
                })
            elif shrink_rate >= shrink_warning:
                warning_shrink_brokers.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'isr_shrink_rate': shrink_rate
                })

            # Check under min ISR
            if under_min_isr >= under_min_isr_critical:
                critical_under_min_isr.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'under_min_isr_partitions': under_min_isr
                })
            elif under_min_isr > under_min_isr_warning:
                warning_under_min_isr.append({
                    'node_id': broker['node_id'],
                    'public_ip': broker['public_ip'],
                    'under_min_isr_partitions': under_min_isr
                })

        # Determine overall status
        if critical_shrink_brokers or critical_under_min_isr:
            status = 'critical'
            severity = 10
            issues = []
            if critical_shrink_brokers:
                issues.append(f"{len(critical_shrink_brokers)} broker(s) with high ISR shrink rate")
            if critical_under_min_isr:
                issues.append(f"{len(critical_under_min_isr)} broker(s) with partitions under min ISR")
            message = " and ".join(issues)
        elif warning_shrink_brokers or warning_under_min_isr:
            status = 'warning'
            severity = 7
            issues = []
            if warning_shrink_brokers:
                issues.append(f"{len(warning_shrink_brokers)} broker(s) with elevated ISR shrink rate")
            if warning_under_min_isr:
                issues.append(f"{len(warning_under_min_isr)} broker(s) with partitions under min ISR")
            message = " and ".join(issues)
        else:
            status = 'healthy'
            severity = 0
            message = f"ISR health is stable across {len(node_data)} brokers"

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_isr': {
                'status': status,
                'data': node_data,
                'metadata': {
                    'source': 'prometheus',
                    'metrics': ['isr_shrink_rate', 'isr_expand_rate', 'under_min_isr_partitions'],
                    'broker_count': len(node_data)
                }
            },
            'cluster_aggregate': {
                'total_isr_shrink_rate': round(total_shrink_rate, 3),
                'total_isr_expand_rate': round(total_expand_rate, 3),
                'total_under_min_isr_partitions': total_under_min_isr,
                'avg_isr_shrink_rate': round(total_shrink_rate / len(node_data), 3),
                'avg_isr_expand_rate': round(total_expand_rate / len(node_data), 3),
                'broker_count': len(node_data),
                'thresholds': {
                    'shrink_warning': shrink_warning,
                    'shrink_critical': shrink_critical,
                    'under_min_isr_warning': under_min_isr_warning,
                    'under_min_isr_critical': under_min_isr_critical
                }
            }
        }

        # Add issue details
        if critical_shrink_brokers:
            findings['critical_isr_shrink'] = {
                'count': len(critical_shrink_brokers),
                'brokers': critical_shrink_brokers,
                'recommendation': 'Investigate replication issues immediately - replicas are frequently leaving ISR'
            }

        if warning_shrink_brokers:
            findings['warning_isr_shrink'] = {
                'count': len(warning_shrink_brokers),
                'brokers': warning_shrink_brokers,
                'recommendation': 'Monitor ISR stability - some replicas are struggling to stay in sync'
            }

        if critical_under_min_isr:
            findings['critical_under_min_isr'] = {
                'count': len(critical_under_min_isr),
                'brokers': critical_under_min_isr,
                'recommendation': 'URGENT: Some partitions lack sufficient in-sync replicas for durability guarantees'
            }

        if warning_under_min_isr:
            findings['warning_under_min_isr'] = {
                'count': len(warning_under_min_isr),
                'brokers': warning_under_min_isr,
                'recommendation': 'Monitor partitions - some have fewer ISRs than min.insync.replicas setting'
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
        builder.text(f"- Total ISR Shrink Rate: {round(total_shrink_rate, 3)}/sec")
        builder.text(f"- Total ISR Expand Rate: {round(total_expand_rate, 3)}/sec")
        builder.text(f"- Partitions Under Min ISR: {total_under_min_isr}")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.blank()

        # Show critical ISR shrink issues
        if critical_shrink_brokers:
            builder.text(f"*⚠️  Critical ISR Shrink Rate ({len(critical_shrink_brokers)}):*")
            for broker in critical_shrink_brokers:
                builder.text(
                    f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                    f"{broker['isr_shrink_rate']}/sec"
                )
            builder.text(f"_Recommendation: {findings['critical_isr_shrink']['recommendation']}_")
            builder.blank()

        # Show warning ISR shrink issues
        if warning_shrink_brokers:
            builder.text(f"*⚠️  Elevated ISR Shrink Rate ({len(warning_shrink_brokers)}):*")
            for broker in warning_shrink_brokers:
                builder.text(
                    f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                    f"{broker['isr_shrink_rate']}/sec"
                )
            builder.text(f"_Recommendation: {findings['warning_isr_shrink']['recommendation']}_")
            builder.blank()

        # Show critical under min ISR issues
        if critical_under_min_isr:
            builder.text(f"*🔴 Partitions Under Min ISR ({len(critical_under_min_isr)} brokers):*")
            for broker in critical_under_min_isr:
                builder.text(
                    f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                    f"{broker['under_min_isr_partitions']} partitions"
                )
            builder.text(f"_Recommendation: {findings['critical_under_min_isr']['recommendation']}_")
            builder.blank()

        # Show warning under min ISR issues
        if warning_under_min_isr:
            builder.text(f"*⚠️  Partitions Under Min ISR ({len(warning_under_min_isr)} brokers):*")
            for broker in warning_under_min_isr:
                builder.text(
                    f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): "
                    f"{broker['under_min_isr_partitions']} partitions"
                )
            builder.text(f"_Recommendation: {findings['warning_under_min_isr']['recommendation']}_")
            builder.blank()

        # Add recommendations if issues found
        if critical_shrink_brokers or warning_shrink_brokers or critical_under_min_isr or warning_under_min_isr:
            recommendations = {}

            if critical_shrink_brokers or critical_under_min_isr:
                recommendations["critical"] = [
                    "Check broker health - high CPU, disk I/O, or network issues can cause ISR problems",
                    "Review replica.lag.time.max.ms setting (default 30s)",
                    "Verify brokers can communicate without packet loss",
                    "Check for GC pauses that might cause replication delays",
                    "Review recent configuration changes"
                ]

            if warning_shrink_brokers or warning_under_min_isr:
                recommendations["high"] = [
                    "Monitor ISR metrics for trends",
                    "Check if ISR issues correlate with traffic spikes",
                    "Review partition distribution across brokers",
                    "Ensure adequate resources for replication"
                ]

            recommendations["general"] = [
                "ISR Health Best Practices:",
                "  • Set min.insync.replicas=2 for critical topics (requires RF≥3)",
                "  • Monitor replica.lag.time.max.ms (time before replica is kicked from ISR)",
                "  • Ensure brokers have sufficient resources for replication",
                "  • Use rack awareness for better availability",
                "  • Avoid large message batches that cause replication lag",
                "",
                "ISR Shrink Causes:",
                "  • Slow followers (resource constrained)",
                "  • Network issues between brokers",
                "  • Broker GC pauses",
                "  • Disk I/O bottlenecks",
                "  • Large message batches"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus ISR health check failed: {e}", exc_info=True)
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

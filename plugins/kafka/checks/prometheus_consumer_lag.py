"""
Kafka Consumer Lag Check (Prometheus - Instaclustr)

Monitors consumer lag metrics from Instaclustr Prometheus endpoints.
Tracks how far behind consumers are from the latest messages.

Health Check: prometheus_consumer_lag
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Consumer Lag Metrics:
- kafka_consumerGroup_consumerlag - Per-consumer lag
- kafka_consumerGroup_consumergrouplag - Per-group aggregate lag

High consumer lag indicates consumers cannot keep up with producers.
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def check_prometheus_consumer_lag(connector, settings):
    """
    Check consumer lag metrics via Prometheus (Instaclustr managed service).

    Monitors:
    - Consumer lag per consumer group
    - Total lag across all consumers

    Thresholds:
    - WARNING: Lag > 10,000 messages
    - CRITICAL: Lag > 100,000 messages

    Note: Appropriate lag thresholds vary by use case. Configure thresholds
    based on your message rate and acceptable processing delay.

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Consumer Lag (Prometheus)")

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

        # Extract consumer lag metrics
        consumer_lag_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^kafka_consumerGroup_consumerlag$'
        )
        consumer_group_lag_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^kafka_consumerGroup_consumergrouplag$'
        )

        if not (consumer_lag_metrics or consumer_group_lag_metrics):
            # No consumer lag metrics - might mean no active consumers
            builder.text("ℹ️  No consumer lag metrics found")
            builder.blank()
            builder.text("This could indicate:")
            builder.text("- No active consumer groups")
            builder.text("- Consumers are not registered with consumer groups")
            builder.text("- Consumer metrics not yet available")
            builder.blank()

            findings = {
                'status': 'info',
                'message': 'No consumer lag metrics available',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Get thresholds
        lag_warning = settings.get('kafka_consumer_lag_warning', 10000)
        lag_critical = settings.get('kafka_consumer_lag_critical', 100000)

        # Process consumer group lag metrics (aggregate per group)
        consumer_groups = {}

        for metric in consumer_group_lag_metrics:
            labels = metric.get('labels', {})
            group_id = labels.get('consumerGroup', 'unknown')
            lag = int(metric['value'])

            if group_id not in consumer_groups:
                consumer_groups[group_id] = {
                    'group_id': group_id,
                    'total_lag': lag,
                    'topics': set()
                }
            else:
                consumer_groups[group_id]['total_lag'] += lag

            # Track topics
            topic = labels.get('topic', 'unknown')
            if topic != 'unknown':
                consumer_groups[group_id]['topics'].add(topic)

        # Convert to list
        group_data = []
        for group_id, data in consumer_groups.items():
            group_data.append({
                'group_id': group_id,
                'total_lag': data['total_lag'],
                'topics': list(data['topics'])
            })

        if not group_data:
            builder.text("ℹ️  No consumer group lag data available")
            findings = {
                'status': 'info',
                'message': 'No consumer groups detected',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Identify groups with issues
        critical_lag_groups = []
        warning_lag_groups = []
        healthy_groups = []

        total_lag = 0

        for group in group_data:
            lag = group['total_lag']
            total_lag += lag

            if lag >= lag_critical:
                critical_lag_groups.append(group)
            elif lag >= lag_warning:
                warning_lag_groups.append(group)
            else:
                healthy_groups.append(group)

        # Determine overall status
        if critical_lag_groups:
            status = 'critical'
            severity = 10
            message = f"🔴 {len(critical_lag_groups)} consumer group(s) with critical lag (>{lag_critical:,} messages)"
        elif warning_lag_groups:
            status = 'warning'
            severity = 7
            message = f"⚠️  {len(warning_lag_groups)} consumer group(s) with high lag (>{lag_warning:,} messages)"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ All {len(group_data)} consumer group(s) have healthy lag"

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_consumer_group_lag': {
                'status': status,
                'data': group_data,
                'metadata': {
                    'source': 'prometheus',
                    'metric': 'kafka_consumerGroup_consumergrouplag',
                    'group_count': len(group_data)
                }
            },
            'cluster_aggregate': {
                'total_lag_all_groups': total_lag,
                'groups_with_critical_lag': len(critical_lag_groups),
                'groups_with_warning_lag': len(warning_lag_groups),
                'total_groups': len(group_data),
                'thresholds': {
                    'warning': lag_warning,
                    'critical': lag_critical
                }
            }
        }

        # Add issue details
        if critical_lag_groups:
            findings['critical_lag_groups'] = {
                'count': len(critical_lag_groups),
                'groups': critical_lag_groups,
                'recommendation': 'Consumers cannot keep up with producers - investigate consumer performance'
            }

        if warning_lag_groups:
            findings['warning_lag_groups'] = {
                'count': len(warning_lag_groups),
                'groups': warning_lag_groups,
                'recommendation': 'Consumer lag is elevated - monitor for continued growth'
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
        builder.text(f"- Total Consumer Groups: {len(group_data)}")
        builder.text(f"- Total Lag (all groups): {total_lag:,} messages")
        builder.text(f"- Groups with Critical Lag: {len(critical_lag_groups)}")
        builder.text(f"- Groups with Warning Lag: {len(warning_lag_groups)}")
        builder.text(f"- Healthy Groups: {len(healthy_groups)}")
        builder.blank()

        # Show critical lag groups
        if critical_lag_groups:
            builder.text(f"*🔴 Critical Lag Consumer Groups ({len(critical_lag_groups)}):*")
            for group in sorted(critical_lag_groups, key=lambda x: x['total_lag'], reverse=True):
                topics_str = ', '.join(group['topics'][:3])
                if len(group['topics']) > 3:
                    topics_str += f" (+{len(group['topics'])-3} more)"
                builder.text(
                    f"- Group: {group['group_id']}"
                )
                builder.text(f"  Lag: {group['total_lag']:,} messages")
                builder.text(f"  Topics: {topics_str}")
            builder.text(f"_Recommendation: {findings['critical_lag_groups']['recommendation']}_")
            builder.blank()

        # Show warning lag groups
        if warning_lag_groups:
            builder.text(f"*⚠️  High Lag Consumer Groups ({len(warning_lag_groups)}):*")
            for group in sorted(warning_lag_groups, key=lambda x: x['total_lag'], reverse=True):
                topics_str = ', '.join(group['topics'][:3])
                if len(group['topics']) > 3:
                    topics_str += f" (+{len(group['topics'])-3} more)"
                builder.text(
                    f"- Group: {group['group_id']}"
                )
                builder.text(f"  Lag: {group['total_lag']:,} messages")
                builder.text(f"  Topics: {topics_str}")
            builder.text(f"_Recommendation: {findings['warning_lag_groups']['recommendation']}_")
            builder.blank()

        # Add recommendations if issues found
        if critical_lag_groups or warning_lag_groups:
            recommendations = {}

            if critical_lag_groups:
                recommendations["critical"] = [
                    "Investigate consumer application performance immediately",
                    "Check consumer logs for errors or slow processing",
                    "Verify consumer instances are running and healthy",
                    "Review consumer configuration (fetch.min.bytes, fetch.max.wait.ms)",
                    "Consider adding more consumer instances (up to partition count)",
                    "Check if consumers are doing expensive processing per message"
                ]

            if warning_lag_groups:
                recommendations["high"] = [
                    "Monitor consumer lag trends - is it growing or stable?",
                    "Review consumer processing performance",
                    "Check if lag correlates with traffic spikes",
                    "Verify consumer resources (CPU, memory, network)",
                    "Consider consumer scaling if lag persists"
                ]

            recommendations["general"] = [
                "Consumer Lag Best Practices:",
                "  • Set appropriate max.poll.records based on processing time",
                "  • Monitor both lag and consumer throughput",
                "  • Scale consumers horizontally (up to partition count)",
                "  • Use session.timeout.ms and max.poll.interval.ms appropriately",
                "  • Consider async processing for expensive operations",
                "",
                "Common Causes of Consumer Lag:",
                "  • Slow message processing (expensive operations)",
                "  • Insufficient consumer instances",
                "  • Consumer crashes or restarts",
                "  • Network issues",
                "  • Producer burst exceeding consumer capacity",
                "  • Inefficient consumer code",
                "",
                "Monitoring Tips:",
                "  • Appropriate lag varies by use case",
                "  • High-volume low-latency: aim for < 1000 messages",
                "  • Batch processing: higher lag may be acceptable",
                "  • Monitor lag growth rate, not just absolute value",
                "  • Set alerts based on your SLA requirements"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus consumer lag check failed: {e}", exc_info=True)
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

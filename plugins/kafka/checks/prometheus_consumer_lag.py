"""
Kafka Consumer Lag Check (Unified Adaptive)

Monitors consumer lag using adaptive collection strategy.
Tracks how far behind consumers are from the latest messages in topics.

Health Check: prometheus_consumer_lag
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- consumer_lag - Per-consumer-group lag (messages behind latest offset)

CRITICAL IMPORTANCE:
- High consumer lag means consumers cannot keep up with producers
- Can lead to data loss if messages expire before processing
- Indicates performance bottlenecks in consumer applications
- May signal need for consumer scaling

NOTE: Appropriate lag thresholds vary greatly by use case.
Configure based on your message rate and acceptable processing delay.
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


def check_prometheus_consumer_lag(connector, settings):
    """
    Check consumer lag metrics via adaptive collection strategy.

    Monitors:
    - Consumer lag per consumer group
    - Total lag across all consumers

    Thresholds:
    - WARNING: Lag > 10,000 messages
    - CRITICAL: Lag > 100,000 messages

    Note: Appropriate lag thresholds vary by use case. Configure thresholds
    based on your message rate and acceptable processing delay.

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Consumer Lag (Prometheus)")

    try:
        # Get metric definition
        consumer_lag_def = get_metric_definition('consumer_lag')

        if not consumer_lag_def:
            builder.error("❌ Consumer lag metric definition not found")
            findings = {
                'status': 'error',
                'error_message': 'Metric definition not found',
                'data': [],
                'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Collect metric adaptively
        result = collect_metric_adaptive(consumer_lag_def, connector, settings)

        if not result:
            builder.text("ℹ️  Consumer lag metrics not available")
            builder.blank()
            builder.text("This could indicate:")
            builder.text("  • No active consumer groups")
            builder.text("  • Consumers not registered with groups")
            builder.text("  • Metrics collection method unavailable")
            findings = {
                'status': 'info',
                'message': 'No consumer lag metrics available',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data - consumer lag metrics are per-group, not per-broker
        method = result.get('method')

        # For consumer lag, node_metrics contains group-level data
        # The key may be group ID or combined group+topic
        group_metrics = result.get('node_metrics', {})

        if not group_metrics:
            builder.text("ℹ️  No consumer group lag data available")
            findings = {
                'status': 'info',
                'message': 'No consumer groups detected',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Get thresholds
        lag_warning = settings.get('kafka_consumer_lag_warning', 10000)
        lag_critical = settings.get('kafka_consumer_lag_critical', 100000)

        # Process consumer group lag
        # Group metrics by consumer group (metrics may be per-topic-partition)
        consumer_groups = {}

        for key, lag_value in group_metrics.items():
            # Key format varies - may be "group_id" or "group_id:topic:partition"
            # Extract group ID (first part before colon if present)
            group_id = key.split(':')[0] if ':' in key else key

            if group_id not in consumer_groups:
                consumer_groups[group_id] = {
                    'group_id': group_id,
                    'total_lag': 0,
                    'partition_count': 0
                }

            consumer_groups[group_id]['total_lag'] += int(lag_value)
            consumer_groups[group_id]['partition_count'] += 1

        # Convert to list
        group_data = list(consumer_groups.values())

        if not group_data:
            builder.text("ℹ️  No consumer group lag data after processing")
            findings = {
                'status': 'info',
                'message': 'No consumer groups detected',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
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
            'data': {
                'per_consumer_group_lag': group_data,
                'cluster_aggregate': {
                    'total_lag_all_groups': total_lag,
                    'groups_with_critical_lag': len(critical_lag_groups),
                    'groups_with_warning_lag': len(warning_lag_groups),
                    'healthy_groups': len(healthy_groups),
                    'total_groups': len(group_data),
                    'thresholds': {
                        'warning': lag_warning,
                        'critical': lag_critical
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['consumer_lag'],
                'group_count': len(group_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Add issue details
        if critical_lag_groups:
            findings['data']['critical_lag_groups'] = {
                'count': len(critical_lag_groups),
                'groups': critical_lag_groups,
                'recommendation': 'Consumers cannot keep up with producers - investigate consumer performance'
            }

        if warning_lag_groups:
            findings['data']['warning_lag_groups'] = {
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
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        # Show critical lag groups
        if critical_lag_groups:
            builder.text(f"*🔴 Critical Lag Consumer Groups ({len(critical_lag_groups)}):*")
            for group in sorted(critical_lag_groups, key=lambda x: x['total_lag'], reverse=True):
                builder.text(f"- Group: {group['group_id']}")
                builder.text(f"  Lag: {group['total_lag']:,} messages")
                builder.text(f"  Partitions: {group['partition_count']}")
            builder.text(f"_Recommendation: {findings['data']['critical_lag_groups']['recommendation']}_")
            builder.blank()

        # Show warning lag groups
        if warning_lag_groups:
            builder.text(f"*⚠️  High Lag Consumer Groups ({len(warning_lag_groups)}):*")
            for group in sorted(warning_lag_groups, key=lambda x: x['total_lag'], reverse=True):
                builder.text(f"- Group: {group['group_id']}")
                builder.text(f"  Lag: {group['total_lag']:,} messages")
                builder.text(f"  Partitions: {group['partition_count']}")
            builder.text(f"_Recommendation: {findings['data']['warning_lag_groups']['recommendation']}_")
            builder.blank()

        # Add recommendations if issues found
        if critical_lag_groups or warning_lag_groups:
            recommendations = {}

            if critical_lag_groups:
                recommendations["critical"] = [
                    "Consumer Lag Crisis - Immediate Actions:",
                    "  1. Check if consumer application is running and healthy",
                    "  2. Review consumer logs for errors or exceptions",
                    "  3. Verify consumer processing performance (slow operations?)",
                    "  4. Check consumer resource utilization (CPU, memory, network)",
                    "  5. Add more consumer instances (up to partition count)",
                    "",
                    "Common Causes:",
                    "  • Consumer crashes or restarts",
                    "  • Expensive processing per message (database lookups, API calls)",
                    "  • Insufficient consumer instances for load",
                    "  • Network issues between consumer and Kafka",
                    "  • Consumer rebalancing (temporary lag spikes)",
                    "",
                    "Quick Fixes:",
                    "  • Scale consumers horizontally (max = partition count)",
                    "  • Optimize consumer processing logic",
                    "  • Increase max.poll.records if processing is fast",
                    "  • Use async/batch processing for expensive operations"
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
                "  • Set max.poll.records based on processing time per message",
                "  • Monitor both lag and consumer throughput",
                "  • Scale consumers horizontally (up to partition count)",
                "  • Use session.timeout.ms and max.poll.interval.ms appropriately",
                "  • Consider async processing for expensive operations",
                "",
                "Configuration Tuning:",
                "  • max.poll.records: Records returned in single poll (default: 500)",
                "  • max.poll.interval.ms: Max time between polls (default: 5 min)",
                "  • session.timeout.ms: Max time before consumer considered dead",
                "  • fetch.min.bytes: Min data before returning from fetch",
                "  • fetch.max.wait.ms: Max wait if fetch.min.bytes not met",
                "",
                "Monitoring Tips:",
                "  • Appropriate lag varies by use case",
                "  • High-volume low-latency: aim for < 1000 messages",
                "  • Batch processing: higher lag may be acceptable",
                "  • Monitor lag growth rate, not just absolute value",
                "  • Set alerts based on your SLA requirements",
                "",
                "Scaling Consumers:",
                "  • Max consumers = total partitions across subscribed topics",
                "  • More consumers than partitions = idle consumers",
                "  • Each partition consumed by exactly one consumer in group",
                "  • Use multiple consumer groups for different processing needs"
            ]

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Consumer lag check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

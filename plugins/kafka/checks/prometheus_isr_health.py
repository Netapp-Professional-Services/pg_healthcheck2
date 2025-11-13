"""
Kafka ISR (In-Sync Replica) Health Check (Unified Adaptive)

Monitors ISR shrink rate using adaptive collection strategy.
ISR shrinks indicate replicas falling out of sync - a sign of replication problems.

Health Check: prometheus_isr_health
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

ISR HEALTH INDICATORS:
- ISR Shrink Rate: Replicas being removed from ISR (bad - indicates replication problems)
- High shrink rate means replicas can't keep up with leader

Metrics:
- ic_node_isr_shrink_rate / kafka_server_replicamanager_isrshrinkspersec_oneminuterate

CRITICAL IMPORTANCE:
- ISR shrinks reduce fault tolerance (fewer in-sync replicas)
- Persistent ISR shrinks indicate serious cluster health issues
- Can lead to data loss if combined with broker failures
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def check_prometheus_isr_health(connector, settings):
    """
    Check ISR health via adaptive collection strategy.

    Monitors:
    - ISR shrink rate (replicas leaving ISR - indicates problems)

    Thresholds:
    - WARNING: > 1/sec
    - CRITICAL: > 10/sec

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("ISR Health (Prometheus)")

    try:
        # Get metric definition
        metric_def = get_metric_definition('isr_shrink_rate')
        if not metric_def:
            builder.error("❌ ISR shrink rate metric definition not found")
            findings = {
                'status': 'error',
                'error_message': 'Metric definition not found',
                'data': [],
                'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Collect metric adaptively
        result = collect_metric_adaptive(metric_def, connector, settings)

        if not result:
            builder.warning(
                "⚠️ Could not collect ISR health metrics\n\n"
                "*Tried collection methods:*\n"
                "1. Instaclustr Prometheus API - Not configured or unavailable\n"
                "2. Local Prometheus JMX exporter - Not found or SSH unavailable\n"
                "3. Standard JMX - Not available or SSH unavailable"
            )
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect ISR health metrics using any method',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = result.get('method')
        node_metrics = result.get('node_metrics', {})

        if not node_metrics:
            builder.error("❌ No broker data available")
            findings = {
                'status': 'error',
                'error_message': 'No broker data available',
                'data': [],
                'metadata': {'method': method, 'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Get thresholds
        warning_threshold = settings.get('kafka_isr_shrink_warning', 1)
        critical_threshold = settings.get('kafka_isr_shrink_critical', 10)

        # Process broker data
        node_data = []
        critical_brokers = []
        warning_brokers = []

        for node_host, shrink_rate in node_metrics.items():
            broker_entry = {
                'node_id': node_host,
                'host': node_host,
                'isr_shrink_rate': round(shrink_rate, 2)
            }
            node_data.append(broker_entry)

            if shrink_rate >= critical_threshold:
                critical_brokers.append(broker_entry)
            elif shrink_rate >= warning_threshold:
                warning_brokers.append(broker_entry)

        # Determine overall status
        if critical_brokers:
            status = 'critical'
            severity = 10
            message = f"🔴 CRITICAL: {len(critical_brokers)} broker(s) with high ISR shrink rate (>{critical_threshold}/sec)"
        elif warning_brokers:
            status = 'warning'
            severity = 8
            message = f"⚠️  {len(warning_brokers)} broker(s) with elevated ISR shrink rate (>{warning_threshold}/sec)"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ ISR health is good across {len(node_data)} brokers"

        # Calculate cluster aggregate
        avg_shrink = result.get('cluster_avg', 0)
        total_shrink = result.get('cluster_total', 0)

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_isr': node_data,
                'cluster_aggregate': {
                    'avg_isr_shrink_rate': round(avg_shrink, 2),
                    'total_isr_shrink_rate': round(total_shrink, 2),
                    'brokers_critical': len(critical_brokers),
                    'brokers_warning': len(warning_brokers),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'warning': warning_threshold,
                        'critical': critical_threshold
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['isr_shrink_rate'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        if critical_brokers:
            findings['data']['critical_isr_shrinks'] = {
                'count': len(critical_brokers),
                'brokers': critical_brokers,
                'recommendation': 'URGENT: Investigate replication issues - replicas falling out of sync'
            }

        if warning_brokers:
            findings['data']['warning_isr_shrinks'] = {
                'count': len(warning_brokers),
                'brokers': warning_brokers,
                'recommendation': 'Monitor ISR health - intermittent replication issues'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*🚨 CRITICAL: Replication Problems Detected 🚨*")
            builder.blank()
            builder.text("High ISR shrink rate indicates:")
            builder.text("• Replicas cannot keep up with leader")
            builder.text("• Reduced fault tolerance (fewer in-sync replicas)")
            builder.text("• Risk of data loss if broker fails")
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg ISR Shrink Rate: {round(avg_shrink, 2)}/sec")
        builder.text(f"- Total Cluster Shrink Rate: {round(total_shrink, 2)}/sec")
        builder.text(f"- Brokers with Issues: {len(critical_brokers)} critical, {len(warning_brokers)} warning")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker ISR Shrink Rates:*")
        for broker in sorted(node_data, key=lambda x: x['isr_shrink_rate'], reverse=True):
            status_icon = "🔴" if broker['isr_shrink_rate'] >= critical_threshold else "⚠️" if broker['isr_shrink_rate'] >= warning_threshold else "✅"
            builder.text(f"{status_icon} Broker {broker['node_id']}: {broker['isr_shrink_rate']}/sec")
        builder.blank()

        if critical_brokers or warning_brokers:
            recommendations = {
                "critical" if critical_brokers else "high": [
                    "ISR Shrink Root Causes:",
                    "  • Network issues between brokers",
                    "  • Slow replica brokers (disk I/O, CPU, memory pressure)",
                    "  • Replication lag exceeding replica.lag.time.max.ms (default: 30s)",
                    "  • Broker restarts or failures",
                    "  • Very high throughput overwhelming replicas",
                    "",
                    "Immediate Actions:",
                    "  1. Check broker health: CPU, disk I/O, memory, network",
                    "  2. Review replication lag: Check under-replicated partitions",
                    "  3. Check network connectivity between brokers",
                    "  4. Review broker logs for errors",
                    "  5. Monitor disk space and I/O wait times",
                    "",
                    "Long-term Solutions:",
                    "  • Increase replica.lag.time.max.ms if transient issues",
                    "  • Upgrade broker hardware (faster disks, more RAM)",
                    "  • Reduce partition count per broker",
                    "  • Review replication factor (too high for cluster size?)",
                    "  • Enable compression to reduce network/disk load"
                ],
                "general": [
                    "ISR Health Best Practices:",
                    "  • Monitor ISR shrink/expand rates continuously",
                    "  • ISR shrinks should be rare in healthy clusters",
                    "  • Occasional shrinks during rolling restarts are normal",
                    "  • Persistent shrinks indicate serious health issues",
                    "  • Alert at > 1/sec (warning), > 10/sec (critical)"
                ]
            }
            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"ISR health check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

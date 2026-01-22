"""
Kafka JVM Heap Usage Check (Unified Adaptive)

Monitors JVM heap memory usage using adaptive collection strategy.
High heap usage can lead to performance degradation and OutOfMemoryErrors.

Health Check: prometheus_jvm_heap
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"}

IMPORTANCE:
- High heap usage causes increased GC frequency and pause times
- > 90% heap usage is dangerous - can lead to OutOfMemoryError
- Monitor trends - consistently high usage indicates need for more heap
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder, get_metric_collection_error_message
from plugins.common.metric_collection_strategies import collect_metric_adaptive
from plugins.kafka.utils.kafka_metric_definitions import get_metric_definition

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def check_prometheus_jvm_heap(connector, settings):
    """
    Check JVM heap memory usage via adaptive collection strategy.

    Monitors:
    - Heap memory used
    - Heap memory max
    - Heap usage percentage

    Thresholds:
    - WARNING: > 75% heap usage
    - CRITICAL: > 90% heap usage

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("JVM Heap Usage (Prometheus)")

    try:
        # Get metric definitions
        heap_used_def = get_metric_definition('jvm_heap_used')
        heap_max_def = get_metric_definition('jvm_heap_max')

        if not heap_used_def or not heap_max_def:
            builder.error("❌ JVM heap metric definitions not found")
            findings = {
                'status': 'error',
                'error_message': 'Metric definitions not found',
                'data': [],
                'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Collect both metrics adaptively
        used_result = collect_metric_adaptive(heap_used_def, connector, settings)
        max_result = collect_metric_adaptive(heap_max_def, connector, settings)

        if not used_result and not max_result:
            error_msg = get_metric_collection_error_message(connector, "JVM heap metrics")
            builder.warning(error_msg)
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect JVM heap metrics using any method',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = used_result.get('method') if used_result else max_result.get('method')
        used_metrics = used_result.get('node_metrics', {}) if used_result else {}
        max_metrics = max_result.get('node_metrics', {}) if max_result else {}

        # Get thresholds
        warning_pct = settings.get('kafka_heap_warning_pct', 75)
        critical_pct = settings.get('kafka_heap_critical_pct', 90)

        # Combine broker data and calculate percentages
        all_hosts = set(used_metrics.keys()) | set(max_metrics.keys())
        node_data = []
        critical_brokers = []
        warning_brokers = []

        for host in all_hosts:
            heap_used = used_metrics.get(host, 0)
            heap_max = max_metrics.get(host, 1)  # Avoid division by zero
            heap_pct = (heap_used / heap_max * 100) if heap_max > 0 else 0

            broker_entry = {
                'node_id': host,
                'host': host,
                'heap_used_bytes': int(heap_used),
                'heap_max_bytes': int(heap_max),
                'heap_used_mb': round(heap_used / 1024 / 1024, 1),
                'heap_max_mb': round(heap_max / 1024 / 1024, 1),
                'heap_usage_pct': round(heap_pct, 1)
            }
            node_data.append(broker_entry)

            # Check thresholds
            if heap_pct >= critical_pct:
                critical_brokers.append(broker_entry)
            elif heap_pct >= warning_pct:
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
            severity = 10
            message = f"🔴 CRITICAL: {len(critical_brokers)} broker(s) with dangerous heap usage (>{critical_pct}%)"
        elif warning_brokers:
            status = 'warning'
            severity = 7
            message = f"⚠️  {len(warning_brokers)} broker(s) with high heap usage (>{warning_pct}%)"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ JVM heap usage healthy across {len(node_data)} brokers"

        # Calculate cluster aggregates
        avg_heap_pct = sum(b['heap_usage_pct'] for b in node_data) / len(node_data)
        total_heap_used_gb = sum(b['heap_used_bytes'] for b in node_data) / 1024 / 1024 / 1024
        total_heap_max_gb = sum(b['heap_max_bytes'] for b in node_data) / 1024 / 1024 / 1024

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_heap': node_data,
                'cluster_aggregate': {
                    'avg_heap_usage_pct': round(avg_heap_pct, 1),
                    'total_heap_used_gb': round(total_heap_used_gb, 2),
                    'total_heap_max_gb': round(total_heap_max_gb, 2),
                    'brokers_critical': len(critical_brokers),
                    'brokers_warning': len(warning_brokers),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'warning_pct': warning_pct,
                        'critical_pct': critical_pct
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['jvm_heap_used', 'jvm_heap_max'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        if critical_brokers:
            findings['data']['critical_heap_usage'] = {
                'count': len(critical_brokers),
                'brokers': critical_brokers,
                'recommendation': 'URGENT: Increase heap size or reduce memory usage - OutOfMemoryError imminent'
            }

        if warning_brokers:
            findings['data']['warning_heap_usage'] = {
                'count': len(warning_brokers),
                'brokers': warning_brokers,
                'recommendation': 'Monitor heap usage trends - plan heap increase'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*🚨 CRITICAL: OutOfMemoryError Risk 🚨*")
            builder.blank()
            builder.text("High heap usage (>90%) can cause:")
            builder.text("• Frequent Full GC pauses (broker unresponsiveness)")
            builder.text("• OutOfMemoryError crashes")
            builder.text("• Severe performance degradation")
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Heap Usage: {round(avg_heap_pct, 1)}%")
        builder.text(f"- Total Heap Allocated: {round(total_heap_max_gb, 2)} GB")
        builder.text(f"- Total Heap Used: {round(total_heap_used_gb, 2)} GB")
        builder.text(f"- Brokers at Risk: {len(critical_brokers)} critical, {len(warning_brokers)} warning")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker Heap Usage:*")
        for broker in sorted(node_data, key=lambda x: x['heap_usage_pct'], reverse=True):
            status_icon = "🔴" if broker['heap_usage_pct'] >= critical_pct else "⚠️" if broker['heap_usage_pct'] >= warning_pct else "✅"
            builder.text(
                f"{status_icon} Broker {broker['node_id']}: "
                f"{broker['heap_used_mb']:.1f}/{broker['heap_max_mb']:.1f} MB ({broker['heap_usage_pct']}%)"
            )
        builder.blank()

        if critical_brokers or warning_brokers:
            recommendations = {
                "critical" if critical_brokers else "high": [
                    "JVM Heap Issues - Immediate Actions:",
                    "  1. Increase heap size: Edit -Xms and -Xmx JVM options",
                    "  2. Typical Kafka heap: 6-8 GB (but keep < 32 GB for compressed oops)",
                    "  3. Restart broker (rolling restart for zero downtime)",
                    "  4. Monitor heap usage after restart",
                    "",
                    "Root Cause Analysis:",
                    "  • Check for memory leaks (heap grows continuously)",
                    "  • Review partition count (2 FDs + memory per partition)",
                    "  • Check message sizes (large messages increase heap pressure)",
                    "  • Review consumer fetch sizes (fetch.max.bytes)",
                    "  • Analyze GC logs for patterns",
                    "",
                    "Tuning Recommendations:",
                    "  • Use G1GC: -XX:+UseG1GC (default in modern Kafka)",
                    "  • Set Xms = Xmx (avoid heap resizing overhead)",
                    "  • Keep heap < 32 GB (compressed oops boundary)",
                    "  • For very large heaps, consider ZGC or Shenandoah",
                    "  • Monitor off-heap memory (page cache) usage"
                ],
                "general": [
                    "Best Practices:",
                    "  • Typical production heap: 6-8 GB",
                    "  • Alert at 75% (warning), 90% (critical)",
                    "  • Monitor heap trends over time",
                    "  • Plan for 2x expected usage as buffer",
                    "  • Remember: Kafka relies heavily on page cache, not just heap"
                ]
            }
            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"JVM heap check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

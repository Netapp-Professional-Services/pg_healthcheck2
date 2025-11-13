"""
Kafka GC (Garbage Collection) Health Check (Unified Adaptive)

Monitors JVM garbage collection performance using adaptive collection strategy.
Excessive GC time indicates memory pressure and can cause performance degradation.

Health Check: prometheus_gc_health
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- young_gc_time - Young generation GC collection time (cumulative milliseconds)
- old_gc_time - Old generation (Full) GC collection time (cumulative milliseconds)

IMPORTANCE:
- High GC time indicates memory pressure and heap sizing issues
- Full GC pauses cause broker unresponsiveness and latency spikes
- Excessive GC can lead to cascading failures in cluster

NOTE: GC time metrics are CUMULATIVE since broker start.
Monitor trends and rates, not absolute values.
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


def check_prometheus_gc_health(connector, settings):
    """
    Check GC health metrics via adaptive collection strategy.

    Monitors:
    - Young generation GC collection time (cumulative)
    - Old generation (Full) GC collection time (cumulative)

    Thresholds:
    NOTE: Since metrics are cumulative, we cannot directly apply percentage thresholds.
    This check reports values for trend analysis.

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("GC Health (Prometheus)")

    try:
        # Get metric definitions
        young_gc_def = get_metric_definition('young_gc_time')
        old_gc_def = get_metric_definition('old_gc_time')

        if not young_gc_def or not old_gc_def:
            builder.error("❌ GC metric definitions not found")
            findings = {
                'status': 'error',
                'error_message': 'Metric definitions not found',
                'data': [],
                'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
            }
            return builder.build(), findings

        # Collect both metrics adaptively
        young_result = collect_metric_adaptive(young_gc_def, connector, settings)
        old_result = collect_metric_adaptive(old_gc_def, connector, settings)

        if not young_result and not old_result:
            builder.warning(
                "⚠️ Could not collect GC health metrics\n\n"
                "*Tried collection methods:*\n"
                "1. Instaclustr Prometheus API - Not configured or unavailable\n"
                "2. Local Prometheus JMX exporter - Not found or SSH unavailable\n"
                "3. Standard JMX - Not available or SSH unavailable"
            )
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect GC health metrics using any method',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = young_result.get('method') if young_result else old_result.get('method')
        young_gc_metrics = young_result.get('node_metrics', {}) if young_result else {}
        old_gc_metrics = old_result.get('node_metrics', {}) if old_result else {}

        # Combine broker data
        all_hosts = set(young_gc_metrics.keys()) | set(old_gc_metrics.keys())
        node_data = []

        for host in all_hosts:
            young_gc_time = young_gc_metrics.get(host, 0)
            old_gc_time = old_gc_metrics.get(host, 0)

            broker_entry = {
                'node_id': host,
                'host': host,
                'young_gc_time_ms': round(young_gc_time, 2),
                'old_gc_time_ms': round(old_gc_time, 2),
                'total_gc_time_ms': round(young_gc_time + old_gc_time, 2)
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

        # Calculate cluster aggregates
        avg_young_gc = sum(b['young_gc_time_ms'] for b in node_data) / len(node_data)
        avg_old_gc = sum(b['old_gc_time_ms'] for b in node_data) / len(node_data)
        avg_total_gc = sum(b['total_gc_time_ms'] for b in node_data) / len(node_data)

        # Since these are cumulative values, we report them as info
        # In a production monitoring system, you'd calculate rates from consecutive scrapes
        status = 'info'
        severity = 0
        message = f"ℹ️  GC metrics collected from {len(node_data)} brokers (cumulative values)"

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_gc': node_data,
                'cluster_aggregate': {
                    'avg_young_gc_time_ms': round(avg_young_gc, 2),
                    'avg_old_gc_time_ms': round(avg_old_gc, 2),
                    'avg_total_gc_time_ms': round(avg_total_gc, 2),
                    'broker_count': len(node_data),
                    'note': 'Values are cumulative since broker start - monitor trends over time'
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['young_gc_time', 'old_gc_time'],
                'broker_count': len(node_data),
                'metric_type': 'cumulative',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Generate AsciiDoc output
        builder.text(message)
        builder.blank()

        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Young GC Time: {round(avg_young_gc, 2)} ms (cumulative)")
        builder.text(f"- Avg Old GC Time: {round(avg_old_gc, 2)} ms (cumulative)")
        builder.text(f"- Avg Total GC Time: {round(avg_total_gc, 2)} ms (cumulative)")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker GC Metrics:*")
        for broker in sorted(node_data, key=lambda x: x['total_gc_time_ms'], reverse=True):
            builder.text(
                f"- Broker {broker['node_id']}: "
                f"Young {broker['young_gc_time_ms']}ms, "
                f"Old {broker['old_gc_time_ms']}ms, "
                f"Total {broker['total_gc_time_ms']}ms"
            )
        builder.blank()

        builder.text("*⚠️  IMPORTANT: GC Time Interpretation*")
        builder.text("These are cumulative values since broker start, not rates.")
        builder.text("To assess GC health properly:")
        builder.text("  • Monitor trends - rapid increases indicate memory pressure")
        builder.text("  • Calculate rate by comparing values over time")
        builder.text("  • Compare with heap usage metrics")
        builder.text("  • Look for correlation with latency spikes")
        builder.blank()

        # Always show recommendations for GC monitoring
        recommendations = {
            "general": [
                "GC Health Monitoring Best Practices:",
                "  • Monitor GC time rate (delta over time), not absolute values",
                "  • Typical healthy: < 5% time in Young GC, < 2% in Old GC",
                "  • Old (Full) GC is more concerning than Young GC",
                "  • Calculate rate: (current_value - previous_value) / time_elapsed",
                "",
                "Warning Signs:",
                "  • Rapidly increasing old GC time",
                "  • Frequent Full GC events (> 1 per minute)",
                "  • Long GC pause times (> 100ms for Young, > 1s for Old)",
                "  • Heap usage consistently > 80%",
                "  • Latency spikes correlating with GC events",
                "",
                "GC Tuning Recommendations:",
                "  • Use G1GC for heaps > 4GB (default in Kafka 2.0+)",
                "    JVM flag: -XX:+UseG1GC",
                "  • Set Xms = Xmx to avoid heap resizing overhead",
                "  • Keep heap < 32GB for compressed oops benefit",
                "  • Typical Kafka heap: 6-8 GB for production brokers",
                "  • For very large heaps, consider ZGC or Shenandoah (ultra-low pause)",
                "",
                "Root Cause Analysis:",
                "  • Check heap usage trends (use prometheus_jvm_heap check)",
                "  • Review partition count (more partitions = more memory)",
                "  • Analyze message sizes (large messages increase heap pressure)",
                "  • Check for memory leaks (heap grows but never shrinks)",
                "  • Review consumer fetch sizes (large fetches = heap spikes)",
                "",
                "Quick Fixes:",
                "  • Increase heap size if frequently hitting limits",
                "  • Reduce partition count per broker",
                "  • Enable compression to reduce memory usage",
                "  • Tune log.segment.bytes to reduce open file handles",
                "  • Review and optimize broker configuration",
                "",
                "Advanced Monitoring:",
                "  • Enable GC logging: -Xlog:gc*:file=gc.log",
                "  • Use JVM monitoring tools: JConsole, VisualVM, JMC",
                "  • Set up alerts on GC time rate (percentage)",
                "  • Monitor GC pause time distributions (P50, P99)",
                "  • Track GC event frequency (collections per second)"
            ]
        }

        builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"GC health check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

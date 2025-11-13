"""
Kafka Request Handler Saturation Check (Unified Adaptive)

Monitors request handler thread saturation using adaptive collection strategy.
Low handler idle percentage indicates broker overload.

Health Check: prometheus_request_handler
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- ic_node_request_handler_avg_idle_percent / kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_oneminuterate

IMPORTANCE:
- Low idle % means request handlers are saturated
- Indicates broker is overloaded and cannot process requests efficiently
- Can lead to increased latency and timeout errors
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


def check_prometheus_request_handler(connector, settings):
    """
    Check request handler saturation via adaptive collection strategy.

    Monitors:
    - Request handler idle percentage (inverted - low is bad)

    Thresholds:
    - WARNING: < 30% idle
    - CRITICAL: < 10% idle

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Request Handler Saturation (Prometheus)")

    try:
        # Get metric definition
        metric_def = get_metric_definition('request_handler_idle')
        if not metric_def:
            builder.error("❌ Request handler metric definition not found")
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
                "⚠️ Request handler idle metric not available\n\n"
                "*Why This Metric Is Unavailable:*\n\n"
                "The RequestHandlerAvgIdlePercent metric requires specific JMX exporter configuration.\n"
                "*Recommendation:*\n"
                "This is a LOW priority metric for most use cases. Request handler saturation "
                "is better monitored via produce/fetch latency metrics."
            )
            findings = {
                'status': 'skipped',
                'reason': 'Metric not available.',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus', 'jmx'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'note': 'Cluster JMX exporter configuration does not expose idle percentage gauge'
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

        # Get thresholds (NOTE: inverted - low values are bad)
        critical_threshold = settings.get('kafka_handler_idle_critical_pct', 10)
        warning_threshold = settings.get('kafka_handler_idle_warning_pct', 30)

        # Process broker data
        node_data = []
        critical_brokers = []
        warning_brokers = []

        for node_host, idle_pct in node_metrics.items():
            broker_entry = {
                'node_id': node_host,
                'host': node_host,
                'handler_idle_pct': round(idle_pct, 1)
            }
            node_data.append(broker_entry)

            # LOW values are bad for this metric
            if idle_pct < critical_threshold:
                critical_brokers.append(broker_entry)
            elif idle_pct < warning_threshold:
                warning_brokers.append(broker_entry)

        # Determine overall status
        if critical_brokers:
            status = 'critical'
            severity = 9
            message = f"🔴 {len(critical_brokers)} broker(s) with critical request handler saturation (<{critical_threshold}% idle)"
        elif warning_brokers:
            status = 'warning'
            severity = 7
            message = f"⚠️  {len(warning_brokers)} broker(s) with high request handler load (<{warning_threshold}% idle)"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ Request handlers healthy across {len(node_data)} brokers"

        # Calculate cluster aggregate
        avg_idle = result.get('cluster_avg', 0)

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_handler': node_data,
                'cluster_aggregate': {
                    'avg_handler_idle_pct': round(avg_idle, 1),
                    'brokers_critical': len(critical_brokers),
                    'brokers_warning': len(warning_brokers),
                    'broker_count': len(node_data),
                    'thresholds': {
                        'warning_pct': warning_threshold,
                        'critical_pct': critical_threshold
                    }
                },
                'collection_method': method
            },
            'metadata': {
                'source': method,
                'metrics': ['request_handler_idle_pct'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        if critical_brokers:
            findings['data']['critical_handler_saturation'] = {
                'count': len(critical_brokers),
                'brokers': critical_brokers,
                'recommendation': 'URGENT: Broker overloaded - scale up or distribute load'
            }

        if warning_brokers:
            findings['data']['warning_handler_saturation'] = {
                'count': len(warning_brokers),
                'brokers': warning_brokers,
                'recommendation': 'Monitor request handler load - plan capacity increase'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*Low idle % means request handlers are saturated - broker is overloaded*")
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*\n\n")
        builder.text(f"- Avg Handler Idle: {round(avg_idle, 1)}%")
        builder.text(f"- Saturated Brokers: {len(critical_brokers)} critical, {len(warning_brokers)} warning")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        builder.text("*Per-Broker Handler Idle:*")
        for broker in sorted(node_data, key=lambda x: x['handler_idle_pct']):
            status_icon = "🔴" if broker['handler_idle_pct'] < critical_threshold else "⚠️" if broker['handler_idle_pct'] < warning_threshold else "✅"
            builder.text(f"{status_icon} Broker {broker['node_id']}: {broker['handler_idle_pct']}% idle")
        builder.blank()

        if critical_brokers or warning_brokers:
            recommendations = {
                "critical" if critical_brokers else "high": [
                    "Request Handler Saturation Actions:\n\n",
                    "  * Scale up: Add more brokers to distribute load",
                    "  • Increase num.network.threads (default: 3, try 8-16)",
                    "  • Increase num.io.threads (default: 8, try 16-32)",
                    "  • Review client request patterns for inefficiencies",
                    "  • Check for slow storage causing request backlog",
                    "",
                    "Typical Causes:\n\n",
                    "  • Too many concurrent client connections",
                    "  • Large message sizes requiring more processing time",
                    "  • Slow disk I/O delaying request completion",
                    "  • Insufficient CPU resources"
                ],
                "general": [
                    "Prevention Best Practices:\n\n",
                    "  • Monitor handler idle % proactively",
                    "  • Alert at < 30% idle (warning), < 10% idle (critical)",
                    "  • Typical healthy value: > 50% idle",
                    "  • Plan capacity for 2x expected load"
                ]
            }
            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Request handler check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

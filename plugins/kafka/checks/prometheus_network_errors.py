"""
Kafka Network Errors Check (Unified Adaptive)

Monitors network errors using adaptive collection strategy.
Network errors indicate packet loss, corruption, or infrastructure issues.

Health Check: prometheus_network_errors
Collection Methods (in order of preference):
1. Instaclustr Prometheus API
2. Local Prometheus JMX Exporter (port 7500)
3. Standard JMX (port 9999)

Metrics:
- network_rx_errors, network_tx_errors, network_rx_drops, network_tx_drops

CRITICAL IMPORTANCE:
- Packet loss causes message corruption/loss
- Indicates NIC/switch/network infrastructure problems
- Can cause mysterious replication issues and data corruption
- Should always be 0 in healthy production
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


def check_prometheus_network_errors(connector, settings):
    """
    Check network errors via adaptive collection strategy.

    Monitors:
    - Network receive/transmit errors
    - Network receive/transmit drops

    Thresholds:
    - WARNING: Any errors/drops detected
    - CRITICAL: > 100 errors/drops

    Args:
        connector: Kafka connector with adaptive metric collection support
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Network Errors (Prometheus)")

    try:
        # Get metric definitions
        rx_err_def = get_metric_definition('network_rx_errors')
        tx_err_def = get_metric_definition('network_tx_errors')
        rx_drop_def = get_metric_definition('network_rx_drops')
        tx_drop_def = get_metric_definition('network_tx_drops')

        # Collect all four metrics
        rx_err_result = collect_metric_adaptive(rx_err_def, connector, settings) if rx_err_def else None
        tx_err_result = collect_metric_adaptive(tx_err_def, connector, settings) if tx_err_def else None
        rx_drop_result = collect_metric_adaptive(rx_drop_def, connector, settings) if rx_drop_def else None
        tx_drop_result = collect_metric_adaptive(tx_drop_def, connector, settings) if tx_drop_def else None

        if not any([rx_err_result, tx_err_result, rx_drop_result, tx_drop_result]):
            builder.warning(
                "⚠️ Could not collect network error metrics\n\n"
                "*Tried collection methods:*\n"
                "1. Instaclustr Prometheus API - Not configured or unavailable\n"
                "2. Local Prometheus JMX exporter - Not found or SSH unavailable\n"
                "3. Standard JMX - Not available or SSH unavailable\n\n"
                "*Note:* Network metrics not available via JMX"
            )
            findings = {
                'status': 'skipped',
                'reason': 'Unable to collect network error metrics',
                'data': [],
                'metadata': {
                    'attempted_methods': ['instaclustr_prometheus', 'local_prometheus'],
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Extract data
        method = (rx_err_result or tx_err_result or rx_drop_result or tx_drop_result).get('method')
        rx_err_metrics = rx_err_result.get('node_metrics', {}) if rx_err_result else {}
        tx_err_metrics = tx_err_result.get('node_metrics', {}) if tx_err_result else {}
        rx_drop_metrics = rx_drop_result.get('node_metrics', {}) if rx_drop_result else {}
        tx_drop_metrics = tx_drop_result.get('node_metrics', {}) if tx_drop_result else {}

        # Get thresholds
        warning_threshold = settings.get('kafka_network_errors_warning', 1)
        critical_threshold = settings.get('kafka_network_errors_critical', 100)

        # Combine broker data
        all_hosts = set(rx_err_metrics.keys()) | set(tx_err_metrics.keys()) | set(rx_drop_metrics.keys()) | set(tx_drop_metrics.keys())
        node_data = []
        critical_brokers = []
        warning_brokers = []

        for host in all_hosts:
            rx_errors = int(rx_err_metrics.get(host, 0))
            tx_errors = int(tx_err_metrics.get(host, 0))
            rx_drops = int(rx_drop_metrics.get(host, 0))
            tx_drops = int(tx_drop_metrics.get(host, 0))
            total_errors = rx_errors + tx_errors + rx_drops + tx_drops

            broker_entry = {
                'node_id': host,
                'host': host,
                'rx_errors': rx_errors,
                'tx_errors': tx_errors,
                'rx_drops': rx_drops,
                'tx_drops': tx_drops,
                'total_errors': total_errors
            }
            node_data.append(broker_entry)

            if total_errors >= critical_threshold:
                critical_brokers.append(broker_entry)
            elif total_errors >= warning_threshold:
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
            message = f"🔴 {len(critical_brokers)} broker(s) with critical network errors"
        elif warning_brokers:
            status = 'warning'
            severity = 8
            message = f"⚠️  {len(warning_brokers)} broker(s) with network errors"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ No network errors detected across {len(node_data)} brokers"

        # Calculate cluster aggregate
        total_errors = sum(b['total_errors'] for b in node_data)

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'data': {
                'per_broker_network': node_data,
                'cluster_aggregate': {
                    'total_errors_cluster': total_errors,
                    'brokers_with_errors': len(critical_brokers) + len(warning_brokers),
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
                'metrics': ['network_errors', 'network_drops'],
                'broker_count': len(node_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*🚨 NETWORK INFRASTRUCTURE ISSUES DETECTED 🚨*")
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        builder.text("*Cluster Summary:*")
        builder.text(f"- Total Errors/Drops: {total_errors}")
        builder.text(f"- Brokers with Issues: {len(critical_brokers) + len(warning_brokers)}/{len(node_data)}")
        builder.text(f"- Collection Method: {method}")
        builder.blank()

        if critical_brokers or warning_brokers:
            builder.text("*Brokers with Network Issues:*")
            for broker in critical_brokers + warning_brokers:
                symbol = "🔴" if broker in critical_brokers else "⚠️"
                builder.text(f"{symbol} Broker {broker['node_id']}:")
                builder.text(f"   RX Errors: {broker['rx_errors']}, TX Errors: {broker['tx_errors']}")
                builder.text(f"   RX Drops: {broker['rx_drops']}, TX Drops: {broker['tx_drops']}")
            builder.blank()

            recommendations = {
                "critical" if critical_brokers else "high": [
                    "Network errors indicate infrastructure problems:",
                    "  • Faulty NIC (network interface card)",
                    "  • Switch port errors",
                    "  • Cable issues",
                    "  • Network congestion",
                    "  • MTU mismatch",
                    "",
                    "Immediate Actions:",
                    "  1. Check NIC status: ethtool eth0",
                    "  2. Check for CRC errors: netstat -i",
                    "  3. Review switch port statistics",
                    "  4. Verify cable connections",
                    "  5. Check for duplex mismatch",
                    "",
                    "Impact on Kafka:",
                    "  • Message corruption/loss",
                    "  • Replication failures",
                    "  • Mysterious ISR shrinks",
                    "  • Client connection drops",
                    "  • Performance degradation"
                ],
                "general": [
                    "Prevention:",
                    "  • Use quality network hardware",
                    "  • Monitor network metrics proactively",
                    "  • Ensure proper MTU configuration",
                    "  • Separate replication traffic from client traffic if possible",
                    "  • Regular network infrastructure audits"
                ]
            }
            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Network errors check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': [],
            'metadata': {'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

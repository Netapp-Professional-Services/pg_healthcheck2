"""
Kafka Network Errors Check (Prometheus - Instaclustr)

Monitors network errors from Instaclustr Prometheus endpoints.
Network errors indicate packet loss, corruption, or infrastructure issues.

Health Check: prometheus_network_errors
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Metrics:
- ic_node_networkinerrorsdelta - Receive errors delta
- ic_node_networkouterrorsdelta - Transmit errors delta
- ic_node_networkindroppeddelta - Receive drops delta
- ic_node_networkoutdroppeddelta - Transmit drops delta

CRITICAL IMPORTANCE:
- Packet loss causes message corruption/loss
- Indicates NIC/switch/network infrastructure problems
- Can cause mysterious replication issues and data corruption
- Should always be 0 in healthy production
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def check_prometheus_network_errors(connector, settings):
    """
    Check network errors via Prometheus (Instaclustr managed service).

    Monitors:
    - Network receive/transmit errors
    - Network receive/transmit drops

    Thresholds:
    - WARNING: Any errors/drops detected
    - CRITICAL: > 100 errors/drops in measurement period

    Args:
        connector: Kafka connector
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Network Errors (Prometheus)")

    if not settings.get('instaclustr_prometheus_enabled'):
        findings = {
            'status': 'skipped',
            'reason': 'Prometheus monitoring not enabled',
            'data': [],
            'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}
        }
        return builder.build(), findings

    try:
        from plugins.common.prometheus_client import get_instaclustr_client

        client = get_instaclustr_client(
            cluster_id=settings['instaclustr_cluster_id'],
            username=settings['instaclustr_prometheus_username'],
            api_key=settings['instaclustr_prometheus_api_key'],
            prometheus_base_url=settings.get('instaclustr_prometheus_base_url')
        )

        all_metrics = client.scrape_all_nodes()

        if not all_metrics:
            builder.error("❌ No metrics available")
            return builder.build(), {'status': 'error', 'error_message': 'No metrics', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

        rx_err = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_networkinerrorsdelta$')
        tx_err = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_networkouterrorsdelta$')
        rx_drop = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_networkindroppeddelta$')
        tx_drop = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_networkoutdroppeddelta$')

        if not (rx_err or tx_err or rx_drop or tx_drop):
            builder.error("❌ Network error metrics not found")
            return builder.build(), {'status': 'error', 'error_message': 'Network metrics not found', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

        warning_threshold = settings.get('kafka_network_errors_warning', 1)
        critical_threshold = settings.get('kafka_network_errors_critical', 100)

        broker_data = {}

        for metric in rx_err:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id not in broker_data:
                broker_data[node_id] = {'node_id': node_id, 'public_ip': target_labels.get('PublicIp', 'unknown'), 'rack': target_labels.get('Rack', 'unknown')}
            broker_data[node_id]['rx_errors'] = int(metric['value'])

        for metric in tx_err:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['tx_errors'] = int(metric['value'])

        for metric in rx_drop:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['rx_drops'] = int(metric['value'])

        for metric in tx_drop:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['tx_drops'] = int(metric['value'])

        if not broker_data:
            builder.error("❌ No broker data")
            return builder.build(), {'status': 'error', 'error_message': 'No data', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

        node_data = list(broker_data.values())
        critical_brokers = []
        warning_brokers = []

        for broker in node_data:
            total_errors = broker.get('rx_errors', 0) + broker.get('tx_errors', 0) + broker.get('rx_drops', 0) + broker.get('tx_drops', 0)
            broker['total_errors'] = total_errors

            if total_errors >= critical_threshold:
                critical_brokers.append({'node_id': broker['node_id'], 'public_ip': broker['public_ip'], 'total_errors': total_errors, 'rx_errors': broker.get('rx_errors', 0), 'tx_errors': broker.get('tx_errors', 0), 'rx_drops': broker.get('rx_drops', 0), 'tx_drops': broker.get('tx_drops', 0)})
            elif total_errors >= warning_threshold:
                warning_brokers.append({'node_id': broker['node_id'], 'public_ip': broker['public_ip'], 'total_errors': total_errors, 'rx_errors': broker.get('rx_errors', 0), 'tx_errors': broker.get('tx_errors', 0), 'rx_drops': broker.get('rx_drops', 0), 'tx_drops': broker.get('tx_drops', 0)})

        if critical_brokers:
            status, severity = 'critical', 10
            message = f"🔴 {len(critical_brokers)} broker(s) with critical network errors"
        elif warning_brokers:
            status, severity = 'warning', 8
            message = f"⚠️  {len(warning_brokers)} broker(s) with network errors"
        else:
            status, severity = 'healthy', 0
            message = f"✅ No network errors detected across {len(node_data)} brokers"

        total_errors = sum(b['total_errors'] for b in node_data)

        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_network': {
                'status': status,
                'data': node_data,
                'metadata': {'source': 'prometheus', 'metrics': ['network_errors', 'network_drops'], 'broker_count': len(node_data)}
            },
            'cluster_aggregate': {
                'total_errors_cluster': total_errors,
                'brokers_with_errors': len(critical_brokers) + len(warning_brokers),
                'broker_count': len(node_data)
            }
        }

        if critical_brokers:
            findings['critical_network_errors'] = {'count': len(critical_brokers), 'brokers': critical_brokers, 'recommendation': 'URGENT: Network infrastructure issues - investigate NIC/switch/cabling'}
        if warning_brokers:
            findings['warning_network_errors'] = {'count': len(warning_brokers), 'brokers': warning_brokers, 'recommendation': 'Monitor network health - errors detected'}

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
        builder.blank()

        if critical_brokers or warning_brokers:
            builder.text("*Brokers with Network Issues:*")
            for broker in critical_brokers + warning_brokers:
                symbol = "🔴" if broker in critical_brokers else "⚠️"
                builder.text(f"{symbol} Broker {broker['node_id'][:8]} ({broker['public_ip']}):")
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
                    "  • Ensure proper MTU configuration (jumbo frames for cluster)",
                    "  • Separate replication traffic from client traffic if possible",
                    "  • Regular network infrastructure audits"
                ]
            }
            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus network errors check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

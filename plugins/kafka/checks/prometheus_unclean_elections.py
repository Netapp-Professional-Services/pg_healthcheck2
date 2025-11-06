"""
Kafka Unclean Leader Elections Check (Prometheus - Instaclustr)

Monitors unclean leader elections - a sign of DATA LOSS events.

Health Check: prometheus_unclean_elections
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

Metric: ic_node_unclean_leader_elections_kraft

CRITICAL IMPORTANCE:
- Unclean election = choosing leader with LESS data than followers
- This means PERMANENT DATA LOSS has occurred
- Should ALWAYS be 0 in production
- Triggered when all ISR replicas are unavailable and unclean.leader.election.enable=true
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    return 10  # Highest priority - indicates data loss


def check_prometheus_unclean_elections(connector, settings):
    builder = CheckContentBuilder()
    builder.h3("Unclean Leader Elections (Prometheus)")

    if not settings.get('instaclustr_prometheus_enabled'):
        return builder.build(), {'status': 'skipped', 'reason': 'Prometheus not enabled', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

    try:
        from plugins.common.prometheus_client import get_instaclustr_client
        client = get_instaclustr_client(cluster_id=settings['instaclustr_cluster_id'], username=settings['instaclustr_prometheus_username'], api_key=settings['instaclustr_prometheus_api_key'], prometheus_base_url=settings.get('instaclustr_prometheus_base_url'))
        all_metrics = client.scrape_all_nodes()

        if not all_metrics:
            builder.error("❌ No metrics available")
            return builder.build(), {'status': 'error', 'error_message': 'No metrics', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

        # Try both _kraft and non-kraft versions
        unclean_metrics = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_unclean_leader_elections(_kraft)?$')

        if not unclean_metrics:
            builder.error("❌ Unclean election metrics not found")
            return builder.build(), {'status': 'error', 'error_message': 'Metrics not found', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

        broker_data = {}
        for metric in unclean_metrics:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id not in broker_data:
                broker_data[node_id] = {'node_id': node_id, 'public_ip': target_labels.get('PublicIp', 'unknown'), 'rack': target_labels.get('Rack', 'unknown')}
            broker_data[node_id]['unclean_elections'] = int(metric['value'])

        if not broker_data:
            builder.error("❌ No broker data")
            return builder.build(), {'status': 'error', 'error_message': 'No data', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

        node_data = list(broker_data.values())
        brokers_with_unclean = [b for b in node_data if b.get('unclean_elections', 0) > 0]
        total_unclean = sum(b.get('unclean_elections', 0) for b in node_data)

        if total_unclean > 0:
            status, severity = 'critical', 10
            message = f"🔴 DATA LOSS: {total_unclean} unclean leader election(s) detected!"
        else:
            status, severity = 'healthy', 0
            message = f"✅ No unclean leader elections (checked {len(node_data)} brokers)"

        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_unclean': {'status': status, 'data': node_data, 'metadata': {'source': 'prometheus', 'metric': 'unclean_leader_elections', 'broker_count': len(node_data)}},
            'cluster_aggregate': {'total_unclean_elections': total_unclean, 'brokers_with_unclean': len(brokers_with_unclean), 'broker_count': len(node_data)}
        }

        if brokers_with_unclean:
            findings['brokers_with_data_loss'] = {'count': len(brokers_with_unclean), 'brokers': brokers_with_unclean, 'recommendation': 'DATA LOSS EVENT - review logs, check affected topics, notify stakeholders'}

        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*🚨 DATA LOSS EVENT DETECTED 🚨*")
            builder.blank()
            builder.text("Unclean leader election means a broker was elected leader WITH LESS DATA than other replicas.")
            builder.text("This results in PERMANENT DATA LOSS for messages that were not replicated.")
            builder.blank()
            builder.text("*Brokers with Unclean Elections:*")
            for broker in brokers_with_unclean:
                builder.text(f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}): {broker['unclean_elections']} unclean election(s)")
            builder.blank()

            recommendations = {
                "critical": [
                    "🚨 DATA LOSS EVENT - IMMEDIATE ACTION REQUIRED",
                    "",
                    "What Happened:",
                    "  • All in-sync replicas for a partition became unavailable",
                    "  • Kafka elected an out-of-sync replica as leader (to maintain availability)",
                    "  • Messages that were on the ISR but not on new leader are LOST",
                    "",
                    "Immediate Actions:",
                    "  1. Identify affected topics/partitions in broker logs",
                    "  2. Estimate data loss window (time between last ISR sync and election)",
                    "  3. Notify application teams of potential data loss",
                    "  4. Review why all ISR replicas were unavailable (cascading failure?)",
                    "  5. Check if disaster recovery/backup restore needed",
                    "",
                    "Investigation:",
                    "  • Check logs: grep 'Unclean leader election' /var/log/kafka/server.log",
                    "  • Review recent broker outages",
                    "  • Check if this was during maintenance",
                    "  • Verify replication factor is adequate (should be ≥3)",
                    "",
                    "Prevention:",
                    "  • Set unclean.leader.election.enable=false (sacrifice availability for durability)",
                    "  • Ensure replication factor ≥ 3 for all topics",
                    "  • Use min.insync.replicas=2 with acks=all for critical topics",
                    "  • Distribute replicas across racks/AZs",
                    "  • Never perform maintenance that takes down multiple replicas simultaneously"
                ],
                "general": [
                    "Understanding Unclean Elections:",
                    "  • Normal election: Choose leader from ISR (in-sync replicas)",
                    "  • Unclean election: ALL ISR unavailable, choose out-of-sync replica",
                    "  • Trade-off: Availability vs Durability",
                    "  • Default: unclean.leader.election.enable=false (Kafka 3.x+)"
                ]
            }
            builder.recs(recommendations)
        else:
            builder.success(message)
            builder.blank()
            builder.text("*Cluster Status:*")
            builder.text(f"- Total Unclean Elections: {total_unclean}")
            builder.text(f"- Brokers Monitored: {len(node_data)}")
            builder.blank()
            builder.text("No data loss from unclean elections detected. This is expected in healthy production.")

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Unclean elections check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

"""
Kafka Offline Partitions Check (Prometheus - Instaclustr)

Monitors offline partitions from Instaclustr Prometheus endpoints.
Offline partitions mean data is completely unavailable - CRITICAL issue.

Health Check: prometheus_offline_partitions
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

CRITICAL IMPORTANCE:
Offline partitions indicate that no replicas are available for a partition.
This means data is completely inaccessible and writes/reads will fail.
This is more severe than under-replicated partitions.

Metrics:
- ic_node_offline_partitions_kraft (KRaft mode)
- ic_node_offline_partitions (ZooKeeper mode - if available)
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10). Highest priority - offline data is critical."""
    return 10


def check_prometheus_offline_partitions(connector, settings):
    """
    Check for offline partitions via Prometheus (Instaclustr managed service).

    Monitors:
    - ic_node_offline_partitions_kraft

    Thresholds:
    - WARNING: > 0 offline partitions
    - CRITICAL: ANY offline partitions (data unavailable)

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Offline Partitions (Prometheus)")

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

        # Get cached Prometheus client (avoids rate limiting)
        client = get_instaclustr_client(
            cluster_id=settings['instaclustr_cluster_id'],
            username=settings['instaclustr_prometheus_username'],
            api_key=settings['instaclustr_prometheus_api_key'],
            prometheus_base_url=settings.get('instaclustr_prometheus_base_url')
        )

        # Scrape all metrics from service discovery
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

        # Extract offline partition metrics (try KRaft first, then ZooKeeper)
        offline_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^ic_node_offline_partitions_kraft$'
        )

        if not offline_metrics:
            # Try ZooKeeper mode metric
            offline_metrics = client.filter_metrics(
                all_metrics,
                name_pattern=r'^ic_node_offline_partitions$'
            )

        if not offline_metrics:
            builder.error("❌ Offline partition metrics not found")
            findings = {
                'status': 'error',
                'error_message': 'Offline partition metrics not found in Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Process metrics by broker
        broker_data = []
        brokers_with_offline = []
        total_offline = 0

        for metric in offline_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})

            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))
            public_ip = target_labels.get('PublicIp', 'unknown')
            rack = target_labels.get('Rack', 'unknown')
            datacenter = target_labels.get('ClusterDataCenterName', 'unknown')
            offline_count = int(metric['value'])

            broker_entry = {
                'node_id': node_id,
                'public_ip': public_ip,
                'rack': rack,
                'datacenter': datacenter,
                'offline_partitions': offline_count
            }

            broker_data.append(broker_entry)
            total_offline += offline_count

            if offline_count > 0:
                brokers_with_offline.append(broker_entry)

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

        # Determine severity - ANY offline partitions are critical
        if total_offline > 0:
            status = 'critical'
            severity = 10
            message = f"🔴 CRITICAL: {total_offline} offline partition(s) - data completely unavailable!"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ No offline partitions ({len(broker_data)} brokers checked)"

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_offline': {
                'status': status,
                'data': broker_data,
                'metadata': {
                    'source': 'prometheus',
                    'metric': 'ic_node_offline_partitions_kraft',
                    'broker_count': len(broker_data)
                }
            },
            'cluster_aggregate': {
                'total_offline_partitions': total_offline,
                'brokers_with_offline': len(brokers_with_offline),
                'total_brokers': len(broker_data)
            }
        }

        # Add affected brokers if any
        if brokers_with_offline:
            findings['affected_brokers'] = {
                'count': len(brokers_with_offline),
                'brokers': brokers_with_offline,
                'recommendation': 'URGENT: Offline partitions mean data is completely inaccessible - immediate action required'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*🚨 EMERGENCY - DATA UNAVAILABLE 🚨*")
            builder.blank()
            builder.text("Offline partitions indicate NO replicas are available.")
            builder.text("This is more severe than under-replicated partitions.")
            builder.text("Producers and consumers CANNOT access this data.")
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        # Cluster summary
        builder.text("*Cluster Summary:*")
        builder.text(f"- Total Offline Partitions: {total_offline}")
        builder.text(f"- Brokers with Offline Partitions: {len(brokers_with_offline)}/{len(broker_data)}")
        builder.blank()

        # Show affected brokers
        if brokers_with_offline:
            builder.text(f"*🚨 Affected Brokers ({len(brokers_with_offline)}):*")
            for broker in sorted(brokers_with_offline, key=lambda x: x['offline_partitions'], reverse=True):
                builder.text(
                    f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}) "
                    f"in {broker['rack']}: {broker['offline_partitions']} offline partition(s)"
                )
            builder.blank()

            # Critical recommendations
            recommendations = {
                "critical": [
                    "🚨 EMERGENCY RESPONSE REQUIRED 🚨",
                    "Offline partitions mean ALL replicas are down - data is completely inaccessible",
                    "Check if affected brokers are down or unreachable",
                    "Review broker logs immediately for crash/error messages",
                    "Check disk health - disk failure can cause offline partitions",
                    "Verify network connectivity to all brokers",
                    "Check if brokers were recently restarted or upgraded",
                    "Use kafka-topics.sh --describe to identify which topics are affected",
                    "If brokers are healthy, check for disk corruption or mount issues"
                ],
                "general": [
                    "Understanding Offline Partitions:",
                    "  • Offline = NO replicas available (vs under-replicated = SOME replicas missing)",
                    "  • Usually caused by:",
                    "    - All replicas on brokers that are down",
                    "    - Disk failures on all replica brokers",
                    "    - File system corruption",
                    "    - Network partition affecting all replicas",
                    "  • Impact:",
                    "    - Producers with acks=all will fail",
                    "    - Consumers cannot read data",
                    "    - Data may be permanently lost if replicas cannot recover",
                    "",
                    "Immediate Actions:",
                    "  1. Identify affected topics/partitions: kafka-topics.sh --describe --under-replicated-partitions",
                    "  2. Check broker status: verify all brokers are running",
                    "  3. Review broker logs: grep 'OFFLINE\\|FATAL\\|ERROR' /var/log/kafka/server.log",
                    "  4. Check disk health: df -h, smartctl -a /dev/sdX",
                    "  5. If brokers are healthy but partitions offline, check for:",
                    "     - Corrupted log segments",
                    "     - Permissions issues on log directories",
                    "     - Out of disk space",
                    "",
                    "Recovery Steps:",
                    "  1. If broker is down: restart it",
                    "  2. If disk is full: clean up or expand",
                    "  3. If corruption: may need to delete corrupted log segments (DATA LOSS)",
                    "  4. If all else fails: may need to recreate partition from scratch (TOTAL DATA LOSS)",
                    "",
                    "Prevention:",
                    "  • Use replication factor ≥ 3 for critical topics",
                    "  • Distribute replicas across racks/AZs",
                    "  • Monitor disk health proactively",
                    "  • Set up alerting for broker failures",
                    "  • Maintain sufficient disk space headroom",
                    "  • Use RAID for disk redundancy"
                ]
            }

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus offline partitions check failed: {e}", exc_info=True)
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

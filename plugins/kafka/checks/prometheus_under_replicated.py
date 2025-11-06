"""
Kafka Under-Replicated Partitions Check (Prometheus - Instaclustr)

Monitors under-replicated partitions from Instaclustr Prometheus endpoints.
Uses ic_node_under_replicated_partitions metric.

Health Check: prometheus_under_replicated
Source: Instaclustr Prometheus endpoints
Requires: instaclustr_prometheus_enabled: true

CRITICAL IMPORTANCE:
Under-replicated partitions indicate that one or more followers are not
in sync with the leader. This means data is at risk of loss if the leader fails.
ANY under-replicated partitions should be investigated immediately.
"""

import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10). Highest priority for critical metric."""
    return 10


def check_prometheus_under_replicated(connector, settings):
    """
    Check for under-replicated partitions via Prometheus (Instaclustr managed service).

    Monitors:
    - ic_node_under_replicated_partitions

    Thresholds:
    - WARNING: > 0 under-replicated partitions
    - CRITICAL: > 10 under-replicated partitions OR sustained for > 5 minutes

    Args:
        connector: Kafka connector (not used for Prometheus checks)
        settings: Configuration dictionary with Prometheus credentials

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Under-Replicated Partitions (Prometheus)")

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

        # Extract under-replicated partition metrics
        urp_metrics = client.filter_metrics(
            all_metrics,
            name_pattern=r'^ic_node_under_replicated_partitions$'
        )

        if not urp_metrics:
            builder.error("❌ Under-replicated partition metrics not found")
            findings = {
                'status': 'error',
                'error_message': 'Under-replicated partition metrics not found in Prometheus',
                'data': [],
                'metadata': {
                    'source': 'prometheus',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
            }
            return builder.build(), findings

        # Get thresholds
        warning_threshold = settings.get('kafka_urp_warning', 0)  # Any URPs are concerning
        critical_threshold = settings.get('kafka_urp_critical', 10)

        # Process metrics by broker
        broker_data = []
        brokers_with_urp = []
        total_urp = 0

        for metric in urp_metrics:
            labels = metric.get('labels', {})
            target_labels = metric.get('target_labels', {})

            node_id = target_labels.get('NodeId', labels.get('nodeID', 'unknown'))
            public_ip = target_labels.get('PublicIp', 'unknown')
            rack = target_labels.get('Rack', 'unknown')
            datacenter = target_labels.get('ClusterDataCenterName', 'unknown')
            urp_count = int(metric['value'])

            broker_entry = {
                'node_id': node_id,
                'public_ip': public_ip,
                'rack': rack,
                'datacenter': datacenter,
                'under_replicated_partitions': urp_count
            }

            broker_data.append(broker_entry)
            total_urp += urp_count

            if urp_count > 0:
                brokers_with_urp.append(broker_entry)

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

        # Determine severity
        if total_urp >= critical_threshold:
            status = 'critical'
            severity = 10
            message = f"🔴 CRITICAL: {total_urp} under-replicated partitions across cluster"
        elif total_urp > warning_threshold:
            status = 'warning'
            severity = 7
            message = f"⚠️  {total_urp} under-replicated partitions detected"
        else:
            status = 'healthy'
            severity = 0
            message = f"✅ No under-replicated partitions ({len(broker_data)} brokers checked)"

        # Build structured findings
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_urp': {
                'status': status,
                'data': broker_data,
                'metadata': {
                    'source': 'prometheus',
                    'metric': 'ic_node_under_replicated_partitions',
                    'broker_count': len(broker_data)
                }
            },
            'cluster_aggregate': {
                'total_under_replicated_partitions': total_urp,
                'brokers_with_urp': len(brokers_with_urp),
                'total_brokers': len(broker_data),
                'thresholds': {
                    'warning': warning_threshold,
                    'critical': critical_threshold
                }
            }
        }

        # Add affected brokers if any
        if brokers_with_urp:
            findings['affected_brokers'] = {
                'count': len(brokers_with_urp),
                'brokers': brokers_with_urp,
                'recommendation': 'Investigate replication lag, network issues, or broker performance problems'
            }

        # Generate AsciiDoc output
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("*⚠️  IMMEDIATE ACTION REQUIRED*")
            builder.blank()
            builder.text("Under-replicated partitions mean data is at risk of loss if the leader fails.")
            builder.text("This is the most critical Kafka health indicator and must be resolved immediately.")
            builder.blank()
        elif status == 'warning':
            builder.warning(message)
            builder.blank()
        else:
            builder.success(message)
            builder.blank()

        # Cluster summary
        builder.text("*Cluster Summary:*")
        builder.text(f"- Total Under-Replicated Partitions: {total_urp}")
        builder.text(f"- Brokers with URPs: {len(brokers_with_urp)}/{len(broker_data)}")
        builder.text(f"- Warning Threshold: >{warning_threshold}")
        builder.text(f"- Critical Threshold: >{critical_threshold}")
        builder.blank()

        # Show affected brokers
        if brokers_with_urp:
            builder.text(f"*⚠️  Affected Brokers ({len(brokers_with_urp)}):*")
            for broker in sorted(brokers_with_urp, key=lambda x: x['under_replicated_partitions'], reverse=True):
                builder.text(
                    f"- Broker {broker['node_id'][:8]} ({broker['public_ip']}) "
                    f"in {broker['rack']}: {broker['under_replicated_partitions']} URPs"
                )
            builder.blank()

            # Recommendations
            recommendations = {
                "critical" if total_urp >= critical_threshold else "high": [
                    "Check broker logs for errors or warnings about replication",
                    "Verify network connectivity between brokers",
                    "Check broker resource utilization (CPU, disk I/O, network)",
                    "Review partition distribution and rebalance if needed",
                    "Check for slow/dead replicas using kafka-topics.sh --under-replicated-partitions"
                ],
                "general": [
                    "Under-replicated partitions can be caused by:",
                    "  • Slow brokers (high CPU, disk I/O, or network saturation)",
                    "  • Network issues between brokers",
                    "  • Broker failures or restarts",
                    "  • Large message batches causing replication lag",
                    "  • Insufficient min.insync.replicas configuration",
                    "",
                    "Best Practices:",
                    "  • Set min.insync.replicas=2 for critical topics (requires replication factor ≥3)",
                    "  • Monitor replica.lag.time.max.ms (default 30s)",
                    "  • Keep replication traffic separate from client traffic if possible",
                    "  • Ensure brokers have sufficient resources for replication",
                    "  • Use racks/availability zones for broker placement"
                ]
            }

            builder.recs(recommendations)

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Prometheus under-replicated partitions check failed: {e}", exc_info=True)
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

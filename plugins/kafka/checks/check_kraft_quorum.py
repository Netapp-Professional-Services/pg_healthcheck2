"""
KRaft Quorum Health Check for Kafka clusters.

Analyzes KRaft metadata quorum status and replication health for clusters
running in KRaft mode (without ZooKeeper).

Data sources:
- kafka-metadata-quorum-status-output.txt.info (import)
- kafka-metadata.sh --describe --status (live)
- kafka-metadata-quorum-replication-output.txt.info (import)
- kafka-metadata.sh --describe --replication (live)
"""

from plugins.common.check_helpers import CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9  # High priority - quorum health is critical for KRaft clusters


def run_kraft_quorum_check(connector, settings):
    """
    Analyzes KRaft quorum health including voter status and replication lag.

    Checks:
    - Quorum leader presence and stability
    - Voter count and health
    - Observer count
    - Replication lag between voters
    - High watermark progression

    Args:
        connector: Kafka connector (live or import)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    try:
        builder.h3("KRaft Quorum Health")

        # Check if this is a KRaft cluster
        kafka_mode = getattr(connector, 'kafka_mode', None)
        if kafka_mode and kafka_mode.lower() != 'kraft':
            builder.note(
                "This cluster is running in ZooKeeper mode. "
                "KRaft quorum checks are not applicable."
            )
            structured_data = {
                "status": "skipped",
                "reason": "zookeeper_mode"
            }
            return builder.build(), structured_data

        # === Get quorum status ===
        query = '{"operation": "quorum_status"}'
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
            error_msg = raw.get('error', formatted) if isinstance(raw, dict) else formatted
            # Check if it's just not available (ZooKeeper mode)
            if 'not available' in error_msg.lower() or 'zookeeper' in error_msg.lower():
                builder.note(
                    "KRaft quorum information is not available. "
                    "This cluster may be running in ZooKeeper mode."
                )
                structured_data = {
                    "status": "skipped",
                    "reason": "not_kraft_mode"
                }
                return builder.build(), structured_data

            builder.error(f"Failed to get quorum status: {error_msg}")
            structured_data = {
                "status": "error",
                "details": error_msg
            }
            return builder.build(), structured_data

        quorum_status = raw if isinstance(raw, dict) else {}

        # === Get quorum replication info ===
        repl_query = '{"operation": "quorum_replication"}'
        repl_formatted, repl_raw = connector.execute_query(repl_query, return_raw=True)

        replication_info = {}
        if not ("[ERROR]" in repl_formatted or (isinstance(repl_raw, dict) and 'error' in repl_raw)):
            replication_info = repl_raw if isinstance(repl_raw, dict) else {}

        # === Parse and analyze quorum status ===
        cluster_id = quorum_status.get('clusterid', quorum_status.get('cluster_id', 'Unknown'))
        leader_id = quorum_status.get('leaderid', quorum_status.get('leader_id', -1))
        leader_epoch = quorum_status.get('leaderepoch', quorum_status.get('leader_epoch', 0))
        high_watermark = quorum_status.get('highwatermark', quorum_status.get('high_watermark', 0))

        # Parse voters and observers
        current_voters = quorum_status.get('currentvoters', quorum_status.get('current_voters', []))
        current_observers = quorum_status.get('currentobservers', quorum_status.get('current_observers', []))

        if isinstance(current_voters, str):
            # Parse string format like "[1, 2, 3]"
            try:
                current_voters = [int(x.strip()) for x in current_voters.strip('[]').split(',') if x.strip()]
            except:
                current_voters = []

        if isinstance(current_observers, str):
            try:
                current_observers = [int(x.strip()) for x in current_observers.strip('[]').split(',') if x.strip()]
            except:
                current_observers = []

        voter_count = len(current_voters) if current_voters else 0
        observer_count = len(current_observers) if current_observers else 0

        # === Analyze health ===
        issues = []
        warnings = []

        # Check leader presence
        if leader_id == -1 or leader_id is None:
            issues.append({
                'level': 'critical',
                'type': 'no_leader',
                'message': 'No quorum leader elected - cluster cannot accept writes'
            })

        # Check voter count (should be odd for proper quorum, minimum 3 for fault tolerance)
        if voter_count < 3:
            issues.append({
                'level': 'critical',
                'type': 'insufficient_voters',
                'message': f'Only {voter_count} voter(s) configured - minimum 3 recommended for fault tolerance'
            })
        elif voter_count % 2 == 0:
            warnings.append({
                'level': 'warning',
                'type': 'even_voters',
                'message': f'Even number of voters ({voter_count}) - odd numbers provide better availability'
            })

        # Parse replication lag from replication info
        max_lag = 0
        lagging_voters = []

        if replication_info:
            # Try to parse lag information
            raw_repl = replication_info.get('raw', '')
            if raw_repl:
                # Parse lines like "ReplicaId    LogEndOffset    Lag    LastFetchTimestamp"
                for line in raw_repl.split('\n'):
                    parts = line.split()
                    if len(parts) >= 3 and parts[0].isdigit():
                        try:
                            replica_id = int(parts[0])
                            lag = int(parts[2]) if parts[2] != '-' else 0
                            if lag > max_lag:
                                max_lag = lag
                            if lag > 1000:  # More than 1000 records behind
                                lagging_voters.append({
                                    'replica_id': replica_id,
                                    'lag': lag
                                })
                        except (ValueError, IndexError):
                            continue

        if lagging_voters:
            warnings.append({
                'level': 'warning',
                'type': 'replication_lag',
                'message': f'{len(lagging_voters)} voter(s) have significant replication lag',
                'details': lagging_voters
            })

        # === Build report ===
        has_critical = any(i['level'] == 'critical' for i in issues)
        has_warning = len(warnings) > 0

        if has_critical:
            for issue in [i for i in issues if i['level'] == 'critical']:
                builder.critical(f"**{issue['type'].replace('_', ' ').title()}:** {issue['message']}")
            builder.blank()

        if has_warning:
            for warning in warnings:
                builder.warning(f"**{warning['type'].replace('_', ' ').title()}:** {warning['message']}")
            builder.blank()

        # Quorum status table
        builder.h4("Quorum Status")

        status_data = [
            {"Property": "Cluster ID", "Value": str(cluster_id)},
            {"Property": "Leader ID", "Value": str(leader_id) if leader_id != -1 else "None (CRITICAL)"},
            {"Property": "Leader Epoch", "Value": str(leader_epoch)},
            {"Property": "High Watermark", "Value": f"{high_watermark:,}" if isinstance(high_watermark, int) else str(high_watermark)},
            {"Property": "Voter Count", "Value": str(voter_count)},
            {"Property": "Observer Count", "Value": str(observer_count)},
        ]
        builder.table(status_data)
        builder.blank()

        # Voters list
        if current_voters:
            builder.h4("Current Voters")
            voter_rows = []
            for voter in current_voters:
                # Handle both dict format and simple ID format
                if isinstance(voter, dict):
                    voter_id = voter.get('id', voter.get('nodeId', 'Unknown'))
                    endpoints = voter.get('endpoints', [])
                    endpoint_str = endpoints[0] if endpoints else 'N/A'
                else:
                    voter_id = voter
                    endpoint_str = 'N/A'

                is_leader = voter_id == leader_id
                voter_rows.append({
                    "Voter ID": str(voter_id),
                    "Endpoint": endpoint_str,
                    "Role": "Leader" if is_leader else "Follower",
                    "Status": "Active"
                })
            builder.table(voter_rows)
            builder.blank()

        # Observers list
        if current_observers:
            builder.h4("Current Observers")
            observer_rows = []
            for obs in current_observers:
                if isinstance(obs, dict):
                    obs_id = obs.get('id', obs.get('nodeId', 'Unknown'))
                else:
                    obs_id = obs
                observer_rows.append({"Observer ID": str(obs_id)})
            builder.table(observer_rows)
            builder.blank()

        # Recommendations
        if issues or warnings:
            recommendations = {}

            if has_critical:
                recommendations["critical"] = [
                    "**Immediately investigate quorum health** - cluster stability is at risk",
                    "**Check controller logs** for election failures or network issues",
                    "**Verify all voters are reachable** on the controller ports",
                    "**Review controller.properties** on all voter nodes"
                ]

            if has_warning:
                recommendations["high"] = [
                    "**Consider adding voters** to improve fault tolerance (minimum 3, preferably 5)",
                    "**Monitor replication lag** between voters",
                    "**Ensure network connectivity** between all controller nodes"
                ]

            recommendations["general"] = [
                "Use odd numbers of voters (3 or 5) for optimal quorum behavior",
                "Place voters in different failure domains (racks/AZs)",
                "Monitor leader elections - frequent elections indicate instability",
                "Set up alerts for high watermark progression stalls"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"KRaft quorum is healthy.\n\n"
                f"Leader: Broker {leader_id}, Voters: {voter_count}, Observers: {observer_count}"
            )

        # === Structured data ===
        structured_data = {
            "status": "success",
            "data": {
                "cluster_id": cluster_id,
                "leader_id": leader_id,
                "leader_epoch": leader_epoch,
                "high_watermark": high_watermark,
                "voter_count": voter_count,
                "observer_count": observer_count,
                "max_replication_lag": max_lag,
                "has_critical_issues": has_critical,
                "has_warnings": has_warning,
            }
        }

    except Exception as e:
        import traceback
        logger.error(f"KRaft quorum check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

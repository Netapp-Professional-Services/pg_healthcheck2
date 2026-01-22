"""
Internal Topic Replication Factor Check for Kafka.

Specifically audits internal Kafka topics (those starting with '_' or '__') for
replication factor compliance. Internal topics are critical for cluster operation:
- __consumer_offsets: Stores consumer group offsets
- __transaction_state: Stores transactional producer state
- __schema (if using Schema Registry): Stores schemas

These topics should have adequate replication factor to ensure cluster reliability.
"""

from plugins.common.check_helpers import CheckContentBuilder
from plugins.kafka.utils.qrylib.topic_config_queries import get_list_topics_query, get_topic_metadata_query
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8  # High priority - internal topics are critical for cluster operation


def _is_internal_topic(topic_name):
    """Check if a topic is an internal Kafka topic."""
    # Internal topics start with '_' (single) or '__' (double underscore)
    return topic_name.startswith('_')


def run_internal_topic_replication_check(connector, settings):
    """
    Audits internal Kafka topics for replication factor compliance.

    Internal topics are critical for cluster operation and should have
    adequate replication factor to ensure fault tolerance.

    Args:
        connector: Kafka connector with Admin API support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    try:
        # Get thresholds from settings
        min_replication_factor = settings.get('kafka_min_replication_factor', 3)
        min_internal_replication_factor = settings.get('kafka_min_internal_replication_factor', min_replication_factor)

        builder.h3("Internal Topic Replication Factor Check")
        builder.para("Auditing internal Kafka topics for replication factor compliance. "
                    "Internal topics are critical for cluster operation and require adequate replication.")
        builder.blank()

        # Get list of all topics
        query = get_list_topics_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted or not raw or not raw.get('topics'):
            if "[ERROR]" in formatted:
                builder.error(f"Failed to retrieve topic list: {formatted}")
                structured_data["internal_topic_replication"] = {"status": "error", "details": formatted}
            else:
                builder.note("No topics found in cluster.")
                structured_data["internal_topic_replication"] = {"status": "success", "topics_checked": 0, "data": []}
            return builder.build(), structured_data

        all_topics = raw.get('topics', [])
        
        # Filter for internal topics only
        internal_topics = [t for t in all_topics if _is_internal_topic(t)]
        
        if not internal_topics:
            builder.note("No internal topics found in cluster.")
            structured_data["internal_topic_replication"] = {
                "status": "success",
                "topics_checked": 0,
                "data": []
            }
            return builder.build(), structured_data

        builder.para(f"Found {len(internal_topics)} internal topic(s). Analyzing replication factor...")
        builder.blank()

        # Collect internal topic replication data
        all_topic_data = []
        topics_with_issues = []
        critical_issues = []
        warning_issues = []

        for topic_name in internal_topics:
            try:
                # Get topic metadata for replication factor
                meta_query = get_topic_metadata_query(connector, topic_name)
                meta_formatted, meta_raw = connector.execute_query(meta_query, return_raw=True)

                if "[ERROR]" in meta_formatted:
                    logger.warning(f"Could not get metadata for internal topic {topic_name}: {meta_formatted}")
                    continue

                # Parse metadata - describe_topics returns a list
                topic_metadata = {}
                if isinstance(meta_raw, list) and len(meta_raw) > 0:
                    topic_metadata = meta_raw[0]  # Get first (and only) topic
                elif isinstance(meta_raw, dict):
                    topic_metadata = meta_raw

                replication_factor = topic_metadata.get('replication_factor', -1)
                partition_count = topic_metadata.get('partitions', 0)
                under_replicated_partitions = topic_metadata.get('under_replicated_partitions', 0)

                # Analyze replication factor
                issues = []
                severity = None

                if replication_factor < 0:
                    issues.append({
                        'severity': 'warning',
                        'category': 'metadata',
                        'message': f"Could not determine replication factor"
                    })
                    severity = 'warning'
                elif replication_factor == 1:
                    issues.append({
                        'severity': 'critical',
                        'category': 'replication',
                        'message': f"Replication factor 1: NO FAULT TOLERANCE - broker failure causes data loss!"
                    })
                    severity = 'critical'
                elif replication_factor < min_internal_replication_factor:
                    issues.append({
                        'severity': 'critical' if replication_factor == 1 else 'warning',
                        'category': 'replication',
                        'message': f"Low replication factor: {replication_factor} (recommended: {min_internal_replication_factor}+ for internal topics)"
                    })
                    severity = 'warning' if replication_factor > 1 else 'critical'
                elif replication_factor == 2:
                    issues.append({
                        'severity': 'warning',
                        'category': 'replication',
                        'message': f"Replication factor 2: Limited fault tolerance - can only survive 1 broker failure"
                    })
                    severity = 'warning'

                # Check for under-replicated partitions
                if under_replicated_partitions > 0:
                    issues.append({
                        'severity': 'critical',
                        'category': 'under_replicated',
                        'message': f"Under-replicated partitions: {under_replicated_partitions} partition(s) are not fully replicated"
                    })
                    if severity != 'critical':
                        severity = 'critical'

                topic_data = {
                    'topic': topic_name,
                    'replication_factor': replication_factor,
                    'partition_count': partition_count,
                    'under_replicated_partitions': under_replicated_partitions,
                    'issues': issues,
                    'has_critical': any(i['severity'] == 'critical' for i in issues),
                    'has_warning': any(i['severity'] == 'warning' for i in issues)
                }
                all_topic_data.append(topic_data)

                if issues:
                    topics_with_issues.append(topic_name)
                    if any(i['severity'] == 'critical' for i in issues):
                        critical_issues.append(topic_data)
                    elif any(i['severity'] == 'warning' for i in issues):
                        warning_issues.append(topic_data)

            except Exception as e:
                logger.error(f"Error analyzing internal topic {topic_name}: {e}")

        # === REPORT ISSUES ===
        if critical_issues:
            builder.h4("Critical Issues")
            for topic_data in critical_issues:
                critical_msgs = [i['message'] for i in topic_data['issues'] if i['severity'] == 'critical']
                builder.critical_issue(
                    f"Internal Topic: {topic_data['topic']}",
                    {
                        "Replication Factor": str(topic_data['replication_factor']),
                        "Partitions": str(topic_data['partition_count']),
                        "Under-Replicated": str(topic_data['under_replicated_partitions']),
                        "Issues": "; ".join(critical_msgs)
                    }
                )
            builder.blank()

        if warning_issues:
            builder.h4("Warnings")
            for topic_data in warning_issues:
                warning_msgs = [i['message'] for i in topic_data['issues'] if i['severity'] == 'warning']
                builder.warning(f"**{topic_data['topic']}:** {'; '.join(warning_msgs)}")
            builder.blank()

        # === SUMMARY TABLE ===
        if all_topic_data:
            builder.h4("Internal Topics Summary")

            # Sort by issues (critical first, then warnings)
            sorted_topics = sorted(all_topic_data, key=lambda x: (not x['has_critical'], not x['has_warning'], x['topic']))

            table_lines = [
                "|===",
                "|Topic|Replication Factor|Partitions|Under-Replicated|Status"
            ]

            for topic_data in sorted_topics:
                indicator = "🔴" if topic_data['has_critical'] else "⚠️" if topic_data['has_warning'] else "✅"

                table_lines.append(
                    f"|{topic_data['topic']}|{topic_data['replication_factor']}|"
                    f"{topic_data['partition_count']}|{topic_data['under_replicated_partitions']}|{indicator}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === RECOMMENDATIONS ===
        if critical_issues or warning_issues:
            recommendations = {}

            if critical_issues:
                recommendations["critical"] = [
                    "**URGENT: Increase replication factor for internal topics**",
                    "Internal topics with RF=1 have no fault tolerance",
                    "Broker failure will cause permanent data loss for consumer offsets and transaction state",
                    "Use kafka-reassign-partitions tool to increase replication",
                    "Plan maintenance window for replica addition"
                ]

            if warning_issues:
                recommendations["high"] = [
                    "Increase replication factor to at least 3 for internal topics",
                    "RF=2 provides minimal fault tolerance - RF=3 is recommended",
                    "Internal topics are critical for cluster operation",
                    "Consider impact on consumer groups and transactional producers"
                ]

            recommendations["general"] = [
                "Internal topics should have replication factor ≥ 3 for production",
                "Common internal topics: __consumer_offsets, __transaction_state",
                "Low replication on internal topics can cause cluster-wide issues",
                "Monitor internal topic replication during cluster maintenance",
                "Set appropriate default.replication.factor at broker level (recommended: 3)"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"All {len(all_topic_data)} internal topic(s) have adequate replication factor.\n\n"
                "No critical issues or warnings detected."
            )

        # === STRUCTURED DATA ===
        structured_data["internal_topic_replication"] = {
            "status": "success",
            "topics_checked": len(all_topic_data),
            "topics_with_issues": len(topics_with_issues),
            "critical_count": len(critical_issues),
            "warning_count": len(warning_issues),
            "critical_topics": [t['topic'] for t in critical_issues],
            "warning_topics": [t['topic'] for t in warning_issues],
            "thresholds": {
                "min_internal_replication_factor": min_internal_replication_factor
            },
            "data": all_topic_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Internal topic replication check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["internal_topic_replication"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

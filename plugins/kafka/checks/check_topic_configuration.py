"""
Topic Configuration Audit check for Kafka.

Comprehensively audits topic-level configurations for best practices including:
- Retention policies (time and size based)
- Segment sizes
- Compression settings
- Min ISR configuration
- Cleanup policies
- Replication factor
"""

from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.parsers import _safe_int, _parse_size_to_bytes
from plugins.kafka.utils.qrylib.list_topics_queries import get_list_topics_query
from plugins.kafka.utils.qrylib.topic_config_queries import get_topic_config_query, get_topic_metadata_query
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def _parse_retention_ms(value):
    """Parse retention.ms value, handling -1 (infinite)."""
    if value is None:
        return None
    val_int = _safe_int(value, -1)
    return val_int


def _format_retention_time(ms):
    """Format retention time in ms to human-readable format."""
    if ms < 0:
        return "infinite"
    if ms < 60000:  # < 1 minute
        return f"{ms}ms"
    if ms < 3600000:  # < 1 hour
        return f"{ms/60000:.1f}m"
    if ms < 86400000:  # < 1 day
        return f"{ms/3600000:.1f}h"
    return f"{ms/86400000:.1f}d"


def run_topic_configuration_check(connector, settings):
    """
    Audits Kafka topic configurations for best practices.

    Analyzes topic-level settings including retention, compression,
    replication, ISR configuration, segment sizing, and cleanup policies.

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
        min_isr_warning = settings.get('kafka_min_isr_warning', 2)
        max_retention_days = settings.get('kafka_max_retention_days', 30)
        min_retention_hours = settings.get('kafka_min_retention_hours', 1)
        small_segment_mb = settings.get('kafka_small_segment_mb', 512)
        max_topics_to_detail = settings.get('kafka_max_topics_to_detail', 20)

        builder.h3("Topic Configuration Audit")
        builder.para("Auditing topic-level configurations for best practices and potential issues.")
        builder.blank()

        # Get list of topics
        query = get_list_topics_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted or not raw or not raw.get('topics'):
            if "[ERROR]" in formatted:
                builder.error(f"Failed to retrieve topic list: {formatted}")
                structured_data["topic_configuration"] = {"status": "error", "details": formatted}
            else:
                builder.note("No topics found in cluster.")
                structured_data["topic_configuration"] = {"status": "success", "topics_checked": 0, "data": []}
            return builder.build(), structured_data

        topics = raw.get('topics', [])
        builder.para(f"Analyzing configuration for {len(topics)} topic(s).")
        builder.blank()

        # Collect topic configuration issues
        all_topic_data = []
        topics_with_issues = []
        critical_issues = []
        warning_issues = []

        for topic_name in topics:
            if topic_name.startswith('_'):  # Skip internal topics
                continue

            # Skip provider-managed topics (Instaclustr, Confluent, etc.)
            # These are system/monitoring topics created by the managed service provider
            managed_topic_patterns = ['instaclustr-', 'confluent-', '_confluent', '__']
            if any(topic_name.startswith(pattern) for pattern in managed_topic_patterns):
                logger.info(f"Skipping provider-managed topic: {topic_name}")
                continue

            try:
                # Get topic configuration
                config_query = get_topic_config_query(connector, topic_name)
                config_formatted, config_raw = connector.execute_query(config_query, return_raw=True)

                if "[ERROR]" in config_formatted:
                    logger.warning(f"Could not get config for topic {topic_name}: {config_formatted}")
                    continue

                # Get topic metadata for replication factor
                meta_query = get_topic_metadata_query(connector, topic_name)
                meta_formatted, meta_raw = connector.execute_query(meta_query, return_raw=True)

                # Parse configuration
                configs = config_raw.get('configs', {}) if isinstance(config_raw, dict) else {}

                # Parse metadata - describe_topics returns a list
                topic_metadata = {}
                if isinstance(meta_raw, list) and len(meta_raw) > 0:
                    topic_metadata = meta_raw[0]  # Get first (and only) topic
                elif isinstance(meta_raw, dict):
                    topic_metadata = meta_raw

                # Extract configuration values
                retention_ms_str = configs.get('retention.ms', configs.get('log.retention.ms', '-1'))
                retention_ms = _parse_retention_ms(retention_ms_str)
                retention_bytes_str = configs.get('retention.bytes', configs.get('log.retention.bytes', '-1'))
                retention_bytes = _safe_int(retention_bytes_str, -1)

                segment_bytes_str = configs.get('segment.bytes', configs.get('log.segment.bytes', '1073741824'))
                segment_bytes = _safe_int(segment_bytes_str, 1073741824)
                segment_ms_str = configs.get('segment.ms', configs.get('log.segment.ms', '604800000'))
                segment_ms = _safe_int(segment_ms_str, 604800000)

                compression_type = configs.get('compression.type', 'producer')
                cleanup_policy = configs.get('cleanup.policy', 'delete')
                min_insync_replicas_str = configs.get('min.insync.replicas', '1')
                min_insync_replicas = _safe_int(min_insync_replicas_str, 1)

                max_message_bytes_str = configs.get('max.message.bytes', '1048588')
                max_message_bytes = _safe_int(max_message_bytes_str, 1048588)

                # Get replication factor and partition count from metadata
                replication_factor = topic_metadata.get('replication_factor',-1)
                partition_count = topic_metadata.get('partitions', 0)

                # Analyze configuration
                issues = []

                # Check replication factor
                if replication_factor < min_replication_factor:
                    issues.append({
                        'severity': 'critical' if replication_factor == 1 else 'warning',
                        'category': 'replication',
                        'message': f"Low replication factor: {replication_factor} (recommended: {min_replication_factor}+)"
                    })

                # Check min.insync.replicas
                if min_insync_replicas >= replication_factor:
                    issues.append({
                        'severity': 'critical',
                        'category': 'min_isr',
                        'message': f"min.insync.replicas ({min_insync_replicas}) >= replication.factor ({replication_factor}) - WILL BLOCK WRITES!"
                    })
                elif min_insync_replicas < min_isr_warning and replication_factor > 1:
                    issues.append({
                        'severity': 'warning',
                        'category': 'min_isr',
                        'message': f"Low min.insync.replicas: {min_insync_replicas} (recommended: {min_isr_warning} for RF={replication_factor})"
                    })

                # Check retention settings
                if retention_ms > 0:
                    retention_days = retention_ms / 86400000.0
                    if retention_days > max_retention_days:
                        issues.append({
                            'severity': 'warning',
                            'category': 'retention',
                            'message': f"Long retention: {_format_retention_time(retention_ms)} (>{max_retention_days}d may increase storage costs)"
                        })
                    retention_hours = retention_ms / 3600000.0
                    if retention_hours < min_retention_hours:
                        issues.append({
                            'severity': 'info',
                            'category': 'retention',
                            'message': f"Short retention: {_format_retention_time(retention_ms)} (<{min_retention_hours}h may cause data loss)"
                        })

                # Check segment size
                segment_mb = segment_bytes / (1024 * 1024)
                if segment_mb < small_segment_mb:
                    issues.append({
                        'severity': 'info',
                        'category': 'segment',
                        'message': f"Small segment size: {segment_mb:.0f}MB (may cause excessive file handles)"
                    })

                # Check compression
                if compression_type == 'producer' or compression_type == 'uncompressed':
                    issues.append({
                        'severity': 'info',
                        'category': 'compression',
                        'message': f"No broker-side compression ({compression_type})"
                    })

                # Check cleanup policy for compacted topics
                if cleanup_policy == 'compact' and retention_ms > 0 and retention_ms < 86400000:
                    issues.append({
                        'severity': 'warning',
                        'category': 'cleanup',
                        'message': f"Compacted topic with short retention ({_format_retention_time(retention_ms)})"
                    })

                topic_data = {
                    'topic': topic_name,
                    'replication_factor': replication_factor,
                    'partition_count': partition_count,
                    'min_insync_replicas': min_insync_replicas,
                    'retention_ms': retention_ms,
                    'retention_bytes': retention_bytes,
                    'retention_formatted': _format_retention_time(retention_ms),
                    'segment_bytes': segment_bytes,
                    'segment_mb': round(segment_mb, 0),
                    'segment_ms': segment_ms,
                    'compression_type': compression_type,
                    'cleanup_policy': cleanup_policy,
                    'max_message_bytes': max_message_bytes,
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
                logger.error(f"Error analyzing topic {topic_name}: {e}")

        # === REPORT ISSUES ===
        if critical_issues:
            builder.h4("Critical Configuration Issues")
            for topic_data in critical_issues[:max_topics_to_detail]:
                critical_msgs = [i['message'] for i in topic_data['issues'] if i['severity'] == 'critical']
                builder.critical_issue(
                    f"Topic: {topic_data['topic']}",
                    {
                        "Replication Factor": str(topic_data['replication_factor']),
                        "Min ISR": str(topic_data['min_insync_replicas']),
                        "Issues": ", ".join(critical_msgs)
                    }
                )
            if len(critical_issues) > max_topics_to_detail:
                builder.para(f"_... and {len(critical_issues) - max_topics_to_detail} more topics with critical issues_")
            builder.blank()

        if warning_issues:
            builder.h4("Configuration Warnings")
            for topic_data in warning_issues[:max_topics_to_detail]:
                warning_msgs = [i['message'] for i in topic_data['issues'] if i['severity'] == 'warning']
                builder.warning(f"**{topic_data['topic']}:** {'; '.join(warning_msgs)}")
            if len(warning_issues) > max_topics_to_detail:
                builder.para(f"_... and {len(warning_issues) - max_topics_to_detail} more topics with warnings_")
            builder.blank()

        # === SUMMARY TABLE ===
        if all_topic_data:
            builder.h4("Topic Configuration Summary")

            # Sort by issues (critical first, then warnings)
            sorted_topics = sorted(all_topic_data, key=lambda x: (not x['has_critical'], not x['has_warning'], x['topic']))

            table_lines = [
                "|===",
                "|Topic|RF|Min ISR|Retention|Compression|Status"
            ]

            for topic_data in sorted_topics[:max_topics_to_detail]:
                indicator = "🔴" if topic_data['has_critical'] else "⚠️" if topic_data['has_warning'] else "✅"

                table_lines.append(
                    f"|{topic_data['topic']}|{topic_data['replication_factor']}|{topic_data['min_insync_replicas']}|"
                    f"{topic_data['retention_formatted']}|{topic_data['compression_type']}|{indicator}"
                )

            if len(all_topic_data) > max_topics_to_detail:
                table_lines.append(f"|_... and {len(all_topic_data) - max_topics_to_detail} more topics_|||||")
            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === RECOMMENDATIONS ===
        if critical_issues or warning_issues:
            recommendations = {}

            if critical_issues:
                recommendations["critical"] = [
                    "**Fix min.insync.replicas immediately:** Values >= RF will block all writes",
                    "**Increase replication factor:** Single-replica topics risk data loss",
                    "**Review critical misconfigurations:** Can cause service outages or data loss"
                ]

            if warning_issues:
                recommendations["high"] = [
                    "Review replication factor for non-critical topics",
                    "Optimize retention policies to balance storage costs and requirements",
                    "Consider enabling compression to reduce disk and network usage",
                    "Standardize topic configurations using defaults where appropriate"
                ]

            recommendations["general"] = [
                "Use topic configuration templates for consistency",
                "Document rationale for non-standard configurations",
                "Review topic configs during capacity planning",
                "Set appropriate default.replication.factor at broker level (recommended: 3)",
                "Consider min.insync.replicas = RF - 1 for durability",
                "Enable compression (lz4, snappy, zstd) to save disk and bandwidth",
                "Typical production settings: RF=3, min.insync.replicas=2, retention=7d"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"All {len(all_topic_data)} topic configurations follow best practices.\n\n"
                "No critical issues or warnings detected."
            )

        # === STRUCTURED DATA ===
        structured_data["topic_configuration"] = {
            "status": "success",
            "topics_checked": len(all_topic_data),
            "topics_with_issues": len(topics_with_issues),
            "critical_count": len(critical_issues),
            "warning_count": len(warning_issues),
            "critical_topics": [t['topic'] for t in critical_issues],
            "warning_topics": [t['topic'] for t in warning_issues],
            "thresholds": {
                "min_replication_factor": min_replication_factor,
                "min_isr_warning": min_isr_warning,
                "max_retention_days": max_retention_days
            },
            "data": all_topic_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Topic configuration check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["topic_configuration"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data

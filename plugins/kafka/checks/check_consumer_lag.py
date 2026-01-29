"""
Consumer Lag Analysis check for Kafka clusters.

Analyzes consumer lag across all consumer groups to identify
groups falling behind and potential data processing issues.
"""

from plugins.common.check_helpers import CheckContentBuilder
from plugins.kafka.utils.qrylib.consumer_lag_queries import get_all_consumer_lag_query
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8  # High priority - consumer lag is critical for data processing health


def run_consumer_lag(connector, settings):
    """
    Analyzes consumer lag across all consumer groups.

    Checks:
    - Per-partition lag for each consumer group
    - Total lag per consumer group
    - Critical lag thresholds that may cause data loss

    Args:
        connector: Kafka connector
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Get thresholds from settings
    warning_lag = settings.get('kafka_lag_warning', 1000)
    critical_lag = settings.get('kafka_lag_critical', 10000)

    try:
        builder.h3("Consumer Lag Analysis")
        builder.para(
            f"Analyzing consumer group lag. Thresholds: warning={warning_lag:,}, critical={critical_lag:,} messages."
        )
        builder.blank()

        query = get_all_consumer_lag_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            builder.error(f"Failed to retrieve consumer lag data: {formatted}")
            structured_data["consumer_lag"] = {"status": "error", "data": raw}
            return builder.build(), structured_data

        if not raw or not raw.get('group_lags'):
            builder.note(
                "No consumer groups or lag data available.\n\n"
                "This could mean:\n\n"
                "* No consumer groups exist in the cluster\n"
                "* Consumer groups have no committed offsets\n"
                "* Consumer groups are reading from topics with no data"
            )
            structured_data["consumer_lag"] = {"status": "success", "data": [], "count": 0}
            return builder.build(), structured_data

        # Analyze lag data
        group_lags = raw.get('group_lags', [])

        # Aggregate by group
        group_totals = {}
        group_max = {}
        group_partitions = {}

        for item in group_lags:
            group_id = item.get('group_id')
            lag = item.get('lag', 0)
            topic = item.get('topic', 'unknown')
            partition = item.get('partition', 0)

            if group_id not in group_totals:
                group_totals[group_id] = 0
                group_max[group_id] = 0
                group_partitions[group_id] = []

            group_totals[group_id] += lag
            group_max[group_id] = max(group_max[group_id], lag)
            group_partitions[group_id].append({
                'topic': topic,
                'partition': partition,
                'lag': lag
            })

        # Categorize groups by severity
        critical_groups = []
        warning_groups = []
        healthy_groups = []

        for group_id, total_lag in group_totals.items():
            group_data = {
                'group_id': group_id,
                'total_lag': total_lag,
                'max_partition_lag': group_max[group_id],
                'partition_count': len(group_partitions[group_id])
            }

            if total_lag > critical_lag:
                critical_groups.append(group_data)
            elif total_lag > warning_lag:
                warning_groups.append(group_data)
            else:
                healthy_groups.append(group_data)

        # Sort by lag (highest first)
        critical_groups.sort(key=lambda x: x['total_lag'], reverse=True)
        warning_groups.sort(key=lambda x: x['total_lag'], reverse=True)

        # Report findings
        has_critical = len(critical_groups) > 0
        has_warning = len(warning_groups) > 0

        if has_critical:
            builder.critical(
                f"**Critical Consumer Lag:** {len(critical_groups)} consumer group(s) have lag exceeding {critical_lag:,} messages.\n\n"
                "Data may be lost if retention period expires before consumers catch up."
            )
            builder.blank()

        if has_warning:
            builder.warning(
                f"**High Consumer Lag:** {len(warning_groups)} consumer group(s) have lag exceeding {warning_lag:,} messages."
            )
            builder.blank()

        # Critical groups detail table
        if critical_groups:
            builder.h4("Critical Lag Consumer Groups")

            critical_rows = []
            for group in critical_groups[:20]:  # Limit to top 20
                critical_rows.append({
                    "Status": "Critical",
                    "Consumer Group": group['group_id'],
                    "Total Lag": f"{group['total_lag']:,}",
                    "Max Partition Lag": f"{group['max_partition_lag']:,}",
                    "Partitions": str(group['partition_count'])
                })

            builder.table(critical_rows)
            if len(critical_groups) > 20:
                builder.para(f"_... and {len(critical_groups) - 20} more critical groups_")
            builder.blank()

        # Warning groups table
        if warning_groups:
            builder.h4("Warning Lag Consumer Groups")

            warning_rows = []
            for group in warning_groups[:20]:  # Limit to top 20
                warning_rows.append({
                    "Status": "Warning",
                    "Consumer Group": group['group_id'],
                    "Total Lag": f"{group['total_lag']:,}",
                    "Max Partition Lag": f"{group['max_partition_lag']:,}",
                    "Partitions": str(group['partition_count'])
                })

            builder.table(warning_rows)
            if len(warning_groups) > 20:
                builder.para(f"_... and {len(warning_groups) - 20} more warning groups_")
            builder.blank()

        # Summary
        builder.h4("Consumer Lag Summary")

        summary_data = [
            {"Metric": "Total Consumer Groups", "Value": str(len(group_totals))},
            {"Metric": "Critical Lag Groups", "Value": str(len(critical_groups))},
            {"Metric": "Warning Lag Groups", "Value": str(len(warning_groups))},
            {"Metric": "Healthy Groups", "Value": str(len(healthy_groups))},
            {"Metric": "Critical Threshold", "Value": f"{critical_lag:,} messages"},
            {"Metric": "Warning Threshold", "Value": f"{warning_lag:,} messages"},
        ]
        builder.table(summary_data)
        builder.blank()

        # Recommendations
        if has_critical or has_warning:
            recommendations = {}

            if has_critical:
                recommendations["critical"] = [
                    "**Immediately investigate consumer health** - check if consumers are running",
                    "**Verify consumer processing capacity** - are consumers overwhelmed?",
                    "**Check for slow processing** - profile consumer message handling logic",
                    "**Review consumer errors** - look for exceptions blocking processing",
                    "**Check topic retention** - ensure messages won't expire before consumption",
                    "**Scale consumers horizontally** - add more consumer instances to the group"
                ]

            if has_warning:
                recommendations["high"] = [
                    "**Monitor lag trends** - is lag increasing or stabilizing?",
                    "**Review consumer throughput** - compare with producer rate",
                    "**Optimize message processing** - reduce per-message processing time",
                    "**Check for consumer rebalancing** - frequent rebalances cause lag spikes",
                    "**Review batch processing settings** - `max.poll.records` and `fetch.max.bytes`"
                ]

            recommendations["general"] = [
                "Set up alerts for consumer lag at warning and critical thresholds",
                "Monitor consumer offset commit rate alongside lag",
                "Implement consumer health checks in your application",
                "Use consumer group monitoring tools (Burrow, kafka-lag-exporter)",
                "Consider increasing partition count if consumers can't keep up",
                "Review `max.poll.interval.ms` to prevent consumer eviction",
                "Track lag per partition to identify hot partitions"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"All consumer groups are healthy.\n\n"
                f"{len(healthy_groups)} consumer group(s) have lag within acceptable thresholds."
            )

        # Structured data
        structured_data["consumer_lag"] = {
            "status": "success",
            "total_groups": len(group_totals),
            "critical_lag_groups": len(critical_groups),
            "high_lag_groups": len(warning_groups),
            "healthy_groups": len(healthy_groups),
            "thresholds": {
                "warning": warning_lag,
                "critical": critical_lag
            },
            "critical_groups": critical_groups,
            "warning_groups": warning_groups,
            "max_total_lag": max(group_totals.values()) if group_totals else 0,
            "max_lag_group": max(group_totals, key=group_totals.get) if group_totals else None,
            "data": group_lags,
            "count": len(group_lags)
        }

    except Exception as e:
        import traceback
        logger.error(f"Consumer lag check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["consumer_lag"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data

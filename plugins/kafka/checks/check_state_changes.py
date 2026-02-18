"""
State Changes Log Analysis for Kafka clusters.

Analyzes state-change.log files to detect:
- Partition leadership changes
- Replica state transitions
- Potential instability patterns

Data sources:
- state-change.log* files (import and live via SSH)
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 6  # Medium-high priority


def run_state_changes_check(connector, settings):
    """
    Analyzes Kafka state change logs for partition leadership churn.

    Checks:
    - Leadership change frequency
    - ISR shrink/expand events
    - Offline partition events
    - Broker shutdown/startup patterns

    Args:
        connector: Kafka connector (live or import)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "State changes analysis")
    if not available:
        return skip_msg, skip_data

    try:
        builder.h3("Partition State Changes Analysis")

        # Get thresholds
        leadership_change_warning = settings.get('kafka_leadership_change_warning', 10)
        leadership_change_critical = settings.get('kafka_leadership_change_critical', 50)

        # === Collect state change logs from all brokers ===
        all_events = []
        broker_events = {}
        errors = []

        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Read state-change.log
                command = "cat /var/log/kafka/state-change.log* 2>/dev/null || cat /opt/kafka/logs/state-change.log* 2>/dev/null || echo ''"
                results = connector.execute_ssh_on_all_hosts(command, "Read state change logs")

                for result in results:
                    if result.get('host') == ssh_host and result.get('success'):
                        log_content = result.get('output', '')
                        if log_content.strip():
                            events = _parse_state_change_log(log_content, broker_id)
                            broker_events[broker_id] = events
                            all_events.extend(events)

            except Exception as e:
                logger.warning(f"Failed to read state change logs from {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        if not all_events:
            builder.note(
                "No state change events found in logs. This could mean:\n\n"
                "* State change logging is not enabled\n"
                "* Log files are rotated or empty\n"
                "* Cluster is very stable with no recent changes"
            )
            structured_data = {
                "status": "success",
                "data": {
                    "total_events": 0,
                    "leadership_changes": 0,
                    "isr_shrinks": 0,
                    "isr_expands": 0,
                    "offline_events": 0,
                    "has_critical_issues": False,
                    "has_warnings": False,
                }
            }
            return builder.build(), structured_data

        # === Analyze events ===
        analysis = _analyze_state_changes(all_events)

        # === Build report ===
        total_events = len(all_events)
        leadership_changes = analysis['leadership_changes']
        isr_shrinks = analysis['isr_shrinks']
        isr_expands = analysis['isr_expands']
        offline_events = analysis['offline_events']

        issues = []
        warnings = []

        # Check leadership change frequency
        if leadership_changes >= leadership_change_critical:
            issues.append({
                'level': 'critical',
                'type': 'excessive_leadership_changes',
                'message': f'{leadership_changes} leadership changes detected - indicates cluster instability'
            })
        elif leadership_changes >= leadership_change_warning:
            warnings.append({
                'level': 'warning',
                'type': 'high_leadership_changes',
                'message': f'{leadership_changes} leadership changes detected - monitor for patterns'
            })

        # Check for offline partitions
        if offline_events > 0:
            issues.append({
                'level': 'critical',
                'type': 'offline_partitions',
                'message': f'{offline_events} offline partition event(s) detected'
            })

        # Check ISR health
        if isr_shrinks > isr_expands * 2:
            warnings.append({
                'level': 'warning',
                'type': 'isr_shrink_trend',
                'message': f'ISR shrinks ({isr_shrinks}) significantly exceed expands ({isr_expands})'
            })

        has_critical = any(i['level'] == 'critical' for i in issues)
        has_warning = len(warnings) > 0

        if has_critical:
            for issue in issues:
                builder.critical(f"**{issue['type'].replace('_', ' ').title()}:** {issue['message']}")
            builder.blank()

        if has_warning:
            for warning in warnings:
                builder.warning(f"**{warning['type'].replace('_', ' ').title()}:** {warning['message']}")
            builder.blank()

        # Summary table
        builder.h4("State Change Summary")

        summary_data = [
            {"Event Type": "Leadership Changes", "Count": str(leadership_changes), "Status": "Critical" if leadership_changes >= leadership_change_critical else ("Warning" if leadership_changes >= leadership_change_warning else "OK")},
            {"Event Type": "ISR Shrinks", "Count": str(isr_shrinks), "Status": "Warning" if isr_shrinks > 10 else "OK"},
            {"Event Type": "ISR Expands", "Count": str(isr_expands), "Status": "OK"},
            {"Event Type": "Offline Events", "Count": str(offline_events), "Status": "Critical" if offline_events > 0 else "OK"},
            {"Event Type": "Total Events", "Count": str(total_events), "Status": "-"},
        ]
        builder.table(summary_data)
        builder.blank()

        # Top affected topics
        if analysis['topic_changes']:
            builder.h4("Most Affected Topics")

            topic_rows = []
            sorted_topics = sorted(analysis['topic_changes'].items(), key=lambda x: x[1], reverse=True)[:10]
            for topic, count in sorted_topics:
                topic_rows.append({
                    "Topic": topic,
                    "State Changes": str(count),
                    "Status": "High" if count > 10 else "Normal"
                })

            builder.table(topic_rows)
            builder.blank()

        # Per-broker breakdown
        if broker_events:
            builder.h4("Events by Broker")

            broker_rows = []
            for broker_id, events in sorted(broker_events.items()):
                broker_rows.append({
                    "Broker": str(broker_id),
                    "Events": str(len(events))
                })

            builder.table(broker_rows)
            builder.blank()

        # Errors
        if errors:
            builder.h4("Collection Errors")
            builder.warning(
                f"Could not collect state change logs from {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']}: {e['error']}" for e in errors])
            )

        # Recommendations
        if issues or warnings:
            recommendations = {}

            if has_critical:
                recommendations["critical"] = [
                    "**Immediately investigate offline partition events** - these cause data unavailability",
                    "**Check broker health** - excessive leadership changes indicate instability",
                    "**Review network connectivity** between brokers",
                    "**Check broker logs** for errors or warnings"
                ]

            if has_warning:
                recommendations["high"] = [
                    "**Monitor leadership change trends** over time",
                    "**Review under-replicated partitions** that may be causing ISR changes",
                    "**Check broker resource utilization** (CPU, memory, disk I/O)"
                ]

            recommendations["general"] = [
                "Enable and monitor state change logging for visibility into cluster health",
                "Set up alerts for leadership changes exceeding normal thresholds",
                "Regular review of state change patterns can reveal issues before they become critical",
                "Consider increasing `replica.lag.time.max.ms` if ISR churn is network-related"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Partition state is stable.\n\n"
                f"Analyzed {total_events} events with no concerning patterns detected."
            )

        # === Structured data ===
        structured_data = {
            "status": "success",
            "data": {
                "total_events": total_events,
                "leadership_changes": leadership_changes,
                "isr_shrinks": isr_shrinks,
                "isr_expands": isr_expands,
                "offline_events": offline_events,
                "has_critical_issues": has_critical,
                "has_warnings": has_warning,
            }
        }

    except Exception as e:
        import traceback
        logger.error(f"State changes check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data


def _parse_state_change_log(content, broker_id):
    """
    Parse state-change.log content into structured events.

    Example log lines:
    [2024-01-15 10:30:45,123] INFO [Partition topic-1-0 broker=1] ... (kafka.cluster.Partition)
    [2024-01-15 10:30:46,456] INFO Partition [topic-1,0] ... changing leader from 1 to 2
    """
    events = []

    for line in content.split('\n'):
        line = line.strip()
        if not line:
            continue

        event = {
            'broker_id': broker_id,
            'raw': line,
            'type': 'unknown'
        }

        # Try to extract timestamp
        timestamp_match = re.match(r'\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
        if timestamp_match:
            event['timestamp'] = timestamp_match.group(1)

        # Classify event type
        line_lower = line.lower()

        if 'leader' in line_lower and ('change' in line_lower or 'from' in line_lower):
            event['type'] = 'leadership_change'
        elif 'isr' in line_lower and 'shrink' in line_lower:
            event['type'] = 'isr_shrink'
        elif 'isr' in line_lower and ('expand' in line_lower or 'add' in line_lower):
            event['type'] = 'isr_expand'
        elif 'offline' in line_lower:
            event['type'] = 'offline'
        elif 'online' in line_lower:
            event['type'] = 'online'
        elif 'replica' in line_lower:
            event['type'] = 'replica_change'

        # Try to extract topic name
        topic_match = re.search(r'topic[=:\s]+([a-zA-Z0-9_.-]+)', line, re.IGNORECASE)
        if not topic_match:
            topic_match = re.search(r'\[([a-zA-Z0-9_.-]+),\d+\]', line)
        if topic_match:
            event['topic'] = topic_match.group(1)

        # Try to extract partition
        partition_match = re.search(r'partition[=:\s]+(\d+)', line, re.IGNORECASE)
        if not partition_match:
            partition_match = re.search(r',(\d+)\]', line)
        if partition_match:
            event['partition'] = int(partition_match.group(1))

        events.append(event)

    return events


def _analyze_state_changes(events):
    """Analyze state change events and return summary statistics."""
    analysis = {
        'leadership_changes': 0,
        'isr_shrinks': 0,
        'isr_expands': 0,
        'offline_events': 0,
        'topic_changes': defaultdict(int)
    }

    for event in events:
        event_type = event.get('type', 'unknown')

        if event_type == 'leadership_change':
            analysis['leadership_changes'] += 1
        elif event_type == 'isr_shrink':
            analysis['isr_shrinks'] += 1
        elif event_type == 'isr_expand':
            analysis['isr_expands'] += 1
        elif event_type == 'offline':
            analysis['offline_events'] += 1

        # Track per-topic changes
        topic = event.get('topic')
        if topic:
            analysis['topic_changes'][topic] += 1

    return analysis

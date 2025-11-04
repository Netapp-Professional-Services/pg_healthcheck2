"""
Patroni Failover History Check

Tracks and analyzes switchovers, failovers, and leadership changes in the Patroni cluster.
Provides insights into cluster stability, failover frequency, and recovery performance.

Uses Patroni REST API with SSH fallback following the Instaclustr pattern.

Data Sources:
- GET /history - Timeline of leadership changes and failover events

Output:
- Recent failover/switchover events table
- Frequency analysis (last 24h, 7d, 30d)
- Recovery time statistics
- Actionable recommendations based on patterns
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.patroni_client import create_patroni_client_from_settings
from plugins.postgres.utils.patroni_helpers import (
    skip_if_not_patroni,
    build_error_result,
    format_timestamp
)

logger = logging.getLogger(__name__)


def check_patroni_failover_history(connector, settings: Dict) -> Tuple[str, Dict]:
    """
    Check Patroni cluster failover and switchover history.

    Analyzes historical leadership changes to identify:
    - Cluster stability issues
    - Failover frequency and patterns
    - Recovery time performance
    - Unplanned vs planned events

    Args:
        connector: PostgreSQL connector with environment detection
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Patroni Failover History")

    # Skip if not Patroni
    skip_result = skip_if_not_patroni(connector)
    if skip_result:
        return skip_result

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Fetch history from Patroni API
        history_data = _fetch_failover_history(settings)

        if not history_data.get('success'):
            return build_error_result(
                'patroni_failover_history',
                history_data.get('error', 'Could not fetch failover history'),
                builder
            )

        events = history_data.get('events', [])

        # Analyze history
        analysis = _analyze_failover_history(events, settings)

        # Build output with actionable advice
        _build_history_output(builder, events, analysis)

        # Build findings for trend storage
        findings = {
            'patroni_failover_history': {
                'status': 'success',
                'timestamp': timestamp,
                'total_events': len(events),
                'analysis': analysis,
                'recent_events': events[:10] if events else [],
                'source': 'patroni_api'
            }
        }

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Failed to check Patroni failover history: {e}", exc_info=True)
        return build_error_result(
            'patroni_failover_history',
            str(e),
            builder
        )


def _fetch_failover_history(settings: Dict) -> Dict:
    """
    Fetch failover history from Patroni REST API.

    Args:
        settings: Configuration dictionary

    Returns:
        Dictionary with success flag and events list
    """
    client = create_patroni_client_from_settings(settings)
    if not client:
        return {'success': False, 'error': 'Could not create Patroni client - check configuration'}

    try:
        success, result = client.get_history()
        client.close()

        if not success:
            return {'success': False, 'error': result.get('error', 'Unknown error')}

        # Patroni history returns a list of events
        events = result.get('data', [])

        return {'success': True, 'events': events}

    except Exception as e:
        logger.debug(f"Could not fetch history via API: {e}")
        return {'success': False, 'error': str(e)}


def _analyze_failover_history(events: List[Dict], settings: Dict) -> Dict:
    """
    Analyze failover history for patterns and statistics.

    Args:
        events: List of failover/switchover events
        settings: Configuration dictionary

    Returns:
        Analysis dictionary with statistics and insights
    """
    if not events:
        return {
            'total_events': 0,
            'failovers': 0,
            'switchovers': 0,
            'last_24h': 0,
            'last_7d': 0,
            'last_30d': 0,
            'stability_score': 100,
            'issues': []
        }

    now = datetime.utcnow()
    cutoff_24h = now - timedelta(hours=24)
    cutoff_7d = now - timedelta(days=7)
    cutoff_30d = now - timedelta(days=30)

    # Categorize events
    failovers = 0
    switchovers = 0
    events_24h = 0
    events_7d = 0
    events_30d = 0
    unplanned_events = []

    for event in events:
        # Skip if event is not a dict (Patroni sometimes returns array format)
        if not isinstance(event, dict):
            logger.debug(f"Skipping non-dict event: {type(event).__name__}")
            continue

        # Parse timestamp
        try:
            event_time = _parse_event_timestamp(event)

            if event_time:
                if event_time > cutoff_24h:
                    events_24h += 1
                if event_time > cutoff_7d:
                    events_7d += 1
                if event_time > cutoff_30d:
                    events_30d += 1
        except Exception as e:
            logger.debug(f"Could not parse event timestamp: {e}")

        # Categorize by type (Patroni history events don't always have explicit type)
        # We infer from timeline changes
        timeline = event.get('timeline')
        if timeline:
            # Timeline change indicates a failover (unplanned)
            failovers += 1
            unplanned_events.append(event)
        else:
            # No timeline change likely means switchover (planned)
            switchovers += 1

    # Calculate stability score (0-100)
    stability_score = _calculate_stability_score(
        failovers, switchovers, events_24h, events_7d, events_30d
    )

    # Identify issues
    issues = _identify_history_issues(
        failovers, switchovers, events_24h, events_7d, events_30d, unplanned_events
    )

    return {
        'total_events': len(events),
        'failovers': failovers,
        'switchovers': switchovers,
        'last_24h': events_24h,
        'last_7d': events_7d,
        'last_30d': events_30d,
        'stability_score': stability_score,
        'issues': issues,
        'unplanned_events': len(unplanned_events)
    }


def _parse_event_timestamp(event: Dict) -> Optional[datetime]:
    """
    Parse timestamp from Patroni history event.

    Args:
        event: Event dictionary

    Returns:
        datetime object or None if parsing fails
    """
    # Try common timestamp fields
    timestamp_str = event.get('timestamp') or event.get('time')

    if not timestamp_str:
        return None

    try:
        # Remove 'Z' suffix if present
        timestamp_str = str(timestamp_str).rstrip('Z')
        return datetime.fromisoformat(timestamp_str)
    except Exception as e:
        logger.debug(f"Could not parse timestamp {timestamp_str}: {e}")
        return None


def _calculate_stability_score(
    failovers: int,
    switchovers: int,
    events_24h: int,
    events_7d: int,
    events_30d: int
) -> int:
    """
    Calculate cluster stability score based on failover history.

    Score factors:
    - Recent failovers (last 24h/7d) heavily penalized
    - Unplanned failovers more impactful than switchovers
    - Frequency over time

    Args:
        failovers: Total unplanned failovers
        switchovers: Total planned switchovers
        events_24h: Events in last 24 hours
        events_7d: Events in last 7 days
        events_30d: Events in last 30 days

    Returns:
        Stability score (0-100)
    """
    score = 100

    # Recent events are most concerning
    score -= events_24h * 20  # -20 per event in last 24h
    score -= (events_7d - events_24h) * 10  # -10 per event in last week
    score -= (events_30d - events_7d) * 5  # -5 per event in last month

    # Unplanned failovers more concerning than switchovers
    score -= failovers * 5
    score -= switchovers * 2

    return max(0, min(100, score))


def _identify_history_issues(
    failovers: int,
    switchovers: int,
    events_24h: int,
    events_7d: int,
    events_30d: int,
    unplanned_events: List[Dict]
) -> List[Dict]:
    """
    Identify issues from failover history patterns.

    Args:
        failovers: Total unplanned failovers
        switchovers: Total planned switchovers
        events_24h: Events in last 24 hours
        events_7d: Events in last 7 days
        events_30d: Events in last 30 days
        unplanned_events: List of unplanned failover events

    Returns:
        List of issue dictionaries
    """
    issues = []

    # Multiple failovers in 24h (flapping)
    if events_24h >= 3:
        issues.append({
            'severity': 'critical',
            'type': 'failover_flapping',
            'message': f'{events_24h} failover events in the last 24 hours indicates cluster instability',
            'recommendation': 'Investigate leader node health, network stability, and DCS connectivity immediately'
        })

    # Multiple failovers in 7d
    elif events_7d >= 5:
        issues.append({
            'severity': 'high',
            'type': 'frequent_failovers',
            'message': f'{events_7d} failover events in the last 7 days is unusually high',
            'recommendation': 'Review Patroni logs, check for resource exhaustion, and verify network stability'
        })

    # High ratio of unplanned failovers
    total_events = failovers + switchovers
    if total_events > 0 and (failovers / total_events) > 0.7:
        issues.append({
            'severity': 'warning',
            'type': 'unplanned_failovers',
            'message': f'{int((failovers/total_events)*100)}% of events are unplanned failovers',
            'recommendation': 'Investigate root causes of unplanned failovers. Consider improving monitoring and alerting.'
        })

    return issues


def _build_history_output(
    builder: CheckContentBuilder,
    events: List[Dict],
    analysis: Dict
):
    """
    Build AsciiDoc output for failover history with actionable advice.

    Args:
        builder: CheckContentBuilder instance
        events: List of failover events
        analysis: Analysis dictionary
    """
    # Summary section
    builder.text("*Failover History Summary*")
    builder.blank()

    summary_data = [
        {'Metric': 'Total Events', 'Value': str(analysis['total_events'])},
        {'Metric': 'Unplanned Failovers', 'Value': str(analysis['failovers'])},
        {'Metric': 'Planned Switchovers', 'Value': str(analysis['switchovers'])},
        {'Metric': 'Events (Last 24h)', 'Value': str(analysis['last_24h'])},
        {'Metric': 'Events (Last 7d)', 'Value': str(analysis['last_7d'])},
        {'Metric': 'Events (Last 30d)', 'Value': str(analysis['last_30d'])},
        {'Metric': 'Stability Score', 'Value': f"{analysis['stability_score']}/100"}
    ]

    builder.table(summary_data)
    builder.blank()

    # Stability assessment with actionable advice
    stability_score = analysis['stability_score']

    if stability_score >= 90:
        builder.note("**Cluster Stability: EXCELLENT**\n\nYour cluster has excellent stability with minimal failover events. Continue monitoring and maintain current best practices.")
    elif stability_score >= 70:
        builder.warning("**Cluster Stability: MODERATE**\n\n**Recommendation**: Review recent failover events below to identify patterns. Consider:\n\n• Monitoring DCS (etcd/Consul/ZooKeeper) health more closely\n• Checking network stability between nodes\n• Reviewing Patroni TTL and timeout settings")
    else:
        builder.critical("**Cluster Stability: POOR**\n\n**URGENT ACTIONS REQUIRED:**\n\n• Immediately investigate the cause of frequent failovers\n• Check leader node resource utilization (CPU, memory, disk I/O)\n• Verify DCS cluster health and quorum\n• Review network connectivity and latency between nodes\n• Consider increasing Patroni TTL if timeouts are too aggressive\n• Check PostgreSQL logs for crashes or restarts")

    builder.blank()

    # Recent events
    if events:
        builder.text("*Recent Failover Events*")
        builder.blank()

        # Show last 10 events
        recent_events = events[:10]
        event_table = []

        for event in recent_events:
            # Skip if event is not a dict (Patroni sometimes returns array format)
            if not isinstance(event, dict):
                logger.debug(f"Skipping non-dict event in output: {type(event).__name__}")
                continue

            # Extract event details
            timestamp = event.get('timestamp', event.get('time', 'Unknown'))
            try:
                timestamp = format_timestamp(timestamp)
            except:
                pass

            event_table.append({
                'Timestamp': timestamp,
                'Timeline': str(event.get('timeline', 'N/A')),
                'Leader': event.get('leader', 'Unknown')
            })

        if event_table:
            builder.table(event_table)
            builder.blank()
    else:
        builder.note("**No failover events recorded**\n\nThis could indicate:\n\n• A newly deployed cluster with no history\n• History retention is configured to clear old events\n• The /history endpoint is not enabled")

    # Issues and recommendations - group by severity
    issues = analysis.get('issues', [])
    if issues:
        # Group issues by severity
        critical_issues = [i for i in issues if i['severity'] == 'critical']
        high_issues = [i for i in issues if i['severity'] == 'high']
        warning_issues = [i for i in issues if i['severity'] == 'warning']

        # Format issue details for admonition blocks
        if critical_issues:
            details = []
            for issue in critical_issues:
                details.append(f"*{issue['type'].replace('_', ' ').title()}*")
                details.append(f"{issue['message']}")
                details.append(f"_Action_: {issue['recommendation']}")
            builder.critical_issue("Critical Failover Issues", details)

        if high_issues:
            details = []
            for issue in high_issues:
                details.append(f"*{issue['type'].replace('_', ' ').title()}*")
                details.append(f"{issue['message']}")
                details.append(f"_Action_: {issue['recommendation']}")
            builder.warning_issue("High Priority Failover Issues", details)

        if warning_issues:
            details = []
            for issue in warning_issues:
                details.append(f"*{issue['type'].replace('_', ' ').title()}*")
                details.append(f"{issue['message']}")
                details.append(f"_Action_: {issue['recommendation']}")
            builder.warning_issue("Failover Warnings", details)


def _build_findings(events: List[Dict], analysis: Dict, timestamp: str) -> Dict:
    """
    Build structured findings for trend storage.

    Args:
        events: List of events
        analysis: Analysis dictionary
        timestamp: ISO 8601 timestamp

    Returns:
        Structured findings dictionary
    """
    return {
        'patroni_failover_history': {
            'status': 'success',
            'timestamp': timestamp,
            'total_events': analysis['total_events'],
            'failovers': analysis['failovers'],
            'switchovers': analysis['switchovers'],
            'last_24h': analysis['last_24h'],
            'last_7d': analysis['last_7d'],
            'last_30d': analysis['last_30d'],
            'stability_score': analysis['stability_score'],
            'issues': analysis['issues'],
            'recent_events': events[:10] if events else [],
            'source': 'patroni_api'
        }
    }

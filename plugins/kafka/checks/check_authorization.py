"""
Authorization Log Analysis for Kafka clusters.

Analyzes kafka-authorizer.log files to detect:
- Access denied events
- Unusual access patterns
- Security audit information

Data sources:
- kafka-authorizer.log* files (import and live via SSH)
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7  # High priority - security related


def run_authorization_check(connector, settings):
    """
    Analyzes Kafka authorization logs for security events.

    Checks:
    - Access denied events
    - Denied principals (users/clients)
    - Denied operations
    - Denied resources (topics, groups, etc.)

    Args:
        connector: Kafka connector (live or import)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Authorization log analysis")
    if not available:
        return skip_msg, skip_data

    try:
        builder.h3("Authorization Audit")

        # Get thresholds
        denied_warning = settings.get('kafka_auth_denied_warning', 10)
        denied_critical = settings.get('kafka_auth_denied_critical', 100)

        # === Collect authorization logs from all brokers ===
        all_events = []
        broker_events = {}
        errors = []

        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Read authorization logs
                command = "cat /var/log/kafka/kafka-authorizer.log* 2>/dev/null || cat /opt/kafka/logs/kafka-authorizer.log* 2>/dev/null || echo ''"
                results = connector.execute_ssh_on_all_hosts(command, "Read authorization logs")

                for result in results:
                    if result.get('host') == ssh_host and result.get('success'):
                        log_content = result.get('output', '')
                        if log_content.strip():
                            events = _parse_authorizer_log(log_content, broker_id)
                            broker_events[broker_id] = events
                            all_events.extend(events)

            except Exception as e:
                logger.warning(f"Failed to read authorization logs from {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        if not all_events:
            builder.note(
                "No authorization events found in logs. This could mean:\n\n"
                "* Authorization logging is not enabled\n"
                "* No ACLs are configured (allow all)\n"
                "* Log files are rotated or empty"
            )
            structured_data["authorization"] = {
                "status": "success",
                "total_events": 0,
                "message": "No authorization events found"
            }
            return builder.build(), structured_data

        # === Analyze events ===
        analysis = _analyze_auth_events(all_events)

        # === Build report ===
        total_events = len(all_events)
        denied_events = analysis['denied_count']
        allowed_events = analysis['allowed_count']

        issues = []
        warnings = []

        # Check denied event frequency
        if denied_events >= denied_critical:
            issues.append({
                'level': 'critical',
                'type': 'excessive_denied_access',
                'message': f'{denied_events} access denied events - potential security issue or misconfiguration'
            })
        elif denied_events >= denied_warning:
            warnings.append({
                'level': 'warning',
                'type': 'frequent_denied_access',
                'message': f'{denied_events} access denied events - review ACL configuration'
            })

        # Check for specific patterns
        if analysis['denied_principals']:
            top_denied = sorted(analysis['denied_principals'].items(), key=lambda x: x[1], reverse=True)[:3]
            if top_denied[0][1] > 50:  # More than 50 denials for a single principal
                warnings.append({
                    'level': 'warning',
                    'type': 'principal_denied_frequently',
                    'message': f'Principal "{top_denied[0][0]}" denied {top_denied[0][1]} times'
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
        builder.h4("Authorization Summary")

        summary_data = [
            {"Metric": "Total Events", "Count": str(total_events)},
            {"Metric": "Allowed", "Count": str(allowed_events)},
            {"Metric": "Denied", "Count": str(denied_events)},
            {"Metric": "Unique Principals", "Count": str(len(analysis['all_principals']))},
            {"Metric": "Unique Resources", "Count": str(len(analysis['all_resources']))},
        ]
        builder.table(summary_data)
        builder.blank()

        # Denied principals
        if analysis['denied_principals']:
            builder.h4("Denied Principals (Top 10)")

            principal_rows = []
            sorted_principals = sorted(analysis['denied_principals'].items(), key=lambda x: x[1], reverse=True)[:10]
            for principal, count in sorted_principals:
                principal_rows.append({
                    "Principal": principal,
                    "Denied Count": str(count),
                    "Status": "High" if count > 20 else "Normal"
                })

            builder.table(principal_rows)
            builder.blank()

        # Denied operations
        if analysis['denied_operations']:
            builder.h4("Denied Operations")

            op_rows = []
            sorted_ops = sorted(analysis['denied_operations'].items(), key=lambda x: x[1], reverse=True)
            for operation, count in sorted_ops:
                op_rows.append({
                    "Operation": operation,
                    "Denied Count": str(count)
                })

            builder.table(op_rows)
            builder.blank()

        # Denied resources
        if analysis['denied_resources']:
            builder.h4("Denied Resources (Top 10)")

            resource_rows = []
            sorted_resources = sorted(analysis['denied_resources'].items(), key=lambda x: x[1], reverse=True)[:10]
            for resource, count in sorted_resources:
                resource_rows.append({
                    "Resource": resource,
                    "Denied Count": str(count)
                })

            builder.table(resource_rows)
            builder.blank()

        # Errors
        if errors:
            builder.h4("Collection Errors")
            builder.warning(
                f"Could not collect authorization logs from {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']}: {e['error']}" for e in errors])
            )

        # Recommendations
        if issues or warnings:
            recommendations = {}

            if has_critical:
                recommendations["critical"] = [
                    "**Immediately review denied access patterns** - may indicate attack or misconfiguration",
                    "**Check ACL configuration** for the affected principals",
                    "**Review client configurations** - credentials may be incorrect",
                    "**Check for brute force attempts** from unknown principals"
                ]

            if has_warning:
                recommendations["high"] = [
                    "**Review frequently denied principals** and their intended access",
                    "**Update ACLs** if denials are legitimate access requests",
                    "**Verify client configurations** match expected permissions"
                ]

            recommendations["general"] = [
                "Enable authorization logging with `authorizer.class.name=kafka.security.authorizer.AclAuthorizer`",
                "Regularly audit ACLs using `kafka-acls.sh --list`",
                "Follow principle of least privilege when granting access",
                "Set up alerts for unusual denial patterns",
                "Consider using super.users sparingly and with strong authentication"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Authorization audit passed.\n\n"
                f"Analyzed {total_events} events. Allowed: {allowed_events}, Denied: {denied_events}."
            )

        # === Structured data ===
        structured_data["authorization"] = {
            "status": "success",
            "total_events": total_events,
            "allowed_count": allowed_events,
            "denied_count": denied_events,
            "denied_principals": dict(analysis['denied_principals']),
            "denied_operations": dict(analysis['denied_operations']),
            "denied_resources": dict(analysis['denied_resources']),
            "issues": issues,
            "warnings": warnings,
            "has_critical_issues": has_critical,
            "has_warnings": has_warning,
            "errors": errors
        }

    except Exception as e:
        import traceback
        logger.error(f"Authorization check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["authorization"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data


def _parse_authorizer_log(content, broker_id):
    """
    Parse kafka-authorizer.log content into structured events.

    Example log lines:
    [2024-01-15 10:30:45,123] DEBUG Principal = User:alice is Allowed Operation = Read from host = 192.168.1.10 ...
    [2024-01-15 10:30:46,456] INFO Principal = User:bob is Denied Operation = Write from host = 192.168.1.11 ...
    """
    events = []

    for line in content.split('\n'):
        line = line.strip()
        if not line:
            continue

        event = {
            'broker_id': broker_id,
            'raw': line,
            'result': 'unknown'
        }

        # Try to extract timestamp
        timestamp_match = re.match(r'\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
        if timestamp_match:
            event['timestamp'] = timestamp_match.group(1)

        # Extract result (Allowed/Denied)
        if 'Allowed' in line or 'ALLOWED' in line:
            event['result'] = 'allowed'
        elif 'Denied' in line or 'DENIED' in line:
            event['result'] = 'denied'

        # Extract principal
        principal_match = re.search(r'Principal\s*=\s*([^\s,]+)', line, re.IGNORECASE)
        if principal_match:
            event['principal'] = principal_match.group(1)

        # Extract operation
        operation_match = re.search(r'Operation\s*=\s*(\w+)', line, re.IGNORECASE)
        if operation_match:
            event['operation'] = operation_match.group(1)

        # Extract host
        host_match = re.search(r'host\s*=\s*([^\s,]+)', line, re.IGNORECASE)
        if host_match:
            event['host'] = host_match.group(1)

        # Extract resource
        resource_match = re.search(r'resource\s*=\s*([^\s,\]]+)', line, re.IGNORECASE)
        if not resource_match:
            resource_match = re.search(r'on\s+resource\s*=?\s*([^\s,\]]+)', line, re.IGNORECASE)
        if resource_match:
            event['resource'] = resource_match.group(1)

        events.append(event)

    return events


def _analyze_auth_events(events):
    """Analyze authorization events and return summary statistics."""
    analysis = {
        'allowed_count': 0,
        'denied_count': 0,
        'denied_principals': defaultdict(int),
        'denied_operations': defaultdict(int),
        'denied_resources': defaultdict(int),
        'all_principals': set(),
        'all_resources': set()
    }

    for event in events:
        result = event.get('result', 'unknown')
        principal = event.get('principal', 'unknown')
        operation = event.get('operation', 'unknown')
        resource = event.get('resource', 'unknown')

        analysis['all_principals'].add(principal)
        analysis['all_resources'].add(resource)

        if result == 'allowed':
            analysis['allowed_count'] += 1
        elif result == 'denied':
            analysis['denied_count'] += 1
            analysis['denied_principals'][principal] += 1
            analysis['denied_operations'][operation] += 1
            analysis['denied_resources'][resource] += 1

    return analysis

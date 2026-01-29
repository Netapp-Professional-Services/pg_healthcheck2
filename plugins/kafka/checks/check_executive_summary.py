"""
Executive Summary check for Kafka health checks.

Aggregates data from all other checks to provide:
- Overall cluster health score
- Quick status overview
- Key concerns ranked by severity
- Cross-broker comparisons
- Resource utilization summary

This check should run AFTER all other checks to aggregate their findings.
"""

from plugins.common.check_helpers import CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight - run last to aggregate."""
    return 1


def _get_nested_data(findings, check_name, inner_key=None, data_key='data'):
    """
    Get data from findings with flexible structure handling.

    Handles both:
    - findings['check_name']['data'] (flat structure)
    - findings['check_name']['inner_key']['data'] (nested structure)

    Always returns a dict, never a list.
    """
    check_data = findings.get(check_name, {})
    if not isinstance(check_data, dict):
        return {}

    # Try flat structure first
    if data_key in check_data:
        result = check_data.get(data_key, {})
        if isinstance(result, list):
            return result[0] if result else {}
        return result if isinstance(result, dict) else {}

    # Try nested structure
    if inner_key and inner_key in check_data:
        nested = check_data.get(inner_key, {})
        if isinstance(nested, dict):
            result = nested.get(data_key, nested)
            if isinstance(result, list):
                return result[0] if result else {}
            return result if isinstance(result, dict) else nested

    # Fallback: look for any nested dict with the data_key
    for key, value in check_data.items():
        if isinstance(value, dict):
            if data_key in value:
                result = value.get(data_key, {})
                if isinstance(result, list):
                    return result[0] if result else {}
                return result if isinstance(result, dict) else {}
            # If no data key, return the nested dict itself
            return value

    return {}


def calculate_health_score(findings):
    """
    Calculate overall cluster health score (0-100).

    Args:
        findings: Dict of all check findings

    Returns:
        tuple: (score, list of deductions)
    """
    score = 100
    deductions = []

    # Check ISR health - handle nested structure
    isr = _get_nested_data(findings, 'check_isr_health', 'isr_health', 'summary')
    under_replicated = isr.get('under_replicated_partitions', isr.get('under_replicated_count', 0))
    offline_count = isr.get('offline_partitions', isr.get('offline_partition_count', 0))

    if offline_count > 0:
        score -= 25
        deductions.append(f"Offline partitions ({offline_count}): -25")
    if under_replicated > 50:
        score -= 20
        deductions.append(f"Many under-replicated partitions ({under_replicated}): -20")
    elif under_replicated > 10:
        score -= 10
        deductions.append(f"Under-replicated partitions ({under_replicated}): -10")
    elif under_replicated > 0:
        score -= 5
        deductions.append(f"Some under-replicated partitions ({under_replicated}): -5")

    # Check consumer lag
    consumer_lag = _get_nested_data(findings, 'check_consumer_lag', 'consumer_lag')
    critical_lag_groups = consumer_lag.get('critical_lag_groups', 0)
    high_lag_groups = consumer_lag.get('high_lag_groups', 0)

    if critical_lag_groups > 0:
        score -= 15
        deductions.append(f"Critical consumer lag ({critical_lag_groups} groups): -15")
    if high_lag_groups > 0:
        score -= 8
        deductions.append(f"High consumer lag ({high_lag_groups} groups): -8")

    # Check consumer group health
    consumer_groups = _get_nested_data(findings, 'check_consumer_group_health', 'consumer_groups')
    dead_groups = consumer_groups.get('dead_groups', 0)
    rebalancing_groups = consumer_groups.get('rebalancing_groups', 0)
    # Also check issues_detected
    issues = consumer_groups.get('issues_detected', {})
    if isinstance(issues, dict):
        dead_groups = issues.get('dead_groups', dead_groups)
        rebalancing_groups = issues.get('rebalancing_groups', rebalancing_groups)
        unstable_groups = issues.get('unstable_groups', 0)
        if unstable_groups > 0:
            score -= 10
            deductions.append(f"Unstable consumer groups ({unstable_groups}): -10")

    if dead_groups > 0:
        score -= 15
        deductions.append(f"Dead consumer groups ({dead_groups}): -15")
    if rebalancing_groups > 3:
        score -= 10
        deductions.append(f"Many rebalancing groups ({rebalancing_groups}): -10")
    elif rebalancing_groups > 0:
        score -= 5
        deductions.append(f"Rebalancing groups ({rebalancing_groups}): -5")

    # Check KRaft quorum
    kraft = _get_nested_data(findings, 'check_kraft_quorum', 'kraft_quorum')
    if kraft.get('status') == 'success':
        if kraft.get('quorum_degraded', False):
            score -= 20
            deductions.append("KRaft quorum degraded: -20")
        if kraft.get('warnings'):
            for warning in kraft.get('warnings', []):
                if 'replication_lag' in warning.get('type', ''):
                    score -= 5
                    deductions.append("KRaft replication lag: -5")
                    break

    # Check topic configuration
    topic_config = _get_nested_data(findings, 'check_topic_configuration', 'topic_configuration')
    critical_topics = topic_config.get('critical_count', 0)
    warning_topics = topic_config.get('warning_count', 0)

    if critical_topics > 0:
        score -= min(15, critical_topics * 5)
        deductions.append(f"Critical topic config issues ({critical_topics}): -{min(15, critical_topics * 5)}")
    if warning_topics > 3:
        score -= 5
        deductions.append(f"Topic config warnings ({warning_topics}): -5")

    # Check broker config
    broker_config = _get_nested_data(findings, 'check_broker_config', 'broker_config')
    critical_issues = 0
    warning_issues = 0
    # Sum across all brokers
    broker_data = broker_config.get('data', [])
    if isinstance(broker_data, list):
        for broker in broker_data:
            if isinstance(broker, dict):
                critical_issues += broker.get('critical_count', 0)
                warning_issues += broker.get('warning_count', 0)

    if critical_issues > 0:
        score -= min(15, critical_issues * 5)
        deductions.append(f"Critical config issues ({critical_issues}): -{min(15, critical_issues * 5)}")
    if warning_issues > 5:
        score -= 5
        deductions.append(f"Config warnings ({warning_issues}): -5")

    # Check internal topic replication
    internal_topics = _get_nested_data(findings, 'check_internal_topic_replication', 'internal_topic_replication')
    internal_critical = internal_topics.get('critical_count', 0)
    if internal_critical > 0:
        score -= 20
        deductions.append(f"Critical internal topic issues ({internal_critical}): -20")

    # Check GC pauses
    gc = _get_nested_data(findings, 'check_gc_pauses', 'gc_pauses')
    critical_gc = gc.get('critical_broker_count', 0)
    warning_gc = gc.get('warning_broker_count', 0)
    if critical_gc > 0:
        score -= 15
        deductions.append(f"Critical GC issues ({critical_gc} brokers): -15")
    elif warning_gc > 0:
        score -= 5
        deductions.append(f"GC warnings ({warning_gc} brokers): -5")

    # Check log errors
    logs = _get_nested_data(findings, 'check_log_errors', 'log_errors')
    critical_brokers = len(logs.get('critical_brokers', []))
    if critical_brokers > 0:
        score -= 15
        deductions.append(f"Critical log errors ({critical_brokers} brokers): -15")

    # Check state changes (leadership churn)
    state_changes = _get_nested_data(findings, 'check_state_changes', 'state_changes')
    leadership_changes = state_changes.get('leadership_changes', 0)
    if leadership_changes > 100:
        score -= 15
        deductions.append(f"Excessive leadership changes ({leadership_changes}): -15")
    elif leadership_changes > 50:
        score -= 10
        deductions.append(f"High leadership changes ({leadership_changes}): -10")

    # Check CVEs
    cve = _get_nested_data(findings, 'check_cve_vulnerabilities', 'cve_vulnerabilities', 'data')
    # Also try flat structure
    if not cve:
        cve_check = findings.get('check_cve_vulnerabilities', {})
        if 'data' in cve_check:
            cve = cve_check.get('data', {})

    core = cve.get('core', {})
    severity_counts = core.get('severity_counts', {})
    critical_cves = severity_counts.get('critical', 0)
    high_cves = severity_counts.get('high', 0)

    if critical_cves > 0:
        score -= min(20, critical_cves * 5)
        deductions.append(f"Critical CVEs ({critical_cves}): -{min(20, critical_cves * 5)}")
    if high_cves > 0:
        score -= min(10, high_cves * 3)
        deductions.append(f"High CVEs ({high_cves}): -{min(10, high_cves * 3)}")

    # Check system limits
    system_limits = _get_nested_data(findings, 'check_system_limits', 'system_limits')
    limit_critical = system_limits.get('critical_brokers', 0)
    if limit_critical > 0:
        score -= 10
        deductions.append(f"System limit issues ({limit_critical} brokers): -10")

    # Check I/O issues
    iostat = _get_nested_data(findings, 'check_iostat', 'iostat')
    io_critical = iostat.get('critical_broker_count', 0)
    if io_critical > 0:
        score -= 10
        deductions.append(f"Critical I/O issues ({io_critical} brokers): -10")

    return max(0, score), deductions


def get_health_grade(score):
    """Convert score to letter grade."""
    if score >= 90:
        return 'A', 'Excellent'
    elif score >= 80:
        return 'B', 'Good'
    elif score >= 70:
        return 'C', 'Fair'
    elif score >= 60:
        return 'D', 'Poor'
    else:
        return 'F', 'Critical'


def get_key_concerns(findings):
    """
    Extract and rank key concerns from all findings.

    Returns list of (severity, concern, recommendation)
    """
    concerns = []

    # Check ISR health
    isr = _get_nested_data(findings, 'check_isr_health', 'isr_health', 'summary')
    offline = isr.get('offline_partitions', isr.get('offline_partition_count', 0))
    under_replicated = isr.get('under_replicated_partitions', isr.get('under_replicated_count', 0))

    if offline > 0:
        concerns.append(('CRITICAL', f'{offline} offline partition(s)',
                        'Investigate broker health and restore replicas immediately'))
    if under_replicated > 50:
        concerns.append(('CRITICAL', f'{under_replicated} under-replicated partitions',
                        'Check for broker failures or network issues'))
    elif under_replicated > 10:
        concerns.append(('HIGH', f'{under_replicated} under-replicated partitions',
                        'Monitor closely and check broker health'))

    # Check topic configuration
    topic_config = _get_nested_data(findings, 'check_topic_configuration', 'topic_configuration')
    critical_topics = topic_config.get('critical_count', 0)
    if critical_topics > 0:
        concerns.append(('CRITICAL', f'{critical_topics} topic(s) with critical config issues',
                        'Fix min.insync.replicas and replication factor immediately'))

    # Check consumer groups
    consumer_groups = _get_nested_data(findings, 'check_consumer_group_health', 'consumer_groups')
    issues = consumer_groups.get('issues_detected', {})
    unstable = issues.get('unstable_groups', 0)
    if unstable > 0:
        concerns.append(('HIGH', f'{unstable} unstable consumer group(s)',
                        'Investigate rebalancing or consumer failures'))

    # Check KRaft quorum
    kraft = _get_nested_data(findings, 'check_kraft_quorum', 'kraft_quorum')
    if kraft.get('warnings'):
        for warning in kraft.get('warnings', []):
            if 'replication_lag' in warning.get('type', ''):
                concerns.append(('HIGH', 'KRaft voters have replication lag',
                                'Check controller performance and network'))

    # Check state changes
    state_changes = _get_nested_data(findings, 'check_state_changes', 'state_changes')
    leadership_changes = state_changes.get('leadership_changes', 0)
    if leadership_changes > 100:
        concerns.append(('CRITICAL', f'{leadership_changes} leadership changes detected',
                        'Check broker stability and network connectivity'))
    elif leadership_changes > 50:
        concerns.append(('HIGH', f'{leadership_changes} leadership changes',
                        'Monitor broker health'))

    # Check system limits
    system_limits = _get_nested_data(findings, 'check_system_limits', 'system_limits')
    limit_critical = system_limits.get('critical_brokers', 0)
    if limit_critical > 0:
        concerns.append(('CRITICAL', f'{limit_critical} broker(s) with inadequate system limits',
                        'Update /etc/security/limits.conf and restart brokers'))

    # Check I/O
    iostat = _get_nested_data(findings, 'check_iostat', 'iostat')
    io_critical = iostat.get('critical_broker_count', 0)
    if io_critical > 0:
        concerns.append(('HIGH', f'{io_critical} broker(s) with high I/O wait',
                        'Check disk performance and consider storage upgrade'))

    # Check GC
    gc = _get_nested_data(findings, 'check_gc_pauses', 'gc_pauses')
    warning_gc = gc.get('warning_broker_count', 0)
    if warning_gc > 0:
        concerns.append(('MEDIUM', f'{warning_gc} broker(s) with high GC frequency',
                        'Review JVM heap settings'))

    # Check CVEs
    cve = _get_nested_data(findings, 'check_cve_vulnerabilities', 'cve_vulnerabilities', 'data')
    if not cve:
        cve = findings.get('check_cve_vulnerabilities', {}).get('data', {})
    core = cve.get('core', {})
    severity_counts = core.get('severity_counts', {})
    critical_cves = severity_counts.get('critical', 0)
    if critical_cves > 0:
        concerns.append(('HIGH', f'{critical_cves} critical CVE(s) for this Kafka version',
                        'Plan upgrade to patched version'))

    # Sort by severity
    severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2}
    concerns.sort(key=lambda x: severity_order.get(x[0], 3))

    return concerns


def get_resource_summary(findings):
    """
    Aggregate resource utilization across the cluster.
    """
    resources = {
        'total_brokers': 0,
        'total_topics': 0,
        'total_partitions': 0,
        'total_consumer_groups': 0,
        'kafka_mode': 'unknown',
        'kafka_version': 'unknown',
    }

    # From topic count check
    topics = _get_nested_data(findings, 'check_topic_count_and_naming', 'topic_analysis', 'data')
    if isinstance(topics, list) and topics:
        topics = topics[0]
    resources['total_topics'] = topics.get('total_count', 0)

    # From ISR check
    isr = _get_nested_data(findings, 'check_isr_health', 'isr_health', 'summary')
    resources['total_partitions'] = isr.get('total_partitions', 0)
    resources['total_brokers'] = isr.get('total_brokers', 0)

    # From consumer group health
    consumer_groups = _get_nested_data(findings, 'check_consumer_group_health', 'consumer_groups')
    resources['total_consumer_groups'] = consumer_groups.get('groups_analyzed', 0)

    # From db_metadata if available
    db_meta = findings.get('db_metadata', {})
    if db_meta:
        resources['kafka_version'] = db_meta.get('version', 'unknown')
        env_details = db_meta.get('environment_details', {})
        resources['kafka_mode'] = env_details.get('kafka_mode', 'unknown').upper()
        if resources['total_brokers'] == 0:
            resources['total_brokers'] = env_details.get('node_count', 0)

    return resources


def run_executive_summary_check(connector, settings, all_findings=None):
    """
    Generate executive summary from all check findings.

    Args:
        connector: Database connector
        settings: Configuration settings
        all_findings: Dict of all check findings (passed from report builder)

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)

    # Try to get findings from connector if not passed directly
    if all_findings is None:
        all_findings = getattr(connector, '_all_findings', {})

    # Ensure db_metadata is available (get directly from connector if not in findings)
    if 'db_metadata' not in all_findings and hasattr(connector, 'get_db_metadata'):
        try:
            all_findings['db_metadata'] = connector.get_db_metadata()
        except Exception:
            pass

    if not all_findings:
        builder.note("Executive summary requires findings from other checks.")
        return builder.build(), {'status': 'no_data'}

    # Calculate health score
    score, deductions = calculate_health_score(all_findings)
    grade, grade_desc = get_health_grade(score)

    # Health Score Section
    builder.h3("Cluster Health Score")

    if score >= 80:
        builder.success(f"**{grade} ({score}/100)** - {grade_desc}")
    elif score >= 60:
        builder.warning(f"**{grade} ({score}/100)** - {grade_desc}")
    else:
        builder.critical(f"**{grade} ({score}/100)** - {grade_desc}")
    builder.blank()

    # Quick Status
    resources = get_resource_summary(all_findings)
    isr = _get_nested_data(all_findings, 'check_isr_health', 'isr_health', 'summary')

    builder.h3("Quick Status")
    status_items = [
        f"**Kafka Version:** {resources['kafka_version']}",
        f"**Mode:** {resources['kafka_mode']}",
        f"**Brokers:** {resources['total_brokers']}",
        f"**Topics:** {resources['total_topics']} ({resources['total_partitions']} partitions)",
        f"**Consumer Groups:** {resources['total_consumer_groups']}",
    ]

    # Add ISR status
    under_replicated = isr.get('under_replicated_partitions', 0)
    if under_replicated == 0:
        status_items.append("**Replication:** All in-sync")
    else:
        status_items.append(f"**Replication:** {under_replicated} under-replicated")

    for item in status_items:
        builder.text(item)
    builder.blank()

    # Key Concerns
    concerns = get_key_concerns(all_findings)
    if concerns:
        builder.h3("Key Concerns")

        critical = [c for c in concerns if c[0] == 'CRITICAL']
        high = [c for c in concerns if c[0] == 'HIGH']
        medium = [c for c in concerns if c[0] == 'MEDIUM']

        if critical:
            builder.critical(f"**{len(critical)} Critical Issue(s):**")
            for _, concern, rec in critical:
                builder.text(f"- {concern}")
                builder.text(f"  _Action: {rec}_")
            builder.blank()

        if high:
            builder.warning(f"**{len(high)} High Priority Issue(s):**")
            for _, concern, rec in high:
                builder.text(f"- {concern}")
                builder.text(f"  _Action: {rec}_")
            builder.blank()

        if medium:
            builder.note(f"**{len(medium)} Medium Priority Issue(s):**")
            for _, concern, rec in medium:
                builder.text(f"- {concern}")
            builder.blank()
    else:
        builder.success("No significant concerns detected.")
        builder.blank()

    # Score Breakdown (if there are deductions)
    if deductions and score < 100:
        builder.h3("Health Score Breakdown")
        builder.text("*Deductions from base score of 100:*")
        for ded in deductions[:10]:  # Limit to top 10
            builder.text(f"- {ded}")
        if len(deductions) > 10:
            builder.text(f"- ... and {len(deductions) - 10} more")
        builder.blank()

    # Structured data
    structured_data = {
        'status': 'success',
        'data': {
            'health_score': score,
            'health_grade': grade,
            'health_description': grade_desc,
            'total_concerns': len(concerns),
            'critical_concerns': len([c for c in concerns if c[0] == 'CRITICAL']),
            'high_concerns': len([c for c in concerns if c[0] == 'HIGH']),
            'medium_concerns': len([c for c in concerns if c[0] == 'MEDIUM']),
            'resources': resources,
            'deductions': deductions
        }
    }

    return builder.build(), structured_data

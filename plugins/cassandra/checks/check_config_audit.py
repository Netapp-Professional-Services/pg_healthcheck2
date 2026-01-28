"""
Configuration audit check for Cassandra.

Analyzes cassandra.yaml for:
- Security issues (authentication, authorization)
- Performance tuning (commitlog, compaction, memory)
- Reliability settings (hints, repairs)
- Best practice violations

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import yaml
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def parse_cassandra_yaml(yaml_content):
    """
    Parse cassandra.yaml content.

    Args:
        yaml_content: Raw YAML content

    Returns:
        dict: Parsed configuration
    """
    try:
        # Handle comments and special characters
        return yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        logger.warning(f"YAML parsing error: {e}")
        # Try to extract key values manually
        config = {}
        for line in yaml_content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and ':' in line:
                parts = line.split(':', 1)
                key = parts[0].strip()
                value = parts[1].strip() if len(parts) > 1 else ''
                # Remove inline comments
                if '#' in value:
                    value = value.split('#')[0].strip()
                # Remove quotes
                value = value.strip("'\"")
                if value:
                    config[key] = value
        return config


def run_check_config_audit(connector, settings):
    """
    Audit Cassandra configuration for issues and best practices.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Configuration Audit")
    builder.para("Analyzing `cassandra.yaml` for security, performance, and best practice issues.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Configuration audit")
    if not available:
        return skip_msg, skip_data

    try:
        # Get cassandra.yaml from all hosts
        results = connector.execute_ssh_on_all_hosts(
            "cat /etc/cassandra/cassandra.yaml || cat /etc/cassandra/conf/cassandra.yaml",
            "Get Cassandra configuration"
        )

        all_configs = []
        security_issues = []
        performance_issues = []
        reliability_issues = []
        recommendations = []

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            config = parse_cassandra_yaml(result['output'])
            config['_node'] = node_ip
            all_configs.append(config)

            # === SECURITY CHECKS ===

            # Check authenticator
            authenticator = config.get('authenticator', 'AllowAllAuthenticator')
            if 'AllowAllAuthenticator' in str(authenticator):
                security_issues.append({
                    'node': node_ip,
                    'severity': 'critical',
                    'issue': 'Authentication disabled',
                    'detail': 'authenticator: AllowAllAuthenticator',
                    'recommendation': 'Enable PasswordAuthenticator for production'
                })

            # Check authorizer
            authorizer = config.get('authorizer', 'AllowAllAuthorizer')
            if 'AllowAllAuthorizer' in str(authorizer):
                security_issues.append({
                    'node': node_ip,
                    'severity': 'warning',
                    'issue': 'Authorization disabled',
                    'detail': 'authorizer: AllowAllAuthorizer',
                    'recommendation': 'Enable CassandraAuthorizer for role-based access'
                })

            # Check client encryption
            client_encryption = config.get('client_encryption_options', {})
            if isinstance(client_encryption, dict):
                if not client_encryption.get('enabled', False):
                    security_issues.append({
                        'node': node_ip,
                        'severity': 'warning',
                        'issue': 'Client encryption disabled',
                        'detail': 'client_encryption_options.enabled: false',
                        'recommendation': 'Enable TLS for client connections'
                    })

            # Check internode encryption
            server_encryption = config.get('server_encryption_options', {})
            if isinstance(server_encryption, dict):
                internode_encryption = server_encryption.get('internode_encryption', 'none')
                if internode_encryption == 'none':
                    security_issues.append({
                        'node': node_ip,
                        'severity': 'warning',
                        'issue': 'Inter-node encryption disabled',
                        'detail': 'server_encryption_options.internode_encryption: none',
                        'recommendation': 'Enable internode_encryption: all or dc for production'
                    })

            # === PERFORMANCE CHECKS ===

            # Check commitlog sync mode
            commitlog_sync = config.get('commitlog_sync', 'periodic')
            if commitlog_sync == 'batch':
                performance_issues.append({
                    'node': node_ip,
                    'severity': 'info',
                    'issue': 'Commitlog batch sync enabled',
                    'detail': f'commitlog_sync: {commitlog_sync}',
                    'recommendation': 'Batch sync provides durability but may impact write latency'
                })

            # Check concurrent reads/writes
            concurrent_reads = config.get('concurrent_reads')
            concurrent_writes = config.get('concurrent_writes')
            if concurrent_reads:
                try:
                    if int(concurrent_reads) < 32:
                        performance_issues.append({
                            'node': node_ip,
                            'severity': 'info',
                            'issue': 'Low concurrent_reads setting',
                            'detail': f'concurrent_reads: {concurrent_reads}',
                            'recommendation': 'Consider increasing for SSDs (32-64 typical)'
                        })
                except:
                    pass

            # Check memtable allocation type
            memtable_allocation = config.get('memtable_allocation_type', 'heap_buffers')
            if memtable_allocation == 'heap_buffers':
                performance_issues.append({
                    'node': node_ip,
                    'severity': 'info',
                    'issue': 'Memtables using heap memory',
                    'detail': f'memtable_allocation_type: {memtable_allocation}',
                    'recommendation': 'Consider offheap_buffers for large heaps to reduce GC pressure'
                })

            # Check compaction throughput
            compaction_throughput = config.get('compaction_throughput_mb_per_sec')
            if compaction_throughput:
                try:
                    throughput_val = int(compaction_throughput)
                    if throughput_val <= 16:
                        # Default value or very low - this is a common cause of compaction backlog
                        performance_issues.append({
                            'node': node_ip,
                            'severity': 'warning',
                            'issue': 'Very low compaction throughput (default value)',
                            'detail': f'compaction_throughput_mb_per_sec: {compaction_throughput}',
                            'recommendation': 'Increase to 64-128 MB/s for SSDs — low throughput causes compaction backlog and read latency'
                        })
                    elif throughput_val < 64:
                        performance_issues.append({
                            'node': node_ip,
                            'severity': 'info',
                            'issue': 'Low compaction throughput',
                            'detail': f'compaction_throughput_mb_per_sec: {compaction_throughput}',
                            'recommendation': 'Consider increasing for faster compaction (64-128 MB/s typical for SSDs)'
                        })
                except:
                    pass

            # === RELIABILITY CHECKS ===

            # Check hinted handoff
            hinted_handoff = config.get('hinted_handoff_enabled', True)
            if str(hinted_handoff).lower() == 'false':
                reliability_issues.append({
                    'node': node_ip,
                    'severity': 'warning',
                    'issue': 'Hinted handoff disabled',
                    'detail': 'hinted_handoff_enabled: false',
                    'recommendation': 'Enable for better consistency during node failures'
                })

            # Check max hint window
            max_hint_window = config.get('max_hint_window_in_ms')
            if max_hint_window:
                try:
                    hours = int(max_hint_window) / 3600000
                    if hours < 3:
                        reliability_issues.append({
                            'node': node_ip,
                            'severity': 'info',
                            'issue': f'Short hint window ({hours:.1f} hours)',
                            'detail': f'max_hint_window_in_ms: {max_hint_window}',
                            'recommendation': 'Consider increasing for longer outage tolerance'
                        })
                except:
                    pass

            # Check phi convict threshold
            phi_threshold = config.get('phi_convict_threshold')
            if phi_threshold:
                try:
                    if float(phi_threshold) < 8:
                        reliability_issues.append({
                            'node': node_ip,
                            'severity': 'warning',
                            'issue': 'Aggressive failure detection',
                            'detail': f'phi_convict_threshold: {phi_threshold}',
                            'recommendation': 'Low threshold may cause false positives (8-12 recommended)'
                        })
                except:
                    pass

        if not all_configs:
            builder.warning("Could not read cassandra.yaml configuration.")
            return builder.build(), {'status': 'no_data'}

        # === BUILD OUTPUT ===

        # Summary
        critical_count = len([i for i in security_issues if i['severity'] == 'critical'])
        warning_count = len([i for i in security_issues + performance_issues + reliability_issues
                            if i['severity'] == 'warning'])

        if critical_count > 0:
            builder.critical(f"**{critical_count} critical security issue(s) found**")
        elif warning_count > 0:
            builder.warning(f"**{warning_count} configuration warning(s) found**")
        else:
            builder.success("Configuration follows best practices.")
        builder.blank()

        # Security issues (deduplicated)
        if security_issues:
            builder.h4("Security Issues")
            seen_issues = set()

            critical_security = [i for i in security_issues if i['severity'] == 'critical']
            if critical_security:
                builder.critical("**Critical security vulnerabilities:**")
                for issue in critical_security:
                    issue_key = f"{issue['issue']}:{issue['detail']}"
                    if issue_key not in seen_issues:
                        seen_issues.add(issue_key)
                        builder.text(f"- **{issue['issue']}**: {issue['detail']}")
                builder.blank()

            warning_security = [i for i in security_issues if i['severity'] == 'warning']
            if warning_security:
                builder.warning("**Security warnings:**")
                for issue in warning_security:
                    issue_key = f"{issue['issue']}:{issue['detail']}"
                    if issue_key not in seen_issues:
                        seen_issues.add(issue_key)
                        builder.text(f"- **{issue['issue']}**: {issue['detail']}")
                builder.blank()

            builder.text("*Recommendations:*")
            seen_recommendations = set()
            for issue in security_issues:
                if issue['recommendation'] not in seen_recommendations:
                    seen_recommendations.add(issue['recommendation'])
                    builder.text(f"- {issue['recommendation']}")
            builder.blank()

        # Performance issues (deduplicated - show unique issues only)
        if performance_issues:
            builder.h4("Performance Tuning")
            seen_issues = set()
            for issue in performance_issues:
                issue_key = f"{issue['issue']}:{issue['detail']}"
                if issue_key not in seen_issues:
                    seen_issues.add(issue_key)
                    builder.text(f"- **{issue['issue']}**: {issue['detail']}")
                    builder.text(f"  _{issue['recommendation']}_")
            builder.blank()

        # Reliability issues (deduplicated)
        if reliability_issues:
            builder.h4("Reliability Settings")
            seen_issues = set()
            for issue in reliability_issues:
                issue_key = f"{issue['issue']}:{issue['detail']}"
                if issue_key not in seen_issues:
                    seen_issues.add(issue_key)
                    severity_icon = "⚠️ " if issue['severity'] == 'warning' else ""
                    builder.text(f"- {severity_icon}**{issue['issue']}**: {issue['detail']}")
                    builder.text(f"  _{issue['recommendation']}_")
            builder.blank()

        # Show key config values
        if all_configs:
            builder.h4("Key Configuration Values")
            config = all_configs[0]  # Sample from first node

            key_settings = [
                ('cluster_name', config.get('cluster_name', 'N/A')),
                ('num_tokens', config.get('num_tokens', 'N/A')),
                ('authenticator', config.get('authenticator', 'N/A')),
                ('authorizer', config.get('authorizer', 'N/A')),
                ('commitlog_sync', config.get('commitlog_sync', 'N/A')),
                ('concurrent_reads', config.get('concurrent_reads', 'N/A')),
                ('concurrent_writes', config.get('concurrent_writes', 'N/A')),
                ('memtable_allocation_type', config.get('memtable_allocation_type', 'N/A')),
                ('compaction_throughput_mb_per_sec', config.get('compaction_throughput_mb_per_sec', 'N/A')),
            ]

            config_table = [{'Setting': k, 'Value': str(v)} for k, v in key_settings]
            builder.table(config_table)
            builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_configs),
                'security_issues': len(security_issues),
                'performance_issues': len(performance_issues),
                'reliability_issues': len(reliability_issues),
                'authentication_enabled': not any(
                    'AllowAllAuthenticator' in str(c.get('authenticator', ''))
                    for c in all_configs
                ),
                'client_encryption_enabled': any(
                    c.get('client_encryption_options', {}).get('enabled', False)
                    for c in all_configs if isinstance(c.get('client_encryption_options'), dict)
                ),
                'internode_encryption_enabled': any(
                    c.get('server_encryption_options', {}).get('internode_encryption', 'none') != 'none'
                    for c in all_configs if isinstance(c.get('server_encryption_options'), dict)
                ),
                'issues': security_issues + performance_issues + reliability_issues
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Config audit check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Config audit check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

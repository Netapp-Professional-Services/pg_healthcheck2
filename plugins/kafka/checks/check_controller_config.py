"""
Controller Configuration Check for Kafka KRaft clusters.

Analyzes controller.properties for KRaft mode clusters to validate:
- Controller quorum configuration
- Voter configuration
- Network settings
- Security settings

Data sources:
- controller.properties (import and live via SSH)
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7  # High priority - controller config is critical for KRaft


# Best practice recommendations for controller configuration
CONTROLLER_CONFIG_RECOMMENDATIONS = {
    'controller.quorum.election.timeout.ms': {
        'recommended': 1000,
        'description': 'Quorum election timeout',
        'issue_if': lambda v: v < 500 or v > 5000,
        'recommendation': 'Set between 500-5000ms for optimal election speed'
    },
    'controller.quorum.fetch.timeout.ms': {
        'recommended': 2000,
        'description': 'Quorum fetch timeout',
        'issue_if': lambda v: v < 1000 or v > 10000,
        'recommendation': 'Set between 1000-10000ms'
    },
    'controller.quorum.retry.backoff.ms': {
        'recommended': 20,
        'description': 'Retry backoff',
        'issue_if': lambda v: v < 10 or v > 500,
        'recommendation': 'Keep low (10-100ms) for fast retries'
    },
    'controller.listener.names': {
        'required': True,
        'description': 'Controller listener names',
        'recommendation': 'Must be configured for controller communication'
    },
    'controller.quorum.voters': {
        'required': True,
        'description': 'Quorum voter configuration',
        'recommendation': 'Must list all controller voters'
    },
    'process.roles': {
        'required': True,
        'description': 'Process roles (controller, broker, or both)',
        'recommendation': 'Required for KRaft mode'
    },
    'node.id': {
        'required': True,
        'description': 'Unique node identifier',
        'recommendation': 'Must be unique across cluster'
    },
}


def run_controller_config_check(connector, settings):
    """
    Analyzes KRaft controller configuration for best practices.

    Checks:
    - Required settings presence
    - Quorum configuration
    - Timeout values
    - Voter consistency across nodes

    Args:
        connector: Kafka connector (live or import)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check if this is a KRaft cluster
    kafka_mode = getattr(connector, 'kafka_mode', None)
    if kafka_mode and kafka_mode.lower() != 'kraft':
        builder.h3("Controller Configuration")
        builder.note(
            "This cluster is running in ZooKeeper mode. "
            "Controller configuration checks are only applicable to KRaft clusters."
        )
        structured_data["controller_config"] = {
            "status": "skipped",
            "reason": "zookeeper_mode"
        }
        return builder.build(), structured_data

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Controller configuration check")
    if not available:
        return skip_msg, skip_data

    try:
        builder.h3("Controller Configuration Analysis")

        # === Collect controller configs from all nodes ===
        all_configs = {}
        errors = []

        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Read controller.properties
                command = "cat /etc/kafka/controller.properties 2>/dev/null || cat /opt/kafka/config/controller.properties 2>/dev/null || cat /opt/kafka/config/kraft/controller.properties 2>/dev/null || echo ''"
                results = connector.execute_ssh_on_all_hosts(command, "Read controller config")

                for result in results:
                    if result.get('host') == ssh_host and result.get('success'):
                        config_content = result.get('output', '')
                        if config_content.strip():
                            config = _parse_properties(config_content)
                            all_configs[broker_id] = config

            except Exception as e:
                logger.warning(f"Failed to read controller config from {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        if not all_configs:
            builder.note(
                "No controller configuration files found. This could mean:\n\n"
                "* Cluster is running in ZooKeeper mode\n"
                "* Controller config is in a non-standard location\n"
                "* SSH access to config files is restricted"
            )
            structured_data["controller_config"] = {
                "status": "skipped",
                "reason": "no_config_found"
            }
            return builder.build(), structured_data

        # === Analyze configurations ===
        issues = []
        warnings = []
        info = []

        # Check each node's config
        for broker_id, config in all_configs.items():
            node_issues = _check_controller_config(config, broker_id)
            issues.extend([i for i in node_issues if i['level'] == 'critical'])
            warnings.extend([i for i in node_issues if i['level'] == 'warning'])
            info.extend([i for i in node_issues if i['level'] == 'info'])

        # Check consistency across nodes
        consistency_issues = _check_config_consistency(all_configs)
        warnings.extend(consistency_issues)

        has_critical = len(issues) > 0
        has_warning = len(warnings) > 0

        # === Build report ===
        if has_critical:
            for issue in issues:
                builder.critical(f"**{issue['type'].replace('_', ' ').title()}:** {issue['message']}")
            builder.blank()

        if has_warning:
            for warning in warnings:
                builder.warning(f"**{warning['type'].replace('_', ' ').title()}:** {warning['message']}")
            builder.blank()

        # Configuration summary
        builder.h4("Controller Nodes")

        node_rows = []
        for broker_id, config in sorted(all_configs.items()):
            process_roles = config.get('process.roles', 'N/A')
            node_id = config.get('node.id', 'N/A')
            voters = config.get('controller.quorum.voters', 'N/A')
            # Truncate voters for display
            if len(str(voters)) > 50:
                voters = str(voters)[:47] + '...'

            node_rows.append({
                "Host": str(broker_id),
                "Node ID": str(node_id),
                "Process Roles": str(process_roles),
                "Quorum Voters": str(voters)
            })

        builder.table(node_rows)
        builder.blank()

        # Key settings (from first node)
        first_config = list(all_configs.values())[0]
        builder.h4("Key Controller Settings")

        key_settings_rows = []
        key_settings = [
            'process.roles',
            'node.id',
            'controller.quorum.voters',
            'controller.listener.names',
            'controller.quorum.election.timeout.ms',
            'controller.quorum.fetch.timeout.ms',
            'listeners',
            'inter.broker.listener.name',
        ]

        for setting in key_settings:
            value = first_config.get(setting, 'Not set')
            # Truncate long values
            if len(str(value)) > 60:
                value = str(value)[:57] + '...'
            key_settings_rows.append({
                "Setting": setting,
                "Value": str(value)
            })

        builder.table(key_settings_rows)
        builder.blank()

        # Errors
        if errors:
            builder.h4("Collection Errors")
            builder.warning(
                f"Could not collect controller config from {len(errors)} node(s):\n\n" +
                "\n".join([f"* Node {e['broker_id']}: {e['error']}" for e in errors])
            )

        # Recommendations
        if issues or warnings:
            recommendations = {}

            if has_critical:
                recommendations["critical"] = [
                    "**Fix missing required settings** - controller cannot function properly",
                    "**Verify process.roles** includes 'controller' for controller nodes",
                    "**Configure controller.quorum.voters** with all controller node addresses"
                ]

            if has_warning:
                recommendations["high"] = [
                    "**Review timeout settings** for your network conditions",
                    "**Ensure configuration consistency** across all controller nodes",
                    "**Update deprecated settings** to their modern equivalents"
                ]

            recommendations["general"] = [
                "All controller nodes should have identical quorum.voters configuration",
                "Use odd numbers of controllers (3 or 5) for optimal quorum",
                "Place controllers in different failure domains",
                "Monitor controller logs for election and quorum issues"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Controller configuration looks good.\n\n"
                f"Analyzed configuration from {len(all_configs)} node(s)."
            )

        # === Structured data ===
        structured_data["controller_config"] = {
            "status": "success",
            "nodes_checked": len(all_configs),
            "issues": issues,
            "warnings": warnings,
            "info": info,
            "has_critical_issues": has_critical,
            "has_warnings": has_warning,
            "errors": errors,
            "data": {
                "configs": {k: dict(v) for k, v in all_configs.items()}
            }
        }

    except Exception as e:
        import traceback
        logger.error(f"Controller config check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["controller_config"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data


def _parse_properties(content):
    """Parse Java properties file content."""
    props = {}
    for line in content.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            key, value = line.split('=', 1)
            props[key.strip()] = value.strip()
    return props


def _check_controller_config(config, broker_id):
    """Check a single controller configuration for issues."""
    issues = []

    # Check required settings
    for setting, info in CONTROLLER_CONFIG_RECOMMENDATIONS.items():
        if info.get('required'):
            if setting not in config or not config[setting]:
                issues.append({
                    'level': 'critical',
                    'type': 'missing_required_setting',
                    'broker_id': broker_id,
                    'setting': setting,
                    'message': f'Missing required setting: {setting}'
                })

    # Check value recommendations
    for setting, info in CONTROLLER_CONFIG_RECOMMENDATIONS.items():
        if setting in config and 'issue_if' in info:
            try:
                value = int(config[setting])
                if info['issue_if'](value):
                    issues.append({
                        'level': 'warning',
                        'type': 'suboptimal_setting',
                        'broker_id': broker_id,
                        'setting': setting,
                        'value': value,
                        'message': f'{setting}={value} - {info["recommendation"]}'
                    })
            except (ValueError, TypeError):
                pass

    # Check process.roles includes 'controller'
    process_roles = config.get('process.roles', '')
    if process_roles and 'controller' not in process_roles.lower():
        issues.append({
            'level': 'info',
            'type': 'not_a_controller',
            'broker_id': broker_id,
            'message': f'Node {broker_id} does not have controller role (roles: {process_roles})'
        })

    return issues


def _check_config_consistency(all_configs):
    """Check configuration consistency across nodes."""
    issues = []

    if len(all_configs) < 2:
        return issues

    # Settings that should be identical across all nodes
    consistent_settings = [
        'controller.quorum.voters',
        'controller.listener.names',
    ]

    configs_list = list(all_configs.items())
    first_id, first_config = configs_list[0]

    for setting in consistent_settings:
        first_value = first_config.get(setting)

        for broker_id, config in configs_list[1:]:
            other_value = config.get(setting)
            if first_value != other_value:
                issues.append({
                    'level': 'warning',
                    'type': 'inconsistent_config',
                    'setting': setting,
                    'message': f'{setting} differs between nodes: {first_id}={first_value}, {broker_id}={other_value}'
                })
                break  # Only report once per setting

    return issues

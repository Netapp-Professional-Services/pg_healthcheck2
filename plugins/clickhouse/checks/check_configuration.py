"""
ClickHouse Configuration Analysis Check

Analyzes server configuration for drift detection, security issues, and optimization opportunities.
More actionable than instacollector by providing context-specific recommendations.

Requirements:
- ClickHouse client access to system.server_settings
- Optional: system.build_options
"""

import logging
from typing import Dict, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.clickhouse.utils.qrylib import qry_configuration

logger = logging.getLogger(__name__)


# Check metadata
check_metadata = {
    'requires_api': False,
    'requires_ssh': False,
    'requires_connection': True,
    'description': 'Configuration analysis, drift detection, and optimization recommendations'
}


# Production best practice recommendations
RECOMMENDED_SETTINGS = {
    'max_concurrent_queries': {
        'min': 100,
        'recommended': 200,
        'description': 'Controls maximum number of simultaneous queries',
        'impact': 'High - affects concurrency and throughput'
    },
    'max_server_memory_usage': {
        'min_ratio': 0.7,  # Should be at least 70% of RAM
        'recommended_ratio': 0.8,  # Recommended 80% of RAM
        'description': 'Maximum server memory usage limit',
        'impact': 'Critical - prevents OOM but should use most available RAM'
    },
    'background_pool_size': {
        'min': 16,
        'recommended': 32,
        'description': 'Background operations thread pool size',
        'impact': 'High - affects merges and background operations'
    },
    'max_table_size_to_drop': {
        'min': 50 * 1024**3,  # 50GB minimum
        'description': 'Safety limit for table drops',
        'impact': 'Medium - prevents accidental large table deletion'
    }
}


def get_weight():
    """Returns the importance score for this check."""
    return 8  # High priority - configuration affects all aspects of operation


def run_check_configuration(connector, settings) -> Tuple[str, Dict]:
    """
    Analyze ClickHouse configuration for issues and optimization opportunities.

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("Configuration Analysis & Optimization")
    builder.para(
        "Analysis of ClickHouse server configuration for drift detection, "
        "security issues, and optimization opportunities."
    )

    try:
        # 1. Get version information
        version_query = qry_configuration.get_version_query(connector)
        version_result = connector.execute_query(version_query)
        version = version_result[0][0] if version_result else "Unknown"

        builder.h4("Server Information")
        builder.para(f"**ClickHouse Version:** {version}")
        builder.blank()

        # 2. Get changed settings
        changed_query = qry_configuration.get_changed_server_settings_query(connector)
        changed_result = connector.execute_query(changed_query)

        # 3. Get critical settings
        critical_query = qry_configuration.get_critical_settings_query(connector)
        critical_result = connector.execute_query(critical_query)

        # 4. Get security settings
        security_query = qry_configuration.get_security_settings_query(connector)
        security_result = connector.execute_query(security_query)

        # 5. Get resource limit settings
        resource_query = qry_configuration.get_resource_limit_settings_query(connector)
        resource_result = connector.execute_query(resource_query)

        # Process changed settings
        changed_settings = []
        if changed_result:
            for row in changed_result:
                changed_settings.append({
                    'name': row[0],
                    'value': str(row[1]),
                    'default': str(row[2]),
                    'description': row[4] if len(row) > 4 else '',
                    'type': row[5] if len(row) > 5 else ''
                })

        # Analyze configuration drift
        builder.h4("Configuration Drift Analysis")

        if not changed_settings:
            builder.success("✅ All settings are at default values - no configuration drift detected")
        else:
            builder.note(
                f"**{len(changed_settings)} setting(s) differ from defaults**\n\n"
                "Configuration drift detected. Review changes to ensure they are intentional."
            )

            # Display changed settings with context
            changed_table = []
            for setting in changed_settings[:20]:  # Show top 20
                changed_table.append({
                    "Setting": setting['name'],
                    "Current Value": setting['value'][:50] + "..." if len(setting['value']) > 50 else setting['value'],
                    "Default Value": setting['default'][:50] + "..." if len(setting['default']) > 50 else setting['default'],
                    "Type": setting['type']
                })
            builder.table(changed_table)

            if len(changed_settings) > 20:
                builder.para(f"...and {len(changed_settings) - 20} more changed settings")

        builder.blank()

        # Analyze critical settings
        critical_settings_data = []
        if critical_result:
            builder.h4("Critical Settings Review")

            for row in critical_result:
                setting_name = row[0]
                setting_value = row[1]
                default_value = row[2]
                is_changed = row[3]

                critical_settings_data.append({
                    'name': setting_name,
                    'value': str(setting_value),
                    'default': str(default_value),
                    'changed': is_changed
                })

            # Check against recommended values
            issues, recommendations_list = _analyze_critical_settings(critical_settings_data)

            if issues:
                builder.warning(
                    "⚠️ **Configuration Issues Detected**\n\n" +
                    "\n".join(f"- {issue}" for issue in issues)
                )
                builder.blank()

            # Display critical settings
            critical_table = []
            for setting in critical_settings_data:
                status = "🔴" if setting['name'] in [i.split(':')[0] for i in issues] else ("⚠️" if setting['changed'] else "✅")
                critical_table.append({
                    "Status": status,
                    "Setting": setting['name'],
                    "Current Value": setting['value'][:40] + "..." if len(setting['value']) > 40 else setting['value'],
                    "Changed": "Yes" if setting['changed'] else "No"
                })
            builder.table(critical_table)
            builder.blank()

        # Security settings analysis
        security_settings = []
        security_issues = []

        if security_result:
            builder.h4("Security Configuration")

            for row in security_result:
                setting_name = row[0]
                setting_value = str(row[1])
                is_changed = row[3]

                security_settings.append({
                    'name': setting_name,
                    'value': setting_value,
                    'changed': is_changed
                })

                # Check for potential security issues
                if 'password' in setting_name.lower() and setting_value and setting_value != '':
                    security_issues.append(f"Password visible in {setting_name}")

                if 'listen' in setting_name.lower() and ('0.0.0.0' in setting_value or '::' in setting_value):
                    security_issues.append(f"{setting_name} is listening on all interfaces")

            if security_issues:
                builder.warning(
                    "⚠️ **Potential Security Issues**\n\n" +
                    "\n".join(f"- {issue}" for issue in security_issues)
                )
                builder.blank()

            # Display security settings
            sec_table = []
            for setting in security_settings[:15]:
                sec_table.append({
                    "Setting": setting['name'],
                    "Value": setting['value'][:50] + "..." if len(setting['value']) > 50 else setting['value'],
                    "Changed": "Yes" if setting['changed'] else "No"
                })
            builder.table(sec_table)
            builder.blank()

        # Resource configuration analysis
        resource_settings = []
        resource_issues = []

        if resource_result:
            builder.h4("Resource Limit Configuration")

            for row in resource_result:
                setting_name = row[0]
                setting_value = str(row[1])
                is_changed = row[3]

                resource_settings.append({
                    'name': setting_name,
                    'value': setting_value,
                    'changed': is_changed
                })

            # Analyze for common issues
            for setting in resource_settings:
                if setting['name'] == 'max_concurrent_queries':
                    try:
                        value = int(setting['value'])
                        if value < 100:
                            resource_issues.append(
                                f"max_concurrent_queries ({value}) is low - may limit concurrency"
                            )
                    except:
                        pass

            if resource_issues:
                builder.note(
                    "**Resource Configuration Notes:**\n\n" +
                    "\n".join(f"- {issue}" for issue in resource_issues)
                )
                builder.blank()

        # Generate comprehensive recommendations
        recommendations = _generate_configuration_recommendations(
            changed_settings,
            critical_settings_data,
            security_issues,
            resource_issues
        )

        if recommendations['critical'] or recommendations['high']:
            builder.recs(recommendations)
        elif changed_settings:
            builder.note(
                "**Configuration Baseline Established**\n\n"
                "Review changed settings periodically for drift. "
                "Consider documenting intentional changes."
            )
        else:
            builder.success("✅ Configuration is at recommended state")

        # Structured data for trend tracking
        structured_data["configuration_drift"] = {
            "status": "success",
            "data": changed_settings,
            "metadata": {
                "version": version,
                "total_changed_settings": len(changed_settings),
                "security_issues": len(security_issues),
                "resource_issues": len(resource_issues),
                "timestamp": connector.get_current_timestamp()
            }
        }

        if critical_settings_data:
            structured_data["critical_settings"] = {
                "status": "success",
                "data": critical_settings_data,
                "metadata": {
                    "count": len(critical_settings_data),
                    "timestamp": connector.get_current_timestamp()
                }
            }

    except Exception as e:
        logger.error(f"Configuration check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["configuration_drift"] = {
            "status": "error",
            "data": [],
            "error_message": str(e)
        }

    return builder.build(), structured_data


def _analyze_critical_settings(critical_settings):
    """Analyze critical settings against best practices."""
    issues = []
    recommendations = []

    for setting in critical_settings:
        name = setting['name']
        value = setting['value']

        # Check against recommended values
        if name in RECOMMENDED_SETTINGS:
            rec = RECOMMENDED_SETTINGS[name]

            try:
                if 'min' in rec:
                    current_val = int(value)
                    if current_val < rec['min']:
                        issues.append(
                            f"{name}: {current_val} is below minimum recommended value {rec['min']}"
                        )
                        recommendations.append(
                            f"Increase {name} to at least {rec.get('recommended', rec['min'])}"
                        )
            except ValueError:
                pass  # Can't convert to int, skip numeric comparison

    return issues, recommendations


def _generate_configuration_recommendations(changed_settings, critical_settings, security_issues, resource_issues):
    """Generate actionable configuration recommendations."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if security_issues:
        recs["critical"].extend([
            f"Security issue detected: {issue}" for issue in security_issues
        ])
        recs["critical"].extend([
            "Review security settings to ensure proper access control",
            "Consider restricting listen_host to specific interfaces",
            "Ensure passwords are not exposed in configuration files",
            "Use authentication and SSL/TLS for production deployments"
        ])

    if len(changed_settings) > 50:
        recs["high"].append(
            f"{len(changed_settings)} settings changed from defaults - extensive configuration drift detected"
        )
        recs["high"].append(
            "Document all configuration changes for audit trail"
        )

    if resource_issues:
        recs["high"].extend([
            f"Resource configuration issue: {issue}" for issue in resource_issues
        ])

    # General best practices
    recs["general"].extend([
        "Maintain configuration documentation for all non-default settings",
        "Use configuration management tools (Ansible, Chef, Puppet) for consistency",
        "Review configuration changes before applying to production",
        "Test configuration changes in staging environment first",
        "Monitor configuration drift over time",
        "Set max_server_memory_usage to 80% of available RAM",
        "Configure appropriate values for max_concurrent_queries based on workload",
        "Enable query logging for auditing and performance analysis",
        "Configure backup settings and retention policies",
        "Set appropriate table/partition size limits for safety"
    ])

    if changed_settings:
        recs["general"].append(
            "Periodically review changed settings to ensure they remain appropriate"
        )

    return recs

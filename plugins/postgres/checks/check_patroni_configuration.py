"""
Patroni Configuration Check

Analyzes Patroni configuration for best practices and optimal settings.
Provides recommendations for improving cluster reliability, failover behavior, and data safety.

Uses Patroni REST API with SSH fallback following the Instaclustr pattern.

Data Sources:
- GET /config - Dynamic configuration stored in DCS

Output:
- Configuration parameter analysis
- Best practice recommendations
- Warning/critical misconfigurations
- Actionable advice for optimization
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.patroni_client import create_patroni_client_from_settings
from plugins.postgres.utils.patroni_helpers import (
    skip_if_not_patroni,
    build_error_result
)

logger = logging.getLogger(__name__)


# Best practice configuration values
BEST_PRACTICES = {
    'ttl': {
        'recommended': 30,
        'min': 20,
        'max': 60,
        'description': 'How long (seconds) the leader holds the DCS lock',
        'impact': 'Too low = unnecessary failovers. Too high = slow failure detection.'
    },
    'loop_wait': {
        'recommended': 10,
        'min': 5,
        'max': 30,
        'description': 'How often (seconds) Patroni checks leader status',
        'impact': 'Too low = increased DCS load. Too high = slower detection of issues.'
    },
    'retry_timeout': {
        'recommended': 10,
        'min': 5,
        'max': 30,
        'description': 'Timeout (seconds) for DCS and PostgreSQL operations',
        'impact': 'Too low = false failures. Too high = delayed error detection.'
    },
    'maximum_lag_on_failover': {
        'recommended': 1048576,  # 1MB
        'max': 104857600,  # 100MB
        'description': 'Maximum replication lag (bytes) allowed for failover candidate',
        'impact': 'Too high = potential data loss. Too low = may prevent failover.'
    },
    'synchronous_mode': {
        'recommended': True,
        'description': 'Enable zero data loss with synchronous replication',
        'impact': 'Disabled = risk of data loss on failover. Enabled = guaranteed consistency.'
    },
    'synchronous_mode_strict': {
        'recommended': False,
        'description': 'Require synchronous replica for writes',
        'impact': 'True = no writes if sync replica unavailable (favors consistency over availability).'
    }
}


def check_patroni_configuration(connector, settings: Dict) -> Tuple[str, Dict]:
    """
    Check Patroni configuration against best practices.

    Analyzes key configuration parameters and provides recommendations
    for optimization, reliability, and data safety.

    Args:
        connector: PostgreSQL connector with environment detection
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Patroni Configuration Analysis")

    # Skip if not Patroni
    skip_result = skip_if_not_patroni(connector)
    if skip_result:
        return skip_result

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Fetch configuration from Patroni API
        config_data = _fetch_patroni_configuration(settings)

        if not config_data.get('success'):
            return build_error_result(
                'patroni_configuration',
                config_data.get('error', 'Could not fetch configuration'),
                builder
            )

        config = config_data.get('config', {})

        # Analyze configuration
        analysis = _analyze_configuration(config)

        # Build output with actionable advice
        _build_configuration_output(builder, config, analysis)

        # Build findings for trend storage
        findings = {
            'patroni_configuration': {
                'status': 'success',
                'timestamp': timestamp,
                'config': config,
                'analysis': analysis,
                'source': 'patroni_api'
            }
        }

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Failed to check Patroni configuration: {e}", exc_info=True)
        return build_error_result(
            'patroni_configuration',
            str(e),
            builder
        )


def _fetch_patroni_configuration(settings: Dict) -> Dict:
    """
    Fetch configuration from Patroni REST API.

    Args:
        settings: Configuration dictionary

    Returns:
        Dictionary with success flag and config data
    """
    client = create_patroni_client_from_settings(settings)
    if not client:
        return {'success': False, 'error': 'Could not create Patroni client - check configuration'}

    try:
        success, result = client.get_config()
        client.close()

        if not success:
            return {'success': False, 'error': result.get('error', 'Unknown error')}

        config = result.get('data', {})

        return {'success': True, 'config': config}

    except Exception as e:
        logger.debug(f"Could not fetch configuration via API: {e}")
        return {'success': False, 'error': str(e)}


def _analyze_configuration(config: Dict) -> Dict:
    """
    Analyze Patroni configuration against best practices.

    Args:
        config: Configuration dictionary from Patroni

    Returns:
        Analysis dictionary with issues and recommendations
    """
    issues = []
    recommendations = []
    config_score = 100

    # Extract key parameters
    ttl = config.get('ttl', config.get('loop_wait', 30))
    loop_wait = config.get('loop_wait', 10)
    retry_timeout = config.get('retry_timeout', 10)
    max_lag = config.get('maximum_lag_on_failover', 1048576)
    sync_mode = config.get('synchronous_mode', False)
    sync_mode_strict = config.get('synchronous_mode_strict', False)

    # Analyze TTL
    ttl_bp = BEST_PRACTICES['ttl']
    if ttl < ttl_bp['min']:
        issues.append({
            'parameter': 'ttl',
            'severity': 'warning',
            'current_value': ttl,
            'recommended_value': ttl_bp['recommended'],
            'message': f"TTL is very low ({ttl}s). This can cause unnecessary failovers.",
            'action': f"Consider increasing to {ttl_bp['recommended']}s or higher to reduce false failovers."
        })
        config_score -= 10
    elif ttl > ttl_bp['max']:
        issues.append({
            'parameter': 'ttl',
            'severity': 'info',
            'current_value': ttl,
            'recommended_value': ttl_bp['recommended'],
            'message': f"TTL is high ({ttl}s). Failover detection will be slower.",
            'action': f"Consider reducing to {ttl_bp['recommended']}s for faster failure detection."
        })
        config_score -= 5

    # Analyze loop_wait
    loop_bp = BEST_PRACTICES['loop_wait']
    if loop_wait < loop_bp['min']:
        issues.append({
            'parameter': 'loop_wait',
            'severity': 'warning',
            'current_value': loop_wait,
            'recommended_value': loop_bp['recommended'],
            'message': f"Loop wait is very low ({loop_wait}s). This increases DCS load.",
            'action': f"Consider increasing to {loop_bp['recommended']}s to reduce DCS query frequency."
        })
        config_score -= 10

    # Analyze maximum_lag_on_failover
    lag_bp = BEST_PRACTICES['maximum_lag_on_failover']
    lag_mb = max_lag / (1024 * 1024)
    if max_lag > lag_bp['max']:
        issues.append({
            'parameter': 'maximum_lag_on_failover',
            'severity': 'high',
            'current_value': f"{lag_mb:.2f} MB",
            'recommended_value': f"{lag_bp['recommended']/(1024*1024):.2f} MB",
            'message': f"Maximum lag is very high ({lag_mb:.0f} MB). Risk of data loss on failover.",
            'action': "Reduce to 1-10 MB to minimize potential data loss during unplanned failovers."
        })
        config_score -= 15

    # Analyze synchronous mode
    if not sync_mode:
        recommendations.append({
            'parameter': 'synchronous_mode',
            'severity': 'info',
            'current_value': False,
            'recommended_value': True,
            'message': "Synchronous mode is disabled. There is a risk of data loss on failover.",
            'action': "Enable synchronous_mode for zero data loss (RPO=0) if your use case requires it."
        })
        config_score -= 10

    # Analyze synchronous_mode_strict
    if sync_mode and sync_mode_strict:
        recommendations.append({
            'parameter': 'synchronous_mode_strict',
            'severity': 'info',
            'current_value': True,
            'recommended_value': False,
            'message': "Strict synchronous mode is enabled. Writes will fail if no sync replica is available.",
            'action': "This favors consistency over availability. Ensure this aligns with your requirements."
        })

    return {
        'config_score': max(0, config_score),
        'issues': issues,
        'recommendations': recommendations,
        'parameters': {
            'ttl': ttl,
            'loop_wait': loop_wait,
            'retry_timeout': retry_timeout,
            'maximum_lag_on_failover': max_lag,
            'synchronous_mode': sync_mode,
            'synchronous_mode_strict': sync_mode_strict
        }
    }


def _build_configuration_output(
    builder: CheckContentBuilder,
    config: Dict,
    analysis: Dict
):
    """
    Build AsciiDoc output for configuration analysis with actionable advice.

    Args:
        builder: CheckContentBuilder instance
        config: Configuration dictionary
        analysis: Analysis dictionary
    """
    # Configuration score
    config_score = analysis['config_score']
    params = analysis['parameters']

    builder.text("*Configuration Health Score*")
    builder.blank()
    builder.text(f"Score: *{config_score}/100*")
    builder.blank()

    if config_score >= 90:
        builder.note("Your Patroni configuration follows best practices.")
    elif config_score >= 70:
        builder.warning("Your configuration is good but could be optimized. Review recommendations below.")
    else:
        builder.critical("Your configuration has issues that should be addressed for optimal cluster reliability.")

    builder.blank()

    # Key parameters table
    builder.text("*Current Configuration*")
    builder.blank()

    param_table = [
        {
            'Parameter': 'TTL',
            'Current Value': f"{params['ttl']}s",
            'Recommended': f"{BEST_PRACTICES['ttl']['recommended']}s",
            'Status': _get_status_icon(params['ttl'], BEST_PRACTICES['ttl'])
        },
        {
            'Parameter': 'Loop Wait',
            'Current Value': f"{params['loop_wait']}s",
            'Recommended': f"{BEST_PRACTICES['loop_wait']['recommended']}s",
            'Status': _get_status_icon(params['loop_wait'], BEST_PRACTICES['loop_wait'])
        },
        {
            'Parameter': 'Retry Timeout',
            'Current Value': f"{params['retry_timeout']}s",
            'Recommended': f"{BEST_PRACTICES['retry_timeout']['recommended']}s",
            'Status': _get_status_icon(params['retry_timeout'], BEST_PRACTICES['retry_timeout'])
        },
        {
            'Parameter': 'Max Lag on Failover',
            'Current Value': f"{params['maximum_lag_on_failover']/(1024*1024):.2f} MB",
            'Recommended': f"{BEST_PRACTICES['maximum_lag_on_failover']['recommended']/(1024*1024):.2f} MB",
            'Status': '✅ OK' if params['maximum_lag_on_failover'] <= BEST_PRACTICES['maximum_lag_on_failover']['max'] else '⚠️ HIGH'
        },
        {
            'Parameter': 'Synchronous Mode',
            'Current Value': 'Enabled' if params['synchronous_mode'] else 'Disabled',
            'Recommended': 'Enabled (for zero data loss)',
            'Status': '✅ OK' if params['synchronous_mode'] else 'ℹ️  Consider'
        }
    ]

    builder.table(param_table)
    builder.blank()

    # Issues section with detailed explanations - group by severity
    issues = analysis['issues']
    if issues:
        # Group issues by severity
        high_issues = [i for i in issues if i.get('severity') == 'high']
        warning_issues = [i for i in issues if i.get('severity') == 'warning']
        info_issues = [i for i in issues if i.get('severity') == 'info']

        # Format issue details for admonition blocks
        if high_issues:
            details = []
            for issue in high_issues:
                details.append(f"*{issue['parameter']}*")
                details.append(f"Current: `{issue['current_value']}`")
                details.append(f"Recommended: `{issue['recommended_value']}`")
                details.append(f"Issue: {issue['message']}")
                details.append(f"*Action*: {issue['action']}")
            builder.critical_issue("High Priority Configuration Issues", details)

        if warning_issues:
            details = []
            for issue in warning_issues:
                details.append(f"*{issue['parameter']}*")
                details.append(f"Current: `{issue['current_value']}`")
                details.append(f"Recommended: `{issue['recommended_value']}`")
                details.append(f"Issue: {issue['message']}")
                details.append(f"*Action*: {issue['action']}")
            builder.warning_issue("Configuration Warnings", details)

        if info_issues:
            details = []
            for issue in info_issues:
                details.append(f"*{issue['parameter']}*")
                details.append(f"Current: `{issue['current_value']}`")
                details.append(f"Recommended: `{issue['recommended_value']}`")
                details.append(f"Issue: {issue['message']}")
                details.append(f"*Action*: {issue['action']}")
            builder.note(f"**Configuration Notes**\n\n" + "\n\n".join(details))

    # Recommendations section
    recommendations = analysis['recommendations']
    if recommendations:
        builder.text("*💡 Recommendations*")
        builder.blank()

        for rec in recommendations:
            builder.text(f"• *{rec['parameter']}*: {rec['message']}")
            builder.text(f"  Action: {rec['action']}")
            builder.blank()

    # Parameter explanations
    builder.text("*Understanding Key Parameters*")
    builder.blank()

    builder.text("*TTL (Time to Live)*")
    builder.text(f"{BEST_PRACTICES['ttl']['description']}")
    builder.text(f"Impact: {BEST_PRACTICES['ttl']['impact']}")
    builder.blank()

    builder.text("*Loop Wait*")
    builder.text(f"{BEST_PRACTICES['loop_wait']['description']}")
    builder.text(f"Impact: {BEST_PRACTICES['loop_wait']['impact']}")
    builder.blank()

    builder.text("*Maximum Lag on Failover*")
    builder.text(f"{BEST_PRACTICES['maximum_lag_on_failover']['description']}")
    builder.text(f"Impact: {BEST_PRACTICES['maximum_lag_on_failover']['impact']}")
    builder.blank()

    builder.text("*Synchronous Mode*")
    builder.text(f"{BEST_PRACTICES['synchronous_mode']['description']}")
    builder.text(f"Impact: {BEST_PRACTICES['synchronous_mode']['impact']}")
    builder.blank()

    # How to update configuration
    builder.text("*How to Update Configuration*")
    builder.blank()
    builder.text("To modify Patroni configuration:")
    builder.text("1. Edit the configuration via DCS:")
    builder.text("   `patronictl -c /etc/patroni/patroni.yml edit-config`")
    builder.text("2. Or update the YAML file and reload:")
    builder.text("   `systemctl reload patroni`")
    builder.text("3. Verify changes:")
    builder.text("   `patronictl -c /etc/patroni/patroni.yml show-config`")
    builder.blank()
    builder.warning("Configuration changes via DCS take effect immediately. Plan changes during maintenance windows.")
    builder.blank()


def _get_status_icon(value: int, bp: Dict) -> str:
    """
    Get status icon based on value comparison with best practice.

    Args:
        value: Current value
        bp: Best practice dictionary

    Returns:
        Status icon string
    """
    if 'min' in bp and 'max' in bp:
        if bp['min'] <= value <= bp['max']:
            return '✅ OK'
        else:
            return '⚠️ Suboptimal'
    elif 'recommended' in bp:
        if value == bp['recommended']:
            return '✅ Optimal'
        else:
            return 'ℹ️  Consider'

    return 'ℹ️  Review'

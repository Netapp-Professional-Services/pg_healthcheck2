"""
Read Repair Settings Check

Queries system_schema.tables to analyze read repair configurations.

Read repair is critical for eventual consistency:
- read_repair_chance: Probability of read repair on non-quorum reads
- dclocal_read_repair_chance: Probability of read repair within datacenter

Recommendations:
- Most tables should have dclocal_read_repair_chance = 0.1 (10%)
- read_repair_chance should usually be 0 (use dclocal instead)
- High read repair values can impact read latency

CQL-only check - works on managed Instaclustr clusters.
Returns structured data compatible with trend analysis.
"""

import logging
from datetime import datetime
from typing import Dict, List
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def check_read_repair_settings(connector, settings):
    """
    Analyze read repair settings across all user tables

    Checks for:
    - Tables with non-standard read_repair_chance
    - Tables with suboptimal dclocal_read_repair_chance
    - Tables with both settings enabled (redundant)
    - Distribution of settings across keyspaces

    Args:
        connector: Cassandra connector with active session
        settings: Configuration settings

    Returns:
        Tuple of (adoc_content, structured findings dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Read Repair Settings")

    if not connector or not connector.session:
        builder.error("❌ No active database connection")
        return builder.build(), {
            'read_repair_settings': {
                'status': 'error',
                'error_message': 'No active database connection'
            }
        }

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # System keyspaces to exclude
        system_keyspaces = {
            'system', 'system_schema', 'system_auth',
            'system_distributed', 'system_traces', 'system_views',
            'system_virtual_schema'
        }

        # Query all tables (filtering in Python since Cassandra doesn't support WHERE NOT IN)
        query = """
        SELECT
            keyspace_name,
            table_name,
            read_repair_chance,
            dclocal_read_repair_chance
        FROM system_schema.tables
        """

        result = connector.session.execute(query)
        # Filter out system keyspaces in Python
        tables = [row for row in result if row['keyspace_name'] not in system_keyspaces]

        if not tables:
            builder.warning("⚠️ No user tables found")
            return builder.build(), {
                'read_repair_settings': {
                    'read_repair_distribution': {
                        'status': 'success',
                        'data': [],
                        'message': 'No user tables found'
                    }
                }
            }

        # Analyze read repair settings
        findings = _analyze_read_repair(tables, timestamp)

        # Add summary to builder
        total_tables = findings['read_repair_distribution']['total_tables']
        non_standard_count = findings['read_repair_distribution']['non_standard_count']

        if non_standard_count == 0:
            builder.success(f"✅ All {total_tables} table(s) have recommended read repair settings")
        else:
            builder.warning(f"⚠️ {non_standard_count} of {total_tables} table(s) have non-recommended read repair settings")

            # Add explanation
            builder.blank()
            builder.text("*Why This Matters:*")
            builder.text("In Cassandra 3.0+, read repair settings were deprecated in favor of automatic read repair. ")
            builder.text("The recommended setting is `read_repair = 'NONE'` as Cassandra now handles read repair automatically ")
            builder.text("during reads when inconsistencies are detected.")
            builder.blank()

            # Add details
            builder.text("*Current Configuration:*")
            for setting_type, data in findings.items():
                if setting_type.startswith('read_repair_') and data.get('count', 0) > 0:
                    setting_name = setting_type.replace('read_repair_', '').replace('_', ' ').title()
                    builder.text(f"- {setting_name}: {data['count']} table(s)")
            builder.blank()

            # Add recommendations
            builder.text("*Recommended Actions:*")
            builder.text("1. No immediate action required - Cassandra handles read repair automatically")
            builder.text("2. For new tables, use: `WITH read_repair = 'NONE'`")
            builder.text("3. Existing tables will continue to work, but explicit settings are ignored in modern Cassandra versions")

        return builder.build(), {'read_repair_settings': findings}

    except Exception as e:
        logger.error(f"Failed to analyze read repair settings: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"❌ Failed to analyze read repair settings: {e}")
        return builder.build(), {
            'read_repair_settings': {
                'status': 'error',
                'error_message': str(e),
                'data': []
            }
        }


def _analyze_read_repair(tables: List[Dict], timestamp: str) -> Dict:
    """
    Analyze read repair configurations

    Args:
        tables: List of table metadata from system_schema.tables
        timestamp: ISO 8601 timestamp

    Returns:
        Structured findings with read repair analysis
    """
    # Track settings combinations
    settings_distribution = {}
    non_standard_tables = []
    optimal_tables = []
    both_enabled_tables = []

    # Recommended settings:
    # - read_repair_chance = 0
    # - dclocal_read_repair_chance = 0.1
    RECOMMENDED_RR = 0.0
    RECOMMENDED_DCLOCAL = 0.1

    for table in tables:
        ks = table.get('keyspace_name')
        table_name = table.get('table_name')
        rr_chance = table.get('read_repair_chance', 0.0)
        dclocal_rr_chance = table.get('dclocal_read_repair_chance', 0.0)

        # Track distribution
        key = (rr_chance, dclocal_rr_chance)
        if key not in settings_distribution:
            settings_distribution[key] = {
                'read_repair_chance': rr_chance,
                'dclocal_read_repair_chance': dclocal_rr_chance,
                'table_count': 0,
                'tables': []
            }
        settings_distribution[key]['table_count'] += 1
        settings_distribution[key]['tables'].append(f"{ks}.{table_name}")

        # Check for optimal settings
        is_optimal = (rr_chance == RECOMMENDED_RR and dclocal_rr_chance == RECOMMENDED_DCLOCAL)

        if is_optimal:
            optimal_tables.append({
                'keyspace': ks,
                'table': table_name,
                'read_repair_chance': rr_chance,
                'dclocal_read_repair_chance': dclocal_rr_chance
            })
        else:
            # Flag non-standard settings
            recommendation = _get_recommendation(rr_chance, dclocal_rr_chance)

            non_standard_tables.append({
                'keyspace': ks,
                'table': table_name,
                'read_repair_chance': rr_chance,
                'dclocal_read_repair_chance': dclocal_rr_chance,
                'recommendation': recommendation,
                'severity': _get_severity(rr_chance, dclocal_rr_chance)
            })

        # Check if both are enabled (often redundant)
        if rr_chance > 0 and dclocal_rr_chance > 0:
            both_enabled_tables.append({
                'keyspace': ks,
                'table': table_name,
                'read_repair_chance': rr_chance,
                'dclocal_read_repair_chance': dclocal_rr_chance,
                'issue': 'Both read repair settings enabled - may be redundant'
            })

    # Build distribution data
    distribution_data = []
    for (rr, dclocal), info in sorted(settings_distribution.items()):
        is_recommended = (rr == RECOMMENDED_RR and dclocal == RECOMMENDED_DCLOCAL)
        distribution_data.append({
            'read_repair_chance': rr,
            'dclocal_read_repair_chance': dclocal,
            'table_count': info['table_count'],
            'percentage': round((info['table_count'] / len(tables) * 100), 1),
            'is_recommended': is_recommended,
            'timestamp': timestamp
        })

    # Determine overall status
    non_standard_count = len(non_standard_tables)
    overall_status = 'success' if non_standard_count == 0 else 'warning'

    # Sort non-standard tables by severity
    non_standard_tables.sort(key=lambda x: x['severity'], reverse=True)

    return {
        'read_repair_distribution': {
            'status': 'success',
            'data': distribution_data,
            'total_tables': len(tables),
            'optimal_count': len(optimal_tables),
            'non_standard_count': non_standard_count,
            'metadata': {
                'query_timestamp': timestamp,
                'source': 'system_schema.tables',
                'recommended_settings': {
                    'read_repair_chance': RECOMMENDED_RR,
                    'dclocal_read_repair_chance': RECOMMENDED_DCLOCAL
                }
            }
        },
        'non_standard_settings': {
            'status': overall_status,
            'data': non_standard_tables,
            'count': non_standard_count,
            'message': f'{non_standard_count} table(s) have non-recommended read repair settings' if non_standard_count > 0 else 'All tables have recommended read repair settings'
        },
        'both_enabled': {
            'status': 'warning' if len(both_enabled_tables) > 0 else 'success',
            'data': both_enabled_tables,
            'count': len(both_enabled_tables),
            'message': f'{len(both_enabled_tables)} table(s) have both read repair settings enabled' if both_enabled_tables else 'No tables have redundant read repair configuration'
        }
    }


def _get_recommendation(rr_chance: float, dclocal_rr_chance: float) -> str:
    """
    Get recommendation for non-standard read repair settings

    Args:
        rr_chance: read_repair_chance value
        dclocal_rr_chance: dclocal_read_repair_chance value

    Returns:
        Recommendation string
    """
    if rr_chance > 0 and dclocal_rr_chance == 0:
        return "Use dclocal_read_repair_chance (0.1) instead of read_repair_chance for better multi-DC performance"

    if rr_chance == 0 and dclocal_rr_chance == 0:
        return "Consider enabling dclocal_read_repair_chance (0.1) for better consistency"

    if dclocal_rr_chance > 0.2:
        return f"dclocal_read_repair_chance ({dclocal_rr_chance}) is high - may impact read latency. Consider 0.1"

    if rr_chance > 0 and dclocal_rr_chance > 0:
        return "Both settings enabled - disable read_repair_chance and use dclocal_read_repair_chance only"

    return "Non-standard configuration - review for optimization"


def _get_severity(rr_chance: float, dclocal_rr_chance: float) -> int:
    """
    Get severity score for non-standard settings (higher = more severe)

    Args:
        rr_chance: read_repair_chance value
        dclocal_rr_chance: dclocal_read_repair_chance value

    Returns:
        Severity score (0-10)
    """
    severity = 0

    # High read_repair_chance is more severe
    if rr_chance > 0.1:
        severity += 5
    elif rr_chance > 0:
        severity += 3

    # Very high dclocal is concerning
    if dclocal_rr_chance > 0.3:
        severity += 4
    elif dclocal_rr_chance > 0.2:
        severity += 2

    # Both enabled is wasteful
    if rr_chance > 0 and dclocal_rr_chance > 0:
        severity += 2

    # No read repair at all
    if rr_chance == 0 and dclocal_rr_chance == 0:
        severity += 1

    return severity


# Register check metadata
check_metadata = {
    'name': 'read_repair_settings',
    'description': 'Analyze read repair settings for consistency tuning',
    'category': 'configuration',
    'requires_api': False,
    'requires_ssh': False,
    'requires_cql': True
}

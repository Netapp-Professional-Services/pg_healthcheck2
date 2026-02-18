"""
Table Statistics Check

Queries system_schema.tables to gather comprehensive table metadata.

Provides:
- Table count per keyspace
- Compaction strategy distribution
- Bloom filter false positive rates
- CDC enabled tables
- Min/Max TTL settings

CQL-only check - works on managed Instaclustr clusters.
Returns structured data compatible with trend analysis.
"""

import logging

from datetime import datetime
from typing import Dict, List

from table_count_checks import check_table_counts
from plugins.common.check_helpers import CheckContentBuilder
from plugins.cassandra.utils.keyspace_filter import KeyspaceFilter

logger = logging.getLogger(__name__)


def check_table_statistics(connector, settings):
    """
    Gather comprehensive table statistics via CQL

    Queries system_schema.tables to analyze:
    - Table counts per keyspace
    - Compaction strategies in use
    - Bloom filter configurations
    - CDC settings
    - TTL configurations

    Args:
        connector: Cassandra connector with active session
        settings: Configuration settings

    Returns:
        Tuple of (adoc_content, structured findings dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Table Statistics")

    if not connector or not connector.session:
        builder.error("❌ No active database connection")
        return builder.build(), {
            'table_statistics': {
                'status': 'error',
                'error_message': 'No active database connection'
            }
        }

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Query all tables (filtering in Python since Cassandra doesn't support WHERE NOT IN)
        query = """
        SELECT
            keyspace_name,
            table_name,
            bloom_filter_fp_chance,
            cdc,
            compaction,
            default_time_to_live,
            min_index_interval,
            max_index_interval
        FROM system_schema.tables
        """

        result = connector.session.execute(query)

        # Filter out system keyspaces using centralized filter
        ks_filter = KeyspaceFilter(settings)
        tables = [row for row in result if not ks_filter.is_excluded(row['keyspace_name'])]

        if not tables:
            builder.warning("⚠️ No user tables found")
            return builder.build(), {
                'table_statistics': {
                    'table_counts': {
                        'status': 'success',
                        'data': [],
                        'total_tables': 0,
                        'message': 'No user tables found'
                    }
                }
            }

        # Analyze tables
        findings = _analyze_tables(tables, timestamp)

        # Add summary to builder
        total_tables = findings['table_counts']['total_tables']
        total_keyspaces = findings['table_counts']['total_keyspaces']
        builder.text(f"✅ Analyzed {total_tables} table(s) across {total_keyspaces} keyspace(s)")
        builder=check_table_counts(total_tables, builder)

        builder.success(f"✅ Analyzed {total_tables} table(s) across {total_keyspaces} keyspace(s)")

        return builder.build(), {'table_statistics': findings}

    except Exception as e:
        logger.error(f"Failed to gather table statistics: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"❌ Failed to gather table statistics: {e}")
        return builder.build(), {
            'table_statistics': {
                'status': 'error',
                'error_message': str(e),
                'data': []
            }
        }


def _analyze_tables(tables: List[Dict], timestamp: str) -> Dict:
    """
    Analyze table metadata and generate structured findings

    Args:
        tables: List of table metadata from system_schema.tables
        timestamp: ISO 8601 timestamp

    Returns:
        Structured findings with multiple sub-checks
    """
    # Track statistics
    keyspace_counts = {}
    compaction_strategies = {}
    high_bloom_fp_tables = []
    cdc_tables = []
    ttl_tables = []
    low_index_interval_tables = []

    for table in tables:
        ks = table.get('keyspace_name')
        table_name = table.get('table_name')

        # Table counts per keyspace
        keyspace_counts[ks] = keyspace_counts.get(ks, 0) + 1

        # Compaction strategies
        compaction = table.get('compaction', {})
        if compaction:
            strategy_class = compaction.get('class', 'Unknown')
            # Extract short name (e.g., "SizeTieredCompactionStrategy")
            strategy_name = strategy_class.split('.')[-1]
            compaction_strategies[strategy_name] = compaction_strategies.get(strategy_name, 0) + 1

        # Bloom filter FP chance
        bloom_fp = table.get('bloom_filter_fp_chance', 0.01)
        if bloom_fp > 0.1:  # High false positive rate
            high_bloom_fp_tables.append({
                'keyspace': ks,
                'table': table_name,
                'bloom_filter_fp_chance': bloom_fp
            })

        # CDC enabled
        if table.get('cdc') is True:
            cdc_tables.append({
                'keyspace': ks,
                'table': table_name
            })

        # TTL settings
        ttl = table.get('default_time_to_live', 0)
        if ttl > 0:
            ttl_tables.append({
                'keyspace': ks,
                'table': table_name,
                'default_ttl_seconds': ttl,
                'default_ttl_days': round(ttl / 86400, 2)
            })

        # Index intervals (low values can impact performance)
        min_interval = table.get('min_index_interval', 128)
        max_interval = table.get('max_index_interval', 2048)
        if min_interval < 64 or max_interval < 512:
            low_index_interval_tables.append({
                'keyspace': ks,
                'table': table_name,
                'min_index_interval': min_interval,
                'max_index_interval': max_interval,
                'issue': 'Low index interval may increase memory usage'
            })

    # Build structured findings
    total_tables = len(tables)

    # 1. Table counts per keyspace
    table_count_data = []
    for ks, count in sorted(keyspace_counts.items()):
        table_count_data.append({
            'keyspace_name': ks,
            'table_count': count,
            'percentage': round((count / total_tables * 100), 1),
            'timestamp': timestamp
        })

    # 2. Compaction strategy distribution
    compaction_data = []
    for strategy, count in sorted(compaction_strategies.items(), key=lambda x: x[1], reverse=True):
        compaction_data.append({
            'compaction_strategy': strategy,
            'table_count': count,
            'percentage': round((count / total_tables * 100), 1),
            'timestamp': timestamp
        })

    # 3. Bloom filter analysis
    bloom_status = 'success' if len(high_bloom_fp_tables) == 0 else 'warning'
    bloom_data = {
        'status': bloom_status,
        'data': high_bloom_fp_tables,
        'high_fp_count': len(high_bloom_fp_tables),
        'total_tables': total_tables,
        'message': f'{len(high_bloom_fp_tables)} table(s) with bloom_filter_fp_chance > 0.1' if high_bloom_fp_tables else 'All tables have acceptable bloom filter settings'
    }

    # 4. CDC analysis
    cdc_data = {
        'status': 'success',
        'data': cdc_tables,
        'cdc_enabled_count': len(cdc_tables),
        'total_tables': total_tables,
        'message': f'{len(cdc_tables)} table(s) have CDC enabled' if cdc_tables else 'No tables have CDC enabled'
    }

    # 5. TTL analysis
    ttl_data = {
        'status': 'success',
        'data': ttl_tables,
        'ttl_enabled_count': len(ttl_tables),
        'total_tables': total_tables,
        'message': f'{len(ttl_tables)} table(s) have default TTL configured' if ttl_tables else 'No tables have default TTL configured'
    }

    # 6. Index interval analysis
    index_status = 'success' if len(low_index_interval_tables) == 0 else 'warning'
    index_data = {
        'status': index_status,
        'data': low_index_interval_tables,
        'low_interval_count': len(low_index_interval_tables),
        'total_tables': total_tables,
        'message': f'{len(low_index_interval_tables)} table(s) have unusually low index intervals' if low_index_interval_tables else 'All tables have appropriate index intervals'
    }

    return {
        'table_counts': {
            'status': 'success',
            'data': table_count_data,
            'total_tables': total_tables,
            'total_keyspaces': len(keyspace_counts),
            'metadata': {

                'query_timestamp': timestamp,
                'source': 'system_schema.tables'
            }
        },
        'compaction_strategies': {
            'status': 'success',
            'data': compaction_data,
            'total_strategies': len(compaction_strategies),
            'metadata': {
                'query_timestamp': timestamp,
                'source': 'system_schema.tables'
            }
        },
        'bloom_filter_settings': bloom_data,
        'cdc_enabled_tables': cdc_data,
        'ttl_settings': ttl_data,
        'index_intervals': index_data
    }


# Register check metadata
check_metadata = {
    'name': 'table_statistics',
    'description': 'Comprehensive table statistics from system_schema',
    'category': 'configuration',
    'requires_api': False,
    'requires_ssh': False,
    'requires_cql': True
}



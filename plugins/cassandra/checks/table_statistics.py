"""
Table Statistics Check

Queries system_schema.tables to gather comprehensive table metadata.

Provides:
- Table count per keyspace with configurable warn/critical thresholds
- Compaction strategy distribution
- Bloom filter false positive rates
- CDC-enabled tables
- Tables with default TTL configured
- Min/max index interval anomalies

CQL-only check — works on managed Instaclustr clusters.
Returns structured data compatible with trend analysis.

Findings structure notes:
- Returns structured_data directly (no extra wrapper). report_builder.py wraps
  it as all_structured_findings['check_table_statistics'] = structured_data.
- data dict contains scalars only. The rules engine checks is_list_like on the
  data dict and recurses instead of evaluating rules if any value is a list.
  Lists from _analyze_tables() are used for display only.
"""

import logging
import traceback
from datetime import datetime
from typing import Dict, List
from plugins.common.check_helpers import CheckContentBuilder
from plugins.cassandra.utils.keyspace_filter import KeyspaceFilter

logger = logging.getLogger(__name__)


def check_table_statistics(connector, settings):
    """
    Gather comprehensive table statistics via CQL.

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
        builder.error("No active database connection")
        return builder.build(), {
            'status': 'error',
            'error_message': 'No active database connection',
            'data': {}
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

        # Filter out system keyspaces using centralized filter.
        # Use .get() to avoid KeyError if keyspace_name is null in an unusual row.
        ks_filter = KeyspaceFilter(settings)
        tables = [row for row in result if not ks_filter.is_excluded(row.get('keyspace_name'))]

        if not tables:
            builder.warning("No user tables found")
            return builder.build(), {
                'status': 'success',
                'data': {
                    'total_tables': 0,
                    'total_keyspaces': 0,
                    'high_bloom_fp_count': 0,
                    'cdc_enabled_count': 0,
                    'ttl_enabled_count': 0,
                    'low_index_interval_count': 0,
                    'compaction_strategy_count': 0,
                    'query_timestamp': timestamp,
                }
            }

        # Analyze tables
        analysis = _analyze_tables(tables)

        total_tables = analysis['total_tables']
        total_keyspaces = analysis['total_keyspaces']

        # Table count health check with thresholds.
        # Note: defaults (warn=200, critical=500) are intentionally tighter than
        # the original version (warn=300, critical=1000). If sharing config YAML
        # between repos, set cassandra_table_count_warn and cassandra_table_count_critical
        # explicitly to avoid surprises.
        builder = _check_table_counts(total_tables, builder, settings)

        builder.para(f"Analyzed {total_tables} table(s) across {total_keyspaces} keyspace(s)")

        # Compaction strategy breakdown
        if analysis['compaction_strategies']:
            builder.h4("Compaction Strategy Distribution")
            builder.table(analysis['compaction_strategies'])
            builder.blank()

        # Bloom filter warnings
        if analysis['high_bloom_fp_tables']:
            builder.warning(
                f"{analysis['high_bloom_fp_count']} table(s) have bloom_filter_fp_chance > 0.1 "
                f"— high false positive rates increase read amplification."
            )
            builder.table(analysis['high_bloom_fp_tables'])
            builder.blank()

        # CDC tables
        if analysis['cdc_tables']:
            builder.h4("CDC-Enabled Tables")
            builder.table(analysis['cdc_tables'])
            builder.blank()

        # TTL tables
        if analysis['ttl_tables']:
            builder.h4("Tables with Default TTL")
            builder.table(analysis['ttl_tables'])
            builder.blank()

        # Low index intervals
        if analysis['low_index_interval_tables']:
            builder.warning(
                f"{analysis['low_index_interval_count']} table(s) have unusually low index intervals "
                f"— may increase memory usage."
            )
            builder.table(analysis['low_index_interval_tables'])
            builder.blank()

        # Scalars only in data dict — lists from _analyze_tables() are used above
        # for display but must not appear here or is_list_like triggers recursion
        # in dynamic_prompt_generator instead of rule evaluation.
        structured_data = {
            'status': 'success',
            'data': {
                'total_tables': total_tables,
                'total_keyspaces': total_keyspaces,
                'high_bloom_fp_count': analysis['high_bloom_fp_count'],
                'cdc_enabled_count': analysis['cdc_enabled_count'],
                'ttl_enabled_count': analysis['ttl_enabled_count'],
                'low_index_interval_count': analysis['low_index_interval_count'],
                'compaction_strategy_count': analysis['compaction_strategy_count'],
                'query_timestamp': timestamp,
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Failed to gather table statistics: {e}")
        logger.error(traceback.format_exc())
        builder.error(f"Failed to gather table statistics: {e}")
        return builder.build(), {
            'status': 'error',
            'error_message': str(e),
            'data': {}
        }


def _check_table_counts(total_tables: int, builder: CheckContentBuilder, settings: dict) -> CheckContentBuilder:
    """
    Evaluate table count against configured thresholds and add advisory to builder.

    Thresholds are configurable via settings:
        cassandra_table_count_warn     (default: 200)
        cassandra_table_count_critical (default: 500)

    Args:
        total_tables: Total number of user tables found
        builder: CheckContentBuilder instance
        settings: Configuration settings

    Returns:
        builder (for chaining)
    """
    table_count_warn = settings.get('cassandra_table_count_warn', 200)
    table_count_critical = settings.get('cassandra_table_count_critical', 500)

    builder.h4("User Table Count")
    builder.para(
        "Counts tables in user keyspaces from system_schema.tables. "
        "Large table counts increase per-node memory pressure and can degrade compaction and repair performance."
    )

    if total_tables < table_count_warn:
        builder.success(f"Found *{total_tables}* user table(s) — within healthy range.")
    elif total_tables < table_count_critical:
        builder.warning(
            f"Found *{total_tables}* user tables — exceeds recommended limit of *{table_count_warn}*."
        )
        builder.recs([
            "Review and drop unused tables.",
            "Avoid creating per-user or per-tenant tables — use partition keys instead.",
            f"Target fewer than {table_count_warn} user tables for optimal performance.",
        ])
    else:
        builder.critical(
            f"Found *{total_tables}* user tables — at or above critical threshold of *{table_count_critical}*. "
            f"Performance issues (OOM, premature memtable flushes, slow repairs) are likely."
        )
        builder.recs([
            "Drop unused tables immediately.",
            "Redesign data model to consolidate tables using partition keys.",
            "If experiencing OOM errors, increase Java heap size as a short-term measure.",
            "If experiencing rapid memtable flushes, increase memtable_total_space.",
        ])

    builder.tip(
        f"Instaclustr recommends keeping table count below {table_count_warn}. "
        f"Do not create separate tables per user/tenant — use partition keys to organize data within a single table."
    )

    return builder


def _analyze_tables(tables: List[Dict]) -> Dict:
    """
    Analyze table metadata and return summary statistics.

    Returns both scalar counts (placed in structured_data['data'] for rule evaluation)
    and display-ready lists (passed to builder.table() for the report). These are kept
    separate so that only scalars end up in the rules-evaluated data dict.
    """
    keyspace_counts = {}
    compaction_strategies = {}
    high_bloom_fp_tables = []
    cdc_tables = []
    ttl_tables = []
    low_index_interval_tables = []

    for table in tables:
        ks = table.get('keyspace_name')
        table_name = table.get('table_name')

        keyspace_counts[ks] = keyspace_counts.get(ks, 0) + 1

        # compaction is a map column — guard against None explicitly
        compaction = table.get('compaction') or {}
        if compaction:
            strategy_class = compaction.get('class', 'Unknown')
            strategy_name = strategy_class.split('.')[-1]
            compaction_strategies[strategy_name] = compaction_strategies.get(strategy_name, 0) + 1

        # Use `or` fallback to handle explicit None values from the driver,
        # since .get(key, default) only applies the default for missing keys.
        bloom_fp = table.get('bloom_filter_fp_chance') or 0.01
        if bloom_fp > 0.1:
            high_bloom_fp_tables.append({
                'Keyspace': ks,
                'Table': table_name,
                'Bloom FP Chance': bloom_fp,
            })

        if table.get('cdc') is True:
            cdc_tables.append({'Keyspace': ks, 'Table': table_name})

        ttl = table.get('default_time_to_live') or 0
        if ttl > 0:
            ttl_tables.append({
                'Keyspace': ks,
                'Table': table_name,
                'TTL (days)': round(ttl / 86400, 2),
            })

        min_interval = table.get('min_index_interval') or 128
        max_interval = table.get('max_index_interval') or 2048
        if min_interval < 64 or max_interval < 512:
            low_index_interval_tables.append({
                'Keyspace': ks,
                'Table': table_name,
                'Min Interval': min_interval,
                'Max Interval': max_interval,
            })

    total_tables = len(tables)

    compaction_display = [
        {
            'Strategy': strategy,
            'Tables': count,
            'Pct': f"{round(count / total_tables * 100, 1) if total_tables else 0}%",
        }
        for strategy, count in sorted(compaction_strategies.items(), key=lambda x: x[1], reverse=True)
    ]

    return {
        'total_tables': total_tables,
        'total_keyspaces': len(keyspace_counts),
        'compaction_strategy_count': len(compaction_strategies),
        'compaction_strategies': compaction_display,
        'high_bloom_fp_count': len(high_bloom_fp_tables),
        'high_bloom_fp_tables': high_bloom_fp_tables,
        'cdc_enabled_count': len(cdc_tables),
        'cdc_tables': cdc_tables,
        'ttl_enabled_count': len(ttl_tables),
        'ttl_tables': ttl_tables,
        'low_index_interval_count': len(low_index_interval_tables),
        'low_index_interval_tables': low_index_interval_tables,
    }

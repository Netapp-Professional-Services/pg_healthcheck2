"""
Secondary Indexes Check

Queries system_schema.indexes to analyze secondary index usage.

Secondary indexes in Cassandra have specific use cases and limitations:
- Best for low-cardinality columns
- Can cause performance issues on high-cardinality columns
- SASI indexes offer better performance but are experimental
- Custom indexes should be reviewed carefully

This check flags:
- Presence of secondary indexes
- Index types (SASI, custom, standard)
- Potential performance concerns

CQL-only check - works on managed Instaclustr clusters.
Returns structured data compatible with trend analysis.
"""

import logging
from datetime import datetime
from typing import Dict, List
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def check_secondary_indexes(connector, settings):
    """
    Analyze secondary index configurations

    Checks for:
    - All secondary indexes in user keyspaces
    - Index types (standard vs SASI vs custom)
    - Potential performance concerns
    - Distribution across keyspaces

    Args:
        connector: Cassandra connector with active session
        settings: Configuration settings

    Returns:
        Tuple of (adoc_content, structured findings dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Secondary Indexes")

    if not connector or not connector.session:
        builder.error("❌ No active database connection")
        return builder.build(), {
            'secondary_indexes': {
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

        # Query all indexes (filtering in Python since Cassandra doesn't support WHERE NOT IN)
        query = """
        SELECT
            keyspace_name,
            table_name,
            index_name,
            kind,
            options
        FROM system_schema.indexes
        """

        result = connector.session.execute(query)
        # Filter out system keyspaces in Python
        indexes = [row for row in result if row['keyspace_name'] not in system_keyspaces]

        if not indexes:
            builder.success("✅ No secondary indexes found (good - use native CQL queries when possible)")
            return builder.build(), {
                'secondary_indexes': {
                    'index_summary': {
                        'status': 'success',
                        'data': [],
                        'total_indexes': 0,
                        'message': 'No secondary indexes found (good - use native CQL queries when possible)'
                    }
                }
            }

        # Analyze indexes
        findings = _analyze_indexes(indexes, timestamp)

        # Add summary to builder
        total_indexes = findings['index_summary']['total_indexes']
        standard_count = findings['index_summary']['standard_count']
        sasi_count = findings['index_summary']['sasi_count']
        custom_count = findings['index_summary']['custom_count']
        builder.warning(f"⚠️ Found {total_indexes} secondary index(es): {standard_count} standard, {sasi_count} SASI, {custom_count} custom")

        # Add explanation
        builder.blank()
        builder.text("*Why This Matters:*")
        builder.text("Secondary indexes in Cassandra should be used sparingly. They work well for low-cardinality ")
        builder.text("columns (where <10% of values are unique) but can cause severe performance degradation on ")
        builder.text("high-cardinality columns. Each indexed read requires querying all nodes that own the data, ")
        builder.text("potentially resulting in cluster-wide scans.")
        builder.blank()

        # Add current configuration breakdown
        builder.text("*Current Configuration:*")
        if standard_count > 0:
            builder.text(f"- *Standard Indexes ({standard_count})*: Traditional secondary indexes using COMPOSITES")
            # Show a few examples
            standard_examples = findings['standard_indexes']['data'][:3]
            for idx in standard_examples:
                builder.text(f"  • {idx['keyspace']}.{idx['table']}.{idx['index_name']} on column '{idx.get('indexed_column', 'unknown')}'")
            if len(findings['standard_indexes']['data']) > 3:
                builder.text(f"  • ...and {len(findings['standard_indexes']['data']) - 3} more")

        if sasi_count > 0:
            builder.text(f"- *SASI Indexes ({sasi_count})*: SSTable Attached Secondary Index (experimental)")
            sasi_examples = findings['sasi_indexes']['data'][:2]
            for idx in sasi_examples:
                builder.text(f"  • {idx['keyspace']}.{idx['table']}.{idx['index_name']}")

        if custom_count > 0:
            builder.text(f"- *Custom Indexes ({custom_count})*: Custom implementation")
            custom_examples = findings['custom_indexes']['data'][:2]
            for idx in custom_examples:
                builder.text(f"  • {idx['keyspace']}.{idx['table']}.{idx['index_name']} ({idx.get('class_name', 'Unknown')})")
        builder.blank()

        # Add recommendations
        builder.text("*Recommended Actions:*")
        if standard_count > 0:
            builder.text("1. *Review Standard Indexes*: Verify indexed columns have low cardinality (<10% unique values)")
            builder.text("   - Use `SELECT COUNT(DISTINCT column) FROM table` to check cardinality")
            builder.text("   - Consider materialized views or denormalization for high-cardinality queries")

        if sasi_count > 0:
            builder.text("2. *SASI Indexes*: Monitor performance closely - this feature is experimental")
            builder.text("   - Have rollback plan ready")
            builder.text("   - Consider migrating to standard approaches if issues arise")

        if custom_count > 0:
            builder.text("3. *Custom Indexes*: Ensure proper monitoring and maintenance")
            builder.text("   - Verify implementation handles compaction and repair correctly")
            builder.text("   - Document behavior and performance characteristics")

        if standard_count > 5:
            builder.text(f"4. *Data Model Review*: {standard_count} indexes suggests query-driven redesign may be needed")
            builder.text("   - Consider creating dedicated query tables (denormalization)")
            builder.text("   - Review whether Cassandra is the right fit for this access pattern")
        builder.blank()

        builder.text("*Alternatives to Consider*:")
        builder.text("- *Materialized Views*: Automatically maintained query tables (better than indexes for many cases)")
        builder.text("- *Denormalization*: Create duplicate tables optimized for specific queries")
        builder.text("- *External Search*: Use Elasticsearch/Solr for complex search requirements")

        return builder.build(), {'secondary_indexes': findings}

    except Exception as e:
        logger.error(f"Failed to analyze secondary indexes: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"❌ Failed to analyze secondary indexes: {e}")
        return builder.build(), {
            'secondary_indexes': {
                'status': 'error',
                'error_message': str(e),
                'data': []
            }
        }


def _analyze_indexes(indexes: List[Dict], timestamp: str) -> Dict:
    """
    Analyze secondary index configurations

    Args:
        indexes: List of index metadata from system_schema.indexes
        timestamp: ISO 8601 timestamp

    Returns:
        Structured findings with index analysis
    """
    # Categorize indexes
    standard_indexes = []
    sasi_indexes = []
    custom_indexes = []
    keyspace_counts = {}

    for idx in indexes:
        ks = idx.get('keyspace_name')
        table = idx.get('table_name')
        index_name = idx.get('index_name')
        kind = idx.get('kind', 'UNKNOWN')
        options = idx.get('options', {})

        # Count per keyspace
        keyspace_counts[ks] = keyspace_counts.get(ks, 0) + 1

        index_data = {
            'keyspace': ks,
            'table': table,
            'index_name': index_name,
            'kind': kind,
            'options': options,
            'timestamp': timestamp
        }

        # Categorize by type
        if kind == 'COMPOSITES':
            # Standard secondary index
            target = options.get('target', '')

            # Check if it's a SASI index
            class_name = options.get('class_name', '')
            if 'sasi' in class_name.lower():
                index_data['index_type'] = 'SASI'
                index_data['sasi_class'] = class_name
                index_data['warning'] = 'SASI indexes are experimental - monitor performance'
                sasi_indexes.append(index_data)
            else:
                index_data['index_type'] = 'Standard'
                index_data['indexed_column'] = _extract_column_name(target)
                index_data['recommendation'] = _get_index_recommendation(options)
                standard_indexes.append(index_data)

        elif kind == 'CUSTOM':
            # Custom index implementation
            index_data['index_type'] = 'Custom'
            index_data['class_name'] = options.get('class_name', 'Unknown')
            index_data['warning'] = 'Custom index - ensure proper maintenance and monitoring'
            custom_indexes.append(index_data)

        else:
            # Unknown index type
            index_data['index_type'] = f'Unknown ({kind})'
            index_data['warning'] = 'Unknown index type - review configuration'
            custom_indexes.append(index_data)

    # Build summary data
    total_indexes = len(indexes)

    # Keyspace distribution
    keyspace_data = []
    for ks, count in sorted(keyspace_counts.items()):
        keyspace_data.append({
            'keyspace': ks,
            'index_count': count,
            'percentage': round((count / total_indexes * 100), 1),
            'timestamp': timestamp
        })

    # Determine overall status
    # Having any secondary indexes is a warning (not an error, but should be reviewed)
    overall_status = 'warning' if total_indexes > 0 else 'success'

    # Build recommendations
    recommendations = _build_recommendations(
        standard_count=len(standard_indexes),
        sasi_count=len(sasi_indexes),
        custom_count=len(custom_indexes)
    )

    return {
        'index_summary': {
            'status': overall_status,
            'data': keyspace_data,
            'total_indexes': total_indexes,
            'standard_count': len(standard_indexes),
            'sasi_count': len(sasi_indexes),
            'custom_count': len(custom_indexes),
            'metadata': {
                'query_timestamp': timestamp,
                'source': 'system_schema.indexes'
            },
            'message': f'Found {total_indexes} secondary index(es) - review for performance impact'
        },
        'standard_indexes': {
            'status': 'warning' if len(standard_indexes) > 0 else 'success',
            'data': standard_indexes,
            'count': len(standard_indexes),
            'message': f'{len(standard_indexes)} standard secondary index(es)' if standard_indexes else 'No standard secondary indexes'
        },
        'sasi_indexes': {
            'status': 'warning' if len(sasi_indexes) > 0 else 'success',
            'data': sasi_indexes,
            'count': len(sasi_indexes),
            'message': f'{len(sasi_indexes)} SASI index(es) - experimental feature' if sasi_indexes else 'No SASI indexes'
        },
        'custom_indexes': {
            'status': 'warning' if len(custom_indexes) > 0 else 'success',
            'data': custom_indexes,
            'count': len(custom_indexes),
            'message': f'{len(custom_indexes)} custom index(es)' if custom_indexes else 'No custom indexes'
        },
        'recommendations': recommendations
    }


def _extract_column_name(target: str) -> str:
    """
    Extract column name from index target string

    Args:
        target: Index target string (e.g., "values(column_name)")

    Returns:
        Column name
    """
    if not target:
        return 'unknown'

    # Remove "values(" prefix and ")" suffix if present
    if target.startswith('values(') and target.endswith(')'):
        return target[7:-1]

    # Remove "keys(" prefix for map indexes
    if target.startswith('keys(') and target.endswith(')'):
        return f"keys({target[5:-1]})"

    # Remove "entries(" prefix for map indexes
    if target.startswith('entries(') and target.endswith(')'):
        return f"entries({target[8:-1]})"

    return target


def _get_index_recommendation(options: Dict) -> str:
    """
    Get recommendation for standard index

    Args:
        options: Index options from system_schema.indexes

    Returns:
        Recommendation string
    """
    target = options.get('target', '')

    if target.startswith('keys('):
        return "Map key index - ensure map has low cardinality"

    if target.startswith('entries('):
        return "Map entries index - can be expensive, consider alternatives"

    # Standard column index
    return "Ensure indexed column has low cardinality (<10% unique values)"


def _build_recommendations(standard_count: int, sasi_count: int, custom_count: int) -> Dict:
    """
    Build recommendations based on index analysis

    Args:
        standard_count: Number of standard indexes
        sasi_count: Number of SASI indexes
        custom_count: Number of custom indexes

    Returns:
        Recommendations dict
    """
    recommendations = []

    if standard_count > 0:
        recommendations.append({
            'priority': 'high',
            'category': 'Standard Indexes',
            'recommendation': 'Review query patterns - secondary indexes should only be used on low-cardinality columns',
            'action': 'Consider denormalization or materialized views for high-cardinality queries'
        })

    if sasi_count > 0:
        recommendations.append({
            'priority': 'medium',
            'category': 'SASI Indexes',
            'recommendation': 'SASI indexes are experimental - monitor performance and stability',
            'action': 'Have rollback plan and consider migrating to standard approaches'
        })

    if custom_count > 0:
        recommendations.append({
            'priority': 'medium',
            'category': 'Custom Indexes',
            'recommendation': 'Custom indexes require careful maintenance',
            'action': 'Ensure proper monitoring, testing, and documentation'
        })

    if standard_count + sasi_count + custom_count == 0:
        recommendations.append({
            'priority': 'info',
            'category': 'No Indexes',
            'recommendation': 'No secondary indexes found - good practice',
            'action': 'Continue using partition key and clustering column queries for best performance'
        })

    if standard_count > 5:
        recommendations.append({
            'priority': 'high',
            'category': 'Index Count',
            'recommendation': f'{standard_count} secondary indexes is excessive',
            'action': 'Review data model - consider denormalization or query-driven table design'
        })

    return {
        'status': 'warning' if (standard_count > 0 or sasi_count > 0) else 'success',
        'data': recommendations,
        'count': len(recommendations)
    }


# Register check metadata
check_metadata = {
    'name': 'secondary_indexes',
    'description': 'Analyze secondary index usage and performance implications',
    'category': 'performance',
    'requires_api': False,
    'requires_ssh': False,
    'requires_cql': True
}

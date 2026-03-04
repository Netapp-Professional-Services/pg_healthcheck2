"""
Table statistics check for Cassandra.

Analyzes nodetool cfstats/tablestats output to detect:
- High tombstone counts (causes slow reads)
- Read/write latency issues
- Large tables and SSTable counts
- Bloom filter efficiency

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def parse_cfstats(cfstats_output):
    """
    Parse nodetool cfstats output into structured data.

    Args:
        cfstats_output: Raw cfstats output

    Returns:
        dict: Parsed table statistics by keyspace
    """
    keyspaces = {}
    current_keyspace = None
    current_table = None
    table_data = {}

    lines = cfstats_output.strip().split('\n')

    for line in lines:
        line_stripped = line.strip()

        # Skip empty lines and separators
        if not line_stripped or line_stripped.startswith('---'):
            continue

        # Keyspace header
        if line_stripped.startswith('Keyspace :') or line_stripped.startswith('Keyspace:'):
            # Save previous table if exists
            if current_table and table_data:
                if current_keyspace not in keyspaces:
                    keyspaces[current_keyspace] = {'tables': {}}
                keyspaces[current_keyspace]['tables'][current_table] = table_data
                table_data = {}

            ks_name = line_stripped.split(':')[1].strip()
            current_keyspace = ks_name
            current_table = None
            if ks_name not in keyspaces:
                keyspaces[ks_name] = {'tables': {}}
            continue

        # Table header (starts with Tab or Table)
        if line_stripped.startswith('Table:') or line_stripped.startswith('Table :'):
            # Save previous table
            if current_table and table_data:
                if current_keyspace:
                    keyspaces[current_keyspace]['tables'][current_table] = table_data
            table_data = {}
            current_table = line_stripped.split(':')[1].strip()
            continue

        # Table attributes
        if ':' in line_stripped and current_keyspace:
            key, value = line_stripped.split(':', 1)
            key = key.strip()
            value = value.strip()

            # Parse numeric values
            if key == 'SSTable count':
                table_data['sstable_count'] = int(value)
            elif key == 'Space used (live)':
                table_data['space_used_live'] = value
                # Parse bytes value
                table_data['space_used_live_bytes'] = parse_size(value)
            elif key == 'Space used (total)':
                table_data['space_used_total'] = value
            elif key == 'Number of partitions (estimate)':
                table_data['partition_count'] = int(value)
            elif key == 'Local read count':
                table_data['read_count'] = int(value)
            elif key == 'Local read latency':
                table_data['read_latency_ms'] = parse_latency(value)
            elif key == 'Local write count':
                table_data['write_count'] = int(value)
            elif key == 'Local write latency':
                table_data['write_latency_ms'] = parse_latency(value)
            elif key == 'Average tombstones per slice (last five minutes)':
                table_data['avg_tombstones'] = parse_float(value)
            elif key == 'Maximum tombstones per slice (last five minutes)':
                table_data['max_tombstones'] = parse_int(value)
            elif key == 'Bloom filter false ratio':
                table_data['bloom_filter_false_ratio'] = parse_float(value)
            elif key == 'Bloom filter false positives':
                table_data['bloom_filter_false_positives'] = parse_int(value)
            elif key == 'Percent repaired':
                table_data['percent_repaired'] = parse_float(value)
            elif key == 'Pending flushes':
                table_data['pending_flushes'] = parse_int(value)
            elif key == 'Dropped Mutations':
                table_data['dropped_mutations'] = value

    # Save last table
    if current_table and table_data and current_keyspace:
        keyspaces[current_keyspace]['tables'][current_table] = table_data

    return keyspaces


def parse_size(value):
    """Parse size string to bytes."""
    try:
        value = value.lower()
        if 'bytes' in value:
            return int(value.replace('bytes', '').strip())
        elif 'kib' in value or 'kb' in value:
            return int(float(value.replace('kib', '').replace('kb', '').strip()) * 1024)
        elif 'mib' in value or 'mb' in value:
            return int(float(value.replace('mib', '').replace('mb', '').strip()) * 1024 * 1024)
        elif 'gib' in value or 'gb' in value:
            return int(float(value.replace('gib', '').replace('gb', '').strip()) * 1024 * 1024 * 1024)
        elif 'tib' in value or 'tb' in value:
            return int(float(value.replace('tib', '').replace('tb', '').strip()) * 1024 * 1024 * 1024 * 1024)
        return int(value)
    except:
        return 0


def parse_latency(value):
    """Parse latency string to ms."""
    try:
        if 'nan' in value.lower():
            return None
        return float(value.replace('ms', '').strip())
    except:
        return None


def parse_float(value):
    """Parse float value."""
    try:
        if 'nan' in value.lower():
            return None
        return float(value)
    except:
        return None


def parse_int(value):
    """Parse int value."""
    try:
        return int(value)
    except:
        return 0


def run_check_table_stats(connector, settings):
    """
    Check table statistics for performance issues.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Table Statistics Analysis")
    builder.para("Analyzing table health from `nodetool cfstats/tablestats`.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Table statistics")
    if not available:
        return skip_msg, skip_data

    try:
        # Try tablestats first (newer), fall back to cfstats
        results = connector.execute_ssh_on_all_hosts(
            "nodetool tablestats",
            "Get table statistics"
        )

        # Check if we got data, otherwise try cfstats
        has_data = any(r.get('success') and r.get('output') for r in results)
        if not has_data:
            results = connector.execute_ssh_on_all_hosts(
                "nodetool cfstats",
                "Get column family statistics"
            )

        # Thresholds
        tombstone_warning = settings.get('cfstats_tombstone_warning', 100)
        tombstone_critical = settings.get('cfstats_tombstone_critical', 1000)
        read_latency_warning = settings.get('cfstats_read_latency_warning_ms', 10)
        read_latency_critical = settings.get('cfstats_read_latency_critical_ms', 100)
        sstable_warning = settings.get('cfstats_sstable_warning', 50)
        bloom_false_ratio_warning = settings.get('cfstats_bloom_false_ratio_warning', 0.1)

        # Aggregate stats across nodes
        all_tables = {}  # keyspace.table -> aggregated stats
        issues = []

        # System keyspaces to skip
        system_keyspaces = {'system', 'system_schema', 'system_traces', 'system_auth',
                           'system_distributed', 'system_views', 'system_virtual_schema'}

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            keyspaces = parse_cfstats(result['output'])

            for ks_name, ks_data in keyspaces.items():
                # Skip system keyspaces
                if ks_name in system_keyspaces:
                    continue

                for table_name, table_data in ks_data.get('tables', {}).items():
                    full_name = f"{ks_name}.{table_name}"

                    if full_name not in all_tables:
                        all_tables[full_name] = {
                            'keyspace': ks_name,
                            'table': table_name,
                            'nodes': [],
                            'total_sstables': 0,
                            'total_space_bytes': 0,
                            'total_reads': 0,
                            'total_writes': 0,
                            'max_tombstones': 0,
                            'avg_tombstones': 0,
                            'max_read_latency': 0,
                            'bloom_false_ratios': []
                        }

                    stats = all_tables[full_name]
                    stats['nodes'].append(node_ip)
                    stats['total_sstables'] += table_data.get('sstable_count', 0)
                    stats['total_space_bytes'] += table_data.get('space_used_live_bytes', 0)
                    stats['total_reads'] += table_data.get('read_count', 0)
                    stats['total_writes'] += table_data.get('write_count', 0)

                    max_tomb = table_data.get('max_tombstones', 0) or 0
                    if max_tomb > stats['max_tombstones']:
                        stats['max_tombstones'] = max_tomb

                    avg_tomb = table_data.get('avg_tombstones', 0) or 0
                    if avg_tomb > stats['avg_tombstones']:
                        stats['avg_tombstones'] = avg_tomb

                    read_lat = table_data.get('read_latency_ms')
                    if read_lat and read_lat > stats['max_read_latency']:
                        stats['max_read_latency'] = read_lat

                    bf_ratio = table_data.get('bloom_filter_false_ratio')
                    if bf_ratio is not None:
                        stats['bloom_false_ratios'].append(bf_ratio)

        if not all_tables:
            builder.warning("No user table statistics available.")
            return builder.build(), {'status': 'no_data'}

        # Analyze issues
        tombstone_issues = []
        latency_issues = []
        sstable_issues = []
        bloom_issues = []

        for full_name, stats in all_tables.items():
            # Check tombstones
            if stats['max_tombstones'] >= tombstone_critical:
                tombstone_issues.append({
                    'table': full_name,
                    'max_tombstones': stats['max_tombstones'],
                    'severity': 'critical'
                })
            elif stats['max_tombstones'] >= tombstone_warning:
                tombstone_issues.append({
                    'table': full_name,
                    'max_tombstones': stats['max_tombstones'],
                    'severity': 'warning'
                })

            # Check read latency
            if stats['max_read_latency'] >= read_latency_critical:
                latency_issues.append({
                    'table': full_name,
                    'latency_ms': stats['max_read_latency'],
                    'severity': 'critical'
                })
            elif stats['max_read_latency'] >= read_latency_warning:
                latency_issues.append({
                    'table': full_name,
                    'latency_ms': stats['max_read_latency'],
                    'severity': 'warning'
                })

            # Check SSTable count
            if stats['total_sstables'] >= sstable_warning:
                sstable_issues.append({
                    'table': full_name,
                    'sstable_count': stats['total_sstables']
                })

            # Check bloom filter
            if stats['bloom_false_ratios']:
                avg_bf = sum(stats['bloom_false_ratios']) / len(stats['bloom_false_ratios'])
                if avg_bf >= bloom_false_ratio_warning:
                    bloom_issues.append({
                        'table': full_name,
                        'false_ratio': avg_bf
                    })

        # Summary
        total_issues = len(tombstone_issues) + len(latency_issues) + len(sstable_issues)
        if total_issues > 0:
            critical_count = len([t for t in tombstone_issues if t['severity'] == 'critical']) + \
                           len([l for l in latency_issues if l['severity'] == 'critical'])
            if critical_count > 0:
                builder.critical(f"**{critical_count} critical table issue(s) detected**")
            else:
                builder.warning(f"**{total_issues} table issue(s) detected**")
        else:
            builder.success("All tables appear healthy.")
        builder.blank()

        # Show largest tables
        largest_tables = sorted(all_tables.values(),
                               key=lambda x: x['total_space_bytes'], reverse=True)[:10]
        if largest_tables:
            builder.h4("Largest Tables")
            table_data = []
            for t in largest_tables:
                size_gb = t['total_space_bytes'] / (1024**3)
                table_data.append({
                    'Table': f"{t['keyspace']}.{t['table']}",
                    'Size': f"{size_gb:.2f} GB",
                    'SSTables': t['total_sstables'],
                    'Reads': f"{t['total_reads']:,}",
                    'Writes': f"{t['total_writes']:,}"
                })
            builder.table(table_data)
            builder.blank()

        # Show tombstone issues
        if tombstone_issues:
            builder.h4("High Tombstone Tables")
            tombstone_issues.sort(key=lambda x: x['max_tombstones'], reverse=True)

            critical_tombs = [t for t in tombstone_issues if t['severity'] == 'critical']
            if critical_tombs:
                builder.critical(f"**{len(critical_tombs)} table(s) with critical tombstone counts (>{tombstone_critical})**")
                builder.blank()

            tomb_table = [{
                'Table': t['table'],
                'Max Tombstones': f"{'🔴 ' if t['severity'] == 'critical' else '⚠️ '}{t['max_tombstones']}"
            } for t in tombstone_issues[:10]]
            builder.table(tomb_table)
            builder.blank()

            # Detailed explanation and recommendations
            builder.text("*Why This Matters:*")
            builder.blank()
            builder.text("Tombstones are deletion markers that Cassandra keeps until `gc_grace_seconds` expires.")
            builder.text("High tombstone counts cause: slow reads (scanning dead data), increased heap usage, and GC pressure.")
            builder.blank()

            builder.text("*Immediate Actions:*")
            builder.blank()
            builder.text("First, check compaction strategy for affected tables:")
            for t in tombstone_issues[:3]:
                ks, tbl = t['table'].split('.') if '.' in t['table'] else ('keyspace', t['table'])
                builder.text(f"   `nodetool describetable {ks} {tbl}`")
            builder.blank()
            builder.text("Then, if using **STCS**, force compaction with `-s` flag:")
            for t in tombstone_issues[:3]:
                ks, tbl = t['table'].split('.') if '.' in t['table'] else ('keyspace', t['table'])
                builder.text(f"   `nodetool compact -s {ks} {tbl}`")
            builder.blank()
            builder.note(
                "The `-s` flag performs a \"split\" compaction, preventing creation of one huge SSTable. "
                "Only use `nodetool compact` for STCS tables — for LCS/TWCS, compaction runs automatically."
            )
            builder.blank()

            builder.text("*Root Cause Investigation:*")
            builder.blank()
            builder.text("- Check for DELETE-heavy workloads or TTL expiration patterns")
            builder.text("- Review `gc_grace_seconds` setting (default: 10 days) - can be reduced if repairs run frequently")
            builder.text("- Consider using TWCS (TimeWindowCompactionStrategy) for time-series data with TTLs")
            builder.blank()

            builder.text("*Prevention:*")
            builder.blank()
            builder.text("- Avoid single-partition deletes in bulk; use range tombstones instead")
            builder.text("- For time-series: set appropriate TTL and use TWCS")
            builder.text("- Schedule regular maintenance during low-traffic periods (STCS tables only)")
            builder.blank()

        # Show latency issues
        if latency_issues:
            builder.h4("High Latency Tables")
            latency_issues.sort(key=lambda x: x['latency_ms'], reverse=True)

            lat_table = [{
                'Table': l['table'],
                'Read Latency': f"{'🔴 ' if l['severity'] == 'critical' else '⚠️ '}{l['latency_ms']:.1f} ms"
            } for l in latency_issues[:10]]
            builder.table(lat_table)
            builder.blank()

            builder.text("*Why This Matters:*")
            builder.blank()
            builder.text("High read latency directly impacts application response times and user experience.")
            builder.text(f"Target: <{read_latency_warning}ms for most reads. Critical: >{read_latency_critical}ms causes timeouts.")
            builder.blank()

            builder.text("*Common Causes & Fixes:*")
            builder.blank()
            builder.text("1. **Too many SSTables (STCS only)** → `nodetool compact -s <keyspace> <table>`")
            builder.text("2. **Large partitions** → Check with `nodetool tablehistograms <keyspace> <table>`")
            builder.text("3. **Disk I/O bottleneck** → Check `iostat -x 1` for high %util")
            builder.text("4. **Tombstones** → See tombstone section above")
            builder.text("5. **Bloom filter issues** → Increase `bloom_filter_fp_chance` or rebuild")
            builder.text("6. **Check compaction strategy** → `nodetool describetable <keyspace> <table>`")
            builder.blank()

            builder.text("*Diagnostic Commands:*")
            builder.blank()
            for l in latency_issues[:2]:
                ks, tbl = l['table'].split('.') if '.' in l['table'] else ('keyspace', l['table'])
                builder.text(f"- `nodetool tablehistograms {ks} {tbl}` — Check partition sizes")
                builder.text(f"- `nodetool cfstats {ks}.{tbl}` — Detailed table stats")
            builder.blank()

        # Show SSTable issues
        if sstable_issues:
            builder.h4("High SSTable Count Tables")
            sstable_issues.sort(key=lambda x: x['sstable_count'], reverse=True)

            sst_table = [{
                'Table': s['table'],
                'SSTable Count': s['sstable_count']
            } for s in sstable_issues[:10]]
            builder.table(sst_table)
            builder.blank()

            builder.text("*Important Context:*")
            builder.blank()
            builder.note(
                "High SSTable counts are **not always a problem**. The significance depends on "
                "compaction strategy:\n\n"
                "• **LCS (LeveledCompactionStrategy)**: Naturally maintains more SSTables (one per level). "
                "Counts of 50-100+ can be normal for active tables.\n"
                "• **UCS (UnifiedCompactionStrategy)**: With LCS-like settings, also maintains higher counts.\n"
                "• **STCS (SizeTieredCompactionStrategy)**: High counts indicate compaction backlog.\n"
                "• **TWCS (TimeWindowCompactionStrategy)**: High counts may indicate blocked tombstone cleanup.\n\n"
                "**Check compaction strategy first:** `nodetool describetable <keyspace> <table>`"
            )
            builder.blank()

            builder.text("*Why High SSTable Counts Can Matter (for STCS):*")
            builder.blank()
            builder.text("When using SizeTieredCompactionStrategy, high SSTable counts indicate compaction is falling behind.")
            builder.text("Each read must check multiple SSTables, increasing latency and I/O.")
            builder.blank()

            builder.text("*Diagnostic Steps:*")
            builder.blank()
            builder.text("1. **Check compaction strategy for each table:**")
            for s in sstable_issues[:3]:
                ks, tbl = s['table'].split('.') if '.' in s['table'] else ('keyspace', s['table'])
                builder.text(f"   `nodetool describetable {ks} {tbl}` — look for `compaction = {{'class': '...'}}`")
            builder.blank()

            builder.text("2. **If using STCS and compaction is behind:**")
            builder.text("   Force compaction with `-s` flag to avoid creating one huge SSTable:")
            for s in sstable_issues[:3]:
                ks, tbl = s['table'].split('.') if '.' in s['table'] else ('keyspace', s['table'])
                builder.text(f"   `nodetool compact -s {ks} {tbl}`")
            builder.blank()

            builder.text("3. **If using TWCS with high SSTable count:**")
            builder.text("   This may indicate SSTables are \"blocked\" from being dropped due to:")
            builder.text("   • Active (non-expired) data mixed into older time-window files")
            builder.text("   • Long-lived tombstones preventing SSTable cleanup")
            builder.text("   • Rows updated after initial write, spreading data across time windows")
            builder.blank()
            builder.text("   **Resolution for TWCS:**")
            builder.text("   • Review TTL settings and ensure consistent TTL across all columns")
            builder.text("   • Avoid updates/deletes on time-series data (write-once pattern)")
            builder.text("   • Consider `unsafe_aggressive_sstable_expiration: true` (use with caution)")
            builder.blank()

            builder.text("*Long-term Fixes (if compaction is actually behind):*")
            builder.blank()
            builder.text("1. **Increase compaction throughput:**")
            builder.text("   - `nodetool setcompactionthroughput 64` (MB/s, temporary)")
            builder.text("   - Permanent: Set `compaction_throughput_mb_per_sec: 64` in cassandra.yaml")
            builder.text("2. **Increase concurrent compactors:**")
            builder.text("   - `nodetool setconcurrentcompactors 4`")
            builder.text("3. **Consider changing compaction strategy if workload pattern changed:**")
            builder.text("   - STCS → LCS: For read-heavy workloads (more I/O during compaction)")
            builder.text("   - STCS → TWCS: For time-series data with TTL")
            builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'total_tables': len(all_tables),
                'tombstone_issues': len(tombstone_issues),
                'latency_issues': len(latency_issues),
                'sstable_issues': len(sstable_issues),
                'bloom_issues': len(bloom_issues),
                'max_tombstones': max((t['max_tombstones'] for t in tombstone_issues), default=0),
                'max_read_latency_ms': max((l['latency_ms'] for l in latency_issues), default=0),
                'tables': list(all_tables.values())
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Table stats check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Table stats check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

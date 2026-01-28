"""
Compaction statistics check for Cassandra.

Analyzes nodetool compactionstats output to detect:
- Pending compaction tasks (backlog indicator)
- Active compactions (current load)
- Compaction backlog across nodes

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def parse_compactionstats(output):
    """
    Parse nodetool compactionstats output.

    Example format:
        pending tasks: 5

        compaction type  keyspace  table  completed  total  unit  progress
        Compaction       iml       imei   123456     500000 bytes 24.69%

    Args:
        output: Raw compactionstats output

    Returns:
        dict: Parsed compaction statistics
    """
    result = {
        'pending_tasks': 0,
        'active_compactions': [],
        'compaction_types': {}
    }

    if not output:
        return result

    lines = output.strip().split('\n')

    for line in lines:
        line = line.strip()

        # Parse pending tasks
        if line.lower().startswith('pending tasks:'):
            match = re.search(r'pending tasks:\s*(\d+)', line, re.IGNORECASE)
            if match:
                result['pending_tasks'] = int(match.group(1))

        # Parse active compaction lines (skip header)
        elif line and not line.startswith('compaction type') and not line.startswith('pending'):
            parts = line.split()
            if len(parts) >= 6:
                try:
                    compaction = {
                        'type': parts[0],
                        'keyspace': parts[1],
                        'table': parts[2],
                        'completed': parts[3],
                        'total': parts[4],
                        'unit': parts[5] if len(parts) > 5 else 'bytes',
                        'progress': parts[6] if len(parts) > 6 else 'N/A'
                    }
                    result['active_compactions'].append(compaction)

                    # Track compaction types
                    ctype = compaction['type']
                    result['compaction_types'][ctype] = result['compaction_types'].get(ctype, 0) + 1
                except (IndexError, ValueError):
                    continue

    return result


def run_check_compaction_stats(connector, settings):
    """
    Analyze compaction statistics across all nodes.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Compaction Statistics")
    builder.para("Analyzing compaction status and pending tasks from `nodetool compactionstats`.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Compaction stats")
    if not available:
        return skip_msg, skip_data

    try:
        # Get compactionstats from all hosts
        results = connector.execute_ssh_on_all_hosts(
            "nodetool compactionstats",
            "Get compaction statistics"
        )

        all_stats = []
        nodes_with_pending = []
        nodes_with_active = []
        total_pending = 0
        total_active = 0

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            stats = parse_compactionstats(result['output'])
            stats['node'] = node_ip
            all_stats.append(stats)

            pending = stats['pending_tasks']
            active = len(stats['active_compactions'])

            total_pending += pending
            total_active += active

            if pending > 0:
                nodes_with_pending.append({'node': node_ip, 'pending': pending})

            if active > 0:
                nodes_with_active.append({'node': node_ip, 'active': active})

        if not all_stats:
            builder.note("Compaction statistics not available.")
            return builder.build(), {'status': 'no_data'}

        # Thresholds
        pending_warning = settings.get('compaction_pending_warning', 10)
        pending_critical = settings.get('compaction_pending_critical', 50)

        # Summary
        if total_pending >= pending_critical:
            builder.critical(f"**{total_pending} pending compaction tasks** - severe compaction backlog!")
        elif total_pending >= pending_warning:
            builder.warning(f"**{total_pending} pending compaction tasks** - compaction backlog building")
        elif total_pending > 0:
            builder.note(f"{total_pending} pending compaction tasks across cluster.")
        else:
            builder.success("No compaction backlog detected.")
        builder.blank()

        # Active compactions
        if total_active > 0:
            builder.text(f"**Active compactions:** {total_active}")
            builder.blank()

        # Pending tasks by node
        if nodes_with_pending:
            builder.h4("Pending Compaction Tasks by Node")
            pending_table = []
            for item in sorted(nodes_with_pending, key=lambda x: x['pending'], reverse=True):
                status = "🔴" if item['pending'] >= pending_critical else ("⚠️" if item['pending'] >= pending_warning else "")
                pending_table.append({
                    'Node': f"{status} {item['node']}",
                    'Pending Tasks': item['pending']
                })
            builder.table(pending_table)
            builder.blank()

        # Active compaction details
        all_active = []
        for stats in all_stats:
            for comp in stats['active_compactions']:
                comp['node'] = stats['node']
                all_active.append(comp)

        if all_active:
            builder.h4("Active Compactions")
            active_table = []
            for comp in all_active[:20]:  # Limit to 20
                active_table.append({
                    'Node': comp['node'],
                    'Type': comp['type'],
                    'Keyspace': comp['keyspace'],
                    'Table': comp['table'],
                    'Progress': comp.get('progress', 'N/A')
                })
            builder.table(active_table)
            builder.blank()

        # Detailed recommendations based on severity
        if total_pending >= pending_warning:
            builder.h4("Recommended Actions")

            if total_pending >= pending_critical:
                builder.critical(f"**URGENT: Severe compaction backlog ({total_pending} tasks)**")
                builder.text("This level of backlog impacts read performance and disk space.")
                builder.blank()

                builder.text("*Immediate Actions:*")
                builder.blank()
                builder.text("1. **Increase compaction throughput (immediate, no restart):**")
                builder.text("   ```")
                builder.text("   nodetool setcompactionthroughput 128")
                builder.text("   ```")
                builder.text("2. **Increase concurrent compactors (immediate, no restart):**")
                builder.text("   ```")
                builder.text("   nodetool setconcurrentcompactors 8")
                builder.text("   ```")
                builder.text("3. **Monitor progress:**")
                builder.text("   ```")
                builder.text("   watch -n 5 'nodetool compactionstats'")
                builder.text("   ```")
                builder.blank()
            else:
                builder.text("*Short-term Actions:*")
                builder.blank()
                builder.text("- Increase throughput: `nodetool setcompactionthroughput 64`")
                builder.text("- Increase compactors: `nodetool setconcurrentcompactors 4`")
                builder.blank()

            builder.text("*Make Changes Permanent (in cassandra.yaml):*")
            builder.blank()
            builder.text("```yaml")
            builder.text("# Compaction tuning")
            builder.text("compaction_throughput_mb_per_sec: 64  # Default: 16")
            builder.text("concurrent_compactors: 4              # Default: min(cpus, 4)")
            builder.text("```")
            builder.blank()

            builder.text("*Root Cause Analysis:*")
            builder.blank()
            builder.text("1. **Check write rate:** High write volume creates more SSTables")
            builder.text("   - `nodetool proxyhistograms` — check write latency trends")
            builder.text("2. **Check disk I/O capacity:**")
            builder.text("   - `iostat -x 1 5` — if %util >80%, disk is bottleneck")
            builder.text("3. **Review compaction strategy per table:**")
            builder.text("   - STCS: Good for write-heavy, but can accumulate SSTables")
            builder.text("   - LCS: Better for read-heavy, more I/O during compaction")
            builder.text("   - TWCS: Best for time-series with TTL")
            builder.blank()

            # Identify tables with most active compactions
            if all_active:
                active_tables = {}
                for comp in all_active:
                    key = f"{comp['keyspace']}.{comp['table']}"
                    active_tables[key] = active_tables.get(key, 0) + 1

                if active_tables:
                    top_tables = sorted(active_tables.items(), key=lambda x: x[1], reverse=True)[:3]
                    builder.text("*Tables with most active compactions:*")
                    builder.blank()
                    for table, count in top_tables:
                        builder.text(f"- `{table}`: {count} compaction(s) running")
                        builder.text(f"  Review strategy: `nodetool describetable {table.replace('.', ' ')}`")
                    builder.blank()

            builder.text("*Prevention:*")
            builder.blank()
            builder.text("- Monitor `pending tasks` metric and alert when >10")
            builder.text("- Schedule maintenance compaction during low-traffic periods")
            builder.text("- Consider pre-splitting large tables to distribute compaction load")
            builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_stats),
                'total_pending_tasks': total_pending,
                'total_active_compactions': total_active,
                'nodes_with_pending': len(nodes_with_pending),
                'nodes_with_active': len(nodes_with_active),
                'has_backlog': total_pending >= pending_warning,
                'has_severe_backlog': total_pending >= pending_critical,
                'node_stats': [{
                    'node': s['node'],
                    'pending_tasks': s['pending_tasks'],
                    'active_compactions': len(s['active_compactions'])
                } for s in all_stats]
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Compaction stats check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Compaction stats check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

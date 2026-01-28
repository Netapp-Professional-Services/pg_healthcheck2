"""
GC log analysis check for Cassandra.

Analyzes gc.log files to detect:
- Full GC events (major GC that pauses application)
- Long GC pause times
- Memory pressure indicators
- GC frequency and patterns

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def parse_gc_log(gc_log_content, max_events=100):
    """
    Parse GC log content into structured data.

    Supports multiple formats:
    - Java 8 CMS: [GC ... ParNew ... ] or [Full GC ... CMS ...]
    - Java 8 G1: [GC pause (G1 Evacuation Pause) ...]
    - Java 11+ Unified logging: [timestamp][info][gc] GC(N) ...

    Args:
        gc_log_content: Raw GC log content
        max_events: Maximum number of events to parse

    Returns:
        dict: Parsed GC statistics
    """
    gc_events = []
    full_gc_events = []
    pause_times = []
    full_gc_pauses = []

    lines = gc_log_content.strip().split('\n')

    for line in lines:
        event = None

        # Java 8 CMS/ParNew format
        # 2025-12-09T10:29:15.670-0700: 19307911.194: [GC (Allocation Failure) ... : 820379K->1496K(1228800K), 0.0154812 secs]
        if '[GC (' in line or '[Full GC (' in line:
            event = parse_java8_gc_line(line)

        # Java 11+ Unified logging format
        # [2025-01-01T10:00:00.000+0000][info][gc] GC(123) Pause Young ...
        elif '[gc]' in line.lower() and ('Pause' in line or 'pause' in line):
            event = parse_unified_gc_line(line)

        if event:
            pause_times.append(event.get('pause_ms', 0))

            if event.get('is_full_gc', False):
                full_gc_events.append(event)
                full_gc_pauses.append(event.get('pause_ms', 0))
            else:
                gc_events.append(event)

            if len(gc_events) + len(full_gc_events) >= max_events:
                break

    return {
        'gc_events': gc_events[-50:],  # Keep last 50 minor GCs
        'full_gc_events': full_gc_events,
        'total_gc_count': len(gc_events) + len(full_gc_events),
        'full_gc_count': len(full_gc_events),
        'avg_pause_ms': sum(pause_times) / len(pause_times) if pause_times else 0,
        'max_pause_ms': max(pause_times) if pause_times else 0,
        'avg_full_gc_pause_ms': sum(full_gc_pauses) / len(full_gc_pauses) if full_gc_pauses else 0,
        'max_full_gc_pause_ms': max(full_gc_pauses) if full_gc_pauses else 0,
    }


def parse_java8_gc_line(line):
    """Parse Java 8 GC log line."""
    event = {'raw': line[:200]}

    # Detect Full GC
    event['is_full_gc'] = '[Full GC' in line

    # Extract timestamp
    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', line)
    if timestamp_match:
        event['timestamp'] = timestamp_match.group(1)

    # Extract cause
    cause_match = re.search(r'\[(Full )?GC \(([^)]+)\)', line)
    if cause_match:
        event['cause'] = cause_match.group(2)

    # Extract pause time
    # Format: "0.0154812 secs]" or "0.0157116 secs]"
    pause_match = re.search(r'(\d+\.\d+) secs\]', line)
    if pause_match:
        event['pause_ms'] = float(pause_match.group(1)) * 1000

    # Extract heap sizes
    # Format: "2862946K->2044873K(7979008K)"
    heap_match = re.search(r'(\d+)K->(\d+)K\((\d+)K\)', line)
    if heap_match:
        event['heap_before_kb'] = int(heap_match.group(1))
        event['heap_after_kb'] = int(heap_match.group(2))
        event['heap_total_kb'] = int(heap_match.group(3))
        event['heap_reclaimed_kb'] = event['heap_before_kb'] - event['heap_after_kb']

    return event


def parse_unified_gc_line(line):
    """Parse Java 11+ unified GC log line."""
    event = {'raw': line[:200]}

    # Detect Full GC
    event['is_full_gc'] = 'Pause Full' in line

    # Extract timestamp from [timestamp]
    timestamp_match = re.search(r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', line)
    if timestamp_match:
        event['timestamp'] = timestamp_match.group(1)

    # Extract cause
    cause_match = re.search(r'Pause (?:Full|Young|Mixed) \(([^)]+)\)', line)
    if cause_match:
        event['cause'] = cause_match.group(1)

    # Extract pause time (ms)
    pause_match = re.search(r'(\d+\.?\d*)ms', line)
    if pause_match:
        event['pause_ms'] = float(pause_match.group(1))

    # Extract heap sizes
    heap_match = re.search(r'(\d+)([KMG])->(\d+)([KMG])\((\d+)([KMG])\)', line)
    if heap_match:
        def to_kb(val, unit):
            val = int(val)
            if unit == 'M':
                return val * 1024
            elif unit == 'G':
                return val * 1024 * 1024
            return val

        event['heap_before_kb'] = to_kb(heap_match.group(1), heap_match.group(2))
        event['heap_after_kb'] = to_kb(heap_match.group(3), heap_match.group(4))
        event['heap_total_kb'] = to_kb(heap_match.group(5), heap_match.group(6))
        event['heap_reclaimed_kb'] = event['heap_before_kb'] - event['heap_after_kb']

    return event


def run_check_gc_analysis(connector, settings):
    """
    Analyze GC logs for performance issues.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("GC Log Analysis")
    builder.para("Analyzing garbage collection logs for memory pressure and pause times.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "GC log analysis")
    if not available:
        return skip_msg, skip_data

    try:
        # Try to read GC logs - check multiple possible locations and file names
        gc_log_commands = [
            "cat /var/log/cassandra/gc.log.0 2>/dev/null || cat /var/log/cassandra/gc.log 2>/dev/null || echo 'GC_LOG_NOT_FOUND'",
        ]

        # For Instacollector, the connector should map this to loaded gc.log files
        results = connector.execute_ssh_on_all_hosts(
            gc_log_commands[0],
            "Read GC log"
        )

        # Thresholds
        pause_warning_ms = settings.get('gc_pause_warning_ms', 200)
        pause_critical_ms = settings.get('gc_pause_critical_ms', 1000)
        full_gc_warning = settings.get('gc_full_gc_warning_count', 5)
        full_gc_critical = settings.get('gc_full_gc_critical_count', 20)

        all_gc_stats = []
        critical_nodes = []
        warning_nodes = []

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            output = result['output']
            if 'GC_LOG_NOT_FOUND' in output:
                continue

            node_ip = result['host']
            gc_stats = parse_gc_log(output)
            gc_stats['node'] = node_ip
            all_gc_stats.append(gc_stats)

            # Check for issues
            has_critical = False
            has_warning = False

            if gc_stats['max_pause_ms'] >= pause_critical_ms:
                has_critical = True
            elif gc_stats['max_pause_ms'] >= pause_warning_ms:
                has_warning = True

            if gc_stats['full_gc_count'] >= full_gc_critical:
                has_critical = True
            elif gc_stats['full_gc_count'] >= full_gc_warning:
                has_warning = True

            if has_critical:
                critical_nodes.append(node_ip)
            elif has_warning:
                warning_nodes.append(node_ip)

        if not all_gc_stats:
            builder.note("GC logs not available. GC logging may not be enabled or logs are not in expected location.")
            return builder.build(), {'status': 'no_data'}

        # Summary
        total_full_gcs = sum(s['full_gc_count'] for s in all_gc_stats)
        max_pause = max(s['max_pause_ms'] for s in all_gc_stats)

        if critical_nodes:
            builder.critical(f"**{len(critical_nodes)} node(s) with critical GC issues**")
            builder.text(f"Total Full GCs detected: {total_full_gcs}")
            builder.text(f"Maximum pause time: {max_pause:.1f} ms")
        elif warning_nodes:
            builder.warning(f"**{len(warning_nodes)} node(s) with GC warnings**")
            builder.text(f"Total Full GCs detected: {total_full_gcs}")
        elif total_full_gcs > 0:
            builder.note(f"Detected {total_full_gcs} Full GC event(s) in logs.")
        else:
            builder.success("GC performance is healthy. No Full GC events detected.")
        builder.blank()

        # GC Summary table
        builder.h4("GC Statistics by Node")
        gc_table = []
        for stats in all_gc_stats:
            status = "🔴" if stats['node'] in critical_nodes else ("⚠️" if stats['node'] in warning_nodes else "✅")
            gc_table.append({
                'Node': f"{status} {stats['node']}",
                'Full GCs': stats['full_gc_count'],
                'Avg Pause (ms)': f"{stats['avg_pause_ms']:.1f}",
                'Max Pause (ms)': f"{stats['max_pause_ms']:.1f}",
                'Avg Full GC (ms)': f"{stats['avg_full_gc_pause_ms']:.1f}" if stats['full_gc_count'] > 0 else "N/A"
            })
        builder.table(gc_table)
        builder.blank()

        # Full GC events detail
        all_full_gcs = []
        for stats in all_gc_stats:
            for event in stats['full_gc_events']:
                event['node'] = stats['node']
                all_full_gcs.append(event)

        if all_full_gcs:
            builder.h4("Full GC Events")
            builder.warning(f"**{len(all_full_gcs)} Full GC event(s) detected**")
            builder.blank()
            builder.text("Full GCs pause the entire application and can cause timeouts.")
            builder.blank()

            # Group by cause
            causes = {}
            for event in all_full_gcs:
                cause = event.get('cause', 'Unknown')
                causes[cause] = causes.get(cause, 0) + 1

            if causes:
                builder.text("*Full GC Causes:*")
                builder.blank()
                for cause, count in sorted(causes.items(), key=lambda x: x[1], reverse=True):
                    builder.text(f"- **{cause}**: {count} event(s)")
                builder.blank()

            # Show recent Full GC events
            recent_full_gcs = sorted(all_full_gcs,
                                    key=lambda x: x.get('timestamp', ''),
                                    reverse=True)[:10]

            if recent_full_gcs:
                builder.text("*Recent Full GC Events (last 10):*")
                builder.blank()
                fgc_table = []
                for event in recent_full_gcs:
                    heap_before_mb = event.get('heap_before_kb', 0) / 1024
                    heap_after_mb = event.get('heap_after_kb', 0) / 1024
                    fgc_table.append({
                        'Node': event['node'],
                        'Timestamp': event.get('timestamp', 'N/A'),
                        'Cause': event.get('cause', 'Unknown'),
                        'Pause (ms)': f"{event.get('pause_ms', 0):.0f}",
                        'Heap Before→After': f"{heap_before_mb:.0f}→{heap_after_mb:.0f} MB"
                    })
                builder.table(fgc_table)
                builder.blank()

            # Comprehensive recommendations based on findings
            builder.h4("Recommended Actions")

            # Allocation Failure - most critical
            if 'Allocation Failure' in causes:
                builder.critical("**URGENT: Allocation Failures Detected**")
                builder.text("The JVM cannot allocate memory for new objects. This is critical!")
                builder.blank()
                builder.text("*Immediate Fix:*")
                builder.blank()
                builder.text("1. Increase heap size in `jvm.options` or `cassandra-env.sh`:")
                builder.text("   - Current recommendation: Set `-Xms` and `-Xmx` to 8GB-31GB")
                builder.text("   - Example: `-Xms16G -Xmx16G` (always set min=max)")
                builder.text("2. Restart Cassandra: `sudo systemctl restart cassandra`")
                builder.blank()

            builder.text("*GC Tuning Recommendations:*")
            builder.blank()

            builder.text("**1. If using CMS (older, deprecated):**")
            builder.text("   Migrate to G1GC by updating `jvm.options`:")
            builder.text("   ```")
            builder.text("   -XX:+UseG1GC")
            builder.text("   -XX:MaxGCPauseMillis=500")
            builder.text("   -XX:G1RSetUpdatingPauseTimePercent=5")
            builder.text("   -XX:InitiatingHeapOccupancyPercent=70")
            builder.text("   ```")
            builder.blank()

            builder.text("**2. If using G1GC (recommended):**")
            builder.text("   Fine-tune pause targets:")
            builder.text("   - For latency-sensitive: `-XX:MaxGCPauseMillis=200`")
            builder.text("   - For throughput: `-XX:MaxGCPauseMillis=500`")
            builder.blank()

            builder.text("**3. Heap Sizing Guidelines:**")
            builder.text("   - Minimum: 4GB (`-Xms4G -Xmx4G`)")
            builder.text("   - Production: 8GB-16GB typically")
            builder.text("   - Maximum: 31GB (stay under 32GB for compressed OOPs)")
            builder.text("   - Rule: Leave 50% of RAM for OS page cache")
            builder.blank()

            builder.text("**4. Monitor After Changes:**")
            builder.text("   - Watch GC logs: `tail -f /var/log/cassandra/gc.log`")
            builder.text("   - Check stats: `nodetool gcstats`")
            builder.text("   - Target: <200ms avg pause, <1s max pause")
            builder.blank()

            # Specific recommendations based on pause times
            if max_pause > 5000:
                builder.warning(f"**Very High Pause Time ({max_pause:.0f}ms):**")
                builder.blank()
                builder.text("Pauses >5 seconds indicate serious memory pressure.")
                builder.text("- Immediately review heap size")
                builder.text("- Check for large in-memory caches (row cache, key cache)")
                builder.text("- Consider disabling row cache: `ALTER TABLE ... WITH caching = {{'keys': 'ALL', 'rows_per_partition': 'NONE'}}`")
                builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_gc_stats),
                'critical_nodes': len(critical_nodes),
                'warning_nodes': len(warning_nodes),
                'total_full_gc_count': total_full_gcs,
                'max_pause_ms': max_pause,
                'has_allocation_failures': any(
                    'Allocation Failure' in str(e.get('cause', ''))
                    for s in all_gc_stats
                    for e in s['full_gc_events']
                ),
                'node_stats': [{
                    'node': s['node'],
                    'full_gc_count': s['full_gc_count'],
                    'avg_pause_ms': s['avg_pause_ms'],
                    'max_pause_ms': s['max_pause_ms']
                } for s in all_gc_stats]
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"GC analysis check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"GC analysis check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

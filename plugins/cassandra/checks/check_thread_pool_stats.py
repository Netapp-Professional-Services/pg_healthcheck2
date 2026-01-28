"""
Thread pool statistics check for Cassandra.

Analyzes nodetool tpstats output to detect:
- Pending tasks (indicates backpressure)
- Blocked tasks (indicates severe bottlenecks)
- Dropped messages (indicates overload)

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9


def parse_tpstats(tpstats_output):
    """
    Parse nodetool tpstats output into structured data.

    Args:
        tpstats_output: Raw tpstats output

    Returns:
        dict: Parsed thread pool stats and dropped messages
    """
    thread_pools = []
    dropped_messages = []

    lines = tpstats_output.strip().split('\n')
    section = 'pools'  # 'pools' or 'dropped'

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Detect section change
        if line.startswith('Message type'):
            section = 'dropped'
            continue
        if line.startswith('Pool Name'):
            section = 'pools'
            continue

        if section == 'pools':
            # Parse thread pool line
            # Format: PoolName  Active  Pending  Completed  Blocked  AllTimeBlocked
            parts = line.split()
            if len(parts) >= 6:
                try:
                    pool = {
                        'name': parts[0],
                        'active': int(parts[1]),
                        'pending': int(parts[2]),
                        'completed': int(parts[3]),
                        'blocked': int(parts[4]),
                        'all_time_blocked': int(parts[5])
                    }
                    thread_pools.append(pool)
                except (ValueError, IndexError):
                    continue

        elif section == 'dropped':
            # Parse dropped message line
            # Format: MESSAGE_TYPE  count
            parts = line.split()
            if len(parts) >= 2:
                try:
                    msg_type = parts[0]
                    count = int(parts[1])
                    if count > 0:  # Only track non-zero drops
                        dropped_messages.append({
                            'type': msg_type,
                            'count': count
                        })
                except (ValueError, IndexError):
                    continue

    return {
        'thread_pools': thread_pools,
        'dropped_messages': dropped_messages
    }


def run_check_thread_pool_stats(connector, settings):
    """
    Check thread pool statistics for signs of backpressure or overload.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Thread Pool Statistics")
    builder.para("Analyzing thread pool health and message drops from `nodetool tpstats`.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Thread pool stats")
    if not available:
        return skip_msg, skip_data

    try:
        # Get tpstats from all hosts
        results = connector.execute_ssh_on_all_hosts(
            "nodetool tpstats",
            "Get thread pool statistics"
        )

        # Thresholds
        pending_warning = settings.get('tpstats_pending_warning', 100)
        pending_critical = settings.get('tpstats_pending_critical', 1000)
        blocked_warning = settings.get('tpstats_blocked_warning', 1)
        dropped_warning = settings.get('tpstats_dropped_warning', 100)
        dropped_critical = settings.get('tpstats_dropped_critical', 10000)

        all_node_stats = []
        issues = []
        critical_nodes = []
        warning_nodes = []

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            stats = parse_tpstats(result['output'])
            stats['node'] = node_ip
            all_node_stats.append(stats)

            node_has_critical = False
            node_has_warning = False

            # Check for pending tasks
            for pool in stats['thread_pools']:
                if pool['pending'] >= pending_critical:
                    issues.append({
                        'node': node_ip,
                        'type': 'critical',
                        'pool': pool['name'],
                        'message': f"Critical pending tasks: {pool['pending']}"
                    })
                    node_has_critical = True
                elif pool['pending'] >= pending_warning:
                    issues.append({
                        'node': node_ip,
                        'type': 'warning',
                        'pool': pool['name'],
                        'message': f"High pending tasks: {pool['pending']}"
                    })
                    node_has_warning = True

                # Check for blocked tasks
                if pool['blocked'] >= blocked_warning or pool['all_time_blocked'] > 0:
                    issues.append({
                        'node': node_ip,
                        'type': 'critical' if pool['blocked'] > 0 else 'warning',
                        'pool': pool['name'],
                        'message': f"Blocked: {pool['blocked']}, All-time blocked: {pool['all_time_blocked']}"
                    })
                    if pool['blocked'] > 0:
                        node_has_critical = True
                    else:
                        node_has_warning = True

            # Check for dropped messages
            total_dropped = sum(d['count'] for d in stats['dropped_messages'])
            if total_dropped >= dropped_critical:
                node_has_critical = True
            elif total_dropped >= dropped_warning:
                node_has_warning = True

            if node_has_critical:
                critical_nodes.append(node_ip)
            elif node_has_warning:
                warning_nodes.append(node_ip)

        # Build output
        if not all_node_stats:
            builder.warning("No thread pool statistics available.")
            return builder.build(), {'status': 'no_data'}

        # Show critical/warning summary
        if critical_nodes:
            builder.critical(f"**{len(critical_nodes)} node(s) with critical thread pool issues**")
            builder.blank()
        elif warning_nodes:
            builder.warning(f"**{len(warning_nodes)} node(s) with thread pool warnings**")
            builder.blank()
        else:
            builder.success("Thread pools are healthy across all nodes.")
            builder.blank()

        # Show pending tasks if any
        pending_pools = []
        for stats in all_node_stats:
            for pool in stats['thread_pools']:
                if pool['pending'] > 0:
                    pending_pools.append({
                        'Node': stats['node'],
                        'Pool': pool['name'],
                        'Pending': pool['pending'],
                        'Active': pool['active'],
                        'Blocked': pool['blocked']
                    })

        if pending_pools:
            builder.h4("Pending Tasks")
            builder.table(pending_pools[:20])  # Limit to top 20
            builder.blank()

        # Show blocked pools if any
        blocked_pools = []
        for stats in all_node_stats:
            for pool in stats['thread_pools']:
                if pool['blocked'] > 0 or pool['all_time_blocked'] > 0:
                    blocked_pools.append({
                        'Node': stats['node'],
                        'Pool': pool['name'],
                        'Currently Blocked': pool['blocked'],
                        'All-Time Blocked': pool['all_time_blocked']
                    })

        if blocked_pools:
            builder.h4("Blocked Thread Pools")

            # Check if any are CURRENTLY blocked vs just historically
            currently_blocked = [b for b in blocked_pools if b['Currently Blocked'] > 0]
            only_historical = [b for b in blocked_pools if b['Currently Blocked'] == 0 and b['All-Time Blocked'] > 0]

            if currently_blocked:
                builder.critical("**Active thread blocking detected — requests are being delayed NOW!**")
            else:
                builder.warning("**Historical thread blocking detected** (no current blocks)")
                builder.text("_Note: 'All-Time Blocked' counts are cumulative since node startup._")
            builder.blank()

            builder.table(blocked_pools)
            builder.blank()

            if currently_blocked:
                builder.text("*Why This Matters:*")
                builder.blank()
                builder.text("Blocked threads mean requests are waiting because the system cannot keep up.")
                builder.text("This causes client timeouts and degraded performance.")
                builder.blank()

                builder.text("*Immediate Actions:*")
                builder.blank()
                builder.text("1. **Check disk I/O:** `iostat -x 1 5` — Look for high %util or await times")
                builder.text("2. **Check GC pressure:** `nodetool gcstats` — High GC pauses block threads")
                builder.text("3. **Check network:** `netstat -s | grep -i error` — Network issues cause backpressure")
                builder.text("4. **Reduce load:** Consider enabling client-side rate limiting temporarily")
                builder.blank()
            else:
                builder.text("*Historical Blocking Analysis:*")
                builder.blank()
                builder.text("These blocks occurred in the past but are not happening now.")
                builder.text("Review node uptimes to understand the time period covered.")
                builder.blank()

            # Identify which pools are blocked
            blocked_pool_names = set(b['Pool'] for b in blocked_pools)
            if 'MutationStage' in blocked_pool_names:
                builder.text("*MutationStage:* Write request thread pool")
                builder.text("   → Check commitlog disk speed, increase `concurrent_writes`")
                builder.blank()
            if 'ReadStage' in blocked_pool_names:
                builder.text("*ReadStage:* Read request thread pool")
                builder.text("   → Check data disk I/O, increase `concurrent_reads`")
                builder.blank()
            if 'Native-Transport-Requests' in blocked_pool_names:
                builder.text("*Native-Transport-Requests:* Client connection handler")
                builder.text("   → Increase `native_transport_max_threads` or reduce client connections")
                builder.blank()

        # Show dropped messages
        all_dropped = []
        for stats in all_node_stats:
            if stats['dropped_messages']:
                for drop in stats['dropped_messages']:
                    all_dropped.append({
                        'Node': stats['node'],
                        'Message Type': drop['type'],
                        'Dropped Count': drop['count']
                    })

        if all_dropped:
            builder.h4("Dropped Messages")
            total_drops = sum(d['Dropped Count'] for d in all_dropped)

            if total_drops >= dropped_critical:
                builder.critical(f"**{total_drops:,} total messages dropped**")
            else:
                builder.warning(f"**{total_drops:,} total messages dropped**")
            builder.blank()

            builder.text("_Note: These counts are cumulative since each node's last restart._")
            builder.text("_Check node uptimes in Cluster Overview to understand the time period._")
            builder.blank()

            builder.table(all_dropped)
            builder.blank()

            # Categorize dropped message types for targeted recommendations
            dropped_types = {}
            for d in all_dropped:
                msg_type = d['Message Type']
                dropped_types[msg_type] = dropped_types.get(msg_type, 0) + d['Dropped Count']

            builder.text("*Impact Analysis:*")
            builder.blank()
            if 'MUTATION' in dropped_types:
                builder.text(f"- **MUTATION ({dropped_types['MUTATION']:,})**: Write requests failed — clients saw write timeouts")
            if 'READ' in dropped_types:
                builder.text(f"- **READ ({dropped_types['READ']:,})**: Read requests failed — clients saw read timeouts")
            if 'READ_REPAIR' in dropped_types:
                builder.text(f"- **READ_REPAIR ({dropped_types['READ_REPAIR']:,})**: Consistency repairs skipped — potential data inconsistency")
            if 'HINT' in dropped_types:
                builder.text(f"- **HINT ({dropped_types['HINT']:,})**: Hints dropped — recovery after node failure may be incomplete")
            if 'RANGE_SLICE' in dropped_types:
                builder.text(f"- **RANGE_SLICE ({dropped_types['RANGE_SLICE']:,})**: Range scans failed — application queries failed")
            if 'REQUEST_RESPONSE' in dropped_types:
                builder.text(f"- **REQUEST_RESPONSE ({dropped_types['REQUEST_RESPONSE']:,})**: Internode responses dropped")
            builder.blank()

            builder.text("*Root Cause Investigation:*")
            builder.blank()
            builder.text("1. **Check timeouts in cassandra.yaml:**")
            builder.text("   - `read_request_timeout_in_ms` (default: 5000)")
            builder.text("   - `write_request_timeout_in_ms` (default: 2000)")
            builder.text("   - Consider increasing if network latency is high")
            builder.text("2. **Check cluster capacity:**")
            builder.text("   - CPU: `top -d 1` — sustained >80% indicates need to scale")
            builder.text("   - Memory: `free -g` — check for swapping")
            builder.text("   - Disk: `iostat -x 1` — high await times")
            builder.text("3. **Check for hot partitions:**")
            builder.text("   - Enable `nodetool toppartitions` during load")
            builder.text("   - Review data model for partition key distribution")
            builder.blank()

            builder.text("*Remediation Steps:*")
            builder.blank()
            builder.text("1. **Short-term:** Increase timeouts to prevent drops during spikes")
            builder.text("2. **Medium-term:** Scale cluster horizontally (add nodes)")
            builder.text("3. **Long-term:** Review data model and query patterns")
            builder.blank()

        # General recommendations for pending tasks
        if pending_pools:
            builder.h4("Pending Tasks Analysis")
            builder.blank()

            builder.text("*Why This Matters:*")
            builder.blank()
            builder.text("Pending tasks indicate the cluster is receiving work faster than it can process.")
            builder.text("Sustained high pending counts lead to blocked threads and dropped messages.")
            builder.blank()

            builder.text("*Recommended Actions by Pool:*")
            builder.blank()

            # Identify which pools have pending tasks
            pending_pool_names = set(p['Pool'] for p in pending_pools)

            # Known pool-specific recommendations
            pool_recommendations = {
                'CompactionExecutor': ("Compaction backlog", "Increase `compaction_throughput_mb_per_sec` and `concurrent_compactors`"),
                'MemtableFlushWriter': ("Memtable flushes backing up", "Check commitlog disk speed, consider SSD for commitlog"),
                'MemtablePostFlush': ("Post-flush processing delayed", "Check disk I/O performance"),
                'ReadStage': ("Read requests queuing", "Check data disk I/O, increase `concurrent_reads`"),
                'MutationStage': ("Write requests queuing", "Check commitlog disk, increase `concurrent_writes`"),
                'RequestResponseStage': ("Internode responses delayed", "Check network latency between nodes"),
                'Native-Transport-Requests': ("Client requests queuing", "Increase `native_transport_max_threads`"),
                'GossipStage': ("Gossip protocol delayed", "Check network connectivity, may indicate node issues"),
                'AntiEntropyStage': ("Repair operations queuing", "Reduce repair parallelism or schedule during low traffic"),
                'ValidationExecutor': ("Validation tasks queuing", "Reduce repair stream throughput"),
                'UserDefinedFunctions': ("UDF execution delayed", "Review UDF complexity, consider increasing thread pool"),
            }

            found_recommendations = False
            for pool_name in sorted(pending_pool_names):
                if pool_name in pool_recommendations:
                    desc, action = pool_recommendations[pool_name]
                    builder.text(f"- **{pool_name}:** {desc}")
                    builder.text(f"  → {action}")
                    builder.blank()
                    found_recommendations = True
                else:
                    # Generic recommendation for unknown pools
                    builder.text(f"- **{pool_name}:** Tasks pending in this pool")
                    builder.text(f"  → Monitor and check system resources")
                    builder.blank()
                    found_recommendations = True

            if not found_recommendations:
                builder.text("- Monitor pending task counts and investigate if they persist")
                builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_node_stats),
                'critical_nodes': len(critical_nodes),
                'warning_nodes': len(warning_nodes),
                'total_dropped_messages': sum(
                    sum(d['count'] for d in s['dropped_messages'])
                    for s in all_node_stats
                ),
                'has_blocked_threads': any(
                    any(p['blocked'] > 0 for p in s['thread_pools'])
                    for s in all_node_stats
                ),
                'has_pending_tasks': any(
                    any(p['pending'] > 0 for p in s['thread_pools'])
                    for s in all_node_stats
                ),
                'node_stats': all_node_stats
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Thread pool stats check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Thread pool stats check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

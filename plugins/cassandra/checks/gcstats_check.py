"""
Garbage Collection Statistics check for Cassandra nodes.

Analyzes GC activity and heap usage across all nodes in the cluster.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import json
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def run_gcstats_check(connector, settings):
    """
    Analyzes garbage collection statistics on all Cassandra nodes using 'nodetool gcstats'.

    Args:
        connector: Cassandra connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "GC stats check")
    if not available:
        return skip_msg, skip_data

    try:
        # Get thresholds
        max_gc_elapsed_warning = settings.get('cassandra_gc_elapsed_warning_ms', 1000)  # 1 second
        max_gc_elapsed_critical = settings.get('cassandra_gc_elapsed_critical_ms', 5000)  # 5 seconds

        builder.h3("Garbage Collection Statistics (All Nodes)")
        builder.para("Analyzing GC activity and heap usage using `nodetool gcstats`.")
        builder.blank()

        # === CHECK ALL NODES ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_gc_data = []
        issues_found = False
        critical_nodes = []
        warning_nodes = []
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            node_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                ssh_manager = connector.get_ssh_manager(ssh_host)
                if not ssh_manager:
                    continue

                ssh_manager.ensure_connected()

                # Execute nodetool gcstats
                command = "nodetool gcstats"
                stdout, stderr, exit_code = ssh_manager.execute_command(command)

                if exit_code != 0:
                    logger.warning(f"nodetool gcstats failed on {ssh_host}: {stderr}")
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': f"nodetool gcstats failed: {stderr}"
                    })
                    continue

                # Parse the output using the NodetoolParser
                from plugins.common.parsers import NodetoolParser
                parser = NodetoolParser()
                raw = parser.parse('gcstats', stdout)

                # Raw should be a dict with GC stats
                if not isinstance(raw, dict) or not raw:
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': 'No GC statistics returned'
                    })
                    continue

                # Extract GC metrics from parsed data
                interval_ms = raw.get('interval_ms')
                max_gc_elapsed_ms = raw.get('max_gc_elapsed_ms')
                total_gc_elapsed_ms = raw.get('total_gc_elapsed_ms')
                stdev_gc_elapsed_ms = raw.get('stdev_gc_elapsed_ms')
                gc_reclaimed_mb = raw.get('gc_reclaimed_mb')
                collections = raw.get('collections')

                # Skip if no valid data (all None)
                if max_gc_elapsed_ms is None and total_gc_elapsed_ms is None:
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': 'No valid GC data in output'
                    })
                    continue

                # Handle None values (convert to 0 for calculations)
                max_gc_elapsed_ms = max_gc_elapsed_ms or 0
                total_gc_elapsed_ms = total_gc_elapsed_ms or 0
                collections = collections or 0
                gc_reclaimed_mb = gc_reclaimed_mb or 0

                # Calculate average GC pause time
                avg_gc_elapsed_ms = (total_gc_elapsed_ms / collections) if collections > 0 else 0

                gc_info = {
                    'host': ssh_host,
                    'node_id': node_id,
                    'interval_ms': interval_ms,
                    'max_gc_elapsed_ms': max_gc_elapsed_ms,
                    'total_gc_elapsed_ms': total_gc_elapsed_ms,
                    'avg_gc_elapsed_ms': avg_gc_elapsed_ms,
                    'stdev_gc_elapsed_ms': stdev_gc_elapsed_ms or 0,
                    'gc_reclaimed_mb': gc_reclaimed_mb,
                    'collections': collections,
                    'exceeds_critical': max_gc_elapsed_ms >= max_gc_elapsed_critical,
                    'exceeds_warning': max_gc_elapsed_ms >= max_gc_elapsed_warning
                }
                all_gc_data.append(gc_info)

                # === Check thresholds ===
                if max_gc_elapsed_ms >= max_gc_elapsed_critical:
                    issues_found = True
                    if node_id not in critical_nodes:
                        critical_nodes.append(node_id)

                    builder.critical_issue(
                        "Critical GC Pause Time",
                        {
                            "Node": f"{node_id} ({ssh_host})",
                            "Max GC Pause": f"{max_gc_elapsed_ms} ms",
                            "Threshold": f"{max_gc_elapsed_critical} ms",
                            "Total GC Time": f"{total_gc_elapsed_ms} ms",
                            "Collections": str(collections),
                            "Avg Pause": f"{avg_gc_elapsed_ms:.2f} ms"
                        }
                    )
                    builder.para("**⚠️ Excessive GC pause times can cause query timeouts!**")
                    builder.blank()

                elif max_gc_elapsed_ms >= max_gc_elapsed_warning:
                    issues_found = True
                    if node_id not in warning_nodes:
                        warning_nodes.append(node_id)

                    builder.warning_issue(
                        "High GC Pause Time",
                        {
                            "Node": f"{node_id} ({ssh_host})",
                            "Max GC Pause": f"{max_gc_elapsed_ms} ms",
                            "Threshold": f"{max_gc_elapsed_warning} ms",
                            "Collections": str(collections)
                        }
                    )

            except Exception as e:
                logger.error(f"Error checking GC stats on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'node_id': node_id,
                    'error': str(e)
                })

        # === SUMMARY TABLE ===
        if all_gc_data:
            builder.h4("GC Statistics Summary")

            table_lines = [
                "|===",
                "|Node|Host|Collections|Max Pause (ms)|Avg Pause (ms)|Total GC Time (ms)|GC Reclaimed (MB)"
            ]

            for gc in sorted(all_gc_data, key=lambda x: x['max_gc_elapsed_ms'], reverse=True):
                indicator = ""
                if gc['exceeds_critical']:
                    indicator = "🔴 "
                elif gc['exceeds_warning']:
                    indicator = "⚠️ "

                table_lines.append(
                    f"|{gc['node_id']}|{gc['host']}|{gc['collections']}|"
                    f"{indicator}{gc['max_gc_elapsed_ms']}|{gc['avg_gc_elapsed_ms']:.2f}|"
                    f"{gc['total_gc_elapsed_ms']}|{gc['gc_reclaimed_mb']}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check GC stats on {len(errors)} node(s):\n\n" +
                "\n".join([f"* Node {e['node_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if critical_nodes:
                recommendations["critical"] = [
                    "**Review JVM heap settings:** Check `nodetool info` for heap usage - may need to increase heap size",
                    "**Enable GC logging:** Add `-XX:+PrintGCDetails` to cassandra-env.sh for detailed analysis",
                    "**Check for memory leaks:** Review heap dumps if GC frequency is increasing",
                    "**Optimize queries:** Long-running queries can increase GC pressure",
                    "**Consider G1GC:** For heaps >4GB, G1GC may provide better pause times than CMS"
                ]

            if warning_nodes:
                recommendations["high"] = [
                    "**Monitor GC trends:** Track GC pause times over time",
                    "**Review heap utilization:** Use `nodetool info` to check heap usage patterns",
                    "**Check compaction activity:** Heavy compactions increase GC pressure",
                    "**Tune GC parameters:** Consider adjusting NewSize and MaxNewSize based on workload"
                ]

            recommendations["general"] = [
                "Set up alerts for GC pause times exceeding 500ms",
                "Monitor correlation between GC and query latency",
                "Regular heap dump analysis during high GC periods",
                "Review cassandra.yaml settings: memtable sizes, cache sizes",
                "Consider upgrading to newer Java versions with improved GC"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"GC performance is healthy across all nodes.\n\n"
                f"All nodes have GC pause times below {max_gc_elapsed_warning} ms."
            )

        # === STRUCTURED DATA ===
        structured_data["gc_stats"] = {
            "status": "success",
            "nodes_checked": len(connector.get_ssh_hosts()),
            "nodes_with_errors": len(set(e['node_id'] for e in errors)),
            "critical_nodes": critical_nodes,
            "warning_nodes": warning_nodes,
            "thresholds": {
                "warning_ms": max_gc_elapsed_warning,
                "critical_ms": max_gc_elapsed_critical
            },
            "errors": errors,
            "data": all_gc_data
        }

    except Exception as e:
        import traceback
        logger.error(f"GC stats check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["gc_stats"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data
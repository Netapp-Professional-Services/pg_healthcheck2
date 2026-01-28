"""
Executive Summary check for Cassandra Instacollector imports.

Aggregates data from all other checks to provide:
- Overall cluster health score
- Quick status overview
- Key concerns ranked by severity
- Cross-node comparisons
- Resource utilization summary

This check should run AFTER all other checks to aggregate their findings.
"""

from plugins.common.check_helpers import CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight - run last to aggregate."""
    return 1


def calculate_health_score(findings):
    """
    Calculate overall cluster health score (0-100).

    Args:
        findings: Dict of all check findings

    Returns:
        int: Health score 0-100
    """
    score = 100
    deductions = []

    # Check cluster overview
    overview = findings.get('check_cluster_overview', {}).get('data', {})
    if overview.get('nodes_down', 0) > 0:
        score -= 20
        deductions.append(f"Nodes down: -{20}")

    # Check gossip state
    gossip = findings.get('check_gossip_state', {}).get('data', {})
    if gossip.get('has_schema_mismatch', False):
        score -= 25
        deductions.append(f"Schema mismatch: -{25}")
    if gossip.get('has_non_normal_nodes', False):
        score -= 15
        deductions.append(f"Nodes not normal: -{15}")
    if gossip.get('has_mixed_versions', False):
        score -= 10
        deductions.append(f"Mixed versions: -{10}")

    # Check thread pools
    threads = findings.get('check_thread_pool_stats', {}).get('data', {})
    dropped = threads.get('total_dropped_messages', 0)
    if dropped > 100000:
        score -= 20
        deductions.append(f"High dropped messages ({dropped:,}): -{20}")
    elif dropped > 10000:
        score -= 10
        deductions.append(f"Dropped messages ({dropped:,}): -{10}")
    elif dropped > 1000:
        score -= 5
        deductions.append(f"Some dropped messages ({dropped:,}): -{5}")

    if threads.get('has_blocked_threads', False):
        score -= 15
        deductions.append(f"Blocked threads: -{15}")

    # Check compaction
    compaction = findings.get('check_compaction_stats', {}).get('data', {})
    if compaction.get('has_severe_backlog', False):
        score -= 15
        deductions.append(f"Compaction backlog severe: -{15}")
    elif compaction.get('has_backlog', False):
        score -= 8
        deductions.append(f"Compaction backlog: -{8}")

    # Check GC
    gc = findings.get('check_gc_analysis', {}).get('data', {})
    if gc.get('critical_nodes', 0) > 0:
        score -= 15
        deductions.append(f"GC critical nodes: -{15}")
    if gc.get('has_allocation_failures', False):
        score -= 10
        deductions.append(f"Allocation failures: -{10}")

    # Check I/O
    io = findings.get('check_io_stats', {}).get('data', {})
    if io.get('has_critical_io', False):
        score -= 15
        deductions.append(f"Critical I/O: -{15}")
    elif io.get('has_io_issues', False):
        score -= 8
        deductions.append(f"I/O issues: -{8}")

    # Check system log
    logs = findings.get('check_system_log', {}).get('data', {})
    if logs.get('has_oom_errors', False):
        score -= 20
        deductions.append(f"OOM errors: -{20}")
    if logs.get('has_critical_errors', False):
        score -= 15
        deductions.append(f"Critical errors: -{15}")
    elif logs.get('total_errors', 0) > 10:
        score -= 5
        deductions.append(f"Errors in logs: -{5}")

    # Check config issues
    config = findings.get('check_config_audit', {}).get('data', {})
    if not config.get('authentication_enabled', True):
        score -= 15
        deductions.append(f"Auth disabled: -{15}")

    jvm = findings.get('check_jvm_config', {}).get('data', {})
    if jvm.get('uses_cms', False):
        score -= 5
        deductions.append(f"Deprecated CMS GC: -{5}")

    # Check ring balance
    ring = findings.get('check_ring_analysis', {}).get('data', {})
    if ring.get('has_severe_imbalance', False):
        score -= 10
        deductions.append(f"Severe ring imbalance: -{10}")

    # Check CVEs
    cve = findings.get('check_cve_vulnerabilities', {}).get('data', {})
    critical_cves = cve.get('critical_count', 0)
    if critical_cves > 0:
        score -= min(20, critical_cves * 5)
        deductions.append(f"Critical CVEs ({critical_cves}): -{min(20, critical_cves * 5)}")

    return max(0, score), deductions


def get_health_grade(score):
    """Convert score to letter grade."""
    if score >= 90:
        return 'A', 'Excellent'
    elif score >= 80:
        return 'B', 'Good'
    elif score >= 70:
        return 'C', 'Fair'
    elif score >= 60:
        return 'D', 'Poor'
    else:
        return 'F', 'Critical'


def get_key_concerns(findings):
    """
    Extract and rank key concerns from all findings.

    Returns list of (severity, concern, recommendation)
    """
    concerns = []

    # Check for schema mismatch
    gossip = findings.get('check_gossip_state', {}).get('data', {})
    if gossip.get('has_schema_mismatch', False):
        concerns.append(('CRITICAL', 'Schema version mismatch across nodes',
                        'Run nodetool describecluster and resolve schema inconsistency'))

    # Check for down nodes
    overview = findings.get('check_cluster_overview', {}).get('data', {})
    down = overview.get('nodes_down', 0)
    if down > 0:
        concerns.append(('CRITICAL', f'{down} node(s) are DOWN',
                        'Investigate and restore failed nodes immediately'))

    # Check for high dropped messages
    threads = findings.get('check_thread_pool_stats', {}).get('data', {})
    dropped = threads.get('total_dropped_messages', 0)
    if dropped > 100000:
        concerns.append(('CRITICAL', f'{dropped:,} messages dropped - cluster overloaded',
                        'Scale cluster, reduce load, or increase timeouts'))
    elif dropped > 10000:
        concerns.append(('HIGH', f'{dropped:,} messages dropped',
                        'Monitor closely and review timeout settings'))

    # Check for blocked threads
    if threads.get('has_blocked_threads', False):
        concerns.append(('CRITICAL', 'Blocked threads detected - severe backpressure',
                        'Check for slow disks, GC pauses, or network issues'))

    # Check for OOM errors
    logs = findings.get('check_system_log', {}).get('data', {})
    if logs.get('has_oom_errors', False):
        concerns.append(('CRITICAL', 'Out of memory errors in logs',
                        'Increase heap size or reduce load'))

    # Check for compaction backlog
    compaction = findings.get('check_compaction_stats', {}).get('data', {})
    if compaction.get('has_severe_backlog', False):
        pending = compaction.get('total_pending_tasks', 0)
        concerns.append(('HIGH', f'Severe compaction backlog ({pending} pending tasks)',
                        'Increase compaction throughput'))

    # Check for GC issues
    gc = findings.get('check_gc_analysis', {}).get('data', {})
    if gc.get('critical_nodes', 0) > 0:
        concerns.append(('HIGH', f"{gc.get('critical_nodes')} node(s) with critical GC issues",
                        'Review GC settings and heap size'))

    # Check for I/O issues
    io = findings.get('check_io_stats', {}).get('data', {})
    if io.get('has_critical_io', False):
        concerns.append(('HIGH', f"Critical disk I/O utilization ({io.get('max_disk_util', 0):.0f}%)",
                        'Consider faster storage or spreading load'))

    # Check for CVEs
    cve = findings.get('check_cve_vulnerabilities', {}).get('data', {})
    critical_cves = cve.get('critical_count', 0)
    if critical_cves > 0:
        concerns.append(('HIGH', f'{critical_cves} critical CVE(s) for this version',
                        'Plan upgrade to patched version'))

    # Check for deprecated GC
    jvm = findings.get('check_jvm_config', {}).get('data', {})
    if jvm.get('uses_cms', False):
        concerns.append(('MEDIUM', 'Deprecated CMS garbage collector in use',
                        'Migrate to G1GC for better performance'))

    # Check for auth disabled
    config = findings.get('check_config_audit', {}).get('data', {})
    if not config.get('authentication_enabled', True):
        concerns.append(('CRITICAL', 'Authentication is DISABLED',
                        'Enable PasswordAuthenticator immediately'))

    # Check for mixed versions
    if gossip.get('has_mixed_versions', False):
        concerns.append(('MEDIUM', 'Mixed Cassandra versions in cluster',
                        'Complete rolling upgrade to single version'))

    # Sort by severity
    severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2}
    concerns.sort(key=lambda x: severity_order.get(x[0], 3))

    return concerns


def get_resource_summary(findings):
    """
    Aggregate resource utilization across nodes.
    """
    resources = {
        'total_nodes': 0,
        'total_load_gb': 0,
        'avg_load_gb': 0,
        'max_heap_pct': 0,
        'avg_heap_pct': 0,
        'max_disk_util': 0,
        'total_dropped': 0,
    }

    # From gossip state
    gossip = findings.get('check_gossip_state', {}).get('data', {})
    resources['total_nodes'] = gossip.get('total_nodes', 0)
    resources['total_load_gb'] = gossip.get('max_load_gb', 0) * resources['total_nodes']  # Approximation
    resources['avg_load_gb'] = (gossip.get('min_load_gb', 0) + gossip.get('max_load_gb', 0)) / 2

    # From cluster overview - heap usage
    overview = findings.get('check_cluster_overview', {}).get('data', {})
    nodes = overview.get('nodes', [])
    if nodes:
        heap_pcts = []
        for node in nodes:
            if isinstance(node, dict) and 'heap_used_pct' in node:
                try:
                    heap_pcts.append(float(node['heap_used_pct']))
                except (ValueError, TypeError):
                    pass
        if heap_pcts:
            resources['max_heap_pct'] = max(heap_pcts)
            resources['avg_heap_pct'] = sum(heap_pcts) / len(heap_pcts)

    # From I/O stats
    io = findings.get('check_io_stats', {}).get('data', {})
    resources['max_disk_util'] = io.get('max_disk_util', 0)

    # From thread pools
    threads = findings.get('check_thread_pool_stats', {}).get('data', {})
    resources['total_dropped'] = threads.get('total_dropped_messages', 0)

    return resources


def _sum_dropped_messages(node_stat):
    """Sum dropped messages from node stat entry."""
    dropped = node_stat.get('dropped_messages', [])
    if isinstance(dropped, list):
        # List of {type: X, count: Y} dicts
        return sum(d.get('count', 0) for d in dropped if isinstance(d, dict))
    elif isinstance(dropped, (int, float)):
        return dropped
    return 0


def get_node_comparison(findings):
    """
    Build cross-node comparison data.
    """
    comparisons = []

    # Get node-level stats from thread pools
    threads = findings.get('check_thread_pool_stats', {}).get('data', {})
    node_stats = threads.get('node_stats', [])

    if node_stats and isinstance(node_stats, list):
        # Find node with most dropped messages
        # Calculate dropped count for each node
        node_dropped = []
        for ns in node_stats:
            if isinstance(ns, dict):
                total_dropped = _sum_dropped_messages(ns)
                node_dropped.append((ns.get('node', 'Unknown'), total_dropped))

        if node_dropped:
            max_node = max(node_dropped, key=lambda x: x[1])
            if max_node[1] > 0:
                comparisons.append({
                    'metric': 'Most Dropped Messages',
                    'node': max_node[0],
                    'value': f"{max_node[1]:,}"
                })

    # From cluster overview - heap usage
    overview = findings.get('check_cluster_overview', {}).get('data', {})
    nodes = overview.get('nodes', [])
    if nodes:
        node_heap = [(n.get('address', 'Unknown'), float(n.get('heap_used_pct', 0)))
                     for n in nodes if isinstance(n, dict) and 'heap_used_pct' in n]
        if node_heap:
            max_heap = max(node_heap, key=lambda x: x[1])
            comparisons.append({
                'metric': 'Highest Heap Usage',
                'node': max_heap[0],
                'value': f"{max_heap[1]:.1f}%"
            })

    # From gossip - load distribution
    gossip = findings.get('check_gossip_state', {}).get('data', {})
    if gossip.get('max_load_gb', 0) > 0:
        comparisons.append({
            'metric': 'Load Range',
            'node': 'Cluster',
            'value': f"{gossip.get('min_load_gb', 0):.1f} - {gossip.get('max_load_gb', 0):.1f} GB"
        })
        if gossip.get('load_skew', 1.0) > 1.2:
            comparisons.append({
                'metric': 'Load Skew',
                'node': 'Cluster',
                'value': f"{gossip.get('load_skew', 1.0):.2f}x"
            })

    return comparisons


def run_check_executive_summary(connector, settings, all_findings=None):
    """
    Generate executive summary from all check findings.

    Args:
        connector: Database connector
        settings: Configuration settings
        all_findings: Dict of all check findings (passed from report builder)

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    # Section title "Executive Summary" is provided by report definition

    # Try to get findings from connector if not passed directly
    if all_findings is None:
        all_findings = getattr(connector, '_all_findings', {})

    if not all_findings:
        builder.note("Executive summary requires findings from other checks.")
        return builder.build(), {'status': 'no_data'}

    # Calculate health score
    score, deductions = calculate_health_score(all_findings)
    grade, grade_desc = get_health_grade(score)

    # Health Score Section
    builder.h3("Cluster Health Score")

    if score >= 80:
        builder.success(f"**{grade} ({score}/100)** - {grade_desc}")
    elif score >= 60:
        builder.warning(f"**{grade} ({score}/100)** - {grade_desc}")
    else:
        builder.critical(f"**{grade} ({score}/100)** - {grade_desc}")
    builder.blank()

    # Quick Status
    overview = all_findings.get('check_cluster_overview', {}).get('data', {})
    gossip = all_findings.get('check_gossip_state', {}).get('data', {})

    builder.h3("Quick Status")
    status_items = [
        f"**Nodes:** {overview.get('nodes_up', 0)}/{overview.get('total_nodes', 0)} UP",
        f"**Datacenters:** {gossip.get('datacenter_count', 0)}",
        f"**Schema:** {'✅ Consistent' if not gossip.get('has_schema_mismatch') else '❌ MISMATCH'}",
        f"**Versions:** {'✅ Uniform' if not gossip.get('has_mixed_versions') else '⚠️ Mixed'}",
    ]
    for item in status_items:
        builder.text(item)
    builder.blank()

    # Key Concerns
    concerns = get_key_concerns(all_findings)
    if concerns:
        builder.h3("Key Concerns")

        critical = [c for c in concerns if c[0] == 'CRITICAL']
        high = [c for c in concerns if c[0] == 'HIGH']
        medium = [c for c in concerns if c[0] == 'MEDIUM']

        if critical:
            builder.critical(f"**{len(critical)} Critical Issue(s):**")
            for _, concern, rec in critical:
                builder.text(f"- {concern}")
                builder.text(f"  _→ {rec}_")
            builder.blank()

        if high:
            builder.warning(f"**{len(high)} High Priority Issue(s):**")
            for _, concern, rec in high:
                builder.text(f"- {concern}")
                builder.text(f"  _→ {rec}_")
            builder.blank()

        if medium:
            builder.note(f"**{len(medium)} Medium Priority Issue(s):**")
            for _, concern, rec in medium:
                builder.text(f"- {concern}")
            builder.blank()
    else:
        builder.success("No significant concerns detected.")
        builder.blank()

    # Resource Summary
    resources = get_resource_summary(all_findings)
    if resources['total_nodes'] > 0:
        builder.h3("Resource Summary")
        builder.text(f"**Total Nodes:** {resources['total_nodes']}")
        builder.text(f"**Avg Load per Node:** {resources['avg_load_gb']:.1f} GB")
        if resources['avg_heap_pct'] > 0:
            builder.text(f"**Avg Heap Usage:** {resources['avg_heap_pct']:.1f}% (max: {resources['max_heap_pct']:.1f}%)")
        if resources['max_disk_util'] > 0:
            builder.text(f"**Max Disk Utilization:** {resources['max_disk_util']:.1f}%")
        if resources['total_dropped'] > 0:
            builder.text(f"**Total Dropped Messages:** {resources['total_dropped']:,}")
        builder.blank()

    # Node Comparison
    comparisons = get_node_comparison(all_findings)
    if comparisons:
        builder.h3("Node Highlights")
        comp_table = []
        for comp in comparisons:
            comp_table.append({
                'Metric': comp['metric'],
                'Node': comp['node'],
                'Value': comp['value']
            })
        builder.table(comp_table)
        builder.blank()

    # Structured data
    structured_data = {
        'status': 'success',
        'data': {
            'health_score': score,
            'health_grade': grade,
            'total_concerns': len(concerns),
            'critical_concerns': len([c for c in concerns if c[0] == 'CRITICAL']),
            'high_concerns': len([c for c in concerns if c[0] == 'HIGH']),
            'medium_concerns': len([c for c in concerns if c[0] == 'MEDIUM']),
            'resources': resources,
            'deductions': deductions
        }
    }

    return builder.build(), structured_data

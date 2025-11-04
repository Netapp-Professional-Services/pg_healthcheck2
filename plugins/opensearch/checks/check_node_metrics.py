"""
OpenSearch Node Metrics Health Check

Monitors JVM health, heap usage, GC activity, and system resources across all nodes.
Adaptive check that works in multiple modes:
- AWS OpenSearch: Uses CloudWatch metrics for OS-level data
- Self-hosted with SSH: Uses SSH commands for OS-level data
- REST-only: Uses OpenSearch node_stats API only

Requirements:
- REST API access (always required)
- AWS credentials (optional, for AWS OpenSearch Service)
- SSH access (optional, for self-hosted clusters)
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder, require_ssh, require_aws

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 10  # High priority - core health monitoring


def run_check_node_metrics(connector, settings):
    """
    Monitor node-level health metrics including JVM, heap, GC, and system resources.

    Args:
        connector: OpenSearch connector instance
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Add check header
    builder.h3("Node Health Metrics")
    builder.para(
        "Comprehensive health monitoring of all OpenSearch nodes including JVM performance, "
        "heap usage, garbage collection activity, and system resource utilization."
    )

    try:
        # 1. Get node statistics via REST API (works for all modes)
        node_stats = connector.execute_query({
            "operation": "node_stats",
            "metrics": ["jvm", "process", "os", "fs", "thread_pool", "breaker"]
        })

        if "error" in node_stats:
            builder.error(f"Could not retrieve node statistics: {node_stats['error']}")
            structured_data["node_metrics"] = {"status": "error", "details": node_stats['error']}
            return builder.build(), structured_data

        # 2. Detect mode and get additional metrics if available
        mode = _detect_monitoring_mode(connector)
        builder.para(f"**Monitoring Mode:** {mode}")
        builder.blank()

        # 3. Process node statistics
        all_nodes_data = []
        critical_issues = []
        warnings = []

        for node_id, node_data in node_stats.get('nodes', {}).items():
            node_info = _extract_node_metrics(node_id, node_data)

            # Add mode-specific metrics
            if mode == "AWS CloudWatch":
                _add_cloudwatch_metrics(connector, node_info)
            elif mode == "SSH":
                _add_ssh_metrics(connector, node_info, node_data.get('name'))

            all_nodes_data.append(node_info)

            # Check for critical issues
            issues = _check_node_health(node_info, settings)
            if issues['critical']:
                critical_issues.extend(issues['critical'])
            if issues['warnings']:
                warnings.extend(issues['warnings'])

        # 4. Display results
        if critical_issues:
            builder.h4("🔴 Critical Issues Detected")
            for issue in critical_issues:
                builder.critical_issue(issue['title'], issue['details'])

        if warnings:
            builder.h4("⚠️ Warnings")
            for warning in warnings:
                builder.warning_issue(warning['title'], warning['details'])

        # 5. Summary table of all nodes
        builder.h4("Node Health Summary")
        _build_node_summary_table(builder, all_nodes_data)

        # 6. JVM Heap Details
        builder.h4("JVM Heap Usage Details")
        _build_heap_details_table(builder, all_nodes_data)

        # 7. Garbage Collection Statistics
        builder.h4("Garbage Collection Statistics")
        _build_gc_stats_table(builder, all_nodes_data)

        # 8. Thread Pool Status
        builder.h4("Thread Pool Status")
        _build_thread_pool_table(builder, all_nodes_data, node_stats.get('nodes', {}))

        # 9. Circuit Breaker Status
        builder.h4("Circuit Breaker Status")
        _build_circuit_breaker_table(builder, all_nodes_data, node_stats.get('nodes', {}))

        # 10. Recommendations if issues found
        if critical_issues or warnings:
            recommendations = _generate_recommendations(critical_issues, warnings, mode)
            builder.recs(recommendations)
        else:
            builder.success("✅ All nodes are healthy. No issues detected.")

        structured_data["node_metrics"] = {
            "status": "success",
            "mode": mode,
            "nodes": all_nodes_data,
            "critical_issues": len(critical_issues),
            "warnings": len(warnings)
        }

    except Exception as e:
        logger.error(f"Node metrics check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["node_metrics"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _detect_monitoring_mode(connector):
    """Detect which monitoring mode is available."""
    if connector.environment == 'aws' and connector.has_aws_support():
        return "AWS CloudWatch"
    elif connector.environment == 'self_hosted' and connector.has_ssh_support():
        return "SSH"
    else:
        return "REST API Only"


def _extract_node_metrics(node_id, node_data):
    """Extract key metrics from node stats."""
    jvm = node_data.get('jvm', {})
    mem = jvm.get('mem', {})
    gc = jvm.get('gc', {}).get('collectors', {})
    os_data = node_data.get('os', {})
    process = node_data.get('process', {})
    fs = node_data.get('fs', {})

    # Calculate heap usage percentage
    heap_used = mem.get('heap_used_in_bytes', 0)
    heap_max = mem.get('heap_max_in_bytes', 1)
    heap_percent = (heap_used / heap_max * 100) if heap_max > 0 else 0

    # Get old generation stats
    old_gen = gc.get('old', {}) or gc.get('ConcurrentMarkSweep', {}) or {}
    young_gen = gc.get('young', {}) or gc.get('ParNew', {}) or {}

    return {
        'node_id': node_id,
        'name': node_data.get('name', node_id),
        'heap_used_bytes': heap_used,
        'heap_max_bytes': heap_max,
        'heap_percent': round(heap_percent, 1),
        'heap_used_gb': round(heap_used / (1024**3), 2),
        'heap_max_gb': round(heap_max / (1024**3), 2),
        'old_gen_collections': old_gen.get('collection_count', 0),
        'old_gen_time_ms': old_gen.get('collection_time_in_millis', 0),
        'young_gen_collections': young_gen.get('collection_count', 0),
        'young_gen_time_ms': young_gen.get('collection_time_in_millis', 0),
        'cpu_percent': os_data.get('cpu', {}).get('percent', 0),
        'load_average_1m': os_data.get('cpu', {}).get('load_average', {}).get('1m', 0),
        'open_file_descriptors': process.get('open_file_descriptors', 0),
        'max_file_descriptors': process.get('max_file_descriptors', 0),
        'total_disk_bytes': fs.get('total', {}).get('total_in_bytes', 0),
        'available_disk_bytes': fs.get('total', {}).get('available_in_bytes', 0),
    }


def _add_cloudwatch_metrics(connector, node_info):
    """Add CloudWatch metrics to node info (AWS mode)."""
    try:
        # Fetch CloudWatch metrics
        metrics = connector.get_cloudwatch_metrics(
            metric_names=[
                'CPUUtilization',
                'JVMMemoryPressure',
                'MasterCPUUtilization',
                'SearchableDocuments',
                'ClusterStatus.red',
                'ClusterStatus.yellow'
            ],
            period=300,
            hours_back=1
        )
        node_info['cloudwatch_metrics'] = metrics
        node_info['has_cloudwatch'] = True
    except Exception as e:
        logger.warning(f"Could not fetch CloudWatch metrics: {e}")
        node_info['has_cloudwatch'] = False


def _add_ssh_metrics(connector, node_info, node_name):
    """Add SSH-based OS metrics to node info (self-hosted mode)."""
    try:
        # Execute OS-level commands via SSH
        ssh_host = node_info.get('name')  # Try to match by node name
        if ssh_host in connector.ssh_managers:
            # Get memory info
            mem_result = connector.execute_ssh_command(ssh_host, "free -m | grep Mem", "Memory check")
            if mem_result.get('status') == 'success':
                node_info['ssh_memory'] = mem_result.get('output', '')

            # Get disk info
            disk_result = connector.execute_ssh_command(ssh_host, "df -h /var/lib/opensearch 2>/dev/null || df -h /", "Disk check")
            if disk_result.get('status') == 'success':
                node_info['ssh_disk'] = disk_result.get('output', '')

            node_info['has_ssh'] = True
    except Exception as e:
        logger.warning(f"Could not fetch SSH metrics for {node_name}: {e}")
        node_info['has_ssh'] = False


def _check_node_health(node_info, settings):
    """Check node health and return issues."""
    critical = []
    warnings = []

    # Heap thresholds
    heap_warning = settings.get('heap_warning_percent', 75)
    heap_critical = settings.get('heap_critical_percent', 85)

    if node_info['heap_percent'] >= heap_critical:
        critical.append({
            'title': f"Critical Heap Usage on {node_info['name']}",
            'details': {
                'Heap Usage': f"{node_info['heap_percent']}% ({node_info['heap_used_gb']}GB / {node_info['heap_max_gb']}GB)",
                'Threshold': f"{heap_critical}%",
                'Status': "🔴 CRITICAL"
            }
        })
    elif node_info['heap_percent'] >= heap_warning:
        warnings.append({
            'title': f"High Heap Usage on {node_info['name']}",
            'details': {
                'Heap Usage': f"{node_info['heap_percent']}% ({node_info['heap_used_gb']}GB / {node_info['heap_max_gb']}GB)",
                'Threshold': f"{heap_warning}%",
                'Status': "⚠️ WARNING"
            }
        })

    # File descriptor check
    max_fd = node_info.get('max_file_descriptors', 0)
    open_fd = node_info.get('open_file_descriptors', 0)
    if max_fd > 0:
        fd_percent = (open_fd / max_fd) * 100
        if fd_percent >= 90:
            critical.append({
                'title': f"File Descriptor Exhaustion on {node_info['name']}",
                'details': {
                    'Open': open_fd,
                    'Max': max_fd,
                    'Usage': f"{fd_percent:.1f}%",
                    'Status': "🔴 CRITICAL"
                }
            })
        elif fd_percent >= 75:
            warnings.append({
                'title': f"High File Descriptor Usage on {node_info['name']}",
                'details': {
                    'Open': open_fd,
                    'Max': max_fd,
                    'Usage': f"{fd_percent:.1f}%",
                    'Status': "⚠️ WARNING"
                }
            })

    # Disk space check
    total_disk = node_info.get('total_disk_bytes', 0)
    available_disk = node_info.get('available_disk_bytes', 0)
    if total_disk > 0:
        disk_used_percent = ((total_disk - available_disk) / total_disk) * 100
        if disk_used_percent >= 90:
            critical.append({
                'title': f"Critical Disk Usage on {node_info['name']}",
                'details': {
                    'Usage': f"{disk_used_percent:.1f}%",
                    'Available': f"{available_disk / (1024**3):.2f} GB",
                    'Status': "🔴 CRITICAL - Near disk watermark threshold"
                }
            })
        elif disk_used_percent >= 80:
            warnings.append({
                'title': f"High Disk Usage on {node_info['name']}",
                'details': {
                    'Usage': f"{disk_used_percent:.1f}%",
                    'Available': f"{available_disk / (1024**3):.2f} GB",
                    'Status': "⚠️ WARNING"
                }
            })

    return {'critical': critical, 'warnings': warnings}


def _build_node_summary_table(builder, all_nodes_data):
    """Build summary table of all nodes."""
    table_data = []
    for node in all_nodes_data:
        # Determine status icon
        status_icon = "✅"
        if node['heap_percent'] >= 85:
            status_icon = "🔴"
        elif node['heap_percent'] >= 75:
            status_icon = "⚠️"

        table_data.append({
            "Status": status_icon,
            "Node": node['name'],
            "Heap Usage": f"{node['heap_percent']}%",
            "Heap (GB)": f"{node['heap_used_gb']} / {node['heap_max_gb']}",
            "CPU %": f"{node.get('cpu_percent', 'N/A')}",
            "Load (1m)": f"{node.get('load_average_1m', 'N/A')}"
        })

    if table_data:
        builder.table(table_data)


def _build_heap_details_table(builder, all_nodes_data):
    """Build detailed heap usage table."""
    table_data = []
    for node in all_nodes_data:
        table_data.append({
            "Node": node['name'],
            "Used (GB)": node['heap_used_gb'],
            "Max (GB)": node['heap_max_gb'],
            "Usage %": f"{node['heap_percent']}%"
        })

    if table_data:
        builder.table(table_data)


def _build_gc_stats_table(builder, all_nodes_data):
    """Build GC statistics table."""
    table_data = []
    for node in all_nodes_data:
        old_gen_avg_ms = (node['old_gen_time_ms'] / node['old_gen_collections']) if node['old_gen_collections'] > 0 else 0
        young_gen_avg_ms = (node['young_gen_time_ms'] / node['young_gen_collections']) if node['young_gen_collections'] > 0 else 0

        table_data.append({
            "Node": node['name'],
            "Old Gen Collections": node['old_gen_collections'],
            "Old Gen Time (s)": f"{node['old_gen_time_ms'] / 1000:.2f}",
            "Old Gen Avg (ms)": f"{old_gen_avg_ms:.1f}",
            "Young Gen Collections": node['young_gen_collections'],
            "Young Gen Time (s)": f"{node['young_gen_time_ms'] / 1000:.2f}"
        })

    if table_data:
        builder.table(table_data)


def _build_thread_pool_table(builder, all_nodes_data, node_stats_raw):
    """Build thread pool statistics table."""
    table_data = []

    for node_id, node_data in node_stats_raw.items():
        thread_pools = node_data.get('thread_pool', {})
        node_name = node_data.get('name', node_id)

        # Focus on key thread pools
        for pool_name in ['search', 'write', 'get', 'bulk']:
            pool = thread_pools.get(pool_name, {})
            if pool:
                rejected = pool.get('rejected', 0)
                queue = pool.get('queue', 0)
                active = pool.get('active', 0)

                status = "✅"
                if rejected > 0:
                    status = "🔴"
                elif queue > 0:
                    status = "⚠️"

                table_data.append({
                    "Node": node_name,
                    "Pool": pool_name,
                    "Status": status,
                    "Active": active,
                    "Queue": queue,
                    "Rejected": rejected
                })

    if table_data:
        builder.table(table_data)
    else:
        builder.para("No thread pool data available.")


def _build_circuit_breaker_table(builder, all_nodes_data, node_stats_raw):
    """Build circuit breaker statistics table."""
    table_data = []

    for node_id, node_data in node_stats_raw.items():
        breakers = node_data.get('breakers', {})
        node_name = node_data.get('name', node_id)

        for breaker_name, breaker_data in breakers.items():
            limit_bytes = breaker_data.get('limit_size_in_bytes', 0)
            estimated_bytes = breaker_data.get('estimated_size_in_bytes', 0)
            tripped = breaker_data.get('tripped', 0)

            if limit_bytes > 0:
                usage_percent = (estimated_bytes / limit_bytes) * 100

                status = "✅"
                if tripped > 0:
                    status = "🔴"
                elif usage_percent >= 80:
                    status = "⚠️"

                table_data.append({
                    "Node": node_name,
                    "Breaker": breaker_name,
                    "Status": status,
                    "Usage %": f"{usage_percent:.1f}",
                    "Tripped": tripped
                })

    if table_data:
        builder.table(table_data)
    else:
        builder.para("No circuit breaker data available.")


def _generate_recommendations(critical_issues, warnings, mode):
    """Generate recommendations based on detected issues."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    # Analyze issues and generate specific recommendations
    has_heap_issues = any('Heap' in issue['title'] for issue in critical_issues + warnings)
    has_fd_issues = any('File Descriptor' in issue['title'] for issue in critical_issues + warnings)
    has_disk_issues = any('Disk' in issue['title'] for issue in critical_issues + warnings)

    if has_heap_issues:
        recs["critical"].append(
            "Increase JVM heap size or scale cluster by adding nodes to distribute load"
        )
        recs["high"].append(
            "Review query patterns and optimize heavy aggregations or large result sets"
        )
        recs["high"].append(
            "Enable slow query logging to identify memory-intensive queries"
        )

    if has_fd_issues:
        recs["critical"].append(
            "Increase system file descriptor limits (ulimit -n) for OpenSearch process"
        )
        recs["high"].append(
            "Review number of shards per node - too many shards increases file descriptor usage"
        )

    if has_disk_issues:
        recs["critical"].append(
            "Free up disk space immediately or add storage capacity"
        )
        recs["critical"].append(
            "Delete old indices or move them to warm/cold storage if using ILM"
        )
        recs["high"].append(
            "Review disk watermark settings (cluster.routing.allocation.disk.watermark.*)"
        )

    # Mode-specific recommendations
    if mode == "REST API Only":
        recs["general"].append(
            "Consider configuring SSH access or AWS CloudWatch for enhanced OS-level monitoring"
        )

    # General best practices
    recs["general"].append(
        "Monitor GC activity - frequent old generation collections indicate memory pressure"
    )
    recs["general"].append(
        "Watch for thread pool rejections - indicates cluster is overloaded"
    )
    recs["general"].append(
        "Set up alerting on heap usage (>75%), disk usage (>80%), and circuit breaker trips"
    )

    return recs

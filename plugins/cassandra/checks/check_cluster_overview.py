"""
Cluster overview check for Cassandra.

Displays comprehensive cluster status from nodetool status, info, and gossipinfo.
Designed to work with both live SSH and Instacollector imported data.

Data Sources:
- nodetool status: Cluster topology, node states, load
- nodetool info: JVM heap, datacenter, rack, caches
- nodetool gossipinfo: Gossip state, schema versions
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 10


def parse_nodetool_status(status_output):
    """
    Parse nodetool status output into structured data.

    Args:
        status_output: Raw nodetool status output

    Returns:
        dict: Parsed cluster status data
    """
    nodes = []
    datacenters = {}
    current_dc = None

    for line in status_output.strip().split('\n'):
        line = line.strip()

        # Datacenter header
        if line.startswith('Datacenter:'):
            current_dc = line.split(':')[1].strip()
            datacenters[current_dc] = {'nodes': [], 'total_load': 0}
            continue

        # Node line (starts with status like UN, DN, etc.)
        if len(line) >= 2 and line[0] in 'UD' and line[1] in 'NLJM':
            parts = line.split()
            if len(parts) >= 6:
                status = parts[0]
                address = parts[1]
                load = parts[2]
                load_unit = parts[3] if len(parts) > 3 and parts[3] in ('KiB', 'MiB', 'GiB', 'TiB', 'bytes', 'KB', 'MB', 'GB', 'TB') else ''

                # Find tokens count
                tokens = None
                for i, p in enumerate(parts):
                    if p.isdigit() and i > 3:
                        tokens = int(p)
                        break

                # Find rack (usually last element)
                rack = parts[-1] if parts[-1] not in ('?', 'KiB', 'MiB', 'GiB', 'TiB') else 'unknown'

                node_data = {
                    'status': status,
                    'address': address,
                    'load': f"{load} {load_unit}".strip(),
                    'tokens': tokens,
                    'rack': rack,
                    'datacenter': current_dc,
                    'is_up': status[0] == 'U',
                    'state': 'Normal' if status[1] == 'N' else ('Leaving' if status[1] == 'L' else ('Joining' if status[1] == 'J' else 'Moving'))
                }

                nodes.append(node_data)
                if current_dc and current_dc in datacenters:
                    datacenters[current_dc]['nodes'].append(node_data)

    return {
        'nodes': nodes,
        'datacenters': datacenters,
        'total_nodes': len(nodes),
        'nodes_up': len([n for n in nodes if n['is_up']]),
        'nodes_down': len([n for n in nodes if not n['is_up']])
    }


def parse_nodetool_info(info_output):
    """
    Parse nodetool info output into structured data.

    Args:
        info_output: Raw nodetool info output

    Returns:
        dict: Parsed node info
    """
    info = {}

    for line in info_output.strip().split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            # Parse specific fields
            if key == 'Heap Memory (MB)':
                # Format: "2572.52 / 7792.00"
                parts = value.split('/')
                if len(parts) == 2:
                    info['heap_used_mb'] = float(parts[0].strip())
                    info['heap_max_mb'] = float(parts[1].strip())
                    info['heap_percent'] = (info['heap_used_mb'] / info['heap_max_mb']) * 100 if info['heap_max_mb'] > 0 else 0
            elif key == 'Off Heap Memory (MB)':
                info['off_heap_mb'] = float(value)
            elif key == 'Data Center':
                info['datacenter'] = value
            elif key == 'Rack':
                info['rack'] = value
            elif key == 'Load':
                info['load'] = value
            elif key == 'Uptime (seconds)':
                info['uptime_seconds'] = int(value)
                # Convert to human-readable
                days = info['uptime_seconds'] // 86400
                hours = (info['uptime_seconds'] % 86400) // 3600
                info['uptime_human'] = f"{days}d {hours}h" if days > 0 else f"{hours}h"
            elif key == 'Gossip active':
                info['gossip_active'] = value.lower() == 'true'
            elif key == 'Native Transport active':
                info['native_transport_active'] = value.lower() == 'true'
            elif key == 'Key Cache':
                # Parse cache stats
                hit_rate_match = re.search(r'(\d+\.\d+)\s+recent hit rate', value)
                if hit_rate_match:
                    info['key_cache_hit_rate'] = float(hit_rate_match.group(1))
            elif key == 'Percent Repaired':
                info['percent_repaired'] = float(value.rstrip('%'))
            elif key == 'Exceptions':
                info['exceptions'] = int(value)

    return info


def run_check_cluster_overview(connector, settings):
    """
    Display comprehensive cluster overview from nodetool data.

    Works with both live SSH and Instacollector imported data.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Cluster Overview")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Cluster overview")
    if not available:
        return skip_msg, skip_data

    try:
        # Get nodetool status from all hosts
        status_results = connector.execute_ssh_on_all_hosts(
            "nodetool status",
            "Get cluster status"
        )

        # Get nodetool info from all hosts
        info_results = connector.execute_ssh_on_all_hosts(
            "nodetool info",
            "Get node info"
        )

        # Parse the first successful status output (cluster-wide view)
        cluster_status = None
        for result in status_results:
            if result.get('success') and result.get('output'):
                cluster_status = parse_nodetool_status(result['output'])
                break

        # Parse info from each node
        node_infos = {}
        for result in info_results:
            if result.get('success') and result.get('output'):
                node_ip = result['host']
                node_infos[node_ip] = parse_nodetool_info(result['output'])

        # Build output
        if cluster_status:
            total = cluster_status['total_nodes']
            up = cluster_status['nodes_up']
            down = cluster_status['nodes_down']

            # Cluster health status
            if down > 0:
                builder.warning(f"**Cluster Status:** {up}/{total} nodes UP, {down} DOWN")
            else:
                builder.success(f"**Cluster Status:** All {total} nodes UP and healthy")
            builder.blank()

            # Datacenter summary
            builder.h4("Datacenters")
            builder.blank()

            for dc_name, dc_data in cluster_status['datacenters'].items():
                dc_nodes = dc_data['nodes']
                dc_up = len([n for n in dc_nodes if n['is_up']])
                dc_total = len(dc_nodes)
                builder.text(f"**{dc_name}:** {dc_up}/{dc_total} nodes")
            builder.blank()

            # Node status table
            builder.h4("Node Status")
            builder.blank()

            table_data = []
            for node in cluster_status['nodes']:
                status_icon = "✅" if node['is_up'] else "❌"
                table_data.append({
                    'Status': f"{status_icon} {node['status']}",
                    'Address': node['address'],
                    'Datacenter': node['datacenter'] or 'N/A',
                    'Rack': node['rack'],
                    'Load': node['load'],
                    'State': node['state']
                })

            if table_data:
                builder.table(table_data)
            builder.blank()

        # JVM and resource info per node
        if node_infos:
            builder.h4("Node Resources")
            builder.blank()

            resource_data = []
            heap_warnings = []

            for node_ip, info in node_infos.items():
                heap_used = info.get('heap_used_mb', 0)
                heap_max = info.get('heap_max_mb', 0)
                heap_pct = info.get('heap_percent', 0)

                # Check for heap warnings
                if heap_pct >= 85:
                    heap_warnings.append(f"{node_ip}: {heap_pct:.1f}%")

                heap_icon = "🔴" if heap_pct >= 85 else ("⚠️" if heap_pct >= 70 else "")

                resource_data.append({
                    'Node': node_ip,
                    'Heap': f"{heap_icon} {heap_used:.0f}/{heap_max:.0f} MB ({heap_pct:.1f}%)",
                    'Off-Heap': f"{info.get('off_heap_mb', 0):.0f} MB",
                    'Load': info.get('load', 'N/A'),
                    'Uptime': info.get('uptime_human', 'N/A'),
                    'Repaired': f"{info.get('percent_repaired', 0):.1f}%"
                })

            if resource_data:
                builder.table(resource_data)
            builder.blank()

            # Show heap warnings
            if heap_warnings:
                builder.warning("**High Heap Usage Detected:**")
                for warning in heap_warnings:
                    builder.text(f"  - {warning}")
                builder.blank()
                builder.text("*Recommendation:* Consider increasing heap size or investigating memory usage.")
                builder.blank()

            # Check for low repair percentages
            repair_warnings = []
            for node_ip, info in node_infos.items():
                repair_pct = info.get('percent_repaired', 0)
                if repair_pct < 50:
                    repair_warnings.append((node_ip, repair_pct))

            if repair_warnings:
                builder.h4("Repair Status Warning")
                builder.blank()
                builder.warning(f"**Low Repair Percentage Detected on {len(repair_warnings)} node(s)**")
                builder.blank()

                for node_ip, pct in repair_warnings:
                    builder.text(f"  - {node_ip}: {pct:.1f}% repaired")
                builder.blank()

                builder.text("*Why This Matters:*")
                builder.blank()
                builder.text("Low repair percentages indicate data may be inconsistent across replicas.")
                builder.text("Unrepaired data is more vulnerable to data loss if multiple nodes fail.")
                builder.blank()

                builder.text("*Recommended Actions:*")
                builder.blank()
                builder.text("1. **Run incremental repair:** `nodetool repair -pr <keyspace>`")
                builder.text("2. **Schedule regular repairs:** Run repair on each node within `gc_grace_seconds` (default 10 days)")
                builder.text("3. **Consider using Reaper:** https://cassandra-reaper.io/ for automated repair scheduling")
                builder.text("4. **Monitor repair progress:** `nodetool netstats` during repair")
                builder.blank()

        # Cache statistics (from first node with info)
        first_info = next(iter(node_infos.values()), {}) if node_infos else {}
        if first_info.get('key_cache_hit_rate') is not None:
            builder.h4("Cache Statistics (Sample Node)")
            builder.blank()
            hit_rate = first_info.get('key_cache_hit_rate', 0)
            if hit_rate < 0.5:
                builder.warning(f"Key Cache Hit Rate: {hit_rate:.3f} (Low - consider increasing key_cache_size)")
            else:
                builder.text(f"Key Cache Hit Rate: {hit_rate:.3f}")
            builder.blank()

        # Build structured data
        structured_data = {
            'status': 'success',
            'data': {
                'total_nodes': cluster_status['total_nodes'] if cluster_status else 0,
                'nodes_up': cluster_status['nodes_up'] if cluster_status else 0,
                'nodes_down': cluster_status['nodes_down'] if cluster_status else 0,
                'datacenters': list(cluster_status['datacenters'].keys()) if cluster_status else [],
                'datacenter_count': len(cluster_status['datacenters']) if cluster_status else 0,
                'nodes': cluster_status['nodes'] if cluster_status else [],
                'node_resources': node_infos,
                'heap_usage_critical': len(heap_warnings) if heap_warnings else 0,
                'cluster_healthy': cluster_status['nodes_down'] == 0 if cluster_status else False
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Cluster overview check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Cluster overview check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

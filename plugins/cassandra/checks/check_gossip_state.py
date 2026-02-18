"""
Gossip state analysis check for Cassandra.

Analyzes nodetool gossipinfo output to detect:
- Node state consistency
- Schema version agreement
- Generation/heartbeat drift
- Datacenter and rack configuration
- Load imbalance indicators

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def parse_gossipinfo(content):
    """
    Parse nodetool gossipinfo output.

    Example format:
        /65.28.129.52
          generation:1745993458
          heartbeat:20154980
          STATUS:16:NORMAL,-1024096238832995618
          LOAD:20154946:4.9934211999E10
          SCHEMA:12:fcf45848-3e0c-382a-b320-997feaf0f783
          DC:8:dc2
          RACK:10:rack1
          RELEASE_VERSION:4:3.11.2
          ...

    Args:
        content: Raw gossipinfo output

    Returns:
        dict: Parsed gossip state
    """
    result = {
        'nodes': {},
        'schema_versions': defaultdict(list),
        'datacenters': defaultdict(list),
        'versions': defaultdict(list),
        'node_states': defaultdict(list)
    }

    if not content:
        return result

    lines = content.strip().split('\n')
    current_node = None
    current_data = {}

    for line in lines:
        # Node header (IP address)
        node_match = re.match(r'^[/]?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', line.strip())
        if node_match:
            if current_node and current_data:
                result['nodes'][current_node] = current_data
            current_node = node_match.group(1)
            current_data = {}
            continue

        # Also handle hostname/IP format
        host_match = re.match(r'^[\w-]+[/](\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', line.strip())
        if host_match:
            if current_node and current_data:
                result['nodes'][current_node] = current_data
            current_node = host_match.group(1)
            current_data = {}
            continue

        # Parse key:version:value format
        if current_node and ':' in line:
            line = line.strip()

            # Parse generation and heartbeat
            if line.startswith('generation:'):
                current_data['generation'] = int(line.split(':')[1])
            elif line.startswith('heartbeat:'):
                current_data['heartbeat'] = int(line.split(':')[1])

            # Parse STATUS:version:value
            elif line.startswith('STATUS:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    status_value = parts[2].split(',')[0]  # NORMAL, LEAVING, etc
                    current_data['status'] = status_value

            # Parse LOAD
            elif line.startswith('LOAD:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    try:
                        load_bytes = float(parts[2])
                        current_data['load_bytes'] = load_bytes
                        current_data['load_gb'] = load_bytes / (1024 * 1024 * 1024)
                    except ValueError:
                        pass

            # Parse SCHEMA
            elif line.startswith('SCHEMA:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    current_data['schema'] = parts[2]

            # Parse DC
            elif line.startswith('DC:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    current_data['datacenter'] = parts[2]

            # Parse RACK
            elif line.startswith('RACK:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    current_data['rack'] = parts[2]

            # Parse RELEASE_VERSION
            elif line.startswith('RELEASE_VERSION:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    current_data['release_version'] = parts[2]

            # Parse RPC_READY
            elif line.startswith('RPC_READY:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    current_data['rpc_ready'] = parts[2].lower() == 'true'

            # Parse HOST_ID
            elif line.startswith('HOST_ID:'):
                parts = line.split(':')
                if len(parts) >= 3:
                    current_data['host_id'] = parts[2]

    # Add last node
    if current_node and current_data:
        result['nodes'][current_node] = current_data

    # Build aggregations
    for node_ip, data in result['nodes'].items():
        if 'schema' in data:
            result['schema_versions'][data['schema']].append(node_ip)
        if 'datacenter' in data:
            result['datacenters'][data['datacenter']].append(node_ip)
        if 'release_version' in data:
            result['versions'][data['release_version']].append(node_ip)
        if 'status' in data:
            result['node_states'][data['status']].append(node_ip)

    # Convert defaultdicts
    result['schema_versions'] = dict(result['schema_versions'])
    result['datacenters'] = dict(result['datacenters'])
    result['versions'] = dict(result['versions'])
    result['node_states'] = dict(result['node_states'])

    return result


def run_check_gossip_state(connector, settings):
    """
    Analyze gossip state across cluster.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Gossip State Analysis")
    builder.para("Analyzing cluster gossip state from `nodetool gossipinfo`.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Gossip state analysis")
    if not available:
        return skip_msg, skip_data

    try:
        # Get gossipinfo from one node (cluster-wide view)
        results = connector.execute_ssh_on_all_hosts(
            "nodetool gossipinfo",
            "Get gossip info"
        )

        # Use first successful result
        gossip_output = None
        for result in results:
            if result.get('success') and result.get('output'):
                gossip_output = result['output']
                break

        if not gossip_output:
            builder.note("Gossip info not available.")
            return builder.build(), {'status': 'no_data'}

        # Parse gossip state
        gossip_data = parse_gossipinfo(gossip_output)

        if not gossip_data['nodes']:
            builder.note("Could not parse gossip info.")
            return builder.build(), {'status': 'no_data'}

        num_nodes = len(gossip_data['nodes'])
        builder.text(f"**Cluster nodes:** {num_nodes}")
        builder.blank()

        # Schema version analysis
        num_schemas = len(gossip_data['schema_versions'])
        if num_schemas > 1:
            builder.critical(f"**Schema version mismatch!** {num_schemas} different schema versions detected")
            builder.blank()
            builder.h4("Schema Versions")
            for schema, nodes in gossip_data['schema_versions'].items():
                builder.text(f"- `{schema}`: {len(nodes)} node(s)")
            builder.blank()
        elif num_schemas == 1:
            schema = list(gossip_data['schema_versions'].keys())[0]
            builder.success(f"All nodes agree on schema version: `{schema[:20]}...`")
            builder.blank()

        # Node state analysis
        non_normal = {state: nodes for state, nodes in gossip_data['node_states'].items()
                      if state != 'NORMAL'}

        if non_normal:
            builder.warning(f"**Nodes not in NORMAL state detected:**")
            for state, nodes in non_normal.items():
                builder.text(f"- **{state}**: {', '.join(nodes)}")
            builder.blank()
        else:
            normal_count = len(gossip_data['node_states'].get('NORMAL', []))
            builder.success(f"All {normal_count} nodes in NORMAL state")
            builder.blank()

        # Version analysis
        if len(gossip_data['versions']) > 1:
            builder.warning("**Mixed Cassandra versions detected:**")
            for version, nodes in gossip_data['versions'].items():
                builder.text(f"- **{version}**: {len(nodes)} node(s)")
            builder.blank()
        elif gossip_data['versions']:
            version = list(gossip_data['versions'].keys())[0]
            builder.text(f"**Cassandra Version:** {version}")
            builder.blank()

        # Datacenter breakdown
        if gossip_data['datacenters']:
            builder.h4("Datacenter Distribution")
            dc_table = []
            for dc, nodes in sorted(gossip_data['datacenters'].items()):
                dc_table.append({
                    'Datacenter': dc,
                    'Nodes': len(nodes),
                    'Node IPs': ', '.join(nodes[:5]) + ('...' if len(nodes) > 5 else '')
                })
            builder.table(dc_table)
            builder.blank()

        # Load distribution
        loads = [(ip, data.get('load_gb', 0)) for ip, data in gossip_data['nodes'].items()
                 if 'load_gb' in data]
        if loads:
            loads.sort(key=lambda x: x[1], reverse=True)
            min_load = min(l[1] for l in loads)
            max_load = max(l[1] for l in loads)
            avg_load = sum(l[1] for l in loads) / len(loads)

            if max_load > 0 and min_load > 0:
                skew = max_load / min_load
                builder.h4("Load Distribution")
                builder.text(f"**Min load:** {min_load:.2f} GB")
                builder.text(f"**Max load:** {max_load:.2f} GB")
                builder.text(f"**Avg load:** {avg_load:.2f} GB")
                builder.text(f"**Load skew:** {skew:.2f}x")

                if skew > 2.0:
                    builder.warning("Significant load imbalance detected!")
                builder.blank()

        # Node details table
        builder.h4("Node Gossip Details")
        node_table = []
        for ip, data in sorted(gossip_data['nodes'].items()):
            status_icon = "✅" if data.get('status') == 'NORMAL' else "⚠️"
            rpc_status = "Ready" if data.get('rpc_ready', False) else "Not Ready"

            node_table.append({
                'Node': f"{status_icon} {ip}",
                'Status': data.get('status', 'Unknown'),
                'DC': data.get('datacenter', 'N/A'),
                'Rack': data.get('rack', 'N/A'),
                'Load': f"{data.get('load_gb', 0):.2f} GB",
                'RPC': rpc_status
            })
        builder.table(node_table)
        builder.blank()

        # Recommendations
        recommendations = []
        if num_schemas > 1:
            recommendations.append("Schema mismatch detected - run 'nodetool describecluster' and resolve inconsistency")
        if non_normal:
            recommendations.append("Nodes not in NORMAL state - investigate and resolve before other operations")
        if len(gossip_data['versions']) > 1:
            recommendations.append("Mixed versions detected - complete rolling upgrade to single version")

        if recommendations:
            builder.h4("Recommendations")
            builder.tip("**Gossip state issues:**")
            for rec in recommendations:
                builder.text(f"- {rec}")
            builder.blank()

        # Structured data
        loads_gb = [data.get('load_gb', 0) for data in gossip_data['nodes'].values() if 'load_gb' in data]

        structured_data = {
            'status': 'success',
            'data': {
                'total_nodes': num_nodes,
                'schema_version_count': num_schemas,
                'has_schema_mismatch': num_schemas > 1,
                'datacenter_count': len(gossip_data['datacenters']),
                'version_count': len(gossip_data['versions']),
                'has_mixed_versions': len(gossip_data['versions']) > 1,
                'nodes_normal': len(gossip_data['node_states'].get('NORMAL', [])),
                'nodes_not_normal': sum(len(nodes) for state, nodes in gossip_data['node_states'].items() if state != 'NORMAL'),
                'has_non_normal_nodes': bool(non_normal),
                'min_load_gb': round(min(loads_gb), 2) if loads_gb else 0,
                'max_load_gb': round(max(loads_gb), 2) if loads_gb else 0,
                'load_skew': round(max(loads_gb) / min(loads_gb), 2) if loads_gb and min(loads_gb) > 0 else 1.0,
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Gossip state check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Gossip state check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

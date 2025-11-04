"""
Network Topology Check

Queries system.peers_v2 and system.local to analyze cluster topology.

Provides insights into:
- Datacenter distribution
- Rack distribution within datacenters
- Node counts per DC and rack
- Potential single points of failure

This helps identify:
- Unbalanced cluster topology
- Single-rack deployments (no rack diversity)
- Multi-DC configuration details

CQL-only check - works on managed Instaclustr clusters.
Returns structured data compatible with trend analysis.
"""

import logging
from datetime import datetime
from typing import Dict, List
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)


def check_network_topology(connector, settings):
    """
    Analyze cluster network topology

    Queries system tables to understand:
    - How many datacenters
    - Nodes per datacenter
    - Rack distribution
    - Potential availability concerns

    Args:
        connector: Cassandra connector with active session
        settings: Configuration settings

    Returns:
        Tuple of (adoc_content, structured findings dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Network Topology")

    if not connector or not connector.session:
        builder.error("❌ No active database connection")
        return builder.build(), {
            'network_topology': {
                'status': 'error',
                'error_message': 'No active database connection'
            }
        }

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Get version to determine which peers table to query
        version_info = connector.version_info or {}
        major_version = version_info.get('major_version', 0)

        # Discover topology using connector's method (which handles version)
        nodes = connector._discover_nodes()

        # Query local node info
        local_query = """
        SELECT
            data_center,
            rack,
            broadcast_address,
            listen_address,
            release_version
        FROM system.local
        """
        local_result = list(connector.session.execute(local_query))

        # Query peer nodes (use appropriate table based on version)
        if major_version >= 4:
            peers_query = """
            SELECT
                data_center,
                rack,
                peer,
                release_version
            FROM system.peers_v2
            """
        else:
            peers_query = """
            SELECT
                data_center,
                rack,
                peer,
                release_version
            FROM system.peers
            """

        peers_result = list(connector.session.execute(peers_query))

        # Combine local and peers
        all_nodes = []

        if local_result:
            local = local_result[0]
            all_nodes.append({
                'address': str(local.get('broadcast_address') or local.get('listen_address')),
                'datacenter': local.get('data_center', 'unknown'),
                'rack': local.get('rack', 'unknown'),
                'version': local.get('release_version', 'unknown')
            })

        for peer in peers_result:
            all_nodes.append({
                'address': str(peer.get('peer')),
                'datacenter': peer.get('data_center', 'unknown'),
                'rack': peer.get('rack', 'unknown'),
                'version': peer.get('release_version', 'unknown')
            })

        if not all_nodes:
            builder.warning("⚠️ No node topology information available")
            return builder.build(), {
                'network_topology': {
                    'datacenter_summary': {
                        'status': 'warning',
                        'data': [],
                        'message': 'No node topology information available'
                    }
                }
            }

        # Analyze topology
        findings = _analyze_topology(all_nodes, timestamp)

        # Add summary to builder
        total_datacenters = findings['datacenter_summary']['total_datacenters']
        total_nodes = findings['datacenter_summary']['total_nodes']
        total_racks = findings['rack_distribution']['total_racks']
        warning_count = findings['topology_analysis']['warning_count']

        if warning_count == 0:
            builder.success(f"✅ Topology healthy: {total_nodes} node(s) across {total_datacenters} datacenter(s) and {total_racks} rack(s)")
        else:
            builder.warning(f"⚠️ {warning_count} topology concern(s): {total_nodes} node(s) across {total_datacenters} datacenter(s) and {total_racks} rack(s)")

            # Add explanation
            builder.blank()
            builder.text("*Why This Matters:*")
            builder.text("Cassandra's availability and fault tolerance depend heavily on proper topology configuration. ")
            builder.text("With NetworkTopologyStrategy and RF=3, Cassandra distributes replicas across racks to ensure ")
            builder.text("data remains available during rack or node failures. Single-rack deployments create a single ")
            builder.text("point of failure, while insufficient nodes prevent proper quorum operations.")
            builder.blank()

            # Add datacenter distribution details
            builder.text("*Current Distribution:*")
            for dc_info in findings['datacenter_summary']['data']:
                builder.text(f"- *Datacenter '{dc_info['datacenter']}'*: {dc_info['node_count']} node(s) across {dc_info['rack_count']} rack(s)")
            builder.blank()

            # Show rack breakdown
            builder.text("*Rack Distribution:*")
            for rack_info in findings['rack_distribution']['data']:
                builder.text(f"- {rack_info['datacenter']}:{rack_info['rack']} → {rack_info['node_count']} node(s)")
            builder.blank()

            # Add topology warnings
            topology_warnings = findings['topology_analysis']['warnings']
            if topology_warnings:
                builder.text("*Identified Concerns:*")
                for idx, warning in enumerate(topology_warnings, 1):
                    severity_emoji = "🔴" if warning['severity'] == 'critical' else "⚠️" if warning['severity'] == 'warning' else "ℹ️"
                    builder.text(f"{idx}. {severity_emoji} *{warning['issue']}*")
                    builder.text(f"   - Impact: {warning['impact']}")
                    if 'recommendation' in warning:
                        builder.text(f"   - Action: {warning['recommendation']}")
                builder.blank()

            # Add recommendations
            builder.text("*Recommended Actions:*")

            # Check for specific issues and provide targeted advice
            has_single_rack = any(w['severity'] == 'warning' and 'rack' in w['issue'].lower() for w in topology_warnings)
            has_small_cluster = any('node(s)' in w['issue'] and w['severity'] == 'warning' for w in topology_warnings)
            has_version_mismatch = any('version' in w['issue'].lower() for w in topology_warnings)

            if has_single_rack:
                builder.text("1. *Deploy Across Multiple Racks*: Minimum 3 racks recommended for RF=3")
                builder.text("   - Add nodes to different racks to eliminate single rack SPOF")
                builder.text("   - Ensure rack names are properly configured in cassandra.yaml (GossipingPropertyFileSnitch)")
                builder.text("   - After adding nodes, run `nodetool rebuild` to redistribute data")

            if has_small_cluster:
                builder.text("2. *Scale to Minimum 3 Nodes Per DC*: Required for RF=3 with proper quorum")
                builder.text("   - RF=3 with 2 nodes: quorum reads/writes cannot tolerate any node failure")
                builder.text("   - 3 nodes allows QUORUM (2 nodes) to succeed with 1 node down")
                builder.text("   - Add nodes incrementally and run `nodetool cleanup` on existing nodes")

            if has_version_mismatch:
                builder.text("3. *Standardize Cassandra Versions*: All nodes should run the same version")
                builder.text("   - Plan rolling upgrade to bring all nodes to latest stable version")
                builder.text("   - Version mismatches can cause protocol incompatibilities and data corruption")

            if total_datacenters == 1:
                builder.text("4. *Consider Multi-DC for DR*: Single DC = no geographic redundancy")
                builder.text("   - Multi-DC provides disaster recovery capability")
                builder.text("   - Use NetworkTopologyStrategy: `{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}`")
            builder.blank()

            builder.text("*Best Practices:*")
            builder.text("- *Minimum Configuration*: 3 nodes per DC, distributed across 3+ racks, RF=3")
            builder.text("- *Rack Awareness*: Configure `endpoint_snitch` (GossipingPropertyFileSnitch recommended)")
            builder.text("- *Balanced Distribution*: Equal node counts across racks prevents hotspots")
            builder.text("- *Multi-DC Strategy*: For production, consider 2+ DCs for disaster recovery")

        return builder.build(), {'network_topology': findings}

    except Exception as e:
        logger.error(f"Failed to analyze network topology: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"❌ Failed to analyze network topology: {e}")
        return builder.build(), {
            'network_topology': {
                'status': 'error',
                'error_message': str(e),
                'data': []
            }
        }


def _analyze_topology(nodes: List[Dict], timestamp: str) -> Dict:
    """
    Analyze cluster topology

    Args:
        nodes: List of node information
        timestamp: ISO 8601 timestamp

    Returns:
        Structured findings with topology analysis
    """
    # Track counts
    dc_counts = {}
    rack_counts = {}
    dc_rack_map = {}
    version_map = {}

    for node in nodes:
        dc = node['datacenter']
        rack = node['rack']
        version = node['version']
        address = node['address']

        # Count nodes per DC
        if dc not in dc_counts:
            dc_counts[dc] = {
                'node_count': 0,
                'nodes': [],
                'racks': set()
            }
        dc_counts[dc]['node_count'] += 1
        dc_counts[dc]['nodes'].append(address)
        dc_counts[dc]['racks'].add(rack)

        # Track DC+Rack combination
        dc_rack_key = f"{dc}:{rack}"
        if dc_rack_key not in rack_counts:
            rack_counts[dc_rack_key] = {
                'datacenter': dc,
                'rack': rack,
                'node_count': 0,
                'nodes': []
            }
        rack_counts[dc_rack_key]['node_count'] += 1
        rack_counts[dc_rack_key]['nodes'].append(address)

        # Track versions
        version_map[version] = version_map.get(version, 0) + 1

    # Build datacenter summary
    dc_summary_data = []
    for dc, info in sorted(dc_counts.items()):
        dc_summary_data.append({
            'datacenter': dc,
            'node_count': info['node_count'],
            'rack_count': len(info['racks']),
            'percentage': round((info['node_count'] / len(nodes) * 100), 1),
            'timestamp': timestamp
        })

    # Build rack distribution
    rack_distribution_data = []
    for rack_key, info in sorted(rack_counts.items()):
        rack_distribution_data.append({
            'datacenter': info['datacenter'],
            'rack': info['rack'],
            'node_count': info['node_count'],
            'timestamp': timestamp
        })

    # Build version distribution
    version_data = []
    for version, count in sorted(version_map.items()):
        version_data.append({
            'version': version,
            'node_count': count,
            'percentage': round((count / len(nodes) * 100), 1),
            'timestamp': timestamp
        })

    # Analyze topology health
    topology_warnings = []
    topology_status = 'success'

    # Check for single DC
    if len(dc_counts) == 1:
        topology_warnings.append({
            'severity': 'info',
            'issue': 'Single datacenter deployment',
            'impact': 'No geographic redundancy - consider multi-DC for disaster recovery'
        })

    # Check for single rack in any DC
    for dc, info in dc_counts.items():
        if len(info['racks']) == 1:
            topology_warnings.append({
                'severity': 'warning',
                'issue': f"Datacenter '{dc}' has only 1 rack",
                'impact': 'Single rack = single point of failure - no rack diversity',
                'recommendation': 'Deploy nodes across multiple racks for availability'
            })
            topology_status = 'warning'

    # Check for version mismatches
    if len(version_map) > 1:
        topology_warnings.append({
            'severity': 'warning',
            'issue': f"Multiple Cassandra versions detected: {list(version_map.keys())}",
            'impact': 'Version mismatch can cause compatibility issues',
            'recommendation': 'Upgrade all nodes to same version'
        })
        topology_status = 'warning'

    # Check for small clusters (<3 nodes per DC)
    for dc, info in dc_counts.items():
        if info['node_count'] < 3:
            topology_warnings.append({
                'severity': 'warning',
                'issue': f"Datacenter '{dc}' has only {info['node_count']} node(s)",
                'impact': 'Minimum 3 nodes recommended for fault tolerance with RF=3',
                'recommendation': 'Add more nodes to reach RF=3 for proper quorum'
            })
            topology_status = 'warning'

    # Check for unbalanced DCs
    if len(dc_counts) > 1:
        node_counts = [info['node_count'] for info in dc_counts.values()]
        if max(node_counts) > min(node_counts) * 2:
            topology_warnings.append({
                'severity': 'warning',
                'issue': 'Unbalanced node distribution across datacenters',
                'impact': 'Can lead to hotspots and uneven load',
                'recommendation': 'Balance node count across datacenters'
            })
            topology_status = 'warning'

    return {
        'datacenter_summary': {
            'status': 'success',
            'data': dc_summary_data,
            'total_datacenters': len(dc_counts),
            'total_nodes': len(nodes),
            'metadata': {
                'query_timestamp': timestamp,
                'source': 'system.local + system.peers'
            }
        },
        'rack_distribution': {
            'status': 'success',
            'data': rack_distribution_data,
            'total_racks': len(rack_counts),
            'message': f'{len(rack_counts)} rack(s) across {len(dc_counts)} datacenter(s)'
        },
        'version_distribution': {
            'status': 'success' if len(version_map) == 1 else 'warning',
            'data': version_data,
            'unique_versions': len(version_map),
            'message': 'All nodes same version' if len(version_map) == 1 else f'{len(version_map)} different versions detected'
        },
        'topology_analysis': {
            'status': topology_status,
            'warnings': topology_warnings,
            'warning_count': len(topology_warnings),
            'message': f'{len(topology_warnings)} topology concern(s) identified' if topology_warnings else 'Topology looks healthy'
        }
    }


# Register check metadata
check_metadata = {
    'name': 'network_topology',
    'description': 'Analyze cluster datacenter and rack distribution',
    'category': 'topology',
    'requires_api': False,
    'requires_ssh': False,
    'requires_cql': True
}

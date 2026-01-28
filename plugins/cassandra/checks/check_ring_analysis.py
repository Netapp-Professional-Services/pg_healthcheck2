"""
Ring analysis check for Cassandra.

Analyzes nodetool ring output to detect:
- Token distribution balance across nodes
- Data skew (uneven load distribution)
- Ownership percentages
- Token count per node (vnodes)

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 6


def parse_ring_output(output):
    """
    Parse nodetool ring output.

    Example format:
        Datacenter: dc1
        ==========
        Address       Rack        Status State   Load            Owns                Token
                                                                                     9220511345620901215
        65.28.131.54  rack1       Up     Normal  46.68 GiB       ?                   -9202836984824646798

    Args:
        output: Raw ring output

    Returns:
        dict: Parsed ring data with per-node statistics
    """
    result = {
        'datacenters': {},
        'nodes': defaultdict(lambda: {
            'tokens': [],
            'load': None,
            'owns': None,
            'rack': None,
            'status': None,
            'state': None,
            'datacenter': None
        }),
        'total_tokens': 0
    }

    if not output:
        return result

    lines = output.strip().split('\n')
    current_dc = None

    for line in lines:
        line = line.strip()

        # Detect datacenter
        if line.startswith('Datacenter:'):
            current_dc = line.split(':', 1)[1].strip()
            if current_dc not in result['datacenters']:
                result['datacenters'][current_dc] = {'nodes': set(), 'token_count': 0}
            continue

        # Skip headers and separators
        if not line or line.startswith('=') or line.startswith('Address'):
            continue

        # Parse node lines
        # Format: Address Rack Status State Load Owns Token
        parts = line.split()
        if len(parts) >= 4:
            # Check if first part looks like an IP address
            ip_match = re.match(r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$', parts[0])
            if ip_match:
                address = parts[0]
                rack = parts[1] if len(parts) > 1 else 'unknown'
                status = parts[2] if len(parts) > 2 else 'unknown'
                state = parts[3] if len(parts) > 3 else 'unknown'

                # Load can be like "46.68 GiB" (two parts) or "46.68" (one part)
                load_str = None
                owns_str = None
                token = None

                # Find load (usually contains a number followed by optional unit)
                load_idx = 4
                if len(parts) > load_idx:
                    # Check if next part is a unit
                    if len(parts) > load_idx + 1 and parts[load_idx + 1] in ['GiB', 'MiB', 'KiB', 'bytes', 'GB', 'MB', 'KB']:
                        load_str = f"{parts[load_idx]} {parts[load_idx + 1]}"
                        owns_idx = load_idx + 2
                    else:
                        load_str = parts[load_idx]
                        owns_idx = load_idx + 1

                    # Ownership (may be '?' or a percentage)
                    if len(parts) > owns_idx:
                        owns_str = parts[owns_idx]

                    # Token is typically the last value (large negative or positive number)
                    if len(parts) > owns_idx + 1:
                        token = parts[-1]

                node_data = result['nodes'][address]
                node_data['rack'] = rack
                node_data['status'] = status
                node_data['state'] = state
                node_data['load'] = load_str
                node_data['owns'] = owns_str
                node_data['datacenter'] = current_dc

                if token and (token.lstrip('-').isdigit() or 'E' in token.upper()):
                    node_data['tokens'].append(token)
                    result['total_tokens'] += 1

                if current_dc:
                    result['datacenters'][current_dc]['nodes'].add(address)
                    result['datacenters'][current_dc]['token_count'] += 1

    # Convert sets to lists for JSON serialization
    for dc in result['datacenters']:
        result['datacenters'][dc]['nodes'] = list(result['datacenters'][dc]['nodes'])

    # Convert defaultdict to regular dict
    result['nodes'] = dict(result['nodes'])

    return result


def calculate_token_distribution(ring_data):
    """
    Calculate token distribution statistics.

    Args:
        ring_data: Parsed ring data

    Returns:
        dict: Distribution statistics
    """
    stats = {
        'nodes': [],
        'min_tokens': 0,
        'max_tokens': 0,
        'avg_tokens': 0,
        'skew_ratio': 0.0,
        'imbalanced_nodes': []
    }

    if not ring_data['nodes']:
        return stats

    node_tokens = []
    for address, data in ring_data['nodes'].items():
        token_count = len(data['tokens'])
        node_tokens.append({
            'address': address,
            'tokens': token_count,
            'load': data.get('load'),
            'datacenter': data.get('datacenter'),
            'rack': data.get('rack')
        })

    if not node_tokens:
        return stats

    token_counts = [n['tokens'] for n in node_tokens]
    stats['nodes'] = node_tokens
    stats['min_tokens'] = min(token_counts)
    stats['max_tokens'] = max(token_counts)
    stats['avg_tokens'] = sum(token_counts) / len(token_counts)

    # Calculate skew ratio (max/min, higher = more imbalanced)
    if stats['min_tokens'] > 0:
        stats['skew_ratio'] = stats['max_tokens'] / stats['min_tokens']

    # Find imbalanced nodes (>20% deviation from average)
    for node in node_tokens:
        if stats['avg_tokens'] > 0:
            deviation = abs(node['tokens'] - stats['avg_tokens']) / stats['avg_tokens']
            if deviation > 0.2:
                stats['imbalanced_nodes'].append({
                    'address': node['address'],
                    'tokens': node['tokens'],
                    'deviation_pct': round(deviation * 100, 1)
                })

    return stats


def run_check_ring_analysis(connector, settings):
    """
    Analyze token ring distribution.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Token Ring Analysis")
    builder.para("Analyzing token distribution and data balance from `nodetool ring`.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Ring analysis")
    if not available:
        return skip_msg, skip_data

    try:
        # Get ring output from one node (ring is cluster-wide)
        results = connector.execute_ssh_on_all_hosts(
            "nodetool ring",
            "Get token ring"
        )

        # Use first successful result
        ring_output = None
        for result in results:
            if result.get('success') and result.get('output'):
                ring_output = result['output']
                break

        if not ring_output:
            builder.note("Ring data not available.")
            return builder.build(), {'status': 'no_data'}

        # Parse ring data
        ring_data = parse_ring_output(ring_output)

        if not ring_data['nodes']:
            builder.note("Could not parse ring data.")
            return builder.build(), {'status': 'no_data'}

        # Calculate distribution stats
        dist_stats = calculate_token_distribution(ring_data)

        # Thresholds
        skew_warning = settings.get('ring_skew_warning', 1.3)
        skew_critical = settings.get('ring_skew_critical', 2.0)

        # Summary
        num_nodes = len(ring_data['nodes'])
        num_dcs = len(ring_data['datacenters'])
        total_tokens = ring_data['total_tokens']

        builder.text(f"**Cluster:** {num_nodes} nodes across {num_dcs} datacenter(s)")
        builder.text(f"**Total token entries:** {total_tokens}")
        builder.blank()

        # Token distribution analysis
        if dist_stats['skew_ratio'] >= skew_critical:
            builder.critical(f"**Severe token imbalance detected** - skew ratio: {dist_stats['skew_ratio']:.2f}")
            builder.text("Some nodes have significantly more tokens than others, causing uneven load.")
        elif dist_stats['skew_ratio'] >= skew_warning:
            builder.warning(f"**Token imbalance detected** - skew ratio: {dist_stats['skew_ratio']:.2f}")
        elif dist_stats['skew_ratio'] > 1.0:
            builder.success(f"Token distribution is balanced (skew ratio: {dist_stats['skew_ratio']:.2f})")
        builder.blank()

        # Per-datacenter breakdown
        if ring_data['datacenters']:
            builder.h4("Datacenter Token Distribution")
            dc_table = []
            for dc_name, dc_data in ring_data['datacenters'].items():
                dc_table.append({
                    'Datacenter': dc_name,
                    'Nodes': len(dc_data['nodes']),
                    'Token Entries': dc_data['token_count']
                })
            builder.table(dc_table)
            builder.blank()

        # Per-node token counts
        if dist_stats['nodes']:
            builder.h4("Tokens per Node")
            token_table = []
            for node in sorted(dist_stats['nodes'], key=lambda x: x['tokens'], reverse=True):
                deviation = ""
                if dist_stats['avg_tokens'] > 0:
                    dev_pct = ((node['tokens'] - dist_stats['avg_tokens']) / dist_stats['avg_tokens']) * 100
                    if abs(dev_pct) > 10:
                        deviation = f" ({'+' if dev_pct > 0 else ''}{dev_pct:.0f}%)"

                token_table.append({
                    'Node': node['address'],
                    'Datacenter': node.get('datacenter', 'N/A'),
                    'Tokens': f"{node['tokens']}{deviation}",
                    'Load': node.get('load', 'N/A')
                })
            builder.table(token_table)
            builder.blank()

        # Imbalanced nodes
        if dist_stats['imbalanced_nodes']:
            builder.h4("Imbalanced Nodes")
            builder.warning(f"**{len(dist_stats['imbalanced_nodes'])} node(s)** with significant token imbalance:")
            builder.blank()
            for node in dist_stats['imbalanced_nodes']:
                builder.text(f"- **{node['address']}**: {node['tokens']} tokens ({node['deviation_pct']}% deviation)")
            builder.blank()

        # Recommendations
        if dist_stats['skew_ratio'] >= skew_warning:
            builder.h4("Recommendations")
            builder.tip("**To improve token balance:**")
            builder.text("- **Check num_tokens setting**: All nodes should have the same num_tokens value")
            builder.text("- **Consider rebalancing**: Use `nodetool cleanup` after adding nodes")
            builder.text("- **Review node additions**: New nodes may need time to stream data")
            builder.text("- **Check for failed bootstraps**: Nodes that failed to bootstrap fully may have fewer tokens")
            builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'total_nodes': num_nodes,
                'total_datacenters': num_dcs,
                'total_tokens': total_tokens,
                'min_tokens_per_node': dist_stats['min_tokens'],
                'max_tokens_per_node': dist_stats['max_tokens'],
                'avg_tokens_per_node': round(dist_stats['avg_tokens'], 1),
                'skew_ratio': round(dist_stats['skew_ratio'], 2),
                'has_imbalance': dist_stats['skew_ratio'] >= skew_warning,
                'has_severe_imbalance': dist_stats['skew_ratio'] >= skew_critical,
                'imbalanced_node_count': len(dist_stats['imbalanced_nodes']),
                'datacenters': list(ring_data['datacenters'].keys())
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Ring analysis check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Ring analysis check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

"""
Patroni Cluster Topology Check

Discovers and visualizes Patroni-managed PostgreSQL cluster topology,
including leader/replica roles, replication lag, and cluster health.

This check uses multiple data sources:
1. Patroni REST API (/cluster endpoint)
2. PostgreSQL replication views (pg_stat_replication, pg_stat_wal_receiver)
3. SSH commands (patronictl, if available)

Returns structured data compatible with trend analysis.
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple
from plugins.common.check_helpers import CheckContentBuilder
from plugins.common.plantuml_helpers import ClusterTopologyDiagram, embed_diagram_in_adoc

logger = logging.getLogger(__name__)


def check_patroni_topology(connector, settings):
    """
    Discover and analyze Patroni cluster topology.

    Args:
        connector: PostgreSQL connector with Patroni detection
        settings: Configuration settings

    Returns:
        Tuple of (adoc_content, structured findings dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Patroni Cluster Topology")

    # Check if this is a Patroni cluster
    if connector.environment != 'patroni':
        builder.text("⏭️  Skipped - Not a Patroni cluster")
        return builder.build(), {
            'patroni_topology': {
                'status': 'skipped',
                'reason': 'Not a Patroni-managed cluster',
                'environment': connector.environment
            }
        }

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Gather topology from multiple sources
        topology_data = _discover_topology(connector, settings)

        if topology_data['status'] == 'error':
            builder.error(f"❌ Failed to discover topology: {topology_data.get('error', 'Unknown error')}")
            return builder.build(), {'patroni_topology': topology_data}

        # Analyze topology health
        health_analysis = _analyze_topology_health(topology_data)

        # Build visualization
        _build_topology_visualization(builder, topology_data, health_analysis)

        # Add health warnings if any using admonition wrappers
        if health_analysis['critical_issues']:
            builder.critical_issue(
                "Critical Cluster Issues Detected",
                health_analysis['critical_issues']
            )

        if health_analysis['warnings']:
            builder.warning_issue(
                "Cluster Warnings",
                health_analysis['warnings']
            )

        # Build structured findings for rules engine
        # Report builder wraps with module name (check_patroni_topology)
        # Return structured_data directly like legacy checks

        # CRITICAL: Do NOT include list values in data dict!
        # The rules engine will recurse into dicts with list values instead of evaluating them
        # Pre-compute values needed for rule evaluation

        members = topology_data.get('members', [])
        replicas = topology_data.get('replicas', [])

        # Extract timelines from members to check for divergence
        timelines = set()
        for member in members:
            if isinstance(member, dict) and member.get('timeline') is not None:
                timelines.add(member.get('timeline'))

        # Count unique timelines (divergence if > 1)
        unique_timeline_count = len(timelines)
        timeline_divergence = unique_timeline_count > 1

        # Get leader info
        leader = topology_data.get('leader', {})
        leader_timeline = leader.get('timeline') if isinstance(leader, dict) else None

        structured_data = {
            'status': 'success',
            'timestamp': timestamp,
            'data': {
                # Cluster topology data (for rules to access) - SCALARS ONLY
                'cluster_name': topology_data.get('cluster_name'),
                'total_nodes': topology_data.get('total_nodes', 0),
                'replica_count': len(replicas),
                'member_count': len(members),

                # Timeline information for rule evaluation
                'unique_timeline_count': unique_timeline_count,
                'timeline_divergence': timeline_divergence,
                'leader_timeline': leader_timeline,

                # Leader info (dict but no lists inside)
                'leader_name': leader.get('name') if isinstance(leader, dict) else None,
                'leader_host': leader.get('host') if isinstance(leader, dict) else None,
                'leader_state': leader.get('state') if isinstance(leader, dict) else None,

                # Health analysis data (for rules to access)
                'health_score': health_analysis.get('health_score', 0),
                'critical_issue_count': len(health_analysis.get('critical_issues', [])),
                'warning_count': len(health_analysis.get('warnings', [])),

                # Metadata
                'source': topology_data.get('source', 'unknown'),
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Failed to check Patroni topology: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {
            'patroni_topology': {
                'status': 'error',
                'error': str(e),
                'timestamp': timestamp
            }
        }


def _discover_topology(connector, settings) -> Dict:
    """
    Discover cluster topology using multiple methods.

    Priority:
    1. Patroni REST API (most comprehensive)
    2. PostgreSQL replication views (fallback)
    3. SSH patronictl commands (if available)

    Args:
        connector: PostgreSQL connector
        settings: Configuration settings

    Returns:
        Dictionary with topology data
    """
    # Method 1: Try Patroni REST API
    api_topology = _discover_via_patroni_api(connector, settings)
    if api_topology['status'] == 'success':
        return api_topology

    # Method 2: Fallback to PostgreSQL views
    logger.info("Patroni API unavailable, using PostgreSQL replication views")
    pg_topology = _discover_via_pg_views(connector)
    if pg_topology['status'] == 'success':
        return pg_topology

    # Method 3: Try SSH if available
    if connector.has_ssh_support():
        logger.info("Trying SSH patronictl commands")
        ssh_topology = _discover_via_ssh(connector)
        if ssh_topology['status'] == 'success':
            return ssh_topology

    return {
        'status': 'error',
        'error': 'All topology discovery methods failed',
        'source': 'none'
    }


def _discover_via_patroni_api(connector, settings) -> Dict:
    """Discover topology via Patroni REST API."""
    try:
        from plugins.postgres.utils.patroni_client import create_patroni_client_from_settings

        client = create_patroni_client_from_settings(settings)
        if not client:
            return {'status': 'error', 'error': 'Could not create Patroni client'}

        success, result = client.get_cluster_topology()
        client.close()

        if not success:
            return {'status': 'error', 'error': result.get('error', 'Unknown error')}

        # Parse Patroni API response
        cluster_data = result['data']
        members = cluster_data.get('members', [])

        if not members:
            return {'status': 'error', 'error': 'No cluster members found in API response'}

        # Extract topology information
        topology = {
            'status': 'success',
            'source': 'patroni_api',
            'cluster_name': cluster_data.get('scope', 'unknown'),
            'members': []
        }

        for member in members:
            member_info = {
                'name': member.get('name', 'unknown'),
                'host': member.get('host', 'unknown'),
                'port': member.get('port', 5432),
                'role': member.get('role', 'unknown'),
                'state': member.get('state', 'unknown'),
                'timeline': member.get('timeline', 0),
                'lag': member.get('lag', 0)
            }

            # Parse lag (can be in bytes or 'unknown')
            if isinstance(member_info['lag'], int):
                member_info['lag_mb'] = round(member_info['lag'] / (1024 * 1024), 2)
            else:
                member_info['lag_mb'] = None

            topology['members'].append(member_info)

        # Identify leader and replicas
        topology['leader'] = next((m for m in topology['members'] if m['role'] == 'leader'), None)
        topology['replicas'] = [m for m in topology['members'] if m['role'] != 'leader']

        return topology

    except ImportError as e:
        logger.debug(f"Cannot import patroni_client: {e}")
        return {'status': 'error', 'error': 'Patroni client not available'}
    except Exception as e:
        logger.error(f"Error discovering via Patroni API: {e}")
        return {'status': 'error', 'error': str(e)}


def _discover_via_pg_views(connector) -> Dict:
    """Discover topology via PostgreSQL replication views."""
    try:
        # Use direct Patroni connection if available, otherwise use primary connection
        conn, cursor = connector.get_patroni_connection()

        # Check if we're on the leader by querying pg_stat_replication
        cursor.execute("""
            SELECT
                application_name,
                client_addr,
                state,
                sync_state,
                COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn), 0) as send_lag_bytes,
                COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn), 0) as write_lag_bytes,
                COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn), 0) as flush_lag_bytes,
                COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0) as replay_lag_bytes
            FROM pg_stat_replication
        """)
        replicas = cursor.fetchall()

        if replicas:
            # We're on the leader
            topology = {
                'status': 'success',
                'source': 'pg_views_leader',
                'cluster_name': 'unknown',
                'leader': {
                    'name': connector.settings.get('host', 'unknown'),
                    'host': connector.settings.get('host', 'unknown'),
                    'port': connector.settings.get('port', 5432),
                    'role': 'leader',
                    'state': 'running',
                    'timeline': None,
                    'lag': 0,
                    'lag_mb': 0
                },
                'replicas': [],
                'members': []
            }

            for replica in replicas:
                replica_info = {
                    'name': replica[0] or 'unknown',
                    'host': str(replica[1]) if replica[1] else 'unknown',
                    'port': 5432,
                    'role': 'replica',
                    'state': replica[2] or 'unknown',
                    'sync_state': replica[3] or 'async',
                    'lag': replica[7],  # replay_lag_bytes
                    'lag_mb': round(replica[7] / (1024 * 1024), 2) if replica[7] else 0
                }
                topology['replicas'].append(replica_info)

            topology['members'] = [topology['leader']] + topology['replicas']
            return topology

        else:
            # We might be on a replica - check pg_stat_wal_receiver
            cursor.execute("""
                SELECT
                    status,
                    conninfo,
                    COALESCE(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()), 0) as replay_lag_bytes
                FROM pg_stat_wal_receiver
            """)
            wal_receiver = cursor.fetchone()

            if wal_receiver:
                # We're on a replica
                topology = {
                    'status': 'success',
                    'source': 'pg_views_replica',
                    'cluster_name': 'unknown',
                    'leader': None,  # We don't know leader details from replica
                    'replicas': [{
                        'name': connector.settings.get('host', 'unknown'),
                        'host': connector.settings.get('host', 'unknown'),
                        'port': connector.settings.get('port', 5432),
                        'role': 'replica',
                        'state': wal_receiver[0] or 'unknown',
                        'lag': wal_receiver[2],
                        'lag_mb': round(wal_receiver[2] / (1024 * 1024), 2) if wal_receiver[2] else 0
                    }],
                    'members': []
                }
                topology['members'] = topology['replicas']
                return topology
            else:
                # Standalone or can't determine
                return {
                    'status': 'error',
                    'error': 'No replication information found - might be standalone instance'
                }

    except Exception as e:
        logger.error(f"Error discovering via PostgreSQL views: {e}")
        return {'status': 'error', 'error': str(e)}


def _discover_via_ssh(connector) -> Dict:
    """Discover topology via SSH patronictl commands."""
    try:
        # Try patronictl list command
        result = connector.execute_ssh_command("patronictl list 2>/dev/null || echo 'NOT_FOUND'")

        if not result or not result.get('success') or 'NOT_FOUND' in result.get('stdout', ''):
            return {'status': 'error', 'error': 'patronictl command not available'}

        # Parse patronictl list output
        # Format: +-----+------+------+--------+-------+---------+
        #         | ... | Role | State| ... |
        # This is a simplified parser - would need enhancement for production
        return {
            'status': 'error',
            'error': 'SSH topology discovery not fully implemented yet'
        }

    except Exception as e:
        logger.error(f"Error discovering via SSH: {e}")
        return {'status': 'error', 'error': str(e)}


def _analyze_topology_health(topology_data: Dict) -> Dict:
    """
    Analyze topology for health issues and configuration problems.

    Args:
        topology_data: Topology data from discovery

    Returns:
        Dictionary with health analysis
    """
    critical_issues = []
    warnings = []
    info = []

    members = topology_data.get('members', [])
    leader = topology_data.get('leader')
    replicas = topology_data.get('replicas', [])

    # Check 1: Leader availability
    if not leader:
        critical_issues.append("No leader found in cluster - cluster may be in failover")

    # Check 2: Replica states
    for replica in replicas:
        state = replica.get('state', '').lower()
        if state not in ['running', 'streaming']:
            warnings.append(f"Replica {replica['name']} is in state: {state}")

        # Check replication lag
        lag_mb = replica.get('lag_mb', 0)
        if lag_mb is not None:
            if lag_mb > 1000:  # >1GB
                critical_issues.append(f"Replica {replica['name']} has critical lag: {lag_mb} MB")
            elif lag_mb > 100:  # >100MB
                warnings.append(f"Replica {replica['name']} has high lag: {lag_mb} MB")

    # Check 3: Timeline consistency
    if leader:
        leader_timeline = leader.get('timeline')
        if leader_timeline is not None:
            for replica in replicas:
                replica_timeline = replica.get('timeline')
                if replica_timeline is not None and replica_timeline != leader_timeline:
                    critical_issues.append(
                        f"Timeline divergence detected: leader={leader_timeline}, "
                        f"replica {replica['name']}={replica_timeline}"
                    )

    # Check 4: Cluster size
    total_nodes = len(members)
    if total_nodes == 1:
        warnings.append("Single-node cluster - no high availability")
    elif total_nodes == 2:
        warnings.append("Two-node cluster - limited fault tolerance")
    elif total_nodes % 2 == 0:
        info.append(f"Even number of nodes ({total_nodes}) - consider odd number for DCS quorum")

    # Check 5: Synchronous replication
    has_sync_replica = any(r.get('sync_state') == 'sync' for r in replicas)
    if not has_sync_replica and len(replicas) > 0:
        info.append("No synchronous replicas configured - using async replication")

    return {
        'critical_issues': critical_issues,
        'warnings': warnings,
        'info': info,
        'health_score': _calculate_health_score(critical_issues, warnings)
    }


def _calculate_health_score(critical_issues: List, warnings: List) -> int:
    """
    Calculate overall health score (0-100).

    Args:
        critical_issues: List of critical issues
        warnings: List of warnings

    Returns:
        Health score (0-100)
    """
    score = 100
    score -= len(critical_issues) * 30  # -30 per critical issue
    score -= len(warnings) * 10  # -10 per warning
    return max(0, score)


def _build_topology_visualization(builder: CheckContentBuilder, topology_data: Dict, health_analysis: Dict):
    """
    Build PlantUML visualization of cluster topology using common helpers.

    Args:
        builder: CheckContentBuilder instance
        topology_data: Topology data
        health_analysis: Health analysis results
    """
    cluster_name = topology_data.get('cluster_name', 'unknown')
    source = topology_data.get('source', 'unknown')
    leader = topology_data.get('leader')
    replicas = topology_data.get('replicas', [])
    members = topology_data.get('members', [])

    # Summary - using table for cleaner formatting
    summary_data = [
        {'Attribute': 'Cluster Name', 'Value': cluster_name},
        {'Attribute': 'Data Source', 'Value': source},
        {'Attribute': 'Total Nodes', 'Value': str(len(members))},
    ]

    if leader:
        summary_data.append({'Attribute': 'Leader', 'Value': leader['name']})

    summary_data.extend([
        {'Attribute': 'Replicas', 'Value': str(len(replicas))},
        {'Attribute': 'Health Score', 'Value': f"{health_analysis['health_score']}/100"}
    ])

    builder.table(summary_data)
    builder.blank()

    # Generate PlantUML diagram using helper
    builder.text("==== Cluster Topology Diagram")
    builder.blank()

    if leader:
        # Create diagram using helper
        diagram = ClusterTopologyDiagram(title=f"Patroni Cluster: {cluster_name}")

        # Add leader node
        leader_metrics = {}
        if leader.get('timeline'):
            leader_metrics['Timeline'] = leader['timeline']

        diagram.add_leader_node(
            node_id="leader",
            address=f"{leader['host']}:{leader['port']}",
            state=leader.get('state', 'unknown'),
            metrics=leader_metrics
        )

        # Add replica nodes and replication connections
        for i, replica in enumerate(replicas):
            replica_id = f"replica{i}"
            lag_mb = replica.get('lag_mb', 0)

            # Determine lag display and state
            if lag_mb is not None and isinstance(lag_mb, (int, float)):
                if lag_mb > 1000:
                    lag_display = f"⚠️ {lag_mb} MB"
                    replica_state = "warning"
                elif lag_mb > 100:
                    lag_display = f"⚠ {lag_mb} MB"
                    replica_state = "warning"
                else:
                    lag_display = f"{lag_mb} MB"
                    replica_state = replica.get('state', 'streaming')
            else:
                lag_display = "unknown"
                replica_state = replica.get('state', 'streaming')

            replica_metrics = {}
            if replica.get('timeline'):
                replica_metrics['Timeline'] = replica['timeline']

            diagram.add_replica_node(
                node_id=replica_id,
                address=f"{replica['host']}:{replica['port']}",
                state=replica_state,
                sync_mode=replica.get('sync_state', 'async'),
                lag=lag_display,
                metrics=replica_metrics
            )

            # Add replication connection
            is_sync = replica.get('sync_state') == 'sync'
            diagram.add_replication(
                source_id="leader",
                target_id=replica_id,
                sync=is_sync
            )

        # Add synchronous replication note to legend if applicable
        if any(r.get('sync_state') == 'sync' for r in replicas):
            diagram.add_legend_item("REPLICA_COLOR", "Synchronous Replica")

        # Generate and embed diagram
        plantuml_code = diagram.generate()
        adoc_block = embed_diagram_in_adoc(plantuml_code, "patroni-topology", "svg")

        # Split and add each line
        for line in adoc_block.split('\n'):
            builder.text(line)
        builder.blank()

        # Add detailed node information
        builder.text("==== Node Details")
        builder.blank()

        # Leader details
        leader_state_icon = "✅" if leader.get('state', '').lower() == 'running' else "⚠️"
        builder.text(f"*{leader_state_icon} Leader: {leader['name']}*")
        builder.blank()

        leader_details = [
            {'Attribute': 'Address', 'Value': f"`{leader['host']}:{leader['port']}`"},
            {'Attribute': 'State', 'Value': leader.get('state', 'unknown')},
        ]
        if leader.get('timeline'):
            leader_details.append({'Attribute': 'Timeline', 'Value': str(leader['timeline'])})

        builder.table(leader_details)
        builder.blank()

        # Replica details
        if replicas:
            builder.text("*Replicas:*")
            builder.blank()

            for i, replica in enumerate(replicas, 1):
                replica_state_icon = "✅" if replica.get('state', '').lower() in ['running', 'streaming'] else "⚠️"
                sync_indicator = "SYNC" if replica.get('sync_state') == 'sync' else "ASYNC"
                lag_display = f"{replica.get('lag_mb', 'unknown')} MB" if replica.get('lag_mb') is not None else "unknown"

                builder.text(f"*{replica_state_icon} Replica {i}: {replica['name']}* [{sync_indicator}]")
                builder.blank()

                replica_details = [
                    {'Attribute': 'Address', 'Value': f"`{replica['host']}:{replica['port']}`"},
                    {'Attribute': 'State', 'Value': replica.get('state', 'unknown')},
                    {'Attribute': 'Replication Lag', 'Value': lag_display},
                ]
                if replica.get('timeline'):
                    replica_details.append({'Attribute': 'Timeline', 'Value': str(replica['timeline'])})

                builder.table(replica_details)
                builder.blank()
    else:
        builder.text("⚠️  No leader information available")
        builder.blank()
        if members:
            builder.text("*Known Members:*")
            for member in members:
                builder.text(f"- {member.get('name', 'unknown')} ({member.get('host', 'unknown')}:{member.get('port', 5432)})")


# Register check metadata
check_metadata = {
    'name': 'patroni_topology',
    'description': 'Discover and analyze Patroni cluster topology',
    'category': 'high_availability',
    'requires_api': False,
    'requires_ssh': False,
    'requires_cql': False
}

"""
Comprehensive Cluster Connectivity Diagnostics for Cassandra.

This check consolidates multiple connectivity and gossip checks into a single
comprehensive diagnostic that analyzes:
- Cluster topology and node states
- Gossip communication between nodes
- Port connectivity (7000, 7001, 9042)
- Firewall configuration
- System logs for connection issues
- Network diagnostics

This replaces and supersedes:
- nodetool_gossipinfo_peers_check.py
- cluster_connectivity_check.py
- Parts of network_connection_stats_check.py
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 9  # Critical - cluster connectivity affects entire system


def run_cluster_connectivity_diagnostics(connector, settings):
    """
    Comprehensive cluster connectivity and health diagnostics.

    Args:
        connector: Cassandra connector with multi-host SSH support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Cluster connectivity diagnostics")
    if not available:
        return skip_msg, skip_data

    try:
        builder.h3("Cluster Health & Connectivity Diagnostics")
        builder.para(
            "Comprehensive analysis of cluster topology, gossip state, "
            "port connectivity, firewall configuration, and network health."
        )
        builder.blank()

        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_nodes = list(connector.get_ssh_hosts())

        # Data collection structures
        topology_data = {}
        gossip_data = {}
        connectivity_matrix = {}
        firewall_data = {}
        log_issues = {}
        network_diagnostics = {}
        errors = []

        # ========================================
        # PHASE 1: COLLECT DATA FROM ALL NODES
        # ========================================

        for ssh_host in all_nodes:
            node_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                ssh_manager = connector.get_ssh_manager(ssh_host)
                if not ssh_manager:
                    continue

                ssh_manager.ensure_connected()

                # === 1.1 Cluster Topology (nodetool status) ===
                stdout, stderr, exit_code = ssh_manager.execute_command("nodetool status")
                if exit_code == 0:
                    topology_data[node_id] = _parse_nodetool_status(stdout)
                else:
                    errors.append({
                        'node': node_id,
                        'check': 'nodetool status',
                        'error': stderr
                    })

                # === 1.2 Gossip Information ===
                stdout, stderr, exit_code = ssh_manager.execute_command("nodetool gossipinfo")
                if exit_code == 0:
                    from plugins.common.parsers import NodetoolParser
                    parser = NodetoolParser()
                    gossip_states = parser.parse('gossipinfo', stdout)
                    gossip_data[node_id] = gossip_states
                else:
                    errors.append({
                        'node': node_id,
                        'check': 'nodetool gossipinfo',
                        'error': stderr
                    })

                # === 1.3 Firewall Status ===
                firewall_info = _check_firewall_status(ssh_manager)
                firewall_data[node_id] = firewall_info

                # === 1.4 System Log Analysis (last 100 lines with gossip/connection issues) ===
                log_cmd = (
                    "tail -n 200 /var/log/cassandra/system.log 2>/dev/null | "
                    "grep -iE 'gossip|connection.*refused|timeout|unreachable|Exception.*connect' | "
                    "tail -n 20"
                )
                stdout, stderr, exit_code = ssh_manager.execute_command(log_cmd)
                if exit_code == 0 and stdout.strip():
                    log_issues[node_id] = stdout.strip().split('\n')
                else:
                    log_issues[node_id] = []

                # === 1.5 Cassandra Configuration ===
                cassandra_yaml_checks = {
                    'phi_convict_threshold': "grep -E '^phi_convict_threshold:' /etc/cassandra/cassandra.yaml 2>/dev/null | awk '{print $2}'",
                    'listen_address': "grep -E '^listen_address:' /etc/cassandra/cassandra.yaml 2>/dev/null | awk '{print $2}'",
                    'endpoint_snitch': "grep -E '^endpoint_snitch:' /etc/cassandra/cassandra.yaml 2>/dev/null | awk '{print $2}'"
                }
                config_info = {}
                for key, cmd in cassandra_yaml_checks.items():
                    stdout, _, exit_code = ssh_manager.execute_command(cmd)
                    config_info[key] = stdout.strip() if exit_code == 0 else 'N/A'

                network_diagnostics[node_id] = config_info

            except Exception as e:
                logger.error(f"Error collecting data from {ssh_host}: {e}")
                errors.append({
                    'node': node_id,
                    'check': 'data collection',
                    'error': str(e)
                })

        # ========================================
        # PHASE 2: PORT CONNECTIVITY MATRIX
        # ========================================

        builder.h4("🔌 Port Connectivity Matrix")
        builder.para("Testing inter-node connectivity on Cassandra ports (7000, 7001, 9042)")
        builder.blank()

        ports_to_test = {
            '7000': 'Gossip',
            '7001': 'SSL Gossip',
            '9042': 'CQL Native'
        }

        for source_host in all_nodes:
            source_node = ssh_host_to_node.get(source_host, source_host)
            connectivity_matrix[source_node] = {}

            try:
                ssh_manager = connector.get_ssh_manager(source_host)
                if not ssh_manager:
                    continue

                for target_host in all_nodes:
                    if source_host == target_host:
                        continue

                    target_node = ssh_host_to_node.get(target_host, target_host)
                    connectivity_matrix[source_node][target_node] = {}

                    for port, port_name in ports_to_test.items():
                        # Use nc (netcat) with 2 second timeout
                        cmd = f"timeout 2 nc -zv {target_host} {port} 2>&1"
                        stdout, stderr, exit_code = ssh_manager.execute_command(cmd)

                        # nc returns 0 on success
                        is_open = exit_code == 0 or 'succeeded' in stdout.lower()

                        connectivity_matrix[source_node][target_node][port] = {
                            'open': is_open,
                            'name': port_name,
                            'output': stdout.strip() or stderr.strip()
                        }

            except Exception as e:
                logger.error(f"Error testing connectivity from {source_host}: {e}")

        # Build connectivity table
        if connectivity_matrix:
            table_lines = [
                "|===",
                "|Source Node|Target Node|Port 7000 (Gossip)|Port 7001 (SSL)|Port 9042 (CQL)"
            ]

            for source_node in sorted(connectivity_matrix.keys()):
                for target_node in sorted(connectivity_matrix[source_node].keys()):
                    conn_data = connectivity_matrix[source_node][target_node]

                    p7000 = "✅" if conn_data.get('7000', {}).get('open') else "❌"
                    p7001 = "✅" if conn_data.get('7001', {}).get('open') else "❌"
                    p9042 = "✅" if conn_data.get('9042', {}).get('open') else "❌"

                    table_lines.append(
                        f"|{source_node}|{target_node}|{p7000}|{p7001}|{p9042}"
                    )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # ========================================
        # PHASE 3: CLUSTER TOPOLOGY ANALYSIS
        # ========================================

        builder.h4("🗺️ Cluster Topology")

        if topology_data:
            # Show topology from each node's perspective
            builder.para("Each node's view of the cluster (from `nodetool status`):")
            builder.blank()

            for node_id, topo in topology_data.items():
                builder.text(f"**From Node {node_id}:**")

                if 'nodes' in topo and topo['nodes']:
                    table_lines = [
                        "|===",
                        "|Address|Status|State|Load|Tokens|Datacenter|Rack"
                    ]

                    for node_info in topo['nodes']:
                        status_icon = "🔴" if node_info.get('status') != 'UN' else ""
                        table_lines.append(
                            f"|{node_info.get('address', 'N/A')}|"
                            f"{status_icon}{node_info.get('status', 'N/A')}|"
                            f"{node_info.get('state', 'N/A')}|"
                            f"{node_info.get('load', 'N/A')}|"
                            f"{node_info.get('tokens', 'N/A')}|"
                            f"{node_info.get('datacenter', 'N/A')}|"
                            f"{node_info.get('rack', 'N/A')}"
                        )

                    table_lines.append("|===")
                    builder.add("\n".join(table_lines))
                else:
                    builder.para("_No topology data available_")

                builder.blank()

        # ========================================
        # PHASE 4: GOSSIP STATE ANALYSIS
        # ========================================

        builder.h4("💬 Gossip State Analysis")

        # Aggregate peer states across all nodes
        all_peer_states = {}
        all_schemas = {}

        for node_id, gossip_states in gossip_data.items():
            if not isinstance(gossip_states, dict):
                continue

            for peer_ip, peer_info in gossip_states.items():
                # Extract and clean status
                state_raw = peer_info.get('status', 'UNKNOWN')
                state = state_raw
                if ':' in state:
                    state = state.split(':', 1)[1]
                if ',' in state:
                    state = state.split(',')[0]
                state = state.strip()

                if peer_ip not in all_peer_states:
                    all_peer_states[peer_ip] = []
                all_peer_states[peer_ip].append({
                    'state': state,
                    'observed_from': node_id
                })

                # Track schema
                schema = peer_info.get('schema', peer_info.get('schema_version'))
                if schema:
                    if peer_ip not in all_schemas:
                        all_schemas[peer_ip] = []
                    all_schemas[peer_ip].append({
                        'schema': schema,
                        'observed_from': node_id
                    })

        # Detect inconsistencies
        unhealthy_peers = []
        schema_inconsistencies = []

        for peer_ip, states in all_peer_states.items():
            non_normal = [s for s in states if s['state'] != 'NORMAL']
            if non_normal:
                unhealthy_peers.append({
                    'peer_ip': peer_ip,
                    'states': states
                })

        for peer_ip, schemas in all_schemas.items():
            unique_schemas = list(set(s['schema'] for s in schemas))
            if len(unique_schemas) > 1:
                schema_inconsistencies.append({
                    'peer_ip': peer_ip,
                    'schemas': schemas,
                    'unique_schemas': unique_schemas
                })

        # Display gossip summary
        if all_peer_states:
            table_lines = [
                "|===",
                "|Peer IP|Gossip State(s)|Schema Consistency"
            ]

            for peer_ip in sorted(all_peer_states.keys()):
                states = all_peer_states[peer_ip]
                unique_states = list(set(s['state'] for s in states))
                state_str = ", ".join(unique_states)

                schemas = all_schemas.get(peer_ip, [])
                unique_schemas = list(set(s['schema'][:8] + "..." for s in schemas if s['schema']))
                schema_str = "✅ Consistent" if len(unique_schemas) <= 1 else "⚠️ Mismatch"

                indicator = "⚠️ " if len(unique_states) > 1 or len(unique_schemas) > 1 else ""

                table_lines.append(f"|{peer_ip}|{indicator}{state_str}|{schema_str}")

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

            if unhealthy_peers:
                builder.warning(
                    f"**{len(unhealthy_peers)} peer(s) with inconsistent gossip states detected!**\n\n" +
                    "Nodes are not seeing each other correctly:"
                )
                for peer in unhealthy_peers:
                    states_detail = ", ".join([f"{s['state']} (from {s['observed_from']})"
                                              for s in peer['states']])
                    builder.text(f"* **{peer['peer_ip']}**: {states_detail}")
                builder.blank()

            if schema_inconsistencies:
                builder.warning(
                    f"**{len(schema_inconsistencies)} peer(s) with schema version mismatches!**"
                )

        # ========================================
        # PHASE 5: FIREWALL & NETWORK CONFIG
        # ========================================

        builder.h4("🔥 Firewall & Network Configuration")

        if firewall_data:
            table_lines = [
                "|===",
                "|Node|Firewall Active|Port 7000 Rule|Port 7001 Rule|Port 9042 Rule"
            ]

            for node_id in sorted(firewall_data.keys()):
                fw_info = firewall_data[node_id]
                active = "✅ Active" if fw_info.get('active') else "Inactive"

                # If firewall is inactive, ports can't be "blocked" - show N/A
                if not fw_info.get('active'):
                    p7000 = "N/A"
                    p7001 = "N/A"
                    p9042 = "N/A"
                else:
                    # Firewall is active - check if ports are explicitly allowed
                    p7000 = "✅ Allowed" if fw_info.get('port_7000_open') else "❌ Blocked"
                    p7001 = "✅ Allowed" if fw_info.get('port_7001_open') else "❌ Blocked"
                    p9042 = "✅ Allowed" if fw_info.get('port_9042_open') else "❌ Blocked"

                table_lines.append(f"|{node_id}|{active}|{p7000}|{p7001}|{p9042}")

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # Network configuration
        if network_diagnostics:
            builder.text("**Cassandra Configuration:**")
            builder.blank()

            table_lines = [
                "|===",
                "|Node|phi_convict_threshold|listen_address|endpoint_snitch"
            ]

            for node_id in sorted(network_diagnostics.keys()):
                config = network_diagnostics[node_id]
                table_lines.append(
                    f"|{node_id}|"
                    f"{config.get('phi_convict_threshold', 'N/A')}|"
                    f"{config.get('listen_address', 'N/A')}|"
                    f"{config.get('endpoint_snitch', 'N/A')}"
                )

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # ========================================
        # PHASE 6: SYSTEM LOG ISSUES
        # ========================================

        builder.h4("📋 System Log Issues (Connection/Gossip)")

        has_log_issues = False
        for node_id, log_lines in log_issues.items():
            if log_lines:
                has_log_issues = True
                builder.text(f"**Node {node_id}:** (Last 20 connection-related log entries)")
                builder.literal('\n'.join(log_lines[-20:]))
                builder.blank()

        if not has_log_issues:
            builder.success("No connection or gossip errors found in recent system logs.")

        # ========================================
        # PHASE 7: ERROR SUMMARY
        # ========================================

        if errors:
            builder.h4("❌ Collection Errors")
            builder.warning(
                f"Encountered {len(errors)} error(s) during diagnostics:\n\n" +
                "\n".join([f"* **{e['node']}** ({e['check']}): {e['error']}"
                          for e in errors])
            )

        # ========================================
        # PHASE 8: RECOMMENDATIONS
        # ========================================

        # Analyze findings and provide recommendations
        issues_found = bool(unhealthy_peers or schema_inconsistencies or has_log_issues)

        # Check connectivity issues
        connectivity_issues = False
        for source in connectivity_matrix.values():
            for target_data in source.values():
                for port_data in target_data.values():
                    if not port_data.get('open'):
                        connectivity_issues = True
                        break

        if issues_found or connectivity_issues:
            recommendations = {}

            if connectivity_issues:
                recommendations["critical"] = [
                    "**Port connectivity failures detected!** Check firewall rules immediately",
                    "**Verify Cassandra ports are open:** `sudo firewall-cmd --list-ports` or `sudo ufw status`",
                    "**Open required ports:** `sudo firewall-cmd --add-port=7000/tcp --add-port=7001/tcp --add-port=9042/tcp --permanent && sudo firewall-cmd --reload`",
                    "**Check iptables:** `sudo iptables -L -n | grep -E '7000|7001|9042'`",
                    "**Verify listen_address in cassandra.yaml** matches actual network interface"
                ]

            if unhealthy_peers:
                if "critical" not in recommendations:
                    recommendations["critical"] = []
                recommendations["critical"].extend([
                    "**Gossip state inconsistencies detected!** Nodes cannot communicate properly",
                    "**Check network connectivity:** Ensure all nodes can reach each other",
                    "**Review cassandra.yaml:** Verify `seed` nodes are correctly configured",
                    "**Check DNS resolution:** Ensure hostnames resolve correctly on all nodes"
                ])

            if schema_inconsistencies:
                recommendations["high"] = [
                    "**Schema version mismatches detected!** This can cause query failures",
                    "**Wait for propagation:** Schema changes can take time via gossip (check every 30s)",
                    "**Force schema sync:** Run `nodetool describecluster` to check agreement",
                    "**Last resort:** `nodetool resetlocalschema` (CAUTION: only if cluster is stable)"
                ]

            recommendations["general"] = [
                "Monitor gossip activity regularly with `nodetool gossipinfo`",
                "Set up alerts for schema disagreements lasting >5 minutes",
                "Review `phi_convict_threshold` if false positives occur (default=8)",
                "Ensure adequate network bandwidth and low latency between nodes",
                "Check for asymmetric routing or network issues",
                "Verify system time sync (NTP) across all nodes"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                "✅ Cluster connectivity is healthy!\n\n"
                "All nodes can communicate properly via gossip, and port connectivity is working."
            )

        # ========================================
        # STRUCTURED DATA
        # ========================================

        structured_data["cluster_connectivity"] = {
            "status": "success",
            "nodes_checked": len(all_nodes),
            "topology": topology_data,
            "gossip_states": gossip_data,
            "connectivity_matrix": connectivity_matrix,
            "unhealthy_peers": len(unhealthy_peers),
            "schema_inconsistencies": len(schema_inconsistencies),
            "firewall_data": firewall_data,
            "log_issues_found": has_log_issues,
            "errors": errors
        }

    except Exception as e:
        import traceback
        logger.error(f"Cluster connectivity diagnostics failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["cluster_connectivity"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data


def _parse_nodetool_status(output: str) -> dict:
    """
    Parse nodetool status output.

    Returns:
        dict: Parsed topology data with nodes list
    """
    nodes = []
    current_dc = None

    for line in output.split('\n'):
        line = line.strip()

        # Detect datacenter
        if line.startswith('Datacenter:'):
            current_dc = line.split(':', 1)[1].strip()
            continue

        # Parse node lines (start with UN, DN, UL, etc.)
        if re.match(r'^[UD][NLJM]', line):
            parts = line.split()
            if len(parts) >= 6:
                nodes.append({
                    'status': parts[0],
                    'address': parts[1],
                    'load': parts[2],
                    'tokens': parts[3],
                    'owns': parts[4] if '%' in parts[4] else 'N/A',
                    'host_id': parts[5] if len(parts) > 5 else 'N/A',
                    'rack': parts[6] if len(parts) > 6 else 'N/A',
                    'datacenter': current_dc,
                    'state': parts[0][0],  # U or D
                })

    return {'nodes': nodes}


def _check_firewall_status(ssh_manager) -> dict:
    """
    Check firewall status and Cassandra port rules.

    Args:
        ssh_manager: SSH connection manager

    Returns:
        dict: Firewall status information
    """
    firewall_info = {
        'active': False,
        'port_7000_open': False,
        'port_7001_open': False,
        'port_9042_open': False,
        'firewall_type': 'unknown'
    }

    # Check firewalld
    stdout, stderr, exit_code = ssh_manager.execute_command("systemctl is-active firewalld 2>/dev/null")
    if exit_code == 0 and 'active' in stdout:
        firewall_info['active'] = True
        firewall_info['firewall_type'] = 'firewalld'

        # Check ports
        stdout, _, _ = ssh_manager.execute_command("firewall-cmd --list-ports 2>/dev/null")
        firewall_info['port_7000_open'] = '7000/tcp' in stdout
        firewall_info['port_7001_open'] = '7001/tcp' in stdout
        firewall_info['port_9042_open'] = '9042/tcp' in stdout
        return firewall_info

    # Check ufw
    stdout, stderr, exit_code = ssh_manager.execute_command("ufw status 2>/dev/null")
    if exit_code == 0 and 'active' in stdout.lower():
        firewall_info['active'] = True
        firewall_info['firewall_type'] = 'ufw'

        firewall_info['port_7000_open'] = '7000' in stdout
        firewall_info['port_7001_open'] = '7001' in stdout
        firewall_info['port_9042_open'] = '9042' in stdout
        return firewall_info

    # Check iptables
    stdout, stderr, exit_code = ssh_manager.execute_command("iptables -L -n 2>/dev/null | wc -l")
    if exit_code == 0:
        try:
            rule_count = int(stdout.strip())
            if rule_count > 10:  # Likely has active rules
                firewall_info['active'] = True
                firewall_info['firewall_type'] = 'iptables'

                # Check for port rules
                stdout, _, _ = ssh_manager.execute_command("iptables -L -n 2>/dev/null | grep -E 'dpt:(7000|7001|9042)'")
                firewall_info['port_7000_open'] = 'dpt:7000' in stdout
                firewall_info['port_7001_open'] = 'dpt:7001' in stdout
                firewall_info['port_9042_open'] = 'dpt:9042' in stdout
        except ValueError:
            pass

    return firewall_info

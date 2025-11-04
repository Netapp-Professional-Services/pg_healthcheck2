"""
Peer Gossip Information check for Cassandra nodes.

Analyzes gossip information to verify peer connectivity and schema consistency.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def run_nodetool_gossipinfo_peers_check(connector, settings):
    """
    Analyzes gossip information from all Cassandra nodes using 'nodetool gossipinfo'.

    Args:
        connector: Cassandra connector with multi-host SSH support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Gossip info check")
    if not available:
        return skip_msg, skip_data

    try:
        builder.h3("Peer Gossip Information (All Nodes)")
        builder.para("Inspecting peer states and schema consistency using `nodetool gossipinfo`.")
        builder.blank()

        # === CHECK ALL NODES ===
        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        all_gossip_data = []
        all_peer_states = {}  # Map peer IP -> list of states seen from different nodes
        all_schemas = {}      # Map peer IP -> list of schemas seen from different nodes
        errors = []

        for ssh_host in connector.get_ssh_hosts():
            node_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                ssh_manager = connector.get_ssh_manager(ssh_host)
                if not ssh_manager:
                    continue

                ssh_manager.ensure_connected()

                # Execute nodetool gossipinfo
                command = "nodetool gossipinfo"
                stdout, stderr, exit_code = ssh_manager.execute_command(command)

                if exit_code != 0:
                    logger.warning(f"nodetool gossipinfo failed on {ssh_host}: {stderr}")
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': f"nodetool gossipinfo failed: {stderr}"
                    })
                    continue

                # Parse the output using the NodetoolParser
                from plugins.common.parsers import NodetoolParser
                parser = NodetoolParser()
                gossip_states = parser.parse('gossipinfo', stdout)

                # gossip_states is a dict: {peer_ip: {key: value, ...}}
                if not isinstance(gossip_states, dict) or not gossip_states:
                    errors.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'error': 'No gossip data returned'
                    })
                    continue

                # Store data for this node
                node_gossip = {
                    'host': ssh_host,
                    'node_id': node_id,
                    'peers': gossip_states
                }
                all_gossip_data.append(node_gossip)

                # Aggregate peer states and schemas across all nodes
                for peer_ip, peer_info in gossip_states.items():
                    # Track state (STATUS field)
                    # STATUS can have various formats:
                    # - "NORMAL,-9223372036854775808" (state with token)
                    # - "19:NORMAL" (with generation prefix)
                    # - "NORMAL" (just the state)
                    state_raw = peer_info.get('status', 'UNKNOWN')

                    # Clean up the state - extract just the state name
                    state = state_raw
                    if ':' in state:
                        # Remove generation prefix like "19:NORMAL" -> "NORMAL"
                        state = state.split(':', 1)[1]
                    if ',' in state:
                        # Remove token suffix like "NORMAL,-9223..." -> "NORMAL"
                        state = state.split(',')[0]

                    state = state.strip()

                    if peer_ip not in all_peer_states:
                        all_peer_states[peer_ip] = []
                    all_peer_states[peer_ip].append({
                        'state': state,
                        'observed_from': node_id
                    })

                    # Track schema version
                    schema = peer_info.get('schema', peer_info.get('schema_version'))
                    if schema:
                        if peer_ip not in all_schemas:
                            all_schemas[peer_ip] = []
                        all_schemas[peer_ip].append({
                            'schema': schema,
                            'observed_from': node_id
                        })

            except Exception as e:
                logger.error(f"Error checking gossip info on {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'node_id': node_id,
                    'error': str(e)
                })

        # === ANALYZE PEER STATES ===
        unhealthy_peers = []
        schema_inconsistencies = []

        for peer_ip, states in all_peer_states.items():
            # Check if any node sees this peer as not NORMAL
            non_normal_states = [s for s in states if s['state'] != 'NORMAL']
            if non_normal_states:
                unhealthy_peers.append({
                    'peer_ip': peer_ip,
                    'states': states
                })

        for peer_ip, schemas in all_schemas.items():
            # Check if different nodes see different schema versions for this peer
            unique_schemas = list(set(s['schema'] for s in schemas))
            if len(unique_schemas) > 1:
                schema_inconsistencies.append({
                    'peer_ip': peer_ip,
                    'schemas': schemas,
                    'unique_schemas': unique_schemas
                })

        # === DISPLAY ISSUES ===
        issues_found = False

        if unhealthy_peers:
            issues_found = True
            builder.h4("⚠️ Unhealthy Peer States Detected")

            for peer in unhealthy_peers:
                states_str = ", ".join([f"{s['state']} (seen from {s['observed_from']})"
                                       for s in peer['states']])
                builder.critical_issue(
                    f"Peer {peer['peer_ip']} Not in NORMAL State",
                    {
                        "Peer IP": peer['peer_ip'],
                        "States Observed": states_str
                    }
                )

        if schema_inconsistencies:
            issues_found = True
            builder.h4("⚠️ Schema Version Inconsistencies")

            for inconsistency in schema_inconsistencies:
                schemas_str = "\n".join([f"  - {s['schema'][:8]}... (seen from {s['observed_from']})"
                                        for s in inconsistency['schemas']])
                builder.warning_issue(
                    f"Schema Mismatch for Peer {inconsistency['peer_ip']}",
                    {
                        "Peer IP": inconsistency['peer_ip'],
                        "Schema Versions": schemas_str,
                        "Unique Count": str(len(inconsistency['unique_schemas']))
                    }
                )

        # === SUMMARY TABLE ===
        if all_gossip_data and all_peer_states:
            builder.h4("Gossip Summary")
            builder.para(f"Gossip information collected from {len(all_gossip_data)} node(s).")
            builder.para(f"Total unique peers seen: {len(all_peer_states)}")
            builder.blank()

            # Show peer state summary
            table_lines = [
                "|===",
                "|Peer IP|State(s)|Schema Version(s)"
            ]

            for peer_ip in sorted(all_peer_states.keys()):
                states = all_peer_states[peer_ip]
                unique_states = list(set(s['state'] for s in states))
                state_str = ", ".join(unique_states)

                schemas = all_schemas.get(peer_ip, [])
                unique_schemas = list(set(s['schema'][:8] + "..." for s in schemas if s['schema']))
                schema_str = ", ".join(unique_schemas) if unique_schemas else "N/A"

                indicator = "⚠️ " if len(unique_states) > 1 or len(unique_schemas) > 1 else ""

                table_lines.append(f"|{peer_ip}|{indicator}{state_str}|{schema_str}")

            table_lines.append("|===")
            builder.add("\n".join(table_lines))
            builder.blank()

        # === ERROR SUMMARY ===
        if errors:
            builder.h4("Checks with Errors")
            builder.warning(
                f"Could not check gossip info on {len(errors)} node(s):\n\n" +
                "\n".join([f"* Node {e['node_id']} ({e['host']}): {e['error']}"
                          for e in errors])
            )

        # === RECOMMENDATIONS ===
        if issues_found:
            recommendations = {}

            if unhealthy_peers:
                recommendations["critical"] = [
                    "**Check node connectivity:** Verify network connectivity between nodes (ports 7000/7001)",
                    "**Review Cassandra logs:** Check `/var/log/cassandra/system.log` for errors",
                    "**Verify firewall rules:** Ensure inter-node communication is not blocked",
                    "**Run `nodetool status`:** Cross-check node states from multiple perspectives",
                    "**Check gossip settings:** Review `phi_convict_threshold` in cassandra.yaml"
                ]

            if schema_inconsistencies:
                recommendations["high"] = [
                    "**Wait for propagation:** Schema changes can take time to propagate via gossip",
                    "**Run `nodetool describecluster`:** Check for schema agreement issues",
                    "**Verify recent DDL:** Ensure all schema changes completed successfully",
                    "**Check for unreachable nodes:** Unreachable nodes may have outdated schemas",
                    "**Force schema agreement:** Use `nodetool resetlocalschema` as last resort"
                ]

            recommendations["general"] = [
                "Monitor gossip activity with `nodetool gossipinfo` regularly",
                "Set up alerts for schema disagreements lasting >5 minutes",
                "Document schema change procedures to ensure proper propagation",
                "Review `system_schema` keyspace for inconsistencies",
                "Ensure adequate gossip settings for cluster size"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"All peers are in healthy state (NORMAL).\n\n"
                f"Schema versions are consistent across the cluster."
            )

        # === STRUCTURED DATA ===
        structured_data["gossip_info"] = {
            "status": "success",
            "nodes_checked": len(connector.get_ssh_hosts()),
            "nodes_with_errors": len(errors),
            "total_peers": len(all_peer_states),
            "unhealthy_peers": len(unhealthy_peers),
            "schema_inconsistencies": len(schema_inconsistencies),
            "errors": errors,
            "data": all_gossip_data
        }

    except Exception as e:
        import traceback
        logger.error(f"Gossip info check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data["gossip_info"] = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data
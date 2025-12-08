import json
import logging
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from typing import Dict, List, Optional

# Import shared utilities
from plugins.common import (
    SSHConnectionManager,
    ShellExecutor,
    AsciiDocFormatter,
    SSHSupportMixin
)
from plugins.common.parsers import NodetoolParser
from plugins.common.cve_mixin import CVECheckMixin

logger = logging.getLogger(__name__)


class CassandraConnector(SSHSupportMixin, CVECheckMixin):
    """
    Handles all direct communication with Cassandra, including CQL, nodetool, and shell commands.
    
    This connector provides a unified interface for:
    1. CQL queries (standard Cassandra queries)
    2. Nodetool commands (via SSH for operational metrics)
    3. Shell commands (via SSH for system-level metrics)
    
    Query Formats:
        CQL: Standard SQL string
            SELECT * FROM system.local;
        
        Nodetool (single node):
            {"operation": "nodetool", "command": "status"}
        
        Nodetool (cluster-wide):
            {"operation": "nodetool_cluster", "command": "tpstats"}
        
        Shell command:
            {"operation": "shell", "command": "df -h"}
    
    SSH Configuration (required for nodetool and shell):
        - ssh_hosts: List of hostnames/IPs of Cassandra nodes
        - ssh_user: SSH username
        - ssh_key_file: Path to private key (or ssh_password)
        - ssh_timeout: Connection timeout in seconds (default: 10)
    
    Example:
        connector = CassandraConnector(settings)
        connector.connect()
        
        # CQL query
        result = connector.execute_query("SELECT * FROM system.local")
        
        # Nodetool command
        result = connector.execute_query('{"operation": "nodetool", "command": "status"}')
        
        # Shell command
        result = connector.execute_query('{"operation": "shell", "command": "df -h"}')
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.cluster = None
        self.session = None
        self.version_info = {}

        # Initialize formatters and parsers
        self.formatter = AsciiDocFormatter()
        self.parser = NodetoolParser()

        # Multi-node support
        self.cluster_nodes = []  # List of discovered node addresses

        # Environment detection
        self.environment = None
        self.environment_details = {}

        # Technology name for CVE lookups
        self.technology_name = 'cassandra'

        # Initialize SSH support (from mixin)
        self.initialize_ssh()

        # Initialize CVE support (from mixin)
        self.initialize_cve_support()

        logger.info("Cassandra connector initialized")

    def connect(self):
        """Establishes CQL connection to the cluster and SSH connections to all nodes."""
        try:
            contact_points = self.settings.get('hosts', ['localhost'])
            port = self.settings.get('port', 9042)

            auth_provider = None
            if self.settings.get('user') and self.settings.get('password'):
                auth_provider = PlainTextAuthProvider(
                    username=self.settings.get('user'),
                    password=self.settings.get('password')
                )

            # Setup load balancing policy (DC-aware for multi-DC clusters)
            local_dc = self.settings.get('local_dc') or self.settings.get('datacenter')
            load_balancing_policy = None

            if local_dc:
                # Use DC-aware policy wrapped with token-aware for optimal routing
                load_balancing_policy = TokenAwarePolicy(
                    DCAwareRoundRobinPolicy(local_dc=local_dc)
                )
                logger.info(f"Using DC-aware load balancing policy for datacenter: {local_dc}")

            # Create execution profile
            execution_profiles = None
            if load_balancing_policy:
                # When using profiles, row_factory must be in the profile
                execution_profiles = {
                    EXEC_PROFILE_DEFAULT: ExecutionProfile(
                        load_balancing_policy=load_balancing_policy,
                        row_factory=dict_factory
                    )
                }

            self.cluster = Cluster(
                contact_points=contact_points,
                port=port,
                auth_provider=auth_provider,
                execution_profiles=execution_profiles
            )

            self.session = self.cluster.connect()

            # Only set row_factory on session if NOT using execution profiles
            if not execution_profiles:
                self.session.row_factory = dict_factory
            
            # Set keyspace if specified
            keyspace = self.settings.get('keyspace')
            if keyspace:
                self.session.set_keyspace(keyspace)
            
            self.version_info = self._get_version_info()
            
            # Connect all SSH hosts (from mixin)
            connected_ssh_hosts = self.connect_all_ssh()
            
            # Map SSH hosts to Cassandra nodes
            if connected_ssh_hosts:
                self._map_ssh_hosts_to_nodes()

            # Detect environment (Instaclustr vs self-hosted)
            self._detect_environment()

            # Display connection status
            print("✅ Successfully connected to Cassandra cluster")
            
            # Get cluster metadata for detailed status
            try:
                # Get node information
                nodes = self._discover_nodes()
                
                print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")
                print(f"   - Nodes: {len(nodes)}")
                print(f"   - Keyspace: {self.session.keyspace if self.session else 'None'}")
                
                # Show contact points
                if contact_points:
                    print(f"   - Contact Points:")
                    for cp in contact_points[:5]:
                        print(f"      • {cp}")
                    if len(contact_points) > 5:
                        print(f"      ... and {len(contact_points) - 5} more")
                
                # SSH status (from mixin)
                if self.has_ssh_support():
                    print(f"   - SSH: Connected to {len(connected_ssh_hosts)}/{len(self.get_ssh_hosts())} host(s)")
                    unmapped_hosts = []
                    for ssh_host in connected_ssh_hosts:
                        node_id = self.ssh_host_to_node.get(ssh_host)
                        if node_id:
                            print(f"      • {ssh_host} (Node: {node_id})")
                        else:
                            print(f"      • {ssh_host} (⚠️  Not in cluster membership)")
                            unmapped_hosts.append(ssh_host)

                    if unmapped_hosts:
                        print(f"   ⚠️  WARNING: {len(unmapped_hosts)} SSH host(s) are not recognized as cluster members!")
                        print(f"      This may indicate nodes that are down or not fully joined to the cluster.")
                else:
                    print(f"   - SSH: Not configured (nodetool checks unavailable)")
                    
            except Exception as e:
                logger.warning(f"Could not retrieve detailed cluster info: {e}")
            
            logger.info("✅ Connected to Cassandra cluster")
            logger.info(f"Version: {self.version_info.get('version_string', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise ConnectionError(f"Could not connect to Cassandra: {e}")

    def disconnect(self):
        """Closes the CQL connection and all SSH connections."""
        if self.cluster:
            try:
                self.cluster.shutdown()
                logger.info("Disconnected from Cassandra")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.cluster = None
                self.session = None
        
        # Disconnect all SSH (from mixin)
        self.disconnect_all_ssh()

    def close(self):
        """Alias for disconnect()."""
        self.disconnect()

    def _map_ssh_hosts_to_nodes(self):
        """Cassandra-specific logic to map SSH hosts to node IP addresses."""
        try:
            # Discover all nodes in the cluster
            nodes = self._discover_nodes()
            self.cluster_nodes = nodes
            
            # Build host-to-node mapping
            # For Cassandra, the node IP is the identifier
            host_node_mapping = {}
            for node_ip in nodes:
                host_node_mapping[node_ip] = node_ip
            
            # Use mixin's mapping method
            self.map_ssh_hosts_to_nodes(host_node_mapping)
                    
        except Exception as e:
            logger.warning(f"Could not map SSH hosts to Cassandra nodes: {e}")

    def _discover_nodes(self):
        """
        Discover all nodes in the cluster using the driver's metadata API.

        This is more reliable than querying system tables because the driver
        round-robins queries across contact points, which can lead to
        inconsistent results when querying system.local and system.peers_v2.

        Returns:
            list[str]: List of node IP addresses in the cluster
        """
        nodes = []

        try:
            # Use Cassandra driver's cluster metadata (more reliable than system tables)
            # The metadata is already populated when we connect
            if self.cluster and self.cluster.metadata:
                all_hosts = self.cluster.metadata.all_hosts()
                for host in all_hosts:
                    # host.address is the broadcast_address or rpc_address
                    nodes.append(str(host.address))

                logger.info(f"Discovered {len(nodes)} nodes via cluster metadata: {nodes}")
            else:
                logger.warning("Cluster metadata not available, falling back to system tables")

                # Fallback to system tables (less reliable due to round-robin)
                local = self.session.execute("SELECT broadcast_address, listen_address FROM system.local")
                local_row = list(local)

                if local_row:
                    local_addr = (
                        local_row[0].get('broadcast_address') or
                        local_row[0].get('listen_address')
                    )
                    if local_addr:
                        nodes.append(str(local_addr))

                # Get peer nodes
                major_version = self.version_info.get('major_version', 0)
                if major_version >= 4:
                    peers = self.session.execute("SELECT peer FROM system.peers_v2")
                else:
                    peers = self.session.execute("SELECT peer FROM system.peers")

                for peer in list(peers):
                    peer_addr = peer.get('peer')
                    if peer_addr:
                        nodes.append(str(peer_addr))

                # Remove duplicates
                nodes = list(dict.fromkeys(nodes))
                logger.info(f"Discovered {len(nodes)} nodes via system tables: {nodes}")

        except Exception as e:
            logger.error(f"Failed to discover cluster nodes: {e}")
            import traceback
            logger.error(traceback.format_exc())

        return nodes

    def _get_version_info(self):
        """Fetches Cassandra version via CQL."""
        try:
            rows = self.session.execute("SELECT release_version FROM system.local")
            version_string = list(rows)[0]['release_version'] if rows else 'Unknown'
            parts = version_string.split('.')
            major = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0

            return {
                'version_string': version_string,
                'major_version': major
            }
        except Exception as e:
            logger.warning(f"Could not fetch version: {e}")
            return {'version_string': 'Unknown', 'major_version': 0}

    def _detect_environment(self):
        """
        Detect the hosting environment (Instaclustr managed vs self-hosted)

        Detection logic:
        1. Instaclustr: If instaclustr_cluster_id is configured
        2. Self-hosted: Otherwise

        Updates self.environment and self.environment_details
        """
        try:
            # Check for Instaclustr configuration
            cluster_id = self.settings.get('instaclustr_cluster_id')
            api_username = self.settings.get('instaclustr_api_username')
            api_key = self.settings.get('instaclustr_api_key')

            if cluster_id:
                self.environment = 'instaclustr_managed'
                self.environment_details = {
                    'provider': 'Instaclustr',
                    'cluster_id': cluster_id,
                    'api_configured': bool(api_username and api_key),
                    'api_available': bool(api_username and api_key),
                    'ssh_available': self.has_ssh_support(),
                    'monitoring_via': 'API' if (api_username and api_key) else 'CQL only'
                }
                logger.info(f"Detected Instaclustr managed environment: {cluster_id}")
                return

            # Default to self-hosted
            self.environment = 'self_hosted'
            self.environment_details = {
                'ssh_available': self.has_ssh_support(),
                'node_count': len(self.cluster_nodes) if self.cluster_nodes else 0,
                'datacenter': self.settings.get('local_dc') or self.settings.get('datacenter', 'unknown')
            }
            logger.info("Detected self-hosted Cassandra environment")

        except Exception as e:
            logger.warning(f"Could not detect environment: {e}")
            self.environment = 'unknown'
            self.environment_details = {}

    def get_db_metadata(self):
        """Fetches basic database metadata including environment context."""
        keyspace = self.session.keyspace if self.session else 'system'
        return {
            'version': self.version_info.get('version_string', 'N/A'),
            'db_name': keyspace,
            'environment': self.environment or 'unknown',
            'environment_details': self.environment_details or {}
        }

    def execute_query(self, query, params=None, return_raw=False):
        """
        Executes a CQL query, nodetool command, or shell command based on query format.
        
        Supports three operation types:
        1. CQL queries (standard SQL strings)
        2. Nodetool commands: {"operation": "nodetool", "command": "status"}
        3. Shell commands: {"operation": "shell", "command": "df -h"}
        
        Args:
            query: CQL query string or JSON command
            params: Optional parameters for CQL query
            return_raw: If True, returns tuple (formatted, raw_data)
        
        Returns:
            str or tuple: Formatted AsciiDoc table, or (formatted, raw) if return_raw=True
        """
        try:
            # Check if the query is a JSON command for nodetool/shell
            if query.strip().startswith('{'):
                query_obj = json.loads(query)
                operation = query_obj.get('operation')
                command = query_obj.get('command')
                
                if not command:
                    raise ValueError("Operation requires a 'command' field")
                
                # Route to appropriate handler
                if operation == 'nodetool':
                    return self._execute_nodetool_command(command, return_raw)
                elif operation == 'nodetool_cluster':
                    return self._execute_nodetool_cluster_command(command, return_raw)
                elif operation == 'shell':
                    return self._execute_shell_command(command, return_raw)
                else:
                    raise ValueError(f"Unsupported operation: {operation}")
            
            # Otherwise, treat as CQL query
            return self._execute_cql_query(query, params, return_raw)
            
        except json.JSONDecodeError:
            # Not JSON, treat as CQL query
            return self._execute_cql_query(query, params, return_raw)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            error_msg = self.formatter.format_error(f"Query failed: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _execute_cql_query(self, query, params=None, return_raw=False):
        """Execute a standard CQL query."""
        try:
            if params:
                result = self.session.execute(query, params)
            else:
                result = self.session.execute(query)
            
            rows = list(result)
            
            if not rows:
                note = self.formatter.format_note("Query returned no results.")
                return (note, []) if return_raw else note
            
            formatted = self.formatter.format_table(rows)
            return (formatted, rows) if return_raw else formatted
            
        except Exception as e:
            logger.error(f"CQL query failed: {e}")
            error_msg = self.formatter.format_error(f"CQL query failed: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _execute_nodetool_command(self, command, return_raw=False):
        """
        Execute nodetool command on the primary SSH host.
        
        Args:
            command: Nodetool command to execute (e.g., 'status', 'info')
            return_raw: If True, returns tuple (formatted, parsed_data)
        
        Returns:
            str or tuple: Formatted output or (formatted, parsed) if return_raw=True
        """
        if not self.has_ssh_support():
            error_msg = self.formatter.format_error("SSH not configured - cannot execute nodetool commands")
            return (error_msg, {'error': 'SSH not configured'}) if return_raw else error_msg
        
        try:
            # Execute on primary SSH host (first host in the list)
            ssh_hosts = self.get_ssh_hosts()
            if not ssh_hosts:
                error_msg = self.formatter.format_error("No SSH hosts configured")
                return (error_msg, {'error': 'No SSH hosts'}) if return_raw else error_msg
            
            primary_host = ssh_hosts[0]
            ssh_manager = self.get_ssh_manager(primary_host)
            
            if not ssh_manager or not ssh_manager.is_connected():
                error_msg = self.formatter.format_error(f"No SSH connection available for {primary_host}")
                return (error_msg, {'error': 'No SSH connection'}) if return_raw else error_msg
            
            # Execute nodetool command
            stdout, stderr, exit_code = ssh_manager.execute_command(f"nodetool {command}")
            
            if exit_code != 0:
                error_msg = self.formatter.format_error(f"Nodetool command failed: {stderr}")
                return (error_msg, {'error': stderr}) if return_raw else error_msg
            
            if not stdout or not stdout.strip():
                note = self.formatter.format_note("Nodetool command returned no output.")
                return (note, []) if return_raw else note
            
            # Parse the output
            parsed_data = self.parser.parse(command, stdout)
            
            # Format the output
            formatted = self._format_nodetool_output(command, parsed_data)
            
            return (formatted, parsed_data) if return_raw else formatted
            
        except Exception as e:
            logger.error(f"Nodetool command failed: {e}")
            error_msg = self.formatter.format_error(f"Nodetool command failed: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _execute_nodetool_cluster_command(self, command, return_raw=False):
        """
        Execute nodetool command on all cluster nodes using mixin's SSH support.
        
        Args:
            command: Nodetool command to execute (e.g., 'status', 'tpstats')
            return_raw: If True, returns tuple (formatted, parsed_data)
        
        Returns:
            str or tuple: Formatted output or (formatted, parsed) if return_raw=True
        """
        if not self.has_ssh_support():
            error_msg = self.formatter.format_error("SSH not configured - cannot execute cluster-wide nodetool commands")
            return (error_msg, {'error': 'SSH not configured'}) if return_raw else error_msg
        
        # Discover nodes if not already done
        if not self.cluster_nodes:
            self.cluster_nodes = self._discover_nodes()
        
        if not self.cluster_nodes:
            error_msg = self.formatter.format_error("No nodes discovered in cluster")
            return (error_msg, {'error': 'No nodes discovered'}) if return_raw else error_msg
        
        results = {}
        
        for node_ip in self.cluster_nodes:
            try:
                # Get SSH manager for this node (from mixin's ssh_managers dict)
                ssh_manager = self.get_ssh_manager(node_ip)
                
                if not ssh_manager or not ssh_manager.is_connected():
                    results[node_ip] = {
                        'success': False,
                        'error': 'SSH connection not available'
                    }
                    continue
                
                # Execute nodetool command
                stdout, stderr, exit_code = ssh_manager.execute_command(f"nodetool {command}")
                
                if exit_code != 0:
                    results[node_ip] = {
                        'success': False,
                        'error': stderr
                    }
                    continue
                
                if not stdout or not stdout.strip():
                    results[node_ip] = {
                        'success': False,
                        'error': 'Empty output from nodetool command'
                    }
                    continue
                
                # Parse the output
                parsed_data = self.parser.parse(command, stdout)
                
                results[node_ip] = {
                    'success': True,
                    'data': parsed_data
                }
                
            except Exception as e:
                logger.error(f"Failed to execute nodetool on {node_ip}: {e}")
                results[node_ip] = {
                    'success': False,
                    'error': str(e)
                }
        
        # Aggregate results
        all_nodes_data = []
        errors = []
        
        for node_ip, result in results.items():
            if result.get('success'):
                data = result['data']
                # Add node_ip to each row for identification
                if isinstance(data, list):
                    for row in data:
                        if isinstance(row, dict):
                            row['node'] = node_ip
                    all_nodes_data.extend(data)
                elif isinstance(data, dict):
                    # Handle compactionstats format
                    data['node'] = node_ip
                    all_nodes_data.append(data)
            else:
                errors.append(f"Node {node_ip}: {result.get('error', 'Unknown error')}")
        
        # Format output
        if errors:
            error_msg = "[WARNING]\n====\nSome nodes failed:\n" + "\n".join(errors) + "\n====\n\n"
        else:
            error_msg = ""
        
        if all_nodes_data:
            formatted = error_msg + self.formatter.format_table(all_nodes_data)
        else:
            formatted = error_msg + self.formatter.format_note("No data returned from any node.")
        
        return (formatted, all_nodes_data) if return_raw else formatted

    def _execute_shell_command(self, command, return_raw=False):
        """
        Execute shell command on the primary SSH host.
        
        Args:
            command: Shell command to execute
            return_raw: If True, returns tuple (formatted, raw_output)
        
        Returns:
            str or tuple: Formatted output or (formatted, raw) if return_raw=True
        """
        if not self.has_ssh_support():
            error_msg = self.formatter.format_error("SSH not configured - cannot execute shell commands")
            return (error_msg, {'error': 'SSH not configured'}) if return_raw else error_msg
        
        try:
            # Execute on primary SSH host
            ssh_hosts = self.get_ssh_hosts()
            if not ssh_hosts:
                error_msg = self.formatter.format_error("No SSH hosts configured")
                return (error_msg, {'error': 'No SSH hosts'}) if return_raw else error_msg
            
            primary_host = ssh_hosts[0]
            ssh_manager = self.get_ssh_manager(primary_host)
            
            if not ssh_manager or not ssh_manager.is_connected():
                error_msg = self.formatter.format_error(f"No SSH connection available for {primary_host}")
                return (error_msg, {'error': 'No SSH connection'}) if return_raw else error_msg
            
            # Execute shell command
            stdout, stderr, exit_code = ssh_manager.execute_command(command)
            
            if exit_code != 0:
                error_msg = self.formatter.format_error(f"Shell command failed: {stderr}")
                return (error_msg, {'error': stderr, 'stdout': stdout}) if return_raw else error_msg
            
            if not stdout or not stdout.strip():
                note = self.formatter.format_note("Shell command returned no output.")
                return (note, {'stdout': '', 'stderr': stderr}) if return_raw else note
            
            # Format as code block
            formatted = f"[source,bash]\n----\n{stdout}\n----"
            
            raw_output = {
                'stdout': stdout,
                'stderr': stderr,
                'exit_code': exit_code
            }
            
            return (formatted, raw_output) if return_raw else formatted
            
        except Exception as e:
            logger.error(f"Shell command failed: {e}")
            error_msg = self.formatter.format_error(f"Shell command failed: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _format_nodetool_output(self, command, parsed_data):
        """
        Format nodetool command output for display.
        
        Special formatting for certain commands like gcstats.
        """
        # Special handling for gcstats
        if command == 'gcstats':
            def format_value(val):
                if val is None or val == '':
                    return 'N/A'
                return str(val)
            
            output = [
                "[cols=\"1,1\"]",
                "|===",
                "|Metric|Value",
                "",
                f"|Interval (ms)|{format_value(parsed_data.get('interval_ms'))}",
                f"|Max GC Elapsed (ms)|{format_value(parsed_data.get('max_gc_elapsed_ms'))}",
                f"|Total GC Elapsed (ms)|{format_value(parsed_data.get('total_gc_elapsed_ms'))}",
                f"|Stdev GC Elapsed (ms)|{format_value(parsed_data.get('stdev_gc_elapsed_ms'))}",
                f"|GC Reclaimed (MB)|{format_value(parsed_data.get('gc_reclaimed_mb'))}",
                f"|Collections|{format_value(parsed_data.get('collections'))}",
                f"|Direct Memory Bytes|{format_value(parsed_data.get('direct_memory_bytes'))}",
                "|==="
            ]
            
            return '\n'.join(output)
        
        else:
            # For other commands that return list[dict], use standard table format
            if isinstance(parsed_data, list):
                return self.formatter.format_table(parsed_data)
            else:
                return self.formatter.format_note(str(parsed_data))

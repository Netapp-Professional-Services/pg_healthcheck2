"""
ClickHouse connector implementation with multi-node SSH support.

Provides unified interface for:
1. SQL queries (via clickhouse-connect client)
2. Shell commands (via SSH for system-level metrics)
3. Multi-node cluster operations
"""

import logging
import clickhouse_connect
from typing import Dict, List, Optional, Any

# Import shared utilities
from plugins.common.ssh_mixin import SSHSupportMixin
from plugins.common.output_formatters import AsciiDocFormatter

logger = logging.getLogger(__name__)


class ClickHouseConnector(SSHSupportMixin):
    """
    Connector for ClickHouse clusters with multi-node SSH support.

    Query Formats:
        SQL Query: Standard SQL string
            SELECT * FROM system.metrics

        Shell command (via SSH):
            {"operation": "shell", "command": "df -h"}

    SSH Configuration (optional, for OS-level checks):
        - ssh_hosts: List of hostnames/IPs of ClickHouse nodes
        - ssh_user: SSH username
        - ssh_key_file: Path to private key (or ssh_password)
        - ssh_timeout: Connection timeout in seconds (default: 10)

    Example:
        connector = ClickHouseConnector(settings)
        connector.connect()

        # SQL query
        result = connector.execute_query("SELECT * FROM system.metrics")

        # Shell command
        result = connector.execute_query('{"operation": "shell", "command": "free -m"}')
    """

    def __init__(self, settings):
        """Initialize ClickHouse connector."""
        self.settings = settings
        self.client = None
        self._version_info = {}
        self.formatter = AsciiDocFormatter()

        # Multi-node support
        self.cluster_topology = []  # List of discovered cluster nodes
        self.cluster_name = None

        # Environment detection
        self.environment = None  # 'self_hosted', 'cloud', etc.
        self.environment_details = {}

        # Initialize SSH support (from mixin)
        self.initialize_ssh()

        logger.info("ClickHouse connector initialized")

    def _parse_connection_params(self):
        """
        Parse connection parameters from settings.

        Handles both modern (hosts, http_port, protocol) and legacy (host, port) formats.

        Returns:
            dict: Parameters for clickhouse_connect.get_client()
        """
        # Get host - support both 'hosts' (list) and 'host' (string)
        hosts = self.settings.get('hosts', [])
        if not hosts:
            # Fallback to legacy 'host' setting
            host = self.settings.get('host', 'localhost')
        else:
            # Use first host from list
            host = hosts[0] if isinstance(hosts, list) else hosts

        # Get protocol and security settings
        protocol = self.settings.get('protocol', 'http').lower()
        secure = self.settings.get('secure', False)

        # Determine port based on protocol
        if protocol == 'native':
            # Native protocol
            port = self.settings.get('native_port', 9440 if secure else 9000)
            interface = 'native'
        else:
            # HTTP protocol (default)
            port = self.settings.get('http_port', 8443 if secure else 8123)
            interface = 'https' if secure else 'http'

        # Legacy 'port' setting overrides protocol-specific ports
        if 'port' in self.settings:
            port = self.settings.get('port')

        # Build connection parameters
        params = {
            'host': host,
            'port': port,
            'username': self.settings.get('user', 'default'),
            'password': self.settings.get('password', ''),
            'database': self.settings.get('database', 'default'),
            'interface': interface,
            'secure': secure,
            'connect_timeout': self.settings.get('connection_timeout', 10),
            'send_receive_timeout': self.settings.get('request_timeout', 30),
            'client_name': self.settings.get('client_name', 'pg_healthcheck2')
        }

        # Add compression if configured
        if self.settings.get('compression', True):
            params['compress'] = True

        logger.info(f"Connecting to ClickHouse: {protocol}://{host}:{port} (secure={secure})")
        return params

    def _detect_environment(self):
        """
        Detect ClickHouse environment type.

        Returns:
            tuple: (environment_type, details_dict)
        """
        try:
            # Check for cloud provider indicators in host
            hosts = self.settings.get('hosts', [])
            host = hosts[0] if hosts else self.settings.get('host', '')

            if 'clickhouse.cloud' in host or 'altinity.cloud' in host:
                details = {
                    'type': 'cloud',
                    'provider': 'clickhouse_cloud' if 'clickhouse.cloud' in host else 'altinity',
                    'endpoint': host
                }
                logger.info("Detected ClickHouse Cloud environment")
                return 'cloud', details
            elif 'instaclustr.com' in host or 'cnodes.io' in host:
                # Instaclustr managed ClickHouse
                details = {
                    'type': 'managed',
                    'provider': 'instaclustr',
                    'endpoint': host
                }
                logger.info("Detected Instaclustr managed ClickHouse")
                return 'managed', details
            else:
                # Self-hosted ClickHouse
                details = {
                    'type': 'self_hosted',
                    'host': host
                }
                logger.info("Detected self-hosted ClickHouse environment")
                return 'self_hosted', details

        except Exception as e:
            logger.warning(f"Could not detect environment: {e}")
            return 'unknown', {}

    def connect(self):
        """Establishes connection to ClickHouse and all SSH hosts."""
        try:
            # 1. Detect environment
            self.environment, self.environment_details = self._detect_environment()

            # 2. Parse connection parameters from settings
            connection_params = self._parse_connection_params()

            # 3. Connect to ClickHouse
            self.client = clickhouse_connect.get_client(**connection_params)

            # 3. Get version info
            self._version_info = self._get_version_info()

            # 4. Discover cluster topology
            self._discover_cluster_topology()

            # 5. Connect all SSH hosts if configured (self-hosted only)
            connected_ssh_hosts = []
            if self.environment == 'self_hosted' and self.has_ssh_support():
                connected_ssh_hosts = self.connect_all_ssh()
                if connected_ssh_hosts:
                    self._map_ssh_hosts_to_nodes()

            # 6. Display enhanced connection status
            self._display_connection_status(connected_ssh_hosts)

            logger.info("✅ Connected to ClickHouse cluster")

        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise ConnectionError(f"Could not connect to ClickHouse: {e}")

    def _get_version_info(self):
        """
        Retrieves and parses ClickHouse version information.

        Returns:
            dict: Version info with keys:
                - version: Full version string (e.g., "25.3.6.56")
                - hostname: Server hostname
                - major_version: Major version number (e.g., 25)
                - minor_version: Minor version number (e.g., 3)
                - version_tuple: Tuple of version numbers (e.g., (25, 3, 6, 56))
        """
        try:
            result = self.client.query("SELECT version() as version, hostName() as hostname")
            if result.result_rows:
                row = result.result_rows[0]
                version_str = row[0]

                # Parse version string (e.g., "25.3.6.56" -> (25, 3, 6, 56))
                version_parts = version_str.split('.')
                version_tuple = tuple(int(p) for p in version_parts if p.isdigit())

                version_info = {
                    'version': version_str,
                    'hostname': row[1],
                    'major_version': version_tuple[0] if len(version_tuple) > 0 else 0,
                    'minor_version': version_tuple[1] if len(version_tuple) > 1 else 0,
                    'version_tuple': version_tuple
                }

                logger.info(f"ClickHouse version: {version_str} (major: {version_info['major_version']})")
                return version_info
            return {}
        except Exception as e:
            logger.warning(f"Could not get version info: {e}")
            return {}

    def _discover_cluster_topology(self):
        """Discovers all nodes in the ClickHouse cluster from system.clusters."""
        try:
            # Get cluster information
            query = """
            SELECT
                cluster,
                shard_num,
                replica_num,
                host_name,
                host_address,
                port,
                is_local
            FROM system.clusters
            ORDER BY cluster, shard_num, replica_num
            """
            result = self.client.query(query)

            self.cluster_topology = []
            for row in result.result_rows:
                node_info = {
                    'cluster': row[0],
                    'shard_num': row[1],
                    'replica_num': row[2],
                    'host_name': row[3],
                    'host_address': row[4],
                    'port': row[5],
                    'is_local': row[6]
                }
                self.cluster_topology.append(node_info)

            if self.cluster_topology:
                # Set cluster name from first entry
                self.cluster_name = self.cluster_topology[0]['cluster']
                logger.debug(f"Discovered {len(self.cluster_topology)} nodes in cluster")
            else:
                logger.info("No cluster configuration found (standalone instance)")

        except Exception as e:
            logger.warning(f"Could not discover cluster topology: {e}")
            self.cluster_topology = []

    def _map_ssh_hosts_to_nodes(self):
        """Maps SSH hosts to ClickHouse node names/IPs."""
        try:
            # Build host-to-node mapping
            host_node_mapping = {}
            for node in self.cluster_topology:
                node_address = node['host_address']
                node_hostname = node['host_name']
                # Map both address and hostname to the node
                host_node_mapping[node_address] = node_hostname
                host_node_mapping[node_hostname] = node_hostname

            # Update SSH manager with node mapping
            for ssh_host in self.ssh_managers.keys():
                if ssh_host in host_node_mapping:
                    logger.debug(f"Mapped SSH host {ssh_host} to node {host_node_mapping[ssh_host]}")

        except Exception as e:
            logger.warning(f"Could not map SSH hosts to nodes: {e}")

    def _display_connection_status(self, connected_ssh_hosts: List[str]):
        """Displays enhanced connection status."""
        print(f"\n✅ Successfully connected to ClickHouse")
        print(f"   - Version: {self._version_info.get('version', 'Unknown')}")
        print(f"   - Hostname: {self._version_info.get('hostname', 'Unknown')}")
        print(f"   - Environment: {self.environment}")

        if self.cluster_topology:
            print(f"   - Cluster: {self.cluster_name}")
            print(f"   - Nodes: {len(self.cluster_topology)} nodes discovered")

        if connected_ssh_hosts:
            print(f"   - SSH: Connected to {len(connected_ssh_hosts)} host(s)")
            for host in connected_ssh_hosts[:3]:  # Show first 3
                print(f"     • {host}")
            if len(connected_ssh_hosts) > 3:
                print(f"     ... and {len(connected_ssh_hosts) - 3} more")

    def disconnect(self):
        """Closes all connections."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from ClickHouse")

        # Disconnect all SSH connections (from mixin)
        self.disconnect_all_ssh()

    def execute_query(self, query, params=None):
        """
        Executes a query and returns raw results.

        This method does NOT format results - formatting should be done
        by CheckContentBuilder in check modules.

        Args:
            query: SQL string or dict for special operations
            params: Query parameters (optional)

        Returns:
            list: List of tuples (raw query results)
                 For shell operations: list with single tuple containing result
        """
        try:
            # Handle special operations (JSON-formatted commands)
            if isinstance(query, dict) or (isinstance(query, str) and query.strip().startswith('{')):
                return self._execute_special_operation(query)

            # Standard SQL query
            result = self.client.query(query, parameters=params)
            return result.result_rows  # Returns list of tuples

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Failed query: {query}")
            raise

    def _execute_special_operation(self, operation):
        """
        Executes special operations like shell commands.

        Args:
            operation: Dict or JSON string with operation details

        Returns:
            list: Results as list of tuples
        """
        import json

        # Parse operation if it's a string
        if isinstance(operation, str):
            try:
                operation = json.loads(operation)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid operation format: {e}")

        op_type = operation.get('operation', '').lower()

        if op_type == 'shell':
            # Execute shell command via SSH
            command = operation.get('command')
            if not command:
                raise ValueError("Shell operation requires 'command' field")

            # Execute on all SSH hosts or specific host
            target_host = operation.get('host')

            if target_host:
                # Single host
                result = self.execute_ssh_command(target_host, command, f"Shell: {command}")
                return [(target_host, result.get('status'), result.get('output', ''))]
            else:
                # All hosts
                results = []
                for host in self.ssh_managers.keys():
                    result = self.execute_ssh_command(host, command, f"Shell: {command}")
                    results.append((host, result.get('status'), result.get('output', '')))
                return results

        else:
            raise ValueError(f"Unknown operation type: {op_type}")

    @property
    def version_info(self):
        """Returns version information for version-aware queries."""
        return self._version_info

    def get_db_metadata(self):
        """Returns database metadata."""
        return {
            'version': self._version_info.get('version'),
            'hostname': self._version_info.get('hostname'),
            'cluster_name': self.cluster_name,
            'environment': self.environment,
            'nodes': len(self.cluster_topology)
        }

    def has_ssh_support(self):
        """Check if SSH support is configured and available."""
        # Inherited from SSHSupportMixin
        return bool(self.ssh_managers)

    def get_cluster_nodes(self):
        """Returns list of cluster nodes from topology."""
        return self.cluster_topology

    def get_current_timestamp(self):
        """
        Returns current timestamp in ISO 8601 format for metadata.

        Returns:
            str: ISO 8601 formatted timestamp
        """
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

"""
Instacollector Connector for Cassandra diagnostic imports.

This connector implements the same interface as CassandraConnector but reads
from pre-collected Instacollector data instead of making live connections.
This allows all existing health checks to work unchanged against imported data.

Usage:
    from plugins.cassandra.instacollector_connector import InstacollectorConnector

    connector = InstacollectorConnector(settings, '/path/to/instacollector-export')
    connector.connect()

    # Use exactly like CassandraConnector
    status = connector.execute_query('{"operation": "nodetool", "command": "status"}')
    info = connector.execute_query('{"operation": "nodetool", "command": "info"}')

    connector.disconnect()
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from plugins.cassandra.instacollector_loader import InstacollectorLoader
from plugins.common.output_formatters import AsciiDocFormatter
from plugins.common.parsers import NodetoolParser
from plugins.common.cve_mixin import CVECheckMixin

logger = logging.getLogger(__name__)


class InstacollectorConnector(CVECheckMixin):
    """
    Connector that reads from Instacollector exports.

    Implements the same interface as CassandraConnector so existing
    health checks work without modification. Maps execute_query operations
    to the appropriate diagnostic files.
    """

    # Map nodetool commands to loader methods
    NODETOOL_COMMANDS = [
        'status',
        'info',
        'cfstats',
        'tablestats',
        'compactionstats',
        'describecluster',
        'gossipinfo',
        'ring',
        'tpstats',
        'gcstats',
        'version',
        'tablehistograms',
    ]

    # Commands that are cluster-wide (same output from any node)
    CLUSTER_WIDE_COMMANDS = ['status', 'describecluster', 'ring']

    def __init__(self, settings: Dict[str, Any], diagnostic_path: str):
        """
        Initialize the Instacollector connector.

        Args:
            settings: Configuration settings (company_name, etc.)
            diagnostic_path: Path to Instacollector export (directory or tarball)
        """
        self.settings = settings
        self.diagnostic_path = diagnostic_path
        self.loader: Optional[InstacollectorLoader] = None
        self._version_info: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] = {}
        self.cluster_name: Optional[str] = None
        self.cluster_nodes: List[str] = []
        self.formatter = AsciiDocFormatter()
        self.parser = NodetoolParser()

        # Environment info
        self.environment = 'imported'
        self.environment_details = {
            'type': 'instacollector-import',
            'source_path': diagnostic_path
        }

        # Technology name for CVE lookups
        self.technology_name = 'cassandra'

        logger.info(f"InstacollectorConnector initialized for: {diagnostic_path}")

    def connect(self):
        """
        Load the diagnostic data.

        This is analogous to connecting to a live cluster - we load
        all the diagnostic files into memory.
        """
        try:
            self.loader = InstacollectorLoader(self.diagnostic_path)
            if not self.loader.load():
                raise ConnectionError(f"Failed to load Instacollector data from {self.diagnostic_path}")

            # Extract metadata
            self._metadata = self.loader.get_metadata()
            self.cluster_name = self._metadata.get('cluster_name', 'Unknown')
            self.cluster_nodes = self._metadata.get('nodes', [])

            # Build version info
            version_string = self._metadata.get('version', 'Unknown')
            major_version = 0
            if version_string != 'Unknown':
                parts = version_string.split('.')
                if parts and parts[0].isdigit():
                    major_version = int(parts[0])

            self._version_info = {
                'version_string': version_string,
                'major_version': major_version,
            }

            # Initialize CVE support
            self.initialize_cve_support()

            # Display connection status
            self._display_connection_status()

            logger.info("Successfully loaded Instacollector data")

        except Exception as e:
            logger.error(f"Failed to load Instacollector data: {e}")
            raise ConnectionError(f"Could not load Instacollector data: {e}")

    def _display_connection_status(self):
        """Display status of loaded diagnostic data."""
        print("✅ Successfully loaded Instacollector export")
        print(f"   - Source: {self.diagnostic_path}")
        print(f"   - Collection Date: {self._metadata.get('collection_date', 'Unknown')}")
        print(f"   - Version: {self._version_info.get('version_string', 'Unknown')}")
        print(f"   - Cluster: {self.cluster_name}")
        print(f"   - Datacenter: {self._metadata.get('datacenter', 'Unknown')}")
        print(f"   - Nodes: {len(self.cluster_nodes)}")

        if self.cluster_nodes:
            print(f"   - Node IPs:")
            for node in self.cluster_nodes[:5]:
                print(f"      • {node}")
            if len(self.cluster_nodes) > 5:
                print(f"      ... and {len(self.cluster_nodes) - 5} more")

        # Show available files
        files = self.loader.list_files()
        print(f"   - Files Loaded: {len(files)}")

    def disconnect(self):
        """Clean up resources."""
        if self.loader:
            self.loader.cleanup()
            self.loader = None
        logger.info("Instacollector connector closed")

    def close(self):
        """Alias for disconnect()."""
        self.disconnect()

    @property
    def version_info(self) -> Dict[str, Any]:
        """Returns version information."""
        return self._version_info

    def get_db_metadata(self) -> Dict[str, Any]:
        """
        Get cluster metadata for trends database.

        Returns:
            dict with version, cluster name, environment info
        """
        return {
            'version': self._version_info.get('version_string', 'Unknown'),
            'db_name': self.cluster_name or 'Unknown',
            'environment': 'imported',
            'environment_details': {
                **self.environment_details,
                'collection_date': self._metadata.get('collection_date'),
                'datacenter': self._metadata.get('datacenter'),
                'node_count': len(self.cluster_nodes),
            }
        }

    def execute_query(self, query, params=None, return_raw=False) -> Any:
        """
        Execute a query by reading from diagnostic files.

        Supports the same operation types as CassandraConnector:
        - Nodetool commands: {"operation": "nodetool", "command": "status"}
        - Cluster-wide nodetool: {"operation": "nodetool_cluster", "command": "tpstats"}
        - Shell commands: {"operation": "shell", "command": "df -h"}

        CQL queries are NOT supported for imported data.

        Args:
            query: JSON string or dict with operation details
            params: Additional parameters (optional)
            return_raw: If True, return tuple (formatted, raw_data)

        Returns:
            Query results (format depends on operation)
        """
        if not self.loader:
            error_msg = "Instacollector data not loaded. Call connect() first."
            return (self.formatter.format_error(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_error(error_msg)

        try:
            # Check if the query is a JSON command
            if isinstance(query, str) and query.strip().startswith('{'):
                query_obj = json.loads(query)
            elif isinstance(query, dict):
                query_obj = query
            else:
                # CQL queries are not supported for imported data
                error_msg = "CQL queries not supported for imported Instacollector data"
                return (self.formatter.format_error(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_error(error_msg)

            operation = query_obj.get('operation')
            command = query_obj.get('command')

            if operation == 'nodetool':
                return self._execute_nodetool_command(command, return_raw)
            elif operation == 'nodetool_cluster':
                return self._execute_nodetool_cluster_command(command, return_raw)
            elif operation == 'shell':
                return self._execute_shell_command(command, return_raw)
            else:
                error_msg = f"Unsupported operation: {operation}"
                return (self.formatter.format_error(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_error(error_msg)

        except json.JSONDecodeError:
            error_msg = "CQL queries not supported for imported Instacollector data"
            return (self.formatter.format_error(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_error(error_msg)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            error_msg = f"Query failed: {e}"
            return (self.formatter.format_error(error_msg), {'error': str(e)}) if return_raw else self.formatter.format_error(error_msg)

    def _execute_nodetool_command(self, command: str, return_raw: bool = False) -> Any:
        """
        Execute nodetool command by reading from loaded files.

        For cluster-wide commands, returns data from any available node.
        For node-specific commands, returns data from the first available node.

        Args:
            command: Nodetool command name (e.g., 'status', 'info')
            return_raw: If True, return tuple (formatted, parsed_data)

        Returns:
            Formatted output or (formatted, parsed) if return_raw=True
        """
        # Handle cfstats/tablestats aliasing
        actual_command = command
        if command == 'tablestats':
            actual_command = 'cfstats'

        # Get data from loader
        parsed_data = self.loader.get_nodetool_output(actual_command)

        if parsed_data is None:
            note = self.formatter.format_note(f"No data available for nodetool {command}")
            return (note, None) if return_raw else note

        # Format the output
        formatted = self._format_nodetool_output(command, parsed_data)

        return (formatted, parsed_data) if return_raw else formatted

    def _execute_nodetool_cluster_command(self, command: str, return_raw: bool = False) -> Any:
        """
        Execute nodetool command across all nodes.

        Returns aggregated data from all nodes in the collection.

        Args:
            command: Nodetool command name
            return_raw: If True, return tuple (formatted, parsed_data)

        Returns:
            Formatted output or (formatted, parsed) if return_raw=True
        """
        # Handle cfstats/tablestats aliasing
        actual_command = command
        if command == 'tablestats':
            actual_command = 'cfstats'

        # Get aggregated data from all nodes
        # Pass node=None to aggregate
        all_data = []

        for node_ip in self.cluster_nodes:
            node_data = self.loader.get_nodetool_output(actual_command, node=node_ip)
            if node_data:
                if isinstance(node_data, list):
                    for item in node_data:
                        if isinstance(item, dict):
                            item['node'] = node_ip
                        all_data.append(item)
                elif isinstance(node_data, dict):
                    node_data['node'] = node_ip
                    all_data.append(node_data)

        if not all_data:
            note = self.formatter.format_note(f"No data available for nodetool {command} on any node")
            return (note, []) if return_raw else note

        # Format as table
        formatted = self.formatter.format_table(all_data)

        return (formatted, all_data) if return_raw else formatted

    def _execute_shell_command(self, command: str, return_raw: bool = False) -> Any:
        """
        Execute shell command by reading from loaded system info.

        Supports common commands that map to collected files:
        - df -h: Returns disk.info
        - iostat: Returns io_stat.info

        Args:
            command: Shell command string
            return_raw: If True, return tuple (formatted, raw_data)

        Returns:
            Formatted output or (formatted, raw) if return_raw=True
        """
        # Map common commands to info files
        command_lower = command.lower().strip()

        if command_lower.startswith('df'):
            # Return disk info
            disk_info = self.loader.get_system_info('disk')
            if disk_info:
                # Get from first node
                first_node = list(disk_info.keys())[0]
                data = disk_info[first_node]

                # Format df output
                if 'df_output' in data:
                    formatted = self.formatter.format_table(data['df_output'])
                    raw_data = {
                        'stdout': data.get('raw', ''),
                        'df_parsed': data['df_output'],
                        'exit_code': 0
                    }
                    return (formatted, raw_data) if return_raw else formatted

                # Return raw content
                raw = data.get('raw', '')
                formatted = f"[source,bash]\n----\n{raw}\n----"
                return (formatted, {'stdout': raw, 'exit_code': 0}) if return_raw else formatted

        elif 'iostat' in command_lower:
            # Return iostat info
            raw = self.loader.get_raw_file('io_stat.info')
            if raw:
                formatted = f"[source,bash]\n----\n{raw}\n----"
                return (formatted, {'stdout': raw, 'exit_code': 0}) if return_raw else formatted

        # Command not available in collected data
        error_msg = f"Shell command '{command}' not available in Instacollector data"
        return (self.formatter.format_note(error_msg), {'error': error_msg, 'stdout': ''}) if return_raw else self.formatter.format_note(error_msg)

    def _format_nodetool_output(self, command: str, parsed_data: Any) -> str:
        """
        Format nodetool command output for display.

        Args:
            command: Nodetool command name
            parsed_data: Parsed data from loader

        Returns:
            Formatted AsciiDoc string
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

        # Special handling for info
        elif command == 'info':
            if isinstance(parsed_data, dict):
                # Format as key-value table
                rows = []
                for key, value in parsed_data.items():
                    if not key.startswith('_'):  # Skip internal fields
                        rows.append({'metric': key, 'value': str(value)})
                return self.formatter.format_table(rows)

        # Special handling for compactionstats
        elif command == 'compactionstats':
            if isinstance(parsed_data, dict):
                pending = parsed_data.get('pending_tasks', 0)
                active = parsed_data.get('active_compactions', [])

                output = [f"Pending tasks: {pending}"]
                if active:
                    output.append("")
                    output.append(self.formatter.format_table(active))
                return '\n'.join(output)

        # Special handling for describecluster
        elif command == 'describecluster':
            if isinstance(parsed_data, dict):
                output = [
                    f"Cluster Name: {parsed_data.get('name', 'Unknown')}",
                    f"Snitch: {parsed_data.get('snitch', 'Unknown')}",
                    f"Partitioner: {parsed_data.get('partitioner', 'Unknown')}",
                    "",
                    "Schema versions:"
                ]
                for sv in parsed_data.get('schema_versions', []):
                    version = sv.get('version', 'Unknown')
                    endpoints = ', '.join(sv.get('endpoints', []))
                    output.append(f"  {version}: [{endpoints}]")
                return '\n'.join(output)

        # Default: format as table if list, or as-is for other types
        if isinstance(parsed_data, list):
            return self.formatter.format_table(parsed_data)
        elif isinstance(parsed_data, dict):
            return self.formatter.format_note(str(parsed_data))
        else:
            return str(parsed_data)

    def has_ssh_support(self) -> bool:
        """SSH is not applicable for diagnostic imports."""
        return False

    def has_nodetool_data(self) -> bool:
        """
        Check if nodetool data is available from the loaded diagnostic files.

        This allows checks that normally require SSH for nodetool commands
        to proceed using the pre-loaded data instead.

        Returns:
            bool: True if nodetool data is loaded and available
        """
        if not self.loader:
            return False

        # Check if we have any nodetool output loaded
        # The loader stores parsed nodetool output in node_data['nodetool']
        for node_ip in self.cluster_nodes:
            nodetool_data = self.loader.get_nodetool_output('status', node=node_ip)
            if nodetool_data is not None:
                return True

        # Also check cluster-wide commands
        status_data = self.loader.get_nodetool_output('status')
        if status_data is not None:
            return True

        return False

    def get_ssh_hosts(self) -> List[str]:
        """
        Return list of node IPs from loaded data.

        For diagnostic imports, we return the loaded node IPs so checks
        can iterate over them, even though there's no actual SSH.
        """
        return self.cluster_nodes

    def get_ssh_manager(self, host: str) -> None:
        """Return None - SSH not applicable for diagnostic imports."""
        return None

    def execute_ssh_on_all_hosts(self, command: str, description: str = "SSH command") -> List[Dict]:
        """
        Simulate SSH command execution using loaded diagnostic data.

        For diagnostic imports, we map common shell commands to the
        pre-loaded data files and return results in the expected format.

        Args:
            command: Shell command that would have been executed
            description: Human-readable description for logging

        Returns:
            List of dicts with {host, node_id, success, output, stderr, exit_code}
        """
        results = []

        if not self.loader:
            return results

        # Map common commands to loaded data
        command_lower = command.lower().strip()

        for node_ip in self.cluster_nodes:
            result = {
                'host': node_ip,
                'node_id': node_ip,
                'success': False,
                'output': '',
                'stderr': '',
                'exit_code': 1
            }

            # Handle nodetool commands
            if 'nodetool' in command_lower:
                # Extract nodetool subcommand
                parts = command_lower.split('nodetool')
                if len(parts) > 1:
                    subcommand = parts[1].strip().split()[0] if parts[1].strip() else ''
                    raw_output = self.loader.get_raw_file(f'nodetool_{subcommand}.info', node=node_ip)
                    if raw_output:
                        result['success'] = True
                        result['output'] = raw_output
                        result['exit_code'] = 0

            # Handle df commands (disk info)
            elif command_lower.startswith('df') or 'df -' in command_lower:
                raw_output = self.loader.get_raw_file('disk.info', node=node_ip)
                if raw_output:
                    result['success'] = True
                    result['output'] = raw_output
                    result['exit_code'] = 0

            # Handle free commands (memory info) - may not be in Instacollector
            elif 'free' in command_lower:
                # Instacollector doesn't typically capture 'free' output
                result['success'] = False
                result['stderr'] = 'Memory info not available in Instacollector data'

            # Handle iostat
            elif 'iostat' in command_lower:
                raw_output = self.loader.get_raw_file('io_stat.info', node=node_ip)
                if raw_output:
                    result['success'] = True
                    result['output'] = raw_output
                    result['exit_code'] = 0

            # Handle cat/grep for cassandra.yaml
            elif 'cassandra.yaml' in command_lower:
                raw_output = self.loader.get_raw_file('cassandra.yaml', node=node_ip)
                if raw_output:
                    result['success'] = True
                    result['output'] = raw_output
                    result['exit_code'] = 0

            # Handle uptime/load average - extract from nodetool info if available
            elif 'uptime' in command_lower or 'loadavg' in command_lower:
                info_data = self.loader.get_nodetool_output('info', node=node_ip)
                if info_data and isinstance(info_data, dict):
                    uptime = info_data.get('uptime_seconds', 'N/A')
                    result['success'] = True
                    result['output'] = f"uptime_seconds: {uptime}"
                    result['exit_code'] = 0

            # Handle GC log requests
            elif 'gc.log' in command_lower:
                # Try to find GC log files
                gc_content = None
                for suffix in ['0', '1', '2', '3.current', '']:
                    filename = f'gc.log.{suffix}' if suffix else 'gc.log'
                    gc_content = self.loader.get_raw_file(filename, node=node_ip)
                    if gc_content:
                        break
                if gc_content:
                    result['success'] = True
                    result['output'] = gc_content
                    result['exit_code'] = 0
                else:
                    result['output'] = 'GC_LOG_NOT_FOUND'

            # Handle jvm.options
            elif 'jvm.options' in command_lower:
                raw_output = self.loader.get_raw_file('jvm.options', node=node_ip)
                if raw_output:
                    result['success'] = True
                    result['output'] = raw_output
                    result['exit_code'] = 0

            # Handle cassandra-env.sh
            elif 'cassandra-env.sh' in command_lower or 'cassandra-env' in command_lower:
                raw_output = self.loader.get_raw_file('cassandra-env.sh', node=node_ip)
                if raw_output:
                    result['success'] = True
                    result['output'] = raw_output
                    result['exit_code'] = 0

            # Handle system.log
            elif 'system.log' in command_lower:
                raw_output = self.loader.get_raw_file('system.log', node=node_ip)
                if raw_output:
                    result['success'] = True
                    result['output'] = raw_output
                    result['exit_code'] = 0

            # Handle io_stat.info directly (for fallback)
            elif 'io_stat.info' in command_lower:
                raw_output = self.loader.get_raw_file('io_stat.info', node=node_ip)
                if raw_output:
                    result['success'] = True
                    result['output'] = raw_output
                    result['exit_code'] = 0

            results.append(result)

        return results

    # Methods to support checks that need config access

    def get_config(self, filename: str = 'cassandra.yaml', node: Optional[str] = None) -> Optional[Any]:
        """
        Get configuration file content.

        Args:
            filename: Config filename (default: cassandra.yaml)
            node: Specific node IP, or None for first available

        Returns:
            Parsed config (dict for YAML) or raw content
        """
        if self.loader:
            return self.loader.get_config(filename, node)
        return None

    def get_cassandra_yaml(self, node: Optional[str] = None) -> Optional[Dict]:
        """
        Get cassandra.yaml configuration.

        Convenience method for accessing the main config file.

        Args:
            node: Specific node IP, or None for first available

        Returns:
            Parsed cassandra.yaml as dict
        """
        return self.get_config('cassandra.yaml', node)


def create_instacollector_connector(settings: Dict[str, Any], diagnostic_path: str) -> InstacollectorConnector:
    """
    Factory function to create an Instacollector connector.

    Args:
        settings: Configuration settings
        diagnostic_path: Path to Instacollector export

    Returns:
        Configured InstacollectorConnector instance
    """
    connector = InstacollectorConnector(settings, diagnostic_path)
    return connector

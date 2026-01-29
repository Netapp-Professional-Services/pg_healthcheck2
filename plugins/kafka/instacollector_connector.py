"""
Instacollector Connector for Kafka diagnostic imports.

This connector implements the SAME interface as KafkaConnector but reads
from pre-collected Instacollector data instead of making live connections.
This allows ALL existing health checks to work unchanged against imported data.

Design Pattern:
    Following the OpenSearch diagnostic connector pattern, this connector uses
    DECLARATIVE MAPPINGS to map operations and commands to diagnostic files.
    This is cleaner and more maintainable than imperative if/elif chains.

    OPERATION_MAP: Maps execute_query operations to handler methods
    COMMAND_FILE_MAP: Maps SSH commands to diagnostic files

Usage:
    from plugins.kafka.instacollector_connector import KafkaInstacollectorConnector

    connector = KafkaInstacollectorConnector(settings, '/path/to/instacollector-export')
    connector.connect()

    # Use EXACTLY like KafkaConnector - same queries, same return format
    result = connector.execute_query('{"operation": "describe_topics"}', return_raw=True)

    connector.disconnect()
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Callable

from plugins.kafka.instacollector_loader import KafkaInstacollectorLoader
from plugins.common.output_formatters import AsciiDocFormatter
from plugins.common.cve_mixin import CVECheckMixin

logger = logging.getLogger(__name__)


# =============================================================================
# DECLARATIVE MAPPINGS (OpenSearch Pattern)
# =============================================================================

# Maps SSH command patterns to diagnostic file names
# Key: regex pattern to match command, Value: file name or tuple (file_name, is_log_glob)
COMMAND_FILE_MAP = {
    # Memory commands
    r'free|meminfo': ('mem.info', False),
    # CPU commands
    r'cpuinfo|lscpu|nproc': ('cpu.info', False),
    # IO statistics
    r'iostat': ('io_stat.info', False),
    # File descriptors and limits
    r'ulimit|file_descriptor|limits': ('file_descriptor.info', False),
    # Hosts file
    r'/etc/hosts': ('hosts_file.info', False),
    # Broker configuration
    r'broker\.properties': ('broker.properties', False),
    r'server\.properties': ('server.properties', False),
    # Controller configuration (KRaft)
    r'controller\.properties': ('controller.properties', False),
    # GC logs
    r'gc\.log': ('kafkaServer-gc.log*', True),
    # Server logs
    r'server\.log': ('server.log*', True),
    # Controller logs
    r'controller\.log': ('controller.log*', True),
    # State change logs
    r'state-change\.log': ('state-change.log*', True),
    # Authorization logs
    r'authorizer|kafka-authorizer\.log': ('kafka-authorizer.log*', True),
}

# Commands that return empty/simulated responses
SIMULATED_COMMANDS = {
    r'^df\s|df\s+-': ('', 'Disk usage data not available in diagnostic export', 1),
    r'uptime': ('up 7 days', '', 0),
    r'pgrep|ps\s': ('', '', 0),
    r'lsof': ('', 'lsof data not available in diagnostic export', 1),
}


class KafkaInstacollectorConnector(CVECheckMixin):
    """
    Connector that reads from Kafka Instacollector exports.

    Implements the SAME interface as KafkaConnector so existing
    health checks work WITHOUT MODIFICATION. Uses declarative mappings
    to route operations to the appropriate diagnostic files.
    """

    # Maps execute_query operations to handler method names
    OPERATION_MAP = {
        'shell': '_execute_shell_command',
        'list_topics': '_list_topics',
        'admin_list_topics': '_list_all_topics',
        'describe_topics': '_describe_topics',
        'list_consumer_groups': '_list_consumer_groups',
        'describe_consumer_groups': '_describe_consumer_groups',
        'consumer_lag': '_handle_consumer_lag',
        'broker_config': '_get_broker_config',
        'cluster_metadata': '_get_cluster_metadata',
        'topic_config': '_get_topic_config',
        'describe_log_dirs': '_describe_log_dirs',
        'quorum_status': '_get_quorum_status',
        'quorum_replication': '_get_quorum_replication',
        'api_versions': '_get_api_versions',
    }

    def __init__(self, settings: Dict[str, Any], diagnostic_path: str):
        """
        Initialize the Instacollector connector.

        Args:
            settings: Configuration settings
            diagnostic_path: Path to Instacollector export
        """
        self.settings = settings
        self.diagnostic_path = diagnostic_path
        self.loader: Optional[KafkaInstacollectorLoader] = None
        self._version_info: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] = {}
        self.formatter = AsciiDocFormatter()

        # Cluster metadata cache
        self.cluster_metadata: Dict[str, Any] = {}
        self.cluster_nodes: List[str] = []

        # Kafka mode (kraft or zookeeper)
        self.kafka_mode: Optional[str] = None

        # Environment info
        self.environment = 'imported'
        self.environment_details = {
            'type': 'instacollector-import',
            'source_path': diagnostic_path
        }

        # Technology name for CVE lookups
        self.technology_name = 'kafka'

        # Metric collection strategy
        self.metric_collection_strategy = 'instacollector'
        self.metric_collection_details = {'source': 'diagnostic_files'}

        logger.info(f"KafkaInstacollectorConnector initialized for: {diagnostic_path}")

    def connect(self):
        """
        Load the diagnostic data.

        This is analogous to connecting to a live cluster.
        """
        try:
            self.loader = KafkaInstacollectorLoader(self.diagnostic_path)
            if not self.loader.load():
                raise ConnectionError(f"Failed to load Kafka Instacollector data from {self.diagnostic_path}")

            # Extract metadata
            self._metadata = self.loader.get_metadata()
            self.cluster_nodes = self._metadata.get('nodes', [])
            self.kafka_mode = self._metadata.get('kafka_mode', 'unknown')

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

            # Build cluster metadata cache
            self._build_cluster_metadata()

            # Initialize CVE support
            self.initialize_cve_support()

            # Display connection status
            self._display_connection_status()

            logger.info("Successfully loaded Kafka Instacollector data")

        except Exception as e:
            logger.error(f"Failed to load Kafka Instacollector data: {e}")
            raise ConnectionError(f"Could not load Kafka Instacollector data: {e}")

    def _build_cluster_metadata(self):
        """Build cluster metadata from loaded data."""
        quorum_status = self.loader.get_kafka_output('quorum_status')
        topics_data = self.loader.get_kafka_output('topics')
        consumer_data = self.loader.get_kafka_output('consumer_groups')

        topic_count = topics_data.get('topic_count', 0) if topics_data else 0
        group_count = consumer_data.get('group_count', 0) if consumer_data else 0
        broker_count = len(self.cluster_nodes)
        controller_id = quorum_status.get('leaderid', -1) if quorum_status else -1

        self.cluster_metadata = {
            'cluster_id': self._metadata.get('cluster_id', 'Unknown'),
            'broker_count': broker_count,
            'brokers': self.cluster_nodes,
            'controller_id': controller_id,
            'topic_count': topic_count,
            'consumer_group_count': group_count,
            'kafka_mode': self.kafka_mode,
        }

        if quorum_status:
            self.cluster_metadata['quorum'] = quorum_status

    def _display_connection_status(self):
        """Display status of loaded diagnostic data."""
        print("✅ Successfully loaded Kafka Instacollector export")
        print(f"   - Source: {self.diagnostic_path}")
        print(f"   - Collection Date: {self._metadata.get('collection_date', 'Unknown')}")
        print(f"   - Kafka Version: {self._version_info.get('version_string', 'Unknown')}")
        print(f"   - Mode: {self.kafka_mode.upper() if self.kafka_mode else 'Unknown'}")
        print(f"   - Cluster ID: {self._metadata.get('cluster_id', 'Unknown')}")
        print(f"   - Brokers: {len(self.cluster_nodes)}")

        if self.cluster_nodes:
            print(f"   - Broker IPs:")
            for node in self.cluster_nodes[:5]:
                print(f"      • {node}")
            if len(self.cluster_nodes) > 5:
                print(f"      ... and {len(self.cluster_nodes) - 5} more")

        print(f"   - Topics: {self.cluster_metadata.get('topic_count', 0)}")
        print(f"   - Consumer Groups: {self.cluster_metadata.get('consumer_group_count', 0)}")
        print(f"   - Files Loaded: {len(self.loader.list_files())}")

    def disconnect(self):
        """Clean up resources."""
        if self.loader:
            self.loader.cleanup()
            self.loader = None
        logger.info("Kafka Instacollector connector closed")

    def close(self):
        """Alias for disconnect()."""
        self.disconnect()

    @property
    def version_info(self) -> Dict[str, Any]:
        """Returns version information."""
        return self._version_info

    def get_db_metadata(self) -> Dict[str, Any]:
        """Get cluster metadata for trends database."""
        return {
            'version': self._version_info.get('version_string', 'Unknown'),
            'db_name': self._metadata.get('cluster_id', 'Unknown'),
            'environment': 'imported',
            'environment_details': {
                **self.environment_details,
                'collection_date': self._metadata.get('collection_date'),
                'kafka_mode': self.kafka_mode,
                'node_count': len(self.cluster_nodes),
            }
        }

    # =========================================================================
    # SSH Compatibility Methods (using declarative mappings)
    # =========================================================================

    def has_ssh_support(self) -> bool:
        """SSH is simulated for diagnostic imports."""
        return True

    def get_ssh_hosts(self) -> List[str]:
        """Return list of node IPs from loaded data."""
        return self.cluster_nodes

    def get_ssh_manager(self, host: str = None):
        """
        Get a simulated SSH manager for compatibility with checks.

        Args:
            host: Host to get manager for (optional)

        Returns:
            SimulatedSSHManager instance or None
        """
        if not self.loader or not self.cluster_nodes:
            return None

        target_host = host or (self.cluster_nodes[0] if self.cluster_nodes else None)
        if not target_host:
            return None

        return SimulatedSSHManager(self.loader, target_host)

    def execute_ssh_on_all_hosts(self, command: str, description: str = "SSH command") -> List[Dict]:
        """
        Simulate SSH command execution using declarative file mappings.

        Maps commands to pre-loaded diagnostic files using COMMAND_FILE_MAP
        and returns results in the same format as real SSH execution.

        Args:
            command: Shell command that would have been executed
            description: Human-readable description

        Returns:
            List of dicts with {host, node_id, success, output, stderr, exit_code}
        """
        results = []

        if not self.loader:
            return results

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

            # Check simulated commands first
            for pattern, response in SIMULATED_COMMANDS.items():
                if re.search(pattern, command_lower):
                    result['output'], result['stderr'], result['exit_code'] = response
                    result['success'] = result['exit_code'] == 0
                    break
            else:
                # Try to match against file mappings
                for pattern, file_info in COMMAND_FILE_MAP.items():
                    if re.search(pattern, command_lower):
                        filename, is_log_glob = file_info
                        output = self._get_file_content(filename, node_ip, is_log_glob)
                        if output:
                            result['success'] = True
                            result['output'] = output
                            result['exit_code'] = 0
                        break

            results.append(result)

        return results

    def _get_file_content(self, filename: str, node_ip: str, is_log_glob: bool = False) -> Optional[str]:
        """
        Get file content from loader.

        Args:
            filename: File name or glob pattern
            node_ip: Node IP address
            is_log_glob: If True, use get_log_files with glob pattern

        Returns:
            File content or None
        """
        if is_log_glob:
            logs = self.loader.get_log_files(filename, node=node_ip)
            if node_ip in logs and logs[node_ip]:
                try:
                    return logs[node_ip][0].read_text(errors='ignore')
                except Exception:
                    pass
            return None
        else:
            # Try direct file access
            content = self.loader.get_raw_file(filename, node=node_ip)
            if content:
                return content

            # Try alternate files for config
            if filename == 'broker.properties':
                return self.loader.get_raw_file('server.properties', node=node_ip)

            return None

    # =========================================================================
    # execute_query - Main Interface (using declarative operation mapping)
    # =========================================================================

    def execute_query(self, query, params=None, return_raw=False) -> Any:
        """
        Execute Kafka Admin API operations using declarative operation mapping.

        Routes operations to handler methods via OPERATION_MAP dictionary.
        Returns data in the EXACT SAME FORMAT as the live KafkaConnector.

        Args:
            query: JSON string or dict with operation details
            params: Additional parameters (unused, for API compatibility)
            return_raw: If True, returns (formatted, raw_data)

        Returns:
            str or tuple: Formatted results, same format as live connector
        """
        if not self.loader:
            error_msg = "Instacollector data not loaded. Call connect() first."
            return (self.formatter.format_error(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_error(error_msg)

        try:
            # Parse query
            if isinstance(query, str):
                query_obj = json.loads(query)
            elif isinstance(query, dict):
                query_obj = query
            else:
                error_msg = "Invalid query format"
                return (self.formatter.format_error(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_error(error_msg)

            operation = query_obj.get('operation')

            # Look up handler in OPERATION_MAP
            handler_name = self.OPERATION_MAP.get(operation)

            if handler_name:
                handler = getattr(self, handler_name)
                # Pass query_obj for operations that need parameters
                if operation in ('shell', 'describe_topics', 'describe_consumer_groups',
                                 'consumer_lag', 'broker_config', 'topic_config', 'describe_log_dirs'):
                    return handler(query_obj, return_raw)
                else:
                    return handler(return_raw)
            else:
                error_msg = f"Unsupported operation: {operation}"
                logger.warning(error_msg)
                return (self.formatter.format_note(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_note(error_msg)

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON query: {e}"
            logger.error(error_msg)
            return (self.formatter.format_error(error_msg), {'error': str(e)}) if return_raw else self.formatter.format_error(error_msg)
        except Exception as e:
            error_msg = f"Operation failed: {e}"
            logger.error(error_msg)
            return (self.formatter.format_error(error_msg), {'error': str(e)}) if return_raw else self.formatter.format_error(error_msg)

    # =========================================================================
    # Operation Handlers
    # =========================================================================

    def _execute_shell_command(self, query_obj: Dict, return_raw: bool = False):
        """Execute shell command via simulated SSH."""
        command = query_obj.get('command', '')
        results = self.execute_ssh_on_all_hosts(command, "Shell command")

        for result in results:
            if result.get('success'):
                output = result.get('output', '')
                return (output, output) if return_raw else output

        error_msg = f"Command not available in diagnostic data: {command}"
        return (self.formatter.format_error(error_msg), {'error': error_msg}) if return_raw else self.formatter.format_error(error_msg)

    def _list_topics(self, return_raw: bool = False):
        """List user-visible topics (excludes internal __ topics)."""
        topics_data = self.loader.get_kafka_output('topics')
        if not topics_data:
            formatted = self.formatter.format_note("No topic data available.")
            return (formatted, {'topics': [], 'count': 0}) if return_raw else formatted

        all_topics = topics_data.get('topics', [])
        user_topics = sorted([t['name'] for t in all_topics if not t.get('name', '').startswith('__')])

        raw = {'topics': user_topics, 'count': len(user_topics)}

        if not user_topics:
            formatted = self.formatter.format_note("No user topics found.")
        else:
            formatted = f"User Topics ({len(user_topics)}):\n\n"
            formatted += "\n".join(f"  - {t}" for t in user_topics)

        return (formatted, raw) if return_raw else formatted

    def _list_all_topics(self, return_raw: bool = False):
        """List ALL topics including internal."""
        topics_data = self.loader.get_kafka_output('topics')
        if not topics_data:
            formatted = self.formatter.format_note("No topic data available.")
            return (formatted, {'topics': [], 'count': 0}) if return_raw else formatted

        all_topics = sorted([t['name'] for t in topics_data.get('topics', [])])
        raw = {'topics': all_topics, 'count': len(all_topics)}

        if not all_topics:
            formatted = self.formatter.format_note("No topics found.")
        else:
            formatted = f"All Topics ({len(all_topics)}):\n\n"
            formatted += "\n".join(f"  - {t}" for t in all_topics)

        return (formatted, raw) if return_raw else formatted

    def _describe_topics(self, query_obj: Dict, return_raw: bool = False):
        """Describe topics with partition and ISR details."""
        topic_names = query_obj.get('topics', [])
        topics_data = self.loader.get_kafka_output('topics')

        if not topics_data:
            formatted = self.formatter.format_note("No topic data available.")
            return (formatted, []) if return_raw else formatted

        all_topics = topics_data.get('topics', [])

        if topic_names:
            all_topics = [t for t in all_topics if t.get('name') in topic_names]

        # Transform to live connector format
        raw_results = []
        for topic in all_topics:
            partitions = topic.get('partitions', [])
            partition_count = topic.get('partition_count', len(partitions))
            replication_factor = topic.get('replication_factor', 0)

            # Calculate under-replicated partitions
            under_replicated = 0
            for p in partitions:
                replicas = [r.strip() for r in p.get('replicas', '').split(',') if r.strip()]
                isr = [i.strip() for i in p.get('isr', '').split(',') if i.strip()]
                if len(isr) < len(replicas):
                    under_replicated += 1

            raw_results.append({
                'topic': topic.get('name'),
                'partitions': partition_count,
                'replication_factor': replication_factor,
                'under_replicated_partitions': under_replicated
            })

        if not raw_results:
            formatted = self.formatter.format_note("No topics found.")
        else:
            formatted = "|===\n|Topic|Partitions|Replication Factor|Under-Replicated\n"
            for t in raw_results:
                formatted += f"|{t['topic']}|{t['partitions']}|{t['replication_factor']}|{t['under_replicated_partitions']}\n"
            formatted += "|===\n"

        return (formatted, raw_results) if return_raw else formatted

    def _list_consumer_groups(self, return_raw: bool = False):
        """List consumer group names."""
        consumer_data = self.loader.get_kafka_output('consumer_groups')
        if not consumer_data:
            formatted = self.formatter.format_note("No consumer group data available.")
            return (formatted, {'groups': [], 'count': 0}) if return_raw else formatted

        groups = consumer_data.get('groups', [])
        group_names = sorted([g.get('name', '') for g in groups if g.get('name')])

        raw = {'groups': group_names, 'count': len(group_names)}

        if not group_names:
            formatted = self.formatter.format_note("No consumer groups found.")
        else:
            formatted = f"Consumer Groups ({len(group_names)}):\n\n"
            formatted += "\n".join(f"  - {g}" for g in group_names)

        return (formatted, raw) if return_raw else formatted

    def _describe_consumer_groups(self, query_obj: Dict, return_raw: bool = False):
        """Describe consumer groups with member details."""
        group_ids = query_obj.get('group_ids', [])
        consumer_data = self.loader.get_kafka_output('consumer_groups')

        if not consumer_data:
            formatted = self.formatter.format_note("No consumer group data available.")
            return (formatted, []) if return_raw else formatted

        groups = consumer_data.get('groups', [])

        if group_ids:
            groups = [g for g in groups if g.get('name') in group_ids]

        if not groups:
            formatted = self.formatter.format_note("No matching consumer groups found.")
            return (formatted, []) if return_raw else formatted

        # Normalize to expected format (group_id instead of name)
        normalized_groups = []
        for group in groups:
            partitions = group.get('partitions', [])
            # Count unique consumers (members)
            consumers = set()
            for p in partitions:
                if p.get('consumer_id'):
                    consumers.add(p.get('consumer_id'))

            normalized_groups.append({
                'group_id': group.get('name'),
                'state': 'Stable' if consumers else 'Empty',  # Infer state from members
                'members': len(consumers),
                'total_lag': group.get('total_lag', 0),
                'partitions': partitions
            })

        lines = []
        for group in normalized_groups:
            lines.append(f"\n**Group:** {group.get('group_id')}")
            lines.append(f"- State: {group.get('state')}")
            lines.append(f"- Members: {group.get('members')}")
            lines.append(f"- Total Lag: {group.get('total_lag', 0)}")

        formatted = '\n'.join(lines)
        return (formatted, normalized_groups) if return_raw else formatted

    def _handle_consumer_lag(self, query_obj: Dict, return_raw: bool = False):
        """Handle consumer lag operation - routes to specific or all groups."""
        group_id = query_obj.get('group_id')
        if group_id == '*' or not group_id:
            return self._get_all_consumer_lag(return_raw)
        else:
            return self._get_consumer_lag(group_id, return_raw)

    def _get_consumer_lag(self, group_id: str, return_raw: bool = False):
        """Get consumer lag for a specific group."""
        consumer_data = self.loader.get_kafka_output('consumer_groups')
        if not consumer_data:
            formatted = self.formatter.format_note("No consumer group data available.")
            return (formatted, {'error': 'No data'}) if return_raw else formatted

        groups = consumer_data.get('groups', [])
        group = next((g for g in groups if g.get('name') == group_id), None)

        if not group:
            formatted = self.formatter.format_note(f"Consumer group '{group_id}' not found.")
            return (formatted, {'error': 'Group not found'}) if return_raw else formatted

        partitions = group.get('partitions', [])
        lag_data = [{
            'topic': p.get('topic'),
            'partition': p.get('partition'),
            'current_offset': p.get('current_offset'),
            'log_end_offset': p.get('log_end_offset'),
            'lag': p.get('lag'),
            'consumer_id': p.get('consumer_id'),
            'host': p.get('host'),
            'client_id': p.get('client_id')
        } for p in partitions]

        raw = {
            'group_id': group_id,
            'total_lag': group.get('total_lag', 0),
            'partitions': lag_data
        }

        formatted = f"Consumer Group: {group_id}\nTotal Lag: {raw['total_lag']}\n\n"
        if lag_data:
            formatted += "|===\n|Topic|Partition|Current|End|Lag\n"
            for p in lag_data[:20]:
                formatted += f"|{p['topic']}|{p['partition']}|{p['current_offset']}|{p['log_end_offset']}|{p['lag']}\n"
            formatted += "|===\n"

        return (formatted, raw) if return_raw else formatted

    def _get_all_consumer_lag(self, return_raw: bool = False):
        """Get consumer lag for ALL groups."""
        consumer_data = self.loader.get_kafka_output('consumer_groups')
        if not consumer_data:
            formatted = self.formatter.format_note("No consumer group data available.")
            return (formatted, {'groups': []}) if return_raw else formatted

        groups = consumer_data.get('groups', [])

        all_lag = [{
            'group_id': group.get('name'),
            'total_lag': group.get('total_lag', 0),
            'partitions': [{
                'topic': p.get('topic'),
                'partition': p.get('partition'),
                'current_offset': p.get('current_offset'),
                'log_end_offset': p.get('log_end_offset'),
                'lag': p.get('lag')
            } for p in group.get('partitions', [])]
        } for group in groups]

        raw = {'groups': all_lag}

        formatted = f"Consumer Groups ({len(all_lag)}):\n\n"
        for g in all_lag:
            formatted += f"- {g['group_id']}: Total Lag = {g['total_lag']}\n"

        return (formatted, raw) if return_raw else formatted

    def _get_broker_config(self, query_obj: Dict, return_raw: bool = False):
        """Get broker configuration."""
        config = None
        for node_ip in self.cluster_nodes:
            config = self.loader.get_config('broker.properties', node=node_ip)
            if not config:
                config = self.loader.get_config('server.properties', node=node_ip)
            if config:
                break

        if not config:
            formatted = self.formatter.format_note("No broker configuration available.")
            return (formatted, {'error': 'No config'}) if return_raw else formatted

        config_entries = [{'name': k, 'value': v} for k, v in config.items()]

        formatted = "|===\n|Config|Value\n"
        for entry in config_entries[:50]:
            val = '****' if any(s in entry['name'].lower() for s in ['password', 'secret', 'key', 'credential']) else entry['value']
            formatted += f"|{entry['name']}|{val}\n"
        formatted += "|===\n"

        return (formatted, config_entries) if return_raw else formatted

    def _get_cluster_metadata(self, return_raw: bool = False):
        """Get cluster metadata."""
        raw = {
            'cluster_id': self.cluster_metadata.get('cluster_id', 'Unknown'),
            'controller_id': self.cluster_metadata.get('controller_id', -1),
            'broker_count': self.cluster_metadata.get('broker_count', 0),
            'brokers': self.cluster_nodes,
            'kafka_mode': self.kafka_mode,
        }

        formatted = f"""
Cluster Metadata:
- Cluster ID: {raw['cluster_id']}
- Controller: Broker {raw['controller_id']}
- Brokers: {raw['broker_count']}
- Mode: {raw['kafka_mode']}
"""

        return (formatted, raw) if return_raw else formatted

    def _get_topic_config(self, query_obj: Dict, return_raw: bool = False):
        """Get topic configuration."""
        topic = query_obj.get('topic')
        topics_data = self.loader.get_kafka_output('topics')

        if not topics_data:
            formatted = self.formatter.format_note(f"No topic data available for '{topic}'.")
            return (formatted, {'error': 'No data'}) if return_raw else formatted

        topic_info = next((t for t in topics_data.get('topics', []) if t.get('name') == topic), None)

        if not topic_info:
            formatted = self.formatter.format_note(f"Topic '{topic}' not found.")
            return (formatted, {'error': 'Topic not found'}) if return_raw else formatted

        configs_str = topic_info.get('configs', '')
        config_entries = []

        if configs_str:
            for item in configs_str.split(','):
                if '=' in item:
                    key, value = item.split('=', 1)
                    config_entries.append({'name': key.strip(), 'value': value.strip()})

        if not config_entries:
            formatted = self.formatter.format_note(f"No custom configs for topic '{topic}'.")
            return (formatted, config_entries) if return_raw else formatted

        formatted = f"Topic Config: {topic}\n\n|===\n|Config|Value\n"
        for entry in config_entries:
            formatted += f"|{entry['name']}|{entry['value']}\n"
        formatted += "|===\n"

        return (formatted, config_entries) if return_raw else formatted

    def _describe_log_dirs(self, query_obj: Dict, return_raw: bool = False):
        """Describe log directories (limited for diagnostic imports)."""
        formatted = self.formatter.format_note(
            "Log directory information not available from Instacollector export. "
            "Disk usage can be analyzed from iostat and system metrics."
        )
        return (formatted, {'error': 'Not available in diagnostic data'}) if return_raw else formatted

    def _get_quorum_status(self, return_raw: bool = False):
        """Get KRaft quorum status from diagnostic data."""
        if self.kafka_mode != 'kraft':
            formatted = self.formatter.format_note("Quorum status not available - cluster is in ZooKeeper mode.")
            return (formatted, {'error': 'Not KRaft mode'}) if return_raw else formatted

        quorum_data = self.loader.get_kafka_output('quorum_status')

        if not quorum_data:
            formatted = self.formatter.format_note("KRaft quorum status data not available in diagnostic export.")
            return (formatted, {'error': 'No quorum data'}) if return_raw else formatted

        # Format output
        formatted = "KRaft Quorum Status:\n\n"
        for key, value in quorum_data.items():
            formatted += f"  {key}: {value}\n"

        return (formatted, quorum_data) if return_raw else formatted

    def _get_quorum_replication(self, return_raw: bool = False):
        """Get KRaft quorum replication info from diagnostic data."""
        if self.kafka_mode != 'kraft':
            formatted = self.formatter.format_note("Quorum replication not available - cluster is in ZooKeeper mode.")
            return (formatted, {'error': 'Not KRaft mode'}) if return_raw else formatted

        replication_data = self.loader.get_kafka_output('quorum_replication')

        if not replication_data:
            formatted = self.formatter.format_note("KRaft quorum replication data not available in diagnostic export.")
            return (formatted, {'error': 'No replication data'}) if return_raw else formatted

        # Format output
        raw_output = replication_data.get('raw', str(replication_data))
        formatted = f"KRaft Quorum Replication:\n\n{raw_output}"

        return (formatted, replication_data) if return_raw else formatted

    def _get_api_versions(self, return_raw: bool = False):
        """Get API versions from diagnostic data."""
        api_data = self.loader.get_kafka_output('api_versions')

        if not api_data:
            formatted = self.formatter.format_note("API versions data not available in diagnostic export.")
            return (formatted, {'error': 'No API versions data'}) if return_raw else formatted

        # Format output
        raw_output = api_data.get('raw', str(api_data))
        formatted = f"Broker API Versions:\n\n{raw_output}"

        return (formatted, api_data) if return_raw else formatted

    # =========================================================================
    # Convenience Methods for Direct Data Access
    # =========================================================================

    def get_topics(self) -> List[Dict[str, Any]]:
        """Get topic information (convenience method)."""
        topics_data = self.loader.get_kafka_output('topics')
        return topics_data.get('topics', []) if topics_data else []

    def get_consumer_groups(self) -> List[Dict[str, Any]]:
        """Get consumer group information (convenience method)."""
        consumer_data = self.loader.get_kafka_output('consumer_groups')
        return consumer_data.get('groups', []) if consumer_data else []

    def get_quorum_status(self) -> Optional[Dict[str, Any]]:
        """Get KRaft quorum status (convenience method)."""
        return self.loader.get_kafka_output('quorum_status')

    def get_broker_config(self, node: Optional[str] = None) -> Optional[Dict[str, str]]:
        """Get broker configuration (convenience method)."""
        config = self.loader.get_config('broker.properties', node)
        if not config:
            config = self.loader.get_config('server.properties', node)
        return config


# =============================================================================
# Simulated SSH Manager (uses declarative mappings)
# =============================================================================

class SimulatedSSHManager:
    """
    Simulates SSH connection manager using loaded diagnostic files.

    Uses the same COMMAND_FILE_MAP declarative mappings as the connector.
    Provides the same interface as SSHConnectionManager so existing checks work.
    """

    def __init__(self, loader, host: str):
        """Initialize simulated SSH manager."""
        self.loader = loader
        self.host = host
        self.connected = True

    def ensure_connected(self) -> bool:
        """Ensure SSH connection is active. Always returns True for simulated."""
        return True

    def is_connected(self) -> bool:
        """Check if connected. Always returns True for simulated."""
        return True

    def execute_command(self, command: str, timeout: int = 30) -> tuple:
        """
        Simulate command execution using declarative file mappings.

        Args:
            command: Shell command to execute
            timeout: Timeout (ignored for simulated execution)

        Returns:
            tuple: (stdout, stderr, exit_code)
        """
        command_lower = command.lower().strip()

        # Check simulated commands first
        for pattern, response in SIMULATED_COMMANDS.items():
            if re.search(pattern, command_lower):
                return response

        # Try to match against file mappings
        for pattern, file_info in COMMAND_FILE_MAP.items():
            if re.search(pattern, command_lower):
                filename, is_log_glob = file_info
                output = self._get_file_content(filename, is_log_glob)
                if output:
                    return (output, '', 0)
                break

        return ('', f'Command data not available: {command}', 1)

    def _get_file_content(self, filename: str, is_log_glob: bool = False) -> Optional[str]:
        """Get file content from loader."""
        if is_log_glob:
            logs = self.loader.get_log_files(filename, node=self.host)
            if self.host in logs and logs[self.host]:
                try:
                    return logs[self.host][0].read_text(errors='ignore')
                except Exception:
                    pass
            return None
        else:
            content = self.loader.get_raw_file(filename, node=self.host)
            if content:
                return content
            if filename == 'broker.properties':
                return self.loader.get_raw_file('server.properties', node=self.host)
            return None

    def close(self):
        """Close the simulated connection."""
        self.connected = False


# =============================================================================
# Factory Function
# =============================================================================

def create_kafka_instacollector_connector(settings: Dict[str, Any], diagnostic_path: str) -> KafkaInstacollectorConnector:
    """
    Factory function to create a Kafka Instacollector connector.

    Args:
        settings: Configuration settings
        diagnostic_path: Path to Instacollector export

    Returns:
        Configured KafkaInstacollectorConnector instance
    """
    return KafkaInstacollectorConnector(settings, diagnostic_path)

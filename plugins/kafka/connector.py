"""Kafka connector implementation."""

import logging
import json
import socket
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from plugins.common.ssh_mixin import SSHSupportMixin
from plugins.common.output_formatters import AsciiDocFormatter

logger = logging.getLogger(__name__)


class KafkaConnector(SSHSupportMixin):
    """Connector for Kafka clusters with multi-broker SSH support."""
    
    def __init__(self, settings):
        """Initialize Kafka connector."""
        self.settings = settings
        self.admin_client = None
        self._version_info = {}
        self.formatter = AsciiDocFormatter()

        # Environment detection
        self.environment = None
        self.environment_details = {}

        # Initialize SSH support (from mixin)
        self.initialize_ssh()

        logger.info(f"Kafka connector initialized")
    
    def connect(self):
        """Establishes connections to Kafka cluster and all SSH hosts."""
        try:
            bootstrap_servers = self.settings.get('bootstrap_servers', ['localhost:9092'])

            # Handle both list and string formats
            if isinstance(bootstrap_servers, str):
                bootstrap_servers = [s.strip() for s in bootstrap_servers.split(',')]

            # Build connection parameters with higher timeouts for cloud/managed services
            connection_params = {
                'bootstrap_servers': bootstrap_servers,
                'client_id': 'healthcheck_client',
                'request_timeout_ms': 60000,  # 60 seconds for cloud connections
                'connections_max_idle_ms': 540000,  # 9 minutes
                'metadata_max_age_ms': 300000,  # 5 minutes
                'socket_options': [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
            }

            # Specify API version for newer Kafka brokers
            # Convert list from YAML to tuple if present
            api_version = self.settings.get('api_version')
            if api_version:
                if isinstance(api_version, list):
                    api_version = tuple(api_version)
                connection_params['api_version'] = api_version
                logger.info(f"Using API version: {api_version}")

            # Add security configuration if present
            security_protocol = self.settings.get('security_protocol')
            if security_protocol:
                connection_params['security_protocol'] = security_protocol
                logger.info(f"Using security protocol: {security_protocol}")

                # SASL configuration
                if 'SASL' in security_protocol:
                    sasl_mechanism = self.settings.get('sasl_mechanism', 'PLAIN')
                    connection_params['sasl_mechanism'] = sasl_mechanism
                    connection_params['sasl_plain_username'] = self.settings.get('sasl_username')
                    connection_params['sasl_plain_password'] = self.settings.get('sasl_password')
                    logger.info(f"Using SASL mechanism: {sasl_mechanism}")

                # SSL/TLS configuration
                if 'SSL' in security_protocol:
                    ssl_cafile = self.settings.get('ssl_cafile')
                    if ssl_cafile:
                        connection_params['ssl_cafile'] = ssl_cafile
                        logger.info(f"Using SSL CA file: {ssl_cafile}")

                    # Optional: client certificate for mTLS
                    ssl_certfile = self.settings.get('ssl_certfile')
                    ssl_keyfile = self.settings.get('ssl_keyfile')
                    if ssl_certfile and ssl_keyfile:
                        connection_params['ssl_certfile'] = ssl_certfile
                        connection_params['ssl_keyfile'] = ssl_keyfile
                        logger.info("Using mTLS with client certificate")

                    # SSL check hostname (default True for security)
                    if 'ssl_check_hostname' in self.settings:
                        connection_params['ssl_check_hostname'] = self.settings['ssl_check_hostname']

            self.admin_client = KafkaAdminClient(**connection_params)

            self._version_info = self._get_version_info()

            # Detect environment (Instaclustr vs self-hosted)
            self._detect_environment()

            # Connect all SSH hosts (from mixin)
            connected_ssh_hosts = self.connect_all_ssh()
            
            # Map SSH hosts to broker IDs
            if connected_ssh_hosts:
                self._map_ssh_hosts_to_brokers()
            
            # Display connection status
            print("✅ Successfully connected to Kafka cluster")
            
            # Get cluster metadata for detailed status
            try:
                cluster = self.admin_client._client.cluster
                cluster.request_update()
                
                brokers = list(cluster.brokers())
                broker_count = len(brokers)
                
                # Get cluster ID
                try:
                    cluster_id = cluster.cluster_id if hasattr(cluster, 'cluster_id') else 'Unknown'
                except:
                    cluster_id = 'Unknown'
                
                # Get controller info
                try:
                    controller = cluster.controller
                    if callable(controller):
                        controller = controller()
                    controller_id = controller.nodeId if hasattr(controller, 'nodeId') else (controller.id if hasattr(controller, 'id') else -1)
                except:
                    controller_id = -1
                
                print(f"   - Cluster ID: {cluster_id}")

                # Display version
                version = self._version_info.get('version_string', 'Unknown')
                print(f"   - Kafka Version: {version}")

                print(f"   - Brokers: {broker_count}")
                if controller_id != -1:
                    print(f"   - Controller: Broker {controller_id}")

                # Display environment
                if self.environment:
                    env_label = "Instaclustr Managed" if self.environment == 'instaclustr_managed' else "Self-Hosted"
                    print(f"   - Environment: {env_label}")
                
                # Show broker addresses
                if broker_count > 0:
                    print(f"   - Broker Addresses:")
                    for broker in brokers[:5]:
                        try:
                            broker_id = broker.nodeId if hasattr(broker, 'nodeId') else broker.id
                            print(f"      • {broker.host}:{broker.port} (ID: {broker_id})")
                        except Exception as e:
                            logger.debug(f"Could not format broker info: {e}")
                    if broker_count > 5:
                        print(f"      ... and {broker_count - 5} more")
                
                # SSH status (from mixin)
                if self.has_ssh_support():
                    print(f"   - SSH: Connected to {len(connected_ssh_hosts)}/{len(self.get_ssh_hosts())} host(s)")
                    unmapped_hosts = []
                    for ssh_host in connected_ssh_hosts:
                        broker_id = self.ssh_host_to_node.get(ssh_host)
                        if broker_id is not None:
                            print(f"      • {ssh_host} (Broker {broker_id})")
                        else:
                            print(f"      • {ssh_host} (⚠️  Not recognized as cluster broker)")
                            unmapped_hosts.append(ssh_host)

                    if unmapped_hosts:
                        print(f"   ⚠️  WARNING: {len(unmapped_hosts)} SSH host(s) are not recognized as cluster brokers!")
                        print(f"      This may indicate brokers that are down or not part of the cluster.")
                else:
                    print(f"   - SSH: Not configured (OS-level checks unavailable)")
                    
            except Exception as e:
                logger.warning(f"Could not retrieve detailed cluster info: {e}")
            
            logger.info("✅ Connected to Kafka cluster")
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise ConnectionError(f"Could not connect to Kafka: {e}")
    
    def disconnect(self):
        """Closes connections to Kafka and all SSH hosts."""
        if self.admin_client:
            self.admin_client.close()
            logger.info("Disconnected from Kafka cluster")
        
        # Disconnect all SSH (from mixin)
        self.disconnect_all_ssh()
    
    def _map_ssh_hosts_to_brokers(self):
        """Kafka-specific logic to map SSH hosts to broker IDs."""
        try:
            cluster = self.admin_client._client.cluster
            cluster.request_update()
            
            # Build host-to-broker mapping
            host_node_mapping = {}
            for broker in cluster.brokers():
                broker_id = broker.nodeId if hasattr(broker, 'nodeId') else broker.id
                broker_host = broker.host
                host_node_mapping[broker_host] = broker_id
            
            # Use mixin's mapping method
            self.map_ssh_hosts_to_nodes(host_node_mapping)
                    
        except Exception as e:
            logger.warning(f"Could not map SSH hosts to broker IDs: {e}")

    def close(self):
        """Alias for disconnect()."""
        self.disconnect()

    def _detect_environment(self):
        """
        Detect the hosting environment (Instaclustr managed vs self-hosted).

        Detection logic:
        1. Instaclustr: If instaclustr_cluster_id or instaclustr_prometheus_enabled is configured
        2. Self-hosted: Default if no managed indicators

        Updates self.environment and self.environment_details
        """
        try:
            # Check for Instaclustr configuration
            cluster_id = self.settings.get('instaclustr_cluster_id')
            prometheus_enabled = self.settings.get('instaclustr_prometheus_enabled')

            if cluster_id or prometheus_enabled:
                self.environment = 'instaclustr_managed'
                self.environment_details = {
                    'provider': 'Instaclustr',
                    'monitoring': 'prometheus' if prometheus_enabled else 'api',
                    'has_prometheus_metrics': bool(prometheus_enabled)
                }
                if cluster_id:
                    self.environment_details['cluster_id'] = cluster_id
                logger.info(f"Detected Instaclustr managed environment" + (f": {cluster_id}" if cluster_id else ""))
                return

            # Default to self-hosted
            self.environment = 'self_hosted'
            self.environment_details = {
                'provider': 'self_hosted',
                'ssh_access': self.has_ssh_support()
            }
            logger.info("Detected self-hosted Kafka environment")

        except Exception as e:
            logger.warning(f"Error detecting environment: {e}. Assuming self-hosted.")
            self.environment = 'self_hosted'
            self.environment_details = {'provider': 'self_hosted'}

    def _get_version_info(self):
        """Fetches broker version information."""
        try:
            # First check if version is configured in settings
            configured_version = self.settings.get('kafka_version')
            if configured_version:
                logger.info(f"Using configured Kafka version: {configured_version}")
                cluster_metadata = self.admin_client._client.cluster
                brokers = cluster_metadata.brokers()
                return {
                    'version_string': configured_version,
                    'broker_count': len(brokers) if brokers else 0,
                    'source': 'configured'
                }

            # Try to detect from broker API versions
            cluster_metadata = self.admin_client._client.cluster
            brokers = list(cluster_metadata.brokers())
            broker_count = len(brokers) if brokers else 0

            # Attempt to get version from broker metadata
            # Note: kafka-python doesn't directly expose broker version in admin API
            # We can infer from API versions or use configured value
            version_string = 'Unknown'

            # Check if we have API version info
            if brokers:
                first_broker = list(brokers)[0]
                # Get the API version range to infer Kafka version
                try:
                    api_versions = self.admin_client._client.check_version(first_broker.nodeId)
                    if api_versions:
                        # Rough mapping of API versions to Kafka versions
                        if api_versions >= (3, 0, 0):
                            version_string = '3.x (KRaft)'
                        elif api_versions >= (2, 8, 0):
                            version_string = '2.8.x+'
                        elif api_versions >= (2, 0, 0):
                            version_string = '2.x'
                        else:
                            version_string = f'{api_versions[0]}.x'
                        logger.info(f"Detected Kafka version from API: {version_string}")
                except:
                    logger.debug("Could not detect version from API versions")

            return {
                'version_string': version_string,
                'broker_count': broker_count,
                'source': 'detected' if version_string != 'Unknown' else 'unknown'
            }
        except Exception as e:
            logger.warning(f"Could not fetch version: {e}")
            return {'version_string': 'Unknown', 'broker_count': 0, 'source': 'error'}

    @property
    def version_info(self):
        """Returns version information."""
        if self._version_info is None:
            self._version_info = self._get_version_info()
        return self._version_info

    def get_db_metadata(self):
        """
        Fetches cluster-level metadata for trend analysis.

        Returns:
            dict: Metadata including version, cluster_name, nodes, environment
        """
        try:
            # Get cluster information
            cluster_info = self.admin_client.describe_cluster()
            cluster_id = cluster_info.get('cluster_id', 'Unknown')

            # Get broker count
            cluster = self.admin_client._client.cluster
            cluster.request_update()
            brokers = list(cluster.brokers())
            broker_count = len(brokers)

            # Use detected environment
            environment = self.environment if self.environment else 'self_hosted'
            environment_details = self.environment_details if self.environment_details else {}

            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'cluster_name': cluster_id,
                'nodes': broker_count,
                'environment': environment,
                'environment_details': environment_details,
                'db_name': f"Cluster ID: {cluster_id}"  # Keep for backwards compatibility
            }
        except Exception as e:
            logger.warning(f"Could not fetch metadata: {e}")
            return {
                'version': 'N/A',
                'cluster_name': 'Unknown',
                'nodes': 0,
                'environment': 'unknown',
                'db_name': 'N/A'
            }

    def execute_query(self, query, params=None, return_raw=False):
        """
        Executes Kafka Admin API operations or shell commands via JSON dispatch.
        
        Supported operations:
        - list_topics
        - describe_topics
        - list_consumer_groups
        - describe_consumer_groups
        - consumer_lag
        - broker_config
        - topic_config
        - cluster_metadata
        - describe_log_dirs
        - list_consumer_group_offsets
        - shell (requires SSH)
        
        Args:
            query: JSON string defining the operation
            params: Not used (for API compatibility)
            return_raw: If True, returns (formatted, raw_data)
        
        Returns:
            str or tuple: Formatted results
        """
        try:
            query_obj = json.loads(query)
            operation = query_obj.get('operation')

            # Route to appropriate handler
            if operation == 'shell':
                return self._execute_shell_command(query_obj.get('command'), return_raw)
            elif operation == 'list_topics':
                return self._list_topics(return_raw)
            elif operation == 'describe_topics':
                return self._describe_topics(query_obj.get('topics', []), return_raw)
            elif operation == 'list_consumer_groups':
                return self._list_consumer_groups(return_raw)
            elif operation == 'describe_consumer_groups':
                return self._describe_consumer_groups(query_obj.get('group_ids', []), return_raw)
            elif operation == 'consumer_lag':
                group_id = query_obj.get('group_id')
                if not group_id:
                    raise ValueError("'consumer_lag' requires 'group_id'")
                if group_id == '*':
                    return self._get_all_consumer_lag(return_raw)
                return self._get_consumer_lag(group_id, return_raw)
            elif operation == 'broker_config':
                broker_id = query_obj.get('broker_id')
                if broker_id is None:
                    raise ValueError("'broker_config' requires 'broker_id'")
                return self._get_broker_config(broker_id, return_raw)
            elif operation == 'topic_config':
                topic = query_obj.get('topic')
                if not topic:
                    raise ValueError("'topic_config' requires 'topic'")
                return self._get_topic_config(topic, return_raw)
            elif operation == 'cluster_metadata':
                return self._get_cluster_metadata(return_raw)
            elif operation == 'describe_log_dirs':
                broker_ids = query_obj.get('broker_ids', [])
                return self._describe_log_dirs(broker_ids, return_raw)
            elif operation == 'list_consumer_group_offsets':
                group_id = query_obj.get('group_id')
                if not group_id:
                    raise ValueError("'list_consumer_group_offsets' requires 'group_id'")
                return self._list_consumer_group_offsets(group_id, return_raw)
            else:
                raise ValueError(f"Unsupported operation: {operation}")

        except json.JSONDecodeError as e:
            msg = self.formatter.format_error(f"Invalid JSON query: {e}")
            logger.error(msg)
            return (msg, {'error': str(e)}) if return_raw else msg
        except Exception as e:
            msg = self.formatter.format_error(f"Operation failed: {e}")
            logger.debug(f"Exception details: {e}", exc_info=True)  # Full trace only in debug
            logger.error(f"Operation failed: {e}")  # User-friendly error without trace
            return (msg, {'error': str(e)}) if return_raw else msg

    def _execute_shell_command(self, command, return_raw=False):
        """
        Executes a shell command on a Kafka broker via SSH.
        
        Delegates to ShellExecutor which handles:
        - Command sanitization
        - SSH execution
        - Output formatting
        - Error handling
        
        Args:
            command: Shell command to execute (e.g., 'df -h /var/lib/kafka')
            return_raw: If True, returns tuple (formatted, raw_data)
        
        Returns:
            str or tuple: Formatted output or (formatted, raw) if return_raw=True
        """
        if not self.ssh_manager:
            error_msg = self.formatter.format_error(
                "SSH not configured. Required settings: ssh_host, ssh_user, "
                "and ssh_key_file or ssh_password"
            )
            return (error_msg, {'error': 'SSH not configured'}) if return_raw else error_msg
        
        try:
            # Build JSON query for ShellExecutor
            query = json.dumps({
                "operation": "shell",
                "command": command
            })
            
            # Delegate to ShellExecutor (uses shared formatter)
            return self.shell_executor.execute(query, return_raw=return_raw)

        except Exception as e:
            error_msg = self.formatter.format_error(f"Shell command failed: {str(e)}")
            logger.debug(f"Shell command exception details: {e}", exc_info=True)
            logger.error(f"Shell command failed: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _list_topics(self, return_raw=False):
        """Lists all user-visible topics."""
        topics = self.admin_client.list_topics()
        user_topics = sorted([t for t in topics if not t.startswith('__')])
        raw = {'topics': user_topics, 'count': len(user_topics)}
        
        if not user_topics:
            formatted = self.formatter.format_note("No user topics found.")
        else:
            formatted = f"User Topics ({len(user_topics)}):\n\n"
            formatted += "\n".join(f"  - {t}" for t in user_topics)
        
        return (formatted, raw) if return_raw else formatted

    def _describe_topics(self, topics, return_raw=False):
        """Gets detailed information about topics."""
        cluster = self.admin_client._client.cluster
        cluster.request_update()
        
        target_topics = topics or list(cluster.topics(exclude_internal_topics=True))
        raw_results = []

        for topic_name in sorted(target_topics):
            partitions = cluster.partitions_for_topic(topic_name)
            if not partitions:
                continue

            under_replicated = 0
            tp_example = TopicPartition(topic_name, next(iter(partitions)))
            replication_factor = len(cluster.replicas(tp_example))

            for p_id in partitions:
                tp = TopicPartition(topic_name, p_id)
                if len(cluster.in_sync_replicas(tp)) < len(cluster.replicas(tp)):
                    under_replicated += 1
            
            raw_results.append({
                'topic': topic_name,
                'partitions': len(partitions),
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

    def _list_consumer_groups(self, return_raw=False):
        """Lists all consumer groups."""
        try:
            groups = self.admin_client.list_consumer_groups()
            raw_results = [{'group_id': g[0], 'protocol_type': g[1]} for g in groups]
            
            if not raw_results:
                formatted = self.formatter.format_note("No consumer groups found.")
            else:
                formatted = self.formatter.format_table(raw_results)
            
            return (formatted, raw_results) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to list consumer groups: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _describe_consumer_groups(self, group_ids, return_raw=False):
        """Gets detailed information about consumer groups."""
        try:
            if not group_ids:
                # Get all groups
                groups = self.admin_client.list_consumer_groups()
                group_ids = [g[0] for g in groups]
            
            if not group_ids:
                formatted = self.formatter.format_note("No consumer groups found.")
                return (formatted, []) if return_raw else formatted
            
            descriptions = self.admin_client.describe_consumer_groups(group_ids)
            raw_results = []
            
            # Handle both dict and list return types from describe_consumer_groups
            if isinstance(descriptions, dict):
                # Dict format: {group_id: description_object}
                for group_id, description in descriptions.items():
                    raw_results.append({
                        'group_id': group_id,
                        'state': description.state,
                        'protocol_type': description.protocol_type,
                        'members': len(description.members)
                    })
            elif isinstance(descriptions, list):
                # List format: [description_object, ...]
                for description in descriptions:
                    # FIX: Use 'group' not 'group_id'
                    group_id = getattr(description, 'group', getattr(description, 'group_id', 'unknown'))
                    state = getattr(description, 'state', 'unknown')
                    protocol_type = getattr(description, 'protocol_type', 'unknown')
                    members = len(getattr(description, 'members', []))
                    
                    raw_results.append({
                        'group_id': group_id,
                        'state': state,
                        'protocol_type': protocol_type,
                        'members': members
                    })
            else:
                raise ValueError(f"Unexpected descriptions format: {type(descriptions)}")
            
            if not raw_results:
                formatted = self.formatter.format_note("No consumer group details found.")
            else:
                formatted = self.formatter.format_table(raw_results)
            
            return (formatted, raw_results) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to describe consumer groups: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg
        
    def _get_consumer_lag(self, group_id, return_raw=False):
        """Calculates consumer lag for a specific group."""
        try:
            logger.info(f"Fetching consumer lag for group: {group_id}")
            committed_offsets = self.admin_client.list_consumer_group_offsets(group_id)
            
            logger.info(f"Got {len(committed_offsets) if committed_offsets else 0} committed offsets for {group_id}")
            
            if not committed_offsets:
                msg = self.formatter.format_note(f"No offsets for group '{group_id}'.")
                return (msg, {}) if return_raw else msg
    
            partitions = list(committed_offsets.keys())
            logger.info(f"Partitions for {group_id}: {partitions}")

            # Build consumer parameters with same security config
            consumer_params = {'bootstrap_servers': self.settings.get('bootstrap_servers')}

            # Add security configuration if present
            security_protocol = self.settings.get('security_protocol')
            if security_protocol:
                consumer_params['security_protocol'] = security_protocol

                if 'SASL' in security_protocol:
                    consumer_params['sasl_mechanism'] = self.settings.get('sasl_mechanism', 'PLAIN')
                    consumer_params['sasl_plain_username'] = self.settings.get('sasl_username')
                    consumer_params['sasl_plain_password'] = self.settings.get('sasl_password')

                if 'SSL' in security_protocol:
                    ssl_cafile = self.settings.get('ssl_cafile')
                    if ssl_cafile:
                        consumer_params['ssl_cafile'] = ssl_cafile
                    ssl_certfile = self.settings.get('ssl_certfile')
                    ssl_keyfile = self.settings.get('ssl_keyfile')
                    if ssl_certfile and ssl_keyfile:
                        consumer_params['ssl_certfile'] = ssl_certfile
                        consumer_params['ssl_keyfile'] = ssl_keyfile
                    if 'ssl_check_hostname' in self.settings:
                        consumer_params['ssl_check_hostname'] = self.settings['ssl_check_hostname']

            consumer = KafkaConsumer(**consumer_params)
            end_offsets = consumer.end_offsets(partitions)
            consumer.close()
            
            logger.info(f"End offsets: {end_offsets}")
    
            lag_data = []
            for partition, offset_meta in committed_offsets.items():
                committed = offset_meta.offset
                end = end_offsets.get(partition, 0)
                lag = max(0, end - committed)
                lag_data.append({
                    'group_id': group_id,
                    'topic': partition.topic,
                    'partition': partition.partition,
                    'current_offset': committed,
                    'log_end_offset': end,
                    'lag': lag
                })
            
            raw = {
                'group_id': group_id,
                'details': lag_data,
                'total_lag': sum(d['lag'] for d in lag_data)
            }
            
            logger.info(f"Calculated lag for {group_id}: total={raw['total_lag']}, details count={len(lag_data)}")
            
            formatted = f"Consumer Group: {group_id}\nTotal Lag: {raw['total_lag']}\n\n"
            formatted += "|===\n|Topic|Partition|Current|End|Lag\n"
            for item in sorted(lag_data, key=lambda x: (x['topic'], x['partition'])):
                formatted += f"|{item['topic']}|{item['partition']}|{item['current_offset']}|{item['log_end_offset']}|{item['lag']}\n"
            formatted += "|===\n"
            
            return (formatted, raw) if return_raw else formatted
            
        except Exception as e:
            logger.error(f"Failed to calculate lag for {group_id}: {e}", exc_info=True)
            error_msg = self.formatter.format_error(f"Failed to calculate lag: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _get_all_consumer_lag(self, return_raw=False):
        """Calculates consumer lag for all groups.
        
        Returns aggregated facts without interpretation. Returns metadata
        about which groups have no offsets so checks can interpret appropriately.
        """
        try:
            groups = self.admin_client.list_consumer_groups()
            if not groups:
                msg = self.formatter.format_note("No consumer groups found.")
                return (msg, {
                    'group_lags': [],
                    'total_lag': 0,
                    'groups_without_offsets': []  # ✅ Empty list
                }) if return_raw else msg
            
            all_lag_data = []
            total_lag = 0
            errors = []
            groups_without_offsets = []  # ✅ Track groups with no offsets
            
            for group_tuple in groups:
                group_id = group_tuple[0] if isinstance(group_tuple, tuple) else group_tuple
                
                try:
                    _, raw = self._get_consumer_lag(group_id, return_raw=True)
                    
                    # Check for error
                    if isinstance(raw, dict) and 'error' in raw:
                        errors.append({
                            'group_id': group_id,
                            'error': raw['error']
                        })
                        continue
                    
                    # Check for no offsets (FACT, not interpretation)
                    if isinstance(raw, dict) and raw.get('no_committed_offsets'):
                        groups_without_offsets.append(group_id)  # ✅ Just track the fact
                        continue
                    
                    # Aggregate lag data
                    if isinstance(raw, dict) and 'details' in raw:
                        all_lag_data.extend(raw['details'])
                        total_lag += raw.get('total_lag', 0)
                        
                except Exception as e:
                    logger.warning(f"Failed to get lag for group {group_id}: {e}")
                    errors.append({
                        'group_id': group_id,
                        'error': str(e)
                    })
            
            # Build response with FACTS only
            raw = {
                'group_lags': all_lag_data,
                'total_lag': total_lag,
                'groups_without_offsets': groups_without_offsets,  # ✅ Fact
                'groups_with_errors': errors  # ✅ Fact
            }
            
            # Minimal factual formatting
            formatted = f"All Consumer Groups\nTotal Lag: {total_lag}\n"
            formatted += f"Groups Analyzed: {len(groups)}\n"
            formatted += f"Groups With Data: {len(all_lag_data) // max(len(all_lag_data), 1) if all_lag_data else 0}\n"
            formatted += f"Groups Without Offsets: {len(groups_without_offsets)}\n"
            formatted += f"Groups With Errors: {len(errors)}\n\n"
            
            # Show data table if available
            if all_lag_data:
                formatted += self.formatter.format_table(all_lag_data)
            else:
                formatted += "No lag data available.\n"
            
            return (formatted, raw) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to calculate lag for all groups: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg
                        
    def _get_broker_config(self, broker_id, return_raw=False):
        """Gets configuration for a specific broker."""
        try:
            config_resource = ConfigResource(ConfigResourceType.BROKER, str(broker_id))
            configs = self.admin_client.describe_configs([config_resource])
            
            config_dict = {}
            for resource, future in configs.items():
                config = future.result()
                for key, value in config.resources[0][4].items():
                    config_dict[key] = value.value
            
            raw = {'name': str(broker_id), 'configs': config_dict}
            
            formatted = f"Broker {broker_id} Configuration:\n\n"
            formatted += self.formatter.format_dict_as_table(config_dict, 'Setting', 'Value')
            
            return (formatted, raw) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to get broker config: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _get_topic_config(self, topic, return_raw=False):
        """Gets configuration for a specific topic."""
        try:
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic)
            configs = self.admin_client.describe_configs([config_resource])
            
            config_dict = {}
            for resource, future in configs.items():
                config = future.result()
                for key, value in config.resources[0][4].items():
                    config_dict[key] = value.value
            
            raw = {'name': topic, 'configs': config_dict}
            
            formatted = f"Topic '{topic}' Configuration:\n\n"
            formatted += self.formatter.format_dict_as_table(config_dict, 'Setting', 'Value')
            
            return (formatted, raw) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to get topic config: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _get_cluster_metadata(self, return_raw=False):
            """Gets cluster-wide metadata."""
            try:
                cluster = self.admin_client._client.cluster
                cluster.request_update()
                
                brokers = []
                for broker in cluster.brokers():
                    brokers.append({
                        'id': broker.nodeId if hasattr(broker, 'nodeId') else broker.id,
                        'host': broker.host,
                        'port': broker.port
                    })
                
                # Get controller - it's an object, not a method
                controller = cluster.controller
                controller_id = controller.nodeId if hasattr(controller, 'nodeId') else controller.id
                
                # Get cluster_id - it's a property, not a method
                cluster_id = cluster.cluster_id if hasattr(cluster, 'cluster_id') else 'Unknown'
                
                raw = {
                    'cluster_id': cluster_id,
                    'controller_id': controller_id,
                    'brokers': brokers
                }
                
                formatted = f"Cluster ID: {raw['cluster_id']}\n"
                formatted += f"Controller: {raw['controller_id']}\n\n"
                formatted += "Brokers:\n"
                formatted += self.formatter.format_table(brokers)
                
                return (formatted, raw) if return_raw else formatted
                
            except Exception as e:
                error_msg = self.formatter.format_error(f"Failed to get cluster metadata: {e}")
                return (error_msg, {'error': str(e)}) if return_raw else error_msg




    def _old_get_cluster_metadata(self, return_raw=False):
        """Gets cluster-wide metadata."""
        try:
            cluster = self.admin_client._client.cluster
            cluster.request_update()
            
            brokers = []
            for broker in cluster.brokers():
                brokers.append({
                    'id': broker.nodeId if hasattr(broker, 'nodeId') else broker.id,
                    'host': broker.host,
                    'port': broker.port
                })
            
            controller_id = cluster.controller().nodeId if hasattr(cluster.controller(), 'nodeId') else cluster.controller().id
            
            raw = {
                'cluster_id': cluster.cluster_id() if hasattr(cluster, 'cluster_id') else 'Unknown',
                'controller_id': controller_id,
                'brokers': brokers
            }
            
            formatted = f"Cluster ID: {raw['cluster_id']}\n"
            formatted += f"Controller: {raw['controller_id']}\n\n"
            formatted += "Brokers:\n"
            formatted += self.formatter.format_table(brokers)
            
            return (formatted, raw) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to get cluster metadata: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _describe_log_dirs(self, broker_ids=None, return_raw=False):
        """Gets log directory information for brokers.
        
        The response structure is:
        log_dirs = [
            (broker_id, log_dir_path, [
                (topic_name, [
                    (partition_id, size_bytes, offset_lag, is_future),
                    ...
                ]),
                ...
            ]),
            ...
        ]
        """
        try:
            response = self.admin_client.describe_log_dirs()
            
            raw_results = []
            
            # Parse the log_dirs attribute
            if hasattr(response, 'log_dirs'):
                for log_dir_entry in response.log_dirs:
                    # Each entry is: (broker_id, log_dir_path, topics_list)
                    broker_id = log_dir_entry[0]
                    log_dir_path = log_dir_entry[1]
                    topics_list = log_dir_entry[2]
                    
                    # Parse each topic
                    for topic_entry in topics_list:
                        topic_name = topic_entry[0]
                        partitions_list = topic_entry[1]
                        
                        # Parse each partition
                        for partition_info in partitions_list:
                            partition_id = partition_info[0]
                            size_bytes = partition_info[1]
                            offset_lag = partition_info[2]
                            is_future = partition_info[3]
                            
                            raw_results.append({
                                'broker_id': broker_id,
                                'log_dir': log_dir_path,
                                'topic': topic_name,
                                'partition': partition_id,
                                'size_bytes': size_bytes,
                                'offset_lag': offset_lag,
                                'is_future': is_future
                            })
            
            # If broker_ids was specified, filter results
            if broker_ids:
                raw_results = [r for r in raw_results if r['broker_id'] in broker_ids]
            
            if not raw_results:
                formatted = self.formatter.format_note("No log directory information found.")
            else:
                formatted = self.formatter.format_table(raw_results)
            
            return (formatted, raw_results) if return_raw else formatted

        except Exception as e:
            # For managed clusters, this API is typically restricted - handle gracefully
            if self.environment == 'instaclustr_managed':
                logger.debug(f"describe_log_dirs not available on managed cluster (expected): {e}")
                info_msg = self.formatter.format_note("Log directory details not available on managed Kafka clusters (API restricted by provider)")
                return (info_msg, {'unavailable': 'managed_cluster_restriction'}) if return_raw else info_msg
            else:
                logger.warning(f"Failed to describe log dirs: {e}")
                error_msg = self.formatter.format_note(f"Could not retrieve log directory information: {e}")
                return (error_msg, {'error': str(e)}) if return_raw else error_msg
        



    def DEBUG_describe_log_dirs(self, broker_ids=None, return_raw=False):
        """Gets log directory information for brokers."""
        try:
            response = self.admin_client.describe_log_dirs()
            
            # DEBUG: Let's see what we're actually getting
            logger.info(f"describe_log_dirs response type: {type(response)}")
            logger.info(f"describe_log_dirs response dir: {dir(response)}")
            
            # Try to inspect the response
            print(f"[DEBUG] Response type: {type(response)}")
            print(f"[DEBUG] Response attributes: {dir(response)}")
            if hasattr(response, '__dict__'):
                print(f"[DEBUG] Response dict: {response.__dict__}")
            
            # For now, return the debug info
            formatted = f"Response type: {type(response)}\nAttributes: {dir(response)}"
            return (formatted, {'debug': str(response)}) if return_raw else formatted
            
        except Exception as e:
            logger.error(f"Failed to describe log dirs: {e}", exc_info=True)
            error_msg = self.formatter.format_error(f"Failed to describe log dirs: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg






    def _old_describe_log_dirs(self, broker_ids=None, return_raw=False):
        """Gets log directory information for brokers.
        
        Note: kafka-python's describe_log_dirs() doesn't accept broker_ids parameter.
        It always returns data for all brokers in the cluster.
        """
        try:
            # describe_log_dirs() takes no arguments - returns all brokers automatically
            log_dirs = self.admin_client.describe_log_dirs()
            
            raw_results = []
            for broker_id, dirs in log_dirs.items():
                for dir_path, topics in dirs.items():
                    for topic, partitions in topics.items():
                        for partition, info in partitions.items():
                            raw_results.append({
                                'broker_id': broker_id,
                                'log_dir': dir_path,
                                'topic': topic,
                                'partition': partition,
                                'size_bytes': info.get('size', 0),
                                'offset_lag': info.get('offsetLag', 0)
                            })
            
            # If broker_ids was specified, filter results (even though API returned all)
            if broker_ids:
                raw_results = [r for r in raw_results if r['broker_id'] in broker_ids]
            
            if not raw_results:
                formatted = self.formatter.format_note("No log directory information found.")
            else:
                formatted = self.formatter.format_table(raw_results)
            
            return (formatted, raw_results) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to describe log dirs: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _list_consumer_group_offsets(self, group_id, return_raw=False):
        """Lists committed offsets for a consumer group."""
        try:
            offsets = self.admin_client.list_consumer_group_offsets(group_id)
            
            if not offsets:
                msg = self.formatter.format_note(f"No offsets for group '{group_id}'.")
                return (msg, []) if return_raw else msg
            
            raw_results = []
            for partition, offset_meta in offsets.items():
                raw_results.append({
                    'group_id': group_id,
                    'topic': partition.topic,
                    'partition': partition.partition,
                    'offset': offset_meta.offset,
                    'metadata': offset_meta.metadata
                })
            
            formatted = f"Consumer Group '{group_id}' Offsets:\n\n"
            formatted += self.formatter.format_table(raw_results)
            
            return (formatted, raw_results) if return_raw else formatted
            
        except Exception as e:
            error_msg = self.formatter.format_error(f"Failed to list offsets: {e}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

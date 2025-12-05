"""OpenSearch connector implementation with multi-node SSH support and AWS OpenSearch Service support."""

import logging
import json
from opensearchpy import OpenSearch
from typing import Dict, List, Optional, Any

# Import shared utilities
from plugins.common.ssh_mixin import SSHSupportMixin
from plugins.common.aws_handler import AWSSupportMixin, AWSConnectionManager
from plugins.common.output_formatters import AsciiDocFormatter
from plugins.common.cve_mixin import CVECheckMixin

logger = logging.getLogger(__name__)


class OpenSearchConnector(SSHSupportMixin, AWSSupportMixin, CVECheckMixin):
    """
    Connector for OpenSearch clusters with multi-node SSH support.

    This connector provides a unified interface for:
    1. REST API queries (via opensearch-py client)
    2. Shell commands (via SSH for system-level metrics)
    3. Multi-node cluster operations

    Query Formats:
        REST API: Python dict with operation details
            {"operation": "cluster_health"}
            {"operation": "cat_nodes"}
            {"operation": "cat_indices"}

        Shell command:
            {"operation": "shell", "command": "df -h"}

    SSH Configuration (required for OS-level checks):
        - ssh_hosts: List of hostnames/IPs of OpenSearch nodes
        - ssh_user: SSH username
        - ssh_key_file: Path to private key (or ssh_password)
        - ssh_timeout: Connection timeout in seconds (default: 10)

    Example:
        connector = OpenSearchConnector(settings)
        connector.connect()

        # Get cluster health
        result = connector.execute_query('{"operation": "cluster_health"}')

        # Get node info
        result = connector.execute_query('{"operation": "cat_nodes"}')

        # Shell command
        result = connector.execute_query('{"operation": "shell", "command": "free -m"}')
    """

    def __init__(self, settings):
        """Initialize OpenSearch connector."""
        self.settings = settings
        self.client = None
        self._version_info = {}
        self.formatter = AsciiDocFormatter()

        # Multi-node support
        self.cluster_nodes = []  # List of discovered node addresses
        self.cluster_name = None

        # Environment detection
        self.environment = None  # 'aws', 'self_hosted'
        self.environment_details = {}

        # AWS support
        self.aws_manager = None
        self._opensearch_client = None  # AWS OpenSearch Service client
        self._cloudwatch_client = None

        # Technology name for CVE lookups
        self.technology_name = 'opensearch'

        # Initialize SSH support (from mixin)
        self.initialize_ssh()

        # Initialize CVE support (from mixin)
        self.initialize_cve_support()

        logger.info("OpenSearch connector initialized")

    def _detect_environment(self):
        """
        Detect whether this is AWS OpenSearch Service or self-hosted.

        Returns:
            tuple: (environment_type, details_dict)
        """
        try:
            # Check if AWS settings are provided
            is_aws_configured = bool(
                self.settings.get('aws_region') or
                self.settings.get('is_aws_opensearch') or
                self.settings.get('aws_domain_name')
            )

            if is_aws_configured:
                details = {
                    'type': 'aws',
                    'region': self.settings.get('aws_region'),
                    'domain_name': self.settings.get('aws_domain_name'),
                    'endpoint': self.settings.get('host')
                }
                logger.info("Detected AWS OpenSearch Service environment")
                return 'aws', details
            else:
                # Self-hosted OpenSearch
                details = {
                    'type': 'self_hosted',
                    'hosts': self.settings.get('hosts') or [self.settings.get('host')]
                }
                logger.info("Detected self-hosted OpenSearch environment")
                return 'self_hosted', details

        except Exception as e:
            logger.warning(f"Could not detect environment: {e}")
            return 'unknown', {}

    def _initialize_aws_clients(self):
        """Initialize AWS clients for CloudWatch metrics and OpenSearch Service API."""
        if self.environment != 'aws':
            return

        try:
            # Initialize AWS connection manager
            self.aws_manager = AWSConnectionManager(self.settings)
            self._cloudwatch_client = self.aws_manager.cloudwatch_client

            # Try to initialize OpenSearch Service client (boto3 1.26.0+)
            try:
                import boto3
                self._opensearch_client = boto3.client(
                    'opensearch',
                    region_name=self.settings.get('aws_region'),
                    aws_access_key_id=self.settings.get('aws_access_key_id'),
                    aws_secret_access_key=self.settings.get('aws_secret_access_key')
                )
                logger.info("✅ AWS OpenSearch Service client initialized")
            except Exception as e:
                logger.warning(f"Could not initialize OpenSearch Service client: {e}")
                self._opensearch_client = None

        except Exception as e:
            logger.warning(f"AWS initialization failed: {e}")
            self.aws_manager = None

    def connect(self):
        """Establishes connections to OpenSearch cluster and all SSH hosts."""
        try:
            # 1. Detect environment (AWS vs self-hosted)
            self.environment, self.environment_details = self._detect_environment()

            # 2. Connect to OpenSearch via REST API
            hosts = self.settings.get('hosts', [{'host': self.settings.get('host', 'localhost'),
                                                   'port': self.settings.get('port', 9200)}])

            # Handle hosts as list of strings or dicts
            if isinstance(hosts, list) and len(hosts) > 0 and isinstance(hosts[0], str):
                # Convert ["host:port", "host:port"] to [{'host': 'host', 'port': port}]
                parsed_hosts = []
                for h in hosts:
                    if ':' in h:
                        host, port = h.split(':', 1)
                        parsed_hosts.append({'host': host, 'port': int(port)})
                    else:
                        parsed_hosts.append({'host': h, 'port': 9200})
                hosts = parsed_hosts
            elif not isinstance(hosts, list):
                # Single host string
                hosts = [{'host': self.settings.get('host', 'localhost'),
                         'port': self.settings.get('port', 9200)}]

            # Configure authentication
            http_auth = None
            if self.settings.get('user') and self.settings.get('password'):
                http_auth = (self.settings['user'], self.settings['password'])

            self.client = OpenSearch(
                hosts=hosts,
                http_auth=http_auth,
                use_ssl=self.settings.get('use_ssl', True),
                verify_certs=self.settings.get('verify_certs', False),
                ssl_assert_hostname=self.settings.get('ssl_assert_hostname', False),
                ssl_show_warn=False,
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )

            # 2. Get cluster info and version
            cluster_info = self.client.info()
            version_dict = cluster_info.get('version', {})
            version_number = version_dict.get('number', 'Unknown')

            # Extract major version
            major_version = 0
            if version_number and version_number != 'Unknown':
                try:
                    import re
                    match = re.match(r'(\d+)', version_number)
                    if match:
                        major_version = int(match.group(1))
                except (ValueError, AttributeError):
                    logger.debug(f"Could not extract major version from: {version_number}")
                    major_version = 0

            # Store version info with major_version added
            self._version_info = {
                **version_dict,
                'version_string': version_number,
                'major_version': major_version
            }
            self.cluster_name = cluster_info.get('cluster_name', 'Unknown')

            # 3. Discover cluster topology
            self._discover_cluster_topology()

            # 4. Initialize AWS clients if in AWS environment
            if self.environment == 'aws':
                self._initialize_aws_clients()

            # 5. Connect all SSH hosts if configured (self-hosted only)
            connected_ssh_hosts = []
            if self.environment == 'self_hosted' and self.has_ssh_support():
                connected_ssh_hosts = self.connect_all_ssh()
                if connected_ssh_hosts:
                    self._map_ssh_hosts_to_nodes()

            # 6. Display enhanced connection status
            self._display_connection_status(connected_ssh_hosts)

            logger.info("✅ Connected to OpenSearch cluster")

        except Exception as e:
            logger.error(f"Failed to connect to OpenSearch: {e}")
            raise ConnectionError(f"Could not connect to OpenSearch: {e}")

    def _discover_cluster_topology(self):
        """Discovers all nodes in the OpenSearch cluster."""
        try:
            # Get node information using cat API
            nodes_response = self.client.cat.nodes(
                format='json',
                h='name,ip,node.role,master'
            )

            self.cluster_nodes = []
            for node in nodes_response:
                node_info = {
                    'name': node.get('name'),
                    'ip': node.get('ip'),
                    'role': node.get('node.role', 'Unknown'),
                    'is_master': node.get('master') == '*'
                }
                self.cluster_nodes.append(node_info)

            logger.debug(f"Discovered {len(self.cluster_nodes)} nodes in cluster")

        except Exception as e:
            logger.warning(f"Could not discover cluster topology: {e}")
            self.cluster_nodes = []

    def _map_ssh_hosts_to_nodes(self):
        """Maps SSH hosts to OpenSearch node names/IPs."""
        try:
            # Build host-to-node mapping
            host_node_mapping = {}
            for node in self.cluster_nodes:
                node_ip = node['ip']
                node_name = node['name']
                # Map both IP and name to the node
                host_node_mapping[node_ip] = node_name
                if node_name:
                    host_node_mapping[node_name] = node_name

            # Use mixin's mapping method
            self.map_ssh_hosts_to_nodes(host_node_mapping)

        except Exception as e:
            logger.warning(f"Could not map SSH hosts to node IDs: {e}")

    def _display_connection_status(self, connected_ssh_hosts):
        """Displays detailed connection status."""
        print("✅ Successfully connected to OpenSearch cluster")
        print(f"   - Version: {self._version_info.get('number', 'Unknown')}")
        print(f"   - Cluster: {self.cluster_name}")
        print(f"   - Nodes: {len(self.cluster_nodes)}")
        print(f"   - Environment: {self.environment.upper()}")

        # Show AWS-specific details
        if self.environment == 'aws':
            if self.environment_details.get('region'):
                print(f"   - AWS Region: {self.environment_details['region']}")
            if self.environment_details.get('domain_name'):
                print(f"   - Domain: {self.environment_details['domain_name']}")
            if self.has_aws_support():
                print(f"   - AWS CloudWatch: ✅ Available")
            else:
                print(f"   - AWS CloudWatch: ⚠️  Not configured")

        # Show node details
        if self.cluster_nodes:
            print(f"   - Node Details:")
            for node in self.cluster_nodes[:5]:  # Show first 5
                master_indicator = " (Master)" if node['is_master'] else ""
                print(f"      • {node['name']} ({node['ip']}) - {node['role']}{master_indicator}")
            if len(self.cluster_nodes) > 5:
                print(f"      ... and {len(self.cluster_nodes) - 5} more")

        # SSH status (self-hosted only)
        if self.environment == 'self_hosted':
            if self.has_ssh_support():
                ssh_hosts = self.get_ssh_hosts()
                print(f"   - SSH: Connected to {len(connected_ssh_hosts)}/{len(ssh_hosts)} host(s)")

                unmapped_hosts = []
                for ssh_host in connected_ssh_hosts:
                    node_id = self.ssh_host_to_node.get(ssh_host)
                    if node_id:
                        print(f"      • {ssh_host} (Node: {node_id})")
                    else:
                        print(f"      • {ssh_host} (⚠️  Not recognized as cluster node)")
                        unmapped_hosts.append(ssh_host)

                if unmapped_hosts:
                    print(f"   ⚠️  WARNING: {len(unmapped_hosts)} SSH host(s) are not recognized as cluster nodes!")
                    print(f"      This may indicate nodes that are down or not part of the cluster.")
            else:
                print(f"   - SSH: Not configured (OS-level checks unavailable)")
        elif self.environment == 'aws':
            print(f"   - SSH: Not applicable (AWS managed service)")

    def get_cloudwatch_metrics(self, metric_names, period=300, hours_back=1):
        """
        Fetch CloudWatch metrics for AWS OpenSearch Service.

        Args:
            metric_names: List of metric names to fetch
            period: Metric period in seconds (default: 300 = 5 min)
            hours_back: How many hours of history to fetch

        Returns:
            dict: Metrics data {metric_name: data_points}
        """
        if not self.has_aws_support():
            logger.warning("CloudWatch metrics requested but AWS is not configured")
            return {}

        if not self.environment_details.get('domain_name'):
            logger.warning("AWS domain name not configured, cannot fetch CloudWatch metrics")
            return {}

        try:
            return self.aws_manager.get_cloudwatch_metrics(
                namespace='AWS/ES',  # AWS OpenSearch uses AWS/ES namespace
                metric_names=metric_names,
                dimensions=[
                    {'Name': 'DomainName', 'Value': self.environment_details['domain_name']},
                    {'Name': 'ClientId', 'Value': self.settings.get('aws_account_id', '')}
                ] if self.settings.get('aws_account_id') else [
                    {'Name': 'DomainName', 'Value': self.environment_details['domain_name']}
                ],
                period=period,
                hours_back=hours_back
            )
        except Exception as e:
            logger.error(f"Failed to fetch CloudWatch metrics: {e}")
            return {}

    def disconnect(self):
        """Closes connections to OpenSearch and all SSH hosts."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from OpenSearch cluster")

        # Disconnect all SSH (from mixin)
        self.disconnect_all_ssh()

    def close(self):
        """Alias for disconnect()."""
        self.disconnect()

    @property
    def version_info(self):
        """Returns version information."""
        return self._version_info

    def get_db_metadata(self):
        """
        Fetches cluster-level metadata including environment information.

        Returns:
            dict: {'version': str, 'db_name': str, 'environment': str, 'environment_details': dict}
        """
        try:
            return {
                'version': self._version_info.get('number', 'Unknown'),
                'db_name': self.cluster_name or 'Unknown',
                'environment': self.environment,
                'environment_details': self.environment_details
            }
        except Exception as e:
            logger.warning(f"Could not fetch metadata: {e}")
            return {
                'version': 'N/A',
                'db_name': 'N/A',
                'environment': 'unknown',
                'environment_details': {}
            }

    def execute_query(self, query, params=None, return_raw=False):
        """
        Executes OpenSearch REST API operations or shell commands via JSON dispatch.

        Supported operations:
        - cluster_health: Get cluster health status
        - cluster_stats: Get cluster statistics
        - cat_nodes: List all nodes
        - cat_indices: List all indices
        - node_stats: Get node statistics
        - index_stats: Get index statistics
        - cat_shards: List shard allocation
        - cat_allocation: Show shard allocation across nodes
        - hot_threads: Get hot threads for performance diagnostics
        - pending_tasks: Get pending cluster tasks
        - cat_segments: Get segment information
        - cat_recovery: Get shard recovery status
        - tasks: Get current tasks
        - cat_plugins: List installed plugins
        - shell: Execute shell command via SSH

        Args:
            query: JSON string or dict with operation details
            params: Additional parameters (optional)
            return_raw: If True, return raw response

        Returns:
            Query results as dict or list
        """
        try:
            # Parse query if string
            if isinstance(query, str):
                try:
                    query_dict = json.loads(query)
                except json.JSONDecodeError:
                    # Not JSON, treat as direct API call path
                    logger.warning(f"Query is not JSON, treating as simple string: {query}")
                    return {"error": "Invalid query format"}
            else:
                query_dict = query

            operation = query_dict.get('operation')

            # Dispatch to appropriate handler
            if operation == 'cluster_health':
                return self.client.cluster.health()

            elif operation == 'cluster_stats':
                return self.client.cluster.stats()

            elif operation == 'cat_nodes':
                return self.client.cat.nodes(
                    format='json',
                    h='name,ip,heap.percent,ram.percent,cpu,load_1m,load_5m,load_15m,node.role,master'
                )

            elif operation == 'cat_indices':
                return self.client.cat.indices(
                    format='json',
                    h='index,health,status,docs.count,store.size,pri,rep'
                )

            elif operation == 'node_stats':
                node_id = query_dict.get('node_id', '_all')
                metrics = query_dict.get('metrics', [])
                if metrics:
                    return self.client.nodes.stats(node_id=node_id, metric=metrics)
                else:
                    return self.client.nodes.stats(node_id=node_id)

            elif operation == 'index_stats':
                index_name = query_dict.get('index', '_all')
                return self.client.indices.stats(index=index_name)

            elif operation == 'cat_shards':
                return self.client.cat.shards(format='json')

            elif operation == 'cat_allocation':
                return self.client.cat.allocation(format='json')

            elif operation == 'hot_threads':
                # Get hot threads for performance diagnostics
                try:
                    # Hot threads returns text, not JSON
                    response = self.client.nodes.hot_threads()
                    return {"hot_threads": response}
                except Exception as e:
                    return {"error": f"Hot threads unavailable: {e}"}

            elif operation == 'pending_tasks':
                # Get pending cluster tasks
                return self.client.cluster.pending_tasks()

            elif operation == 'cat_segments':
                # Get segment information
                return {"segments": self.client.cat.segments(format='json')}

            elif operation == 'cat_recovery':
                # Get recovery status
                return {"recovery": self.client.cat.recovery(format='json', active_only=False)}

            elif operation == 'tasks':
                # Get current tasks
                return self.client.tasks.list(detailed=True)

            elif operation == 'cat_plugins':
                # Get installed plugins
                return {"plugins": self.client.cat.plugins(format='json')}

            elif operation == 'shell':
                # Execute shell command via SSH
                command = query_dict.get('command')
                if not command:
                    return {"error": "No command specified for shell operation"}

                if not self.has_ssh_support():
                    return {"error": "SSH not configured"}

                # Execute on all hosts
                results = self.execute_ssh_on_all_hosts(command, "Shell command")
                return results

            elif operation == 'cat_thread_pool':
                # Get thread pool stats
                return self.client.cat.thread_pool(format='json', h='node_name,name,active,queue,rejected,completed')

            elif operation == 'allocation':
                # Get shard allocation details
                try:
                    return self.client.cat.allocation(format='json', v=True)
                except Exception as e:
                    return {"error": f"Allocation data unavailable: {e}"}

            elif operation == 'allocation_explain':
                # Get allocation explain (why shards are unassigned)
                try:
                    return self.client.cluster.allocation_explain()
                except Exception as e:
                    # No unassigned shards - this is normal
                    return {"error": f"No unassigned shards or unavailable: {e}"}

            elif operation == 'cluster_settings':
                # Get cluster settings
                return self.client.cluster.get_settings(include_defaults=True)

            elif operation == 'nodes' or operation == 'nodes_info':
                # Get detailed node information
                return self.client.nodes.info()

            elif operation == 'mappings':
                # Get index mappings
                index = query_dict.get('index', '_all')
                return self.client.indices.get_mapping(index=index)

            elif operation == 'settings':
                # Get index settings
                index = query_dict.get('index', '_all')
                return self.client.indices.get_settings(index=index)

            elif operation == 'aliases':
                # Get index aliases
                return self.client.indices.get_alias()

            elif operation == 'templates':
                # Get legacy templates
                return self.client.indices.get_template()

            elif operation == 'index_templates':
                # Get index templates (v2)
                try:
                    return self.client.indices.get_index_template()
                except Exception as e:
                    return {"error": f"Index templates unavailable: {e}"}

            elif operation == 'component_templates':
                # Get component templates
                try:
                    return self.client.cluster.get_component_template()
                except Exception as e:
                    return {"error": f"Component templates unavailable: {e}"}

            elif operation == 'pipelines':
                # Get ingest pipelines
                try:
                    return self.client.ingest.get_pipeline()
                except Exception as e:
                    return {"error": f"Ingest pipelines unavailable: {e}"}

            elif operation == 'repositories':
                # Get snapshot repositories
                try:
                    return self.client.snapshot.get_repository()
                except Exception as e:
                    return {"error": f"Repositories unavailable: {e}"}

            elif operation == 'licenses':
                # Get license information (Elasticsearch)
                try:
                    response = self.client.transport.perform_request('GET', '/_license')
                    return response
                except Exception as e:
                    return {"error": f"License info unavailable: {e}"}

            elif operation == 'ssl_certs':
                # Get SSL certificate information (Elasticsearch/OpenSearch security plugin)
                try:
                    response = self.client.transport.perform_request('GET', '/_ssl/certificates')
                    return response
                except Exception as e:
                    return {"error": f"SSL certificates unavailable: {e}"}

            # Commercial/X-Pack features
            elif operation == 'ilm_policies':
                try:
                    response = self.client.transport.perform_request('GET', '/_ilm/policy')
                    return response
                except Exception as e:
                    return {"error": f"ILM policies unavailable: {e}"}

            elif operation == 'ilm_status':
                try:
                    response = self.client.transport.perform_request('GET', '/_ilm/status')
                    return response
                except Exception as e:
                    return {"error": f"ILM status unavailable: {e}"}

            elif operation == 'slm_policies':
                try:
                    response = self.client.transport.perform_request('GET', '/_slm/policy')
                    return response
                except Exception as e:
                    return {"error": f"SLM policies unavailable: {e}"}

            elif operation == 'slm_stats':
                try:
                    response = self.client.transport.perform_request('GET', '/_slm/stats')
                    return response
                except Exception as e:
                    return {"error": f"SLM stats unavailable: {e}"}

            elif operation == 'watcher_stats':
                try:
                    response = self.client.transport.perform_request('GET', '/_watcher/stats')
                    return response
                except Exception as e:
                    return {"error": f"Watcher stats unavailable: {e}"}

            elif operation == 'ccr_stats':
                try:
                    response = self.client.transport.perform_request('GET', '/_ccr/stats')
                    return response
                except Exception as e:
                    return {"error": f"CCR stats unavailable: {e}"}

            elif operation == 'ccr_autofollow':
                try:
                    response = self.client.transport.perform_request('GET', '/_ccr/auto_follow')
                    return response
                except Exception as e:
                    return {"error": f"CCR auto-follow unavailable: {e}"}

            elif operation == 'ml_info':
                try:
                    response = self.client.transport.perform_request('GET', '/_ml/info')
                    return response
                except Exception as e:
                    return {"error": f"ML info unavailable: {e}"}

            elif operation == 'ml_anomaly_detectors':
                try:
                    response = self.client.transport.perform_request('GET', '/_ml/anomaly_detectors')
                    return response
                except Exception as e:
                    return {"error": f"ML anomaly detectors unavailable: {e}"}

            elif operation == 'ml_datafeeds':
                try:
                    response = self.client.transport.perform_request('GET', '/_ml/datafeeds')
                    return response
                except Exception as e:
                    return {"error": f"ML datafeeds unavailable: {e}"}

            elif operation == 'transform':
                try:
                    response = self.client.transport.perform_request('GET', '/_transform')
                    return response
                except Exception as e:
                    return {"error": f"Transforms unavailable: {e}"}

            elif operation == 'transform_stats':
                try:
                    response = self.client.transport.perform_request('GET', '/_transform/_stats')
                    return response
                except Exception as e:
                    return {"error": f"Transform stats unavailable: {e}"}

            elif operation == 'data_stream':
                try:
                    response = self.client.transport.perform_request('GET', '/_data_stream')
                    return response
                except Exception as e:
                    return {"error": f"Data streams unavailable: {e}"}

            elif operation == 'rollup_jobs':
                try:
                    response = self.client.transport.perform_request('GET', '/_rollup/job')
                    return response
                except Exception as e:
                    return {"error": f"Rollup jobs unavailable: {e}"}

            elif operation == 'enrich_policies':
                try:
                    response = self.client.transport.perform_request('GET', '/_enrich/policy')
                    return response
                except Exception as e:
                    return {"error": f"Enrich policies unavailable: {e}"}

            elif operation == 'enrich_stats':
                try:
                    response = self.client.transport.perform_request('GET', '/_enrich/_stats')
                    return response
                except Exception as e:
                    return {"error": f"Enrich stats unavailable: {e}"}

            elif operation == 'security_users':
                try:
                    response = self.client.transport.perform_request('GET', '/_security/user')
                    return response
                except Exception as e:
                    return {"error": f"Security users unavailable: {e}"}

            elif operation == 'security_roles':
                try:
                    response = self.client.transport.perform_request('GET', '/_security/role')
                    return response
                except Exception as e:
                    return {"error": f"Security roles unavailable: {e}"}

            elif operation == 'security_role_mappings':
                try:
                    response = self.client.transport.perform_request('GET', '/_security/role_mapping')
                    return response
                except Exception as e:
                    return {"error": f"Security role mappings unavailable: {e}"}

            elif operation == 'xpack':
                try:
                    response = self.client.transport.perform_request('GET', '/_xpack/usage')
                    return response
                except Exception as e:
                    return {"error": f"X-Pack usage unavailable: {e}"}

            elif operation == 'dangling_indices':
                try:
                    response = self.client.transport.perform_request('GET', '/_dangling')
                    return response
                except Exception as e:
                    return {"error": f"Dangling indices unavailable: {e}"}

            elif operation == 'remote_info':
                try:
                    response = self.client.transport.perform_request('GET', '/_remote/info')
                    return response
                except Exception as e:
                    return {"error": f"Remote cluster info unavailable: {e}"}

            elif operation == 'cat_nodeattrs':
                try:
                    response = self.client.cat.nodeattrs(format='json')
                    return {"nodeattrs": response}
                except Exception as e:
                    return {"error": f"Node attributes unavailable: {e}"}

            else:
                return {"error": f"Unsupported operation: {operation}"}

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {"error": str(e)}

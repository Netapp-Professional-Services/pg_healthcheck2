import psycopg2
import subprocess
import logging
import re
from datetime import datetime
from plugins.base import BasePlugin
from plugins.common.ssh_mixin import SSHSupportMixin
from plugins.common.cve_mixin import CVECheckMixin
from plugins.common.output_formatters import AsciiDocFormatter

logger = logging.getLogger(__name__)


class PostgresConnector(SSHSupportMixin, CVECheckMixin):
    """
    Enhanced PostgreSQL connector with cross-node and multi-environment support.

    Capabilities:
    - Auto-detects environment (Aurora, RDS, EC2, bare metal)
    - Discovers cluster topology (primary + replicas)
    - Supports cross-node queries
    - CloudWatch metrics for Aurora/RDS
    - SSH support for bare metal/EC2
    """

    def __init__(self, settings):
        self.settings = settings
        self.conn = None  # Primary connection (may be via proxy)
        self.cursor = None
        self.version_info = {}
        self.has_pgstat = False
        self.has_pgstat_legacy_io_time = False
        self.has_pgstat_new_io_time = False

        # Direct connection (bypasses proxies for Patroni/cluster checks)
        self.patroni_direct_conn = None
        self.patroni_direct_cursor = None
        self.has_direct_connection = False

        # Connection health tracking
        self.reconnection_count = 0
        self.connection_failures = []

        # Fallback tracking - when queries fail through primary (PgBouncer) but succeed via direct
        self.fallback_stats = {
            'count': 0,
            'queries': [],  # List of {query, timestamp, primary_error} dicts
            'last_reset': None
        }

        # Multi-environment support
        self.environment = None  # 'aurora', 'rds', 'ec2', 'bare_metal'
        self.environment_details = {}

        # Cross-node support
        self.cluster_topology = []
        self.replica_conns = {}  # {node_host: connection}

        # AWS support
        self._rds_client = None
        self._cloudwatch_client = None
        self._aws_region = None

        # Formatters
        self.formatter = AsciiDocFormatter()

        # Technology identifier for CVE lookups
        self.technology_name = 'postgres'

        # Initialize SSH support (from mixin)
        self.initialize_ssh()

        # Initialize CVE support (from mixin)
        self.initialize_cve_support()

    def connect(self):
        """
        Enhanced connection with environment detection and topology discovery.
        """
        try:
            # 1. Connect to primary database
            timeout = self.settings.get('statement_timeout', 30000)

            # Check if connecting via PgBouncer (doesn't support options parameter)
            is_pgbouncer = bool(self.settings.get('pgbouncer_host'))

            if is_pgbouncer:
                # PgBouncer doesn't support startup parameters in options
                # Explicitly pass empty options to override PGOPTIONS environment variable
                self.conn = psycopg2.connect(
                    host=self.settings['host'],
                    port=self.settings['port'],
                    dbname=self.settings['database'],
                    user=self.settings['user'],
                    password=self.settings['password'],
                    options=""  # Override PGOPTIONS env var
                )
            else:
                # Direct PostgreSQL connection - can use options
                self.conn = psycopg2.connect(
                    host=self.settings['host'],
                    port=self.settings['port'],
                    dbname=self.settings['database'],
                    user=self.settings['user'],
                    password=self.settings['password'],
                    options=f"-c statement_timeout={timeout}"
                )

            self.conn.autocommit = self.settings.get('autocommit', True)
            self.cursor = self.conn.cursor()

            # Set statement timeout after connection for PgBouncer
            if is_pgbouncer and timeout:
                try:
                    self.cursor.execute(f"SET statement_timeout = {timeout}")
                except Exception as e:
                    logger.debug(f"Could not set statement_timeout via SET command: {e}")

            # 2. Get version info
            self.version_info = self._get_version_info()

            # 3. Detect environment
            self.environment, self.environment_details = self._detect_environment()

            # 3.5. Connect directly to Patroni if configured (bypasses proxies)
            self._connect_patroni_direct()

            # 4. Discover cluster topology
            self.cluster_topology = self._discover_cluster_topology()

            # 5. Connect to replicas if configured
            if self.settings.get('connect_to_replicas', False):
                self._connect_all_replicas()

            # 6. Initialize AWS clients if needed
            if self.environment in ['aurora', 'rds']:
                self._initialize_aws_clients()

            # 7. Connect SSH hosts if configured
            if self.has_ssh_support():
                connected_ssh_hosts = self.connect_all_ssh()
                if connected_ssh_hosts:
                    self._map_ssh_hosts_to_nodes()

            # 8. Check pg_stat_statements capabilities
            self._check_pg_stat_capabilities()

            # 9. Display enhanced connection status
            self._display_connection_status()

        except psycopg2.Error as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise

    def _connect_patroni_direct(self):
        """
        Establish direct connection to Patroni, bypassing proxies.

        This connection is used for:
        - Patroni topology detection
        - Patroni failover history
        - Patroni configuration
        - DCS health checks

        While the primary connection may go through PgBouncer/HAProxy/etc,
        this direct connection ensures we can access Patroni-specific metadata.
        """
        # Check if direct connection is configured
        patroni_direct = self.settings.get('patroni_direct', {})
        if not patroni_direct or not patroni_direct.get('enabled'):
            logger.debug("Direct Patroni connection not configured")
            return

        try:
            logger.info("Connecting directly to Patroni (bypassing proxies)...")

            timeout = patroni_direct.get('statement_timeout', 30000)

            self.patroni_direct_conn = psycopg2.connect(
                host=patroni_direct['host'],
                port=patroni_direct.get('port', 5432),
                dbname=patroni_direct.get('database', 'postgres'),
                user=patroni_direct['user'],
                password=patroni_direct['password'],
                connect_timeout=patroni_direct.get('connect_timeout', 10),
                options=f"-c statement_timeout={timeout}"
            )
            self.patroni_direct_conn.autocommit = True
            self.patroni_direct_cursor = self.patroni_direct_conn.cursor()
            self.has_direct_connection = True

            logger.info(f"✅ Direct Patroni connection established: {patroni_direct['host']}:{patroni_direct.get('port', 5432)}")

        except Exception as e:
            logger.warning(f"Could not establish direct Patroni connection: {e}")
            logger.warning("Patroni-specific checks may be limited")
            self.has_direct_connection = False

    def _detect_environment(self):
        """
        Auto-detect PostgreSQL environment.

        Returns:
            tuple: (environment_type, details)
        """
        details = {}

        # Check for explicit override
        if self.settings.get('environment_override'):
            env = self.settings['environment_override']
            logger.info(f"Using explicit environment override: {env}")
            details['detection_method'] = 'explicit_override'
            return env, details

        # Legacy is_aurora flag support
        if self.settings.get('is_aurora'):
            logger.info("Using legacy 'is_aurora' flag")
            details['detection_method'] = 'legacy_is_aurora_flag'
            return 'aurora', details

        # Detect Aurora via database queries
        aurora_detected, aurora_details = self._detect_aurora()
        if aurora_detected:
            details.update(aurora_details)
            details['detection_method'] = 'aurora_system_functions'
            return 'aurora', details

        # Detect RDS
        rds_detected, rds_details = self._detect_rds()
        if rds_detected:
            details.update(rds_details)
            details['detection_method'] = 'rds_indicators'
            return 'rds', details

        # Detect Patroni
        patroni_detected, patroni_details = self._detect_patroni()
        if patroni_detected:
            details.update(patroni_details)
            details['detection_method'] = 'patroni_indicators'
            details['ha_solution'] = 'patroni'
            return 'patroni', details

        # Default to bare_metal
        details['detection_method'] = 'default'
        return 'bare_metal', details

    def _detect_aurora(self):
        """Detect Aurora using database-level signals."""
        details = {}
        confidence_score = 0

        try:
            cursor = self.conn.cursor()

            # Signal 1: Version string contains 'Aurora'
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            if 'Aurora' in version:
                confidence_score += 40
                details['version_string'] = version
                logger.debug("Aurora detected in version string")

            # Signal 2: aurora_version() function exists
            try:
                cursor.execute("SELECT aurora_version();")
                aurora_version = cursor.fetchone()[0]
                confidence_score += 30
                details['aurora_version'] = aurora_version
                logger.debug(f"aurora_version() returned: {aurora_version}")
            except psycopg2.Error:
                pass

            # Signal 3: RDS-specific GUC parameters
            try:
                cursor.execute("""
                    SELECT name, setting
                    FROM pg_settings
                    WHERE name LIKE 'rds.%' OR name LIKE 'apg.%'
                    LIMIT 5;
                """)
                rds_params = cursor.fetchall()
                if rds_params:
                    confidence_score += 20
                    details['rds_parameters_count'] = len(rds_params)
                    logger.debug(f"Found {len(rds_params)} RDS/Aurora parameters")
            except psycopg2.Error:
                pass

            is_aurora = confidence_score >= 40
            details['confidence_score'] = confidence_score

            if is_aurora:
                logger.info(f"✅ Aurora detected (confidence: {confidence_score}%)")

            return is_aurora, details

        except Exception as e:
            logger.error(f"Error during Aurora detection: {e}")
            return False, {'error': str(e)}

    def _detect_rds(self):
        """Detect standard RDS (non-Aurora) PostgreSQL."""
        details = {}
        confidence_score = 0

        try:
            cursor = self.conn.cursor()

            # Signal 1: AWS credentials in settings
            if self.settings.get('aws_region') and self.settings.get('db_identifier'):
                confidence_score += 20
                details['aws_region'] = self.settings['aws_region']

            # Signal 2: rds_superuser role exists
            try:
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'rds_superuser';")
                if cursor.fetchone():
                    confidence_score += 40
                    details['rds_superuser_exists'] = True
                    logger.debug("rds_superuser role found")
            except psycopg2.Error:
                pass

            is_rds = confidence_score >= 40
            details['confidence_score'] = confidence_score

            if is_rds:
                logger.info(f"✅ RDS detected (confidence: {confidence_score}%)")

            return is_rds, details

        except Exception as e:
            logger.error(f"Error during RDS detection: {e}")
            return False, {'error': str(e)}

    def _detect_patroni(self):
        """
        Detect Patroni HA cluster using multiple detection methods.

        Patroni is a template for PostgreSQL HA using Python and a distributed
        configuration store (etcd, Consul, ZooKeeper, or Kubernetes).

        Uses the direct connection if available, otherwise falls back to primary connection.

        Returns:
            tuple: (is_patroni: bool, details: dict)
        """
        details = {}
        confidence_score = 0
        detection_methods = []

        try:
            # Use direct connection for Patroni detection if available
            # This ensures we can detect Patroni even when connecting via proxies
            if self.has_direct_connection:
                cursor = self.patroni_direct_cursor
                detection_conn = self.patroni_direct_conn
                details['detection_connection'] = 'direct'
                logger.debug("Using direct connection for Patroni detection")
            else:
                cursor = self.conn.cursor()
                detection_conn = self.conn
                details['detection_connection'] = 'primary'
                logger.debug("Using primary connection for Patroni detection")

            # Signal 1: Check for Patroni-created replication slots
            # Patroni typically creates slots named like 'patroni' or with cluster name
            try:
                cursor.execute("""
                    SELECT slot_name, slot_type, active
                    FROM pg_replication_slots
                    WHERE slot_name LIKE '%patroni%'
                       OR slot_name LIKE '%pgsql%'
                       OR slot_name ~ '^[a-z]+-cluster-[0-9]+'
                    LIMIT 5;
                """)
                patroni_slots = cursor.fetchall()
                if patroni_slots:
                    confidence_score += 30
                    details['patroni_replication_slots'] = [
                        {'name': row[0], 'type': row[1], 'active': row[2]}
                        for row in patroni_slots
                    ]
                    detection_methods.append('replication_slots')
                    logger.debug(f"Found {len(patroni_slots)} Patroni-like replication slots")
            except psycopg2.Error as e:
                logger.debug(f"Could not check replication slots: {e}")

            # Signal 2: Check for Patroni-specific application names in pg_stat_activity
            try:
                cursor.execute("""
                    SELECT DISTINCT application_name
                    FROM pg_stat_activity
                    WHERE application_name ILIKE '%patroni%'
                       OR application_name ILIKE '%pgsql%'
                    LIMIT 5;
                """)
                patroni_apps = cursor.fetchall()
                if patroni_apps:
                    confidence_score += 25
                    details['patroni_applications'] = [row[0] for row in patroni_apps]
                    detection_methods.append('application_names')
                    logger.debug(f"Found Patroni application names: {[row[0] for row in patroni_apps]}")
            except psycopg2.Error as e:
                logger.debug(f"Could not check pg_stat_activity: {e}")

            # Signal 3: Check for Patroni REST API (default port 8008)
            # Try to detect if Patroni REST API is accessible
            try:
                import requests
                from requests.exceptions import RequestException

                patroni_host = self.settings.get('host')
                patroni_port = self.settings.get('patroni_port', 8008)

                # Try multiple endpoints
                endpoints_to_check = [
                    f"http://{patroni_host}:{patroni_port}/patroni",
                    f"http://{patroni_host}:{patroni_port}/health",
                    f"http://{patroni_host}:{patroni_port}/leader"
                ]

                for endpoint in endpoints_to_check:
                    try:
                        response = requests.get(endpoint, timeout=2)
                        if response.status_code in [200, 503]:  # 503 = replica in Patroni
                            confidence_score += 35
                            details['patroni_api_endpoint'] = endpoint
                            details['patroni_api_accessible'] = True
                            detection_methods.append('rest_api')

                            # Try to parse response for cluster info
                            try:
                                api_data = response.json()
                                if 'role' in api_data or 'state' in api_data or 'cluster' in api_data:
                                    details['patroni_node_role'] = api_data.get('role', 'unknown')
                                    details['patroni_state'] = api_data.get('state', 'unknown')
                                    details['patroni_cluster'] = api_data.get('cluster', 'unknown')
                                    confidence_score += 10
                                    logger.debug(f"Patroni API returned: role={api_data.get('role')}, state={api_data.get('state')}")
                            except Exception:
                                pass
                            break
                    except RequestException:
                        continue
            except ImportError:
                logger.debug("requests library not available for Patroni API detection")
            except Exception as e:
                logger.debug(f"Could not check Patroni REST API: {e}")

            # Signal 4: Check for Patroni via SSH (if SSH is configured)
            if self.has_ssh_support():
                try:
                    # Check for Patroni process
                    result = self.execute_ssh_command("ps aux | grep -i '[p]atroni' | head -1")
                    if result and result['success'] and result['stdout']:
                        confidence_score += 20
                        details['patroni_process_detected'] = True
                        details['patroni_process_info'] = result['stdout'].strip()[:200]
                        detection_methods.append('process_check')
                        logger.debug("Patroni process detected via SSH")

                    # Check for Patroni config file
                    config_paths = [
                        '/etc/patroni/patroni.yml',
                        '/etc/patroni.yml',
                        '/var/lib/postgresql/patroni.yml'
                    ]
                    for config_path in config_paths:
                        result = self.execute_ssh_command(f"test -f {config_path} && echo 'exists' || echo 'not_found'")
                        if result and result['success'] and 'exists' in result['stdout']:
                            confidence_score += 15
                            details['patroni_config_file'] = config_path
                            detection_methods.append('config_file')
                            logger.debug(f"Patroni config found at {config_path}")
                            break
                except Exception as e:
                    logger.debug(f"Could not check Patroni via SSH: {e}")

            # Signal 5: Check PostgreSQL configuration for Patroni indicators
            try:
                cursor.execute("""
                    SELECT name, setting
                    FROM pg_settings
                    WHERE name IN ('archive_command', 'restore_command', 'primary_conninfo')
                       AND setting ILIKE '%patroni%'
                    LIMIT 5;
                """)
                patroni_settings = cursor.fetchall()
                if patroni_settings:
                    confidence_score += 15
                    details['patroni_config_params'] = {row[0]: row[1] for row in patroni_settings}
                    detection_methods.append('config_params')
                    logger.debug(f"Found Patroni references in PostgreSQL config")
            except psycopg2.Error as e:
                logger.debug(f"Could not check PostgreSQL settings: {e}")

            # Calculate final result
            is_patroni = confidence_score >= 30  # Threshold for Patroni detection
            details['confidence_score'] = confidence_score
            details['detection_methods'] = detection_methods

            if is_patroni:
                logger.info(f"✅ Patroni cluster detected (confidence: {confidence_score}%, methods: {', '.join(detection_methods)})")
            else:
                logger.debug(f"Patroni not detected (confidence: {confidence_score}%)")

            return is_patroni, details

        except Exception as e:
            logger.error(f"Error during Patroni detection: {e}")
            return False, {'error': str(e)}

    def _discover_cluster_topology(self):
        """
        Discover cluster topology (primary + replicas).

        Returns:
            list: Node information dictionaries
        """
        if self.environment == 'aurora':
            return self._discover_aurora_topology()
        else:
            return self._discover_standard_topology()

    def _discover_aurora_topology(self):
        """Discover Aurora cluster topology via RDS API."""
        topology = []

        try:
            # Extract cluster ID from endpoint
            cluster_id = self._extract_cluster_id_from_endpoint()
            if not cluster_id and self.settings.get('db_cluster_id'):
                cluster_id = self.settings['db_cluster_id']

            if not cluster_id:
                logger.warning("Could not determine Aurora cluster ID")
                return topology

            # Initialize boto3 client
            import boto3
            region = self._extract_region_from_endpoint()
            if not region and self.settings.get('aws_region'):
                region = self.settings['aws_region']

            if not region:
                logger.warning("Could not determine AWS region")
                return topology

            rds_client = boto3.client('rds', region_name=region)

            # Get cluster info
            response = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
            cluster_info = response['DBClusters'][0]

            # Add cluster-level endpoints
            if cluster_info.get('Endpoint'):
                topology.append({
                    'host': cluster_info['Endpoint'],
                    'port': cluster_info.get('Port', 5432),
                    'role': 'writer',
                    'endpoint_type': 'cluster',
                    'cluster_id': cluster_id
                })

            if cluster_info.get('ReaderEndpoint'):
                topology.append({
                    'host': cluster_info['ReaderEndpoint'],
                    'port': cluster_info.get('Port', 5432),
                    'role': 'reader',
                    'endpoint_type': 'reader_lb',
                    'cluster_id': cluster_id
                })

            # Get instance-specific endpoints
            for member in cluster_info['DBClusterMembers']:
                instance_id = member['DBInstanceIdentifier']
                is_writer = member['IsClusterWriter']

                instance_response = rds_client.describe_db_instances(
                    DBInstanceIdentifier=instance_id
                )
                instance_info = instance_response['DBInstances'][0]

                topology.append({
                    'host': instance_info['Endpoint']['Address'],
                    'port': instance_info['Endpoint']['Port'],
                    'role': 'writer' if is_writer else 'reader',
                    'endpoint_type': 'instance',
                    'instance_id': instance_id,
                    'instance_class': instance_info['DBInstanceClass'],
                    'availability_zone': instance_info['AvailabilityZone'],
                    'status': instance_info['DBInstanceStatus']
                })

            logger.info(f"Discovered Aurora topology: {len(topology)} endpoints")
            return topology

        except Exception as e:
            logger.error(f"Failed to discover Aurora topology: {e}")
            return []

    def _extract_cluster_id_from_endpoint(self):
        """Extract cluster ID from Aurora endpoint."""
        try:
            host = self.settings['host']
            # Pattern: cluster-name.cluster-xxx.region.rds.amazonaws.com
            if '.cluster-' in host:
                parts = host.split('.cluster-')
                if len(parts) >= 2:
                    cluster_name = parts[0]
                    return cluster_name
        except Exception as e:
            logger.debug(f"Could not extract cluster ID from endpoint: {e}")
        return None

    def _extract_region_from_endpoint(self):
        """Extract AWS region from RDS endpoint."""
        try:
            host = self.settings['host']
            # Pattern: xxx.region.rds.amazonaws.com
            if '.rds.amazonaws.com' in host:
                parts = host.split('.')
                if len(parts) >= 4:
                    # parts[-4] should be the region
                    return parts[-4]
        except Exception as e:
            logger.debug(f"Could not extract region from endpoint: {e}")
        return None

    def _discover_standard_topology(self):
        """Discover topology for standard PostgreSQL."""
        topology = []

        try:
            cursor = self.conn.cursor()

            # Get current host (primary)
            cursor.execute("SELECT inet_server_addr(), inet_server_port();")
            result = cursor.fetchone()
            primary_host = result[0] if result and result[0] else self.settings['host']
            primary_port = result[1] if result and result[1] else self.settings['port']

            topology.append({
                'host': str(primary_host),
                'port': primary_port,
                'role': 'writer',
                'endpoint_type': 'instance',
                'state': 'active'
            })

            # Get replicas from pg_stat_replication
            cursor.execute("""
                SELECT
                    client_addr,
                    client_hostname,
                    state,
                    sync_state,
                    COALESCE(
                        EXTRACT(EPOCH FROM (
                            CASE
                                WHEN replay_lsn IS NOT NULL
                                THEN now() - pg_last_xact_replay_timestamp()
                                ELSE NULL
                            END
                        )),
                        0
                    ) AS replication_lag_seconds
                FROM pg_stat_replication
                WHERE client_addr IS NOT NULL;
            """)

            for row in cursor.fetchall():
                topology.append({
                    'host': str(row[0]),
                    'hostname': row[1],
                    'port': self.settings['port'],
                    'role': 'reader',
                    'endpoint_type': 'instance',
                    'state': row[2],
                    'sync_state': row[3],
                    'replication_lag_seconds': float(row[4]) if row[4] else 0
                })

            logger.info(f"Discovered standard topology: {len(topology)} nodes")
            return topology

        except Exception as e:
            logger.error(f"Failed to discover standard topology: {e}")
            return []

    def _connect_all_replicas(self):
        """Connect to all replica nodes for cross-node checks."""
        for node in self.cluster_topology:
            if node['role'] == 'reader' and node['endpoint_type'] == 'instance':
                try:
                    conn = psycopg2.connect(
                        host=node['host'],
                        port=node['port'],
                        database=self.settings['database'],
                        user=self.settings['user'],
                        password=self.settings['password'],
                        connect_timeout=10
                    )
                    conn.autocommit = True
                    self.replica_conns[node['host']] = conn
                    logger.info(f"Connected to replica: {node['host']}")
                except Exception as e:
                    logger.warning(f"Could not connect to replica {node['host']}: {e}")

    def _initialize_aws_clients(self):
        """Initialize boto3 clients for CloudWatch."""
        try:
            import boto3

            region = self._extract_region_from_endpoint()
            if not region and self.settings.get('aws_region'):
                region = self.settings['aws_region']

            if not region:
                logger.warning("AWS region not configured")
                return

            self._aws_region = region
            self._cloudwatch_client = boto3.client('cloudwatch', region_name=region)
            self._rds_client = boto3.client('rds', region_name=region)
            logger.info(f"Initialized AWS clients for region: {region}")

        except ImportError:
            logger.warning("boto3 not installed - CloudWatch metrics unavailable")
        except Exception as e:
            logger.warning(f"Could not initialize AWS clients: {e}")

    def _map_ssh_hosts_to_nodes(self):
        """PostgreSQL-specific SSH host to node mapping."""
        try:
            # Build host-to-role mapping from topology
            host_node_mapping = {}
            for node in self.cluster_topology:
                if node['endpoint_type'] == 'instance':
                    role_label = node['role']
                    if node.get('instance_id'):
                        role_label = f"{node['role']} ({node['instance_id']})"
                    host_node_mapping[node['host']] = role_label

            # Use mixin's mapping method
            self.map_ssh_hosts_to_nodes(host_node_mapping)

        except Exception as e:
            logger.warning(f"Could not map SSH hosts to PostgreSQL nodes: {e}")

    def _display_connection_status(self):
        """Display enhanced connection status."""
        print("✅ Successfully connected to PostgreSQL")

        # Environment display
        env_display = self.environment.upper()
        if 'confidence_score' in self.environment_details:
            score = self.environment_details['confidence_score']
            env_display += f" (confidence: {score}%)"

        detection_method = self.environment_details.get('detection_method', 'unknown')

        print(f"   - Environment: {env_display}")
        print(f"   - Detection: {detection_method}")
        print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")

        # Aurora-specific info
        if self.environment == 'aurora' and 'aurora_version' in self.environment_details:
            print(f"   - Aurora Version: {self.environment_details['aurora_version']}")

        # Patroni-specific info
        if self.environment == 'patroni':
            print(f"   - HA Solution: Patroni")
            if 'patroni_node_role' in self.environment_details:
                print(f"   - Node Role: {self.environment_details['patroni_node_role']}")
            if 'patroni_state' in self.environment_details:
                print(f"   - State: {self.environment_details['patroni_state']}")
            if 'patroni_cluster' in self.environment_details:
                print(f"   - Cluster: {self.environment_details['patroni_cluster']}")
            if 'patroni_api_endpoint' in self.environment_details:
                print(f"   - REST API: {self.environment_details['patroni_api_endpoint']}")

            # Show detection methods used
            detection_methods = self.environment_details.get('detection_methods', [])
            if detection_methods:
                print(f"   - Detected via: {', '.join(detection_methods)}")

        # Node topology
        if self.cluster_topology:
            # Count nodes by endpoint type
            instance_nodes = [n for n in self.cluster_topology if n['endpoint_type'] == 'instance']
            print(f"   - Cluster: {len(instance_nodes)} instance(s)")

            for node in instance_nodes:
                role_display = node['role'].title()
                if node.get('instance_id'):
                    role_display += f" ({node['instance_id']})"
                if node.get('sync_state'):
                    role_display += f" - {node['sync_state']}"
                print(f"      • {node['host']} - {role_display}")

        # Cross-node connections
        if self.replica_conns:
            print(f"   - Cross-Node: Connected to {len(self.replica_conns)} replica(s)")

        # Capabilities
        capabilities = []
        if self.has_ssh_support():
            capabilities.append("SSH")
        if self._cloudwatch_client:
            capabilities.append("CloudWatch")
        if capabilities:
            print(f"   - Data Sources: {', '.join(capabilities)}")

        # SSH status
        if self.has_ssh_support():
            connected_ssh_hosts = list(self.get_ssh_hosts())
            if connected_ssh_hosts:
                print(f"   - SSH: Connected to {len(connected_ssh_hosts)} host(s)")
                unmapped_hosts = []
                for ssh_host in connected_ssh_hosts:
                    node_id = self.ssh_host_to_node.get(ssh_host)
                    if node_id:
                        print(f"      • {ssh_host} ({node_id})")
                    else:
                        print(f"      • {ssh_host} (⚠️  Not in replication topology)")
                        unmapped_hosts.append(ssh_host)

                if unmapped_hosts:
                    print(f"   ⚠️  WARNING: {len(unmapped_hosts)} SSH host(s) not in replication topology!")

        # Direct Patroni connection status
        if self.has_direct_connection:
            direct_config = self.settings.get('patroni_direct', {})
            print(f"   - Direct Patroni Connection: ✅ Active")
            print(f"      • Host: {direct_config.get('host')}:{direct_config.get('port', 5432)}")
            print(f"      • Purpose: Cluster topology & Patroni checks")

        # pg_stat_statements info
        print(f"   - pg_stat_statements: {'Enabled' if self.has_pgstat else 'Not Found'}")
        if self.has_pgstat:
            io_status = "Not Available"
            if self.has_pgstat_new_io_time:
                io_status = "Available (PG17+ Style)"
            elif self.has_pgstat_legacy_io_time:
                io_status = "Available (Legacy Style)"
            print(f"   - I/O Timings: {io_status}")

    def execute_on_all_nodes(self, query, include_replicas=True):
        """
        Execute query on all nodes (primary + replicas).

        Args:
            query: SQL query to execute
            include_replicas: Whether to query replicas

        Returns:
            dict: {'primary': [rows...], 'replica_host1': [rows...], ...}
        """
        results = {}

        # Execute on primary
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            results['primary'] = cursor.fetchall()
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to execute on primary: {e}")
            results['primary'] = {'error': str(e)}

        # Execute on replicas
        if include_replicas:
            for host, conn in self.replica_conns.items():
                try:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    results[host] = cursor.fetchall()
                    cursor.close()
                except Exception as e:
                    logger.error(f"Failed to execute on {host}: {e}")
                    results[host] = {'error': str(e)}

        return results

    def supports_cross_node_queries(self):
        """Check if cross-node queries are supported."""
        return len(self.replica_conns) > 0

    def get_cluster_endpoints(self):
        """Get distinct connection endpoints."""
        endpoints = {
            'writer': None,
            'reader': None,
            'instances': {'writer': [], 'readers': []}
        }

        for node in self.cluster_topology:
            if node['endpoint_type'] == 'cluster' and node['role'] == 'writer':
                endpoints['writer'] = node['host']
            elif node['endpoint_type'] == 'reader_lb':
                endpoints['reader'] = node['host']
            elif node['endpoint_type'] == 'instance':
                if node['role'] == 'writer':
                    endpoints['instances']['writer'].append(node['host'])
                else:
                    endpoints['instances']['readers'].append(node['host'])

        return endpoints

    def get_patroni_connection(self):
        """
        Get the appropriate connection for Patroni checks.

        Returns the direct connection if available, otherwise the primary connection.
        Patroni checks should use this method to ensure they work even when
        connecting via proxies.

        Returns:
            tuple: (connection, cursor)
        """
        if self.has_direct_connection:
            return self.patroni_direct_conn, self.patroni_direct_cursor
        else:
            return self.conn, self.cursor

    def disconnect(self):
        """Closes all database connections."""
        # Close replica connections
        for host, conn in self.replica_conns.items():
            try:
                conn.close()
                logger.debug(f"Closed connection to replica: {host}")
            except Exception as e:
                logger.warning(f"Error closing replica connection {host}: {e}")

        # Close direct Patroni connection
        if self.patroni_direct_conn:
            try:
                self.patroni_direct_conn.close()
                logger.debug("Closed direct Patroni connection")
            except Exception as e:
                logger.warning(f"Error closing direct Patroni connection: {e}")

        # Close primary connection
        if self.conn:
            self.conn.close()
            print("🔌 Disconnected from PostgreSQL.")

        # Disconnect SSH
        self.disconnect_all_ssh()

    def _reconnect_primary(self):
        """
        Reconnect to the primary database after connection loss.

        This is called when the primary connection is detected as closed during query execution.
        """
        from datetime import datetime

        try:
            # Track reconnection attempt
            self.reconnection_count += 1

            # Close existing connection if it exists
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass

            # Reconnect using same logic as initial connection
            timeout = self.settings.get('statement_timeout', 30000)
            is_pgbouncer = bool(self.settings.get('pgbouncer_host'))

            if is_pgbouncer:
                # PgBouncer doesn't support startup parameters in options
                self.conn = psycopg2.connect(
                    host=self.settings['host'],
                    port=self.settings['port'],
                    dbname=self.settings['database'],
                    user=self.settings['user'],
                    password=self.settings['password'],
                    options=""  # Override PGOPTIONS env var
                )
            else:
                # Direct PostgreSQL connection - can use options
                self.conn = psycopg2.connect(
                    host=self.settings['host'],
                    port=self.settings['port'],
                    dbname=self.settings['database'],
                    user=self.settings['user'],
                    password=self.settings['password'],
                    options=f"-c statement_timeout={timeout}"
                )

            self.conn.autocommit = self.settings.get('autocommit', True)
            self.cursor = self.conn.cursor()

            # Set statement timeout after connection for PgBouncer
            if is_pgbouncer and timeout:
                try:
                    self.cursor.execute(f"SET statement_timeout = {timeout}")
                except Exception as e:
                    logger.debug(f"Could not set statement_timeout via SET command: {e}")

            logger.info(f"Primary connection successfully reconnected (attempt #{self.reconnection_count})")

        except psycopg2.Error as e:
            # Track failure
            self.connection_failures.append({
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'error': str(e),
                'attempt': self.reconnection_count
            })
            logger.error(f"Failed to reconnect to primary database: {e}")
            raise

    def _get_version_info(self):
        """Get PostgreSQL version information."""
        try:
            self.cursor.execute("SELECT current_setting('server_version_num');")
            version_num = int(self.cursor.fetchone()[0].strip())

            self.cursor.execute("SELECT current_setting('server_version');")
            version_string = self.cursor.fetchone()[0].strip()

            major_version = version_num // 10000

            return {
                'version_num': version_num,
                'version_string': version_string,
                'major_version': major_version,
                'is_pg10_or_newer': major_version >= 10,
                'is_pg11_or_newer': major_version >= 11,
                'is_pg12_or_newer': major_version >= 12,
                'is_pg13_or_newer': major_version >= 13,
                'is_pg14_or_newer': major_version >= 14,
                'is_pg15_or_newer': major_version >= 15,
                'is_pg16_or_newer': major_version >= 16,
                'is_pg17_or_newer': major_version >= 17,
                'is_pg18_or_newer': major_version >= 18
            }
        except Exception:
            return {
                'version_num': 0, 'version_string': 'unknown', 'major_version': 0,
                'is_pg10_or_newer': False, 'is_pg11_or_newer': False,
                'is_pg12_or_newer': False, 'is_pg13_or_newer': False,
                'is_pg14_or_newer': False, 'is_pg15_or_newer': False,
                'is_pg16_or_newer': False, 'is_pg17_or_newer': False,
                'is_pg18_or_newer': False
            }

    def _check_pg_stat_capabilities(self):
        """Checks for the existence and capabilities of the pg_stat_statements extension."""
        try:
            _, ext_exists = self.execute_query(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');",
                is_check=True, return_raw=True
            )
            self.has_pgstat = (str(ext_exists).lower() in ['t', 'true'])

            if self.has_pgstat:
                query_new = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND column_name = 'shared_blk_read_time');"
                _, col_exists_new = self.execute_query(query_new, is_check=True, return_raw=True)
                self.has_pgstat_new_io_time = (str(col_exists_new).lower() in ['t', 'true'])

                if not self.has_pgstat_new_io_time:
                    query_legacy = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND column_name = 'blk_read_time');"
                    _, col_exists_legacy = self.execute_query(query_legacy, is_check=True, return_raw=True)
                    self.has_pgstat_legacy_io_time = (str(col_exists_legacy).lower() in ['t', 'true'])

        except Exception as e:
            print(f"Warning: Could not check for pg_stat_statements capabilities: {e}")
            self.has_pgstat = False
            self.has_pgstat_legacy_io_time = False
            self.has_pgstat_new_io_time = False

    def get_db_metadata(self):
        """
        Fetches cluster-level metadata including environment information.

        Returns:
            dict: {'version': str, 'db_name': str, 'environment': str, 'environment_details': dict}
        """
        try:
            dbname_query = "SELECT current_database();"
            self.cursor.execute(dbname_query)
            db_name = self.cursor.fetchone()[0].strip()

            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': db_name,
                'environment': self.environment or 'unknown',
                'environment_details': self.environment_details or {}
            }
        except Exception as e:
            print(f"Warning: Could not fetch database metadata: {e}")
            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': 'N/A',
                'environment': self.environment or 'unknown',
                'environment_details': self.environment_details or {}
            }

    def execute_query(self, query, params=None, is_check=False, return_raw=False, allow_fallback=False):
        """Executes a query and returns formatted and raw results.

        Args:
            query: SQL query to execute
            params: Query parameters
            is_check: If True, return single value check result
            return_raw: If True, return tuple of (formatted, raw_results)
            allow_fallback: If True and query fails through primary connection (PgBouncer),
                          automatically retry through direct connection to PostgreSQL

        Returns:
            Formatted results, or tuple of (formatted, raw) if return_raw=True
        """
        try:
            # Check if connection is closed and reconnect if needed
            if not self.conn or self.conn.closed:
                logger.warning("Primary connection closed unexpectedly, attempting to reconnect...")
                self._reconnect_primary()

            # Check if cursor is closed and recreate if needed
            if not self.cursor or self.cursor.closed:
                self.cursor = self.conn.cursor()

            self.cursor.execute(query, params)

            if is_check:
                result = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else ""
                return (str(result), result) if return_raw else str(result)

            if self.cursor.description is None:
                return ("", []) if return_raw else ""

            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            raw_results = [dict(zip(columns, row)) for row in results]

            if not results:
                return "[NOTE]\n====\nNo results returned.\n====\n", [] if return_raw else ""

            table = ['|===', '|' + '|'.join(columns)]
            for row in results:
                sanitized_row = [str(v).replace('|', '\\|') if v is not None else '' for v in row]
                table.append('|' + '|'.join(sanitized_row))
            table.append('|===')
            formatted = '\n'.join(table)

            return (formatted, raw_results) if return_raw else formatted
        except psycopg2.Error as e:
            if self.conn:
                self.conn.rollback()

            # Try fallback to direct connection if enabled and available
            if allow_fallback and self.has_direct_connection:
                logger.warning(f"Query failed through primary connection (PgBouncer): {e}")
                logger.info("Retrying query through direct PostgreSQL connection...")
                try:
                    # Record the fallback event before attempting
                    self._record_fallback_event(query, str(e))
                    return self._execute_query_direct(query, params, is_check, return_raw)
                except Exception as fallback_error:
                    logger.error(f"Fallback query also failed: {fallback_error}")
                    error_str = f"[ERROR]\n====\nQuery failed through both PgBouncer and direct connection.\nPgBouncer error: {e}\nDirect error: {fallback_error}\n====\n"
                    return (error_str, {"error": str(e), "fallback_error": str(fallback_error), "query": query}) if return_raw else error_str

            error_str = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            return (error_str, {"error": str(e), "query": query}) if return_raw else error_str

    def _execute_query_direct(self, query, params=None, is_check=False, return_raw=False):
        """Execute query directly against PostgreSQL (bypassing PgBouncer).

        This is used as a fallback when queries through PgBouncer fail.
        """
        if not self.patroni_direct_conn or self.patroni_direct_conn.closed:
            raise Exception("Direct connection not available or closed")

        if not self.patroni_direct_cursor or self.patroni_direct_cursor.closed:
            self.patroni_direct_cursor = self.patroni_direct_conn.cursor()

        self.patroni_direct_cursor.execute(query, params)

        if is_check:
            result = self.patroni_direct_cursor.fetchone()[0] if self.patroni_direct_cursor.rowcount > 0 else ""
            logger.info(f"✓ Query succeeded via direct connection (bypassed PgBouncer)")
            return (str(result), result) if return_raw else str(result)

        if self.patroni_direct_cursor.description is None:
            return ("", []) if return_raw else ""

        columns = [desc[0] for desc in self.patroni_direct_cursor.description]
        results = self.patroni_direct_cursor.fetchall()
        raw_results = [dict(zip(columns, row)) for row in results]

        if not results:
            return "[NOTE]\n====\nNo results returned.\n====\n", [] if return_raw else ""

        table = ['|===', '|' + '|'.join(columns)]
        for row in results:
            sanitized_row = [str(v).replace('|', '\\|') if v is not None else '' for v in row]
            table.append('|' + '|'.join(sanitized_row))
        table.append('|===')
        formatted = '\n'.join(table)

        logger.info(f"✓ Query succeeded via direct connection (bypassed PgBouncer)")
        return (formatted, raw_results) if return_raw else formatted

    def has_select_privilege(self, view_name):
        """Checks if the current user has SELECT privilege on a given view/table."""
        try:
            query = f"SELECT has_table_privilege(current_user, '{view_name}', 'SELECT');"
            _, has_priv = self.execute_query(query, is_check=True, return_raw=True)
            return (str(has_priv).lower() in ['t', 'true'])
        except Exception as e:
            print(f"Warning: Could not check privilege for {view_name}: {e}")
            return False

    def _record_fallback_event(self, query: str, primary_error: str):
        """
        Record when a query failed through primary connection (PgBouncer)
        but succeeded via direct connection.

        Args:
            query: The SQL query that failed through PgBouncer
            primary_error: The error message from PgBouncer
        """
        # Truncate query for readability (first 100 chars)
        query_preview = query[:100] + '...' if len(query) > 100 else query

        self.fallback_stats['count'] += 1
        self.fallback_stats['queries'].append({
            'query': query_preview,
            'full_query': query,
            'primary_error': primary_error,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        })

        logger.info(f"Fallback event recorded (total: {self.fallback_stats['count']})")

    def get_fallback_stats(self) -> dict:
        """
        Get current fallback statistics.

        Returns:
            Dictionary with fallback count and query details
        """
        return {
            'count': self.fallback_stats['count'],
            'queries': self.fallback_stats['queries'],
            'last_reset': self.fallback_stats['last_reset']
        }

    def reset_fallback_stats(self):
        """
        Reset fallback statistics.
        Typically called at the start of a new health check run.
        """
        self.fallback_stats = {
            'count': 0,
            'queries': [],
            'last_reset': datetime.utcnow().isoformat() + 'Z'
        }
        logger.debug("Fallback stats reset")

    def get_installed_extensions(self):
        """
        Get list of installed PostgreSQL extensions for CVE checking.

        Only checks extensions that have good CPE coverage in NVD to reduce false positives.
        Extensions checked: PostGIS, TimescaleDB, Citus, pgaudit, pg_partman, pg_stat_statements.

        Returns:
            List[Dict[str, str]]: [{'name': 'postgis', 'version': '3.4.0'}, ...]
        """
        # List of extensions with good CPE coverage in NVD
        # These are major extensions that are worth checking for CVEs
        checked_extensions = {
            'postgis',         # PostGIS - widely used, has CVE history
            'timescaledb',     # TimescaleDB - commercial support, tracked
            'citus',           # Citus - Microsoft-backed, tracked
            'pgaudit',         # pgAudit - security-focused, important
            'pg_partman',      # Partition management - has CVE entries
            'pg_stat_statements'  # Built-in but important for monitoring
        }

        try:
            query = """
                SELECT
                    extname AS name,
                    extversion AS version
                FROM pg_extension
                WHERE extname NOT IN ('plpgsql')  -- Exclude built-in procedural language
                ORDER BY extname;
            """
            results = self.execute_query(query)

            # Handle empty results
            if not results:
                logger.debug("No extensions found (only built-ins installed)")
                return []

            # Filter to only checked extensions
            all_extensions = []
            for row in results:
                try:
                    # Handle both tuple and list results
                    if len(row) >= 2:
                        ext_name = str(row[0]) if row[0] else ''
                        ext_version = str(row[1]) if row[1] else ''
                        if ext_name:  # Only add if we have a name
                            all_extensions.append({'name': ext_name, 'version': ext_version})
                except (IndexError, TypeError) as e:
                    logger.debug(f"Skipping malformed extension row: {row} ({e})")
                    continue

            filtered_extensions = [
                ext for ext in all_extensions
                if ext['name'].lower() in checked_extensions
            ]

            if filtered_extensions:
                logger.info(f"Found {len(filtered_extensions)} extensions for CVE checking (out of {len(all_extensions)} installed)")
                for ext in filtered_extensions:
                    logger.debug(f"  Will check CVEs for: {ext['name']} {ext['version']}")
            else:
                if all_extensions:
                    logger.debug(f"No CVE-tracked extensions found ({len(all_extensions)} extensions installed, none in check list)")
                else:
                    logger.debug("No extensions found (only built-ins installed)")

            return filtered_extensions

        except Exception as e:
            logger.warning(f"Could not retrieve installed extensions: {e}")
            import traceback
            logger.debug(f"Extension query traceback: {traceback.format_exc()}")
            return []

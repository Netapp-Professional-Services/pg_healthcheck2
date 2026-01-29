"""
Instacollector Loader for Kafka diagnostic exports.

Loads and parses data collected by Instaclustr's Instacollector tool.
The Instacollector creates per-node directories containing Kafka CLI outputs,
configuration files, logs, and system information.

Directory Structure:
    instacollector_export/
    ├── 192.168.1.10/
    │   ├── InstaCollection_192.168.1.10/
    │   │   ├── kafka-versions-output.txt.info
    │   │   ├── kafka-topics-describe-output.txt.info
    │   │   ├── consumer-groups-output.txt.info
    │   │   ├── kafka-metadata-quorum-status-output.txt.info
    │   │   ├── kafka-metadata-quorum-replication-output.txt.info
    │   │   ├── kafka-api-versions-output.txt.info
    │   │   ├── broker.properties
    │   │   ├── server.properties
    │   │   ├── controller.properties
    │   │   ├── log4j.properties
    │   │   ├── kafkaServer-gc.log*
    │   │   ├── server.log*
    │   │   ├── controller.log*
    │   │   ├── state-change.log*
    │   │   ├── mem.info
    │   │   ├── cpu.info
    │   │   └── io_stat.info
    └── 192.168.1.11/
        └── ...

Usage:
    loader = KafkaInstacollectorLoader('/path/to/export')
    if loader.load():
        nodes = loader.get_nodes()
        topics = loader.get_kafka_output('topics')
        config = loader.get_config('broker.properties')
"""

import logging
import os
import re
import shutil
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class KafkaInstacollectorLoader:
    """
    Loads and parses Kafka Instacollector diagnostic exports.

    Supports:
    - Extracted directories
    - .tar.gz/.tgz tarballs
    - Multi-node collections
    - Nested tarballs (common in Instacollector exports)
    """

    # Known Kafka CLI output files and their command names
    KAFKA_CLI_FILES = {
        'kafka-versions-output.txt.info': 'version',
        'kafka-topics-describe-output.txt.info': 'topics',
        'consumer-groups-output.txt.info': 'consumer_groups',
        'kafka-metadata-quorum-status-output.txt.info': 'quorum_status',
        'kafka-metadata-quorum-replication-output.txt.info': 'quorum_replication',
        'kafka-api-versions-output.txt.info': 'api_versions',
    }

    # Config files
    CONFIG_FILES = [
        'broker.properties',
        'server.properties',
        'controller.properties',
        'log4j.properties',
        'zookeeper.properties',
    ]

    # System info files
    SYSTEM_FILES = {
        'mem.info': 'memory',
        'cpu.info': 'cpu',
        'io_stat.info': 'iostat',
        'file_descriptor.info': 'file_descriptors',
        'hosts_file.info': 'hosts',
    }

    # Log file patterns
    LOG_PATTERNS = [
        'kafkaServer-gc.log*',
        'server.log*',
        'controller.log*',
        'state-change.log*',
        'kafka-authorizer.log*',
    ]

    def __init__(self, source_path: str):
        """
        Initialize the loader.

        Args:
            source_path: Path to Instacollector export (directory or tarball)
        """
        self.source_path = Path(source_path)
        self.data_dir: Optional[Path] = None
        self._temp_dir: Optional[str] = None
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._metadata: Dict[str, Any] = {}
        self._loaded = False

        logger.info(f"KafkaInstacollectorLoader initialized for: {source_path}")

    def load(self) -> bool:
        """
        Load the diagnostic data.

        Returns:
            bool: True if data loaded successfully
        """
        try:
            # Determine source type and prepare data directory
            if self.source_path.is_file():
                if self.source_path.suffix in ('.gz', '.tgz') or self.source_path.name.endswith('.tar.gz'):
                    self._extract_tarball()
                else:
                    logger.error(f"Unsupported file format: {self.source_path}")
                    return False
            elif self.source_path.is_dir():
                self.data_dir = self._find_data_root(self.source_path)
            else:
                logger.error(f"Path does not exist: {self.source_path}")
                return False

            if not self.data_dir:
                logger.error("Could not find valid Kafka Instacollector data")
                return False

            # Discover nodes
            self._discover_nodes()

            if not self._nodes:
                logger.error("No valid node directories found")
                return False

            # Load data from all nodes
            self._load_node_data()

            # Build metadata
            self._build_metadata()

            self._loaded = True
            logger.info(f"Successfully loaded data for {len(self._nodes)} nodes")
            return True

        except Exception as e:
            logger.error(f"Failed to load Kafka Instacollector data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _extract_tarball(self):
        """Extract tarball to temporary directory, including nested tarballs."""
        self._temp_dir = tempfile.mkdtemp(prefix='kafka_instacollector_')

        logger.info(f"Extracting {self.source_path} to {self._temp_dir}")

        with tarfile.open(self.source_path, 'r:gz') as tar:
            tar.extractall(self._temp_dir)

        # Handle nested tarballs (Instacollector packages per-node tarballs)
        self._extract_nested_tarballs(Path(self._temp_dir))

        # Find the actual data directory within extracted content
        self.data_dir = self._find_data_root(Path(self._temp_dir))

    def _extract_nested_tarballs(self, path: Path):
        """
        Extract nested tarballs within IP-named directories.

        Instacollector packages data as:
        - OuterArchive.tar.gz
          - InstaCollection_timestamp/
            - 192.168.1.10/
              - InstaCollection_192.168.1.10.tar.gz  <-- nested tarball
        """
        ip_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')

        # Search recursively for IP-named directories with tarballs
        for item in path.rglob('*'):
            if item.is_dir() and ip_pattern.match(item.name):
                self._extract_node_tarball(item)

    def _extract_node_tarball(self, node_dir: Path):
        """
        Extract a nested tarball within a node directory.

        Args:
            node_dir: Path to IP-named directory that may contain a tarball
        """
        for item in node_dir.iterdir():
            if item.is_file() and (item.suffix == '.gz' or item.name.endswith('.tar.gz')):
                if tarfile.is_tarfile(item):
                    logger.info(f"Extracting nested tarball: {item.name}")
                    try:
                        with tarfile.open(item, 'r:gz') as tar:
                            tar.extractall(node_dir)
                    except Exception as e:
                        logger.warning(f"Failed to extract {item}: {e}")

    def _find_data_root(self, path: Path) -> Optional[Path]:
        """
        Find the root directory containing node directories.

        Args:
            path: Starting path to search

        Returns:
            Path to directory containing node subdirectories
        """
        # Check if path itself contains node directories (IP-like names)
        node_dirs = self._find_node_directories(path)
        if node_dirs:
            return path

        # Check immediate subdirectories
        for subdir in path.iterdir():
            if subdir.is_dir():
                node_dirs = self._find_node_directories(subdir)
                if node_dirs:
                    return subdir

                # Check one level deeper (for InstaCollection_timestamp structure)
                for subsubdir in subdir.iterdir():
                    if subsubdir.is_dir():
                        node_dirs = self._find_node_directories(subsubdir)
                        if node_dirs:
                            return subsubdir

        return None

    def _find_node_directories(self, path: Path) -> List[Path]:
        """
        Find directories that look like node directories (IP addresses).

        Args:
            path: Directory to search

        Returns:
            List of node directory paths
        """
        node_dirs = []
        ip_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')

        for item in path.iterdir():
            if item.is_dir():
                if ip_pattern.match(item.name):
                    if self._is_node_directory(item):
                        node_dirs.append(item)

        return node_dirs

    def _is_node_directory(self, path: Path) -> bool:
        """
        Check if directory contains expected Kafka Instacollector files.

        Args:
            path: Directory to check

        Returns:
            True if this looks like a valid node directory
        """
        # Check for nested InstaCollection_* directory
        for subdir in path.iterdir():
            if subdir.is_dir() and subdir.name.startswith('InstaCollection_'):
                # Check if it contains Kafka files
                for filename in self.KAFKA_CLI_FILES.keys():
                    if (subdir / filename).exists():
                        return True
                for filename in self.CONFIG_FILES:
                    if (subdir / filename).exists():
                        return True

        # Also check directly in the path
        for filename in self.KAFKA_CLI_FILES.keys():
            if (path / filename).exists():
                return True
        for filename in self.CONFIG_FILES:
            if (path / filename).exists():
                return True

        return False

    def _discover_nodes(self):
        """Discover all node directories in the data root."""
        if not self.data_dir:
            return

        node_dirs = self._find_node_directories(self.data_dir)

        for node_dir in node_dirs:
            node_ip = node_dir.name

            # Find the actual data directory (may be nested)
            data_path = node_dir
            for subdir in node_dir.iterdir():
                if subdir.is_dir() and subdir.name.startswith('InstaCollection_'):
                    data_path = subdir
                    break

            self._nodes[node_ip] = {
                'path': data_path,
                'files': {},
                'kafka_cli': {},
                'config': {},
                'system': {},
                'logs': {},
            }
            logger.debug(f"Discovered node: {node_ip}")

    def _load_node_data(self):
        """Load and parse data from all discovered nodes."""
        for node_ip, node_data in self._nodes.items():
            node_path = node_data['path']

            # Load Kafka CLI outputs
            for filename, command in self.KAFKA_CLI_FILES.items():
                file_path = node_path / filename
                if file_path.exists():
                    try:
                        content = file_path.read_text()
                        node_data['files'][filename] = content

                        # Parse the output
                        parsed = self._parse_kafka_output(command, content)
                        node_data['kafka_cli'][command] = parsed
                        logger.debug(f"Loaded {filename} for node {node_ip}")
                    except Exception as e:
                        logger.warning(f"Failed to parse {filename} for {node_ip}: {e}")

            # Load config files
            for filename in self.CONFIG_FILES:
                file_path = node_path / filename
                if file_path.exists():
                    try:
                        content = file_path.read_text()
                        node_data['files'][filename] = content

                        # Parse properties files
                        if filename.endswith('.properties'):
                            node_data['config'][filename] = self._parse_properties(content)
                        else:
                            node_data['config'][filename] = content
                    except Exception as e:
                        logger.warning(f"Failed to load {filename} for {node_ip}: {e}")

            # Load system info files
            for filename, info_type in self.SYSTEM_FILES.items():
                file_path = node_path / filename
                if file_path.exists():
                    try:
                        content = file_path.read_text()
                        node_data['files'][filename] = content
                        node_data['system'][info_type] = self._parse_system_info(info_type, content)
                    except Exception as e:
                        logger.warning(f"Failed to load {filename} for {node_ip}: {e}")

            # Load log files (store paths for lazy loading)
            for pattern in self.LOG_PATTERNS:
                for log_file in node_path.glob(pattern):
                    log_name = log_file.name
                    node_data['logs'][log_name] = log_file

    def _parse_kafka_output(self, command: str, content: str) -> Any:
        """
        Parse Kafka CLI command output.

        Args:
            command: Command name (version, topics, consumer_groups, etc.)
            content: Raw command output

        Returns:
            Parsed data structure
        """
        if command == 'version':
            return {'version': content.strip()}

        elif command == 'topics':
            return self._parse_topics_describe(content)

        elif command == 'consumer_groups':
            return self._parse_consumer_groups(content)

        elif command == 'quorum_status':
            return self._parse_quorum_status(content)

        elif command == 'quorum_replication':
            return self._parse_quorum_replication(content)

        elif command == 'api_versions':
            return {'raw': content}

        return {'raw': content}

    def _parse_topics_describe(self, content: str) -> Dict[str, Any]:
        """Parse kafka-topics.sh --describe output."""
        topics = []
        current_topic = None

        for line in content.strip().split('\n'):
            line = line.strip()
            if not line:
                continue

            if line.startswith('Topic:') and 'Partition:' not in line:
                # Topic header line
                parts = line.split('\t')
                topic_info = {}
                for part in parts:
                    if ':' in part:
                        key, value = part.split(':', 1)
                        key = key.strip().lower().replace(' ', '_')
                        topic_info[key] = value.strip()

                current_topic = {
                    'name': topic_info.get('topic', ''),
                    'topic_id': topic_info.get('topicid', ''),
                    'partition_count': int(topic_info.get('partitioncount', 0)),
                    'replication_factor': int(topic_info.get('replicationfactor', 0)),
                    'configs': topic_info.get('configs', ''),
                    'partitions': []
                }
                topics.append(current_topic)

            elif line.startswith('\tTopic:') or (line.startswith('Topic:') and 'Partition:' in line):
                # Partition detail line
                if current_topic:
                    parts = line.strip().split('\t')
                    partition_info = {}
                    for part in parts:
                        if ':' in part:
                            key, value = part.split(':', 1)
                            key = key.strip().lower().replace(' ', '_')
                            partition_info[key] = value.strip()

                    partition = {
                        'partition': int(partition_info.get('partition', 0)),
                        'leader': int(partition_info.get('leader', -1)),
                        'replicas': partition_info.get('replicas', ''),
                        'isr': partition_info.get('isr', ''),
                    }
                    current_topic['partitions'].append(partition)

        return {'topics': topics, 'topic_count': len(topics)}

    def _parse_consumer_groups(self, content: str) -> Dict[str, Any]:
        """Parse kafka-consumer-groups.sh output."""
        groups = {}
        current_group = None

        for line in content.strip().split('\n'):
            line = line.strip()
            if not line or line.startswith('GROUP'):
                # Header line or empty
                if line.startswith('GROUP') and current_group:
                    # New group section
                    current_group = None
                continue

            parts = line.split()
            if len(parts) >= 6:
                group_name = parts[0]
                topic = parts[1]
                try:
                    partition = int(parts[2])
                    current_offset = int(parts[3]) if parts[3] != '-' else None
                    log_end_offset = int(parts[4]) if parts[4] != '-' else None
                    lag = int(parts[5]) if parts[5] != '-' else None
                except (ValueError, IndexError):
                    continue

                consumer_id = parts[6] if len(parts) > 6 and parts[6] != '-' else None
                host = parts[7] if len(parts) > 7 and parts[7] != '-' else None
                client_id = parts[8] if len(parts) > 8 and parts[8] != '-' else None

                if group_name not in groups:
                    groups[group_name] = {
                        'name': group_name,
                        'partitions': [],
                        'total_lag': 0
                    }

                partition_info = {
                    'topic': topic,
                    'partition': partition,
                    'current_offset': current_offset,
                    'log_end_offset': log_end_offset,
                    'lag': lag,
                    'consumer_id': consumer_id,
                    'host': host,
                    'client_id': client_id
                }
                groups[group_name]['partitions'].append(partition_info)
                if lag is not None:
                    groups[group_name]['total_lag'] += lag

        return {'groups': list(groups.values()), 'group_count': len(groups)}

    def _parse_quorum_status(self, content: str) -> Dict[str, Any]:
        """Parse kafka-metadata.sh quorum status output."""
        result = {}

        for line in content.strip().split('\n'):
            if ':' in line:
                # Handle key: value format
                key, value = line.split(':', 1)
                key = key.strip().lower().replace(' ', '_')
                value = value.strip()

                # Parse JSON-like arrays
                if value.startswith('[') and value.endswith(']'):
                    try:
                        import json
                        value = json.loads(value)
                    except:
                        pass

                # Parse integers
                if isinstance(value, str) and value.isdigit():
                    value = int(value)

                result[key] = value

        return result

    def _parse_quorum_replication(self, content: str) -> Dict[str, Any]:
        """Parse kafka-metadata.sh quorum replication output."""
        return {'raw': content}

    def _parse_properties(self, content: str) -> Dict[str, str]:
        """Parse Java properties file content."""
        props = {}
        for line in content.strip().split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                props[key.strip()] = value.strip()
        return props

    def _parse_system_info(self, info_type: str, content: str) -> Dict[str, Any]:
        """Parse system information files."""
        if info_type == 'memory':
            return self._parse_meminfo(content)
        elif info_type == 'cpu':
            return self._parse_cpuinfo(content)
        elif info_type == 'iostat':
            return {'raw': content}
        elif info_type == 'file_descriptors':
            return {'raw': content}
        return {'raw': content}

    def _parse_meminfo(self, content: str) -> Dict[str, Any]:
        """Parse /proc/meminfo content."""
        mem = {}
        for line in content.strip().split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip().lower().replace(' ', '_')
                value = value.strip()
                # Extract numeric value
                parts = value.split()
                if parts and parts[0].isdigit():
                    mem[key] = int(parts[0])
                    if len(parts) > 1:
                        mem[f'{key}_unit'] = parts[1]
        return mem

    def _parse_cpuinfo(self, content: str) -> Dict[str, Any]:
        """Parse /proc/cpuinfo content."""
        cpus = []
        current_cpu = {}

        for line in content.strip().split('\n'):
            if not line.strip():
                if current_cpu:
                    cpus.append(current_cpu)
                    current_cpu = {}
                continue
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip().lower().replace(' ', '_')
                current_cpu[key] = value.strip()

        if current_cpu:
            cpus.append(current_cpu)

        return {'cpus': cpus, 'cpu_count': len(cpus)}

    def _build_metadata(self):
        """Build metadata from loaded data."""
        version = 'Unknown'
        cluster_id = 'Unknown'
        kafka_mode = 'unknown'

        for node_ip, node_data in self._nodes.items():
            # Get version
            version_data = node_data['kafka_cli'].get('version', {})
            if version_data:
                version = version_data.get('version', version)

            # Get cluster ID and mode from quorum status (KRaft)
            quorum_status = node_data['kafka_cli'].get('quorum_status', {})
            if quorum_status:
                cluster_id = quorum_status.get('clusterid', cluster_id)
                kafka_mode = 'kraft'

            # Check config for mode detection
            broker_config = node_data['config'].get('broker.properties', {})
            if broker_config:
                if 'process.roles' in broker_config:
                    kafka_mode = 'kraft'
                elif 'zookeeper.connect' in broker_config:
                    kafka_mode = 'zookeeper'

            server_config = node_data['config'].get('server.properties', {})
            if server_config:
                if 'process.roles' in server_config:
                    kafka_mode = 'kraft'
                elif 'zookeeper.connect' in server_config:
                    kafka_mode = 'zookeeper'

            break  # Use first node for metadata

        # Get collection date from file timestamps
        collection_date = None
        for node_ip, node_data in self._nodes.items():
            node_path = node_data['path']
            for filename in self.KAFKA_CLI_FILES.keys():
                file_path = node_path / filename
                if file_path.exists():
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if collection_date is None or mtime > collection_date:
                        collection_date = mtime
                    break

        self._metadata = {
            'version': version,
            'cluster_id': cluster_id,
            'kafka_mode': kafka_mode,
            'node_count': len(self._nodes),
            'nodes': list(self._nodes.keys()),
            'collection_date': collection_date.isoformat() if collection_date else 'Unknown',
            'source_path': str(self.source_path),
        }

    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the loaded collection."""
        return self._metadata

    def get_nodes(self) -> List[str]:
        """Get list of node IPs in the collection."""
        return list(self._nodes.keys())

    def get_kafka_output(
        self,
        command: str,
        node: Optional[str] = None,
    ) -> Optional[Any]:
        """
        Get Kafka CLI command output.

        Args:
            command: Command name (version, topics, consumer_groups, etc.)
            node: Specific node IP, or None for first available

        Returns:
            Parsed data structure or None if not available
        """
        if node:
            if node in self._nodes:
                return self._nodes[node]['kafka_cli'].get(command)
            return None

        # Return from first available node
        for node_ip, node_data in self._nodes.items():
            if command in node_data['kafka_cli']:
                return node_data['kafka_cli'][command]

        return None

    def get_config(self, filename: str, node: Optional[str] = None) -> Optional[Any]:
        """
        Get configuration file content.

        Args:
            filename: Config filename (e.g., 'broker.properties')
            node: Specific node IP, or None for first available

        Returns:
            Parsed config dict or raw content
        """
        if node:
            if node in self._nodes:
                return self._nodes[node]['config'].get(filename)
            return None

        # Return from first available node
        for node_ip, node_data in self._nodes.items():
            if filename in node_data['config']:
                return node_data['config'][filename]

        return None

    def get_system_info(self, info_type: str, node: Optional[str] = None) -> Optional[Any]:
        """
        Get system information.

        Args:
            info_type: Type of info ('memory', 'cpu', 'iostat')
            node: Specific node IP, or None for aggregate

        Returns:
            System information
        """
        if node:
            if node in self._nodes:
                return self._nodes[node]['system'].get(info_type)
            return None

        # Aggregate from all nodes
        aggregated = {}
        for node_ip, node_data in self._nodes.items():
            if info_type in node_data['system']:
                aggregated[node_ip] = node_data['system'][info_type]

        return aggregated if aggregated else None

    def get_raw_file(self, filename: str, node: Optional[str] = None) -> Optional[str]:
        """
        Get raw file content.

        Args:
            filename: Filename to retrieve
            node: Specific node IP, or None for first available

        Returns:
            Raw file content as string
        """
        if node:
            if node in self._nodes:
                node_data = self._nodes[node]
                if filename in node_data['files']:
                    return node_data['files'][filename]
                # Check logs
                if filename in node_data['logs']:
                    log_path = node_data['logs'][filename]
                    if isinstance(log_path, Path) and log_path.exists():
                        try:
                            return log_path.read_text()
                        except Exception as e:
                            logger.warning(f"Failed to read log file {log_path}: {e}")
            return None

        # Check all nodes
        for node_ip, node_data in self._nodes.items():
            if filename in node_data['files']:
                return node_data['files'][filename]
            if filename in node_data['logs']:
                log_path = node_data['logs'][filename]
                if isinstance(log_path, Path) and log_path.exists():
                    try:
                        return log_path.read_text()
                    except Exception as e:
                        continue

        return None

    def get_log_files(self, pattern: str = 'kafkaServer-gc.log*', node: Optional[str] = None) -> Dict[str, List[Path]]:
        """
        Get log file paths matching pattern.

        Args:
            pattern: Glob pattern for log files
            node: Specific node IP, or None for all nodes

        Returns:
            Dict mapping node IPs to list of matching log paths
        """
        result = {}

        nodes_to_check = [node] if node else list(self._nodes.keys())

        for node_ip in nodes_to_check:
            if node_ip in self._nodes:
                node_path = self._nodes[node_ip]['path']
                matching = list(node_path.glob(pattern))
                if matching:
                    result[node_ip] = matching

        return result

    def list_files(self) -> List[str]:
        """List all loaded files across all nodes."""
        all_files = set()
        for node_ip, node_data in self._nodes.items():
            all_files.update(node_data['files'].keys())
        return sorted(list(all_files))

    def cleanup(self):
        """Clean up temporary files."""
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
                logger.debug(f"Cleaned up temp directory: {self._temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory: {e}")
            self._temp_dir = None

    def __enter__(self):
        """Context manager entry."""
        self.load()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
        return False


def find_most_recent_collection(directory: Path) -> Optional[Path]:
    """
    Find the most recent Kafka Instacollector export in a directory.

    Args:
        directory: Directory to search

    Returns:
        Path to most recent collection, or None
    """
    candidates = []

    # Look for tarballs
    for pattern in ['InstaCollection_*.tar.gz', 'instacollector_*.tar.gz', '*kafka*.tar.gz', '*.tar.gz']:
        for tarball in directory.glob(pattern):
            mtime = tarball.stat().st_mtime
            candidates.append((mtime, tarball))

    # Look for directories with IP subdirs
    ip_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
    for item in directory.iterdir():
        if item.is_dir():
            has_ip_subdirs = any(
                ip_pattern.match(subdir.name)
                for subdir in item.iterdir()
                if subdir.is_dir()
            )
            if has_ip_subdirs:
                mtime = item.stat().st_mtime
                candidates.append((mtime, item))

    if not candidates:
        return None

    # Return most recent
    candidates.sort(reverse=True, key=lambda x: x[0])
    return candidates[0][1]

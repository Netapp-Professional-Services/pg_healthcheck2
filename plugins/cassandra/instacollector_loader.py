"""
Instacollector Loader for Cassandra diagnostic exports.

Loads and parses data collected by Instaclustr's Instacollector tool.
The Instacollector creates per-node directories containing nodetool outputs,
configuration files, logs, and system information.

Directory Structure:
    instacollector_export/
    ├── 192.168.1.10/
    │   ├── nodetool_status.info
    │   ├── nodetool_info.info
    │   ├── nodetool_cfstats.info
    │   ├── nodetool_compactionstats.info
    │   ├── nodetool_describecluster.info
    │   ├── nodetool_gossipinfo.info
    │   ├── nodetool_ring.info
    │   ├── nodetool_tpstats.info
    │   ├── nodetool_version.info
    │   ├── disk.info
    │   ├── io_stat.info
    │   ├── cassandra.yaml
    │   ├── jvm.options
    │   ├── cassandra-env.sh
    │   ├── gc.log.*
    │   └── system.log
    └── 192.168.1.11/
        └── ...

Usage:
    loader = InstacollectorLoader('/path/to/export')
    if loader.load():
        nodes = loader.get_nodes()
        status = loader.get_nodetool_output('status')  # Cluster-wide
        info = loader.get_nodetool_output('info', node='192.168.1.10')  # Per-node
"""

import logging
import os
import re
import shutil
import tarfile
import tempfile
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

from plugins.common.parsers import NodetoolParser, ShellCommandParser

logger = logging.getLogger(__name__)


class InstacollectorLoader:
    """
    Loads and parses Instacollector diagnostic exports.

    Supports:
    - Extracted directories
    - .tar.gz/.tgz tarballs
    - Multi-node collections
    """

    # Known nodetool output files and their command names
    NODETOOL_FILES = {
        'nodetool_status.info': 'status',
        'nodetool_info.info': 'info',
        'nodetool_cfstats.info': 'cfstats',
        'nodetool_tablestats.info': 'tablestats',
        'nodetool_compactionstats.info': 'compactionstats',
        'nodetool_describecluster.info': 'describecluster',
        'nodetool_gossipinfo.info': 'gossipinfo',
        'nodetool_ring.info': 'ring',
        'nodetool_tpstats.info': 'tpstats',
        'nodetool_version.info': 'version',
        'nodetool_gcstats.info': 'gcstats',
        'nodetool_tablehistograms.info': 'tablehistograms',
    }

    # Config files
    CONFIG_FILES = [
        'cassandra.yaml',
        'jvm.options',
        'cassandra-env.sh',
        'logback.xml',
    ]

    # System info files
    SYSTEM_FILES = [
        'disk.info',
        'io_stat.info',
    ]

    # Log files (may have rotation suffixes)
    LOG_PATTERNS = [
        'gc.log*',
        'system.log',
        'debug.log',
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

        # Initialize parsers
        self.nodetool_parser = NodetoolParser()
        self.shell_parser = ShellCommandParser()

        logger.info(f"InstacollectorLoader initialized for: {source_path}")

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
                # Check if this is a node directory or parent directory
                self.data_dir = self._find_data_root(self.source_path)
            else:
                logger.error(f"Path does not exist: {self.source_path}")
                return False

            if not self.data_dir:
                logger.error("Could not find valid Instacollector data")
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
            logger.error(f"Failed to load Instacollector data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _extract_tarball(self):
        """Extract tarball to temporary directory, including nested tarballs."""
        self._temp_dir = tempfile.mkdtemp(prefix='instacollector_')

        logger.info(f"Extracting {self.source_path} to {self._temp_dir}")

        with tarfile.open(self.source_path, 'r:gz') as tar:
            tar.extractall(self._temp_dir)

        # Handle nested tarballs (Instacollector often packages per-node tarballs)
        self._extract_nested_tarballs(Path(self._temp_dir))

        # Find the actual data directory within extracted content
        self.data_dir = self._find_data_root(Path(self._temp_dir))

    def _extract_nested_tarballs(self, path: Path):
        """
        Extract nested tarballs within IP-named directories.

        Instacollector sometimes packages data as:
        - OuterArchive.tar.gz
          - 192.168.1.10/
            - InstaCollection_192.168.1.10.tar.gz  <-- nested tarball
          - 192.168.1.11/
            - InstaCollection_192.168.1.11.tar.gz

        This method finds and extracts those nested tarballs in place.
        """
        ip_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')

        # Search for IP-named directories
        for item in path.iterdir():
            if item.is_dir():
                if ip_pattern.match(item.name):
                    # Look for nested tarball in this IP directory
                    self._extract_node_tarball(item)
                else:
                    # Check subdirectories (handle extra nesting level)
                    for subitem in item.iterdir():
                        if subitem.is_dir() and ip_pattern.match(subitem.name):
                            self._extract_node_tarball(subitem)

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
                        # Optionally remove the tarball after extraction to save space
                        # item.unlink()
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

        # Check if this IS a single node directory
        if self._is_node_directory(path):
            # Create a virtual parent
            return path.parent

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
                # Check if directory name is an IP address
                if ip_pattern.match(item.name):
                    if self._is_node_directory(item):
                        node_dirs.append(item)

        return node_dirs

    def _is_node_directory(self, path: Path) -> bool:
        """
        Check if directory contains expected Instacollector files.

        Args:
            path: Directory to check

        Returns:
            True if this looks like a valid node directory
        """
        # Must have at least one nodetool output file
        for filename in self.NODETOOL_FILES.keys():
            if (path / filename).exists():
                return True

        # Or have cassandra.yaml
        if (path / 'cassandra.yaml').exists():
            return True

        return False

    def _discover_nodes(self):
        """Discover all node directories in the data root."""
        if not self.data_dir:
            return

        node_dirs = self._find_node_directories(self.data_dir)

        for node_dir in node_dirs:
            node_ip = node_dir.name
            self._nodes[node_ip] = {
                'path': node_dir,
                'files': {},
                'nodetool': {},
                'config': {},
                'system': {},
                'logs': {},
            }
            logger.debug(f"Discovered node: {node_ip}")

    def _load_node_data(self):
        """Load and parse data from all discovered nodes."""
        for node_ip, node_data in self._nodes.items():
            node_path = node_data['path']

            # Load nodetool outputs
            for filename, command in self.NODETOOL_FILES.items():
                file_path = node_path / filename
                if file_path.exists():
                    try:
                        content = file_path.read_text()
                        node_data['files'][filename] = content

                        # Parse the output
                        parsed = self.nodetool_parser.parse(command, content)
                        node_data['nodetool'][command] = parsed
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

                        # Parse YAML configs
                        if filename.endswith('.yaml'):
                            node_data['config'][filename] = yaml.safe_load(content)
                        else:
                            node_data['config'][filename] = content
                    except Exception as e:
                        logger.warning(f"Failed to load {filename} for {node_ip}: {e}")

            # Load system info files
            for filename in self.SYSTEM_FILES:
                file_path = node_path / filename
                if file_path.exists():
                    try:
                        content = file_path.read_text()
                        node_data['files'][filename] = content

                        # Parse disk.info (df -h output)
                        if filename == 'disk.info':
                            node_data['system']['disk'] = self._parse_disk_info(content)
                        else:
                            node_data['system'][filename] = content
                    except Exception as e:
                        logger.warning(f"Failed to load {filename} for {node_ip}: {e}")

            # Load log files (just store paths for now)
            for pattern in self.LOG_PATTERNS:
                for log_file in node_path.glob(pattern):
                    log_name = log_file.name
                    node_data['logs'][log_name] = log_file

    def _parse_disk_info(self, content: str) -> Dict[str, Any]:
        """
        Parse disk.info file which contains df -h output.

        Args:
            content: Raw disk.info file content

        Returns:
            Parsed disk information
        """
        result = {
            'df_output': [],
            'du_output': [],
            'raw': content,
        }

        lines = content.strip().split('\n')
        in_df_section = False
        in_du_section = False

        for line in lines:
            # Detect section headers
            if line.startswith('df -h'):
                in_df_section = True
                in_du_section = False
                continue
            elif line.startswith('du -h'):
                in_df_section = False
                in_du_section = True
                continue

            # Parse df output (skip header line)
            if in_df_section and line.strip() and not line.startswith('Filesystem'):
                parts = line.split()
                if len(parts) >= 6:
                    result['df_output'].append({
                        'filesystem': parts[0],
                        'size': parts[1],
                        'used': parts[2],
                        'avail': parts[3],
                        'use_pct': parts[4].rstrip('%'),
                        'mounted_on': parts[5],
                    })

            # Parse du output
            elif in_du_section and line.strip():
                parts = line.split('\t')
                if len(parts) >= 2:
                    result['du_output'].append({
                        'size': parts[0],
                        'path': parts[1],
                    })

        return result

    def _build_metadata(self):
        """Build metadata from loaded data."""
        # Get version from any node
        version = 'Unknown'
        cluster_name = 'Unknown'
        datacenter = 'Unknown'

        for node_ip, node_data in self._nodes.items():
            # Get version from parsed nodetool version output
            version_data = node_data['nodetool'].get('version', {})
            if version_data and isinstance(version_data, dict):
                version = version_data.get('version') or version_data.get('releaseversion', version)

            # Fallback: try raw file content
            if version == 'Unknown' and 'nodetool_version.info' in node_data['files']:
                version_content = node_data['files'].get('nodetool_version.info', '')
                if version_content:
                    # Parse "ReleaseVersion: X.Y.Z" format
                    for line in version_content.strip().split('\n'):
                        if ':' in line:
                            key, val = line.split(':', 1)
                            if 'version' in key.lower():
                                version = val.strip()
                                break

            # Get cluster info from nodetool describecluster
            describe = node_data['nodetool'].get('describecluster', {})
            if describe:
                cluster_name = describe.get('name', cluster_name)

            # Get datacenter from nodetool info
            info = node_data['nodetool'].get('info', {})
            if info:
                datacenter = info.get('datacenter', datacenter)

            # Get cluster name from cassandra.yaml if not found
            if cluster_name == 'Unknown':
                config = node_data['config'].get('cassandra.yaml', {})
                if config and isinstance(config, dict):
                    cluster_name = config.get('cluster_name', cluster_name)

            break  # Use first node for metadata

        # Get collection date from file timestamps
        collection_date = None
        for node_ip, node_data in self._nodes.items():
            node_path = node_data['path']
            for filename in self.NODETOOL_FILES.keys():
                file_path = node_path / filename
                if file_path.exists():
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if collection_date is None or mtime > collection_date:
                        collection_date = mtime
                    break

        self._metadata = {
            'version': version,
            'cluster_name': cluster_name,
            'datacenter': datacenter,
            'node_count': len(self._nodes),
            'nodes': list(self._nodes.keys()),
            'collection_date': collection_date.isoformat() if collection_date else 'Unknown',
            'source_path': str(self.source_path),
        }

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the loaded collection.

        Returns:
            Metadata dictionary
        """
        return self._metadata

    def get_nodes(self) -> List[str]:
        """
        Get list of node IPs in the collection.

        Returns:
            List of node IP addresses
        """
        return list(self._nodes.keys())

    def get_nodetool_output(
        self,
        command: str,
        node: Optional[str] = None,
        parsed: bool = True
    ) -> Union[Dict, List, str, None]:
        """
        Get nodetool command output.

        For cluster-wide commands (status, describecluster), returns output from
        any available node. For node-specific commands (info, tpstats), returns
        data from the specified node or aggregated from all nodes.

        Args:
            command: Nodetool command name (e.g., 'status', 'info')
            node: Specific node IP, or None for cluster-wide/aggregate
            parsed: If True, return parsed data; if False, return raw text

        Returns:
            Parsed data structure or raw text, or None if not available
        """
        # Cluster-wide commands - same on all nodes
        cluster_wide = ['status', 'describecluster', 'ring']

        if command in cluster_wide:
            # Return from first available node
            for node_ip, node_data in self._nodes.items():
                if command in node_data['nodetool']:
                    return node_data['nodetool'][command] if parsed else node_data['files'].get(f'nodetool_{command}.info')
            return None

        # Node-specific commands
        if node:
            # Return for specific node
            if node in self._nodes:
                node_data = self._nodes[node]
                if parsed:
                    return node_data['nodetool'].get(command)
                else:
                    return node_data['files'].get(f'nodetool_{command}.info')
            return None

        # Aggregate from all nodes
        aggregated = []
        for node_ip, node_data in self._nodes.items():
            if command in node_data['nodetool']:
                data = node_data['nodetool'][command]
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            item['_node'] = node_ip
                        aggregated.append(item)
                elif isinstance(data, dict):
                    data['_node'] = node_ip
                    aggregated.append(data)

        return aggregated if aggregated else None

    def get_config(self, filename: str, node: Optional[str] = None) -> Optional[Any]:
        """
        Get configuration file content.

        Args:
            filename: Config filename (e.g., 'cassandra.yaml')
            node: Specific node IP, or None for first available

        Returns:
            Parsed config or raw content
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
            info_type: Type of info ('disk', 'io_stat')
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
                # Check files dict first
                if filename in node_data['files']:
                    return node_data['files'][filename]
                # Check logs dict (stores Path objects)
                if filename in node_data['logs']:
                    log_path = node_data['logs'][filename]
                    if isinstance(log_path, Path) and log_path.exists():
                        try:
                            return log_path.read_text()
                        except Exception as e:
                            logger.warning(f"Failed to read log file {log_path}: {e}")
                            return None
            return None

        # Check all nodes
        for node_ip, node_data in self._nodes.items():
            if filename in node_data['files']:
                return node_data['files'][filename]
            # Check logs dict
            if filename in node_data['logs']:
                log_path = node_data['logs'][filename]
                if isinstance(log_path, Path) and log_path.exists():
                    try:
                        return log_path.read_text()
                    except Exception as e:
                        logger.warning(f"Failed to read log file {log_path}: {e}")
                        continue

        return None

    def get_log_files(self, pattern: str = 'gc.log*', node: Optional[str] = None) -> Dict[str, List[Path]]:
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
        """
        List all loaded files across all nodes.

        Returns:
            List of filename strings
        """
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
    Find the most recent Instacollector export in a directory.

    Looks for:
    - InstaCollection_*.tar.gz files
    - Directories with IP-like subdirectories

    Args:
        directory: Directory to search

    Returns:
        Path to most recent collection, or None
    """
    candidates = []

    # Look for tarballs
    for pattern in ['InstaCollection_*.tar.gz', 'instacollector_*.tar.gz', '*.tar.gz']:
        for tarball in directory.glob(pattern):
            mtime = tarball.stat().st_mtime
            candidates.append((mtime, tarball))

    # Look for directories with IP subdirs
    ip_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
    for item in directory.iterdir():
        if item.is_dir():
            # Check if this directory contains IP-named subdirs
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

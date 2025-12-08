"""
Diagnostic Loader for Elastic/OpenSearch Support-Diagnostics exports.

This module loads and parses diagnostic exports from the Elastic support-diagnostics
tool (https://github.com/elastic/support-diagnostics), making the data available
for analysis through our health check system.

Supports:
- Extracted directories (api-diagnostics-YYYYMMDD-HHMMSS/)
- Zip archives (api-diagnostics-YYYYMMDD-HHMMSS.zip)

Usage:
    loader = DiagnosticLoader('/path/to/api-diagnostics-20251120-145536')
    loader.load()

    # Access loaded data
    cluster_health = loader.get_file('cluster_health.json')
    nodes_stats = loader.get_file('nodes_stats.json')

    # Get metadata
    metadata = loader.get_metadata()
"""

import json
import logging
import os
import zipfile
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class DiagnosticLoader:
    """
    Loads Elastic/OpenSearch support-diagnostics exports.

    The support-diagnostics tool creates a directory structure like:
        api-diagnostics-20251120-145536/
        ├── manifest.json           # Diagnostic metadata
        ├── diagnostic_manifest.json
        ├── version.json            # Cluster version info
        ├── cluster_health.json
        ├── cluster_stats.json
        ├── nodes_stats.json
        ├── nodes.json
        ├── cat/
        │   ├── cat_nodes.txt
        │   ├── cat_indices.txt
        │   └── ...
        ├── commercial/             # X-Pack/commercial features
        │   ├── ilm_policies.json
        │   ├── security_users.json
        │   └── ...
        └── ...

    This loader:
    1. Extracts zip files if needed
    2. Parses all JSON files into memory
    3. Parses cat/*.txt files into structured data
    4. Provides a unified interface to access all data
    """

    # Files that contain cluster metadata
    METADATA_FILES = ['manifest.json', 'diagnostic_manifest.json', 'version.json']

    # Mapping of cat files to their column headers
    # Based on default Elasticsearch/OpenSearch cat API output
    CAT_FILE_HEADERS = {
        'cat_nodes.txt': ['name', 'ip', 'heap.percent', 'ram.percent', 'cpu',
                         'load_1m', 'load_5m', 'load_15m', 'node.role', 'master'],
        'cat_indices.txt': ['health', 'status', 'index', 'uuid', 'pri', 'rep',
                           'docs.count', 'docs.deleted', 'store.size', 'pri.store.size'],
        'cat_shards.txt': ['index', 'shard', 'prirep', 'state', 'docs', 'store',
                          'ip', 'node'],
        'cat_allocation.txt': ['shards', 'disk.indices', 'disk.used', 'disk.avail',
                              'disk.total', 'disk.percent', 'host', 'ip', 'node'],
        'cat_health.txt': ['epoch', 'timestamp', 'cluster', 'status', 'node.total',
                          'node.data', 'shards', 'pri', 'relo', 'init', 'unassign',
                          'pending_tasks', 'max_task_wait_time', 'active_shards_percent'],
        'cat_recovery.txt': ['index', 'shard', 'time', 'type', 'stage', 'source_host',
                            'source_node', 'target_host', 'target_node', 'repository',
                            'snapshot', 'files', 'files_recovered', 'files_percent',
                            'files_total', 'bytes', 'bytes_recovered', 'bytes_percent',
                            'bytes_total', 'translog_ops', 'translog_ops_recovered',
                            'translog_ops_percent'],
        'cat_segments.txt': ['index', 'shard', 'prirep', 'ip', 'segment', 'generation',
                            'docs.count', 'docs.deleted', 'size', 'size.memory',
                            'committed', 'searchable', 'version', 'compound'],
        'cat_thread_pool.txt': ['node_name', 'name', 'active', 'queue', 'rejected'],
        'cat_pending_tasks.txt': ['insertOrder', 'timeInQueue', 'priority', 'source'],
        'cat_templates.txt': ['name', 'index_patterns', 'order', 'version', 'composed_of'],
        'cat_aliases.txt': ['alias', 'index', 'filter', 'routing.index', 'routing.search',
                           'is_write_index'],
        'cat_fielddata.txt': ['id', 'host', 'ip', 'node', 'field', 'size'],
        'cat_count.txt': ['epoch', 'timestamp', 'count'],
        'cat_master.txt': ['id', 'host', 'ip', 'node'],
        'cat_nodeattrs.txt': ['node', 'host', 'ip', 'attr', 'value'],
        'cat_repositories.txt': ['id', 'type'],
    }

    def __init__(self, path: str):
        """
        Initialize the diagnostic loader.

        Args:
            path: Path to either a zip file or extracted directory
        """
        self.original_path = path
        self.data_dir: Optional[Path] = None
        self._temp_dir: Optional[str] = None
        self._files: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] = {}
        self._loaded = False

    def load(self) -> bool:
        """
        Load and parse all diagnostic files.

        Returns:
            True if loading succeeded, False otherwise
        """
        try:
            # Resolve the data directory (extract zip if needed)
            self.data_dir = self._resolve_data_dir()
            if not self.data_dir:
                return False

            # Load all JSON files
            self._load_json_files()

            # Parse cat files
            self._load_cat_files()

            # Extract metadata
            self._extract_metadata()

            self._loaded = True
            logger.info(f"Successfully loaded diagnostic data from {self.data_dir}")
            logger.info(f"Loaded {len(self._files)} data files")

            return True

        except Exception as e:
            logger.error(f"Failed to load diagnostic data: {e}", exc_info=True)
            return False

    def _resolve_data_dir(self) -> Optional[Path]:
        """
        Resolve the data directory, extracting zip if needed.

        Returns:
            Path to the data directory, or None if resolution failed
        """
        path = Path(self.original_path)

        # Check if it's a zip file
        if path.suffix == '.zip' and path.is_file():
            return self._extract_zip(path)

        # Check if it's a directory
        if path.is_dir():
            # Verify it looks like a diagnostic export
            if self._is_diagnostic_dir(path):
                return path

            # Maybe it's a parent directory containing the export
            for child in path.iterdir():
                if child.is_dir() and self._is_diagnostic_dir(child):
                    return child

        logger.error(f"Path does not appear to be a valid diagnostic export: {path}")
        return None

    def _is_diagnostic_dir(self, path: Path) -> bool:
        """Check if a directory looks like a diagnostic export."""
        # Look for key indicator files
        indicators = ['manifest.json', 'version.json', 'cluster_health.json']
        for indicator in indicators:
            if (path / indicator).exists():
                return True
        return False

    def _extract_zip(self, zip_path: Path) -> Optional[Path]:
        """
        Extract a zip file to a temporary directory.

        Returns:
            Path to the extracted directory
        """
        try:
            self._temp_dir = tempfile.mkdtemp(prefix='diagnostic_import_')

            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(self._temp_dir)

            # Find the actual data directory (zip may contain a subdirectory)
            temp_path = Path(self._temp_dir)

            # Check if extraction created a subdirectory
            children = list(temp_path.iterdir())
            if len(children) == 1 and children[0].is_dir():
                return children[0]

            return temp_path

        except Exception as e:
            logger.error(f"Failed to extract zip file: {e}")
            if self._temp_dir:
                shutil.rmtree(self._temp_dir, ignore_errors=True)
            return None

    def _load_json_files(self):
        """Load all JSON files from the diagnostic directory."""
        if not self.data_dir:
            return

        # Load root-level JSON files
        for json_file in self.data_dir.glob('*.json'):
            self._load_json_file(json_file, '')

        # Load commercial/ subdirectory
        commercial_dir = self.data_dir / 'commercial'
        if commercial_dir.exists():
            for json_file in commercial_dir.glob('*.json'):
                self._load_json_file(json_file, 'commercial/')

    def _load_json_file(self, file_path: Path, prefix: str):
        """Load a single JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
                    key = f"{prefix}{file_path.name}"
                    self._files[key] = data
                    logger.debug(f"Loaded {key}")
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON file {file_path}: {e}")
        except Exception as e:
            logger.warning(f"Failed to load file {file_path}: {e}")

    def _load_cat_files(self):
        """Parse cat/*.txt files into structured data."""
        if not self.data_dir:
            return

        cat_dir = self.data_dir / 'cat'
        if not cat_dir.exists():
            logger.debug("No cat/ directory found")
            return

        for txt_file in cat_dir.glob('*.txt'):
            self._parse_cat_file(txt_file)

    def _parse_cat_file(self, file_path: Path):
        """
        Parse a cat API text file into structured JSON-like data.

        Cat files are space-separated tables with headers in the first line.
        We parse them into lists of dictionaries for consistency with JSON data.

        Support-diagnostics exports include headers in the files, so we always
        read headers from the first line rather than using hardcoded mappings.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if not lines:
                self._files[f"cat/{file_path.name}"] = []
                return

            # First line is always headers in support-diagnostics exports
            first_line = lines[0].strip()
            if not first_line:
                self._files[f"cat/{file_path.name}"] = []
                return

            headers = first_line.split()
            data_lines = lines[1:]

            # Parse data lines
            records = []
            for line in data_lines:
                line = line.strip()
                if not line:
                    continue

                # Split by whitespace, handling variable spacing
                values = line.split()

                # Create record, padding with None if needed
                record = {}
                for i, header in enumerate(headers):
                    if i < len(values):
                        record[header] = self._convert_value(values[i])
                    else:
                        record[header] = None

                records.append(record)

            self._files[f"cat/{file_path.name}"] = records
            logger.debug(f"Parsed {file_path.name}: {len(records)} records")

        except Exception as e:
            logger.warning(f"Failed to parse cat file {file_path}: {e}")

    def _convert_value(self, value: str) -> Any:
        """Convert a string value to appropriate type."""
        if value == '-' or value == '':
            return None

        # Try integer
        try:
            return int(value)
        except ValueError:
            pass

        # Try float
        try:
            return float(value)
        except ValueError:
            pass

        # Keep as string
        return value

    def _extract_metadata(self):
        """Extract metadata from loaded files."""
        # Primary metadata from manifest
        manifest = self._files.get('manifest.json', {})
        diag_manifest = self._files.get('diagnostic_manifest.json', {})
        version = self._files.get('version.json', {})

        # Get collection timestamp
        collection_date = (
            manifest.get('collectionDate') or
            diag_manifest.get('timestamp') or
            datetime.utcnow().isoformat() + 'Z'
        )

        # Parse version info
        version_info = version.get('version', {})
        if isinstance(version_info, str):
            version_number = version_info
        else:
            version_number = version_info.get('number', 'Unknown')

        # Get product info
        product_version = manifest.get('Product Version', {})

        self._metadata = {
            'collection_date': collection_date,
            'cluster_name': version.get('cluster_name', 'Unknown'),
            'cluster_uuid': version.get('cluster_uuid'),
            'node_name': version.get('name'),
            'version': version_number,
            'version_major': product_version.get('major') if product_version else None,
            'version_minor': product_version.get('minor') if product_version else None,
            'build_type': version_info.get('build_type') if isinstance(version_info, dict) else None,
            'build_flavor': version_info.get('build_flavor') if isinstance(version_info, dict) else None,
            'diagnostic_version': manifest.get('diagVersion') or diag_manifest.get('diagnostic'),
            'diagnostic_type': diag_manifest.get('type', 'elasticsearch_diagnostic'),
            'source': 'support-diagnostics-import',
        }

        logger.info(f"Extracted metadata: cluster={self._metadata['cluster_name']}, "
                   f"version={self._metadata['version']}")

    def get_file(self, filename: str) -> Optional[Any]:
        """
        Get the parsed contents of a specific file.

        Args:
            filename: Name of the file (e.g., 'cluster_health.json', 'cat/cat_nodes.txt')

        Returns:
            Parsed file contents, or None if not found
        """
        if not self._loaded:
            logger.warning("Data not loaded. Call load() first.")
            return None

        return self._files.get(filename)

    def get_metadata(self) -> Dict[str, Any]:
        """Get diagnostic metadata (cluster name, version, collection date, etc.)."""
        return self._metadata.copy()

    def list_files(self) -> List[str]:
        """List all loaded file names."""
        return list(self._files.keys())

    def has_file(self, filename: str) -> bool:
        """Check if a specific file was loaded."""
        return filename in self._files

    def get_all_data(self) -> Dict[str, Any]:
        """Get all loaded data as a dictionary."""
        return self._files.copy()

    def cleanup(self):
        """Clean up temporary files if a zip was extracted."""
        if self._temp_dir:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            self._temp_dir = None

    def __enter__(self):
        """Context manager entry."""
        self.load()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup temp files."""
        self.cleanup()
        return False


def load_diagnostic(path: str) -> Optional[DiagnosticLoader]:
    """
    Convenience function to load a diagnostic export.

    Args:
        path: Path to zip file or extracted directory

    Returns:
        Loaded DiagnosticLoader instance, or None if loading failed
    """
    loader = DiagnosticLoader(path)
    if loader.load():
        return loader
    return None

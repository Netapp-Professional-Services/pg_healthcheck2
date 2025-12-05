"""
Diagnostic Connector for Elastic/OpenSearch Support-Diagnostics exports.

This connector implements the same interface as OpenSearchConnector but reads
from pre-collected diagnostic files instead of making live API calls. This allows
all existing health checks to work unchanged against imported diagnostic data.

Usage:
    from plugins.opensearch.diagnostic_connector import DiagnosticConnector

    connector = DiagnosticConnector(settings, '/path/to/diagnostic-export')
    connector.connect()

    # Use exactly like OpenSearchConnector
    health = connector.execute_query({"operation": "cluster_health"})
    nodes = connector.execute_query({"operation": "cat_nodes"})

    connector.disconnect()
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from plugins.opensearch.diagnostic_loader import DiagnosticLoader
from plugins.common.output_formatters import AsciiDocFormatter
from plugins.common.cve_mixin import CVECheckMixin

logger = logging.getLogger(__name__)


class DiagnosticConnector(CVECheckMixin):
    """
    Connector that reads from support-diagnostics exports.

    Implements the same interface as OpenSearchConnector so existing
    health checks work without modification. Maps execute_query operations
    to the appropriate diagnostic files.
    """

    # Map our operation names to diagnostic file names
    OPERATION_FILE_MAP = {
        # Cluster-level
        'cluster_health': 'cluster_health.json',
        'cluster_stats': 'cluster_stats.json',
        'cluster_state': 'cluster_state.json',
        'cluster_settings': 'cluster_settings.json',
        'cluster_settings_defaults': 'cluster_settings_defaults.json',
        'pending_tasks': 'cluster_pending_tasks.json',

        # Node-level
        'node_stats': 'nodes_stats.json',
        'nodes': 'nodes.json',
        'nodes_info': 'nodes.json',
        'nodes_short': 'nodes_short.json',
        'nodes_usage': 'nodes_usage.json',
        'hot_threads': 'nodes_hot_threads.txt',

        # Index-level
        'index_stats': 'indices_stats.json',
        'indices': 'indices.json',
        'mappings': 'mapping.json',
        'settings': 'settings.json',
        'aliases': 'alias.json',
        'templates': 'templates.json',
        'index_templates': 'index_templates.json',
        'component_templates': 'component_templates.json',

        # Shard-level
        'allocation': 'allocation.json',
        'allocation_explain': 'allocation_explain.json',
        'shard_stores': 'shard_stores.json',
        'recovery': 'recovery.json',
        'segments': 'segments.json',

        # Cat API equivalents
        'cat_nodes': 'cat/cat_nodes.txt',
        'cat_indices': 'cat/cat_indices.txt',
        'cat_shards': 'cat/cat_shards.txt',
        'cat_allocation': 'cat/cat_allocation.txt',
        'cat_health': 'cat/cat_health.txt',
        'cat_recovery': 'cat/cat_recovery.txt',
        'cat_segments': 'cat/cat_segments.txt',
        'cat_thread_pool': 'cat/cat_thread_pool.txt',
        'cat_pending_tasks': 'cat/cat_pending_tasks.txt',
        'cat_templates': 'cat/cat_templates.txt',
        'cat_aliases': 'cat/cat_aliases.txt',
        'cat_plugins': 'plugins.json',
        'cat_fielddata': 'cat/cat_fielddata.txt',
        'cat_master': 'cat/cat_master.txt',
        'cat_nodeattrs': 'cat/cat_nodeattrs.txt',
        'cat_repositories': 'cat/cat_repositories.txt',
        'cat_count': 'cat/cat_count.txt',

        # Tasks
        'tasks': 'tasks.json',

        # Other
        'plugins': 'plugins.json',
        'version': 'version.json',
        'licenses': 'licenses.json',
        'pipelines': 'pipelines.json',
        'repositories': 'repositories.json',
        'dangling_indices': 'dangling_indices.json',
        'remote_cluster_info': 'remote_cluster_info.json',
        'remote_info': 'remote_cluster_info.json',
        'fielddata_stats': 'fielddata_stats.json',
        'ssl_certs': 'ssl_certs.json',

        # Commercial/X-Pack features
        'ilm_policies': 'commercial/ilm_policies.json',
        'ilm_status': 'commercial/ilm_status.json',
        'ilm_explain': 'commercial/ilm_explain.json',
        'slm_policies': 'commercial/slm_policies.json',
        'slm_status': 'commercial/slm_status.json',
        'slm_stats': 'commercial/slm_stats.json',
        'ccr_stats': 'commercial/ccr_stats.json',
        'ccr_autofollow': 'commercial/ccr_autofollow_patterns.json',
        'security_users': 'commercial/security_users.json',
        'security_roles': 'commercial/security_roles.json',
        'security_role_mappings': 'commercial/security_role_mappings.json',
        'ml_info': 'commercial/ml_info.json',
        'ml_stats': 'commercial/ml_stats.json',
        'ml_anomaly_detectors': 'commercial/ml_anomaly_detectors.json',
        'ml_datafeeds': 'commercial/ml_datafeeds.json',
        'watcher_stats': 'commercial/watcher_stats.json',
        'transform': 'commercial/transform.json',
        'transform_stats': 'commercial/transform_stats.json',
        'xpack': 'commercial/xpack.json',
        'enrich_policies': 'commercial/enrich_policies.json',
        'enrich_stats': 'commercial/enrich_stats.json',
        'data_stream': 'commercial/data_stream.json',
        'rollup_jobs': 'commercial/rollup_jobs.json',

        # Additional mappings for comprehensive coverage
        'cat_thread_pool': 'cat/cat_thread_pool.txt',
        'allocation_explain_disk': 'allocation_explain_disk.json',
        'watcher_stack': 'commercial/watcher_stack.json',
        'ccr_follower_info': 'commercial/ccr_follower_info.json',
        'ml_datafeeds_stats': 'commercial/ml_datafeeds_stats.json',
        'ml_dataframe': 'commercial/ml_dataframe.json',
        'ml_dataframe_stats': 'commercial/ml_dataframe_stats.json',
        'ml_trained_models': 'commercial/ml_trained_models.json',
        'ml_trained_models_stats': 'commercial/ml_trained_models_stats.json',
        'security_privileges': 'commercial/security_priv.json',
        'ilm_explain_errors': 'commercial/ilm_explain_only_errors.json',
        'ilm_explain_only_errors': 'commercial/ilm_explain_only_errors.json',
    }

    def __init__(self, settings: Dict[str, Any], diagnostic_path: str):
        """
        Initialize the diagnostic connector.

        Args:
            settings: Configuration settings (company_name, etc.)
            diagnostic_path: Path to diagnostic zip or directory
        """
        self.settings = settings
        self.diagnostic_path = diagnostic_path
        self.loader: Optional[DiagnosticLoader] = None
        self._version_info: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] = {}
        self.cluster_name: Optional[str] = None
        self.cluster_nodes: List[Dict[str, Any]] = []
        self.formatter = AsciiDocFormatter()

        # Environment info
        self.environment = 'imported'
        self.environment_details = {
            'type': 'support-diagnostics-import',
            'source_path': diagnostic_path
        }

        # Technology name for CVE lookups
        self.technology_name = 'opensearch'

        logger.info(f"DiagnosticConnector initialized for: {diagnostic_path}")

    def connect(self):
        """
        Load the diagnostic data.

        This is analogous to connecting to a live cluster - we load
        all the diagnostic files into memory.
        """
        try:
            self.loader = DiagnosticLoader(self.diagnostic_path)
            if not self.loader.load():
                raise ConnectionError(f"Failed to load diagnostic data from {self.diagnostic_path}")

            # Extract metadata
            self._metadata = self.loader.get_metadata()
            self.cluster_name = self._metadata.get('cluster_name', 'Unknown')

            # Build version info
            version_number = self._metadata.get('version', 'Unknown')
            self._version_info = {
                'number': version_number,
                'version_string': version_number,  # CVE mixin expects this key
                'build_type': self._metadata.get('build_type'),
                'build_flavor': self._metadata.get('build_flavor'),
                'major_version': self._metadata.get('version_major', 0),
            }

            # Extract node info
            self._extract_cluster_nodes()

            # Initialize CVE support (requires network access to NVD API)
            # This allows CVE checks even for offline diagnostic imports
            self.initialize_cve_support()

            # Display connection status
            self._display_connection_status()

            logger.info("Successfully loaded diagnostic data")

        except Exception as e:
            logger.error(f"Failed to load diagnostic data: {e}")
            raise ConnectionError(f"Could not load diagnostic data: {e}")

    def _extract_cluster_nodes(self):
        """Extract node information from diagnostic data."""
        self.cluster_nodes = []

        # Try cat_nodes first - handles various column header formats
        cat_nodes = self.loader.get_file('cat/cat_nodes.txt')
        if cat_nodes and isinstance(cat_nodes, list):
            for node in cat_nodes:
                # Support both full and abbreviated column names
                # Full: name, ip, node.role, master
                # Abbreviated: n, i, role, m
                node_info = {
                    'name': node.get('name') or node.get('n'),
                    'ip': node.get('ip') or node.get('i'),
                    'role': node.get('node.role') or node.get('role', 'Unknown'),
                    'is_master': (node.get('master') or node.get('m')) == '*'
                }
                self.cluster_nodes.append(node_info)
            return

        # Fallback to nodes.json
        nodes_data = self.loader.get_file('nodes.json')
        if nodes_data and isinstance(nodes_data, dict):
            nodes = nodes_data.get('nodes', {})
            for node_id, node_info in nodes.items():
                self.cluster_nodes.append({
                    'name': node_info.get('name', node_id),
                    'ip': node_info.get('ip'),
                    'role': ','.join(node_info.get('roles', [])),
                    'is_master': False  # Would need cluster_state to determine
                })

    def _display_connection_status(self):
        """Display status of loaded diagnostic data."""
        print("✅ Successfully loaded diagnostic export")
        print(f"   - Source: {self.diagnostic_path}")
        print(f"   - Collection Date: {self._metadata.get('collection_date', 'Unknown')}")
        print(f"   - Version: {self._version_info.get('number', 'Unknown')}")
        print(f"   - Cluster: {self.cluster_name}")
        print(f"   - Nodes: {len(self.cluster_nodes)}")
        print(f"   - Diagnostic Tool Version: {self._metadata.get('diagnostic_version', 'Unknown')}")
        print(f"   - Files Loaded: {len(self.loader.list_files())}")

        if self.cluster_nodes:
            print(f"   - Node Details:")
            for node in self.cluster_nodes[:5]:
                master_indicator = " (Master)" if node.get('is_master') else ""
                print(f"      • {node['name']} ({node.get('ip', 'N/A')}) - {node.get('role', 'N/A')}{master_indicator}")
            if len(self.cluster_nodes) > 5:
                print(f"      ... and {len(self.cluster_nodes) - 5} more")

    def disconnect(self):
        """Clean up resources."""
        if self.loader:
            self.loader.cleanup()
            self.loader = None
        logger.info("Diagnostic connector closed")

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
            'version': self._version_info.get('number', 'Unknown'),
            'db_name': self.cluster_name or 'Unknown',
            'environment': 'imported',
            'environment_details': {
                **self.environment_details,
                'collection_date': self._metadata.get('collection_date'),
                'diagnostic_version': self._metadata.get('diagnostic_version'),
            }
        }

    def execute_query(self, query, params=None, return_raw=False) -> Any:
        """
        Execute a query by reading from diagnostic files.

        Supports the same operation types as OpenSearchConnector,
        but reads from pre-loaded files instead of making API calls.

        Args:
            query: JSON string or dict with operation details
            params: Additional parameters (optional)
            return_raw: If True, return raw response

        Returns:
            Query results as dict or list
        """
        if not self.loader:
            return {"error": "Diagnostic data not loaded. Call connect() first."}

        try:
            # Parse query if string
            if isinstance(query, str):
                try:
                    query_dict = json.loads(query)
                except json.JSONDecodeError:
                    return {"error": f"Invalid query format: {query}"}
            else:
                query_dict = query

            operation = query_dict.get('operation')
            if not operation:
                return {"error": "No operation specified"}

            # Map operation to file
            filename = self.OPERATION_FILE_MAP.get(operation)

            if not filename:
                logger.warning(f"Unknown operation: {operation}")
                return {"error": f"Unsupported operation: {operation}"}

            # Get the file data
            data = self.loader.get_file(filename)

            if data is None:
                logger.debug(f"File not found for operation {operation}: {filename}")
                return {"error": f"Data not available: {filename}"}

            # Handle special cases that need transformation
            return self._transform_response(operation, data, query_dict)

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {"error": str(e)}

    def _transform_response(self, operation: str, data: Any, query_dict: Dict) -> Any:
        """
        Transform diagnostic file data to match live API response format.

        Some operations need response transformation to match what the
        health checks expect from the live API.
        """
        # Cat operations that return lists directly
        if operation.startswith('cat_'):
            # Cat files are already parsed to list of dicts
            if operation == 'cat_segments':
                return {"segments": data}
            elif operation == 'cat_recovery':
                return {"recovery": data}
            elif operation == 'cat_plugins':
                # plugins.json format differs from cat_plugins
                if isinstance(data, list):
                    return {"plugins": data}
                elif isinstance(data, dict):
                    # Handle empty plugins []
                    return {"plugins": data if data else []}
            return data

        # Hot threads - read as text file
        if operation == 'hot_threads':
            # nodes_hot_threads.txt is raw text
            txt_path = self.loader.data_dir / 'nodes_hot_threads.txt'
            if txt_path and txt_path.exists():
                try:
                    with open(txt_path, 'r') as f:
                        return {"hot_threads": f.read()}
                except:
                    pass
            return {"hot_threads": ""}

        # Node stats may need node_id filtering
        if operation == 'node_stats':
            node_id = query_dict.get('node_id', '_all')
            if node_id != '_all' and isinstance(data, dict):
                nodes = data.get('nodes', {})
                if node_id in nodes:
                    return {
                        '_nodes': data.get('_nodes', {}),
                        'cluster_name': data.get('cluster_name'),
                        'nodes': {node_id: nodes[node_id]}
                    }
            return data

        # Index stats may need index filtering
        if operation == 'index_stats':
            index_name = query_dict.get('index', '_all')
            if index_name != '_all' and isinstance(data, dict):
                indices = data.get('indices', {})
                if index_name in indices:
                    return {
                        '_all': data.get('_all', {}),
                        'indices': {index_name: indices[index_name]}
                    }
            return data

        # Pending tasks needs to be wrapped
        if operation == 'pending_tasks':
            if isinstance(data, dict) and 'tasks' not in data:
                return {"tasks": data.get('tasks', [])}
            return data

        # Default: return data as-is
        return data

    def has_ssh_support(self) -> bool:
        """SSH is not applicable for diagnostic imports."""
        return False

    def has_aws_support(self) -> bool:
        """AWS CloudWatch is not applicable for diagnostic imports."""
        return False

    def get_cloudwatch_metrics(self, *args, **kwargs):
        """CloudWatch metrics are not available from diagnostic exports."""
        return {}


def create_diagnostic_connector(settings: Dict[str, Any], diagnostic_path: str) -> DiagnosticConnector:
    """
    Factory function to create a diagnostic connector.

    Args:
        settings: Configuration settings
        diagnostic_path: Path to diagnostic export

    Returns:
        Configured DiagnosticConnector instance
    """
    connector = DiagnosticConnector(settings, diagnostic_path)
    return connector

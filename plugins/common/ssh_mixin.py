"""
SSH support mixin for database connectors.

Provides standard methods for SSH-enabled connectors to check
SSH availability and provide consistent error messages.
Supports both single-host and multi-host SSH configurations.
"""

import logging
from typing import Optional, List, Dict, Callable
from plugins.common.ssh_handler import SSHConnectionManager

logger = logging.getLogger(__name__)


class SSHSupportMixin:
    """
    Mixin class that provides SSH support for single or multiple hosts.
    
    Any connector that needs SSH should inherit this mixin and call
    initialize_ssh() in its __init__ method.
    
    Supports:
    - Single host: ssh_host setting
    - Multiple hosts: ssh_hosts setting (list)
    - Automatic host-to-node mapping via callback
    
    Example:
        class KafkaConnector(SSHSupportMixin):
            def __init__(self, settings):
                self.settings = settings
                self.initialize_ssh()
    """
    
    def initialize_ssh(self, host_to_node_mapper: Optional[Callable] = None):
        """
        Initialize SSH support - call this from connector __init__.
        
        Args:
            host_to_node_mapper: Optional callback function to map hosts to node IDs.
                                Function signature: (host: str) -> str|int
        """
        self.ssh_managers = {}  # Dict of {host: SSHConnectionManager}
        self.ssh_host_to_node = {}  # Map SSH hosts to node IDs (broker, cassandra node, etc.)
        self._host_to_node_mapper = host_to_node_mapper
        
        # Detect single vs multiple SSH hosts
        ssh_hosts = self.settings.get('ssh_hosts')
        if not ssh_hosts:
            # Backward compatible - single host
            single_host = self.settings.get('ssh_host')
            if single_host:
                ssh_hosts = [single_host]
        
        # Initialize SSH managers for each host
        if ssh_hosts:
            for ssh_host in ssh_hosts:
                # Create per-host settings
                host_settings = {
                    'ssh_host': ssh_host,
                    'ssh_user': self.settings.get('ssh_user'),
                    'ssh_key_file': self.settings.get('ssh_key_file'),
                    'ssh_password': self.settings.get('ssh_password'),
                    'ssh_port': self.settings.get('ssh_port', 22),
                    'ssh_timeout': self.settings.get('ssh_timeout', 10),
                    'ssh_command_timeout': self.settings.get('ssh_command_timeout', 30),
                    'ssh_strict_host_key_checking': self.settings.get('ssh_strict_host_key_checking', False)
                }
                
                try:
                    self.ssh_managers[ssh_host] = SSHConnectionManager(host_settings)
                    logger.info(f"SSH manager created for host: {ssh_host}")
                except Exception as e:
                    logger.warning(f"Could not create SSH manager for {ssh_host}: {e}")
            
            logger.info(f"Initialized {len(self.ssh_managers)} SSH manager(s)")
    
    def has_ssh_support(self) -> bool:
        """
        Check if SSH operations are supported and available.
        
        Returns:
            bool: True if at least one SSH connection is configured, False otherwise
        """
        return len(getattr(self, 'ssh_managers', {})) > 0
    
    def get_ssh_hosts(self) -> List[str]:
        """Get list of all configured SSH hosts."""
        return list(getattr(self, 'ssh_managers', {}).keys())
    
    def get_ssh_manager(self, host: Optional[str] = None) -> Optional[SSHConnectionManager]:
        """
        Get SSH manager for a specific host, or the first available one.
        
        Args:
            host: Specific host to get manager for, or None for first available
            
        Returns:
            SSHConnectionManager instance or None
        """
        managers = getattr(self, 'ssh_managers', {})
        
        if not managers:
            return None
        
        if host and host in managers:
            return managers[host]
        
        # Return first available manager (for backward compatibility)
        return next(iter(managers.values()))
    
    def connect_all_ssh(self) -> List[str]:
        """
        Connect all SSH managers.
        
        Returns:
            List of successfully connected hosts
        """
        connected_hosts = []
        
        for ssh_host, ssh_manager in getattr(self, 'ssh_managers', {}).items():
            try:
                ssh_manager.connect()
                connected_hosts.append(ssh_host)
                logger.info(f"SSH connection established to {ssh_host}")
            except Exception as e:
                logger.warning(f"SSH connection failed for {ssh_host}: {e}")
        
        return connected_hosts
    
    def disconnect_all_ssh(self):
        """Disconnect all SSH managers."""
        for ssh_host, ssh_manager in getattr(self, 'ssh_managers', {}).items():
            try:
                ssh_manager.disconnect()
                logger.info(f"Disconnected SSH from {ssh_host}")
            except Exception as e:
                logger.warning(f"Error disconnecting SSH from {ssh_host}: {e}")
    
    def execute_ssh_on_all_hosts(self, command: str, description: str = "SSH command") -> List[Dict]:
        """
        Execute a command on all SSH-enabled hosts.
        
        Args:
            command: Shell command to execute
            description: Human-readable description for logging
        
        Returns:
            List of dicts with {host, node_id, success, output/error, exit_code}
        """
        results = []
        
        for ssh_host, ssh_manager in getattr(self, 'ssh_managers', {}).items():
            node_id = self.ssh_host_to_node.get(ssh_host, 'unknown')
            
            try:
                # Ensure connection is active
                ssh_manager.ensure_connected()
                
                # Execute command
                stdout, stderr, exit_code = ssh_manager.execute_command(command)
                
                if exit_code == 0:
                    results.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'success': True,
                        'output': stdout,
                        'stderr': stderr,
                        'exit_code': exit_code
                    })
                else:
                    logger.warning(f"Command failed on {ssh_host} with exit code {exit_code}: {stderr}")
                    results.append({
                        'host': ssh_host,
                        'node_id': node_id,
                        'success': False,
                        'error': f"Command failed (exit {exit_code}): {stderr}",
                        'exit_code': exit_code
                    })
            except Exception as e:
                logger.warning(f"SSH command failed on {ssh_host}: {e}")
                results.append({
                    'host': ssh_host,
                    'node_id': node_id,
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    def map_ssh_hosts_to_nodes(self, host_node_mapping: Dict[str, any]):
        """
        Map SSH hosts to node IDs (broker IDs, Cassandra nodes, etc.).
        
        This can be called manually or automatically during connect().
        
        Args:
            host_node_mapping: Dict mapping host strings to node IDs
        """
        for ssh_host in getattr(self, 'ssh_managers', {}).keys():
            # Try direct match
            if ssh_host in host_node_mapping:
                self.ssh_host_to_node[ssh_host] = host_node_mapping[ssh_host]
                logger.info(f"Mapped SSH host {ssh_host} to node {host_node_mapping[ssh_host]}")
                continue
            
            # Try partial match
            for node_host, node_id in host_node_mapping.items():
                if ssh_host in node_host or node_host in ssh_host:
                    self.ssh_host_to_node[ssh_host] = node_id
                    logger.info(f"Mapped SSH host {ssh_host} to node {node_id} via partial match")
                    break
            
            # Try mapper callback if provided
            if ssh_host not in self.ssh_host_to_node and self._host_to_node_mapper:
                try:
                    node_id = self._host_to_node_mapper(ssh_host)
                    if node_id:
                        self.ssh_host_to_node[ssh_host] = node_id
                        logger.info(f"Mapped SSH host {ssh_host} to node {node_id} via callback")
                except Exception as e:
                    logger.warning(f"Mapper callback failed for {ssh_host}: {e}")
    
    def get_ssh_skip_message(self, operation_name: Optional[str] = None) -> tuple:
        """
        Generate a standard skip message for checks that require SSH.
        
        Args:
            operation_name: Optional name of the operation (e.g., 'disk usage check')
        
        Returns:
            tuple: (adoc_message, structured_data_dict) suitable for returning from a check
        """
        op_text = f" for {operation_name}" if operation_name else ""
        
        adoc_message = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires SSH access{op_text}.\n\n"
            "Configure the following in your settings:\n\n"
            "**For single host:**\n"
            "* `ssh_host`: Hostname or IP address\n\n"
            "**For multiple hosts (recommended for clusters):**\n"
            "* `ssh_hosts`: List of hostnames/IPs\n\n"
            "**Authentication (required):**\n"
            "* `ssh_user`: SSH username\n"
            "* `ssh_key_file` OR `ssh_password`: Authentication method\n\n"
            "**Optional:**\n"
            "* `ssh_port`: SSH port (default: 22)\n"
            "* `ssh_timeout`: Connection timeout in seconds (default: 10)\n"
            "====\n"
        )
        
        structured_data = {
            "status": "skipped",
            "reason": "SSH not configured",
            "required_settings": ["ssh_host or ssh_hosts", "ssh_user", "ssh_key_file or ssh_password"]
        }
        
        return adoc_message, structured_data
    
    def get_ssh_settings_info(self) -> dict:
        """
        Get information about SSH configuration (safe for logging/debugging).
        
        Returns:
            dict: SSH configuration info with sensitive data masked
        """
        if not hasattr(self, 'settings'):
            return {'configured': False}
        
        settings = self.settings
        ssh_hosts = self.get_ssh_hosts()
        
        return {
            'configured': len(ssh_hosts) > 0,
            'host_count': len(ssh_hosts),
            'hosts': ssh_hosts,
            'user': settings.get('ssh_user', 'Not configured'),
            'port': settings.get('ssh_port', 22),
            'auth_method': 'key' if settings.get('ssh_key_file') else ('password' if settings.get('ssh_password') else 'none'),
            'timeout': settings.get('ssh_timeout', 10),
            'connected_hosts': [h for h, m in getattr(self, 'ssh_managers', {}).items() if m.is_connected()]
        }

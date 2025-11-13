"""
SSH connection management for database health checks.

This module provides a reusable SSH connection manager that can be used
by any database connector that needs to execute remote commands.
"""

import logging
from typing import Optional, Dict, Tuple

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

logger = logging.getLogger(__name__)


class SSHConnectionManager:
    """
    Manages SSH connections for remote command execution.
    
    Features:
    - Connection pooling (reuses connections)
    - Automatic reconnection on failure
    - Support for key-based and password authentication
    - Connection timeout handling
    - Secure host key verification
    
    Example:
        ssh_manager = SSHConnectionManager(settings)
        ssh_manager.connect()
        output, stderr, exit_code = ssh_manager.execute_command("df -h")
        ssh_manager.disconnect()
    """
    
    def __init__(self, settings: Dict):
        """
        Initialize SSH connection manager.
        
        Args:
            settings: Dictionary with SSH configuration:
                - ssh_host: Hostname or IP address
                - ssh_user: SSH username
                - ssh_key_file: Path to private key (optional)
                - ssh_password: SSH password (optional)
                - ssh_timeout: Connection timeout in seconds (default: 10)
                - ssh_port: SSH port (default: 22)
                - ssh_command_timeout: Command execution timeout (default: 30)
                - ssh_strict_host_key_checking: Enable host key verification (default: True)
                - ssh_known_hosts_file: Path to custom known_hosts file (optional)
        """
        if not PARAMIKO_AVAILABLE:
            raise ImportError(
                "Paramiko library is required for SSH support. "
                "Install it with: pip install paramiko"
            )
        
        self.settings = settings
        self.client: Optional[paramiko.SSHClient] = None
        self._validate_settings()
    
    def _validate_settings(self):
        """Validates that required SSH settings are present."""
        required = ['ssh_host', 'ssh_user']
        missing = [key for key in required if not self.settings.get(key)]
        
        if missing:
            raise ValueError(
                f"Missing required SSH settings: {', '.join(missing)}. "
                f"Required: ssh_host, ssh_user. "
                f"Optional: ssh_key_file or ssh_password"
            )
        
        # Require at least one authentication method
        if not self.settings.get('ssh_key_file') and not self.settings.get('ssh_password'):
            raise ValueError(
                "Either ssh_key_file or ssh_password must be configured for SSH authentication"
            )
    
    def connect(self) -> None:
        """
        Establishes SSH connection to the remote host.
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            self.client = paramiko.SSHClient()
            
            # Security: Configure host key policy
            strict_host_key_checking = self.settings.get('ssh_strict_host_key_checking', True)
            
            if strict_host_key_checking:
                # Production mode: Load known_hosts for security
                self.client.load_system_host_keys()
                
                # Optionally load custom known_hosts file
                known_hosts = self.settings.get('ssh_known_hosts_file')
                if known_hosts:
                    try:
                        self.client.load_host_keys(known_hosts)
                        logger.info(f"Loaded custom known_hosts from {known_hosts}")
                    except IOError as e:
                        logger.warning(f"Could not load known_hosts file {known_hosts}: {e}")
                
                # Use RejectPolicy - will raise exception if host key unknown
                self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
                logger.info("SSH host key checking enabled (secure mode)")
            else:
                # Development/testing mode: Auto-accept host keys (INSECURE!)
                logger.warning(
                    "⚠️  SSH host key checking DISABLED! "
                    "This is insecure and should only be used in development/testing. "
                    "Set ssh_strict_host_key_checking=True for production."
                )
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_args = {
                'hostname': self.settings['ssh_host'],
                'username': self.settings['ssh_user'],
                'port': self.settings.get('ssh_port', 22),
                'timeout': self.settings.get('ssh_timeout', 10),
            }

            # Add authentication
            if self.settings.get('ssh_key_file'):
                connect_args['key_filename'] = self.settings['ssh_key_file']
            elif self.settings.get('ssh_password'):
                connect_args['password'] = self.settings['ssh_password']

            self.client.connect(**connect_args)

            # Enable TCP keep-alive to prevent idle connection timeouts
            # This sends periodic keep-alive packets to maintain the connection
            transport = self.client.get_transport()
            if transport:
                # Set keep-alive interval (default 60 seconds)
                keep_alive_interval = self.settings.get('ssh_keepalive_interval', 60)
                transport.set_keepalive(keep_alive_interval)
                logger.debug(f"SSH keep-alive enabled (interval: {keep_alive_interval}s)")

            logger.info(f"✅ SSH connection established to {self.settings['ssh_host']}")
            
        except paramiko.SSHException as e:
            logger.error(f"SSH connection failed: {e}")
            raise ConnectionError(f"Could not establish SSH connection: {e}")
        except Exception as e:
            logger.error(f"SSH connection failed: {e}")
            raise ConnectionError(f"Could not establish SSH connection: {e}")
    
    def disconnect(self) -> None:
        """Closes the SSH connection."""
        if self.client:
            try:
                self.client.close()
                logger.info("🔌 SSH connection closed")
            except Exception as e:
                logger.warning(f"Error closing SSH connection: {e}")
            finally:
                self.client = None
    
    def is_connected(self) -> bool:
        """
        Checks if SSH connection is active.
        
        Returns:
            bool: True if connection is active, False otherwise
        """
        if not self.client:
            return False
        
        transport = self.client.get_transport()
        return transport is not None and transport.is_active()
    
    def ensure_connected(self) -> None:
        """Ensures connection is active, reconnects if necessary."""
        if not self.is_connected():
            logger.info("SSH connection lost, reconnecting...")
            self.connect()
    
    def execute_command(self, command: str, timeout: Optional[int] = None) -> Tuple[str, str, int]:
        """
        Executes a command on the remote host.
        
        Args:
            command: Shell command to execute
            timeout: Optional command timeout in seconds (default: from settings or 30s)
        
        Returns:
            Tuple of (stdout, stderr, exit_code)
        
        Raises:
            ConnectionError: If not connected
            TimeoutError: If command exceeds timeout
            RuntimeError: If command execution fails
        """
        if not self.is_connected():
            raise ConnectionError("SSH connection not established. Call connect() first.")
        
        command_timeout = timeout or self.settings.get('ssh_command_timeout', 30)
        
        try:
            # Execute command
            stdin, stdout, stderr = self.client.exec_command(
                command,
                timeout=command_timeout
            )
            
            # Read output
            exit_code = stdout.channel.recv_exit_status()
            stdout_text = stdout.read().decode('utf-8')
            stderr_text = stderr.read().decode('utf-8')
            
            # Log command (truncate if very long)
            cmd_display = command[:100] + '...' if len(command) > 100 else command
            logger.debug(f"Command executed: {cmd_display}")
            logger.debug(f"Exit code: {exit_code}")
            
            return stdout_text, stderr_text, exit_code
        
        except TimeoutError as e:
            logger.error(f"Command timeout after {command_timeout}s: {command[:50]}...")
            raise TimeoutError(f"Command timed out after {command_timeout}s")
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            cmd_display = command[:50] + '...' if len(command) > 50 else command
            raise RuntimeError(f"Failed to execute command '{cmd_display}': {e}")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

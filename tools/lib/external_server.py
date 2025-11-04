# lib/external_server.py
"""
External server configuration for integration testing.

Handles environment variable overrides and config file settings
for connecting to real database servers instead of containers.

Usage:
    from lib.external_server import get_external_server_config, create_external_connector
    
    config = get_external_server_config('cassandra', settings)
    if config:
        connector = create_external_connector('cassandra', config)
"""

import os
from typing import Dict, Optional


def get_external_server_config(plugin_name: str, settings: dict) -> Optional[Dict]:
    """
    Get external server configuration for a plugin.
    
    Priority order:
    1. Environment variables (highest priority)
    2. settings.yaml external_servers config
    3. None (use container)
    
    Args:
        plugin_name: Name of plugin (e.g., 'cassandra', 'postgres')
        settings: Full settings dictionary from settings.yaml
    
    Returns:
        dict or None: External server config if configured, else None
    """
    plugin_upper = plugin_name.upper()
    
    # Check environment variables first
    env_host = os.environ.get(f'{plugin_upper}_TEST_HOST')
    
    if env_host:
        # Environment variables take precedence
        config = _get_config_from_env(plugin_name, plugin_upper)
        if config:
            print(f"  [DEBUG] External config from env: {config.keys()}")
            if 'ssh' in config:
                print(f"  [DEBUG] SSH config present in env config: {config['ssh'].keys()}")
            return config
    
    # Check settings file
    external_servers = settings.get('integration_tests', {}).get('external_servers', {})
    plugin_config = external_servers.get(plugin_name, {})
    
    if plugin_config.get('enabled', False):
        # Config file has external server enabled
        config = _get_config_from_settings(plugin_name, plugin_config)
        if config:
            print(f"  [DEBUG] External config from file: {config.keys()}")
            if 'ssh' in config:
                print(f"  [DEBUG] SSH config present in file config: {config['ssh'].keys()}")
            return config
    
    # No external server configured
    return None


def _get_config_from_env(plugin_name: str, plugin_upper: str) -> Optional[Dict]:
    """
    Build configuration from environment variables.
    
    Args:
        plugin_name: Plugin name (lowercase)
        plugin_upper: Plugin name (uppercase for env vars)
    
    Returns:
        dict or None: Configuration dict or None if incomplete
    """
    host = os.environ.get(f'{plugin_upper}_TEST_HOST')
    
    if not host:
        return None
    
    config = {
        'source': 'environment',
        'host': host,
        'port': int(os.environ.get(f'{plugin_upper}_TEST_PORT', _get_default_port(plugin_name))),
    }
    
    # Common settings
    user = os.environ.get(f'{plugin_upper}_TEST_USER')
    password = os.environ.get(f'{plugin_upper}_TEST_PASSWORD')
    
    if user:
        config['user'] = user
    if password:
        config['password'] = password
    
    # Plugin-specific settings
    if plugin_name == 'cassandra':
        config['keyspace'] = os.environ.get(f'{plugin_upper}_TEST_KEYSPACE', 'test_keyspace')
        config['datacenter'] = os.environ.get(f'{plugin_upper}_TEST_DATACENTER', 'datacenter1')
        
        # SSH configuration
        ssh_host = os.environ.get(f'{plugin_upper}_SSH_HOST')
        print(f"  [DEBUG] Checking for {plugin_upper}_SSH_HOST: {ssh_host}")
        if ssh_host:
            ssh_config = {
                'host': ssh_host,
                'user': os.environ.get(f'{plugin_upper}_SSH_USER', 'ubuntu'),
                'key_file': os.environ.get(f'{plugin_upper}_SSH_KEY'),
                'password': os.environ.get(f'{plugin_upper}_SSH_PASSWORD'),
                'timeout': int(os.environ.get(f'{plugin_upper}_SSH_TIMEOUT', 10))
            }
            print(f"  [DEBUG] Built SSH config: {ssh_config}")
            config['ssh'] = ssh_config
        else:
            print(f"  [DEBUG] No SSH_HOST found in environment")
    
    elif plugin_name == 'postgres':
        config['database'] = os.environ.get(f'{plugin_upper}_TEST_DATABASE', 'postgres')
        config['sslmode'] = os.environ.get(f'{plugin_upper}_TEST_SSLMODE', 'prefer')
    
    elif plugin_name == 'mongodb':
        config['database'] = os.environ.get(f'{plugin_upper}_TEST_DATABASE', 'test')
        config['auth_source'] = os.environ.get(f'{plugin_upper}_TEST_AUTH_SOURCE', 'admin')
        config['replica_set'] = os.environ.get(f'{plugin_upper}_TEST_REPLICA_SET')
    
    elif plugin_name in ['redis', 'valkey']:
        config['db'] = int(os.environ.get(f'{plugin_upper}_TEST_DB', 0))

    elif plugin_name == 'kafka':
        config['sasl_mechanism'] = os.environ.get(f'{plugin_upper}_TEST_SASL_MECHANISM')
        config['security_protocol'] = os.environ.get(f'{plugin_upper}_TEST_SECURITY_PROTOCOL', 'PLAINTEXT')
    
    return config


def _get_config_from_settings(plugin_name: str, plugin_config: dict) -> Optional[Dict]:
    """
    Build configuration from settings.yaml.
    
    Args:
        plugin_name: Plugin name
        plugin_config: Plugin section from external_servers config
    
    Returns:
        dict or None: Configuration dict or None if incomplete
    """
    host = plugin_config.get('host')
    
    if not host:
        return None
    
    config = {
        'source': 'config_file',
        'host': host,
        'port': plugin_config.get('port', _get_default_port(plugin_name)),
    }
    
    # Copy common settings
    if 'user' in plugin_config:
        config['user'] = plugin_config['user']
    if 'password' in plugin_config:
        config['password'] = plugin_config['password']
    
    # Copy plugin-specific settings
    if plugin_name == 'cassandra':
        config['keyspace'] = plugin_config.get('keyspace', 'test_keyspace')
        config['datacenter'] = plugin_config.get('datacenter', 'datacenter1')
        
        # SSH configuration
        print(f"  [DEBUG] Checking for SSH in plugin_config: {'ssh' in plugin_config}")
        if 'ssh' in plugin_config:
            ssh_config = plugin_config['ssh']
            print(f"  [DEBUG] SSH config from file: {ssh_config}")
            config['ssh'] = {
                'host': ssh_config.get('host'),
                'user': ssh_config.get('user', 'ubuntu'),
                'key_file': ssh_config.get('key_file'),
                'password': ssh_config.get('password'),
                'timeout': ssh_config.get('timeout', 10)
            }
            print(f"  [DEBUG] Built SSH config from file: {config['ssh']}")
        else:
            print(f"  [DEBUG] No SSH section in plugin_config")
    
    elif plugin_name == 'postgres':
        config['database'] = plugin_config.get('database', 'postgres')
        config['sslmode'] = plugin_config.get('sslmode', 'prefer')
    
    elif plugin_name == 'mongodb':
        config['database'] = plugin_config.get('database', 'test')
        config['auth_source'] = plugin_config.get('auth_source', 'admin')
        if 'replica_set' in plugin_config:
            config['replica_set'] = plugin_config['replica_set']
    
    elif plugin_name in ['redis', 'valkey']:
        config['db'] = plugin_config.get('db', 0)

    elif plugin_name == 'kafka':
        if 'sasl_mechanism' in plugin_config:
            config['sasl_mechanism'] = plugin_config['sasl_mechanism']
        config['security_protocol'] = plugin_config.get('security_protocol', 'PLAINTEXT')
    
    return config


def _get_default_port(plugin_name: str) -> int:
    """Get default port for a database technology."""
    defaults = {
        'cassandra': 9042,
        'postgres': 5432,
        'mongodb': 27017,
        'redis': 6379,
        'valkey': 6379,
        'kafka': 9092,
        'mysql': 3306,
        'mariadb': 3306,
    }
    return defaults.get(plugin_name, 0)


def create_external_connector(plugin_name: str, external_config: dict):
    """
    Create a connector to an external server.
    
    Args:
        plugin_name: Name of plugin
        external_config: Configuration from get_external_server_config()
    
    Returns:
        Connected database connector instance
    """
    if plugin_name == 'cassandra':
        from plugins.cassandra.connector import CassandraConnector
        
        settings = {
            'hosts': [external_config['host']],
            'port': external_config['port'],
            'keyspace': external_config.get('keyspace', 'test_keyspace'),
        }
        
        if 'user' in external_config:
            settings['user'] = external_config['user']
        if 'password' in external_config:
            settings['password'] = external_config['password']
        if 'datacenter' in external_config:
            settings['datacenter'] = external_config['datacenter']
        
        # Add SSH config if present
        print(f"  [DEBUG] Creating connector, external_config has SSH: {'ssh' in external_config}")
        if 'ssh' in external_config:
            ssh = external_config['ssh']
            print(f"  [DEBUG] SSH config to add to connector: {ssh}")
            settings['ssh_host'] = ssh.get('host')
            settings['ssh_user'] = ssh.get('user')
            settings['ssh_key_file'] = ssh.get('key_file')
            settings['ssh_password'] = ssh.get('password')
            settings['ssh_timeout'] = ssh.get('timeout', 10)
            print(f"  [DEBUG] Connector settings with SSH: {list(settings.keys())}")
        else:
            print(f"  [DEBUG] No SSH config in external_config")
        
        connector = CassandraConnector(settings)
        connector.connect()
        return connector
    
    elif plugin_name == 'postgres':
        from plugins.postgres.connector import PostgreSQLConnector
        
        settings = {
            'host': external_config['host'],
            'port': external_config['port'],
            'database': external_config.get('database', 'postgres'),
        }
        
        if 'user' in external_config:
            settings['user'] = external_config['user']
        if 'password' in external_config:
            settings['password'] = external_config['password']
        if 'sslmode' in external_config:
            settings['sslmode'] = external_config['sslmode']
        
        connector = PostgreSQLConnector(settings)
        connector.connect()
        return connector
    
    elif plugin_name == 'mongodb':
        from plugins.mongodb.connector import MongoDBConnector
        
        settings = {
            'host': external_config['host'],
            'port': external_config['port'],
            'database': external_config.get('database', 'test'),
        }
        
        if 'user' in external_config:
            settings['user'] = external_config['user']
        if 'password' in external_config:
            settings['password'] = external_config['password']
        if 'auth_source' in external_config:
            settings['auth_source'] = external_config['auth_source']
        if 'replica_set' in external_config:
            settings['replica_set'] = external_config['replica_set']
        
        connector = MongoDBConnector(settings)
        connector.connect()
        return connector
    
    elif plugin_name in ['redis', 'valkey']:
        from plugins.valkey.connector import ValkeyConnector
        
        settings = {
            'host': external_config['host'],
            'port': external_config['port'],
            'db': external_config.get('db', 0),
        }
        
        if 'password' in external_config:
            settings['password'] = external_config['password']
        
        connector = ValkeyConnector(settings)
        connector.connect()
        return connector

    elif plugin_name == 'kafka':
        from plugins.kafka.connector import KafkaConnector
        
        # Kafka expects bootstrap_servers as a list or comma-separated string
        bootstrap_servers = f"{external_config['host']}:{external_config['port']}"
        
        settings = {
            'bootstrap_servers': bootstrap_servers,
            'client_id': 'pg_healthcheck_test'
        }
        
        # Optional authentication settings if provided
        if 'user' in external_config:
            settings['sasl_plain_username'] = external_config['user']
        if 'password' in external_config:
            settings['sasl_plain_password'] = external_config['password']
        if external_config.get('sasl_mechanism'):
            settings['sasl_mechanism'] = external_config['sasl_mechanism']
        if external_config.get('security_protocol'):
            settings['security_protocol'] = external_config['security_protocol']
        
        connector = KafkaConnector(settings)
        connector.connect()
        return connector
        
    else:
        raise NotImplementedError(f"External server support not implemented for {plugin_name}")


def print_external_server_info(plugin_name: str, external_config: dict):
    """Print info about using external server for testing."""
    source = external_config.get('source', 'unknown')
    host = external_config.get('host', 'unknown')
    port = external_config.get('port', 'unknown')
    
    print(f"  [INFO] Using external {plugin_name} server for integration tests")
    print(f"         Source: {source}")
    print(f"         Host: {host}:{port}")
    
    if 'ssh' in external_config:
        ssh = external_config['ssh']
        if ssh.get('host'):
            print(f"         SSH: {ssh.get('user')}@{ssh['host']} (configured)")

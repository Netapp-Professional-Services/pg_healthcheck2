"""
PgBouncer Admin Console Client

Provides interface to PgBouncer admin database for monitoring and statistics.

PgBouncer exposes a special "pgbouncer" database that allows querying:
- Configuration parameters
- Pool statistics
- Connection information
- Performance metrics

Usage:
    client = PgBouncerClient(host='localhost', port=6432, user='postgres', password='secret')
    version = client.show_version()
    pools = client.show_pools()
    client.close()
"""

import logging
import psycopg2
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class PgBouncerClient:
    """Client for PgBouncer admin console."""

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str = 'pgbouncer', timeout: int = 5):
        """
        Initialize PgBouncer admin client.

        Args:
            host: PgBouncer host
            port: PgBouncer port (typically 6432)
            user: Admin username
            password: Admin password
            database: Admin database name (typically 'pgbouncer')
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.conn = None
        self.cursor = None

    def connect(self) -> Tuple[bool, Dict]:
        """
        Connect to PgBouncer admin console.

        Returns:
            Tuple of (success: bool, result: dict)
        """
        try:
            # PgBouncer doesn't support startup parameters in options
            # Explicitly pass empty options to override PGOPTIONS environment variable
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=self.timeout,
                options=""  # Override PGOPTIONS env var
            )
            # PgBouncer admin console doesn't support transactions
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            return True, {'message': 'Connected successfully'}
        except psycopg2.Error as e:
            logger.debug(f"Could not connect to PgBouncer admin console: {e}")
            return False, {'error': str(e)}
        except Exception as e:
            logger.debug(f"Unexpected error connecting to PgBouncer: {e}")
            return False, {'error': str(e)}

    def close(self):
        """Close connection to PgBouncer."""
        if self.cursor:
            try:
                self.cursor.close()
            except:
                pass
        if self.conn:
            try:
                self.conn.close()
            except:
                pass

    def _execute_show_command(self, command: str) -> Tuple[bool, Dict]:
        """
        Execute a SHOW command and return results.

        Args:
            command: SHOW command to execute (e.g., 'SHOW VERSION')

        Returns:
            Tuple of (success: bool, result: dict)
        """
        if not self.conn or not self.cursor:
            success, result = self.connect()
            if not success:
                return False, result

        try:
            self.cursor.execute(command)
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()

            # Convert to list of dicts
            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))

            return True, {'data': data, 'columns': columns}

        except psycopg2.Error as e:
            logger.debug(f"Error executing {command}: {e}")
            return False, {'error': str(e)}
        except Exception as e:
            logger.debug(f"Unexpected error executing {command}: {e}")
            return False, {'error': str(e)}

    def show_version(self) -> Tuple[bool, Dict]:
        """
        Get PgBouncer version.

        Returns:
            Tuple of (success: bool, result: dict with 'version' key)
        """
        success, result = self._execute_show_command('SHOW VERSION')
        if success and result.get('data'):
            # Extract version string
            version_row = result['data'][0]
            version = version_row.get('version', 'Unknown')
            return True, {'version': version, 'raw': result['data']}
        return success, result

    def show_config(self) -> Tuple[bool, Dict]:
        """
        Get PgBouncer configuration parameters.

        Returns:
            Tuple of (success: bool, result: dict with 'data' key containing config params)
        """
        success, result = self._execute_show_command('SHOW CONFIG')
        if success and result.get('data'):
            # Convert to dict for easier access
            config_dict = {}
            for row in result['data']:
                key = row.get('key')
                value = row.get('value')
                if key:
                    config_dict[key] = value
            return True, {'config': config_dict, 'raw': result['data']}
        return success, result

    def show_stats(self) -> Tuple[bool, Dict]:
        """
        Get per-database statistics.

        Returns statistics including:
        - total_xact_count: Total transactions
        - total_query_count: Total queries
        - total_received: Bytes received from clients
        - total_sent: Bytes sent to clients
        - avg_xact_time: Average transaction time (microseconds)
        - avg_query_time: Average query time (microseconds)

        Returns:
            Tuple of (success: bool, result: dict with 'data' key)
        """
        return self._execute_show_command('SHOW STATS')

    def show_pools(self) -> Tuple[bool, Dict]:
        """
        Get pool status for all databases.

        Critical metrics:
        - cl_active: Client connections in active transaction
        - cl_waiting: Clients WAITING for server connection (CRITICAL metric!)
        - sv_active: Server connections in use
        - sv_idle: Server connections idle
        - maxwait: Maximum wait time in seconds

        Returns:
            Tuple of (success: bool, result: dict with 'data' key)
        """
        return self._execute_show_command('SHOW POOLS')

    def show_clients(self) -> Tuple[bool, Dict]:
        """
        Get all client connections.

        Shows detailed information about each client connection including:
        - user, database, state
        - connect_time, request_time
        - wait time

        Returns:
            Tuple of (success: bool, result: dict with 'data' key)
        """
        return self._execute_show_command('SHOW CLIENTS')

    def show_servers(self) -> Tuple[bool, Dict]:
        """
        Get all backend server connections.

        Shows connections from PgBouncer to PostgreSQL backends.

        Returns:
            Tuple of (success: bool, result: dict with 'data' key)
        """
        return self._execute_show_command('SHOW SERVERS')

    def show_lists(self) -> Tuple[bool, Dict]:
        """
        Get internal PgBouncer object lists.

        Shows counts of:
        - free_clients, free_servers
        - used_clients, used_servers
        - login_clients, login_servers

        Returns:
            Tuple of (success: bool, result: dict with 'data' key)
        """
        return self._execute_show_command('SHOW LISTS')

    def show_databases(self) -> Tuple[bool, Dict]:
        """
        Get configured databases.

        Shows database configuration including:
        - database name
        - host, port
        - pool_size, pool_mode

        Returns:
            Tuple of (success: bool, result: dict with 'data' key)
        """
        return self._execute_show_command('SHOW DATABASES')

    def show_users(self) -> Tuple[bool, Dict]:
        """
        Get configured users.

        Returns:
            Tuple of (success: bool, result: dict with 'data' key)
        """
        return self._execute_show_command('SHOW USERS')

    def get_comprehensive_status(self) -> Dict:
        """
        Get comprehensive PgBouncer status.

        Collects all available statistics in a single call.

        Returns:
            Dictionary with all status information
        """
        status = {
            'version': None,
            'config': None,
            'stats': None,
            'pools': None,
            'databases': None,
            'clients_count': 0,
            'servers_count': 0,
            'lists': None,
            'errors': []
        }

        # Version
        success, result = self.show_version()
        if success:
            status['version'] = result.get('version')
        else:
            status['errors'].append(f"show_version: {result.get('error')}")

        # Config
        success, result = self.show_config()
        if success:
            status['config'] = result.get('config', {})
        else:
            status['errors'].append(f"show_config: {result.get('error')}")

        # Stats
        success, result = self.show_stats()
        if success:
            status['stats'] = result.get('data', [])
        else:
            status['errors'].append(f"show_stats: {result.get('error')}")

        # Pools (CRITICAL)
        success, result = self.show_pools()
        if success:
            status['pools'] = result.get('data', [])
        else:
            status['errors'].append(f"show_pools: {result.get('error')}")

        # Databases
        success, result = self.show_databases()
        if success:
            status['databases'] = result.get('data', [])
        else:
            status['errors'].append(f"show_databases: {result.get('error')}")

        # Clients count
        success, result = self.show_clients()
        if success:
            status['clients_count'] = len(result.get('data', []))
        else:
            status['errors'].append(f"show_clients: {result.get('error')}")

        # Servers count
        success, result = self.show_servers()
        if success:
            status['servers_count'] = len(result.get('data', []))
        else:
            status['errors'].append(f"show_servers: {result.get('error')}")

        # Lists
        success, result = self.show_lists()
        if success:
            status['lists'] = result.get('data', [])
        else:
            status['errors'].append(f"show_lists: {result.get('error')}")

        return status


def test_pgbouncer_connection(host: str, port: int, user: str, password: str,
                               timeout: int = 5) -> Tuple[bool, Dict]:
    """
    Test connection to PgBouncer admin console.

    This is a convenience function for quick connection testing.

    Args:
        host: PgBouncer host
        port: PgBouncer port
        user: Username
        password: Password
        timeout: Connection timeout

    Returns:
        Tuple of (success: bool, result: dict)
    """
    client = PgBouncerClient(host, port, user, password, timeout=timeout)

    try:
        success, result = client.connect()
        if not success:
            return False, result

        # Try to get version to confirm it's PgBouncer
        success, version_result = client.show_version()
        if success:
            version = version_result.get('version', '')
            if 'PgBouncer' in str(version) or 'pgbouncer' in str(version).lower():
                return True, {
                    'detected': True,
                    'version': version,
                    'host': host,
                    'port': port
                }
            else:
                return False, {
                    'detected': False,
                    'error': 'Connected but not PgBouncer (version check failed)',
                    'version': version
                }
        else:
            return False, {
                'detected': False,
                'error': 'Could not verify PgBouncer version',
                'details': version_result.get('error')
            }

    finally:
        client.close()

"""
Centralized keyspace filtering utility for Cassandra health checks.

This module provides consistent filtering of system and user-excluded keyspaces
across all Cassandra health check modules.
"""

# Standard system keyspaces in Cassandra
# These are managed internally by Cassandra and should typically be excluded from health checks
DEFAULT_SYSTEM_KEYSPACES = {
    'system',                    # Core system tables
    'system_schema',             # Schema metadata (Cassandra 3.0+)
    'system_auth',               # Authentication and authorization
    'system_distributed',        # Distributed system tables
    'system_traces',             # Request tracing data
    'system_views',              # System views (Cassandra 3.0+)
    'system_virtual_schema',     # Virtual schema (Cassandra 4.0+)
}

# Legacy keyspaces that may appear in older Cassandra versions
LEGACY_SYSTEM_KEYSPACES = {
    'system_auth',
    'system_traces',
}


class KeyspaceFilter:
    """
    Filters keyspaces based on system keyspaces and user-defined exclusions.

    Usage:
        # Basic usage with defaults
        filter = KeyspaceFilter()
        user_keyspaces = filter.filter_keyspaces(all_keyspaces)

        # With custom settings from config
        filter = KeyspaceFilter(settings)
        user_keyspaces = filter.filter_keyspaces(all_keyspaces)

        # Filter with additional custom exclusions
        filter = KeyspaceFilter(additional_exclusions=['test_ks', 'tmp_ks'])
        user_keyspaces = filter.filter_keyspaces(all_keyspaces)
    """

    def __init__(self, settings=None, additional_exclusions=None):
        """
        Initialize the keyspace filter.

        Args:
            settings (dict, optional): Health check settings from config.yaml
                Can include:
                - exclude_system_keyspaces (bool): Whether to exclude system keyspaces (default: True)
                - custom_excluded_keyspaces (list): Additional keyspaces to exclude
                - include_system_auth (bool): Whether to include system_auth in results (default: False)
            additional_exclusions (list, optional): Additional keyspace names to exclude
        """
        self.settings = settings or {}

        # Determine if system keyspaces should be excluded
        self.exclude_system = self.settings.get('exclude_system_keyspaces', True)

        # Build the exclusion set
        self.excluded_keyspaces = set()

        if self.exclude_system:
            self.excluded_keyspaces.update(DEFAULT_SYSTEM_KEYSPACES)

        # Handle special case: system_auth may be needed for specific checks
        if self.settings.get('include_system_auth', False):
            self.excluded_keyspaces.discard('system_auth')

        # Add custom exclusions from config
        custom_exclusions = self.settings.get('custom_excluded_keyspaces', [])
        if custom_exclusions:
            self.excluded_keyspaces.update(custom_exclusions)

        # Add programmatic exclusions
        if additional_exclusions:
            self.excluded_keyspaces.update(additional_exclusions)

    def filter_keyspaces(self, keyspaces, keyspace_field='keyspace_name'):
        """
        Filter a list of keyspace dictionaries, removing system and excluded keyspaces.

        Args:
            keyspaces (list): List of dictionaries containing keyspace information
            keyspace_field (str): Name of the field containing the keyspace name (default: 'keyspace_name')

        Returns:
            list: Filtered list containing only user keyspaces

        Example:
            >>> raw_keyspaces = [
            ...     {'keyspace_name': 'system', 'replication': {...}},
            ...     {'keyspace_name': 'my_app', 'replication': {...}},
            ...     {'keyspace_name': 'system_auth', 'replication': {...}}
            ... ]
            >>> filter = KeyspaceFilter()
            >>> user_keyspaces = filter.filter_keyspaces(raw_keyspaces)
            >>> len(user_keyspaces)
            1
            >>> user_keyspaces[0]['keyspace_name']
            'my_app'
        """
        if not keyspaces:
            return []

        return [
            ks for ks in keyspaces
            if ks.get(keyspace_field) not in self.excluded_keyspaces
        ]

    def is_system_keyspace(self, keyspace_name):
        """
        Check if a keyspace name is a system keyspace.

        Args:
            keyspace_name (str): The keyspace name to check

        Returns:
            bool: True if the keyspace is a system keyspace
        """
        return keyspace_name in DEFAULT_SYSTEM_KEYSPACES

    def is_excluded(self, keyspace_name):
        """
        Check if a keyspace should be excluded from health checks.

        Args:
            keyspace_name (str): The keyspace name to check

        Returns:
            bool: True if the keyspace should be excluded
        """
        return keyspace_name in self.excluded_keyspaces

    def get_excluded_keyspaces(self):
        """
        Get the complete set of excluded keyspace names.

        Returns:
            set: Set of keyspace names that will be excluded
        """
        return self.excluded_keyspaces.copy()

    def get_system_keyspaces(self):
        """
        Get the set of default system keyspace names.

        Returns:
            set: Set of system keyspace names
        """
        return DEFAULT_SYSTEM_KEYSPACES.copy()


def filter_user_keyspaces(keyspaces, settings=None, keyspace_field='keyspace_name'):
    """
    Convenience function to filter keyspaces using default settings.

    This is a simplified interface for the most common use case.

    Args:
        keyspaces (list): List of keyspace dictionaries
        settings (dict, optional): Settings from config.yaml
        keyspace_field (str): Field name containing keyspace name

    Returns:
        list: Filtered list of user keyspaces

    Example:
        >>> from plugins.cassandra.utils.keyspace_filter import filter_user_keyspaces
        >>>
        >>> raw_keyspaces = connector.execute_query("SELECT * FROM system_schema.keyspaces")
        >>> user_keyspaces = filter_user_keyspaces(raw_keyspaces, settings)
    """
    filter_obj = KeyspaceFilter(settings)
    return filter_obj.filter_keyspaces(keyspaces, keyspace_field)


def filter_tables_by_keyspace(tables, settings=None, keyspace_field='keyspace_name'):
    """
    Filter tables by removing those in system/excluded keyspaces.

    This is commonly used for checks that query system_schema.tables.

    Args:
        tables (list): List of table dictionaries
        settings (dict, optional): Settings from config.yaml
        keyspace_field (str): Field name containing keyspace name

    Returns:
        list: Filtered list of tables in user keyspaces

    Example:
        >>> from plugins.cassandra.utils.keyspace_filter import filter_tables_by_keyspace
        >>>
        >>> all_tables = connector.execute_query("SELECT * FROM system_schema.tables")
        >>> user_tables = filter_tables_by_keyspace(all_tables, settings)
    """
    filter_obj = KeyspaceFilter(settings)
    return filter_obj.filter_keyspaces(tables, keyspace_field)

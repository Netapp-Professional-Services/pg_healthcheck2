"""
Centralized keyspace filtering utility for Cassandra health checks.

This module provides consistent filtering of system and user-excluded keyspaces
across all Cassandra health check modules.
"""

import logging

logger = logging.getLogger(__name__)

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

        # Check for common config key mistakes and warn
        self._check_config_keys()

        # Determine if system keyspaces should be excluded
        self.exclude_system = self.settings.get('exclude_system_keyspaces', True)

        # Build the exclusion set
        self.excluded_keyspaces = set()

        if self.exclude_system:
            self.excluded_keyspaces.update(DEFAULT_SYSTEM_KEYSPACES)
            logger.debug(f"Excluding system keyspaces: {DEFAULT_SYSTEM_KEYSPACES}")

        # Handle special case: system_auth may be needed for specific checks
        if self.settings.get('include_system_auth', False):
            self.excluded_keyspaces.discard('system_auth')
            logger.debug("Including system_auth keyspace (include_system_auth=True)")

        # Add custom exclusions from config (with type validation)
        custom_exclusions = self._get_custom_exclusions()
        if custom_exclusions:
            self.excluded_keyspaces.update(custom_exclusions)
            logger.debug(f"Adding custom excluded keyspaces: {custom_exclusions}")

        # Add programmatic exclusions
        if additional_exclusions:
            validated_exclusions = self._validate_exclusion_list(additional_exclusions, "additional_exclusions")
            self.excluded_keyspaces.update(validated_exclusions)
            logger.debug(f"Adding programmatic exclusions: {validated_exclusions}")

        logger.debug(f"Total excluded keyspaces: {self.excluded_keyspaces}")

    def _check_config_keys(self):
        """Check for common config key mistakes and log warnings."""
        # Common misspellings or variations users might try
        key_warnings = {
            'excluded_keyspaces': 'custom_excluded_keyspaces',
            'exclude_keyspaces': 'custom_excluded_keyspaces',
            'keyspace_exclusions': 'custom_excluded_keyspaces',
            'excluded_system_keyspaces': 'exclude_system_keyspaces',
            'filter_system_keyspaces': 'exclude_system_keyspaces',
            'system_keyspace_filter': 'exclude_system_keyspaces',
        }

        for wrong_key, correct_key in key_warnings.items():
            if wrong_key in self.settings:
                logger.warning(
                    f"Config key '{wrong_key}' is not recognized. "
                    f"Did you mean '{correct_key}'? "
                    f"The value will be ignored."
                )

    def _get_custom_exclusions(self):
        """
        Get custom exclusions from config with proper type validation.

        Returns:
            set: Set of keyspace names to exclude

        Raises:
            Logs warning if config value is invalid type
        """
        custom_exclusions = self.settings.get('custom_excluded_keyspaces', [])

        if not custom_exclusions:
            return set()

        return self._validate_exclusion_list(custom_exclusions, "custom_excluded_keyspaces")

    def _validate_exclusion_list(self, exclusions, config_key):
        """
        Validate and normalize an exclusion list.

        Args:
            exclusions: Value to validate (should be list or set)
            config_key: Name of the config key (for error messages)

        Returns:
            set: Validated set of keyspace names
        """
        # Handle string input (common user mistake)
        if isinstance(exclusions, str):
            logger.warning(
                f"Config '{config_key}' should be a list, not a string. "
                f"Got: '{exclusions}'. Converting to single-item list. "
                f"Correct format:\n"
                f"  {config_key}:\n"
                f"    - \"{exclusions}\""
            )
            return {exclusions}

        # Handle list or set
        if isinstance(exclusions, (list, set, tuple)):
            result = set()
            for item in exclusions:
                if isinstance(item, str):
                    result.add(item)
                else:
                    logger.warning(
                        f"Invalid item in '{config_key}': {item} (type: {type(item).__name__}). "
                        f"Expected string. Skipping."
                    )
            return result

        # Handle other invalid types
        logger.warning(
            f"Config '{config_key}' has invalid type: {type(exclusions).__name__}. "
            f"Expected list of strings. Ignoring value."
        )
        return set()

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
            logger.debug("No keyspaces provided to filter")
            return []

        # Track what gets filtered for debugging
        filtered_out = []
        result = []

        for ks in keyspaces:
            ks_name = ks.get(keyspace_field)
            if ks_name in self.excluded_keyspaces:
                filtered_out.append(ks_name)
            else:
                result.append(ks)

        if filtered_out:
            logger.debug(f"Filtered out {len(filtered_out)} keyspaces: {filtered_out}")

        logger.debug(f"Returning {len(result)} user keyspaces out of {len(keyspaces)} total")

        return result

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

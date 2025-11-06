"""
ClickHouse Configuration Monitoring Query Library

Centralized SQL query definitions for configuration analysis and drift detection.

Queries:
- get_changed_server_settings_query(): Settings that differ from defaults
- get_all_server_settings_query(): All server-level settings
- get_merge_tree_settings_query(): MergeTree-specific settings
- get_security_settings_query(): Security-related settings
- get_resource_settings_query(): Resource limit settings
- get_build_info_query(): Build and version information
"""


def get_changed_server_settings_query(connector):
    """
    Returns query for server settings that differ from defaults.

    Critical for configuration drift detection and security audits.

    Note: ClickHouse 25.x removed the 'readonly' column from system.server_settings.

    Returns:
        str: SQL query for changed settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description,
        type
    FROM system.server_settings
    WHERE changed = 1
    ORDER BY name
    """


def get_all_server_settings_query(connector):
    """
    Returns query for all server settings.

    Note: ClickHouse 25.x removed the 'readonly' column from system.server_settings.

    Args:
        connector: ClickHouse connector instance

    Returns:
        str: SQL query for all server settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description,
        type
    FROM system.server_settings
    ORDER BY name
    """


def get_critical_settings_query(connector):
    """
    Returns query for critical performance and security settings.

    Focuses on settings that have significant impact on performance,
    security, or stability.

    Returns:
        str: SQL query for critical settings
    """
    critical_settings = [
        'max_concurrent_queries',
        'max_connections',
        'max_memory_usage',
        'max_server_memory_usage',
        'max_table_size_to_drop',
        'max_partition_size_to_drop',
        'background_pool_size',
        'background_schedule_pool_size',
        'background_merges_mutations_concurrency_ratio',
        'max_replicated_merges_in_queue',
        'listen_host',
        'tcp_port',
        'http_port',
        'interserver_http_port',
        'keep_alive_timeout',
        'users_config',
        'config_file',
        'path',
        'tmp_path',
        'user_files_path',
        'format_schema_path'
    ]

    settings_list = "', '".join(critical_settings)

    return f"""
    SELECT
        name,
        value,
        default,
        changed,
        description,
        type
    FROM system.server_settings
    WHERE name IN ('{settings_list}')
    ORDER BY name
    """


def get_merge_tree_settings_query(connector):
    """
    Returns query for MergeTree-specific settings.

    These settings control merge behavior, part management,
    and storage efficiency.

    Returns:
        str: SQL query for MergeTree settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description
    FROM system.server_settings
    WHERE name LIKE '%merge%'
       OR name LIKE '%part%'
       OR name LIKE '%compact%'
    ORDER BY name
    """


def get_security_settings_query(connector):
    """
    Returns query for security-related settings.

    Includes authentication, authorization, and network security settings.

    Returns:
        str: SQL query for security settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description
    FROM system.server_settings
    WHERE name LIKE '%password%'
       OR name LIKE '%auth%'
       OR name LIKE '%ssl%'
       OR name LIKE '%secure%'
       OR name LIKE '%access%'
       OR name LIKE '%listen%'
       OR name LIKE '%port%'
    ORDER BY name
    """


def get_resource_limit_settings_query(connector):
    """
    Returns query for resource limit settings.

    Includes memory, CPU, connection, and query limits.

    Returns:
        str: SQL query for resource settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description
    FROM system.server_settings
    WHERE name LIKE '%max%'
       OR name LIKE '%limit%'
       OR name LIKE '%memory%'
       OR name LIKE '%thread%'
       OR name LIKE '%pool%'
    ORDER BY name
    """


def get_build_info_query(connector):
    """
    Returns query for ClickHouse build and version information.

    Returns:
        str: SQL query for build information
    """
    return """
    SELECT
        name,
        value
    FROM system.build_options
    ORDER BY name
    """


def get_version_query(connector):
    """
    Returns query for ClickHouse version information.

    Returns:
        str: SQL query for version
    """
    return """
    SELECT version() as version
    """


def get_settings_by_category_query(connector, category):
    """
    Returns query for settings filtered by category/pattern.

    Args:
        connector: ClickHouse connector instance
        category: Category pattern (e.g., 'memory', 'query', 'merge')

    Returns:
        str: SQL query for category settings
    """
    return f"""
    SELECT
        name,
        value,
        default,
        changed,
        description,
        type
    FROM system.server_settings
    WHERE lower(name) LIKE '%{category.lower()}%'
       OR lower(description) LIKE '%{category.lower()}%'
    ORDER BY changed DESC, name
    """


def get_recommended_settings_check_query(connector):
    """
    Returns query to check if recommended production settings are configured.

    This is specific to production best practices.

    Returns:
        str: SQL query for recommended settings check
    """
    return """
    SELECT
        name,
        value,
        default,
        changed
    FROM system.server_settings
    WHERE name IN (
        'max_concurrent_queries',
        'max_connections',
        'max_server_memory_usage',
        'background_pool_size',
        'background_schedule_pool_size',
        'mark_cache_size',
        'uncompressed_cache_size',
        'max_table_size_to_drop',
        'max_partition_size_to_drop'
    )
    ORDER BY name
    """


def get_deprecated_settings_query(connector):
    """
    Returns query for deprecated or obsolete settings.

    Note: This requires knowledge of deprecated settings per version.
    For now, it returns settings with 'obsolete' in the description.

    Returns:
        str: SQL query for deprecated settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description
    FROM system.server_settings
    WHERE lower(description) LIKE '%obsolete%'
       OR lower(description) LIKE '%deprecated%'
       OR lower(description) LIKE '%removed%'
    ORDER BY name
    """


def get_network_settings_query(connector):
    """
    Returns query for network-related settings.

    Includes ports, timeouts, and connection settings.

    Returns:
        str: SQL query for network settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description
    FROM system.server_settings
    WHERE name LIKE '%port%'
       OR name LIKE '%timeout%'
       OR name LIKE '%connection%'
       OR name LIKE '%tcp%'
       OR name LIKE '%http%'
       OR name LIKE '%listen%'
       OR name LIKE '%host%'
    ORDER BY name
    """


def get_logging_settings_query(connector):
    """
    Returns query for logging and monitoring settings.

    Returns:
        str: SQL query for logging settings
    """
    return """
    SELECT
        name,
        value,
        default,
        changed,
        description
    FROM system.server_settings
    WHERE name LIKE '%log%'
       OR name LIKE '%trace%'
       OR name LIKE '%metric%'
       OR name LIKE '%monitor%'
    ORDER BY name
    """

"""Defines the default report structure for PostgreSQL health checks.

This module contains the primary configuration for the standard report,
detailing the sections and the specific check modules to be run in order.

Attributes:
    REPORT_SECTIONS (list): A list of dictionaries defining the report
        structure. Each dictionary represents a section with a title and
        a list of actions (modules to run).
"""

REPORT_SECTIONS = [
    # --- Section 1: Header ---
    {
        'title': '', # No title for the header section
        'actions': [
            {'type': 'header', 'file': 'report_header.txt'},
        ]
    },

    # --- Section 2: System Overview ---
    {
        'title': 'System Overview',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.postgres_overview', 'function': 'run_postgres_overview'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_pgbouncer_detection', 'function': 'check_pgbouncer_detection'},
            {'type': 'module', 'module': 'plugins.postgres.checks.table_object_counts', 'function': 'run_table_object_counts'},
            {'type': 'module', 'module': 'plugins.postgres.checks.database_object_inventory', 'function': 'run_database_object_inventory_query'},
            {'type': 'module', 'module': 'plugins.postgres.checks.extensions_update_check', 'function': 'run_extensions_update_check'},
            {'type': 'module', 'module': 'plugins.postgres.checks.transaction_wraparound', 'function': 'run_transaction_wraparound'},
        ]
    },

    {
        'title': 'Database Structure',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.table_count', 'function': 'run_table_count'},
        ]
    },

    # --- Section 3: Cloud-Specific Metrics (AWS) ---
    {
        'title': 'Cloud-Specific Metrics (AWS)',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.aws_cloudwatch_metrics', 'function': 'run_aws_cloudwatch_metrics'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_aurora_version_upgrades', 'function': 'run_check_aurora_version_upgrades'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_postgres_version_upgrades', 'function': 'run_check_postgres_version_upgrades'},
            {'type': 'module', 'module': 'plugins.postgres.checks.aurora_stat_statements', 'function': 'run_aurora_stat_statements'},
        ]
    },

    # --- Section 4: Core Configuration Health ---
    {
        'title': 'Core Configuration Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.pg_stat_statements_config', 'function': 'run_pg_stat_statements_config'},
            {'type': 'module', 'module': 'plugins.postgres.checks.autovacuum_config', 'function': 'run_autovacuum_config'},
            {'type': 'module', 'module': 'plugins.postgres.checks.bgwriter', 'function': 'run_bgwriter_query'},
# Very similar to bgwriter output           {'type': 'module', 'module': 'plugins.postgres.checks.checkpoint', 'function': 'run_checkpoint_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.suggested_config_values', 'function': 'run_suggested_config_values'},
        ]
    },

    # --- Section 5: Security and Replication ---
    {
        'title': 'Security and Replication',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.security_and_encryption', 'function': 'run_security_and_encryption'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_cve_vulnerabilities', 'function': 'run'},
            {'type': 'module', 'module': 'plugins.postgres.checks.function_audit', 'function': 'run_function_audit'},
            {'type': 'module', 'module': 'plugins.postgres.checks.replication_health', 'function': 'run_replication_health'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_patroni_topology', 'function': 'check_patroni_topology'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_patroni_health_status', 'function': 'check_patroni_health_status'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_patroni_failover_history', 'function': 'check_patroni_failover_history'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_patroni_configuration', 'function': 'check_patroni_configuration'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_patroni_dcs_health', 'function': 'check_patroni_dcs_health'},
        ]
    },

    # --- Section 6: Connection Management ---
    {
        'title': 'Connection Management',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.check_connection_stability', 'function': 'check_connection_stability'},
            {'type': 'module', 'module': 'plugins.postgres.checks.connection_metrics', 'function': 'run_connection_metrics'},
            {'type': 'module', 'module': 'plugins.postgres.checks.superuser_reserved', 'function': 'run_superuser_reserved'},
        ]
    },

    # --- Section 7: Performance Deep Dive ---
    {
        'title': 'Performance Deep Dive',
        'actions': [
            # Comprehensive query analysis - strategic workload overview (weight: 7)
            {'type': 'module', 'module': 'plugins.postgres.checks.check_comprehensive_query_analysis', 'function': 'check_comprehensive_query_analysis'},

            # Query optimization opportunities - actionable recommendations with index suggestions (weight: 8)
            {'type': 'module', 'module': 'plugins.postgres.checks.check_query_optimization_opportunities', 'function': 'check_query_optimization_opportunities'},

            {'type': 'module', 'module': 'plugins.postgres.checks.deep_query_analysis', 'function': 'run_deep_query_analysis'},
#            {'type': 'module', 'module': 'plugins.postgres.checks.query_analysis', 'function': 'run_query_analysis'},
#            {'type': 'module', 'module': 'plugins.postgres.checks.top_io_queries', 'function': 'run_top_io_queries'},
#            {'type': 'module', 'module': 'plugins.postgres.checks.top_queries_by_execution_time', 'function': 'run_top_queries_by_execution_time'},
#            {'type': 'module', 'module': 'plugins.postgres.checks.top_queries_by_mean_time', 'function': 'run_top_queries_by_mean_time'},
#            {'type': 'module', 'module': 'plugins.postgres.checks.top_write_queries', 'function': 'run_top_write_queries'},
#            {'type': 'module', 'module': 'plugins.postgres.checks.hot_queries', 'function': 'run_hot_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.long_running_queries', 'function': 'run_long_running_queries'},
            {'type': 'module', 'module': 'plugins.postgres.checks.current_lock_waits', 'function': 'run_current_lock_waits'},
            {'type': 'module', 'module': 'plugins.postgres.checks.temp_files_analysis', 'function': 'run_temp_files_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.cache_analysis', 'function': 'run_cache_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.monitoring_metrics', 'function': 'run_monitoring_metrics'},
#           This module is not detecting pg_stat_statements.
#           {'type': 'module', 'module': 'plugins.postgres.checks.cpu_intensive_queries', 'function': 'run_cpu_intensive_queries'},


        ]
    },

    # --- Section 8: Table and Index Health ---
    {
        'title': 'Table and Index Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.vacuum_analysis', 'function': 'run_vacuum_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.table_health_analysis', 'function': 'run_table_health_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.index_bloat_analysis',  'function': 'run_index_bloat_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.check_cross_node_index_usage', 'function': 'run_check_cross_node_index_usage'},
            {'type': 'module', 'module': 'plugins.postgres.checks.missing_index_opportunities', 'function': 'run_missing_index_opportunities'},
            {'type': 'module', 'module': 'plugins.postgres.checks.missing_primary_keys', 'function': 'run_missing_primary_keys'},
            {'type': 'module', 'module': 'plugins.postgres.checks.foreign_key_audit', 'function': 'run_foreign_key_audit'},
            {'type': 'module', 'module': 'plugins.postgres.checks.primary_key_analysis', 'function': 'run_primary_key_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.data_checksums_analysis', 'function': 'run_data_checksums_analysis'},
            {'type': 'module', 'module': 'plugins.postgres.checks.high_insert_tables', 'function': 'run_high_insert_tables'},
        ]
    },

    # --- Section 9: Schema Health ---
    {
        'title': 'Schema Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.table_count_check', 'function': 'run_table_count_check'},
        ]
    },

    # --- Section 10: PgBouncer Health Summary (MUST BE LAST) ---
    # This section MUST run last to count all fallback events from previous pgbouncer checks
    {
        'title': 'PgBouncer Health Summary',
        'actions': [
            {'type': 'module', 'module': 'plugins.postgres.checks.check_pgbouncer_health', 'function': 'check_pgbouncer_health'},
        ]
    },
]

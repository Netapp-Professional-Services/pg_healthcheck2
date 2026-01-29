"""
Cassandra report definition for live cluster health checks.

This report includes checks that work with:
- CQL queries (direct database access)
- SSH/nodetool commands (requires SSH configuration)
- Prometheus metrics (Instaclustr managed clusters)

For offline diagnostic imports, use instacollector.py instead.
"""

REPORT_SECTIONS = [
    {
        "title": "Cluster Status",
        "actions": [
            # Cluster overview from nodetool status + info (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_cluster_overview', 'function': 'run_check_cluster_overview'},

            # Gossip state analysis from nodetool gossipinfo (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_gossip_state', 'function': 'run_check_gossip_state'},

            # Schema consistency from nodetool describecluster
            {"type": "module", "module": "plugins.cassandra.checks.schema_version_consistency_check", "function": "run_schema_version_consistency_check"},

            # Token ring analysis from nodetool ring (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_ring_analysis', 'function': 'run_check_ring_analysis'},

            # Comprehensive cluster connectivity & gossip diagnostics
            {'type': 'module', 'module': 'plugins.cassandra.checks.cluster_connectivity_diagnostics', 'function': 'run_cluster_connectivity_diagnostics'},
        ]
    },
    {
        "title": "Performance",
        "actions": [
            # Thread pool statistics from nodetool tpstats (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_thread_pool_stats', 'function': 'run_check_thread_pool_stats'},

            # Compaction statistics from nodetool compactionstats (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_compaction_stats', 'function': 'run_check_compaction_stats'},

            # GC log analysis from gc.log files (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_gc_analysis', 'function': 'run_check_gc_analysis'},

            # GC statistics from nodetool gcstats
            {'type': 'module', 'module': 'plugins.cassandra.checks.gcstats_check', 'function': 'run_gcstats_check'},

            # I/O statistics from iostat (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_io_stats', 'function': 'run_check_io_stats'},

            # Table statistics from nodetool cfstats (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_table_stats', 'function': 'run_check_table_stats'},

            # Compaction pending tasks
            {"type": "module", "module": "plugins.cassandra.checks.compaction_pending_tasks", "function": "run_compaction_pending_tasks"},

            # Tombstone metrics
            {'type': 'module', 'module': 'plugins.cassandra.checks.tombstone_metrics_check', 'function': 'run_tombstone_metrics_check'},
        ]
    },
    {
        "title": "Operational Health",
        "actions": [
            # Prometheus-based checks (Instaclustr managed clusters)
            {"type": "module", "module": "plugins.cassandra.checks.prometheus_jvm_heap", "function": "check_prometheus_jvm_heap"},
            {"type": "module", "module": "plugins.cassandra.checks.prometheus_cpu", "function": "check_prometheus_cpu"},
            {"type": "module", "module": "plugins.cassandra.checks.prometheus_disk_usage", "function": "check_prometheus_disk_usage"},
            {"type": "module", "module": "plugins.cassandra.checks.prometheus_compaction", "function": "check_prometheus_compaction"},
            {"type": "module", "module": "plugins.cassandra.checks.prometheus_latency", "function": "check_prometheus_latency"},

            # CQL-based checks (work on all clusters)
            {"type": "module", "module": "plugins.cassandra.checks.table_statistics", "function": "check_table_statistics"},
            {"type": "module", "module": "plugins.cassandra.checks.read_repair_settings", "function": "check_read_repair_settings"},
            {"type": "module", "module": "plugins.cassandra.checks.secondary_indexes", "function": "check_secondary_indexes"},
            {"type": "module", "module": "plugins.cassandra.checks.network_topology", "function": "check_network_topology"},

            # Memory and resource checks
            {"type": "module", "module": "plugins.cassandra.checks.memory_usage_check", "function": "run_memory_usage_check"},
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_jvm_stats', 'function': 'run_check_jvm_stats'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.cpu_load_average_check', 'function': 'run_cpu_load_average_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.temporary_files_check', 'function': 'run_temporary_files_check'},

            # Table health checks
            {'type': 'module', 'module': 'plugins.cassandra.checks.gc_grace_seconds_audit', 'function': 'run_gc_grace_seconds_audit'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.table_compression_settings', 'function': 'run_table_compression_settings'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.row_cache_check', 'function': 'run_row_cache_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.durable_writes_check', 'function': 'run_durable_writes_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.materialized_views_check', 'function': 'run_materialized_views_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.udf_aggregates_check', 'function': 'run_udf_aggregates_check'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.keyspace_replication_health', 'function': 'run_keyspace_replication_health'},
        ]
    },
    {
        "title": "Storage",
        "actions": [
            # Disk space analysis from nodetool tablestats
            {"type": "module", "module": "plugins.cassandra.checks.disk_space_per_keyspace", "function": "run_disk_space_per_keyspace_check"},

            # Data directory disk space
            {"type": "module", "module": "plugins.cassandra.checks.data_directory_disk_space_check", "function": "run_data_directory_disk_space_check"},

            # Disk usage check
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_disk_usage', 'function': 'run_check_disk_usage'},
        ]
    },
    {
        "title": "Configuration",
        "actions": [
            # Configuration audit from cassandra.yaml (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_config_audit', 'function': 'run_check_config_audit'},

            # JVM configuration audit from jvm.options (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_jvm_config', 'function': 'run_check_jvm_config'},

            # Environment configuration from cassandra-env.sh (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_cassandra_env', 'function': 'run_check_cassandra_env'},

            # Keyspace replication strategy
            {'type': 'module', 'module': 'plugins.cassandra.checks.keyspace_replication_strategy', 'function': 'run_keyspace_replication_strategy'},
        ]
    },
    {
        "title": "Operations",
        "actions": [
            # System log analysis from system.log (requires SSH)
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_system_log', 'function': 'run_check_system_log'},
        ]
    },
    {
        'title': 'Security',
        'actions': [
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_cve_vulnerabilities', 'function': 'run'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.superuser_roles', 'function': 'run_superuser_roles_check'},
        ]
    },
    {
        'title': 'Executive Summary',
        'actions': [
            # Aggregates all findings from previous checks - must run LAST
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_executive_summary', 'function': 'run_check_executive_summary'},
        ]
    },
]


def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS

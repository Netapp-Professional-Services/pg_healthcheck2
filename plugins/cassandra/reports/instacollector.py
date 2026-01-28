"""
Cassandra report definition for Instacollector imports.

This report includes checks that work with offline Instacollector data.
Each check parses data from the loaded diagnostic files.

Data Sources Available:
- nodetool_status.info: Cluster topology, node states, load
- nodetool_info.info: JVM heap, datacenter, rack, caches, uptime
- nodetool_gossipinfo.info: Gossip state, schema versions
- nodetool_describecluster.info: Schema version consistency
- nodetool_cfstats.info / nodetool_tablestats.info: Table statistics
- nodetool_tpstats.info: Thread pool statistics
- nodetool_gcstats.info: GC statistics
- nodetool_compactionstats.info: Compaction status
- nodetool_ring.info: Token distribution
- nodetool_version.info: Cassandra version
- disk.info: df -h and du -h output
- io_stat.info: iostat output
- cassandra.yaml: Configuration
- jvm.options: JVM settings
- cassandra-env.sh: Environment settings
- gc.log.*: Garbage collection logs
- system.log: Application logs

Checks included:
- Cluster Status: overview, schema consistency, gossip state
- Performance: thread pools, table stats, GC analysis, compaction stats, I/O stats
- Storage: keyspace disk usage, data directory usage, ring analysis
- Configuration: config audit, JVM config, environment audit
- Operations: system log analysis
- Security: CVE vulnerabilities
- Executive Summary: aggregated health score, key concerns, cross-node analysis
"""

REPORT_SECTIONS = [
    {
        "title": "Cluster Status",
        "actions": [
            # Cluster overview from nodetool status + info
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_cluster_overview', 'function': 'run_check_cluster_overview'},

            # Schema consistency from nodetool describecluster
            {'type': 'module', 'module': 'plugins.cassandra.checks.schema_version_consistency_check', 'function': 'run_schema_version_consistency_check'},

            # Gossip state analysis from nodetool gossipinfo
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_gossip_state', 'function': 'run_check_gossip_state'},

            # Token ring analysis from nodetool ring
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_ring_analysis', 'function': 'run_check_ring_analysis'},
        ]
    },
    {
        "title": "Performance",
        "actions": [
            # Thread pool statistics from nodetool tpstats
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_thread_pool_stats', 'function': 'run_check_thread_pool_stats'},

            # Table statistics from nodetool cfstats
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_table_stats', 'function': 'run_check_table_stats'},

            # Compaction statistics from nodetool compactionstats
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_compaction_stats', 'function': 'run_check_compaction_stats'},

            # GC log analysis from gc.log files
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_gc_analysis', 'function': 'run_check_gc_analysis'},

            # GC statistics from nodetool gcstats
            {'type': 'module', 'module': 'plugins.cassandra.checks.gcstats_check', 'function': 'run_gcstats_check'},

            # I/O statistics from io_stat.info
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_io_stats', 'function': 'run_check_io_stats'},
        ]
    },
    {
        "title": "Storage",
        "actions": [
            # Disk space analysis from nodetool tablestats
            {'type': 'module', 'module': 'plugins.cassandra.checks.disk_space_per_keyspace', 'function': 'run_disk_space_per_keyspace_check'},

            # Data directory disk space from disk.info (checks /var/lib/cassandra at 80%/90% thresholds)
            {'type': 'module', 'module': 'plugins.cassandra.checks.data_directory_disk_space_check', 'function': 'run_data_directory_disk_space_check'},
        ]
    },
    {
        "title": "Configuration",
        "actions": [
            # Configuration audit from cassandra.yaml
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_config_audit', 'function': 'run_check_config_audit'},

            # JVM configuration audit from jvm.options
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_jvm_config', 'function': 'run_check_jvm_config'},

            # Environment configuration from cassandra-env.sh
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_cassandra_env', 'function': 'run_check_cassandra_env'},
        ]
    },
    {
        "title": "Operations",
        "actions": [
            # System log analysis from system.log
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_system_log', 'function': 'run_check_system_log'},
        ]
    },
    {
        "title": "Security",
        "actions": [
            # CVE check uses version info from nodetool version
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_cve_vulnerabilities', 'function': 'run'},
        ]
    },
    {
        "title": "Executive Summary",
        "actions": [
            # Aggregates all findings from previous checks - must run LAST
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_executive_summary', 'function': 'run_check_executive_summary'},
        ]
    },
]


def get_default_report_definition(connector, settings):
    """Returns the report structure for Instacollector imports."""
    return REPORT_SECTIONS

"""
Kafka report definition for Instacollector imports.

This report uses the SAME checks as the live connector report. The
KafkaInstacollectorConnector implements the same interface as KafkaConnector,
allowing all existing checks to work unchanged with imported diagnostic data.

Design Pattern:
    Following the Cassandra and OpenSearch import patterns:
    1. Connector implements same interface as live connector
    2. Same checks work for both live and imported data
    3. No duplicate check modules needed

Data Sources (100% Coverage from Instacollector exports):
- kafka-versions-output.txt.info: Kafka version
- kafka-topics-describe-output.txt.info: Topic metadata, partitions, replicas, ISR
- consumer-groups-output.txt.info: Consumer groups with lag data
- kafka-metadata-quorum-status-output.txt.info: KRaft quorum status
- kafka-metadata-quorum-replication-output.txt.info: KRaft replication info
- kafka-api-versions-output.txt.info: Broker API versions
- broker.properties / server.properties: Broker configuration
- controller.properties: Controller configuration (KRaft)
- log4j.properties: Logging configuration
- kafkaServer-gc.log*: GC logs
- server.log*: Broker logs
- controller.log*: Controller logs
- state-change.log*: Partition state changes
- kafka-authorizer.log*: Authorization events
- mem.info: Memory information (/proc/meminfo)
- cpu.info: CPU information (/proc/cpuinfo)
- io_stat.info: iostat output
- file_descriptor.info: File descriptor limits
- hosts_file.info: Hosts configuration

Checks included (using existing check modules):
- Security: CVE vulnerability analysis, Authorization audit
- Cluster Health: KRaft quorum, API versions
- Topic Health: ISR status, partition analysis, topic configuration
- Consumer Health: Consumer lag analysis
- OS-Level Metrics: CPU, memory, file descriptors, system limits
- Configuration: Broker config audit, Controller config (KRaft)
- Performance: GC analysis, I/O statistics
- Log Analysis: Error and warning detection, State changes
- Network: Hosts file analysis
- Executive Summary: Aggregated health score, key concerns, cross-broker analysis
"""

REPORT_SECTIONS = [
    {
        "title": "Security",
        "actions": [
            # CVE vulnerability check using version info
            {'type': 'module', 'module': 'plugins.kafka.checks.check_cve_vulnerabilities', 'function': 'run'},

            # Authorization audit from kafka-authorizer.log
            {'type': 'module', 'module': 'plugins.kafka.checks.check_authorization', 'function': 'run_authorization_check'},
        ]
    },
    {
        "title": "Cluster Health",
        "actions": [
            # KRaft quorum health (from quorum status/replication files)
            {'type': 'module', 'module': 'plugins.kafka.checks.check_kraft_quorum', 'function': 'run_kraft_quorum_check'},

            # API versions analysis
            {'type': 'module', 'module': 'plugins.kafka.checks.check_api_versions', 'function': 'run_api_versions_check'},
        ]
    },
    {
        "title": "Topic Health",
        "actions": [
            # ISR health from topics data (uses execute_query -> describe_topics)
            {'type': 'module', 'module': 'plugins.kafka.checks.check_isr_health', 'function': 'run_check_isr_health'},

            # Topic count and naming analysis
            {'type': 'module', 'module': 'plugins.kafka.checks.check_topic_count_and_naming', 'function': 'run_check_topic_count_and_naming'},

            # Topic configuration audit
            {'type': 'module', 'module': 'plugins.kafka.checks.check_topic_configuration', 'function': 'run_topic_configuration_check'},

            # Internal topic replication check
            {'type': 'module', 'module': 'plugins.kafka.checks.check_internal_topic_replication', 'function': 'run_internal_topic_replication_check'},
        ]
    },
    {
        "title": "Consumer Health",
        "actions": [
            # Consumer lag analysis (uses execute_query -> consumer_lag)
            {'type': 'module', 'module': 'plugins.kafka.checks.check_consumer_lag', 'function': 'run_consumer_lag'},

            # Consumer group health
            {'type': 'module', 'module': 'plugins.kafka.checks.check_consumer_group_health', 'function': 'run_check_consumer_group_health'},
        ]
    },
    {
        "title": "OS-Level Metrics",
        "actions": [
            # CPU analysis from cpu.info (uses execute_ssh_on_all_hosts)
            {'type': 'module', 'module': 'plugins.kafka.checks.check_cpu_load', 'function': 'run_cpu_load_check'},

            # Memory analysis from mem.info (uses execute_ssh_on_all_hosts)
            {'type': 'module', 'module': 'plugins.kafka.checks.check_memory_usage', 'function': 'run_memory_usage_check'},

            # File descriptor limits from file_descriptor.info
            {'type': 'module', 'module': 'plugins.kafka.checks.check_file_descriptors', 'function': 'run_file_descriptor_check'},

            # System limits check
            {'type': 'module', 'module': 'plugins.kafka.checks.check_system_limits', 'function': 'run_system_limits_check'},
        ]
    },
    {
        "title": "Configuration",
        "actions": [
            # Broker configuration audit from properties files
            {'type': 'module', 'module': 'plugins.kafka.checks.check_broker_config', 'function': 'run_broker_config_check'},

            # Controller configuration (KRaft mode)
            {'type': 'module', 'module': 'plugins.kafka.checks.check_controller_config', 'function': 'run_controller_config_check'},

            # Network configuration (hosts file)
            {'type': 'module', 'module': 'plugins.kafka.checks.check_network_config', 'function': 'run_network_config_check'},
        ]
    },
    {
        "title": "Performance",
        "actions": [
            # GC analysis from gc.log files
            {'type': 'module', 'module': 'plugins.kafka.checks.check_gc_pauses', 'function': 'run_gc_pauses_check'},

            # I/O statistics from io_stat.info
            {'type': 'module', 'module': 'plugins.kafka.checks.check_iostat', 'function': 'run_check_iostat'},

            # Disk usage analysis
            {'type': 'module', 'module': 'plugins.kafka.checks.check_disk_usage', 'function': 'run_check_disk_usage'},
        ]
    },
    {
        "title": "Log Analysis",
        "actions": [
            # Error analysis from server.log, controller.log
            {'type': 'module', 'module': 'plugins.kafka.checks.check_log_errors', 'function': 'run_log_errors_check'},

            # State change analysis from state-change.log
            {'type': 'module', 'module': 'plugins.kafka.checks.check_state_changes', 'function': 'run_state_changes_check'},
        ]
    },
    {
        "title": "Executive Summary",
        "actions": [
            # Aggregates all findings from previous checks - must run LAST
            {'type': 'module', 'module': 'plugins.kafka.checks.check_executive_summary', 'function': 'run_executive_summary_check'},
        ]
    },
]


def get_default_report_definition(connector, settings):
    """Returns the report structure for Instacollector imports."""
    return REPORT_SECTIONS

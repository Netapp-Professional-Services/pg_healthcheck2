REPORT_SECTIONS = [
    {
        "title": "Kafka Health Check",
        "actions": [
            # Prometheus-based checks (Instaclustr managed clusters)
            # These checks work without SSH access and provide comprehensive monitoring
            # They will be skipped if instaclustr_prometheus_enabled is not set

            # ========== CRITICAL: Data Loss & Availability (Priority 10) ==========
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_unclean_elections', 'function': 'check_prometheus_unclean_elections'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_offline_partitions', 'function': 'check_prometheus_offline_partitions'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_under_replicated', 'function': 'check_prometheus_under_replicated'},

            # ========== HIGH: Infrastructure & Replication (Priority 9) ==========
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_file_descriptors', 'function': 'check_prometheus_file_descriptors'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_network_errors', 'function': 'check_prometheus_network_errors'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_isr_health', 'function': 'check_prometheus_isr_health'},

            # ========== HIGH: Performance & Saturation (Priority 8) ==========
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_request_latency', 'function': 'check_prometheus_request_latency'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_request_handler', 'function': 'check_prometheus_request_handler'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_replica_fetcher', 'function': 'check_prometheus_replica_fetcher'},

            # ========== MEDIUM: Distribution & Resources (Priority 7) ==========
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_partition_balance', 'function': 'check_prometheus_partition_balance'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_broker_health', 'function': 'check_prometheus_broker_health'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_gc_health', 'function': 'check_prometheus_gc_health'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_log_flush', 'function': 'check_prometheus_log_flush'},
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_controller_health', 'function': 'check_prometheus_controller_health'},

            # ========== MEDIUM: Consumer Health (Priority 7) ==========
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_consumer_lag', 'function': 'check_prometheus_consumer_lag'},

            # ========== LOW: Backpressure Indicators (Priority 6) ==========
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_purgatory', 'function': 'check_prometheus_purgatory'},

            # ========== JVM Memory (Priority 9, often not available for Kafka) ==========
            {'type': 'module', 'module': 'plugins.kafka.checks.prometheus_jvm_heap', 'function': 'check_prometheus_jvm_heap'},
        ]
    },
    {
        "title": "OS-Level Metrics",
        "actions": [
            # SSH-based OS metrics across all brokers
            {'type': 'module', 'module': 'plugins.kafka.checks.check_cpu_load', 'function': 'run_cpu_load_check'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_memory_usage', 'function': 'run_memory_usage_check'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_file_descriptors', 'function': 'run_file_descriptor_check'},
        ]
    },
    {
        "title": "Configuration Audits",
        "actions": [
            # SSH-based configuration and log analysis
            {'type': 'module', 'module': 'plugins.kafka.checks.check_broker_config', 'function': 'run_broker_config_check'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_log_errors', 'function': 'run_log_errors_check'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_gc_pauses', 'function': 'run_gc_pauses_check'},
        ]
    },
    {
        "title": "Kafka Overview",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.kafka_overview', 'function': 'run_kafka_overview'},
        ]
    },
    {
        "title": "Kafka Purgatory",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_purgatory_size', 'function': 'run_check_purgatory_size'},
        ]
    },
    {
        "title": "Kafka Cluster Health",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_partition_balance', 'function': 'run_partition_balance'},
        ]
    },
    {
        'title': 'Topic Management',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_topic_count_and_naming', 'function': 'run_check_topic_count_and_naming'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_topic_configuration', 'function': 'run_topic_configuration_check'},
        ]
    },
    {
        "title": "Performance Monitoring",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_iostat', 'function': 'run_check_iostat'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_jvm_stats', 'function': 'run_check_jvm_stats'},
        ]
    },
    {
        'title': 'Replication and ISR Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_isr_health', 'function': 'run_check_isr_health'},
        ]
    },
    {
        'title': 'Kafka Storage Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_storage_health', 'function': 'run_check_storage_health'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_disk_usage', 'function': 'run_check_disk_usage'},
        ]
    },
    {
        'title': 'Broker Availability',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_broker_availability', 'function': 'run_check_broker_availability'},

        ]
    },
    {
        'title': 'Consumer Groups',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_consumer_group_health', 'function': 'run_check_consumer_group_health'},
        ]
    },
    {
        'title': 'Consumer Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_consumer_lag', 'function': 'run_consumer_lag'},
        ]
    },
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS

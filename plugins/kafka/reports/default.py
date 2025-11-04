REPORT_SECTIONS = [
    {
        "title": "Kafka Health Check",
        "actions": []
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

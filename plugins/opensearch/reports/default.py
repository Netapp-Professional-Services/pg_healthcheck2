"""
Default report definition for the OpenSearch plugin.

This defines the sections and checks that will be included in the
OpenSearch health check report.
"""

REPORT_SECTIONS = [
    {
     	'title': '', # No title for the header section
        'actions': [
            {'type': 'header', 'file': 'report_header.txt'},
        ]
    },
    {
        'title': 'Cluster Overview',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.cluster_health_check',
                'function': 'run_cluster_health_check'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_cluster_settings',
                'function': 'run_check_cluster_settings'
            }
        ]
    },
    {
        'title': 'Index Health & Management',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_index_health',
                'function': 'run_check_index_health'
            }
        ]
    },
    {
        'title': 'Node & Resource Health',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_node_metrics',
                'function': 'run_check_node_metrics'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_disk_usage',
                'function': 'run_check_disk_usage'
            }
        ]
    },
    {
        'title': 'Performance Metrics',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_cluster_performance',
                'function': 'run_check_cluster_performance'
            }
        ]
    },
    {
        'title': 'AWS OpenSearch Service',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_aws_service_software',
                'function': 'run_check_aws_service_software'
            }
        ]
    },
    {
        'title': 'Advanced Diagnostics',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_diagnostics',
                'function': 'run_check_diagnostics'
            }
        ]
    }
]

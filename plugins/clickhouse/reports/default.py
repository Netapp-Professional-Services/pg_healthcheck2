def get_report_definition():
    """
    Returns the default comprehensive report definition for the ClickHouse plugin.

    This report provides comprehensive coverage of ClickHouse cluster health including:
    - SQL-based checks (work on all clusters)
    - SSH-based OS checks (require SSH access, auto-skip if unavailable)

    Total checks: 15 (12 SQL-based + 3 SSH-based)
    """
    return [
        {
            'title': 'Security',
            'description': 'Security vulnerability analysis',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_cve_vulnerabilities',
                    'function': 'run'
                }
            ]
        },
        {
            'title': 'Cluster Overview',
            'description': 'High-level cluster health and topology',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.cluster_health_check',
                    'function': 'run_cluster_health_check'
                },
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.system_metrics_check',
                    'function': 'run_system_metrics_check'
                }
            ]
        },
        {
            'title': 'Error Monitoring',
            'description': 'Error tracking, failed queries, and exception analysis',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_errors',
                    'function': 'run_check_errors'
                }
            ]
        },
        {
            'title': 'Backup & Configuration',
            'description': 'Backup monitoring and configuration drift analysis',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_backups',
                    'function': 'run_check_backups'
                },
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_configuration',
                    'function': 'run_check_configuration'
                }
            ]
        },
        {
            'title': 'Performance Metrics',
            'description': 'Query performance, bottleneck analysis, and optimization insights',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_query_performance',
                    'function': 'run_check_query_performance'
                }
            ]
        },
        {
            'title': 'Table Health & Management',
            'description': 'Table health, dictionaries, and storage efficiency',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_table_health',
                    'function': 'run_check_table_health'
                },
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_dictionaries',
                    'function': 'run_check_dictionaries'
                }
            ]
        },
        {
            'title': 'Node & Resource Health',
            'description': 'Node-level metrics and resource utilization',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_node_metrics',
                    'function': 'run_check_node_metrics'
                },
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_disk_usage',
                    'function': 'run_check_disk_usage'
                }
            ]
        },
        {
            'title': 'OS-Level Health (SSH)',
            'description': 'SSH-based OS metrics - auto-skips if SSH unavailable',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_os_system_metrics',
                    'function': 'run_check_os_system_metrics'
                },
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_os_disk_usage',
                    'function': 'run_check_os_disk_usage'
                },
                {
                    'type': 'module',
                    'module': 'plugins.clickhouse.checks.check_os_log_analysis',
                    'function': 'run_check_os_log_analysis'
                }
            ]
        }
    ]

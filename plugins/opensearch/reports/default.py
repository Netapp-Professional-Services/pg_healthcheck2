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
        'title': 'Security',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_cve_vulnerabilities',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_ssl_certificates',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_security_audit',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_security_mappings',
                'function': 'run'
            }
        ]
    },
    {
        'title': 'License & Compliance',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_license_status',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_xpack_features',
                'function': 'run'
            }
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
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_shard_allocation',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_thread_pools',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_zone_awareness',
                'function': 'run'
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
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_ilm_policies',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_templates',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_data_streams',
                'function': 'run'
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
        'title': 'Backup & Recovery',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_snapshot_status',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_dangling_indices',
                'function': 'run'
            }
        ]
    },
    {
        'title': 'Data Ingestion',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_ingest_pipelines',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_enrich_policies',
                'function': 'run'
            }
        ]
    },
    {
        'title': 'Alerting & Automation',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_watcher_status',
                'function': 'run'
            }
        ]
    },
    {
        'title': 'Cross-Cluster & Multi-Region',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_ccr_status',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_remote_clusters',
                'function': 'run'
            }
        ]
    },
    {
        'title': 'Machine Learning',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_ml_status',
                'function': 'run'
            }
        ]
    },
    {
        'title': 'Data Transforms & Rollups',
        'actions': [
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_transforms',
                'function': 'run'
            },
            {
                'type': 'module',
                'module': 'plugins.opensearch.checks.check_rollup_jobs',
                'function': 'run'
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

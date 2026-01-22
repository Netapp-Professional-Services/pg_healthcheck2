"""
Kafka Metric Definitions for Adaptive Collection

Defines how to collect each Kafka metric using different strategies:
- Instaclustr Prometheus API
- Local Prometheus JMX Exporter
- Standard JMX

Used by the generic metric_collection_strategies framework in plugins/common/.
"""

# ============================================================================
# Critical Metrics (Priority 10) - Data Loss & Availability
# ============================================================================

KAFKA_METRICS = {
    'under_replicated_partitions': {
        'description': 'Number of under-replicated partitions (one failure from data loss)',
        'priority': 10,
        'instaclustr_prometheus': 'ic_node_under_replicated_partitions',
        'local_prometheus': 'kafka_server_replicamanager_underreplicatedpartitions',
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions',
            'attribute': 'Value'
        },
        'thresholds': {
            'warning': 0,  # Any URPs is a warning
            'critical': 10  # 10+ URPs is critical
        }
    },

    'offline_partitions': {
        'description': 'Number of offline partitions (controller-only, ZooKeeper mode)',
        'priority': 10,
        'instaclustr_prometheus': 'ic_node_offline_partitions_kraft',
        'local_prometheus': None,  # Controller metric not exported in KRaft self-hosted; use offline_replica_count
        'jmx': {
            'mbean': 'kafka.controller:type=KafkaController,name=OfflinePartitionsCount',
            'attribute': 'Value'
        },
        'thresholds': {
            'critical': 0  # Any offline partitions is critical
        },
        'note': 'Controller-only metric. For KRaft self-hosted, use offline_replica_count instead. Instaclustr exports this for both modes.'
    },

    'unclean_leader_elections': {
        'description': 'Number of unclean leader elections (data loss events)',
        'priority': 10,
        'instaclustr_prometheus': 'ic_node_unclean_leader_elections_kraft',
        'local_prometheus': None,  # Not available in KRaft mode (Kafka 3.x+), only in ZooKeeper mode
        'jmx': {
            'mbean': 'kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec',
            'attribute': 'Count'
        },
        'thresholds': {
            'critical': 0  # Any unclean elections indicates past data loss
        },
        'note': 'Only available in ZooKeeper mode on controller broker. KRaft mode (3.x+) uses different election mechanism and this metric does not exist.'
    },

    'offline_replica_count': {
        'description': 'Number of offline replicas per broker (KRaft-compatible alternative to offline_partitions)',
        'priority': 10,
        'instaclustr_prometheus': None,  # Use offline_partitions instead for Instaclustr
        'local_prometheus': 'kafka_server_replicamanager_offlinereplicacount',
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaManager,name=OfflineReplicaCount',
            'attribute': 'Value'
        },
        'thresholds': {
            'critical': 0  # Any offline replicas is critical
        },
        'note': 'Per-broker metric that works in both ZooKeeper and KRaft modes. Alternative to controller-only offline_partitions metric.'
    },

    # ========================================================================
    # High Priority Metrics (Priority 8-9) - Performance & Infrastructure
    # ========================================================================

    'file_descriptors': {
        'description': 'File descriptor usage (prevents "too many open files" crashes)',
        'priority': 9,
        'instaclustr_prometheus': 'ic_node_filedescriptoropencount',
        'local_prometheus': None,  # Not exposed by standard JMX exporter, use JMX fallback
        'jmx': {
            'mbean': 'java.lang:type=OperatingSystem',
            'attribute': 'OpenFileDescriptorCount'
        },
        'thresholds': {
            'warning': 0.70,  # 70% of limit
            'critical': 0.85  # 85% of limit
        }
    },

    'request_handler_idle': {
        'description': 'Request handler idle percentage (broker saturation)',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_request_handler_avg_idle_percent',
        'local_prometheus': None,  # Not exposed as percentage by standard JMX exporter - only counter available
        'jmx': {
            'mbean': 'kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent',
            'attribute': 'OneMinuteRate'
        },
        'thresholds': {
            'warning': 0.30,  # <30% idle = warning
            'critical': 0.10  # <10% idle = critical (overloaded)
        },
        'inverted': True,  # Low values are bad
        'note': 'Local Prometheus JMX exporter only exposes count_total (cumulative counter), not the actual percentage. Use JMX or Instaclustr API for this metric.'
    },

    'produce_latency': {
        'description': 'Average produce request latency in milliseconds',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_produce_request_time_milliseconds',
        'local_prometheus': {'metric': 'kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"}', 'query_type': 'gauge'},
        'jmx': {
            'mbean': 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce',
            'attribute': 'Mean'
        },
        'thresholds': {
            'warning': 100,  # 100ms
            'critical': 500  # 500ms
        }
    },

    'consumer_fetch_latency': {
        'description': 'Average consumer fetch request latency in milliseconds',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_fetch_consumer_request_time_milliseconds',
        'local_prometheus': {'metric': 'kafka_network_requestmetrics_totaltimems{request="FetchConsumer",quantile="0.99"}', 'query_type': 'gauge'},
        'jmx': {
            'mbean': 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer',
            'attribute': 'Mean'
        },
        'thresholds': {
            'warning': 100,  # 100ms
            'critical': 500  # 500ms
        }
    },

    'follower_fetch_latency': {
        'description': 'Average follower fetch request latency in milliseconds',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_fetch_follower_request_time_milliseconds',
        'local_prometheus': {'metric': 'kafka_network_requestmetrics_totaltimems{request="FetchFollower",quantile="0.99"}', 'query_type': 'gauge'},
        'jmx': {
            'mbean': 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower',
            'attribute': 'Mean'
        },
        'thresholds': {
            'warning': 200,  # 200ms
            'critical': 1000  # 1000ms
        }
    },

    'isr_shrink_rate': {
        'description': 'ISR shrink rate (replicas falling out of sync)',
        'priority': 9,
        'instaclustr_prometheus': 'ic_node_isr_shrink_rate',
        'local_prometheus': {'metric': 'kafka_server_replicamanager_isrshrinks_total', 'query_type': 'counter'},
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaManager,name=IsrShrinksPerSec',
            'attribute': 'OneMinuteRate'
        },
        'thresholds': {
            'warning': 1,  # 1/sec
            'critical': 10  # 10/sec
        }
    },

    # ========================================================================
    # Medium Priority Metrics (Priority 7) - Resources & Distribution
    # ========================================================================

    'partition_count': {
        'description': 'Number of partitions on broker',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_partition_count',
        'local_prometheus': 'kafka_server_replicamanager_partitioncount',
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaManager,name=PartitionCount',
            'attribute': 'Value'
        },
        'thresholds': {
            'warning': 1500,
            'critical': 2000
        }
    },

    'leader_count': {
        'description': 'Number of leader partitions on broker',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_leader_count',
        'local_prometheus': 'kafka_server_replicamanager_leadercount',
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaManager,name=LeaderCount',
            'attribute': 'Value'
        }
    },

    'active_controller': {
        'description': 'Active controller count (should be 1 cluster-wide)',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_active_controller_count_kraft',
        'local_prometheus': 'kafka_controller_kafkacontroller_activecontrollercount',
        'jmx': {
            'mbean': 'kafka.controller:type=KafkaController,name=ActiveControllerCount',
            'attribute': 'Value'
        },
        'note': 'Only available on controller broker. Should be 1 cluster-wide.'
    },

    'consumer_lag': {
        'description': 'Consumer lag in messages',
        'priority': 7,
        'instaclustr_prometheus': 'kafka_consumerGroup_consumerlag',
        'local_prometheus': 'kafka_consumergroup_lag',
        'jmx': None,  # Consumer lag not available via JMX, requires kafka-consumer-groups.sh
        'thresholds': {
            'warning': 10000,
            'critical': 100000
        }
    },

    'purgatory_produce': {
        'description': 'Produce purgatory size (requests waiting for acks)',
        'priority': 6,
        'instaclustr_prometheus': 'ic_node_produce_purgatory_size',
        'local_prometheus': 'kafka_server_delayedoperationpurgatory_purgatorysize{delayedOperation="Produce"}',
        'jmx': {
            'mbean': 'kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Produce',
            'attribute': 'Value'
        },
        'thresholds': {
            'warning': 100,
            'critical': 500
        }
    },

    'purgatory_fetch': {
        'description': 'Fetch purgatory size (requests waiting for data)',
        'priority': 6,
        'instaclustr_prometheus': 'ic_node_fetch_purgatory_size',
        'local_prometheus': 'kafka_server_delayedoperationpurgatory_purgatorysize{delayedOperation="Fetch"}',
        'jmx': {
            'mbean': 'kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Fetch',
            'attribute': 'Value'
        },
        'thresholds': {
            'warning': 100,
            'critical': 500
        }
    },

    # ========================================================================
    # JVM Metrics (Priority 9)
    # ========================================================================

    'jvm_heap_used': {
        'description': 'JVM heap memory used',
        'priority': 9,
        'instaclustr_prometheus': 'jvm_memory_bytes_used{area="heap"}',
        'local_prometheus': 'jvm_memory_bytes_used{area="heap"}',
        'jmx': {
            'mbean': 'java.lang:type=Memory',
            'attribute': 'HeapMemoryUsage.used'
        }
    },

    'jvm_heap_max': {
        'description': 'JVM heap memory maximum',
        'priority': 9,
        'instaclustr_prometheus': 'jvm_memory_bytes_max{area="heap"}',
        'local_prometheus': 'jvm_memory_bytes_max{area="heap"}',
        'jmx': {
            'mbean': 'java.lang:type=Memory',
            'attribute': 'HeapMemoryUsage.max'
        }
    },

    # ========================================================================
    # GC Metrics (Priority 7)
    # ========================================================================

    'young_gc_time': {
        'description': 'Young generation GC collection time (cumulative milliseconds)',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_young_gengc_collection_time',  # Also: ic_node_young_gengc_collection_time_kraft
        'local_prometheus': 'jvm_gc_collection_seconds_sum{gc="G1 Young Generation"}',  # Convert from seconds to ms
        'jmx': {
            'mbean': 'java.lang:type=GarbageCollector,name=G1 Young Generation',
            'attribute': 'CollectionTime'
        },
        'thresholds': {
            'warning': 5,  # 5% of time in GC
            'critical': 10  # 10% of time in GC
        },
        'note': 'Cumulative values - monitor trends over time'
    },

    'old_gc_time': {
        'description': 'Old generation (Full) GC collection time (cumulative milliseconds)',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_old_gengc_collection_time',  # Also: ic_node_old_gengc_collection_time_kraft
        'local_prometheus': 'jvm_gc_collection_seconds_sum{gc="G1 Old Generation"}',  # Convert from seconds to ms
        'jmx': {
            'mbean': 'java.lang:type=GarbageCollector,name=G1 Old Generation',
            'attribute': 'CollectionTime'
        },
        'thresholds': {
            'warning': 2,  # 2% of time in Full GC
            'critical': 5  # 5% of time in Full GC
        },
        'note': 'Full GCs are more concerning than Young GCs'
    },

    # ========================================================================
    # Network Error Metrics (Priority 9)
    # ========================================================================

    'network_rx_errors': {
        'description': 'Network receive errors (packet loss, corruption)',
        'priority': 9,
        'instaclustr_prometheus': 'ic_node_networkinerrorsdelta',
        'local_prometheus': 'node_network_receive_errs_total',  # Rate over time
        'jmx': None,  # Network errors not available via JMX
        'thresholds': {
            'warning': 1,  # Any errors is a warning
            'critical': 100  # 100+ errors is critical
        },
        'note': 'Any network errors indicate infrastructure problems'
    },

    'network_tx_errors': {
        'description': 'Network transmit errors (packet loss, corruption)',
        'priority': 9,
        'instaclustr_prometheus': 'ic_node_networkouterrorsdelta',
        'local_prometheus': 'node_network_transmit_errs_total',  # Rate over time
        'jmx': None,
        'thresholds': {
            'warning': 1,
            'critical': 100
        }
    },

    'network_rx_drops': {
        'description': 'Network receive drops (buffer overflow)',
        'priority': 9,
        'instaclustr_prometheus': 'ic_node_networkindroppeddelta',
        'local_prometheus': 'node_network_receive_drop_total',  # Rate over time
        'jmx': None,
        'thresholds': {
            'warning': 1,
            'critical': 100
        }
    },

    'network_tx_drops': {
        'description': 'Network transmit drops (buffer overflow)',
        'priority': 9,
        'instaclustr_prometheus': 'ic_node_networkoutdroppeddelta',
        'local_prometheus': 'node_network_transmit_drop_total',  # Rate over time
        'jmx': None,
        'thresholds': {
            'warning': 1,
            'critical': 100
        }
    },

    # ========================================================================
    # Log Flush Performance Metrics (Priority 7)
    # ========================================================================

    'log_flush_rate': {
        'description': 'Log flush rate (flushes per second)',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_log_flush_rate',  # Also: ic_node_log_flush_rate_kraft
        'local_prometheus': 'kafka_log_logflushstats_logflushrateandtimems',
        'jmx': {
            'mbean': 'kafka.log:type=LogFlushStats,name=LogFlushRateAndRequestsMs',
            'attribute': 'OneMinuteRate'
        },
        'note': 'Monitor trends - high flush rate may indicate sync issues'
    },

    'log_flush_time': {
        'description': 'Log flush time in milliseconds',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_log_flush_time_milliseconds',  # Also: ic_node_log_flush_time_kraft_milliseconds
        'local_prometheus': 'kafka_log_logflushstats_logflushrateandtimems',
        'jmx': {
            'mbean': 'kafka.log:type=LogFlushStats,name=LogFlushRateAndRequestsMs',
            'attribute': 'Mean'
        },
        'thresholds': {
            'warning': 100,  # 100ms
            'critical': 500  # 500ms
        }
    },

    # ========================================================================
    # Replica Fetcher Metrics (Priority 8)
    # ========================================================================

    'replica_fetcher_failed_partitions': {
        'description': 'Number of failed partitions in replica fetcher',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_replica_fetcher_failed_partitions_count',
        'local_prometheus': 'kafka_server_replicafetchermanager_failedpartitionscount',
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaFetcherManager,name=FailedPartitionsCount',
            'attribute': 'Value'
        },
        'thresholds': {
            'warning': 1,  # Any failed partitions
            'critical': 10  # 10+ failed partitions
        }
    },

    'replica_fetcher_max_lag': {
        'description': 'Maximum replica lag in messages',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_replica_fetcher_max_lag',
        'local_prometheus': 'kafka_server_replicamanager_maxlag',
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaManager,name=MaxLag',
            'attribute': 'Value'
        },
        'thresholds': {
            'warning': 1000,  # 1000 messages
            'critical': 10000  # 10000 messages
        }
    },

    'replica_fetcher_min_fetch_rate': {
        'description': 'Minimum fetch rate for replica fetchers',
        'priority': 8,
        'instaclustr_prometheus': 'ic_node_replica_fetcher_min_fetch_rate',
        'local_prometheus': 'kafka_server_replicafetchermanager_minfetchrate',
        'jmx': {
            'mbean': 'kafka.server:type=ReplicaFetcherManager,name=MinFetchRate',
            'attribute': 'Value'
        },
        'inverted': True  # Low values are bad
    },

    # ========================================================================
    # Broker Health Metrics (Priority 7)
    # ========================================================================

    'broker_cpu_utilization': {
        'description': 'Broker CPU utilization percentage',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_cpu_utilization',
        'local_prometheus': 'node_cpu_seconds_total',  # Requires calculation for percentage
        'jmx': {
            'mbean': 'java.lang:type=OperatingSystem',
            'attribute': 'ProcessCpuLoad'  # Returns 0.0-1.0, multiply by 100
        },
        'thresholds': {
            'warning': 75,  # 75%
            'critical': 90  # 90%
        }
    },

    'broker_disk_utilization': {
        'description': 'Broker disk utilization percentage',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_disk_utilization',
        'local_prometheus': 'node_filesystem_avail_bytes',  # Requires calculation
        'jmx': None,  # Disk metrics not available via JMX
        'thresholds': {
            'warning': 80,  # 80%
            'critical': 90  # 90%
        }
    },

    'broker_disk_available': {
        'description': 'Broker disk available in bytes',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_disk_available',
        'local_prometheus': 'node_filesystem_avail_bytes{mountpoint="/var/lib/kafka"}',
        'jmx': None
    },

    'broker_bytes_in': {
        'description': 'Broker bytes in per second',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_broker_topic_bytes_in',
        'local_prometheus': {'metric': 'kafka_server_brokertopicmetrics_bytesin_total', 'query_type': 'counter'},
        'jmx': {
            'mbean': 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec',
            'attribute': 'OneMinuteRate'
        }
    },

    'broker_bytes_out': {
        'description': 'Broker bytes out per second',
        'priority': 7,
        'instaclustr_prometheus': 'ic_node_broker_topic_bytes_out',
        'local_prometheus': {'metric': 'kafka_server_brokertopicmetrics_bytesout_total', 'query_type': 'counter'},
        'jmx': {
            'mbean': 'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec',
            'attribute': 'OneMinuteRate'
        }
    }
}


# ============================================================================
# Utility Functions
# ============================================================================

def get_metric_definition(metric_name: str):
    """
    Get metric definition by name.

    Args:
        metric_name: Name of the metric

    Returns:
        Metric definition dict or None
    """
    return KAFKA_METRICS.get(metric_name)


def get_metrics_by_priority(priority: int):
    """
    Get all metrics with a specific priority level.

    Args:
        priority: Priority level (1-10)

    Returns:
        List of (metric_name, metric_def) tuples
    """
    return [
        (name, metric)
        for name, metric in KAFKA_METRICS.items()
        if metric.get('priority') == priority
    ]


def get_critical_metrics():
    """Get all priority 10 (critical) metrics."""
    return get_metrics_by_priority(10)


def get_high_priority_metrics():
    """Get all priority 8-9 (high) metrics."""
    return get_metrics_by_priority(8) + get_metrics_by_priority(9)


def list_all_metrics():
    """Get list of all metric names."""
    return list(KAFKA_METRICS.keys())

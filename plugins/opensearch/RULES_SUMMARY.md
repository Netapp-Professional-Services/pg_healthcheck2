# OpenSearch Plugin - Alerting Rules Summary

## Overview

The OpenSearch plugin includes **56 alerting metrics** across **7 rule files**, providing comprehensive monitoring coverage for:
- Cluster health and stability
- Index health and shard distribution
- Node resource utilization (JVM, disk, CPU)
- Performance metrics (search/indexing latency, caching)
- Cluster configuration and settings
- AWS-specific service monitoring

## Rule Coverage by Severity

- **Critical Rules:** 31 metrics (immediate action required)
- **High Priority Rules:** 34 metrics (plan remediation within days)
- **Medium Priority Rules:** 9 metrics (monitor trends)

## Rule Files

### 1. cluster_health_check.json (10 metrics)

Monitors overall cluster health status and operational stability.

| Metric | Critical | High | Medium | Focus Area |
|--------|----------|------|--------|------------|
| cluster_status_red | ✓ | - | - | Primary shard failures |
| cluster_status_yellow | - | ✓ | - | Replica allocation issues |
| unassigned_shards_total | ✓ | - | - | Shard allocation failures |
| active_shards_percent | ✓ | ✓ | - | Overall shard health |
| pending_tasks | ✓ | ✓ | - | Master node workload |
| task_max_waiting_time_ms | ✓ | ✓ | - | Cluster state bottlenecks |
| initializing_shards | - | ✓ | - | Recovery operations |
| relocating_shards | - | ✓ | - | Rebalancing activity |
| number_of_in_flight_fetch | - | ✓ | - | Network/node performance |
| timed_out | ✓ | - | - | API responsiveness |

**Key Thresholds:**
- Red status: Critical (any occurrence)
- Unassigned shards: Critical at 1+
- Active shards: Critical below 80%, High below 90%
- Pending tasks: Critical at 50+, High at 20+

---

### 2. check_node_metrics.json (6 metrics)

Monitors JVM health, garbage collection, and node resource utilization.

| Metric | Critical | High | Medium | Focus Area |
|--------|----------|------|--------|------------|
| node_heap_usage_percent | 85% | 75% | 65% | JVM memory pressure |
| node_disk_usage_percent | 90% | 85% | 75% | Disk watermarks |
| thread_pool_rejections | 1+ | - | - | Cluster overload |
| file_descriptor_usage_percent | 90% | 75% | - | FD exhaustion risk |
| old_generation_gc_time_percent | - | 10% | 5% | GC performance |
| circuit_breaker_tripped | 1+ | - | - | Memory protection |

**Key Thresholds:**
- Heap: Critical 85%, High 75%, Medium 65%
- Disk watermarks: Critical 90% (high watermark), High 85% (low watermark)
- Thread rejections: Critical at any occurrence
- Old gen GC time: High >10%, Medium >5%

---

### 3. check_index_health.json (7 metrics)

Monitors index status, shard distribution, and index sizing.

| Metric | Critical | High | Medium | Focus Area |
|--------|----------|------|--------|------------|
| red_indices | 1+ | - | - | Data inaccessibility |
| yellow_indices | - | 1+ | - | Reduced redundancy |
| unassigned_shards | 1+ | - | - | Allocation failures |
| total_indices | - | 500+ | 200+ | Cluster state size |
| large_index_shard_count | - | 50+ | - | Shard overhead |
| index_size_gb | - | 100+ | - | Index sizing |
| shard_imbalance_percent | - | 30%+ | - | Distribution issues |

**Key Thresholds:**
- Red/Yellow indices: Critical/High at any occurrence
- Total indices: High at 500+, Medium at 200+
- Index size: High at 100GB+
- Shard count per index: High at 50+

---

### 4. check_cluster_performance.json (8 metrics)

Monitors search/indexing performance, thread pools, and caching efficiency.

| Metric | Critical | High | Medium | Focus Area |
|--------|----------|------|--------|------------|
| search_latency_ms | 2000+ | 1000+ | 500+ | Query performance |
| indexing_latency_ms | 500+ | 200+ | - | Write performance |
| thread_pool_write_rejections | 1+ | - | - | Write overload |
| thread_pool_search_rejections | 1+ | - | - | Search overload |
| thread_pool_queue_depth | - | 100+ | - | Capacity warning |
| query_cache_hit_ratio_percent | - | - | <50% | Cache efficiency |
| request_cache_hit_ratio_percent | - | - | <30% | Aggregation caching |
| search_query_total | - | 1M+ | - | Query volume |

**Key Thresholds:**
- Search latency: Critical 2s+, High 1s+, Medium 500ms+
- Indexing latency: Critical 500ms+, High 200ms+
- Thread rejections: Critical at any occurrence
- Cache hit ratios: Medium below 50% (query), 30% (request)

---

### 5. check_cluster_settings.json (8 metrics)

Audits cluster topology, configuration, and production readiness.

| Metric | Critical | High | Medium | Focus Area |
|--------|----------|------|--------|------------|
| master_node_count | 1 | 2 | - | Split-brain prevention |
| data_node_count | 1 | - | - | Data redundancy |
| shards_per_node | 1000+ | 600+ | - | Cluster state bloat |
| index_count | - | 1000+ | 500+ | Management overhead |
| unassigned_shards | 1+ | - | - | Allocation issues |
| pending_tasks | - | 10+ | - | Master bottleneck |
| active_shards_percent | <90% | - | - | Allocation health |
| relocating_shards | - | 10+ | - | Rebalancing activity |

**Key Thresholds:**
- Master nodes: Critical at 1, High at 2 (need 3+ for quorum)
- Data nodes: Critical at 1 (no redundancy)
- Shards per node: Critical 1000+, High 600+
- Indices: High 1000+, Medium 500+

---

### 6. check_disk_usage.json (6 metrics)

Monitors disk space, I/O performance, and storage health.

| Metric | Critical | High | Medium | Focus Area |
|--------|----------|------|--------|------------|
| disk_usage_percent | 95% | 90% | 85% | Flood/high/low watermarks |
| data_directory_usage_percent | 95% | 90% | - | Data directory space |
| available_disk_gb | <10GB | <50GB | - | Absolute space remaining |
| disk_io_await_ms | 100+ | 50+ | - | Storage latency |
| disk_io_utilization_percent | 95% | 80% | - | I/O saturation |
| inode_usage_percent | 90% | - | - | Filesystem inodes |

**Key Thresholds:**
- Disk usage: Critical 95% (flood), High 90% (high watermark), Medium 85% (low watermark)
- Available space: Critical <10GB, High <50GB
- I/O latency: Critical 100ms+, High 50ms+
- I/O utilization: Critical 95%+, High 80%+

---

### 7. check_aws_service_software.json (11 metrics)

AWS OpenSearch Service-specific monitoring (only runs in AWS environment).

| Metric | Critical | High | Medium | Focus Area |
|--------|----------|------|--------|------------|
| dedicated_masters_disabled | ✓ | - | - | Cluster stability |
| multi_az_disabled | ✓ | - | - | High availability |
| https_not_enforced | ✓ | - | - | Security |
| not_in_vpc | ✓ | - | - | Network security |
| service_software_update_required | ✓ | - | - | Mandatory updates |
| service_software_update_available | - | ✓ | - | Optional updates |
| autotune_disabled | - | ✓ | - | Performance optimization |
| outdated_tls_policy | - | ✓ | - | Security compliance |
| insufficient_instance_count | - | ✓ | - | Redundancy |
| master_instance_undersized | - | ✓ | - | Master capacity |
| data_instance_undersized | - | ✓ | - | Data capacity |

**Key Thresholds:**
- Production readiness: Critical for missing dedicated masters, Multi-AZ, VPC, HTTPS
- Required updates: Critical (AWS may force)
- Available updates: High priority
- Auto-Tune: High (recommended by AWS)

---

## Rule Application

### How Rules Work

1. **Health checks collect metrics** from the cluster via REST API, SSH, or CloudWatch
2. **Metrics are compared against rule thresholds** defined in JSON files
3. **Triggered rules are flagged** by severity (critical, high, medium)
4. **Recommendations are provided** based on the reasoning and action items in rules
5. **AI analysis** (if enabled) uses triggered rules to prioritize issues

### Rule Structure

Each rule includes:
```json
{
  "metric_name": {
    "critical": {
      "threshold": 90,
      "reasoning": "Why this threshold matters",
      "recommendations": [
        "Specific action 1",
        "Specific action 2"
      ]
    }
  }
}
```

### OpenSearch-Specific Considerations

**Disk Watermarks (OpenSearch defaults):**
- Low watermark: 85% (stops replica allocation)
- High watermark: 90% (stops all shard allocation)
- Flood watermark: 95% (blocks writes, sets indices read-only)

**Shard Limits:**
- Recommended: <1000 shards per node
- Optimal: 10-50GB per shard

**JVM Heap:**
- Maximum: 32GB (compressed OOPs limit)
- Typical: 50% of system RAM, capped at 32GB

**Master Nodes:**
- Minimum: 3 (for quorum)
- Always odd number (3, 5, or 7)

---

## Monitoring Integration

### Trend Analysis

All triggered rules are stored in the PostgreSQL trend database:
- Historical tracking of issues over time
- Identification of recurring problems
- Capacity planning based on growth trends

### AI Analysis

When AI analysis is enabled (`ai_analyze: true`), the system:
1. Groups triggered rules by severity
2. Provides pre-analysis summary to AI
3. AI correlates issues across different metrics
4. Generates prioritized recommendations

---

## Adding Custom Rules

To add custom alerting rules:

1. **Edit existing rule files** or create new ones in `plugins/opensearch/rules/`
2. **Follow the JSON structure:**
   ```json
   {
     "your_metric_name": {
       "critical": {
         "threshold": 100,
         "reasoning": "Why this is critical",
         "recommendations": [
           "What to do about it"
         ]
       }
     }
   }
   ```

3. **Ensure metrics match** what health checks actually measure
4. **Test rule loading:**
   ```bash
   python -c "from plugins.opensearch import OpenSearchPlugin; print(OpenSearchPlugin().get_rules_config())"
   ```

---

## Rule File Maintenance

- **Review quarterly:** Adjust thresholds based on operational experience
- **Update for new versions:** OpenSearch version changes may require threshold adjustments
- **Customize for workload:** Time-series vs analytical workloads have different optimal settings
- **Validate JSON:** Use `python -m json.tool < rule_file.json` to check syntax

---

**Last Updated:** 2025-10-31
**Plugin Version:** 1.0
**OpenSearch Versions Supported:** 1.x, 2.x, 3.x

# OpenSearch Diagnostic Queries Guide

## Overview

The OpenSearch plugin includes advanced diagnostic capabilities for troubleshooting performance issues, identifying bottlenecks, and understanding cluster behavior. These diagnostics go beyond standard health checks to provide deep insights into cluster operations.

## Diagnostic Check Module

**Module:** `plugins/opensearch/checks/check_diagnostics.py`
**Report Section:** Advanced Diagnostics
**Weight:** 5

This module runs **7 diagnostic queries** that provide troubleshooting information not typically included in routine health checks.

---

## 1. Hot Threads Analysis

**API:** `GET /_nodes/hot_threads`

### What It Does
Identifies threads consuming the most CPU time across all nodes. This is crucial for troubleshooting performance issues and identifying what operations are CPU-intensive.

### When To Use
- During periods of high CPU utilization
- When investigating slow query performance
- To identify runaway operations or infinite loops
- When cluster response time is degraded

### Output
Shows active threads sorted by CPU usage, including:
- Thread name and type
- CPU usage percentage
- Stack trace of what the thread is doing
- Duration the thread has been running

### Interpretation
- **No hot threads:** Normal operation, CPU usage is distributed
- **Hot threads detected:** Review the stack traces to identify:
  - Expensive queries or aggregations
  - Long-running searches
  - Heavy indexing operations
  - Cluster state updates
  - Snapshot/restore operations

---

## 2. Pending Cluster Tasks Detail

**API:** `GET /_cluster/pending_tasks`

### What It Does
Shows cluster state update tasks waiting to be processed by the master node. These are operations that modify cluster metadata (index creation, mapping updates, shard allocation, etc.).

### When To Use
- When cluster feels slow or unresponsive
- During index creation or mapping changes
- When troubleshooting master node performance
- To understand cluster state update bottlenecks

### Output
For each pending task:
- **Priority:** Task priority level (URGENT, HIGH, NORMAL, LOW)
- **Source:** What triggered the task (e.g., "create-index", "put-mapping")
- **Time in Queue:** How long the task has been waiting
- **Executing:** Whether it's currently being processed

### Interpretation
- **0 pending tasks:** Master node keeping up with cluster state changes
- **1-10 tasks:** Normal during active operations
- **10-50 tasks:** Master node under load, monitor closely
- **50+ tasks:** Master node bottleneck - immediate attention required

**Common Causes:**
- Undersized master nodes
- Too many indices/shards (complex cluster state)
- Frequent mapping updates
- Heavy snapshot/restore operations

---

## 3. Index Segment Analysis

**API:** `GET /_cat/segments?format=json`

### What It Does
Provides detailed information about Lucene segments for each shard. Segments are the building blocks of indices - too many small segments can degrade search performance.

### When To Use
- When search performance degrades over time
- After heavy indexing periods
- To plan force merge operations
- During index optimization reviews

### Output
- Segment count per index
- Segment sizes
- Memory usage per segment
- Merge candidates

### Interpretation
- **<50 segments per index:** Healthy
- **50-100 segments:** Consider force merging read-only indices
- **100-200 segments:** Should force merge
- **200+ segments:** Performance impact, force merge urgently

**Force Merge Recommendations:**
```json
POST /index_name/_forcemerge?max_num_segments=1
```

**Note:** Only force merge read-only indices (no active writes)

---

## 4. Shard Recovery Status

**API:** `GET /_cat/recovery?format=json`

### What It Does
Shows all shard recovery operations in progress, including:
- Replica initialization
- Shard relocation (rebalancing)
- Snapshot restore operations
- Primary shard recovery after node failure

### When To Use
- During/after node restarts
- When adding/removing nodes
- After snapshot restore
- When investigating high I/O or network usage
- To monitor cluster rebalancing

### Output
For each recovery:
- **Index & Shard:** Which shard is recovering
- **Stage:** Current recovery stage (index, translog, finalize, done)
- **Type:** Recovery type (replica, relocation, snapshot, peer)
- **Source/Target Nodes:** From where to where
- **Progress:** Percentage complete
- **Bytes Transferred:** Data copied so far

### Stages
1. **index:** Copying segment files
2. **translog:** Replaying transaction log
3. **finalize:** Final verification
4. **done:** Recovery complete

### Interpretation
- **No active recoveries:** Cluster stable
- **Active recoveries:** Normal after topology changes
- **Stuck recoveries:** Check network, disk I/O, or throttling settings

**Throttling Settings:**
```json
PUT /_cluster/settings
{
  "transient": {
    "indices.recovery.max_bytes_per_sec": "100mb"
  }
}
```

---

## 5. Long-Running Tasks

**API:** `GET /_tasks?detailed=true`

### What It Does
Lists all currently running tasks across the cluster, identifying operations that have been running for extended periods (>30 seconds).

### When To Use
- During performance degradation
- When cluster seems "stuck"
- To identify resource-intensive operations
- When troubleshooting timeouts

### Output
For each long-running task:
- **Node:** Where the task is running
- **Action:** What operation is being performed
- **Running Time:** How long it's been executing
- **Description:** Details about the operation

### Common Long-Running Tasks
- **search:** Complex queries or large result sets
- **indices:data/write/bulk:** Large bulk indexing
- **snapshot:** Snapshot creation operations
- **cluster:admin/reindex:** Reindex operations
- **indices:admin/forcemerge:** Force merge in progress

### Interpretation
- Tasks >30 seconds: Review for optimization
- Tasks >5 minutes: Investigate immediately
- Multiple long-running tasks: Cluster capacity issue

**Cancel a Task:**
```json
POST /_tasks/node_id:task_id/_cancel
```

---

## 6. Installed Plugins

**API:** `GET /_cat/plugins?format=json`

### What It Does
Lists all OpenSearch plugins installed on each node, helping understand cluster capabilities and verify consistent plugin deployment.

### When To Use
- During cluster audits
- After plugin installations
- To verify plugin version consistency
- When troubleshooting plugin-related issues

### Output
- **Plugin Name:** Name and type
- **Nodes:** How many nodes have this plugin
- **Version:** Plugin version

### Common Plugins
**Security & Access Control:**
- `opensearch-security`: Authentication, authorization, encryption

**Management & Operations:**
- `opensearch-index-management`: ISM policies, rollover, snapshots
- `opensearch-job-scheduler`: Scheduled jobs
- `opensearch-notifications`: Alerting notifications

**Search & Analytics:**
- `opensearch-sql`: SQL query support
- `opensearch-anomaly-detection`: ML-based anomaly detection
- `opensearch-observability`: Trace analytics, PPL

**Storage:**
- `repository-s3`: S3 snapshot repository
- `repository-azure`: Azure snapshot repository
- `repository-gcs`: Google Cloud Storage repository

**Other:**
- `opensearch-geospatial`: Geospatial query support
- `opensearch-reports-scheduler`: Report generation
- `mapper-size`: Index `_size` field support

### Interpretation
- **Consistent versions:** Good - all nodes have same plugins
- **Missing plugins:** Node may have been added without proper setup
- **Version mismatches:** Could cause compatibility issues

---

## 7. Field Data Memory Usage

**API:** `GET /_nodes/stats/indices?fields=fielddata`

### What It Does
Shows memory consumed by field data structures. Field data loads field values into memory for sorting, aggregations, and scripting. High usage indicates inefficient queries.

### When To Use
- During high heap usage
- When investigating memory pressure
- To optimize aggregation queries
- Before/after query optimizations

### Output
- **Memory Used:** Bytes consumed per node
- **Evictions:** How often field data is evicted from cache
- **Status:** OK or needs review

### Interpretation
- **<1GB:** Normal for most workloads
- **1-5GB:** Review query patterns, consider doc values
- **>5GB:** High usage - optimization needed

### Recommendations for High Usage
1. **Use doc values instead of field data** (default for most fields)
2. **Reduce aggregation cardinality** (fewer unique values)
3. **Limit aggregation depth** (nested aggregations)
4. **Increase field data cache size** (if justified)
5. **Review scripting usage** (scripts load field data)

**Field Data Circuit Breaker:**
```json
PUT /_cluster/settings
{
  "persistent": {
    "indices.breaker.fielddata.limit": "40%"
  }
}
```

---

## Diagnostic Workflow

### For Performance Issues

1. **Check Hot Threads** → Identify CPU-intensive operations
2. **Check Long-Running Tasks** → Find slow operations
3. **Check Segment Counts** → Optimize if high
4. **Check Field Data** → Review query efficiency

### For Cluster State Issues

1. **Check Pending Tasks** → Identify master bottlenecks
2. **Check Recovery Status** → Monitor shard movements
3. **Check Plugins** → Verify consistent deployment

### For Capacity Planning

1. **Check Segment Counts** → Plan force merges
2. **Check Field Data** → Assess memory needs
3. **Check Recovery Times** → Estimate rebalancing impact

---

## Integration with Other Checks

The diagnostic check complements standard health checks:

| Diagnostic Query | Related Health Check | Combined Insight |
|------------------|---------------------|------------------|
| Hot Threads | Performance Metrics | Correlate CPU usage with query latency |
| Pending Tasks | Cluster Settings | Master node capacity vs. cluster complexity |
| Segments | Index Health | Index optimization opportunities |
| Recovery | Cluster Health | Rebalancing impact on operations |
| Tasks | Performance | Identify specific slow operations |
| Plugins | Cluster Settings | Feature availability and consistency |
| Field Data | Node Metrics | Memory pressure root causes |

---

## API Reference - All Diagnostic Endpoints

```bash
# Hot Threads
GET /_nodes/hot_threads
GET /_nodes/hot_threads?type=cpu&interval=500ms&snapshots=10

# Pending Tasks
GET /_cluster/pending_tasks

# Segments
GET /_cat/segments?v
GET /_cat/segments?format=json&bytes=b

# Recovery
GET /_cat/recovery?v
GET /_cat/recovery?format=json&active_only=true

# Tasks
GET /_tasks
GET /_tasks?detailed=true&actions=*search*
POST /_tasks/node_id:task_id/_cancel

# Plugins
GET /_cat/plugins?v
GET /_cat/plugins?format=json

# Field Data
GET /_nodes/stats/indices/fielddata
GET /_cluster/settings?include_defaults=true&filter_path=**.breaker.fielddata.*
```

---

## Troubleshooting Common Issues

### High CPU Usage
1. Run **Hot Threads** to identify CPU consumers
2. Check for expensive queries or aggregations
3. Review search patterns with **Tasks API**

### Slow Cluster Operations
1. Check **Pending Tasks** for master bottleneck
2. Review **Long-Running Tasks**
3. Check **Recovery Status** for rebalancing impact

### Memory Pressure
1. Check **Field Data Usage**
2. Review JVM heap from Node Metrics
3. Optimize queries to use doc values

### Poor Search Performance
1. Check **Segment Counts** - force merge if high
2. Review **Hot Threads** during peak load
3. Check **Field Data** for inefficient aggregations

---

**Detected on Your Cluster (Instaclustr OpenSearch 3.2.0):**

✅ **15 plugins installed:**
- Security (opensearch-security)
- Anomaly Detection (opensearch-anomaly-detection)
- Index Management (opensearch-index-management)
- SQL Support (opensearch-sql)
- Observability (opensearch-observability)
- Geospatial (opensearch-geospatial)
- And 9 more...

✅ **Segment health:** 83 total segments across all indices
✅ **No pending tasks** - master node keeping up
✅ **No active recoveries** - cluster stable
✅ **No long-running tasks** - operations completing promptly
✅ **Minimal field data usage** - efficient queries

---

**Last Updated:** 2025-10-31
**Plugin Version:** 1.0
**OpenSearch Versions:** 1.x, 2.x, 3.x

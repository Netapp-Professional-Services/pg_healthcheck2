# OpenSearch Plugin Architecture

## Query Architecture - Why No qrylib Directory?

### TL;DR
**OpenSearch doesn't need `utils/qrylib/` files because it uses REST APIs instead of SQL/CQL queries.**

---

## Architectural Comparison

### Traditional Database Plugins (Cassandra, Postgres, MySQL)

These databases use **query languages** (SQL, CQL) that require explicit query strings:

```
plugins/cassandra/
├── utils/
│   └── qrylib/                           ← Query library with SQL/CQL strings
│       ├── qry_java_heap_usage.py        ← "SELECT * FROM system.heap..."
│       ├── qry_compaction_pending.py     ← "SELECT compaction_stats..."
│       ├── qry_disk_space.py             ← "SELECT disk_usage..."
│       └── ... (20+ query files)
├── checks/
│   └── check_heap.py                     ← Imports query from qrylib
└── connector.py                          ← Executes CQL statements
```

**How it works:**
1. Define query string in qrylib file
2. Import query in check module
3. Pass query string to connector
4. Connector executes via database driver

---

### OpenSearch Plugin (REST API-based)

OpenSearch uses **REST APIs** provided by the `opensearch-py` client library:

```
plugins/opensearch/
├── connector.py                          ← REST API dispatcher
├── checks/
│   ├── cluster_health_check.py           ← Uses REST operations
│   ├── check_node_metrics.py             ← Uses REST operations
│   └── check_diagnostics.py              ← Uses REST operations
└── No qrylib needed!                     ← All APIs built into opensearch-py
```

**How it works:**
1. Check module calls: `connector.execute_query({"operation": "cluster_health"})`
2. Connector dispatches to opensearch-py client method
3. Client library makes REST API call: `client.cluster.health()`
4. Returns JSON response

---

## All Supported Operations (Our "Queries")

The OpenSearch plugin supports **15 REST API operations** (equivalent to query files in other plugins):

### Core Cluster Operations
1. **cluster_health** - Overall cluster health status
   ```python
   connector.execute_query({"operation": "cluster_health"})
   # Maps to: GET /_cluster/health
   ```

2. **cluster_stats** - Cluster-wide statistics
   ```python
   connector.execute_query({"operation": "cluster_stats"})
   # Maps to: GET /_cluster/stats
   ```

### Node Operations
3. **cat_nodes** - List all nodes with basic stats
   ```python
   connector.execute_query({"operation": "cat_nodes"})
   # Maps to: GET /_cat/nodes?format=json
   ```

4. **node_stats** - Detailed node statistics (JVM, GC, thread pools, etc.)
   ```python
   connector.execute_query({"operation": "node_stats", "node_id": "_all"})
   # Maps to: GET /_nodes/_all/stats
   ```

### Index Operations
5. **cat_indices** - List all indices
   ```python
   connector.execute_query({"operation": "cat_indices"})
   # Maps to: GET /_cat/indices?format=json
   ```

6. **index_stats** - Detailed index statistics
   ```python
   connector.execute_query({"operation": "index_stats", "index": "_all"})
   # Maps to: GET /_stats
   ```

### Shard Operations
7. **cat_shards** - Shard allocation details
   ```python
   connector.execute_query({"operation": "cat_shards"})
   # Maps to: GET /_cat/shards?format=json
   ```

8. **cat_allocation** - Shard allocation across nodes
   ```python
   connector.execute_query({"operation": "cat_allocation"})
   # Maps to: GET /_cat/allocation?format=json
   ```

### Diagnostic Operations
9. **hot_threads** - CPU-intensive operations
   ```python
   connector.execute_query({"operation": "hot_threads"})
   # Maps to: GET /_nodes/hot_threads
   ```

10. **pending_tasks** - Cluster state update queue
    ```python
    connector.execute_query({"operation": "pending_tasks"})
    # Maps to: GET /_cluster/pending_tasks
    ```

11. **cat_segments** - Lucene segment information
    ```python
    connector.execute_query({"operation": "cat_segments"})
    # Maps to: GET /_cat/segments?format=json
    ```

12. **cat_recovery** - Shard recovery status
    ```python
    connector.execute_query({"operation": "cat_recovery"})
    # Maps to: GET /_cat/recovery?format=json
    ```

13. **tasks** - Long-running tasks
    ```python
    connector.execute_query({"operation": "tasks"})
    # Maps to: GET /_tasks?detailed=true
    ```

14. **cat_plugins** - Installed plugins inventory
    ```python
    connector.execute_query({"operation": "cat_plugins"})
    # Maps to: GET /_cat/plugins?format=json
    ```

### System Operations
15. **shell** - Execute OS-level commands via SSH
    ```python
    connector.execute_query({"operation": "shell", "command": "df -h"})
    # Executes via SSH on cluster nodes
    ```

---

## Coverage Comparison

### Cassandra Plugin (23 query files)
- Heap usage queries
- Compaction stats queries
- Disk space queries
- GC statistics queries
- Thread pool queries
- etc.

### OpenSearch Plugin (15 REST operations)
✅ Heap usage - `node_stats` (JVM metrics)
✅ Compaction equivalent - `cat_segments` (segment merging)
✅ Disk space - `node_stats` + SSH `shell` operations
✅ GC statistics - `node_stats` (GC collectors)
✅ Thread pools - `node_stats` (thread pool stats)
✅ Plus 7 diagnostic operations not available in Cassandra

**We have EQUAL or BETTER coverage without needing query files!**

---

## How Checks Use Operations

Example from `check_node_metrics.py`:

```python
def run_check_node_metrics(connector, settings):
    # Get comprehensive node statistics
    node_stats_result = connector.execute_query({
        "operation": "node_stats",
        "node_id": "_all"
    })

    # Process results
    for node_id, node_data in node_stats_result['nodes'].items():
        # Extract metrics
        heap_used = node_data['jvm']['mem']['heap_used_in_bytes']
        heap_max = node_data['jvm']['mem']['heap_max_in_bytes']
        heap_percent = (heap_used / heap_max) * 100

        # Check against alert thresholds
        if heap_percent > 85:
            # Trigger CRITICAL alert
            ...
```

**No query strings needed** - everything is REST API calls with JSON responses!

---

## Advantages of REST API Architecture

### 1. **No Query String Management**
- No need to maintain SQL/CQL query strings
- No query versioning issues
- No query syntax differences between versions

### 2. **Strongly Typed**
- opensearch-py client provides method signatures
- IDE autocomplete for all operations
- Type hints and documentation built-in

### 3. **Simplified Testing**
- Mock REST responses easily
- No need to mock database cursors/sessions
- Can use recorded responses for tests

### 4. **Version Agnostic**
- REST APIs are more stable across versions
- Client library handles API differences
- Works with OpenSearch 1.x, 2.x, 3.x

### 5. **Self-Documenting**
- Operation names clearly indicate what they do
- `{"operation": "cluster_health"}` is self-explanatory
- No need to read query strings to understand intent

---

## Adding New Operations

To add a new operation (equivalent to adding a new qrylib file):

### 1. Add to connector.py dispatch:

```python
elif operation == 'new_operation':
    # Call appropriate opensearch-py method
    return self.client.cluster.new_method()
```

### 2. Use in check module:

```python
result = connector.execute_query({"operation": "new_operation"})
```

That's it! No separate query file needed.

---

## When Would You Need qrylib?

You would need a `utils/qrylib/` directory if:

1. **Custom DSL Queries** - Complex OpenSearch Query DSL aggregations
2. **Stored Scripts** - Reusable Painless scripts
3. **Complex Search Templates** - Parameterized search queries

For basic cluster health monitoring, REST APIs are sufficient and cleaner.

---

## Summary

| Aspect | Cassandra/Postgres | OpenSearch |
|--------|-------------------|------------|
| **Query Method** | SQL/CQL strings | REST API calls |
| **Query Storage** | `utils/qrylib/*.py` files | Built into opensearch-py |
| **Query Execution** | Database driver | HTTP client |
| **Maintainability** | Requires query files | Self-contained |
| **Coverage** | ~20-25 queries | 15 operations |
| **Extensibility** | Add new .py file | Add elif clause |

**Conclusion:** OpenSearch's REST API architecture is actually **cleaner and more maintainable** than requiring separate query files. All "queries" are covered through the 15 supported operations.

---

## Quick Reference

**Want to see all available operations?**
```bash
grep "elif operation ==" plugins/opensearch/connector.py | awk -F"'" '{print $2}'
```

**Want to see how they're used?**
```bash
grep -r "execute_query.*operation" plugins/opensearch/checks/
```

**Want API documentation?**
- [OpenSearch REST API Reference](https://opensearch.org/docs/latest/api-reference/)
- [opensearch-py Client Documentation](https://opensearch-project.github.io/opensearch-py/)

---

**Last Updated:** 2025-10-31
**Plugin Version:** 1.0
**OpenSearch Versions Supported:** 1.x, 2.x, 3.x

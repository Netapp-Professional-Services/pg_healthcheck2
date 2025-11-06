# ClickHouse Health Check Testing Guide

Comprehensive guide for testing ClickHouse health checks using the test data generator.

---

## Overview

The ClickHouse test suite provides tools to:
- Generate test data with various table configurations
- Exercise all health checks (SQL-based and SSH-based)
- Validate check behavior with known scenarios
- Load test the cluster with realistic analytics workloads
- Clean up test data

---

## Prerequisites

### Required Dependencies

```bash
pip install clickhouse-connect faker pyyaml
```

### Configuration

You need a valid ClickHouse configuration file. Examples:

**Instaclustr Managed:**
```bash
config/clickhouse_ic_test.yaml
```

**Self-Hosted with SSH:**
```bash
config/clickhouse_local_SSH.yaml
```

**Self-Hosted without SSH:**
```bash
config/clickhouse_selfmanaged_SSH.yaml
```

---

## Test Data Generator

### Basic Usage

```bash
cd plugins/clickhouse/test-data

# Run all scenarios
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario all

# Run specific scenario
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario optimal

# Load test with custom count
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario analytics --count 100000
```

---

## Test Scenarios

### 1. Optimal Configuration Scenario

**Purpose:** Create tables with best-practice configurations

**What It Creates:**
- `test_optimal` database
- Tables with optimal settings:
  - Proper ORDER BY clauses
  - Monthly partitioning for time-series data
  - LowCardinality for categorical columns
  - Appropriate index granularity (8192)
  - Meaningful primary keys

**Tables Created:**
1. **user_profiles** - MergeTree with proper ordering
2. **events** - Partitioned by month with optimal ordering

**Health Checks Expected:**
- ✅ **Table Health:** Reports 2 tables with optimal configurations
- ✅ **Node Metrics:** Shows data distribution
- ✅ **Disk Usage:** Reports storage usage

**Run It:**
```bash
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario optimal
```

**Expected Output:**
```
✅ Connected to ClickHouse 25.3.6.56
   Host: ip-3-208-112-189.4d30eaf521f04119b5fd12a161b6a1e8.cnodes.io:8443
✅ Created database: test_optimal
  Creating optimal MergeTree table...
  Creating optimal partitioned events table...
  Inserting test data...
  ✅ Optimal scenario complete

📊 Execution Statistics
Duration:           15.23 seconds
Databases created:  1
Tables created:     2
Rows inserted:      1,000
```

---

### 2. Problematic Configuration Scenario

**Purpose:** Create tables with known issues to trigger warnings

**What It Creates:**
- `test_problematic` database
- Tables with issues:
  - Empty ORDER BY (tuple()) - anti-pattern
  - Poor index granularity (256 instead of 8192)
  - Many columns (50+) - potential performance issue

**Health Checks Expected:**
- ⚠️ **Table Health:** Flags tables with tuple() ORDER BY
- ⚠️ **Configuration:** Identifies non-standard settings
- ⚠️ **Table Health:** Reports tables with many columns

**Run It:**
```bash
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario problematic
```

**Validate:**
```bash
# Run health check
cd ../../..
python main.py --config config/clickhouse_ic_test.yaml

# Look for warnings in output:
# - "Table uses tuple() ORDER BY - poor query performance"
# - "Table has non-standard index_granularity"
# - "Table has 50+ columns - consider denormalization"
```

---

### 3. Mixed Engines Scenario

**Purpose:** Test detection of various MergeTree engine variants

**What It Creates:**
- `test_engines` database
- Tables with different engines:
  - **ReplacingMergeTree** - For deduplication
  - **SummingMergeTree** - For pre-aggregation
  - **AggregatingMergeTree** - For complex aggregations
  - **CollapsingMergeTree** - For state changes

**Health Checks Expected:**
- ✅ **Table Health:** Reports 4 different engine types
- ✅ **System Metrics:** Shows engine distribution

**Run It:**
```bash
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario mixed_engines
```

**Validate:**
```sql
-- Query engine types:
SELECT
    engine,
    count() as table_count
FROM system.tables
WHERE database = 'test_engines'
GROUP BY engine;

-- Expected results:
-- ReplacingMergeTree    1
-- SummingMergeTree      1
-- AggregatingMergeTree  1
-- CollapsingMergeTree   1
```

---

### 4. Time-Series Data Scenario

**Purpose:** Create realistic time-series/metrics tables

**What It Creates:**
- `test_time_series` database
- **metrics** table with:
  - DateTime timestamp
  - LowCardinality columns for dimensions
  - Monthly partitioning
  - TTL of 30 days
  - Map(String, String) for tags

**Health Checks Expected:**
- ✅ **Table Health:** Reports partitioned table with TTL
- ✅ **Disk Usage:** Shows partition sizes
- ✅ **Query Performance:** Can analyze time-series queries

**Run It:**
```bash
# Default 50K rows
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario time_series

# More data (500K rows)
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario time_series --count 500000
```

**Expected Performance:**
- Instaclustr Managed: ~10,000-20,000 rows/sec
- Self-hosted (SSD): ~20,000-50,000 rows/sec

**Validate:**
```sql
-- Check partition distribution
SELECT
    partition,
    count() as row_count,
    formatReadableSize(sum(bytes_on_disk)) as size
FROM system.parts
WHERE database = 'test_time_series'
  AND table = 'metrics'
  AND active
GROUP BY partition
ORDER BY partition;
```

---

### 5. Analytics Workload Scenario

**Purpose:** Insert large volume of realistic analytics/web tracking data

**What It Creates:**
- `test_analytics` database
- **page_views** table with:
  - User sessions and page views
  - User-agent strings
  - Geographic data (country codes)
  - Device types
  - Referrer tracking
  - Monthly partitioning

**Health Checks Expected:**
- ✅ Tests check performance with large datasets
- ✅ Exercises query performance analysis
- ✅ Validates compression efficiency

**Run It:**
```bash
# Default 100K rows
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario analytics

# Heavy load (1M rows)
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario analytics --count 1000000
```

**Expected Performance:**
- Small cluster: ~5,000-10,000 rows/sec
- Large cluster: ~20,000-50,000 rows/sec

**Use Cases:**
```sql
-- Top pages by traffic
SELECT
    page_url,
    count() as page_views,
    count(DISTINCT user_id) as unique_users
FROM test_analytics.page_views
GROUP BY page_url
ORDER BY page_views DESC
LIMIT 10;

-- Device distribution
SELECT
    device_type,
    count() as views,
    avg(duration_ms) / 1000 as avg_duration_seconds
FROM test_analytics.page_views
GROUP BY device_type;

-- Geographic distribution
SELECT
    country,
    count() as page_views
FROM test_analytics.page_views
GROUP BY country
ORDER BY page_views DESC;
```

---

### 6. Materialized Views Scenario

**Purpose:** Test materialized view detection and monitoring

**What It Creates:**
- `test_views` database
- **raw_events** base table
- **hourly_stats** materialized view with SummingMergeTree
- Aggregates events by hour and type

**Health Checks Expected:**
- ✅ **Table Health:** Detects materialized views
- ✅ **System Metrics:** Reports view refresh status
- ✅ **Query Performance:** Tracks view query efficiency

**Run It:**
```bash
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario materialized_views
```

**Validate:**
```sql
-- Check materialized view
SELECT * FROM test_views.hourly_stats
ORDER BY hour DESC
LIMIT 10;

-- Verify view is populated
SELECT
    count() as raw_events,
    (SELECT count() FROM test_views.hourly_stats) as aggregated_hours
FROM test_views.raw_events;
```

---

### 7. Dictionaries Scenario

**Purpose:** Test dictionary creation and monitoring

**What It Creates:**
- `test_dictionaries` database
- **country_codes** source table with country data
- **country_dict** flat dictionary
- Dictionary with FLAT layout and 5-minute refresh

**Health Checks Expected:**
- ✅ **Dictionary Health:** Detects dictionaries
- ✅ **Dictionary Monitoring:** Shows load status
- ✅ **Configuration:** Reports dictionary settings

**Run It:**
```bash
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario dictionaries
```

**Note:** Dictionary creation may not be supported on all managed services (e.g., Instaclustr)

**Validate:**
```sql
-- Query dictionary
SELECT
    code,
    dictGet('test_dictionaries.country_dict', 'name', code) as country_name,
    dictGet('test_dictionaries.country_dict', 'continent', code) as continent
FROM test_dictionaries.country_codes
LIMIT 5;

-- Check dictionary status
SELECT
    name,
    status,
    element_count,
    load_exception
FROM system.dictionaries
WHERE database = 'test_dictionaries';
```

---

### 8. Partitioned Tables Scenario

**Purpose:** Test partition management and monitoring

**What It Creates:**
- `test_partitions` database
- Tables with different partition strategies:
  - **monthly_data** - Partitioned by month (toYYYYMM)
  - **daily_data** - Partitioned by day (toYYYYMMDD) with 7-day TTL
  - **hierarchical_data** - Multi-level partitioning (month + country)

**Health Checks Expected:**
- ✅ **Table Health:** Detects partitioning strategies
- ✅ **Disk Usage:** Shows partition sizes
- ✅ **Table Health:** Reports TTL configurations

**Run It:**
```bash
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario partitioned_tables
```

**Validate:**
```sql
-- Check partition counts
SELECT
    database,
    table,
    count() as partition_count
FROM system.parts
WHERE database = 'test_partitions'
  AND active
GROUP BY database, table;

-- Check TTL tables
SELECT
    database,
    table,
    ttl_expression
FROM system.tables
WHERE database = 'test_partitions'
  AND ttl_expression != '';
```

---

## Complete Testing Workflow

### Step 1: Generate Test Data

```bash
cd plugins/clickhouse/test-data

# Create all test scenarios
python clickhouse_test_data_generator.py \
    --config ../../../config/clickhouse_ic_test.yaml \
    --scenario all
```

**Output:**
```
🚀 Running ALL scenarios...

============================================================
Scenario: optimal
============================================================
✅ Created database: test_optimal
  ✅ Optimal scenario complete

============================================================
Scenario: problematic
============================================================
✅ Created database: test_problematic
  ✅ Problematic scenario complete

[... continues for all scenarios ...]

📊 Execution Statistics
Duration:           245.67 seconds
Databases created:  8
Tables created:     18
Views created:      1
Dictionaries:       1
Rows inserted:      7,000
Errors:             0
Insert rate:        28.49 rows/sec
```

### Step 2: Run Health Checks

```bash
cd ../../..  # Back to project root

# Run health check
python main.py --config config/clickhouse_ic_test.yaml
```

### Step 3: Verify Check Results

**Check Table Health:**
```json
{
  "table_health": {
    "summary": {
      "total_tables": 18,
      "total_databases": 9,
      "engine_distribution": {
        "MergeTree": 10,
        "ReplacingMergeTree": 1,
        "SummingMergeTree": 2,
        "AggregatingMergeTree": 1,
        "CollapsingMergeTree": 1
      }
    },
    "problematic_tables": [
      {
        "database": "test_problematic",
        "table": "unordered_table",
        "issue": "Uses tuple() ORDER BY",
        "severity": "warning"
      }
    ]
  }
}
```

**Check Query Performance:**
```json
{
  "query_performance": {
    "summary": {
      "total_queries": 1523,
      "avg_duration_ms": 45.3,
      "p99_duration_ms": 234.5
    },
    "slow_queries": [],
    "detailed_analysis_available": false
  }
}
```

**Check Disk Usage:**
```json
{
  "disk_usage": {
    "total_bytes": 45678912,
    "formatted_size": "43.56 MB",
    "databases": [
      {
        "database": "test_analytics",
        "size": "25.34 MB",
        "table_count": 1
      },
      {
        "database": "test_time_series",
        "size": "12.45 MB",
        "table_count": 1
      }
    ]
  }
}
```

### Step 4: Test SSH Checks (if configured)

If SSH is enabled in your config:

```bash
# SSH checks should execute automatically:
# - OS System Metrics (CPU, memory, load)
# - OS Disk Usage (filesystem, directories)
# - OS Log Analysis (server logs, error patterns)
```

**Expected in report:**
```
=== OS System Metrics ===
✅ Collected system metrics from 3 nodes

Node: 192.168.1.113
  CPU: 8 cores, 2.4 GHz
  Memory: 31.2 GB total, 12.5 GB used (40%)
  Load Average: 1.2, 1.4, 1.3

[... for each node ...]
```

### Step 5: Cleanup Test Data

```bash
cd plugins/clickhouse/test-data

# List test databases
python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml

# Drop specific database
python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml --database test_optimal

# Drop all test databases (with confirmation)
python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml --all

# Drop all without confirmation (use with caution!)
python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml --all --confirm
```

**Cleanup Output:**
```
✅ Connected to ClickHouse

🗑️  Cleaning up ALL test databases...

  Found 8 test database(s):
    - test_optimal
    - test_problematic
    - test_engines
    - test_time_series
    - test_analytics
    - test_views
    - test_dictionaries
    - test_partitions

  Drop ALL 8 test database(s)? (yes/no): yes

  Database: test_optimal
    Tables: 2
    Total Size: 1.23 MB
    Total Rows: 1,000
  🗑️  Dropping database: test_optimal...
  ✅ Dropped test_optimal

[... continues for all databases ...]

  ✅ Cleanup complete: 8/8 databases dropped
```

---

## Validation Checklist

Use this checklist to verify all checks work correctly:

### SQL-Based Checks

- [ ] **Cluster Health**
  - [ ] Reports cluster UUID
  - [ ] Shows all nodes
  - [ ] Detects managed vs self-hosted
  - [ ] Reports ClickHouse version

- [ ] **Table Health**
  - [ ] Reports correct table counts per database
  - [ ] Detects all MergeTree engine variants
  - [ ] Identifies partitioned tables
  - [ ] Flags problematic configurations (tuple() ORDER BY)
  - [ ] Reports TTL configurations

- [ ] **Node Metrics**
  - [ ] Per-node statistics
  - [ ] CPU, memory, disk metrics
  - [ ] Query processing rates
  - [ ] Network throughput

- [ ] **Disk Usage**
  - [ ] Per-database size reporting
  - [ ] Partition size breakdowns
  - [ ] Compression ratios
  - [ ] Growth trends

- [ ] **Query Performance**
  - [ ] Query statistics (count, duration, failures)
  - [ ] Slow query detection
  - [ ] Failed query analysis
  - [ ] Resource-intensive query identification
  - [ ] ProfileEvents analysis (if available)

- [ ] **Error Tracking**
  - [ ] Recent errors from system.errors
  - [ ] Error frequency analysis
  - [ ] Error categorization

- [ ] **Backup Monitoring**
  - [ ] Backup status detection
  - [ ] Backup age analysis
  - [ ] Recommendations

- [ ] **Configuration**
  - [ ] Server settings analysis
  - [ ] Configuration drift detection
  - [ ] Non-default setting identification

- [ ] **Dictionary Health** (if applicable)
  - [ ] Dictionary detection
  - [ ] Load status
  - [ ] Refresh monitoring

### SSH-Based Checks (if configured)

- [ ] **OS System Metrics**
  - [ ] CPU information per node
  - [ ] Memory usage statistics
  - [ ] Load average
  - [ ] File descriptor usage
  - [ ] I/O statistics
  - [ ] Kernel parameters

- [ ] **OS Disk Usage**
  - [ ] Filesystem usage (df -h)
  - [ ] ClickHouse directory sizes
  - [ ] Inode usage
  - [ ] Mount point detection

- [ ] **OS Log Analysis**
  - [ ] Server log analysis
  - [ ] Keeper log analysis (if applicable)
  - [ ] Error pattern detection
  - [ ] Critical message extraction

---

## Troubleshooting

### Connection Issues

**Problem:** `❌ Connection failed: Could not connect to ClickHouse`

**Solution:**
1. Verify host in config file
2. Check network connectivity: `ping <host>`
3. Verify port (8123 for HTTP, 8443 for HTTPS, 9000 for native)
4. Check firewall rules
5. For Instaclustr, verify public IP access is enabled

### Authentication Errors

**Problem:** `Authentication failed`

**Solution:**
1. Verify credentials in config:
   ```yaml
   user: "icclickhouse"
   password: "your_password"
   ```
2. For Instaclustr, get credentials from console
3. Check if user has necessary permissions

### Database Already Exists

**Problem:** Some tests fail because databases exist

**Solution:**
```bash
# Clean up first
python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml --all --confirm

# Then run tests
python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario all
```

### Slow Inserts

**Problem:** Load test is very slow

**Possible Causes:**
1. Network latency (managed cluster in different region)
2. Write throttling on managed cluster
3. Small cluster size
4. Connection overhead (HTTP vs native protocol)

**Solutions:**
- Reduce count: `--count 10000`
- Use batching (already implemented)
- Switch to native protocol (port 9000/9440)
- Check cluster metrics in provider console

### Permission Errors

**Problem:** `Code 497. DB::Exception: ... Access denied`

**Solution:**
1. Verify user has CREATE DATABASE permission
2. For managed services, check role permissions
3. Try with admin/root user first

### SSH Connection Failures

**Problem:** SSH checks skip with "SSH not available"

**Solution:**
1. Verify `ssh_enabled: true` in config
2. Check `ssh_hosts` list
3. Test SSH manually: `ssh user@host`
4. Verify SSH key or password in config
5. Check `ssh_port` (default 22)

---

## Advanced Usage

### Custom Scenarios

You can extend the generator with custom scenarios:

```python
# Add to clickhouse_test_data_generator.py

def scenario_custom(self):
    """Your custom test scenario."""
    database = 'test_custom'
    self.create_test_database(database)

    # Create custom tables
    self.client.command(f"""
        CREATE TABLE {database}.your_table (
            id UUID,
            data String,
            timestamp DateTime
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, id)
    """)
    self.stats['tables_created'] += 1

    # Insert custom data
    # ... your logic ...
```

### Performance Benchmarking

```bash
# Benchmark different scenarios
for scenario in optimal time_series analytics; do
    echo "Testing $scenario..."
    time python clickhouse_test_data_generator.py \
        --config ../../../config/clickhouse_ic_test.yaml \
        --scenario $scenario \
        --count 100000
done
```

### Integration with CI/CD

```bash
#!/bin/bash
# ci_test_clickhouse.sh

# Generate test data
python plugins/clickhouse/test-data/clickhouse_test_data_generator.py \
    --config config/clickhouse_test.yaml \
    --scenario all

# Run health checks
python main.py --config config/clickhouse_test.yaml

# Verify no critical issues
if grep -q "🔴 Critical" adoc_out/*/health_check.adoc; then
    echo "Critical issues found!"
    exit 1
fi

# Cleanup
python plugins/clickhouse/test-data/cleanup_test_data.py \
    --config config/clickhouse_test.yaml \
    --all --confirm
```

---

## Summary

**Test Suite Features:**
- ✅ 8 comprehensive test scenarios
- ✅ Exercises all 15 health checks
- ✅ Compatible with SSH checks
- ✅ Realistic data generation
- ✅ Easy cleanup
- ✅ Detailed statistics
- ✅ Support for managed and self-hosted clusters

**Best Practices:**
1. Always run cleanup after testing
2. Use `--scenario all` for comprehensive testing
3. Verify check output manually first time
4. Use load tests to validate performance
5. Test on non-production clusters first
6. Monitor cluster metrics during heavy loads

**Scenario Summary:**
- **optimal** - Best-practice table configurations
- **problematic** - Known issues for warning detection
- **mixed_engines** - Various MergeTree engines
- **time_series** - Realistic metrics/time-series data
- **analytics** - Large-scale analytics workload
- **materialized_views** - View creation and monitoring
- **dictionaries** - Dictionary management
- **partitioned_tables** - Partition strategies

---

**Need Help?**
- Check structured findings JSON
- Review system.tables queries
- Verify ClickHouse client connection
- Test with clickhouse-client first
- Check Instaclustr console for cluster metrics

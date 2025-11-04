# Cassandra Health Check Testing Guide

Comprehensive guide for testing Cassandra health checks using the test data generator.

---

## Overview

The Cassandra test suite provides tools to:
- Generate test data with various configurations
- Exercise all health checks (CQL-based and API-based)
- Validate check behavior with known scenarios
- Load test the cluster
- Clean up test data

---

## Prerequisites

### Required Dependencies

```bash
pip install cassandra-driver faker pyyaml
```

### Configuration

You need a valid Cassandra configuration file. Examples:

**Instaclustr Managed:**
```bash
config/cassandra_instaclustr.yaml
```

**Self-Hosted:**
```bash
config/cassandra_test.yaml
```

---

## Test Data Generator

### Basic Usage

```bash
cd plugins/cassandra/test-data

# Run all scenarios
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario all

# Run specific scenario
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario optimal

# Load test with custom count
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario load_test --count 50000
```

---

## Test Scenarios

### 1. Optimal Configuration Scenario

**Purpose:** Create tables with best-practice configurations

**What It Creates:**
- `test_optimal` keyspace
- Tables with optimal settings:
  - Proper bloom filter FP chance (0.01)
  - Correct read repair (dclocal only)
  - Appropriate compaction strategies
  - Good index intervals

**Health Checks Expected:**
- ✅ **Table Statistics:** Reports tables with optimal configurations
- ✅ **Read Repair Settings:** All tables pass (0.0 RR, 0.1 dclocal)
- ✅ **Bloom Filter:** All tables have acceptable FP rates

**Run It:**
```bash
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario optimal
```

**Expected Output:**
```
✅ Created keyspace: test_optimal
  Creating optimal STCS table...
  Creating optimal LCS table...
  Inserting test data...
  ✅ Optimal scenario complete

📊 Execution Statistics
Tables created:     2
Rows inserted:      100
```

---

### 2. Problematic Configuration Scenario

**Purpose:** Create tables with known issues to trigger warnings

**What It Creates:**
- `test_problematic` keyspace
- Tables with issues:
  - High bloom filter FP (0.15 - should flag warning)
  - Redundant read repair (both settings enabled)
  - No read repair (0.0 for both)
  - Low index intervals (memory concerns)

**Health Checks Expected:**
- ⚠️ **Table Statistics:** Flags high bloom filter FP
- ⚠️ **Read Repair Settings:** Identifies non-standard configurations
- ⚠️ **Table Statistics:** Reports low index intervals

**Run It:**
```bash
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario problematic
```

**Validate:**
```bash
# Run health check
cd ../../..
python main.py --config config/cassandra_instaclustr.yaml

# Look for warnings in output:
# - "1 table(s) with bloom_filter_fp_chance > 0.1"
# - "4 table(s) have non-recommended read repair settings"
```

---

### 3. Secondary Indexes Scenario

**Purpose:** Test secondary index detection and analysis

**What It Creates:**
- `test_indexes` keyspace
- Table with secondary indexes:
  - Index on low-cardinality column (status - acceptable)
  - Index on high-cardinality column (email - problematic)

**Health Checks Expected:**
- ⚠️ **Secondary Indexes Check:** Detects both indexes
- ⚠️ Provides recommendations for high-cardinality index

**Run It:**
```bash
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario secondary_indexes
```

**Validate:**
```bash
# Check findings for:
# - "Found 2 secondary index(es) - review for performance impact"
# - Recommendations about index usage
```

---

### 4. Mixed Compaction Strategies Scenario

**Purpose:** Test compaction strategy distribution analysis

**What It Creates:**
- `test_compaction` keyspace
- Tables with different strategies:
  - STCS (SizeTieredCompactionStrategy)
  - LCS (LeveledCompactionStrategy)
  - TWCS (TimeWindowCompactionStrategy)

**Health Checks Expected:**
- ✅ **Table Statistics:** Reports 3 different compaction strategies
- ✅ Strategy distribution percentages

**Run It:**
```bash
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario mixed_compaction
```

**Validate:**
```sql
-- Query compaction strategies:
SELECT keyspace_name, table_name, compaction
FROM system_schema.tables
WHERE keyspace_name = 'test_compaction';
```

---

### 5. TTL Tables Scenario

**Purpose:** Test TTL configuration detection

**What It Creates:**
- `test_ttl` keyspace
- Tables with various TTL settings:
  - Short TTL (1 day / 86400 seconds)
  - Medium TTL (7 days / 604800 seconds)
  - Long TTL (90 days / 7776000 seconds)

**Health Checks Expected:**
- ✅ **Table Statistics:** Reports 3 tables with TTL enabled
- ✅ Shows TTL in both seconds and days

**Run It:**
```bash
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario ttl_tables
```

**Validate:**
```bash
# Check findings for:
# - "3 table(s) have default TTL configured"
# - TTL values in days: 1, 7, 90
```

---

### 6. Load Test Scenario

**Purpose:** Insert large volume of data for performance testing

**What It Creates:**
- `test_load` keyspace
- Single table with realistic data
- Default: 10,000 rows (configurable)
- Complex data types (UUID, timestamps, maps)

**Health Checks Expected:**
- ✅ Tests check performance with large datasets
- ✅ Exercises batch operations
- ✅ Validates data insertion patterns

**Run It:**
```bash
# Default (10K rows)
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario load_test

# Custom count (100K rows)
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario load_test --count 100000
```

**Expected Performance:**
- Small cluster: ~500-1000 rows/sec
- Large cluster: ~2000-5000 rows/sec

---

## Complete Testing Workflow

### Step 1: Generate Test Data

```bash
cd plugins/cassandra/test-data

# Create all test scenarios
python cassandra_test_data_generator.py \
    --config ../../../config/cassandra_instaclustr.yaml \
    --scenario all
```

**Output:**
```
🚀 Running ALL scenarios...

============================================================
Scenario: optimal
============================================================
✅ Created keyspace: test_optimal
  ✅ Optimal scenario complete

============================================================
Scenario: problematic
============================================================
✅ Created keyspace: test_problematic
  ✅ Problematic scenario complete

[... continues for all scenarios ...]

📊 Execution Statistics
Duration:           45.23 seconds
Keyspaces created:  6
Tables created:     12
Indexes created:    2
Rows inserted:      400
Errors:             0
```

### Step 2: Run Health Checks

```bash
cd ../../..  # Back to project root

# Run health check
python main.py --config config/cassandra_instaclustr.yaml
```

### Step 3: Verify Check Results

**Check Table Statistics:**
```json
{
  "table_statistics": {
    "table_counts": {
      "total_keyspaces": 7,
      "total_tables": 14
    },
    "compaction_strategies": {
      "data": [
        {"compaction_strategy": "SizeTieredCompactionStrategy", "table_count": 5},
        {"compaction_strategy": "LeveledCompactionStrategy", "table_count": 3},
        {"compaction_strategy": "TimeWindowCompactionStrategy", "table_count": 1}
      ]
    },
    "bloom_filter_settings": {
      "status": "warning",
      "high_fp_count": 1
    }
  }
}
```

**Check Read Repair Settings:**
```json
{
  "read_repair_settings": {
    "non_standard_settings": {
      "status": "warning",
      "count": 4,
      "message": "4 table(s) have non-recommended read repair settings"
    }
  }
}
```

**Check Secondary Indexes:**
```json
{
  "secondary_indexes": {
    "index_summary": {
      "status": "warning",
      "total_indexes": 2
    }
  }
}
```

### Step 4: Test API Checks (if configured)

If Instaclustr API credentials are configured:

```bash
# API checks should also execute:
# - instaclustr_jvm_metrics
# - instaclustr_compaction_metrics
# - instaclustr_disk_metrics
# - instaclustr_performance_metrics
```

### Step 5: Cleanup Test Data

```bash
cd plugins/cassandra/test-data

# List test keyspaces
python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml

# Drop specific keyspace
python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml --keyspace test_optimal

# Drop all test keyspaces (with confirmation)
python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml --all

# Drop all without confirmation (use with caution!)
python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml --all --confirm
```

**Cleanup Output:**
```
✅ Connected to Cassandra

🗑️  Cleaning up ALL test keyspaces...

  Found 6 test keyspace(s):
    - test_optimal
    - test_problematic
    - test_indexes
    - test_compaction
    - test_ttl
    - test_load

  Drop ALL 6 test keyspace(s)? (yes/no): yes

  Keyspace: test_optimal
    Tables: 2
    Replication: NetworkTopologyStrategy
  🗑️  Dropping keyspace: test_optimal...
  ✅ Dropped test_optimal

[... continues for all keyspaces ...]

  ✅ Cleanup complete: 6/6 keyspaces dropped
```

---

## Validation Checklist

Use this checklist to verify all checks work correctly:

### CQL-Based Checks

- [ ] **Table Statistics**
  - [ ] Reports correct table counts per keyspace
  - [ ] Detects all compaction strategies
  - [ ] Flags high bloom filter FP rates
  - [ ] Identifies CDC-enabled tables
  - [ ] Reports TTL configurations

- [ ] **Read Repair Settings**
  - [ ] Detects optimal settings (RR=0.0, dclocal=0.1)
  - [ ] Flags redundant configurations (both enabled)
  - [ ] Identifies no read repair (both=0.0)
  - [ ] Provides severity-scored recommendations

- [ ] **Secondary Indexes**
  - [ ] Detects all indexes
  - [ ] Categorizes by type (standard, SASI, custom)
  - [ ] Provides recommendations
  - [ ] Flags excessive index count (>5)

- [ ] **Network Topology**
  - [ ] Reports all datacenters
  - [ ] Shows rack distribution
  - [ ] Detects version mismatches
  - [ ] Warns about single-rack deployments

### API-Based Checks (if configured)

- [ ] **JVM Metrics**
  - [ ] Reports heap usage per node
  - [ ] Provides cluster aggregate
  - [ ] Flags high heap usage (>75%)

- [ ] **Compaction Metrics**
  - [ ] Reports pending compactions
  - [ ] Shows compaction throughput

- [ ] **Disk Metrics**
  - [ ] Per-node disk usage
  - [ ] Cluster aggregate
  - [ ] Percentage used

- [ ] **Performance Metrics**
  - [ ] Read/write operations
  - [ ] Latency percentiles (p50, p95, p99)

---

## Troubleshooting

### Connection Issues

**Problem:** `❌ Connection failed: No hosts available for the control connection`

**Solution:**
1. Verify hosts in config file
2. Check network connectivity: `ping <host>`
3. Verify port is correct (usually 9042)
4. Check firewall rules

### Authentication Errors

**Problem:** `AuthenticationFailed: Remote end requires authentication`

**Solution:**
1. Add credentials to config:
   ```yaml
   user: "your_username"
   password: "your_password"
   ```

### Keyspace Already Exists

**Problem:** Some tests fail because keyspaces exist

**Solution:**
```bash
# Clean up first
python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml --all --confirm

# Then run tests
python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario all
```

### Slow Inserts

**Problem:** Load test is very slow

**Possible Causes:**
1. Network latency (Instaclustr in different region)
2. Write throttling on managed cluster
3. Small cluster size

**Solutions:**
- Reduce count: `--count 1000`
- Use batch inserts (already implemented)
- Check Instaclustr console for throttling

---

## Advanced Usage

### Custom Scenarios

You can modify the generator to create custom scenarios:

```python
# Add to cassandra_test_data_generator.py

def scenario_custom(self):
    """Your custom test scenario."""
    keyspace = 'test_custom'
    self.create_test_keyspace(keyspace)
    self.session.set_keyspace(keyspace)

    # Your custom tables...
    self.session.execute("""
        CREATE TABLE IF NOT EXISTS your_table (...)
    """)
```

### Integration with CI/CD

```bash
#!/bin/bash
# ci_test_cassandra.sh

# Generate test data
python plugins/cassandra/test-data/cassandra_test_data_generator.py \
    --config config/cassandra_test.yaml \
    --scenario all

# Run health checks
python main.py --config config/cassandra_test.yaml

# Cleanup
python plugins/cassandra/test-data/cleanup_test_data.py \
    --config config/cassandra_test.yaml \
    --all --confirm
```

---

## Summary

**Test Suite Features:**
- ✅ 6 comprehensive test scenarios
- ✅ Exercises all CQL-based checks
- ✅ Compatible with API checks
- ✅ Realistic data generation
- ✅ Easy cleanup
- ✅ Detailed statistics

**Best Practices:**
1. Always run cleanup after testing
2. Use `--scenario all` for comprehensive testing
3. Verify check output manually first time
4. Use load test to validate performance
5. Test on non-production clusters first

---

**Need Help?**
- Check logs in `health_check_runs` table
- Review structured findings JSON
- Verify system_schema queries work
- Test connection with cqlsh first

# OpenSearch Load Tester & Test Data Generator

A comprehensive tool for generating realistic test data and stress testing OpenSearch clusters. This tool helps validate health checks, test cluster performance, and create realistic workload scenarios.

## Features

- **4 Realistic Data Generators:**
  - Application logs with various severity levels
  - E-commerce orders with nested product items
  - System metrics (CPU, memory, disk, network)
  - User activity/clickstream data

- **3 Test Scenarios:**
  - Logs: Simulates application logging (~100 logs/sec)
  - E-commerce: Generates order data with realistic patterns
  - Metrics: Simulates 50 servers reporting metrics every 10 seconds
  - All: Runs all scenarios sequentially

- **Performance Features:**
  - Bulk indexing with configurable batch sizes
  - Real-time progress tracking
  - Query pattern simulation
  - Performance statistics and reporting
  - Automatic index creation with proper mappings

## Prerequisites

```bash
pip install opensearch-py faker pyyaml
```

## Configuration

The tool uses the same YAML configuration files as the health check system. Example config:

```yaml
# config/opensearch_instaclustr.yaml
hosts:
  - "search-106c234734b6493b91ac2cdd5d71bc20.cnodes.io"
port: 9200
use_ssl: true
verify_certs: false
user: "admin"
password: "your-password"
```

## Usage

### Run All Scenarios

```bash
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario all
```

### Run Specific Scenario

**Application Logs** (configurable duration in seconds):
```bash
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario logs \
  --duration 300
```

**E-Commerce Orders** (configurable order count):
```bash
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario ecommerce \
  --count 10000
```

**System Metrics** (configurable servers and intervals):
```bash
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario metrics \
  --count 50
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--config` | Path to YAML config file | Required |
| `--scenario` | Scenario to run: `all`, `logs`, `ecommerce`, `metrics` | Required |
| `--duration` | Duration in seconds (for logs) | 300 |
| `--count` | Number of documents/servers to generate | Varies by scenario |

## Data Generator Details

### 1. Application Logs

**Index:** `test-logs-YYYY.MM.DD`

**Fields:**
- `@timestamp`: Event timestamp
- `level`: Log level (DEBUG, INFO, WARN, ERROR, FATAL)
- `service`: Microservice name (api-gateway, auth-service, etc.)
- `message`: Log message
- `host`: Hostname
- `environment`: Environment (prod, staging)
- `request_id`: Unique request ID
- `user_id`: User ID
- `response_time_ms`: Response time in milliseconds
- `http_status`: HTTP status code (for API gateway)

**Distribution:**
- 50% INFO, 30% DEBUG, 15% WARN, 4% ERROR, 1% FATAL
- 75% production, 25% staging
- Response times: 10-500ms (normal), 10-5000ms (errors)

**Default Rate:** ~100 logs/second

### 2. E-Commerce Orders

**Index:** `test-orders`

**Fields:**
- `@timestamp`: Order timestamp
- `order_id`: Unique order ID
- `customer_id`: Customer ID
- `customer_email`: Customer email
- `status`: Order status (pending, processing, shipped, delivered, cancelled)
- `items[]`: Array of ordered items
  - `product_id`: Product ID
  - `product_name`: Product name
  - `category`: Product category
  - `price`: Item price
  - `quantity`: Quantity ordered
- `total`: Order total
- `payment_method`: Payment method
- `shipping_method`: Shipping method

**Distribution:**
- 35% delivered, 30% shipped, 20% processing, 10% pending, 5% cancelled
- 1-5 items per order
- Price range: $9.99 - $299.99 per item

**Default Count:** 5,000 orders

### 3. System Metrics

**Index:** `test-metrics-YYYY.MM.DD`

**Fields:**
- `@timestamp`: Metric timestamp
- `hostname`: Server hostname
- `datacenter`: Datacenter location
- `cpu_percent`: CPU usage percentage
- `memory_percent`: Memory usage percentage
- `disk_percent`: Disk usage percentage
- `network_in_mbps`: Inbound network traffic
- `network_out_mbps`: Outbound network traffic
- `active_connections`: Active connections count
- `request_rate`: Requests per second
- `error_rate`: Error rate percentage
- `response_time_p50`: 50th percentile response time
- `response_time_p95`: 95th percentile response time
- `response_time_p99`: 99th percentile response time

**Distribution:**
- CPU: 10-95%
- Memory: 40-85%
- Disk: 30-75%
- Network: 10-1000 Mbps

**Default:** 50 servers × 30 intervals = 900 metrics

### 4. User Activity

**Index:** `test-activity`

**Fields:**
- `@timestamp`: Activity timestamp
- `session_id`: Session ID
- `user_id`: User ID
- `action`: User action (page_view, click, search, add_to_cart, purchase, logout)
- `page`: Page visited
- `referrer`: Traffic source
- `device`: Device type (mobile, desktop, tablet)
- `os`: Operating system
- `browser`: Browser type
- `duration_seconds`: Session duration
- `ip_address`: IP address
- `location`: Geographic location

**Distribution:**
- 40% page_view, 30% click, 15% search, 10% add_to_cart, 3% purchase, 2% logout

## Query Patterns

Each scenario includes realistic query patterns:

1. **Match All Query** - Retrieve all documents
2. **Term Query** - Filter by specific field value
3. **Range Query** - Time-based range filtering
4. **Boolean Query** - Complex boolean logic
5. **Aggregation Query** - Date histogram aggregation

## Output Example

```
======================================================================
OpenSearch Load Tester & Test Data Generator
======================================================================
✅ Connected to OpenSearch 3.2.0
   Cluster: opensearch-test

======================================================================
📋 SCENARIO: Application Logs
======================================================================
✅ Created index: test-logs-2025.10.31

📝 Bulk indexing 18,000 documents into 'test-logs-2025.10.31'...
   Progress: 5,000/18,000 docs (2019 docs/sec)
   Progress: 10,000/18,000 docs (2319 docs/sec)
   Progress: 15,000/18,000 docs (2443 docs/sec)
   Progress: 18,000/18,000 docs (2307 docs/sec)
   ✅ Indexed: 18,000 documents
   ⏱️  Time: 7.80s (2307 docs/sec)

🔍 Running 50 search queries against 'test-logs-2025.10.31'...
   Progress: 20/50 queries
   Progress: 40/50 queries
   ✅ Successful: 50/50
   ⏱️  Time: 0.58s (87 queries/sec)

======================================================================
📈 FINAL STATISTICS
======================================================================
   Total Documents Indexed: 23,900
   Total Queries Executed: 120
   Total Errors: 0
   Total Time: 16.18s (0.3 min)
   Avg Indexing Rate: 1477 docs/sec

   Indices Created:
      • test-logs-2025.10.31: 20,000 docs, 4.76 MB
      • test-orders: 20,023 docs, 5.68 MB
      • test-metrics-2025.10.31: 900 docs, 0.22 MB

   💡 Run health check to see cluster impact:
      python main.py --config config/opensearch_instaclustr.yaml
```

## Use Cases

### 1. Health Check Validation

Generate test data to validate that health checks properly detect issues:

```bash
# Generate significant load
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario all

# Run health check
cd ../../..
python main.py --config config/opensearch_instaclustr.yaml
```

### 2. Performance Testing

Test cluster performance under sustained load:

```bash
# High-volume logging scenario (30 minutes)
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario logs \
  --duration 1800
```

### 3. Capacity Planning

Understand how the cluster handles different data volumes:

```bash
# Large e-commerce dataset
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario ecommerce \
  --count 100000
```

### 4. Index Pattern Testing

Test time-based index patterns and rollover:

```bash
# Generate metrics over multiple days
for i in {1..7}; do
  python opensearch_load_tester.py \
    --config ../../../config/opensearch_instaclustr.yaml \
    --scenario metrics \
    --count 100
  sleep 86400  # Wait 1 day
done
```

### 5. Query Performance Analysis

Measure query performance with realistic workloads:

```bash
# Generate data and observe query latency
python opensearch_load_tester.py \
  --config ../../../config/opensearch_instaclustr.yaml \
  --scenario all
```

## Performance Considerations

- **Bulk Size:** Default batch size is 1,000 documents. Adjust in code if needed.
- **Network:** Performance depends on network latency to the cluster.
- **SSL:** SSL verification is disabled by default for testing. Enable in production.
- **Rate Limiting:** The tool does not implement rate limiting. Use `--duration` to control load.

## Cleanup

To remove test indices after testing:

```bash
# Using OpenSearch API
curl -X DELETE "https://your-cluster:9200/test-*" -u admin:password -k

# Or using OpenSearch Dashboards Dev Tools
DELETE test-*
```

## Integration with Health Check

After generating test data, run the health check to see how the cluster responds:

```bash
cd /path/to/pg_healthcheck2
python main.py --config config/opensearch_instaclustr.yaml
```

The health check report will include:
- Updated index statistics
- Performance metrics
- Shard distribution
- Resource utilization
- Diagnostic information

## Troubleshooting

**Connection Issues:**
- Verify the config file path is correct
- Check that credentials are valid
- Ensure the cluster is accessible
- Verify SSL settings match cluster configuration

**Slow Indexing:**
- Check network latency
- Monitor cluster CPU/memory usage
- Review OpenSearch logs for bottlenecks
- Consider reducing batch size for memory-constrained clusters

**Import Errors:**
- Install required dependencies: `pip install opensearch-py faker pyyaml`

## Architecture

```
opensearch_load_tester.py
├── OpenSearchLoadTester (Main class)
│   ├── Data Generators
│   │   ├── generate_log_entry()
│   │   ├── generate_ecommerce_order()
│   │   ├── generate_metrics_document()
│   │   └── generate_user_activity()
│   ├── Index Management
│   │   ├── create_index()
│   │   └── bulk_index_documents()
│   ├── Query Execution
│   │   └── run_search_queries()
│   └── Scenarios
│       ├── run_logs_scenario()
│       ├── run_ecommerce_scenario()
│       └── run_metrics_scenario()
└── CLI Interface (argparse)
```

## Contributing

To add new data generators:

1. Create a new `generate_*()` method
2. Define appropriate mappings
3. Add a `run_*_scenario()` method
4. Update the CLI argument parser

## Version History

- **1.0** - Initial release with 4 generators and 3 scenarios
- Fixed datetime deprecation warnings for Python 3.12+

## License

Part of the pg_healthcheck2 project.

# Kafka Instacollector Import Guide

Generate health check reports from Instaclustr's Instacollector diagnostic exports without requiring live Kafka cluster access.

## Overview

The Instacollector importer allows you to analyze Kafka cluster health from pre-collected diagnostic data. This is useful for:

- **Offline analysis** - Review cluster health without direct cluster access
- **Historical analysis** - Analyze diagnostics collected at a specific point in time
- **Consulting engagements** - Review customer-provided diagnostics
- **Incident review** - Analyze diagnostics captured during an incident
- **Pre-migration assessment** - Evaluate cluster health before upgrades

## Quick Start

```bash
# Simplest approach - use the root-level import script
python import_kafka_diagnostic.py --diagnostic ./InstaCollection_kafka.tar.gz --config config/kafka.yaml

# With company name override
python import_kafka_diagnostic.py --diagnostic ./export.tar.gz --config config/kafka.yaml --company "Acme Corp"

# Alternative: module-based approach (no config file needed)
python -m plugins.kafka.import_instacollector /path/to/InstaCollection.tar.gz --company "Acme Corp"
```

## Installation

No additional dependencies required beyond the main health check tool requirements.

## Usage

### Basic Usage (Recommended)

Use the root-level import script with a config file:

```bash
# Analyze a tarball
python import_kafka_diagnostic.py --diagnostic ./InstaCollection_kafka.tar.gz --config config/kafka.yaml

# Analyze an extracted directory
python import_kafka_diagnostic.py --diagnostic ./instacollector_data/ --config config/kafka.yaml

# With verbose output
python import_kafka_diagnostic.py --diagnostic ./export.tar.gz --config config/kafka.yaml --verbose
```

### Module-Based Usage (Alternative)

For quick analysis without a config file:

```bash
# Analyze a tarball
python -m plugins.kafka.import_instacollector ./InstaCollection_kafka.tar.gz

# Analyze an extracted directory
python -m plugins.kafka.import_instacollector ./instacollector_data/
```

### Using a Config File (Recommended)

Create a YAML config file for repeatable analysis:

```yaml
# config/kafka_import.yaml
company_name: "Customer ABC"
diagnostic_path: "/path/to/InstaCollection.tar.gz"

# Optional: Custom output directory
output_dir: "./reports/customer_abc"

# Optional: Ship to trends database
trend_storage_enabled: true
trends_config: "config/trends.yaml"

# Optional: NVD API key for CVE checks
nvd_api_key: "your-nvd-api-key"

# Optional: AI analysis
ai_analyze: false
# ai_provider: "openai"
# ai_model: "gpt-4"
# ai_api_key: "your-api-key"
```

Run with config:

```bash
python -m plugins.kafka.import_instacollector --config config/kafka_import.yaml
```

### Command Line Options

**Root-level script (`import_kafka_diagnostic.py`):**

| Option | Description |
|--------|-------------|
| `--diagnostic`, `-d` | Path to Instacollector export (tarball or directory) - **required** |
| `--config`, `-c` | Path to YAML config file - **required** |
| `--company` | Company name override (takes precedence over config) |
| `--report-config` | Custom report definition file |
| `--output`, `-o` | Output report filename (default: health_check.adoc) |
| `--no-report` | Skip generating AsciiDoc report |
| `--verbose`, `-v` | Enable verbose logging |

**Module-based script (`python -m plugins.kafka.import_instacollector`):**

| Option | Description |
|--------|-------------|
| `diagnostic_path` | Path to Instacollector export (tarball or directory) |
| `--config` | Path to YAML config file |
| `--company`, `-c` | Company name for report identification |
| `--output-dir`, `-o` | Output directory for reports |
| `--report-config`, `-r` | Custom report definition file |
| `--ship-trends` | Ship results to trends database |
| `--trends-config` | Path to trends configuration file |
| `--json-only` | Output only structured JSON (no AsciiDoc) |
| `--quiet`, `-q` | Suppress progress output |

### Smart Path Detection

When given a directory path, the tool automatically finds the most recent Instacollector export:

```bash
# If /uploads/ contains multiple InstaCollection_*.tar.gz files,
# the most recent one is automatically selected
python -m plugins.kafka.import_instacollector /uploads/
```

The tool looks for:
- `InstaCollection_*.tar.gz` files
- `instacollector_*.tar.gz` files
- Directories containing IP-named subdirectories

## Instacollector Data Structure

The importer expects the standard Instacollector directory structure:

```
instacollector_export/
├── 192.168.1.10/
│   ├── kafka-versions-output.txt.info
│   ├── kafka-topics-describe-output.txt.info
│   ├── consumer-groups-output.txt.info
│   ├── kafka-metadata-quorum-status-output.txt.info
│   ├── kafka-metadata-quorum-replication-output.txt.info
│   ├── kafka-api-versions-output.txt.info
│   ├── broker.properties (or server.properties)
│   ├── controller.properties
│   ├── log4j.properties
│   ├── kafkaServer-gc.log*
│   ├── server.log*
│   ├── controller.log*
│   ├── state-change.log*
│   ├── kafka-authorizer.log*
│   ├── mem.info
│   ├── cpu.info
│   ├── io_stat.info
│   ├── file_descriptor.info
│   └── hosts_file.info
├── 192.168.1.11/
│   └── ...
└── 192.168.1.12/
    └── ...
```

### Supported Files

| File | Description | Used By |
|------|-------------|---------|
| `kafka-versions-output.txt.info` | Kafka version | CVE Check, Version Info |
| `kafka-topics-describe-output.txt.info` | Topic metadata, partitions, replicas, ISR | Topic Health, ISR Analysis |
| `consumer-groups-output.txt.info` | Consumer groups with lag data | Consumer Lag, Consumer Health |
| `kafka-metadata-quorum-status-output.txt.info` | KRaft quorum status | KRaft Quorum Check |
| `kafka-metadata-quorum-replication-output.txt.info` | KRaft replication info | KRaft Quorum Check |
| `kafka-api-versions-output.txt.info` | Broker API versions | API Versions Check |
| `broker.properties` / `server.properties` | Broker configuration | Broker Config Audit |
| `controller.properties` | Controller configuration (KRaft) | Controller Config Check |
| `log4j.properties` | Logging configuration | Config Analysis |
| `kafkaServer-gc.log*` | Garbage collection logs | GC Analysis |
| `server.log*` | Broker application logs | Log Error Analysis |
| `controller.log*` | Controller logs | Log Error Analysis |
| `state-change.log*` | Partition state changes | State Change Analysis |
| `kafka-authorizer.log*` | Authorization events | Security Audit |
| `mem.info` | /proc/meminfo output | Memory Analysis |
| `cpu.info` | /proc/cpuinfo output | CPU Analysis |
| `io_stat.info` | iostat output | I/O Statistics |
| `file_descriptor.info` | File descriptor limits | File Descriptor Check |
| `hosts_file.info` | /etc/hosts content | Network Config Check |

## Checks Included

The Instacollector import runs these health checks:

### Security
- **CVE Vulnerability Check** - Known vulnerabilities for the Kafka version
- **Authorization Audit** - Denied access patterns from kafka-authorizer.log

### Cluster Health
- **KRaft Quorum Health** - Voter status, leader election, replication lag
- **API Versions Analysis** - Broker API compatibility

### Topic Health
- **ISR Health** - In-sync replica status across all partitions
- **Topic Count and Naming** - Topic proliferation, naming conventions
- **Topic Configuration** - Retention, cleanup policy, compression settings
- **Internal Topic Replication** - __consumer_offsets and __transaction_state replication

### Consumer Health
- **Consumer Lag Analysis** - Lag by consumer group and partition
- **Consumer Group Health** - Stable vs rebalancing groups

### OS-Level Metrics
- **CPU Analysis** - Core count, architecture, utilization
- **Memory Analysis** - Available memory, usage patterns
- **File Descriptor Limits** - Open files vs limits
- **System Limits** - ulimit settings for Kafka processes

### Configuration
- **Broker Configuration Audit** - server.properties best practices
- **Controller Configuration** - KRaft controller settings (KRaft mode)
- **Network Configuration** - Hosts file analysis for broker resolution

### Performance
- **GC Analysis** - Full GC events, pause times, memory pressure
- **I/O Statistics** - Disk I/O patterns from iostat
- **Disk Usage** - Log directory utilization

### Log Analysis
- **Error Analysis** - Error patterns from server.log and controller.log
- **State Change Analysis** - Leadership changes, partition movements

## Output

### AsciiDoc Report

The primary output is an AsciiDoc report at:
```
adoc_out/<company_name>/health_check.adoc
```

Convert to HTML or PDF:
```bash
# HTML
asciidoctor adoc_out/customer_abc/health_check.adoc

# PDF
asciidoctor-pdf adoc_out/customer_abc/health_check.adoc
```

### Structured JSON Findings

Machine-readable findings for integration:
```
adoc_out/<company_name>/structured_health_check_findings.json
```

### Console Summary

The tool prints a summary showing:
- Cluster information (ID, version, mode, brokers)
- Checks executed
- Rules triggered (by severity)

Example output:
```
✅ Successfully loaded Instacollector export
   - Source: ./InstaCollection_kafka.tar.gz
   - Collection Date: 2025-01-15T10:30:00
   - Version: 3.6.0
   - Mode: KRAFT
   - Brokers: 3

Generating health check report...

============================================================
REPORT SUMMARY
============================================================
Cluster ID: production-kafka
Kafka Version: 3.6.0
Mode: KRAFT
Brokers: 3
Checks executed: 22

⚡ Rules Triggered: 29
   🔴 CRITICAL: 1
   🟠 HIGH: 4
   🟡 MEDIUM: 12
   🔵 LOW: 12

✅ AsciiDoc report: adoc_out/customer_abc/health_check.adoc
✅ Structured findings: adoc_out/customer_abc/structured_health_check_findings.json

✅ Import complete!
```

## Shipping to Trends Database

To track health over time, ship results to the trends database:

```bash
python -m plugins.kafka.import_instacollector ./export.tar.gz \
    --company "Customer ABC" \
    --ship-trends \
    --trends-config config/trends.yaml
```

Or in config file:
```yaml
trend_storage_enabled: true
trends_config: "config/trends.yaml"
```

## Troubleshooting

### "No valid node directories found"

The importer couldn't find directories named with IP addresses. Ensure your export has the standard structure with IP-named subdirectories.

### "No topic data available"

The `kafka-topics-describe-output.txt.info` file wasn't found. This is required for topic health checks.

### "Quorum data not available"

KRaft quorum files weren't found. This is expected for ZooKeeper-based clusters. The related checks will be skipped.

### "Admin API queries not supported"

Some checks require live Admin API access (e.g., dynamic broker configs). These checks are skipped for Instacollector imports. The Instacollector-specific report only includes checks that work with offline data.

### Nested Tarballs

The importer automatically handles nested tarballs:
```
OuterArchive.tar.gz
└── 192.168.1.10/
    └── InstaCollection_192.168.1.10.tar.gz  ← automatically extracted
```

## Examples

### Basic Analysis

```bash
# Analyze and generate report (using root-level script)
python import_kafka_diagnostic.py \
    --diagnostic ./customer_diagnostic.tar.gz \
    --config config/kafka.yaml \
    --company "Customer XYZ"

# View the report
asciidoctor adoc_out/customer_xyz/health_check.adoc
open adoc_out/customer_xyz/health_check.html
```

### Consulting Workflow

```bash
# 1. Create config for the engagement
cat > config/customer_engagement.yaml << EOF
company_name: "Acme Corp - Q1 Review"
trend_storage_enabled: true
trends_config: "config/trends.yaml"
nvd_api_key: "your-nvd-api-key"
ai_analyze: false
EOF

# 2. Run analysis
python import_kafka_diagnostic.py \
    --diagnostic "/diagnostics/acme/InstaCollection_2025-01-15.tar.gz" \
    --config config/customer_engagement.yaml

# 3. Generate PDF report for customer
asciidoctor-pdf adoc_out/acme_corp___q1_review/health_check.adoc -o reports/acme_q1/health_check.pdf
```

### Batch Processing

```bash
# Process multiple diagnostics
for tarball in /diagnostics/*.tar.gz; do
    name=$(basename "$tarball" .tar.gz)
    python import_kafka_diagnostic.py \
        --diagnostic "$tarball" \
        --config config/kafka.yaml \
        --company "$name"
done
```

## Comparison: Live vs Instacollector Import

| Feature | Live Connection | Instacollector Import |
|---------|----------------|----------------------|
| Admin API queries | Yes | Limited (describe topics, consumers) |
| SSH/OS metrics | Yes | Yes (from files) |
| Configuration files | Yes (via SSH) | Yes (from files) |
| Real-time data | Yes | Point-in-time snapshot |
| Network access required | Yes | No |
| Cluster load | Minimal | None |
| Historical analysis | No | Yes |
| KRaft quorum data | Yes | Yes |
| Log analysis | Yes (via SSH) | Yes (from files) |

## Design Pattern

The Kafka Instacollector import follows the same design pattern as Cassandra and OpenSearch imports:

1. **Same Interface** - `KafkaInstacollectorConnector` implements the same interface as `KafkaConnector`
2. **Declarative Mappings** - Uses `COMMAND_FILE_MAP` and `OPERATION_MAP` for routing
3. **No Duplicate Checks** - The same check modules work for both live and imported data
4. **Transparent Substitution** - Checks don't know if they're running against live data or imports

This means any new check written for live Kafka analysis automatically works with Instacollector imports, and vice versa.

## Related Documentation

- [Instaclustr Instacollector](https://www.instaclustr.com/support/documentation/instacollector/) - Official Instacollector documentation
- [NVD API Key Setup](./NVD_API_KEY_SETUP.md) - Configure NVD API key for CVE checks
- [Cassandra Instacollector Import](./CASSANDRA_INSTACOLLECTOR_IMPORT.md) - Similar guide for Cassandra
- [Diagnostic Import Pattern](./DIAGNOSTIC_IMPORT_PATTERN.md) - Developer guide for the import connector pattern

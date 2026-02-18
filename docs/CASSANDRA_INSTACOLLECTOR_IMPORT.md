# Cassandra Instacollector Import Guide

Generate health check reports from Instaclustr's Instacollector diagnostic exports without requiring live cluster access.

## Overview

The Instacollector importer allows you to analyze Cassandra cluster health from pre-collected diagnostic data. This is useful for:

- **Offline analysis** - Review cluster health without direct cluster access
- **Historical analysis** - Analyze diagnostics collected at a specific point in time
- **Consulting engagements** - Review customer-provided diagnostics
- **Incident review** - Analyze diagnostics captured during an incident

## Quick Start

```bash
# From a tarball (recommended entry point)
python import_cassandra_diagnostic.py --diagnostic ./InstaCollection.tar.gz --config config/cassandra.yaml

# From an extracted directory
python import_cassandra_diagnostic.py --diagnostic ./instacollector_export/ --config config/cassandra.yaml

# Auto-detect most recent export in a directory
python import_cassandra_diagnostic.py --diagnostic /path/to/uploads/ --config config/cassandra.yaml

# With company name override
python import_cassandra_diagnostic.py --diagnostic ./export.tar.gz --config config/cassandra.yaml --company "Acme Corp"
```

## Installation

No additional dependencies required beyond the main health check tool requirements.

## Usage

### Root-Level Entry Point (Recommended)

The `import_cassandra_diagnostic.py` script at the project root provides a simple interface:

```bash
# Basic usage
python import_cassandra_diagnostic.py --diagnostic ./InstaCollection.tar.gz --config config/cassandra.yaml

# With custom output filename
python import_cassandra_diagnostic.py --diagnostic ./export.tar.gz --config config/cassandra.yaml --output client_report.adoc

# Skip report generation (JSON only)
python import_cassandra_diagnostic.py --diagnostic ./export.tar.gz --config config/cassandra.yaml --no-report

# Verbose logging
python import_cassandra_diagnostic.py --diagnostic ./export.tar.gz --config config/cassandra.yaml --verbose
```

### Programmatic Usage

```python
from import_cassandra_diagnostic import CassandraDiagnosticImporter

# Initialize and run import
importer = CassandraDiagnosticImporter(
    config_file='config/cassandra.yaml',
    diagnostic_path='./InstaCollection.tar.gz',
    company_override='Acme Corp'
)
importer.run_import()
importer.write_adoc('health_check.adoc')

# Access findings programmatically
findings = importer.all_structured_findings
health_score = findings.get('check_executive_summary', {}).get('data', {}).get('health_score')
```

### Alternative: Module Invocation

You can also run the importer as a Python module:

```bash
python -m plugins.cassandra.import_instacollector /path/to/InstaCollection.tar.gz --company "Acme Corp"
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--diagnostic`, `-d` | Path to Instacollector export (tarball or directory) - **required** |
| `--config`, `-c` | Path to YAML config file - **required** |
| `--company` | Company name override (takes precedence over config file) |
| `--report-config` | Custom report definition file |
| `--output`, `-o` | Output report filename (default: health_check.adoc) |
| `--no-report` | Skip generating AsciiDoc report |
| `--verbose`, `-v` | Enable verbose logging |

### Configuration File

Create a YAML config file for repeatable analysis:

```yaml
# config/cassandra_import.yaml
company_name: "Customer ABC"
db_type: cassandra

# Optional: Report settings
show_qry: false
row_limit: 100

# Optional: NVD API key for CVE checks
nvd_api_key: "your-nvd-api-key"

# Optional: Ship to trends database
trend_storage_enabled: true

# Optional: AI analysis
ai_analyze: false
ai_run_integrated: false
# ai_provider: "openai"
# ai_model: "gpt-4"
# ai_api_key: "your-api-key"
```

### Smart Path Detection

When given a directory path, the tool automatically finds the most recent Instacollector export:

```bash
# If /uploads/ contains multiple InstaCollection_*.tar.gz files,
# the most recent one is automatically selected
python import_cassandra_diagnostic.py --diagnostic /uploads/ --config config/cassandra.yaml
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
│   ├── nodetool_status.info
│   ├── nodetool_info.info
│   ├── nodetool_cfstats.info
│   ├── nodetool_compactionstats.info
│   ├── nodetool_describecluster.info
│   ├── nodetool_gossipinfo.info
│   ├── nodetool_ring.info
│   ├── nodetool_tpstats.info
│   ├── nodetool_version.info
│   ├── nodetool_gcstats.info
│   ├── disk.info
│   ├── io_stat.info
│   ├── cassandra.yaml
│   ├── jvm.options
│   ├── cassandra-env.sh
│   ├── gc.log.*
│   └── system.log
├── 192.168.1.11/
│   └── ...
└── 192.168.1.12/
    └── ...
```

### Supported Files

| File | Description | Used By |
|------|-------------|---------|
| `nodetool_status.info` | Cluster topology, node states | Cluster Overview |
| `nodetool_info.info` | JVM heap, datacenter, rack, uptime | Cluster Overview, Resources |
| `nodetool_cfstats.info` | Table statistics | Table Stats, Disk Usage |
| `nodetool_tpstats.info` | Thread pool statistics | Thread Pool Stats |
| `nodetool_compactionstats.info` | Compaction status | Compaction Stats |
| `nodetool_gossipinfo.info` | Gossip state | Gossip State Check |
| `nodetool_describecluster.info` | Schema versions | Schema Consistency |
| `nodetool_ring.info` | Token distribution | Ring Analysis |
| `nodetool_version.info` | Cassandra version | CVE Check, Version Info |
| `nodetool_gcstats.info` | GC statistics | GC Stats Check |
| `disk.info` | df -h output | Disk Space Check |
| `io_stat.info` | iostat output | I/O Stats Check |
| `cassandra.yaml` | Main configuration | Config Audit |
| `jvm.options` | JVM settings | JVM Config Check |
| `cassandra-env.sh` | Environment settings | Environment Check |
| `gc.log.*` | GC logs | GC Analysis |
| `system.log` | Application logs | System Log Analysis |

## Checks Included

The Instacollector import runs these health checks:

### Cluster Status
- **Cluster Overview** - Node states, datacenter distribution, resource usage
- **Schema Version Consistency** - Detects schema disagreements
- **Gossip State Analysis** - Node gossip status and potential issues
- **Ring Analysis** - Token distribution and ownership

### Performance
- **Thread Pool Statistics** - Pending tasks, blocked threads, dropped messages
- **Table Statistics** - Read/write latencies, tombstones, SSTable counts
- **Compaction Statistics** - Pending compactions, backlog detection
- **GC Log Analysis** - Full GC events, pause times, memory pressure
- **GC Statistics** - Aggregated GC metrics from nodetool gcstats
- **I/O Statistics** - Disk I/O patterns from iostat

### Storage
- **Disk Space per Keyspace** - Data distribution across keyspaces
- **Data Directory Disk Space** - Cassandra data directory usage with thresholds

### Configuration
- **Configuration Audit** - cassandra.yaml best practices review
- **JVM Configuration** - GC algorithm, heap settings, deprecated flags
- **Environment Configuration** - cassandra-env.sh analysis

### Operations
- **System Log Analysis** - Error patterns, warnings, exceptions

### Security
- **CVE Vulnerability Check** - Known vulnerabilities for the Cassandra version

### Executive Summary
- **Aggregated Health Score** - Cluster-wide health assessment with letter grade (A-F)
- **Key Concerns** - Critical and medium priority issues
- **Resource Summary** - Node count, load distribution, dropped messages

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
- Cluster information (name, version, datacenter, nodes)
- Checks executed
- Rules triggered (by severity)

Example output:
```
============================================================
Cassandra Diagnostic Import Tool v2.1.0
============================================================

Importing: /path/to/InstaCollection_202512120223.tar.gz
Company: cassandra test
✅ Successfully loaded Instacollector export
   - Source: /path/to/InstaCollection_202512120223.tar.gz
   - Collection Date: 2025-12-12T02:22:48
   - Version: 3.11.2
   - Cluster: SpectrumMVNO
   - Datacenter: dc1
   - Nodes: 6

Diagnostic Info:
  Collection Date: 2025-12-12T02:22:48
  Cassandra Version: 3.11.2
  Datacenter: dc1
  Nodes: 6

--- Running Health Checks Against Imported Data ---

--- Summary ---
Checks executed: 23

Rules Triggered: 5

============================================================
Import Complete!
============================================================
Report saved to: /path/to/adoc_out/cassandra_test/health_check.adoc
```

## Shipping to Trends Database

To track health over time, enable trend shipping in your config:

```yaml
# In config/cassandra.yaml
trend_storage_enabled: true
```

Or use the module invocation with explicit flags:

```bash
python -m plugins.cassandra.import_instacollector ./export.tar.gz \
    --company "Customer ABC" \
    --ship-trends \
    --trends-config config/trends.yaml
```

## Troubleshooting

### "No valid node directories found"

The importer couldn't find directories named with IP addresses. Ensure your export has the standard structure with IP-named subdirectories.

### "No data available for nodetool X"

The specific nodetool output file wasn't found in the export. This is normal if the Instacollector didn't capture that command.

### "CQL queries not supported"

Some checks require live CQL access (e.g., system table queries). These checks are skipped for Instacollector imports. The Instacollector-specific report only includes checks that work with offline data.

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
# Analyze and generate report
python import_cassandra_diagnostic.py \
    --diagnostic ./customer_diagnostic.tar.gz \
    --config config/cassandra.yaml \
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
db_type: cassandra
nvd_api_key: "your-nvd-api-key"
trend_storage_enabled: false
ai_analyze: false
EOF

# 2. Run analysis
python import_cassandra_diagnostic.py \
    --diagnostic /diagnostics/acme/InstaCollection_2025-01-15.tar.gz \
    --config config/customer_engagement.yaml

# 3. Generate PDF report for customer
asciidoctor-pdf adoc_out/acme_corp_q1_review/health_check.adoc \
    -o reports/acme_health_check.pdf
```

### Batch Processing

```bash
# Process multiple diagnostics
for tarball in /diagnostics/*.tar.gz; do
    name=$(basename "$tarball" .tar.gz)
    python import_cassandra_diagnostic.py \
        --diagnostic "$tarball" \
        --config config/cassandra.yaml \
        --company "$name"
done
```

## Comparison: Live vs Instacollector Import

| Feature | Live Connection | Instacollector Import |
|---------|----------------|----------------------|
| CQL queries | Yes | No |
| Nodetool commands | Yes (via SSH) | Yes (from files) |
| Configuration files | Yes (via SSH) | Yes (from files) |
| Real-time data | Yes | Point-in-time snapshot |
| Network access required | Yes | No |
| Cluster load | Minimal | None |
| Historical analysis | No | Yes |

## Entry Point Comparison

| Entry Point | Location | Best For |
|-------------|----------|----------|
| `import_cassandra_diagnostic.py` | Project root | Simple CLI usage, programmatic access |
| `python -m plugins.cassandra.import_instacollector` | Module | Advanced options, config file workflows |

Both entry points produce identical results and support the same features.

## Related Documentation

- [Kafka Instacollector Import](./KAFKA_INSTACOLLECTOR_IMPORT.md) - Similar import process for Kafka
- [OpenSearch Diagnostic Import](./OPENSEARCH_DIAGNOSTIC_IMPORT.md) - Import for OpenSearch diagnostics
- [Instaclustr Instacollector](https://www.instaclustr.com/support/documentation/instacollector/) - Official Instacollector documentation
- [NVD API Key Setup](./NVD_API_KEY_SETUP.md) - Configure NVD API key for CVE checks

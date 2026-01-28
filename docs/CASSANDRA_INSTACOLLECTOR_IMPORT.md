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
# From a tarball
python -m plugins.cassandra.import_instacollector /path/to/InstaCollection_192.168.1.10.tar.gz

# From an extracted directory
python -m plugins.cassandra.import_instacollector /path/to/instacollector_export/

# Auto-detect most recent export in a directory
python -m plugins.cassandra.import_instacollector /path/to/uploads/

# With company name for identification
python -m plugins.cassandra.import_instacollector ./export.tar.gz --company "Acme Corp"
```

## Installation

No additional dependencies required beyond the main health check tool requirements.

## Usage

### Basic Usage

```bash
# Analyze a tarball
python -m plugins.cassandra.import_instacollector ./InstaCollection_cluster.tar.gz

# Analyze an extracted directory
python -m plugins.cassandra.import_instacollector ./instacollector_data/
```

### Using a Config File (Recommended)

Create a YAML config file for repeatable analysis:

```yaml
# config/cassandra_import.yaml
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
python -m plugins.cassandra.import_instacollector --config config/cassandra_import.yaml
```

### Command Line Options

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
python -m plugins.cassandra.import_instacollector /uploads/
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
- **Aggregated Health Score** - Cluster-wide health assessment (runs last)

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
✅ Successfully loaded Instacollector export
   - Source: ./InstaCollection_cluster.tar.gz
   - Collection Date: 2025-01-15T10:30:00
   - Version: 4.1.3
   - Cluster: production-cassandra
   - Datacenter: dc1
   - Nodes: 6

Generating health check report...

============================================================
REPORT SUMMARY
============================================================
Cluster: production-cassandra
Version: 4.1.3
Datacenter: dc1
Nodes: 6
Checks executed: 19

⚡ Rules Triggered: 104
   🔴 CRITICAL: 2
   🟠 HIGH: 8
   🟡 MEDIUM: 35
   🔵 LOW: 59

✅ AsciiDoc report: adoc_out/customer_abc/health_check.adoc
✅ Structured findings: adoc_out/customer_abc/structured_health_check_findings.json

✅ Import complete!
```

## Shipping to Trends Database

To track health over time, ship results to the trends database:

```bash
python -m plugins.cassandra.import_instacollector ./export.tar.gz \
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
python -m plugins.cassandra.import_instacollector ./customer_diagnostic.tar.gz \
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
diagnostic_path: "/diagnostics/acme/InstaCollection_2025-01-15.tar.gz"
output_dir: "./reports/acme_q1"
trend_storage_enabled: true
trends_config: "config/trends.yaml"
nvd_api_key: "your-nvd-api-key"
EOF

# 2. Run analysis
python -m plugins.cassandra.import_instacollector --config config/customer_engagement.yaml

# 3. Generate PDF report for customer
asciidoctor-pdf reports/acme_q1/health_check.adoc -o reports/acme_q1/health_check.pdf
```

### Batch Processing

```bash
# Process multiple diagnostics
for tarball in /diagnostics/*.tar.gz; do
    name=$(basename "$tarball" .tar.gz)
    python -m plugins.cassandra.import_instacollector "$tarball" \
        --company "$name" \
        --quiet
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

## Related Documentation

- [Instaclustr Instacollector](https://www.instaclustr.com/support/documentation/instacollector/) - Official Instacollector documentation
- [NVD API Key Setup](./NVD_API_KEY_SETUP.md) - Configure NVD API key for CVE checks

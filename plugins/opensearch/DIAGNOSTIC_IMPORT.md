# OpenSearch Diagnostic Import Tool

Import and analyze Elasticsearch/OpenSearch support-diagnostics exports offline.

## Overview

This tool generates health check reports from pre-collected diagnostic data without requiring live cluster access. It's useful for:

- **Offline analysis** - Analyze customer clusters without direct access
- **Historical review** - Process diagnostics collected at different times
- **Sales preparation** - Identify consulting opportunities before customer meetings
- **Trend tracking** - Ship results to the trends database for longitudinal analysis

## Prerequisites

### Obtaining Diagnostic Exports

Customers collect diagnostics using the Elastic support-diagnostics tool:

```bash
# Download from: https://github.com/elastic/support-diagnostics

# Run against a cluster
./diagnostics.sh --host localhost --port 9200 --user elastic --password <pass>

# Output: api-diagnostics-YYYYMMDD-HHMMSS.zip
```

The tool produces a standardized export containing:
- Cluster health, settings, and state
- Node statistics and configuration
- Index mappings, settings, and stats
- Commercial/X-Pack feature usage
- Security configuration
- And 80+ other diagnostic files

## Usage

### Basic Usage

```bash
# From project root
python -m plugins.opensearch.import_diagnostics /path/to/diagnostic-export
```

### Command Line Options

```
usage: import_diagnostics.py [-h] [--output-dir OUTPUT_DIR] [--company COMPANY]
                             [--report-config REPORT_CONFIG] [--ship-trends]
                             [--trends-config TRENDS_CONFIG] [--json-only] [--quiet]
                             diagnostic_path

positional arguments:
  diagnostic_path       Path to diagnostic export (zip file or extracted directory)

optional arguments:
  -h, --help            show this help message and exit
  --output-dir, -o      Output directory for report (default: adoc_out/<company_name>)
  --company, -c         Company name for report identification (default: opensearch_import)
  --report-config, -r   Custom report configuration file (default: uses plugin default)
  --ship-trends         Ship results to trends database (requires trends config)
  --trends-config       Path to trends configuration file (default: config/trends.yaml)
  --json-only           Output only structured JSON findings (no AsciiDoc)
  --quiet, -q           Suppress progress output
```

### Examples

```bash
# Basic import from zip file
python -m plugins.opensearch.import_diagnostics ./api-diagnostics-20251120-145536.zip

# Import from extracted directory
python -m plugins.opensearch.import_diagnostics ./api-diagnostics-20251120-145536/

# With customer name (used for output directory and trends tracking)
python -m plugins.opensearch.import_diagnostics ./export.zip --company "Acme Corp"

# Custom output directory
python -m plugins.opensearch.import_diagnostics ./export.zip -o ./reports/acme

# JSON only (no AsciiDoc report)
python -m plugins.opensearch.import_diagnostics ./export.zip --json-only

# Ship to trends database for tracking
python -m plugins.opensearch.import_diagnostics ./export.zip --company "Acme Corp" --ship-trends

# Quiet mode (minimal output)
python -m plugins.opensearch.import_diagnostics ./export.zip -q
```

## Output

### Files Generated

```
adoc_out/<company_name>/
├── health_check.adoc                      # Full AsciiDoc report
└── structured_health_check_findings.json  # Structured data for querying
```

### Console Output

```
Loading diagnostic data from: ./api-diagnostics-20251120-145536
✅ Successfully loaded diagnostic export
   - Source: ./api-diagnostics-20251120-145536
   - Collection Date: 2025-11-20T14:55:36.466Z
   - Version: 7.10.2
   - Cluster: docker-cluster
   - Nodes: 1
   - Diagnostic Tool Version: 9.3.1
   - Files Loaded: 88

Generating health check report...
✅ AsciiDoc report: adoc_out/Acme_Corp/health_check.adoc
✅ Structured findings: adoc_out/Acme_Corp/structured_health_check_findings.json

============================================================
REPORT SUMMARY
============================================================
Cluster: docker-cluster
Version: 7.10.2
Checks executed: 30

🔥 Sales/Consulting Signals: 12
   • sso_opportunity (check_security_mappings)
   • compliance_gap (check_security_mappings)
   • ha_consulting_opportunity (check_zone_awareness)
   • dr_architecture_gap (check_remote_clusters)
   ...

⚡ Rules Triggered: 16
   🔴 CRITICAL: 1
   🟠 HIGH: 2
   🟡 MEDIUM: 8
   🔵 LOW: 2
   ⚪ INFO: 3

✅ Import complete!
```

## Shipping to Trends Database

Use `--ship-trends` to store results in the trends PostgreSQL database for historical analysis and sales opportunity tracking.

### Requirements

1. PostgreSQL trends database configured in `config/trends.yaml`:
   ```yaml
   destination: postgresql
   database:
     host: "localhost"
     port: 5432
     dbname: "health_trends"
     user: "postgres"
     password: "your_password"
     sslmode: "allow"
   ```

2. Encryption key file at `/var/lib/postgresql/key/key.txt` (for encrypted findings storage)

### Usage

```bash
python -m plugins.opensearch.import_diagnostics ./export.zip \
    --company "Acme Corp" \
    --ship-trends
```

### Data Stored

When shipping to trends, data is stored in two tables:

#### `health_check_runs` table
- Company association
- Cluster name and version
- Node count
- Full structured findings (encrypted)
- AsciiDoc report content
- Execution metadata (user, host, timestamp)
- Health score (calculated from rule evaluations)

#### `health_check_triggered_rules` table
- Individual rule violations linked to the run
- Severity level (critical, high, medium, low, info)
- Severity score (numeric, for ranking)
- Rule reasoning and recommendations
- Triggered data context

This separation allows for:
1. **Aggregate queries** - Find clusters with specific rule violations
2. **Trend analysis** - Track rule violations over time
3. **Sales targeting** - Query for specific opportunities across all customers

### Example: Triggered Rules Output

```
Shipping to trends database...
   Evaluated 6 triggered rules for storage
Log: Successfully connected to PostgreSQL for trend shipping.
Log: Successfully inserted health check run with ID: 1017
Log: Storing triggered rules for trend analysis...
Log: Stored 6 triggered rules for run 1017
Log: Issue summary - Critical: 1, High: 1, Medium: 4
```

### Technical Implementation

The `--ship-trends` flag triggers the following process:

1. **Rule Evaluation** - `generate_dynamic_prompt()` evaluates all rules against findings
2. **Issue Collection** - Critical, high, and medium issues are collected with full context
3. **Database Storage** - `_store_triggered_rules()` inserts each triggered rule into `health_check_triggered_rules`
4. **Findings Storage** - Full findings JSON is encrypted and stored in `health_check_runs`

This ensures that triggered rules are queryable independently of the full findings blob, enabling efficient sales opportunity queries.

## Querying Sales Opportunities

After shipping to trends, query opportunities:

```sql
-- Find clusters needing SSO
SELECT c.company_name, h.cluster_name, h.db_version
FROM health_check_runs h
JOIN companies c ON h.company_id = c.id
WHERE h.db_technology = 'opensearch';
```

See `SALES_OPPORTUNITY_QUERIES.md` for comprehensive query examples.

## Health Checks Performed

The import runs 30 health checks across these categories:

### Security
- CVE vulnerability analysis
- SSL certificate status
- Security audit (users, roles, privileges)
- Security mappings (SSO/LDAP/SAML)

### License & Compliance
- License status
- X-Pack feature usage and opportunities

### Cluster Overview
- Cluster health
- Cluster settings
- Shard allocation
- Thread pools
- Zone awareness (HA)

### Index Health
- Index health status
- ILM policies
- Templates
- Data streams

### Node & Resource Health
- Node metrics
- Disk usage

### Performance
- Search/indexing latency
- Cache hit ratios
- Thread pool queues

### Backup & Recovery
- Snapshot status
- Dangling indices

### Data Ingestion
- Ingest pipelines
- Enrich policies

### Alerting
- Watcher status

### Cross-Cluster
- CCR status
- Remote clusters

### Machine Learning
- ML job status

### Data Management
- Transforms
- Rollup jobs

### AWS (if applicable)
- Service software updates
- Auto-Tune status

### Diagnostics
- Hot threads
- Pending tasks
- Long-running tasks

## Troubleshooting

### "Data not available" for some checks

Some diagnostic files may be missing if:
- The cluster doesn't have certain features (e.g., no X-Pack)
- The diagnostic tool version is older
- Permissions prevented collection

The tool gracefully handles missing files and reports what's available.

### Encryption errors when shipping to trends

Ensure the encryption key file exists:
```bash
sudo mkdir -p /var/lib/postgresql/key
echo 'your-32-char-encryption-key-here' | sudo tee /var/lib/postgresql/key/key.txt
sudo chmod 600 /var/lib/postgresql/key/key.txt
sudo chown postgres:postgres /var/lib/postgresql/key/key.txt
```

### Connection errors to trends database

Verify PostgreSQL is accessible and credentials are correct in `config/trends.yaml`.

## Related Documentation

- `SALES_OPPORTUNITY_QUERIES.md` - SQL queries for sales analysis
- `ARCHITECTURE.md` - Plugin architecture overview
- `DIAGNOSTIC_QUERIES.md` - Diagnostic query reference
- `RULES_SUMMARY.md` - Health check rules documentation

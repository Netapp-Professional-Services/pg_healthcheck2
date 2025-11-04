# PostgreSQL AI Prompt Environment Context

## Overview

The PostgreSQL plugin now provides enhanced environment-aware context to the AI analysis system, enabling recommendations tailored to the specific hosting environment (Aurora, RDS, or bare metal/self-hosted).

## Implementation

### Files Modified

1. **`plugins/postgres/connector.py`**
   - Updated `get_db_metadata()` to return `environment` and `environment_details`
   - Environment detection was already implemented via `_detect_environment()` method
   - Returns: `'aurora'`, `'rds'`, `'bare_metal'`, or `'unknown'`

2. **`plugins/postgres/templates/prompts/default_prompt.j2`**
   - Enhanced to display environment-specific context
   - Shows Aurora version and AWS region when available
   - Provides environment-specific recommendations for Aurora, RDS, and bare metal

## Environment Detection Logic

The Postgres connector has sophisticated environment detection:

### AWS Aurora PostgreSQL
Detected using database-level signals with confidence scoring:
- **Signal 1:** Version string contains 'Aurora' (+40 points)
- **Signal 2:** `aurora_version()` function exists (+30 points)
- **Signal 3:** RDS-specific GUC parameters exist (+20 points)
- Threshold: 40+ points = Aurora detected

**Environment Details:**
```yaml
environment: aurora
environment_details:
  aurora_version: "14.6"
  version_string: "PostgreSQL 14.6 on ... Aurora PostgreSQL"
  rds_parameters_count: 12
  confidence_score: 90
  detection_method: aurora_system_functions
```

### AWS RDS PostgreSQL (non-Aurora)
Detected using role and configuration checks:
- **Signal 1:** AWS credentials in settings (+20 points)
- **Signal 2:** `rds_superuser` role exists (+40 points)
- Threshold: 40+ points = RDS detected

**Environment Details:**
```yaml
environment: rds
environment_details:
  aws_region: us-east-1
  rds_superuser_exists: true
  confidence_score: 60
  detection_method: rds_indicators
```

### Bare Metal / Self-Hosted
Default when AWS indicators are not detected.

**Environment Details:**
```yaml
environment: bare_metal
environment_details:
  detection_method: default
```

## AI Prompt Context

### For AWS Aurora PostgreSQL

The AI receives:
```
==== Analysis Context
- PostgreSQL Version: PostgreSQL 14.6 on ... Aurora PostgreSQL
- Analysis Timestamp: 2025-10-31T12:00:00Z
- Target Database: production_db
- Environment: AWS Aurora PostgreSQL
- Aurora Version: 14.6
- AWS Region: us-east-1

* Include Aurora-Specific Advice:
  - Aurora-specific features (Fast Clone, Backtrack, Global Database)
  - Read replica auto-scaling and cluster endpoints
  - Aurora Serverless considerations
  - Performance Insights for query analysis
  - Parameter groups and cluster parameter groups
  - Multi-AZ replication topology
  - Aurora Storage Auto-Scaling
  - Note limited access to OS-level metrics
```

### For AWS RDS PostgreSQL

The AI receives:
```
==== Analysis Context
- PostgreSQL Version: PostgreSQL 15.3
- Analysis Timestamp: 2025-10-31T12:00:00Z
- Target Database: production_db
- Environment: AWS RDS PostgreSQL
- AWS Region: us-west-2

* Include RDS-Specific Advice:
  - Read replica configuration and lag monitoring
  - Parameter groups and option groups
  - Multi-AZ deployment for high availability
  - Performance Insights for query analysis
  - RDS Enhanced Monitoring metrics
  - Automated backups and point-in-time recovery
  - Storage autoscaling configuration
  - Note limited access to OS-level metrics
```

### For Bare Metal / Self-Hosted PostgreSQL

The AI receives:
```
==== Analysis Context
- PostgreSQL Version: PostgreSQL 16.1
- Analysis Timestamp: 2025-10-31T12:00:00Z
- Target Database: production_db
- Environment: Self-Hosted PostgreSQL (Bare Metal / VM)

* Include Self-Hosted Considerations:
  - OS-level tuning (vm.swappiness, transparent huge pages)
  - Direct file system optimization (ext4, xfs, zfs)
  - Kernel parameters (shmmax, shmall, sem)
  - Direct hardware resource monitoring
  - Custom replication setup and monitoring
  - Manual backup and disaster recovery
  - PostgreSQL configuration file tuning
  - SSH access for system-level diagnostics
```

## Benefits

### Before Enhancement
- AI received generic Aurora flag via `settings.is_aurora`
- No distinction between Aurora and RDS
- No environment-specific guidance
- Missing region information

### After Enhancement
- AI receives detailed environment classification
- Aurora vs. RDS vs. bare metal distinctions
- Aurora version information visible
- AWS region displayed when available
- Environment-specific recommendations

## Example Use Cases

### Scenario 1: Aurora - High Connection Count
**Without Context:** "Increase max_connections to 1000"
**With Context:** "Aurora Reader endpoints automatically load-balance connections. Consider using Aurora Read Replicas with auto-scaling (1-15 replicas) and connection pooling (RDS Proxy) instead of increasing max_connections on the writer."

### Scenario 2: RDS - Storage Full
**Without Context:** "Add more disk space"
**With Context:** "Enable RDS Storage Autoscaling in the AWS Console (Modify DB Instance → Storage Autoscaling). Set Maximum storage threshold to 2000 GiB. This will automatically expand storage when utilization exceeds 90%."

### Scenario 3: Bare Metal - Checkpoint Tuning
**Without Context:** "Tune checkpoint_completion_target"
**With Context:** "Edit /etc/postgresql/16/main/postgresql.conf and increase checkpoint_completion_target to 0.9. Also consider increasing max_wal_size to 4GB if on SSD storage. Monitor checkpoint frequency with: SELECT * FROM pg_stat_bgwriter. Reload config with: pg_ctl reload"

### Scenario 4: Aurora - Bloat Issue
**Without Context:** "Run VACUUM FULL"
**With Context:** "Aurora storage automatically reclaims space - VACUUM FULL is not needed and will cause downtime. Instead, run VACUUM (without FULL) to update statistics. Consider Aurora Fast Clone for testing table rewrites without production impact."

## Configuration Examples

### Aurora Configuration
```yaml
db_type: postgres
host: mydb.cluster-abc123.us-east-1.rds.amazonaws.com
port: 5432
database: production
user: admin
password: secret

# Aurora detected automatically via database queries
# No additional configuration needed
```

### RDS Configuration
```yaml
db_type: postgres
host: mydb.abc123.us-west-2.rds.amazonaws.com
port: 5432
database: production
user: postgres
password: secret

# Optional: Enhance detection confidence
aws_region: us-west-2
db_identifier: mydb

# RDS detected via rds_superuser role
```

### Bare Metal Configuration
```yaml
db_type: postgres
host: postgres-server-01.example.com
port: 5432
database: production
user: postgres
password: secret

# SSH support for OS-level diagnostics
ssh_enabled: true
ssh_hosts:
  - host: postgres-server-01.example.com
    user: postgres
    key_file: ~/.ssh/id_rsa
```

### Explicit Environment Override
```yaml
db_type: postgres
host: managed-postgres.provider.com
port: 5432
database: production
user: admin
password: secret

# Force specific environment detection
environment_override: bare_metal
```

## Advanced Detection Features

### Confidence Scoring
The detector uses confidence scores to make reliable environment determinations:

- **Aurora:** Requires 40+ points (version string OR aurora_version() function)
- **RDS:** Requires 40+ points (typically rds_superuser role presence)
- **Multiple Signals:** Higher confidence when multiple detection signals agree

### Legacy Compatibility
Supports legacy `is_aurora` flag for backwards compatibility:
```yaml
# Old style (still works)
is_aurora: true

# New style (preferred)
# Auto-detected via database queries
```

## Future Enhancements

### Additional Environment Types
Could add detection for:
- `'azure_postgresql'` - Azure Database for PostgreSQL
- `'gcp_cloud_sql'` - Google Cloud SQL for PostgreSQL
- `'aiven'` - Aiven managed PostgreSQL
- `'elephantsql'` - ElephantSQL hosted PostgreSQL
- `'crunchy_bridge'` - Crunchy Bridge managed PostgreSQL

### Enhanced Metadata
- Replication topology (primary/replica role)
- Cluster size (number of nodes)
- Storage backend (EBS, Aurora Storage, local SSD)
- Managed service provider details

### Detection Improvements
- Check for Azure-specific system views
- Detect GCP Cloud SQL via special roles
- Identify managed Postgres providers via DNS patterns

## Backwards Compatibility

All changes are backwards compatible:
- Old templates continue to work (they just won't use new variables)
- Legacy `is_aurora` flag still supported
- New fields are additive only

## Testing

To test environment detection:

```bash
# Run health check
python main.py --config config/postgres.yaml

# Check detected environment
grep -A 5 "Analysis Context" adoc_out/*/health_check.adoc
```

To verify detection in Python:
```python
from plugins.postgres import PostgresPlugin
plugin = PostgresPlugin(settings)
connector = plugin.get_connector(settings)
connector.connect()
metadata = connector.get_db_metadata()
print(f"Environment: {metadata['environment']}")
print(f"Details: {metadata['environment_details']}")
```

---

**Last Updated:** 2025-10-31
**Related Files:** connector.py, default_prompt.j2

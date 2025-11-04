# OpenSearch AI Prompt Environment Context

## Overview

The OpenSearch plugin now provides environment-aware context to the AI analysis system, enabling recommendations tailored to the specific hosting environment (AWS OpenSearch Service, self-hosted, managed services).

## Implementation

### Files Modified

1. **`plugins/opensearch/connector.py`**
   - Updated `get_db_metadata()` to return `environment` and `environment_details`
   - Environment is detected via `_detect_environment()` method
   - Returns: `'aws'`, `'self_hosted'`, or `'unknown'`

2. **`main.py`**
   - Changed to pass full `db_metadata` dict to `generate_dynamic_prompt()`
   - Previously only passed `db_version` and `db_name`

3. **`utils/dynamic_prompt_generator.py`**
   - Updated function signature to accept `db_metadata` instead of individual parameters
   - Passes `environment` and `environment_details` to Jinja2 template

4. **`plugins/opensearch/templates/prompts/default_prompt.j2`**
   - Enhanced to display environment-specific context
   - Includes AWS region, domain name when available
   - Shows node count for self-hosted deployments
   - Provides environment-specific recommendations

5. **`scripts/rules_tester.py`** and **`utils/offline_ai_processor.py`**
   - Updated to create `db_metadata` dict for offline analysis
   - Sets environment to `'unknown'` when processing offline findings

## Environment Detection Logic

### AWS OpenSearch Service
Detected when any of these settings are present:
- `aws_region`
- `is_aws_opensearch: true`
- `aws_domain_name`

**Environment Details:**
```yaml
environment: aws
environment_details:
  type: aws
  region: us-east-1
  domain_name: my-domain
  endpoint: https://search-domain.us-east-1.es.amazonaws.com
```

### Self-Hosted / Managed Services
Detected when AWS indicators are not present.

**Environment Details:**
```yaml
environment: self_hosted
environment_details:
  type: self_hosted
  hosts: ['node1.example.com', 'node2.example.com']
```

**Note:** Currently, managed services like Instaclustr are classified as `self_hosted`. This is reasonable since they're not AWS, but could be enhanced in future to distinguish managed vs. truly self-hosted.

## AI Prompt Context

### For AWS OpenSearch Service

The AI receives:
```
==== Analysis Context
- OpenSearch Version: 2.11.0
- Analysis Timestamp: 2025-10-31T12:00:00Z
- Target Cluster: my-production-cluster
- Environment: AWS OpenSearch Service
- Region: us-east-1
- Domain Name: my-domain
```

**AI Instructions Include:**
- Auto-Tune recommendations
- Service software updates guidance
- Multi-AZ deployment considerations
- VPC security and endpoint policies
- CloudWatch metrics integration
- Note about lack of OS-level access

### For Self-Hosted OpenSearch

The AI receives:
```
==== Analysis Context
- OpenSearch Version: 2.11.0
- Analysis Timestamp: 2025-10-31T12:00:00Z
- Target Cluster: my-production-cluster
- Environment: Self-Hosted OpenSearch
- Cluster Nodes: 3 node(s)
```

**AI Instructions Include:**
- OS-level tuning (file descriptors, vm.max_map_count, swappiness)
- SSH access for diagnostics
- Manual upgrade procedures
- Hardware resource optimization
- Backup and DR strategies
- Network and firewall configuration

## Benefits

### Before Enhancement
- AI received generic "AWS" or "Self-Hosted" label
- Recommendations were not tailored to environment capabilities
- Missing region/domain context for AWS
- No node count information for self-hosted

### After Enhancement
- AI receives rich environment metadata
- Recommendations are environment-specific
- AWS deployments get AWS-specific guidance
- Self-hosted deployments get OS-level guidance
- Managed services are appropriately classified

## Example Use Cases

### Scenario 1: AWS OpenSearch High Heap Usage
**Without Context:** "Increase heap size to 32GB"
**With Context:** "Use AWS Console to change instance type to r6g.2xlarge.search (32GB heap). This will trigger a blue/green deployment with no downtime."

### Scenario 2: Self-Hosted High Disk I/O
**Without Context:** "Reduce disk I/O"
**With Context:** "SSH to nodes and check `iostat -x 1`. Consider increasing vm.dirty_ratio and vm.dirty_background_ratio for write-heavy workloads. Check if data nodes are on SSD vs. HDD."

### Scenario 3: Managed Service (Instaclustr)
**Current Classification:** self_hosted
**AI Guidance:** Will provide self-hosted recommendations, but with SSH-related items the user can skip

## Future Enhancements

### Managed Service Detection
Could add explicit detection for managed services:

```python
def _detect_environment(self):
    # Detect AWS
    if self.settings.get('is_aws_opensearch'):
        return 'aws', {...}

    # Detect managed services
    if self.settings.get('is_managed_service'):
        provider = self.settings.get('managed_service_provider', 'unknown')
        return 'managed', {'provider': provider, 'hosts': [...]}

    # Default to self-hosted
    return 'self_hosted', {'hosts': [...]}
```

### Additional Environment Types
- `'elastic_cloud'` - Elastic Cloud managed service
- `'aiven'` - Aiven managed service
- `'instaclustr'` - Instaclustr managed service
- `'on_premise'` - Explicitly self-hosted

### Enhanced Metadata
- Cluster tier (dev, staging, production)
- Infrastructure type (bare metal, VM, container)
- Storage backend (EBS, local SSD, NFS)
- Networking model (flat, VPC, isolated)

## Testing

To test the environment detection:

```bash
# Run health check and examine the prompt
python main.py --config config/opensearch_instaclustr.yaml

# Check the generated prompt in adoc_out/
grep -A 10 "Analysis Context" adoc_out/*/health_check.adoc
```

To verify environment detection in Python:

```python
from plugins.opensearch import OpenSearchPlugin
plugin = OpenSearchPlugin(settings)
connector = plugin.get_connector(settings)
metadata = connector.get_db_metadata()
print(f"Environment: {metadata['environment']}")
print(f"Details: {metadata['environment_details']}")
```

## Backwards Compatibility

All changes are backwards compatible:
- Old templates still work (they just won't use new variables)
- Old callers of `get_db_metadata()` still receive `version` and `db_name`
- New fields are additive, not breaking

---

**Last Updated:** 2025-10-31
**Related Files:** connector.py, main.py, dynamic_prompt_generator.py, default_prompt.j2

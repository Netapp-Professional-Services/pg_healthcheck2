"""
AWS OpenSearch Service Software Check

AWS-specific check for service software updates, Auto-Tune status, and domain configuration.
Only runs for AWS OpenSearch Service environments.
"""

import logging
from plugins.common.check_helpers import require_aws, CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 6


def run_check_aws_service_software(connector, settings):
    """
    Check AWS OpenSearch Service configuration and updates.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)

    # Only run for AWS environments
    if connector.environment != 'aws':
        builder.h3("AWS OpenSearch Service Software")
        builder.note(
            "This check is only applicable for AWS OpenSearch Service domains.\n\n"
            "Current environment is self-hosted OpenSearch."
        )
        return builder.build(), {
            "status": "skipped",
            "data": {"reason": "Not AWS environment"}
        }

    # Check AWS availability
    aws_ok, skip_msg, skip_data = require_aws(connector, "AWS service software check")
    if not aws_ok:
        return skip_msg, skip_data

    builder.h3("AWS OpenSearch Service Configuration")
    builder.para("Review of AWS-managed service configuration, updates, and recommendations.")

    try:
        domain_name = connector.environment_details.get('domain_name')
        if not domain_name:
            builder.warning("AWS domain name not configured - cannot fetch service details")
            return builder.build(), {
                "status": "error",
                "error": "Domain name not configured"
            }

        # Get domain configuration from AWS API
        opensearch_client = connector._opensearch_client
        if not opensearch_client:
            builder.warning("AWS OpenSearch client not initialized - some checks unavailable")
            return builder.build(), {
                "status": "partial",
                "data": {"reason": "API client unavailable"}
            }

        # Describe domain
        try:
            response = opensearch_client.describe_domain(DomainName=domain_name)
            domain_status = response.get('DomainStatus', {})
        except Exception as e:
            logger.error(f"Could not describe domain: {e}")
            builder.error(f"Could not retrieve domain information: {e}")
            return builder.build(), {
                "status": "error",
                "error": str(e)
            }

        # Extract key configuration
        opensearch_version = domain_status.get('EngineVersion', 'Unknown')
        service_software = domain_status.get('ServiceSoftwareOptions', {})
        auto_tune_options = domain_status.get('AutoTuneOptions', {})
        domain_endpoint_options = domain_status.get('DomainEndpointOptions', {})

        # Display version information
        builder.h4("OpenSearch Version")
        version_data = [
            {"Setting": "Engine Version", "Value": opensearch_version},
            {"Setting": "Created", "Value": domain_status.get('Created', 'Unknown')},
            {"Setting": "Endpoint", "Value": domain_status.get('Endpoint', 'Unknown')}
        ]
        builder.table(version_data)

        # Service software updates
        builder.h4("Service Software Updates")
        current_version = service_software.get('CurrentVersion', 'Unknown')
        new_version = service_software.get('NewVersion', '')
        update_available = service_software.get('UpdateAvailable', False)
        update_status = service_software.get('UpdateStatus', 'Unknown')
        cancellable = service_software.get('Cancellable', False)
        optional_deployment = service_software.get('OptionalDeployment', False)

        if update_available and new_version:
            builder.warning_issue(
                "Service Software Update Available",
                {
                    "Current Version": current_version,
                    "New Version": new_version,
                    "Update Status": update_status,
                    "Optional": "Yes" if optional_deployment else "No (Required)",
                    "Cancellable": "Yes" if cancellable else "No"
                }
            )
        else:
            builder.note(f"✅ Service software is up to date (Version: {current_version})")

        # Auto-Tune status
        builder.h4("Auto-Tune Configuration")
        autotune_state = auto_tune_options.get('State', 'Unknown')
        autotune_desired = auto_tune_options.get('DesiredState', 'Unknown')

        autotune_status = "✅ Enabled" if autotune_state == 'ENABLED' else f"⚠️ {autotune_state}"
        autotune_data = [
            {"Setting": "Auto-Tune Status", "Value": autotune_status},
            {"Setting": "Desired State", "Value": autotune_desired}
        ]

        if autotune_state != 'ENABLED':
            builder.warning(
                "Auto-Tune is not enabled. AWS recommends enabling Auto-Tune to automatically "
                "optimize cluster performance based on workload patterns."
            )

        builder.table(autotune_data)

        # Domain endpoint options
        builder.h4("Security & Network Configuration")
        enforce_https = domain_endpoint_options.get('EnforceHTTPS', False)
        tls_policy = domain_endpoint_options.get('TLSSecurityPolicy', 'Unknown')

        vpc_options = domain_status.get('VPCOptions', {})
        in_vpc = bool(vpc_options.get('VPCId'))

        security_data = [
            {"Setting": "Enforce HTTPS", "Value": "✅ Yes" if enforce_https else "⚠️ No"},
            {"Setting": "TLS Policy", "Value": tls_policy},
            {"Setting": "VPC Deployment", "Value": "✅ Yes" if in_vpc else "⚠️ Public"},
            {"Setting": "VPC ID", "Value": vpc_options.get('VPCId', 'N/A')}
        ]
        builder.table(security_data)

        if not enforce_https:
            builder.warning("HTTPS is not enforced - recommended to enable for security")

        if not in_vpc:
            builder.warning(
                "Domain is publicly accessible. For production, consider deploying in VPC "
                "for enhanced security and network isolation."
            )

        # Cluster configuration
        builder.h4("Cluster Configuration")
        cluster_config = domain_status.get('ClusterConfig', {})

        instance_type = cluster_config.get('InstanceType', 'Unknown')
        instance_count = cluster_config.get('InstanceCount', 0)
        dedicated_master_enabled = cluster_config.get('DedicatedMasterEnabled', False)
        dedicated_master_type = cluster_config.get('DedicatedMasterType', 'N/A')
        dedicated_master_count = cluster_config.get('DedicatedMasterCount', 0)
        zone_awareness_enabled = cluster_config.get('ZoneAwarenessEnabled', False)

        cluster_data = [
            {"Setting": "Instance Type", "Value": instance_type},
            {"Setting": "Instance Count", "Value": instance_count},
            {"Setting": "Dedicated Masters", "Value": "✅ Yes" if dedicated_master_enabled else "⚠️ No"},
            {"Setting": "Master Instance Type", "Value": dedicated_master_type if dedicated_master_enabled else "N/A"},
            {"Setting": "Master Count", "Value": dedicated_master_count if dedicated_master_enabled else "N/A"},
            {"Setting": "Multi-AZ", "Value": "✅ Enabled" if zone_awareness_enabled else "⚠️ Disabled"}
        ]
        builder.table(cluster_data)

        if not dedicated_master_enabled:
            builder.warning(
                "Dedicated master nodes are not enabled. For production clusters, AWS recommends "
                "using dedicated master nodes for improved stability."
            )

        if not zone_awareness_enabled:
            builder.warning(
                "Multi-AZ deployment is not enabled. For production, enable zone awareness "
                "for high availability across availability zones."
            )

        # Recommendations
        recs = {"high": [], "general": []}

        if update_available and not optional_deployment:
            recs["high"].append("Schedule required service software update during maintenance window")

        if autotune_state != 'ENABLED':
            recs["high"].append("Enable Auto-Tune for automatic performance optimization")

        if not dedicated_master_enabled:
            recs["high"].append("Enable dedicated master nodes for production clusters")

        if not zone_awareness_enabled:
            recs["high"].append("Enable Multi-AZ deployment for high availability")

        if not enforce_https:
            recs["high"].append("Enable HTTPS enforcement for security")

        if not in_vpc:
            recs["high"].append("Deploy domain in VPC for enhanced security")

        recs["general"].extend([
            "Review CloudWatch metrics and set up alarms for key metrics",
            "Enable automated snapshots (AWS handles this automatically)",
            "Review IAM policies for least-privilege access",
            "Implement fine-grained access control if not already enabled",
            "Regular review of instance types and sizing based on workload"
        ])

        if recs["high"]:
            builder.recs(recs)
        else:
            builder.success("✅ AWS OpenSearch Service is well-configured.")
            builder.recs({"general": recs["general"]})

        # Return structured data with proper format for rules engine
        return builder.build(), {
            "status": "success",
            "data": {
                "opensearch_version": opensearch_version,
                "service_software_current": current_version,
                "update_available": update_available,
                "autotune_enabled": autotune_state == 'ENABLED',
                "dedicated_masters": dedicated_master_enabled,
                "multi_az": zone_awareness_enabled,
                "in_vpc": in_vpc,
                "https_enforced": enforce_https,
                "recommendations_count": len(recs["high"]),
                "has_required_update": update_available and not optional_deployment,
                "has_security_issues": not enforce_https or not in_vpc,
                "has_ha_issues": not dedicated_master_enabled or not zone_awareness_enabled,
            }
        }

    except Exception as e:
        logger.error(f"AWS service check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

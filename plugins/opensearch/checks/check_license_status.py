"""
License status and expiry monitoring for Elasticsearch clusters.

This check analyzes the cluster license to identify:
- License type and features available
- License expiry date (for non-basic licenses)
- Licenses that are expired or expiring soon

Data source:
- Live clusters: GET /_license API
- Imported diagnostics: licenses.json

Note: OpenSearch does not use Elasticsearch licensing - it's fully open source.
This check is primarily for Elasticsearch clusters. For OpenSearch clusters,
it will report that licensing is not applicable.
"""

from datetime import datetime, timezone
from plugins.common.check_helpers import CheckContentBuilder


# License type descriptions and feature availability
LICENSE_TYPES = {
    "basic": {
        "description": "Free tier with core features",
        "has_expiry": False,
        "features": ["Core Elasticsearch", "Kibana", "Basic Security", "Canvas"]
    },
    "trial": {
        "description": "30-day trial with all platinum features",
        "has_expiry": True,
        "features": ["All Platinum features for evaluation"]
    },
    "standard": {
        "description": "Paid tier with additional features",
        "has_expiry": True,
        "features": ["Basic features", "Java Low-Level REST Client Support"]
    },
    "gold": {
        "description": "Paid tier with monitoring and security",
        "has_expiry": True,
        "features": ["Standard features", "Monitoring", "Alerting", "Graph"]
    },
    "platinum": {
        "description": "Full-featured commercial license",
        "has_expiry": True,
        "features": ["Gold features", "Machine Learning", "Advanced Security", "Cross-Cluster Replication"]
    },
    "enterprise": {
        "description": "Enterprise-grade features",
        "has_expiry": True,
        "features": ["Platinum features", "Searchable Snapshots", "Enterprise Support"]
    }
}


def run(connector, settings):
    """
    Check license status and expiry.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("License Status")

    try:
        # Get license data
        license_data = connector.execute_query({"operation": "licenses"})

        if isinstance(license_data, dict) and "error" in license_data:
            # Check if this might be OpenSearch
            version_info = getattr(connector, '_version_info', {})
            version_str = version_info.get('number', '')

            # OpenSearch versions start with 1.x or 2.x typically
            if version_str and not version_str.startswith('7.') and not version_str.startswith('8.'):
                builder.note(
                    "**OpenSearch Detected**\n\n"
                    "OpenSearch is fully open source and does not require licensing. "
                    "All features are available without license restrictions."
                )
                return builder.build(), {
                    "status": "not_applicable",
                    "reason": "OpenSearch does not use licensing"
                }

            builder.note(
                "License information is not available.\n\n"
                "This may indicate:\n"
                "- License API is not accessible\n"
                "- Diagnostic export did not include licenses.json"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": license_data.get("error", "Unknown")
            }

        # Extract license info
        license_info = license_data.get("license", license_data)
        if not license_info:
            builder.warning("No license information found")
            return builder.build(), {
                "status": "unavailable",
                "reason": "Empty license response"
            }

        # Parse license details
        license_type = license_info.get("type", "unknown").lower()
        license_status = license_info.get("status", "unknown").lower()
        issued_to = license_info.get("issued_to", "Unknown")
        issuer = license_info.get("issuer", "Unknown")
        issue_date = license_info.get("issue_date", "Unknown")
        max_nodes = license_info.get("max_nodes", "Unlimited")
        uid = license_info.get("uid", "Unknown")

        # Get license type info
        type_info = LICENSE_TYPES.get(license_type, {
            "description": f"Unknown license type: {license_type}",
            "has_expiry": True,
            "features": []
        })

        # Check expiry
        expiry_date = license_info.get("expiry_date") or license_info.get("expiry")
        expiry_date_millis = license_info.get("expiry_date_in_millis")
        days_until_expiry = None
        is_expired = False
        expiry_str = "Never (perpetual)"

        if expiry_date or expiry_date_millis:
            now = datetime.now(timezone.utc)

            if expiry_date_millis:
                expiry_dt = datetime.fromtimestamp(expiry_date_millis / 1000, tz=timezone.utc)
            else:
                try:
                    if expiry_date.endswith("Z"):
                        expiry_dt = datetime.fromisoformat(expiry_date.replace("Z", "+00:00"))
                    else:
                        expiry_dt = datetime.fromisoformat(expiry_date)
                        if expiry_dt.tzinfo is None:
                            expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
                except:
                    expiry_dt = None

            if expiry_dt:
                days_until_expiry = (expiry_dt - now).days
                is_expired = days_until_expiry < 0
                expiry_str = expiry_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

        # Build report
        if is_expired:
            builder.critical(
                f"🔴 **LICENSE EXPIRED**\n\n"
                f"The {license_type} license expired {abs(days_until_expiry)} days ago.\n"
                "Some features may be disabled or degraded."
            )
        elif days_until_expiry is not None and days_until_expiry < 7:
            builder.critical(
                f"🔴 **License expiring in {days_until_expiry} days**\n\n"
                "Renew immediately to avoid service disruption."
            )
        elif days_until_expiry is not None and days_until_expiry < 30:
            builder.warning(
                f"⚠️ **License expiring in {days_until_expiry} days**\n\n"
                "Plan renewal to maintain feature access."
            )
        elif license_status == "active":
            builder.success(
                f"✅ **License is active** ({license_type})"
            )
        else:
            builder.warning(f"⚠️ License status: {license_status}")

        builder.blank()
        builder.h4("License Details")

        # Build details table
        details_data = [
            {"Property": "License Type", "Value": f"{license_type.title()} - {type_info['description']}"},
            {"Property": "Status", "Value": license_status.upper()},
            {"Property": "Issued To", "Value": str(issued_to)},
            {"Property": "Issuer", "Value": str(issuer)},
            {"Property": "Issue Date", "Value": str(issue_date)},
            {"Property": "Expiry Date", "Value": expiry_str},
            {"Property": "Max Nodes", "Value": str(max_nodes)},
            {"Property": "License UID", "Value": str(uid)},
        ]

        if days_until_expiry is not None:
            if days_until_expiry < 0:
                days_str = f"**EXPIRED** ({abs(days_until_expiry)} days ago)"
            elif days_until_expiry < 7:
                days_str = f"**{days_until_expiry} days** 🔴"
            elif days_until_expiry < 30:
                days_str = f"**{days_until_expiry} days** ⚠️"
            else:
                days_str = f"{days_until_expiry} days ✅"
            details_data.insert(6, {"Property": "Days Until Expiry", "Value": days_str})

        builder.table(details_data)
        builder.blank()

        # Show features if known
        if type_info.get("features"):
            builder.h4("License Features")
            for feature in type_info["features"]:
                builder.text(f"- {feature}")
            builder.blank()

        # Recommendations
        if is_expired:
            builder.text("*Recommended Actions:*")
            builder.text("1. **URGENT:** Contact Elastic to renew license")
            builder.text("2. Review which features are currently degraded")
            builder.text("3. Consider downgrading to basic license if renewal is not immediate")
        elif days_until_expiry is not None and days_until_expiry < 30:
            builder.text("*Recommended Actions:*")
            builder.text("1. Contact Elastic to initiate license renewal")
            builder.text("2. Budget for license renewal costs")
            builder.text("3. Plan any required maintenance windows for license updates")

        # Build structured data - flat structure for rules engine
        structured_data = {
            "status": "success",
            "data": {
                "license_type": license_type,
                "license_status": license_status,
                "issued_to": issued_to,
                "issuer": issuer,
                "max_nodes": max_nodes,
                "is_expired": is_expired,
                "days_until_expiry": days_until_expiry,
                "expiry_date": expiry_str if expiry_date else None,
                "has_expiry": type_info.get("has_expiry", True),
                "is_basic_license": license_type == "basic"
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check license status: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

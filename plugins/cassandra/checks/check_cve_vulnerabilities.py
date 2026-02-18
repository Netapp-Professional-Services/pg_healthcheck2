"""
Cassandra CVE Vulnerability Check

Queries the National Vulnerability Database (NVD) for known CVEs affecting
the current Cassandra version.

This check uses the NVD REST API v2.0 to retrieve vulnerability information.
An optional API key can be configured to increase rate limits from 5 to 50
requests per 30 seconds.

Configuration:
    nvd_api_key: Optional NVD API key (get from https://nvd.nist.gov/developers/request-an-api-key)

Example Output:
    - Lists CVEs by severity (Critical, High, Medium, Low)
    - Provides CVSS scores and descriptions
    - Links to detailed vulnerability information
    - Checks Cassandra core
"""

from plugins.common.check_helpers import CheckContentBuilder
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module."""
    return 9  # Security vulnerabilities are critical


def run(connector, settings):
    """
    Check for known CVEs affecting Cassandra.

    Args:
        connector: Cassandra connector (must have CVECheckMixin)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder()

    builder.h3("Security Vulnerability Analysis (CVE)")

    # Check if connector supports CVE checking
    if not hasattr(connector, 'has_cve_support') or not connector.has_cve_support():
        builder.note(
            "CVE checking is not available. To enable CVE checks:\\n\\n"
            "1. Ensure internet access to NVD API (https://services.nvd.nist.gov)\\n"
            "2. Optionally add `nvd_api_key` to your config for higher rate limits\\n"
            "   (Get key at: https://nvd.nist.gov/developers/request-an-api-key)"
        )
        return builder.build(), {
            'status': 'unavailable',
            'reason': 'cve_support_not_initialized'
        }

    # Get current version and environment
    version_string = connector.version_info.get('version_string', 'Unknown')
    major_version = connector.version_info.get('major_version', 0)
    env_name = getattr(connector, 'environment', 'unknown')
    if env_name:
        env_name = env_name.upper()
    else:
        env_name = 'UNKNOWN'

    builder.h4("Current Configuration")
    builder.text(f"- **Cassandra Version:** {version_string}")
    builder.text(f"- **Major Version:** {major_version}")
    builder.text(f"- **Environment:** {env_name}")
    builder.blank()

    # Check core Cassandra CVEs
    builder.h4("Cassandra Core Vulnerabilities")

    logger.info(f"Querying NVD for Cassandra {version_string} CVEs")
    core_cves = connector.get_core_cves(max_results=100)  # Limit to 100 most relevant CVEs

    structured_data = {
        'status': core_cves.get('status'),
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'data': {
            'cassandra_version': version_string,
            'major_version': major_version,
            'environment': env_name,
            'core': core_cves
        }
    }

    if core_cves['status'] == 'error':
        error_msg = core_cves.get('error', 'Unknown error')
        builder.error(f"❌ Failed to retrieve CVE data from NVD: {error_msg}")
        builder.blank()
        builder.text("*Possible causes:*")
        builder.blank()
        builder.text("- Network connectivity issues")
        builder.text("- NVD API service unavailable")
        builder.text("- Rate limit exceeded (consider adding nvd_api_key to config)")
        return builder.build(), structured_data

    if core_cves['status'] == 'unavailable':
        builder.warning(f"⚠️ CVE data unavailable: {core_cves.get('error', 'Unknown reason')}")
        return builder.build(), structured_data

    # Display core CVE summary
    total_cves = core_cves.get('total_cves', 0)
    severity_counts = core_cves.get('severity_counts', {})

    if total_cves == 0:
        builder.success(f"✅ No known CVEs found for Cassandra {version_string}")
        builder.blank()
        builder.text("*Note:* This indicates no CVEs specifically match this exact version in NVD.")
        builder.text("Always review Apache Cassandra security announcements for the latest information:")
        builder.text("https://cassandra.apache.org/")
    else:
        critical_count = severity_counts.get('critical', 0)
        high_count = severity_counts.get('high', 0)
        medium_count = severity_counts.get('medium', 0)
        low_count = severity_counts.get('low', 0)

        # Show appropriate alert level
        if critical_count > 0:
            builder.critical(f"🔴 **{critical_count} Critical CVE(s)** found for Cassandra {version_string}")
        elif high_count > 0:
            builder.error(f"🟠 **{high_count} High severity CVE(s)** found for Cassandra {version_string}")
        elif medium_count > 0:
            builder.warning(f"🟡 **{medium_count} Medium severity CVE(s)** found for Cassandra {version_string}")
        else:
            builder.note(f"**{total_cves} Low severity CVE(s)** found for Cassandra {version_string}")

        builder.blank()

        # Summary counts
        builder.text("*Severity Breakdown:*")
        builder.blank()
        if critical_count > 0:
            builder.text(f"- 🔴 Critical: {critical_count}")
        if high_count > 0:
            builder.text(f"- 🟠 High: {high_count}")
        if medium_count > 0:
            builder.text(f"- 🟡 Medium: {medium_count}")
        if low_count > 0:
            builder.text(f"- ⚪ Low: {low_count}")
        builder.blank()

        # CVE details table
        cve_list = core_cves.get('cves', [])
        if cve_list:
            # Sort by severity (Critical -> High -> Medium -> Low) and CVSS score
            severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3, 'UNKNOWN': 4}
            sorted_cves = sorted(
                cve_list,
                key=lambda x: (severity_order.get(x['severity'], 5), -x['cvss_score'])
            )

            # Show top 20 CVEs (to keep report size manageable)
            display_cves = sorted_cves[:20]

            cve_table = []
            for cve in display_cves:
                # Format published date
                pub_date = cve.get('published', '')[:10] if cve.get('published') else 'N/A'

                # Truncate description for table
                desc = cve.get('description', 'No description available')
                if len(desc) > 100:
                    desc = desc[:97] + '...'

                # Build NVD link
                cve_id = cve.get('cve_id', 'N/A')
                nvd_link = f"https://nvd.nist.gov/vuln/detail/{cve_id}"

                # Format severity with color indicator
                severity = cve.get('severity', 'UNKNOWN')
                if severity == 'CRITICAL':
                    severity_display = '🔴 Critical'
                elif severity == 'HIGH':
                    severity_display = '🟠 High'
                elif severity == 'MEDIUM':
                    severity_display = '🟡 Medium'
                elif severity == 'LOW':
                    severity_display = '⚪ Low'
                else:
                    severity_display = '⚫ Unknown'

                cve_table.append({
                    'CVE ID': f"link:{nvd_link}[{cve_id}]",
                    'Severity': severity_display,
                    'CVSS': f"{cve.get('cvss_score', 0.0):.1f}",
                    'Published': pub_date,
                    'Description': desc
                })

            builder.table(cve_table)

            if len(cve_list) > 20:
                builder.text(f"\\n_Showing 20 of {len(cve_list)} CVEs. Review full list at NVD._")

            builder.blank()

            # Add note about false positives
            builder.note(
                "**About These Results:** NVD's CPE matching may include CVEs for Cassandra "
                "drivers, client libraries, or older versions. Review each CVE's 'Description' "
                "and 'Published' date to determine relevance to your installation."
            )
            builder.blank()

        # Recommendations based on severity
        recommendations = []

        if critical_count > 0:
            recommendations.append("**URGENT:** Address critical CVEs immediately - these pose severe security risks")
            recommendations.append("Review each critical CVE to determine exploitability in your environment")
            recommendations.append("Implement temporary mitigations if immediate patching is not possible")

        if total_cves > 0:
            recommendations.extend([
                f"Upgrade to latest Cassandra {major_version}.x minor version to address known vulnerabilities",
                "Review detailed CVE information at NVD: https://nvd.nist.gov",
                "Check Apache Cassandra security announcements: https://cassandra.apache.org/",
                "Subscribe to Cassandra security mailing list for updates"
            ])

        # Environment-specific recommendations
        if env_name == 'DSE':
            recommendations.append("Contact DataStax Support for managed patching schedules and available versions")
            recommendations.append("Review DataStax Security Advisories: https://www.datastax.com/legal/security-advisories")
        elif env_name == 'INSTACLUSTR':
            recommendations.append("Contact Instaclustr Support for managed upgrade schedules")
            recommendations.append("Review Instaclustr managed cluster upgrade options")

        if recommendations:
            builder.recs(recommendations)

    # Add informational footer
    builder.h4("About This Check")
    builder.text("This check queries the NIST National Vulnerability Database (NVD) API v2.0.")
    builder.text("CVE data is updated continuously by NIST and the security community.")
    builder.blank()
    builder.text("*Rate Limits:*")
    builder.blank()
    builder.text("- Without API key: 5 requests per 30 seconds")
    builder.text("- With API key: 50 requests per 30 seconds")
    builder.blank()
    builder.text("Get an API key at: https://nvd.nist.gov/developers/request-an-api-key")

    return builder.build(), structured_data

"""
PostgreSQL CVE Vulnerability Check

Queries the National Vulnerability Database (NVD) for known CVEs affecting
the current PostgreSQL version and installed extensions.

This check uses the NVD REST API v2.0 to retrieve vulnerability information.
An optional API key can be configured to increase rate limits from 5 to 50
requests per 30 seconds.

Configuration:
    nvd_api_key: Optional NVD API key (get from https://nvd.nist.gov/developers/request-an-api-key)

Example Output:
    - Lists CVEs by severity (Critical, High, Medium, Low)
    - Provides CVSS scores and descriptions
    - Links to detailed vulnerability information
    - Checks both PostgreSQL core and installed extensions
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
    Check for known CVEs affecting PostgreSQL core and extensions.

    Args:
        connector: PostgreSQL connector (must have CVECheckMixin)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder()

    builder.h3("Security Vulnerability Analysis (CVE)")

    # Check if connector supports CVE checking
    if not hasattr(connector, 'has_cve_support') or not connector.has_cve_support():
        builder.note(
            "CVE checking is not available. To enable CVE checks:\n\n"
            "1. Ensure internet access to NVD API (https://services.nvd.nist.gov)\n"
            "2. Optionally add `nvd_api_key` to your config for higher rate limits\n"
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
    builder.text(f"- **PostgreSQL Version:** {version_string}")
    builder.text(f"- **Major Version:** {major_version}")
    builder.text(f"- **Environment:** {env_name}")
    builder.blank()

    # Check core PostgreSQL CVEs
    builder.h4("PostgreSQL Core Vulnerabilities")

    logger.info(f"Querying NVD for PostgreSQL {version_string} CVEs")
    core_cves = connector.get_core_cves(max_results=100)  # Limit to 100 most relevant CVEs

    structured_data = {
        'status': core_cves.get('status'),
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'data': {
            'postgres_version': version_string,
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
        builder.success(f"✅ No known CVEs found for PostgreSQL {version_string}")
        builder.blank()
        builder.text("*Note:* This indicates no CVEs specifically match this exact version in NVD.")
        builder.text("Always review PostgreSQL security announcements for the latest information:")
        builder.text("https://www.postgresql.org/support/security/")
    else:
        critical_count = severity_counts.get('critical', 0)
        high_count = severity_counts.get('high', 0)
        medium_count = severity_counts.get('medium', 0)
        low_count = severity_counts.get('low', 0)

        # Show appropriate alert level
        if critical_count > 0:
            builder.critical(f"🔴 **{critical_count} Critical CVE(s)** found for PostgreSQL {version_string}")
        elif high_count > 0:
            builder.error(f"🟠 **{high_count} High severity CVE(s)** found for PostgreSQL {version_string}")
        elif medium_count > 0:
            builder.warning(f"🟡 **{medium_count} Medium severity CVE(s)** found for PostgreSQL {version_string}")
        else:
            builder.note(f"**{total_cves} Low severity CVE(s)** found for PostgreSQL {version_string}")

        builder.blank()

        # Summary counts
        builder.text("*Severity Breakdown:*")
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
                builder.text(f"\n_Showing 20 of {len(cve_list)} CVEs. Review full list at NVD._")

            builder.blank()

            # Add note about false positives
            builder.note(
                "**About These Results:** NVD's CPE matching may include CVEs for PostgreSQL "
                "extensions, language bindings, or older versions. Review each CVE's 'Description' "
                "and 'Published' date to determine relevance to your installation. "
                "CVEs from 2010 or earlier typically don't affect modern PostgreSQL versions."
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
                f"Upgrade to latest PostgreSQL {major_version}.x minor version to address known vulnerabilities",
                "Review detailed CVE information at NVD: https://nvd.nist.gov",
                "Check PostgreSQL security announcements: https://www.postgresql.org/support/security/",
                "Subscribe to PostgreSQL security mailing list for updates"
            ])

        # Environment-specific recommendations
        if env_name in ['AURORA', 'RDS']:
            recommendations.append("Contact AWS Support for managed patching schedules and available versions")
            recommendations.append("Review AWS Security Bulletins for PostgreSQL: https://aws.amazon.com/security/security-bulletins/")
        elif env_name == 'PATRONI':
            recommendations.append("Plan rolling upgrade across Patroni cluster to minimize downtime")
            recommendations.append("Test upgrade on standby replica first")

        if recommendations:
            builder.recs(recommendations)

    # Check extension CVEs
    builder.h4("Extension Vulnerabilities")

    logger.info("Checking CVEs for installed PostgreSQL extensions")
    extension_cves = connector.get_extension_cves(max_results_per_extension=50)

    structured_data['data']['extensions'] = extension_cves

    if not extension_cves:
        builder.note(
            "No CVE-tracked extensions detected. "
            "Checked extensions: PostGIS, TimescaleDB, Citus, pgAudit, pg_partman, pg_stat_statements. "
            "Other extensions are not checked (limited NVD coverage)."
        )
    else:
        # Filter to only extensions with CVEs or errors
        extensions_with_cves = [
            ext for ext in extension_cves
            if ext.get('status') == 'success' and ext.get('total_cves', 0) > 0
        ]
        extensions_checked = len([ext for ext in extension_cves if ext.get('status') == 'success'])
        extensions_unavailable = len([ext for ext in extension_cves if ext.get('status') == 'unavailable'])

        if not extensions_with_cves:
            builder.success(f"✅ No CVEs found for {extensions_checked} checked extension(s)")
            if extensions_unavailable > 0:
                builder.blank()
                builder.text(f"*Note:* {extensions_unavailable} extension(s) not in NVD database (no CPE mapping)")
        else:
            builder.warning(f"⚠️ CVEs found in {len(extensions_with_cves)} of {extensions_checked} extension(s)")
            builder.blank()

            # Table of extensions with CVEs
            ext_table = []
            for ext in extensions_with_cves:
                ext_name = ext.get('extension_name', 'Unknown')
                ext_version = ext.get('version', 'Unknown')
                total = ext.get('total_cves', 0)
                severity = ext.get('severity_counts', {})

                # Format severity counts
                crit = severity.get('critical', 0)
                high = severity.get('high', 0)
                med = severity.get('medium', 0)

                severity_str = f"C:{crit} H:{high} M:{med}"

                # Color indicator based on highest severity
                if crit > 0:
                    indicator = '🔴'
                elif high > 0:
                    indicator = '🟠'
                elif med > 0:
                    indicator = '🟡'
                else:
                    indicator = '⚪'

                ext_table.append({
                    'Status': indicator,
                    'Extension': ext_name,
                    'Version': ext_version,
                    'Total CVEs': total,
                    'Severity (C/H/M)': severity_str
                })

            builder.table(ext_table)
            builder.blank()

            # Extension recommendations
            ext_recommendations = [
                "Review each extension's CVEs individually for impact assessment",
                "Check if newer versions of affected extensions are available",
                "Consider disabling non-essential extensions with critical vulnerabilities",
                "Verify extension compatibility before upgrading",
                "Test extension upgrades in non-production environment first"
            ]
            builder.recs(ext_recommendations, title="Extension Security Recommendations")

    # Add informational footer
    builder.h4("About This Check")
    builder.text("This check queries the NIST National Vulnerability Database (NVD) API v2.0.")
    builder.text("CVE data is updated continuously by NIST and the security community.")
    builder.blank()
    builder.text("*Rate Limits:*")
    builder.text("- Without API key: 5 requests per 30 seconds")
    builder.text("- With API key: 50 requests per 30 seconds")
    builder.blank()
    builder.text("Get an API key at: https://nvd.nist.gov/developers/request-an-api-key")

    return builder.build(), structured_data

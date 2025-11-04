"""
Aurora Version Upgrade Check

Analyzes the current Aurora PostgreSQL version and identifies available upgrades
using AWS RDS API. Detects both minor and major version upgrade opportunities.
"""

from plugins.common.check_helpers import CheckContentBuilder
import re

def get_weight():
    """Returns the importance score for this module."""
    return 8  # Version upgrades are important for security and features


def run_check_aurora_version_upgrades(connector, settings):
    """
    Checks for available Aurora version upgrades (minor and major).

    Args:
        connector: PostgreSQL connector with AWS support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("Aurora Version Upgrade Analysis")

    # Check if this is Aurora
    if not hasattr(connector, 'environment') or connector.environment != 'aurora':
        env_name = getattr(connector, 'environment', 'unknown').upper() if hasattr(connector, 'environment') else 'UNKNOWN'
        builder.note(f"This check is for Aurora environments only. Current environment: {env_name}")
        structured_data["status"] = "skipped"
        structured_data["reason"] = "not_aurora"
        return builder.build(), structured_data

    # Check if AWS RDS client is available
    if not hasattr(connector, '_rds_client') or not connector._rds_client:
        builder.critical("RDS client not initialized. boto3 may not be installed or AWS credentials not configured.")
        structured_data["status"] = "error"
        structured_data["error"] = "RDS client not initialized"
        return builder.build(), structured_data

    try:
        # Get current version from connector
        current_version_string = connector.version_info.get('version_string', '')
        current_major = connector.version_info.get('major_version', 0)

        # Get Aurora version if available
        aurora_version = None
        if hasattr(connector, 'environment_details') and 'aurora_version' in connector.environment_details:
            aurora_version = connector.environment_details['aurora_version']

        # Parse PostgreSQL version from version_string (e.g., "16.2" from "16.2 (Debian 16.2-1.pgdg110+1)")
        version_match = re.search(r'(\d+)\.(\d+)', current_version_string)
        if version_match:
            parsed_major = int(version_match.group(1))
            current_minor = int(version_match.group(2))

            # Use parsed major if it matches connector's major_version, otherwise use connector's
            if parsed_major != current_major and current_major > 0:
                current_major = parsed_major
        else:
            # Fallback to just major version with minor = 0
            current_minor = 0
            if current_major == 0:
                builder.critical(f"Could not parse PostgreSQL version from: {current_version_string}")
                structured_data["status"] = "error"
                structured_data["error"] = "version_parse_failed"
                return builder.build(), structured_data

        current_pg_version = f"{current_major}.{current_minor}"

        # Display current version info
        builder.h4("Current Version")
        version_info = [
            f"- **PostgreSQL Version**: {current_pg_version}",
        ]
        if aurora_version:
            version_info.append(f"- **Aurora Version**: {aurora_version}")
        builder.add_lines(version_info)
        builder.blank()

        structured_data["current_version"] = {
            "postgresql_version": current_pg_version,
            "postgresql_major": current_major,
            "postgresql_minor": current_minor,
            "aurora_version": aurora_version
        }

        # Query AWS RDS API for available versions
        rds_client = connector._rds_client

        # Determine engine name (aurora-postgresql vs aurora-mysql)
        engine_name = 'aurora-postgresql'  # Default to PostgreSQL

        builder.h4("Querying AWS RDS API for Available Versions...")

        # Get all available engine versions for Aurora PostgreSQL
        try:
            response = rds_client.describe_db_engine_versions(
                Engine=engine_name,
                IncludeAll=True
            )
        except Exception as e:
            builder.critical(f"Failed to query RDS API: {e}")
            structured_data["status"] = "error"
            structured_data["error"] = str(e)
            return builder.build(), structured_data

        available_versions = response.get('DBEngineVersions', [])

        if not available_versions:
            builder.warning("No Aurora PostgreSQL versions found in AWS RDS API response.")
            structured_data["status"] = "error"
            structured_data["available_versions"] = []
            return builder.build(), structured_data

        # Parse and categorize versions
        minor_upgrades = []
        major_upgrades = []
        current_version_details = None

        for version_info in available_versions:
            engine_version = version_info.get('EngineVersion', '')

            # Parse version (e.g., "16.2", "15.4")
            ver_match = re.match(r'(\d+)\.(\d+)', engine_version)
            if not ver_match:
                continue

            ver_major = int(ver_match.group(1))
            ver_minor = int(ver_match.group(2))

            version_data = {
                'version': engine_version,
                'major': ver_major,
                'minor': ver_minor,
                'status': version_info.get('Status', 'available'),
                'description': version_info.get('DBEngineVersionDescription', ''),
                'supports_log_exports': version_info.get('SupportedLogTypes', []),
                'valid_upgrade_targets': version_info.get('ValidUpgradeTarget', [])
            }

            # Check if this is the current version
            if ver_major == current_major and ver_minor == current_minor:
                current_version_details = version_data

            # Check if this is a minor upgrade (same major, higher minor)
            elif ver_major == current_major and ver_minor > current_minor:
                minor_upgrades.append(version_data)

            # Check if this is a major upgrade (higher major version)
            elif ver_major > current_major:
                # Only consider the latest minor version for each major version
                existing = next((v for v in major_upgrades if v['major'] == ver_major), None)
                if not existing or ver_minor > existing['minor']:
                    if existing:
                        major_upgrades.remove(existing)
                    major_upgrades.append(version_data)

        # Sort upgrades
        minor_upgrades.sort(key=lambda x: (x['major'], x['minor']))
        major_upgrades.sort(key=lambda x: (x['major'], x['minor']))

        # Store in structured data
        structured_data["available_minor_upgrades"] = minor_upgrades
        structured_data["available_major_upgrades"] = major_upgrades
        structured_data["status"] = "success"

        # Display minor upgrades
        builder.h4("Available Minor Version Upgrades")
        if minor_upgrades:
            latest_minor = minor_upgrades[-1]

            builder.warning(f"**{len(minor_upgrades)} minor version upgrade(s) available** (same major version {current_major}.x)")
            builder.blank()

            # Show table of available minor versions
            minor_table = []
            for upgrade in minor_upgrades:
                minor_table.append({
                    'Version': f"PostgreSQL {upgrade['version']}",
                    'Status': upgrade['status'],
                    'Description': upgrade['description'][:60] + '...' if len(upgrade['description']) > 60 else upgrade['description']
                })

            builder.table(minor_table)
            builder.blank()

            builder.add(f"**Recommended**: Upgrade to PostgreSQL **{latest_minor['version']}** (latest minor release)")

            # Generate depesz.com link for latest minor upgrade
            # Strip Aurora-specific suffixes like "-limitless" for depesz.com compatibility
            latest_minor_clean = latest_minor['version'].split('-')[0]
            depesz_link = f"https://why-upgrade.depesz.com/show?from={current_pg_version}&to={latest_minor_clean}"

            recommendations = [
                f"Upgrade to PostgreSQL {latest_minor['version']} for latest security patches and bug fixes",
                "Minor version upgrades are generally safe and include important security updates",
                "Test in a staging environment before applying to production",
                f"**What's New**: See detailed changelog at {depesz_link}",
                "Review Aurora release notes: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraPostgreSQLReleaseNotes/"
            ]
            builder.recs(recommendations)
        else:
            builder.note(f"You are running the latest minor version for PostgreSQL {current_major}.x")

        # Display major upgrades
        builder.h4("Available Major Version Upgrades")
        if major_upgrades:
            latest_major = major_upgrades[-1]

            builder.warning(f"**{len(major_upgrades)} major version upgrade(s) available**")
            builder.blank()

            # Show table of available major versions
            major_table = []
            for upgrade in major_upgrades:
                # Strip Aurora-specific suffixes for depesz.com compatibility
                upgrade_clean = upgrade['version'].split('-')[0]
                depesz_major_link = f"https://why-upgrade.depesz.com/show?from={current_pg_version}&to={upgrade_clean}"
                major_table.append({
                    'Version': f"PostgreSQL {upgrade['version']}",
                    'Status': upgrade['status'],
                    'What\'s New': f"link:{depesz_major_link}[Changelog]"
                })

            builder.table(major_table)
            builder.blank()

            builder.add(f"**Latest Major Version**: PostgreSQL **{latest_major['version']}**")

            # Generate depesz.com link for latest major upgrade
            # Strip Aurora-specific suffixes for depesz.com compatibility
            latest_major_clean = latest_major['version'].split('-')[0]
            depesz_link = f"https://why-upgrade.depesz.com/show?from={current_pg_version}&to={latest_major_clean}"

            recommendations = [
                f"Consider upgrading to PostgreSQL {latest_major['version']} for new features and improved performance",
                "IMPORTANT: Major version upgrades require more extensive testing",
                f"**What's New**: Detailed changelog at {depesz_link}",
                "Review breaking changes and deprecated features in release notes",
                "Plan for application compatibility testing before upgrade",
                "Consider using Aurora blue/green deployments for safer major upgrades",
                "Backup your database before performing major version upgrades",
                f"Official release notes: https://www.postgresql.org/docs/{latest_major['major']}/release.html"
            ]
            builder.recs(recommendations, title="Major Upgrade Recommendations")
        else:
            builder.note("You are running the latest major version of Aurora PostgreSQL")

        # Display upgrade path if available
        if current_version_details and current_version_details.get('valid_upgrade_targets'):
            builder.h4("Valid Upgrade Paths from Current Version")

            upgrade_targets = current_version_details['valid_upgrade_targets']
            if upgrade_targets:
                target_table = []
                for target in upgrade_targets[:10]:  # Limit to first 10
                    target_engine = target.get('Engine', 'aurora-postgresql')
                    target_version = target.get('EngineVersion', '')
                    is_major = target.get('IsMajorVersionUpgrade', False)
                    auto_upgrade = target.get('AutoUpgrade', False)

                    target_table.append({
                        'Target Version': target_version,
                        'Upgrade Type': 'Major' if is_major else 'Minor',
                        'Auto Upgrade': 'Yes' if auto_upgrade else 'No'
                    })

                builder.table(target_table)

                if len(upgrade_targets) > 10:
                    builder.add(f"\n_Showing 10 of {len(upgrade_targets)} valid upgrade targets_")
            else:
                builder.note("No specific upgrade targets defined for current version")

    except Exception as e:
        builder.critical(f"Error during Aurora version analysis: {e}")
        structured_data["status"] = "error"
        structured_data["error"] = str(e)
        import traceback
        structured_data["traceback"] = traceback.format_exc()

    return builder.build(), structured_data

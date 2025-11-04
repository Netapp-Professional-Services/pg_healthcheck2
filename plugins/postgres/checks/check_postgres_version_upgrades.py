"""
PostgreSQL Version Upgrade Check (Bare-Metal/EC2)

Analyzes the current PostgreSQL version and identifies available upgrades
by querying OS package repositories. Works for bare-metal and EC2 installations.
Skips Windows systems.
"""

from plugins.common.check_helpers import CheckContentBuilder, require_ssh
import re

# PostgreSQL major version EOL dates (approximate)
POSTGRESQL_EOL_DATES = {
    11: "November 2023",
    12: "November 2024",
    13: "November 2025",
    14: "November 2026",
    15: "November 2027",
    16: "November 2028",
    17: "November 2029",
}

def get_weight():
    """Returns the importance score for this module."""
    return 8  # Version upgrades are important for security and features


def run_check_postgres_version_upgrades(connector, settings):
    """
    Checks for available PostgreSQL version upgrades on bare-metal/EC2.

    Args:
        connector: PostgreSQL connector with SSH support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("PostgreSQL Version Upgrade Analysis")

    # Check if this is Aurora (skip - use Aurora-specific check instead)
    if hasattr(connector, 'environment') and connector.environment == 'aurora':
        builder.note("This check is for bare-metal/EC2 PostgreSQL installations. Aurora version checks are handled separately.")
        structured_data["status"] = "skipped"
        structured_data["reason"] = "aurora_environment"
        return builder.build(), structured_data

    # Check if SSH is available (required for package repository queries)
    ssh_available, skip_msg, skip_data = require_ssh(connector, "PostgreSQL version upgrade check")
    if not ssh_available:
        builder.add(skip_msg)
        return builder.build(), skip_data

    try:
        # Get current version from connector
        current_version_string = connector.version_info.get('version_string', '')
        current_major = connector.version_info.get('major_version', 0)

        # Parse PostgreSQL version from version_string
        version_match = re.search(r'(\d+)\.(\d+)', current_version_string)
        if version_match:
            parsed_major = int(version_match.group(1))
            current_minor = int(version_match.group(2))

            if parsed_major != current_major and current_major > 0:
                current_major = parsed_major
        else:
            current_minor = 0
            if current_major == 0:
                builder.critical(f"Could not parse PostgreSQL version from: {current_version_string}")
                structured_data["status"] = "error"
                structured_data["error"] = "version_parse_failed"
                return builder.build(), structured_data

        current_pg_version = f"{current_major}.{current_minor}"

        # Display current version info
        builder.h4("Current Version")
        version_info = [f"- **PostgreSQL Version**: {current_pg_version}"]

        # Check EOL status
        eol_date = POSTGRESQL_EOL_DATES.get(current_major)
        if eol_date:
            version_info.append(f"- **End of Life**: {eol_date}")
        else:
            version_info.append(f"- **End of Life**: Version {current_major} may be deprecated")

        builder.add_lines(version_info)
        builder.blank()

        structured_data["current_version"] = {
            "postgresql_version": current_pg_version,
            "postgresql_major": current_major,
            "postgresql_minor": current_minor,
            "eol_date": eol_date
        }

        # Detect OS and query package repository
        ssh_hosts = connector.get_ssh_hosts()
        if not ssh_hosts:
            builder.warning("No SSH hosts configured. Cannot query package repositories for available versions.")
            structured_data["status"] = "error"
            structured_data["error"] = "no_ssh_hosts"
            return builder.build(), structured_data

        # Use first SSH host for package queries
        ssh_host = ssh_hosts[0]
        ssh_manager = connector.get_ssh_manager(ssh_host)

        builder.h4("Detecting Operating System...")

        # Detect OS type
        os_info = _detect_os(ssh_manager)

        if not os_info or os_info['os_type'] == 'unknown':
            builder.warning("Could not detect operating system. Version upgrade check requires Linux with apt or yum/dnf.")
            structured_data["status"] = "error"
            structured_data["error"] = "os_detection_failed"
            return builder.build(), structured_data

        if os_info['os_type'] == 'windows':
            builder.note("Windows detected. Version upgrade checks via package repository are not supported on Windows.")
            structured_data["status"] = "skipped"
            structured_data["reason"] = "windows_os"
            return builder.build(), structured_data

        builder.add(f"- **Operating System**: {os_info['os_name']}")
        builder.add(f"- **Package Manager**: {os_info['package_manager']}")
        builder.blank()

        # Query available versions based on OS
        builder.h4("Querying Package Repository for Available Versions...")

        available_versions = _query_available_versions(ssh_manager, os_info, current_major)

        if not available_versions:
            builder.warning(f"Could not query package repository for available PostgreSQL versions. Ensure PostgreSQL repository is configured.")
            structured_data["status"] = "error"
            structured_data["error"] = "package_query_failed"
            return builder.build(), structured_data

        # Categorize versions
        minor_upgrades = []
        major_upgrades = []

        for ver_info in available_versions:
            if ver_info['major'] == current_major and ver_info['minor'] > current_minor:
                minor_upgrades.append(ver_info)
            elif ver_info['major'] > current_major:
                # Only track latest minor for each major
                existing = next((v for v in major_upgrades if v['major'] == ver_info['major']), None)
                if not existing or ver_info['minor'] > existing['minor']:
                    if existing:
                        major_upgrades.remove(existing)
                    major_upgrades.append(ver_info)

        # Sort upgrades
        minor_upgrades.sort(key=lambda x: (x['major'], x['minor']))
        major_upgrades.sort(key=lambda x: (x['major'], x['minor']))

        structured_data["available_minor_upgrades"] = minor_upgrades
        structured_data["available_major_upgrades"] = major_upgrades
        structured_data["os_info"] = os_info
        structured_data["status"] = "success"

        # Display minor upgrades
        builder.h4("Available Minor Version Upgrades")
        if minor_upgrades:
            latest_minor = minor_upgrades[-1]
            latest_minor_version = f"{latest_minor['major']}.{latest_minor['minor']}"

            builder.warning(f"**{len(minor_upgrades)} minor version upgrade(s) available** (same major version {current_major}.x)")
            builder.blank()

            # Show available versions
            versions_list = [f"PostgreSQL {v['major']}.{v['minor']}" for v in minor_upgrades]
            builder.add(f"**Available versions**: {', '.join(versions_list)}")
            builder.blank()

            builder.add(f"**Recommended**: Upgrade to PostgreSQL **{latest_minor_version}** (latest minor release)")

            # Generate depesz.com link
            depesz_link = f"https://why-upgrade.depesz.com/show?from={current_pg_version}&to={latest_minor_version}"

            recommendations = [
                f"Upgrade to PostgreSQL {latest_minor_version} for latest security patches and bug fixes",
                "Minor version upgrades are generally safe and include important security updates",
                f"**What's New**: See detailed changelog at {depesz_link}",
                f"Use package manager to upgrade: `{_get_upgrade_command(os_info, current_major, latest_minor_version)}`",
                "Test in a staging environment before applying to production",
                "Backup your database before performing any upgrade"
            ]
            builder.recs(recommendations)
        else:
            builder.note(f"You are running the latest minor version for PostgreSQL {current_major}.x available in your package repository")

        # Display major upgrades
        builder.h4("Available Major Version Upgrades")
        if major_upgrades:
            latest_major = major_upgrades[-1]
            latest_major_version = f"{latest_major['major']}.{latest_major['minor']}"

            builder.warning(f"**{len(major_upgrades)} major version upgrade(s) available**")
            builder.blank()

            # Show table of major versions
            major_table = []
            for upgrade in major_upgrades:
                upgrade_version = f"{upgrade['major']}.{upgrade['minor']}"
                depesz_major_link = f"https://why-upgrade.depesz.com/show?from={current_pg_version}&to={upgrade_version}"
                eol = POSTGRESQL_EOL_DATES.get(upgrade['major'], 'Unknown')
                major_table.append({
                    'Version': f"PostgreSQL {upgrade_version}",
                    'EOL Date': eol,
                    'What\'s New': f"link:{depesz_major_link}[Changelog]"
                })

            builder.table(major_table)
            builder.blank()

            builder.add(f"**Latest Major Version**: PostgreSQL **{latest_major_version}**")

            # Generate depesz.com link
            depesz_link = f"https://why-upgrade.depesz.com/show?from={current_pg_version}&to={latest_major_version}"

            recommendations = [
                f"Consider upgrading to PostgreSQL {latest_major_version} for new features and improved performance",
                "IMPORTANT: Major version upgrades require extensive testing and planning",
                f"**What's New**: Detailed changelog at {depesz_link}",
                "Review breaking changes and deprecated features before upgrading",
                "Use pg_upgrade for major version upgrades: https://www.postgresql.org/docs/current/pgupgrade.html",
                "Plan for extended testing window and application compatibility verification",
                "Backup your database and test the upgrade process in staging first",
                f"Official release notes: https://www.postgresql.org/docs/{latest_major['major']}/release.html"
            ]
            builder.recs(recommendations, title="Major Upgrade Recommendations")
        else:
            builder.note("You are running the latest major version of PostgreSQL available in your package repository")

        # Show PostgreSQL repository configuration tip if needed
        if not minor_upgrades and not major_upgrades:
            builder.h4("PostgreSQL Repository Configuration")
            builder.note("Consider adding the official PostgreSQL repository for access to latest versions:")

            if os_info['package_manager'] == 'apt':
                repo_link = "https://www.postgresql.org/download/linux/ubuntu/"
            elif os_info['package_manager'] in ['yum', 'dnf']:
                repo_link = "https://www.postgresql.org/download/linux/redhat/"
            else:
                repo_link = "https://www.postgresql.org/download/"

            builder.add(f"Setup instructions: {repo_link}")

    except Exception as e:
        builder.critical(f"Error during PostgreSQL version analysis: {e}")
        structured_data["status"] = "error"
        structured_data["error"] = str(e)
        import traceback
        structured_data["traceback"] = traceback.format_exc()

    return builder.build(), structured_data


def _detect_os(ssh_manager):
    """
    Detect operating system type and package manager.

    Returns:
        dict: OS information including type, name, and package manager
    """
    os_info = {
        'os_type': 'unknown',
        'os_name': 'Unknown',
        'package_manager': 'unknown'
    }

    try:
        # Check for Windows (unlikely but possible)
        stdout, stderr, exit_code = ssh_manager.execute_command("uname -s")
        if exit_code != 0 or 'MINGW' in stdout or 'CYGWIN' in stdout:
            # Might be Windows
            stdout_win, stderr_win, exit_code_win = ssh_manager.execute_command("ver")
            if exit_code_win == 0 and 'Windows' in stdout_win:
                os_info['os_type'] = 'windows'
                os_info['os_name'] = 'Windows'
                return os_info

        # Read /etc/os-release for Linux distro info
        stdout, stderr, exit_code = ssh_manager.execute_command("cat /etc/os-release")
        if exit_code == 0:
            os_release = stdout

            # Parse OS name
            name_match = re.search(r'PRETTY_NAME="([^"]+)"', os_release)
            if name_match:
                os_info['os_name'] = name_match.group(1)

            # Detect package manager based on ID
            if re.search(r'ID=(ubuntu|debian)', os_release, re.IGNORECASE):
                os_info['os_type'] = 'debian'
                os_info['package_manager'] = 'apt'
            elif re.search(r'ID=(rhel|centos|fedora|rocky|alma|amazon)', os_release, re.IGNORECASE):
                os_info['os_type'] = 'redhat'
                # Check if dnf is available
                stdout_dnf, _, exit_code_dnf = ssh_manager.execute_command("which dnf")
                if exit_code_dnf == 0:
                    os_info['package_manager'] = 'dnf'
                else:
                    os_info['package_manager'] = 'yum'

    except Exception as e:
        pass

    return os_info


def _query_available_versions(ssh_manager, os_info, current_major):
    """
    Query package repository for available PostgreSQL versions.

    Returns:
        list: List of available version dicts with major/minor
    """
    available_versions = []

    try:
        if os_info['package_manager'] == 'apt':
            # Use apt-cache madison to get available versions
            cmd = f"apt-cache madison postgresql-{current_major} postgresql-{current_major + 1} postgresql-{current_major + 2} 2>/dev/null | grep -E 'postgresql-[0-9]+' | awk '{{print $3}}' | sort -V | uniq"
            stdout, stderr, exit_code = ssh_manager.execute_command(cmd)

            if exit_code == 0 and stdout:
                for line in stdout.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue

                    # Parse version (e.g., "16.2-1.pgdg120+1" -> 16.2)
                    ver_match = re.match(r'(\d+)\.(\d+)', line)
                    if ver_match:
                        major = int(ver_match.group(1))
                        minor = int(ver_match.group(2))
                        available_versions.append({
                            'major': major,
                            'minor': minor,
                            'full_version': line
                        })

        elif os_info['package_manager'] in ['yum', 'dnf']:
            # Use yum/dnf to list available versions
            pkg_mgr = os_info['package_manager']
            cmd = f"{pkg_mgr} list available postgresql{current_major}* postgresql{current_major + 1}* postgresql{current_major + 2}* 2>/dev/null | grep -E 'postgresql[0-9]+' | awk '{{print $2}}' | sort -V | uniq"
            stdout, stderr, exit_code = ssh_manager.execute_command(cmd)

            if exit_code == 0 and stdout:
                for line in stdout.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue

                    # Parse version (e.g., "16.2-1PGDG.rhel8" -> 16.2)
                    ver_match = re.match(r'(\d+)\.(\d+)', line)
                    if ver_match:
                        major = int(ver_match.group(1))
                        minor = int(ver_match.group(2))
                        available_versions.append({
                            'major': major,
                            'minor': minor,
                            'full_version': line
                        })

    except Exception as e:
        pass

    return available_versions


def _get_upgrade_command(os_info, current_major, target_version):
    """Generate the package manager command to upgrade PostgreSQL."""
    if os_info['package_manager'] == 'apt':
        return f"sudo apt-get update && sudo apt-get install postgresql-{current_major}={target_version}*"
    elif os_info['package_manager'] in ['yum', 'dnf']:
        pkg_mgr = os_info['package_manager']
        return f"sudo {pkg_mgr} update postgresql{current_major}"
    else:
        return "See PostgreSQL documentation for upgrade instructions"

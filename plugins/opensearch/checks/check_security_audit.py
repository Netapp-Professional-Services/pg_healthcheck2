"""
Security configuration audit for Elasticsearch/OpenSearch clusters.

This check analyzes security configuration to identify:
- Users with superuser or admin roles (privilege audit)
- Deprecated users that should be migrated
- Role configurations and potential over-permissioning
- Security feature status

Data source:
- Live clusters: GET /_security/user, GET /_security/role
- Imported diagnostics: commercial/security_users.json, commercial/security_roles.json

Note: This check requires X-Pack security to be enabled. OpenSearch uses
its own security plugin with similar concepts but different APIs.
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Audit security configuration for best practices.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Security Configuration Audit")

    try:
        # Get security data
        users_data = connector.execute_query({"operation": "security_users"})
        roles_data = connector.execute_query({"operation": "security_roles"})

        users_available = not (isinstance(users_data, dict) and "error" in users_data)
        roles_available = not (isinstance(roles_data, dict) and "error" in roles_data)

        if not users_available and not roles_available:
            builder.note(
                "Security configuration information is not available.\n\n"
                "This may indicate:\n"
                "- X-Pack security is not enabled\n"
                "- OpenSearch Security plugin is not configured\n"
                "- Diagnostic export did not include security data\n"
                "- Insufficient permissions to access security APIs"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "Security data not available"
            }

        # Analyze users
        total_users = 0
        system_users = []
        custom_users = []
        superusers = []
        deprecated_users = []
        disabled_users = []

        if users_available and users_data:
            for username, user_data in users_data.items():
                total_users += 1
                roles = user_data.get("roles", [])
                metadata = user_data.get("metadata", {})
                enabled = user_data.get("enabled", True)

                user_info = {
                    "username": username,
                    "roles": roles,
                    "enabled": enabled,
                    "is_reserved": metadata.get("_reserved", False),
                    "is_deprecated": metadata.get("_deprecated", False),
                    "deprecation_reason": metadata.get("_deprecated_reason", "")
                }

                # Categorize user
                if metadata.get("_reserved", False):
                    system_users.append(user_info)
                else:
                    custom_users.append(user_info)

                # Check for superuser role
                if "superuser" in roles or "admin" in roles:
                    superusers.append(user_info)

                # Check for deprecated users
                if metadata.get("_deprecated", False):
                    deprecated_users.append(user_info)

                # Check for disabled users
                if not enabled:
                    disabled_users.append(user_info)

        # Analyze roles
        total_roles = 0
        system_roles = []
        custom_roles = []
        overprivileged_roles = []

        if roles_available and roles_data:
            for role_name, role_data in roles_data.items():
                total_roles += 1
                metadata = role_data.get("metadata", {})

                role_info = {
                    "name": role_name,
                    "cluster_privileges": role_data.get("cluster", []),
                    "indices_privileges": role_data.get("indices", []),
                    "is_reserved": metadata.get("_reserved", False)
                }

                if metadata.get("_reserved", False):
                    system_roles.append(role_info)
                else:
                    custom_roles.append(role_info)

                # Check for overprivileged roles
                cluster_privs = role_data.get("cluster", [])
                if "all" in cluster_privs or "manage" in cluster_privs:
                    overprivileged_roles.append(role_info)

        # Build report
        builder.h4("Security Summary")

        summary_data = [
            {"Metric": "Total Users", "Value": str(total_users)},
            {"Metric": "System Users", "Value": str(len(system_users))},
            {"Metric": "Custom Users", "Value": str(len(custom_users))},
            {"Metric": "Superuser Accounts", "Value": str(len(superusers))},
            {"Metric": "Deprecated Users", "Value": str(len(deprecated_users))},
            {"Metric": "Disabled Users", "Value": str(len(disabled_users))},
            {"Metric": "Total Roles", "Value": str(total_roles)},
            {"Metric": "Custom Roles", "Value": str(len(custom_roles))}
        ]
        builder.table(summary_data)
        builder.blank()

        # Alert on superusers
        if superusers:
            builder.warning(
                f"**{len(superusers)} account(s) with superuser/admin privileges**\n\n"
                "Superuser accounts have unrestricted access. Review if all are necessary."
            )
            superuser_table = []
            for user in superusers:
                superuser_table.append({
                    "Username": user["username"],
                    "Roles": ", ".join(user["roles"]),
                    "Type": "System" if user["is_reserved"] else "Custom",
                    "Status": "Enabled" if user["enabled"] else "Disabled"
                })
            builder.table(superuser_table)
            builder.blank()

        # Alert on deprecated users
        if deprecated_users:
            builder.warning(
                f"**{len(deprecated_users)} deprecated user(s) detected**\n\n"
                "These users should be migrated to their recommended replacements."
            )
            deprecated_table = []
            for user in deprecated_users:
                deprecated_table.append({
                    "Username": user["username"],
                    "Reason": user["deprecation_reason"] or "No reason provided",
                    "Status": "Enabled" if user["enabled"] else "Disabled"
                })
            builder.table(deprecated_table)
            builder.blank()

        # Show custom users (non-system)
        if custom_users:
            builder.h4("Custom Users")
            user_table = []
            for user in custom_users:
                user_table.append({
                    "Username": user["username"],
                    "Roles": ", ".join(user["roles"][:3]) + ("..." if len(user["roles"]) > 3 else ""),
                    "Status": "Enabled" if user["enabled"] else "Disabled"
                })
            builder.table(user_table)
            builder.blank()

        # Show overprivileged roles
        if overprivileged_roles:
            builder.h4("High-Privilege Roles")
            builder.text("Roles with cluster-wide 'all' or 'manage' privileges:")
            builder.blank()
            role_table = []
            for role in overprivileged_roles[:10]:
                role_table.append({
                    "Role": role["name"],
                    "Cluster Privileges": ", ".join(role["cluster_privileges"][:3]),
                    "Type": "System" if role["is_reserved"] else "Custom"
                })
            builder.table(role_table)
            builder.blank()

        # Recommendations
        if deprecated_users or len(superusers) > 2:
            builder.text("*Recommended Actions:*")
            if deprecated_users:
                builder.text("1. Migrate deprecated users to their recommended replacements")
                for user in deprecated_users:
                    if user["deprecation_reason"]:
                        builder.text(f"   - {user['username']}: {user['deprecation_reason']}")
            if len(superusers) > 2:
                builder.text("1. Review superuser accounts - consider reducing to minimum necessary")
                builder.text("   - Principle of least privilege: grant only required permissions")
            builder.blank()

        builder.text("*Security Best Practices:*")
        builder.text("- Regularly audit user accounts and remove unused ones")
        builder.text("- Use role-based access control (RBAC) with minimal privileges")
        builder.text("- Disable default/demo users in production")
        builder.text("- Enable audit logging for security-sensitive operations")
        builder.text("- Implement strong password policies")

        # Build structured data
        structured_data = {
            "status": "success",
            "data": {
                "total_users": total_users,
                "system_user_count": len(system_users),
                "custom_user_count": len(custom_users),
                "superuser_count": len(superusers),
                "deprecated_user_count": len(deprecated_users),
                "disabled_user_count": len(disabled_users),
                "total_roles": total_roles,
                "custom_role_count": len(custom_roles),
                "overprivileged_role_count": len(overprivileged_roles),
                "has_deprecated_users": len(deprecated_users) > 0,
                "has_excess_superusers": len(superusers) > 2
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to audit security configuration: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

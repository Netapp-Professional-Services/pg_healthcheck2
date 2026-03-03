"""
Default User Accounts Check

Flags OpenSearch clusters where default/built-in user accounts are present and
enabled. These accounts often ship with well-known default passwords (e.g.
admin/admin, kibanaserver/kibanaserver). The check recommends changing default
passwords or disabling these accounts.

Passwords cannot be read via API; this check infers risk from reserved
accounts and known default usernames.

Data source:
- Live clusters: GET /_plugins/_security/api/internalusers (OpenSearch Security plugin)
- Imported diagnostics: commercial/security_users.json
"""

import logging
from typing import Any, Dict, List, Tuple

from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)

# Well-documented default account names that historically had default passwords
# (OpenSearch / Elasticsearch built-ins)
DEFAULT_USERNAMES = frozenset({
    "admin",
    "kibanaserver",
    "kibana_system",
    "logstash",
    "readall",
    "snapshotrestore",
    "anomalyadmin",
    "kibanaro",
})


def get_weight():
    """Returns the importance score for this check (security-sensitive)."""
    return 8


def run(connector, settings) -> Tuple[str, Dict[str, Any]]:
    """
    Flag enabled default/built-in user accounts; recommend changing default
    passwords or disabling.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Default User Accounts")
    builder.para(
        "Default and built-in accounts often ship with well-known default passwords. "
        "If any are enabled, change their passwords or disable them in production."
    )

    try:
        users_data = connector.execute_query({"operation": "security_users"})

        if isinstance(users_data, dict) and "error" in users_data:
            builder.note(
                "Security user information is not available.\n\n"
                "This may indicate:\n"
                "- X-Pack security / OpenSearch Security plugin is not enabled\n"
                "- Diagnostic export did not include security users\n"
                "- Insufficient permissions to access the security API"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": "Security users not available",
            }

        default_accounts_enabled: List[Dict[str, Any]] = []

        if users_data:
            for username, user_data in users_data.items():
                if not isinstance(user_data, dict):
                    continue
                # OpenSearch uses backend_roles; ES uses roles; support both
                roles = user_data.get("roles") or user_data.get("backend_roles") or []
                # OpenSearch may not return "enabled"; treat missing as enabled
                enabled = user_data.get("enabled", True)
                if not enabled:
                    continue
                metadata = user_data.get("metadata", {})
                is_reserved = metadata.get("_reserved", False)
                is_known_default = username in DEFAULT_USERNAMES

                if is_reserved or is_known_default:
                    account_type = "Reserved" if is_reserved else "Known default"
                    default_accounts_enabled.append({
                        "username": username,
                        "roles": roles,
                        "account_type": account_type,
                        "recommendation": "Change default password or disable",
                    })

        if not default_accounts_enabled:
            builder.success(
                "No enabled default/built-in user accounts detected. "
                "Or all such accounts are disabled."
            )
            return builder.build(), {
                "status": "success",
                "data": {
                    "count": 0,
                    "default_accounts_enabled": [],
                    "has_default_accounts_enabled": False,
                },
            }

        builder.warning(
            f"**{len(default_accounts_enabled)} enabled default/built-in account(s)** detected.\n\n"
            "These accounts often have well-known default passwords. "
            "Change their passwords or disable them in production."
        )

        table_rows = []
        for item in default_accounts_enabled:
            roles_str = ", ".join(item["roles"][:5])
            if len(item["roles"]) > 5:
                roles_str += "..."
            table_rows.append({
                "Username": item["username"],
                "Roles": roles_str or "-",
                "Type": item["account_type"],
                "Recommendation": item["recommendation"],
            })
        builder.table(table_rows)

        return builder.build(), {
            "status": "success",
            "data": {
                "count": len(default_accounts_enabled),
                "default_accounts_enabled": default_accounts_enabled,
                "has_default_accounts_enabled": True,
            },
        }

    except Exception as e:
        logger.error(f"Default user accounts check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        return builder.build(), {"status": "error", "error": str(e)}

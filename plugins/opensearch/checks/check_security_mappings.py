"""
Security Role Mappings Check

Analyzes security configuration including role mappings, authentication realms,
and external identity provider integration status.

Data source:
- Live clusters: GET /_security/role_mapping
- Imported diagnostics: commercial/security_role_mappings.json

Sales Opportunities:
- SSO/SAML integration services
- LDAP/Active Directory setup
- Role-based access control consulting
- Security audit and compliance services
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check security role mappings and authentication configuration.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Security & Authentication Analysis")

    try:
        # Get security role mappings
        mappings_data = connector.execute_query({"operation": "security_role_mappings"})

        # Get X-Pack security info for authentication realms
        xpack_data = connector.execute_query({"operation": "xpack"})

        security_available = True
        if isinstance(mappings_data, dict) and "error" in mappings_data:
            if "security" in str(mappings_data.get("error", "")).lower():
                security_available = False

        if not security_available:
            builder.warning(
                "**Security Features Not Available**\n\n"
                "Security is not enabled or not available with the current license. "
                "This is a significant security gap for production environments."
            )
            builder.blank()

            builder.h4("Security Enablement Opportunity")
            builder.para(
                "Enabling security provides authentication, authorization, encryption, "
                "and audit logging capabilities essential for:"
            )
            builder.text("- Compliance requirements (SOC2, HIPAA, PCI-DSS)")
            builder.text("- Data access control and multi-tenancy")
            builder.text("- Audit trails and forensics")
            builder.text("- Network encryption (TLS)")
            builder.blank()

            return builder.build(), {
                "status": "success",
                "data": {
                    "security_enabled": False,
                    "has_role_mappings": False,
                    "external_auth_configured": False,
                    "sso_configured": False,
                    # Sales signals
                    "security_gap": True,
                    "sso_opportunity": True,
                    "compliance_risk": True,
                }
            }

        # Analyze role mappings
        # Role mappings data is a dict with mapping names as keys
        role_mappings = {k: v for k, v in mappings_data.items()
                        if isinstance(v, dict) and k not in ['error', 'status']}
        mapping_count = len(role_mappings)

        # Analyze authentication realms from X-Pack data
        security_info = xpack_data.get("security", {}) if isinstance(xpack_data, dict) else {}
        realms = security_info.get("realms", {})

        # Categorize authentication methods
        auth_methods = {
            "native": False,
            "file": False,
            "ldap": False,
            "active_directory": False,
            "saml": False,
            "oidc": False,
            "pki": False,
            "kerberos": False
        }

        external_auth_count = 0
        sso_methods = []

        for realm_type, realm_data in realms.items():
            if realm_data.get("available") and realm_data.get("enabled"):
                if realm_type in auth_methods:
                    auth_methods[realm_type] = True

                # Track external authentication
                if realm_type in ["ldap", "active_directory", "saml", "oidc", "pki", "kerberos"]:
                    external_auth_count += 1
                    if realm_type in ["saml", "oidc"]:
                        sso_methods.append(realm_type.upper())

        has_external_auth = external_auth_count > 0
        has_sso = len(sso_methods) > 0

        # Display authentication analysis
        builder.h4("Authentication Configuration")

        auth_table = []
        for method, enabled in auth_methods.items():
            category = "External/SSO" if method in ["saml", "oidc"] else \
                      "Directory" if method in ["ldap", "active_directory"] else \
                      "Certificate" if method in ["pki", "kerberos"] else "Built-in"

            auth_table.append({
                "Method": method.replace("_", " ").title(),
                "Category": category,
                "Status": "✅ Enabled" if enabled else "❌ Not configured",
                "Enterprise Value": "High" if method in ["saml", "oidc", "ldap", "active_directory"] else "Standard"
            })

        builder.table(auth_table)
        builder.blank()

        # Role mappings analysis
        if mapping_count > 0:
            builder.h4("Role Mappings")
            builder.para(f"{mapping_count} role mapping(s) configured for external identity integration.")

            mapping_table = []
            for mapping_name, mapping_info in list(role_mappings.items())[:10]:
                roles = mapping_info.get("roles", [])
                enabled = mapping_info.get("enabled", True)
                rules = mapping_info.get("rules", {})

                # Determine mapping type based on rules
                mapping_type = "Custom"
                if "field" in str(rules):
                    if "dn" in str(rules).lower():
                        mapping_type = "LDAP/AD"
                    elif "groups" in str(rules).lower():
                        mapping_type = "Group-based"
                    elif "username" in str(rules).lower():
                        mapping_type = "User-based"

                mapping_table.append({
                    "Mapping": mapping_name,
                    "Type": mapping_type,
                    "Roles": ", ".join(roles[:3]) + ("..." if len(roles) > 3 else ""),
                    "Status": "✅ Active" if enabled else "❌ Disabled"
                })

            builder.table(mapping_table)
            builder.blank()
        else:
            builder.note(
                "No role mappings configured.\n\n"
                "Role mappings are required for integrating external identity providers "
                "(LDAP, Active Directory, SAML, OIDC) with Elasticsearch roles."
            )
            builder.blank()

        # SSO/External Auth Recommendations
        builder.h4("Identity Integration Assessment")

        if not has_external_auth:
            builder.warning(
                "**No External Authentication Configured**\n\n"
                "The cluster is using only built-in authentication (native/file realms). "
                "Enterprise environments typically benefit from:"
            )
            builder.blank()
            builder.text("- **SAML/OIDC SSO**: Single sign-on with enterprise identity providers")
            builder.text("- **LDAP/Active Directory**: Centralized user management")
            builder.text("- **PKI/Kerberos**: Certificate-based authentication")
            builder.blank()

            recs = {
                "high": [
                    "Implement SSO integration (SAML or OIDC) for enterprise identity management",
                    "Configure LDAP/AD integration for centralized user provisioning",
                    "Establish role mappings to automate access control"
                ],
                "general": [
                    "Review native users and consider migration to external identity provider",
                    "Implement audit logging for compliance requirements",
                    "Consider PKI authentication for service-to-service communication"
                ]
            }
            builder.recs(recs)

        elif not has_sso:
            builder.note(
                "External authentication is configured but SSO (SAML/OIDC) is not enabled.\n\n"
                "SSO provides improved user experience and centralized access management "
                "through enterprise identity providers like Okta, Azure AD, or PingIdentity."
            )

            recs = {
                "high": [
                    "Consider SAML or OIDC integration for single sign-on capability"
                ],
                "general": [
                    "Review current LDAP/AD integration for optimization",
                    "Ensure role mappings are correctly configured for group-based access"
                ]
            }
            builder.recs(recs)

        else:
            builder.success(
                f"✅ Enterprise authentication is well-configured with SSO ({', '.join(sso_methods)})."
            )

        # Build structured data for rules engine and sales queries
        return builder.build(), {
            "status": "success",
            "data": {
                "security_enabled": True,
                "role_mapping_count": mapping_count,
                "has_role_mappings": mapping_count > 0,
                "has_native_auth": auth_methods.get("native", False),
                "has_ldap": auth_methods.get("ldap", False),
                "has_active_directory": auth_methods.get("active_directory", False),
                "has_saml": auth_methods.get("saml", False),
                "has_oidc": auth_methods.get("oidc", False),
                "has_pki": auth_methods.get("pki", False),
                "external_auth_configured": has_external_auth,
                "sso_configured": has_sso,
                "external_auth_methods": external_auth_count,
                # Sales signals
                "sso_opportunity": not has_sso,
                "directory_integration_opportunity": not (auth_methods.get("ldap", False) or auth_methods.get("active_directory", False)),
                "security_consulting_opportunity": not has_external_auth,
                "compliance_gap": not has_external_auth,
            }
        }

    except Exception as e:
        builder.error(f"Failed to check security mappings: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }

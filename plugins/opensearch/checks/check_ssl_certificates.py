"""
SSL Certificate expiry monitoring for OpenSearch/Elasticsearch clusters.

This check analyzes SSL/TLS certificates used by the cluster and alerts
on certificates that are expired or expiring soon. Certificate issues
can cause unexpected outages when certs expire.

Data source:
- Live clusters: GET /_ssl/certificates API (requires X-Pack security)
- Imported diagnostics: ssl_certs.json file

Structured findings include:
- List of all certificates with expiry dates
- Days until expiry for each certificate
- Certificates grouped by status (expired, critical, warning, ok)
"""

from datetime import datetime, timezone
from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check SSL certificate expiry status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("SSL Certificate Status")

    try:
        # Get SSL certificate data
        certs_data = connector.execute_query({"operation": "ssl_certs"})

        if isinstance(certs_data, dict) and "error" in certs_data:
            # API not available or not supported
            builder.note(
                "SSL certificate information is not available.\n\n"
                "This may indicate:\n"
                "- X-Pack security is not enabled\n"
                "- The cluster does not expose certificate information\n"
                "- Diagnostic export did not include ssl_certs.json"
            )
            return builder.build(), {
                "status": "unavailable",
                "reason": certs_data.get("error", "Unknown")
            }

        # Handle empty response
        if not certs_data:
            builder.note("No SSL certificates found in cluster configuration.")
            return builder.build(), {
                "status": "success",
                "data": {
                    "total_certificates": 0,
                }
            }

        # Ensure we have a list
        if isinstance(certs_data, dict):
            certs_list = certs_data.get("certificates", [certs_data])
        else:
            certs_list = certs_data

        # Analyze certificates
        now = datetime.now(timezone.utc)
        analyzed_certs = []
        expired = []
        critical = []  # < 7 days
        warning = []   # < 30 days
        ok = []

        for cert in certs_list:
            expiry_str = cert.get("expiry")
            if not expiry_str:
                continue

            # Parse expiry date
            try:
                # Handle various date formats
                if expiry_str.endswith("Z"):
                    expiry = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                elif "+" in expiry_str or expiry_str.count("-") > 2:
                    expiry = datetime.fromisoformat(expiry_str)
                else:
                    expiry = datetime.fromisoformat(expiry_str + "+00:00")
            except (ValueError, TypeError) as e:
                # Try alternative parsing
                try:
                    expiry = datetime.strptime(expiry_str[:19], "%Y-%m-%dT%H:%M:%S")
                    expiry = expiry.replace(tzinfo=timezone.utc)
                except:
                    continue

            days_until_expiry = (expiry - now).days

            cert_info = {
                "path": cert.get("path", "Unknown"),
                "subject_dn": cert.get("subject_dn", "Unknown"),
                "serial_number": cert.get("serial_number", "Unknown"),
                "format": cert.get("format", "Unknown"),
                "has_private_key": cert.get("has_private_key", False),
                "expiry": expiry_str,
                "expiry_date": expiry.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "days_until_expiry": days_until_expiry
            }
            analyzed_certs.append(cert_info)

            # Categorize by urgency
            if days_until_expiry < 0:
                expired.append(cert_info)
            elif days_until_expiry < 7:
                critical.append(cert_info)
            elif days_until_expiry < 30:
                warning.append(cert_info)
            else:
                ok.append(cert_info)

        # Build report output
        if expired:
            builder.critical(
                f"🔴 **{len(expired)} EXPIRED CERTIFICATE(S)**\n\n"
                "These certificates have already expired and may be causing connection failures!"
            )
            _add_cert_table(builder, expired, "Expired Certificates")
            builder.blank()

        if critical:
            builder.critical(
                f"🔴 **{len(critical)} certificate(s) expiring within 7 days**\n\n"
                "Immediate action required to prevent service disruption!"
            )
            _add_cert_table(builder, critical, "Certificates Expiring Soon (Critical)")
            builder.blank()

        if warning:
            builder.warning(
                f"⚠️ **{len(warning)} certificate(s) expiring within 30 days**\n\n"
                "Plan certificate renewal to avoid service disruption."
            )
            _add_cert_table(builder, warning, "Certificates Expiring Soon (Warning)")
            builder.blank()

        if ok:
            builder.success(
                f"✅ **{len(ok)} certificate(s) are valid** (>30 days until expiry)"
            )
            _add_cert_table(builder, ok, "Valid Certificates")
            builder.blank()

        # Add recommendations if there are issues
        if expired or critical or warning:
            builder.text("*Recommended Actions:*")
            if expired:
                builder.text("1. **URGENT:** Replace expired certificates immediately")
                builder.text("2. Verify services are still functioning (may have fallback certs)")
            if critical:
                builder.text("1. Generate new certificates before expiry")
                builder.text("2. Schedule certificate rotation during maintenance window")
            if warning:
                builder.text("1. Plan certificate renewal within the next 2 weeks")
                builder.text("2. Document certificate rotation procedure")
            builder.text("")
            builder.text("*Certificate Renewal Steps:*")
            builder.text("1. Generate new certificates with extended validity")
            builder.text("2. Update elasticsearch.yml / opensearch.yml with new cert paths")
            builder.text("3. Perform rolling restart of cluster nodes")
            builder.text("4. Verify TLS connectivity after restart")

        # Calculate summary metrics for rules engine
        earliest_expiry_days = min([c["days_until_expiry"] for c in analyzed_certs]) if analyzed_certs else None

        # Return flat structure - report_builder wraps with module name
        structured_data = {
            "status": "success",
            "data": {
                "total_certificates": len(analyzed_certs),
                "expired_count": len(expired),
                "critical_count": len(critical),
                "warning_count": len(warning),
                "ok_count": len(ok),
                "earliest_expiry_days": earliest_expiry_days,
                "has_expired": len(expired) > 0,
                "has_critical": len(critical) > 0,
                "has_warning": len(warning) > 0,
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        builder.error(f"Failed to check SSL certificates: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _add_cert_table(builder, certs, title):
    """Add a table of certificates to the output."""
    builder.h4(title)

    table_data = []
    for cert in certs:
        days = cert["days_until_expiry"]
        if days < 0:
            days_str = f"**EXPIRED** ({abs(days)} days ago)"
        elif days == 0:
            days_str = "**EXPIRES TODAY**"
        elif days < 7:
            days_str = f"**{days} days** 🔴"
        elif days < 30:
            days_str = f"**{days} days** ⚠️"
        else:
            days_str = f"{days} days ✅"

        # Extract common name from subject_dn
        subject = cert.get("subject_dn", "Unknown")
        cn = subject
        if "CN=" in subject:
            cn = subject.split("CN=")[1].split(",")[0]

        table_data.append({
            "Certificate": cn,
            "Path": cert.get("path", "N/A"),
            "Expiry Date": cert.get("expiry_date", "Unknown"),
            "Days Until Expiry": days_str
        })

    builder.table(table_data)

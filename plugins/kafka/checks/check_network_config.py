"""
Network Configuration Check for Kafka clusters.

Analyzes hosts file and network configuration to validate:
- Broker hostname resolution
- Inter-broker connectivity configuration
- DNS/hosts file consistency

Data sources:
- hosts_file.info / /etc/hosts (import and live via SSH)
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 4  # Lower priority - informational


def run_network_config_check(connector, settings):
    """
    Analyzes network configuration for Kafka cluster health.

    Checks:
    - Hosts file entries for cluster nodes
    - Hostname consistency across nodes
    - Potential DNS/resolution issues

    Args:
        connector: Kafka connector (live or import)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Network configuration check")
    if not available:
        return skip_msg, skip_data

    try:
        builder.h3("Network Configuration Analysis")

        # === Collect hosts files from all brokers ===
        all_hosts_files = {}
        errors = []

        ssh_host_to_node = getattr(connector, 'ssh_host_to_node', {})
        cluster_ips = set(connector.get_ssh_hosts())

        for ssh_host in connector.get_ssh_hosts():
            broker_id = ssh_host_to_node.get(ssh_host, ssh_host)

            try:
                # Read /etc/hosts
                command = "cat /etc/hosts 2>/dev/null || echo ''"
                results = connector.execute_ssh_on_all_hosts(command, "Read hosts file")

                for result in results:
                    if result.get('host') == ssh_host and result.get('success'):
                        hosts_content = result.get('output', '')
                        if hosts_content.strip():
                            hosts_entries = _parse_hosts_file(hosts_content)
                            all_hosts_files[broker_id] = {
                                'ip': ssh_host,
                                'entries': hosts_entries,
                                'raw': hosts_content
                            }

            except Exception as e:
                logger.warning(f"Failed to read hosts file from {ssh_host}: {e}")
                errors.append({
                    'host': ssh_host,
                    'broker_id': broker_id,
                    'error': str(e)
                })

        if not all_hosts_files:
            builder.note(
                "No hosts file data available. This could mean:\n\n"
                "* SSH access to /etc/hosts is restricted\n"
                "* Hosts file data is not included in diagnostic export"
            )
            structured_data = {
                "status": "skipped",
                "reason": "no_data"
            }
            return builder.build(), structured_data

        # === Analyze hosts files ===
        issues = []
        warnings = []

        # Check if cluster nodes are in hosts files
        missing_entries = defaultdict(list)
        for broker_id, hosts_data in all_hosts_files.items():
            entries = hosts_data['entries']
            entry_ips = set(entries.keys())

            for cluster_ip in cluster_ips:
                if cluster_ip not in entry_ips:
                    missing_entries[broker_id].append(cluster_ip)

        if missing_entries:
            for broker_id, missing_ips in missing_entries.items():
                if len(missing_ips) > 0:
                    warnings.append({
                        'level': 'warning',
                        'type': 'missing_hosts_entries',
                        'broker_id': broker_id,
                        'message': f'Broker {broker_id} hosts file missing entries for: {", ".join(missing_ips[:3])}{"..." if len(missing_ips) > 3 else ""}'
                    })

        # Check for hostname consistency
        hostname_issues = _check_hostname_consistency(all_hosts_files, cluster_ips)
        warnings.extend(hostname_issues)

        has_critical = any(i.get('level') == 'critical' for i in issues)
        has_warning = len(warnings) > 0

        # === Build report ===
        if has_critical:
            for issue in issues:
                builder.critical(f"**{issue['type'].replace('_', ' ').title()}:** {issue['message']}")
            builder.blank()

        if has_warning:
            for warning in warnings:
                builder.warning(f"**{warning['type'].replace('_', ' ').title()}:** {warning['message']}")
            builder.blank()

        # Cluster nodes summary
        builder.h4("Cluster Node Network Info")

        node_rows = []
        for broker_id, hosts_data in sorted(all_hosts_files.items()):
            ip = hosts_data['ip']
            entries = hosts_data['entries']
            # Count how many cluster IPs are in this hosts file
            cluster_entries = sum(1 for cip in cluster_ips if cip in entries)

            node_rows.append({
                "Broker": str(broker_id),
                "IP": ip,
                "Hosts Entries": str(len(entries)),
                "Cluster Nodes in Hosts": f"{cluster_entries}/{len(cluster_ips)}"
            })

        builder.table(node_rows)
        builder.blank()

        # Show sample hosts entries (from first node)
        first_hosts = list(all_hosts_files.values())[0]
        if first_hosts['entries']:
            builder.h4("Sample Hosts File Entries (First Node)")

            # Filter to show cluster-related entries
            sample_entries = []
            for ip, hostnames in first_hosts['entries'].items():
                if ip in cluster_ips or any('kafka' in h.lower() for h in hostnames):
                    sample_entries.append({
                        "IP Address": ip,
                        "Hostnames": ", ".join(hostnames[:3]) + ("..." if len(hostnames) > 3 else "")
                    })

            if sample_entries:
                builder.table(sample_entries[:10])
            else:
                builder.para("_No Kafka-related entries found in hosts file._")
            builder.blank()

        # Errors
        if errors:
            builder.h4("Collection Errors")
            builder.warning(
                f"Could not collect hosts file from {len(errors)} broker(s):\n\n" +
                "\n".join([f"* Broker {e['broker_id']}: {e['error']}" for e in errors])
            )

        # Recommendations
        if issues or warnings:
            recommendations = {}

            if has_warning:
                recommendations["high"] = [
                    "**Add missing broker entries** to /etc/hosts on affected nodes",
                    "**Ensure consistent hostnames** across all nodes",
                    "**Verify DNS resolution** if not using hosts file entries"
                ]

            recommendations["general"] = [
                "Using /etc/hosts for broker resolution provides consistent connectivity",
                "Ensure all brokers can resolve all other broker hostnames",
                "Consider using DNS for production environments",
                "Advertised listeners should use resolvable hostnames/IPs"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"Network configuration looks healthy.\n\n"
                f"Analyzed hosts files from {len(all_hosts_files)} node(s)."
            )

        # === Structured data ===
        structured_data = {
            "status": "success",
            "data": {
                "nodes_checked": len(all_hosts_files),
                "has_critical_issues": has_critical,
                "has_warnings": has_warning,
                "error_count": len(errors),
            }
        }

    except Exception as e:
        import traceback
        logger.error(f"Network config check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data


def _parse_hosts_file(content):
    """
    Parse /etc/hosts content into structured format.

    Returns:
        dict: IP -> list of hostnames
    """
    entries = defaultdict(list)

    for line in content.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        parts = line.split()
        if len(parts) >= 2:
            ip = parts[0]
            hostnames = parts[1:]
            entries[ip].extend(hostnames)

    return dict(entries)


def _check_hostname_consistency(all_hosts_files, cluster_ips):
    """Check if hostnames are consistent across nodes."""
    issues = []

    if len(all_hosts_files) < 2:
        return issues

    # Collect hostnames for each cluster IP across all nodes
    ip_hostname_map = defaultdict(lambda: defaultdict(set))

    for broker_id, hosts_data in all_hosts_files.items():
        for ip, hostnames in hosts_data['entries'].items():
            if ip in cluster_ips:
                for hostname in hostnames:
                    ip_hostname_map[ip][hostname].add(broker_id)

    # Check if same IP has different hostnames across nodes
    for ip, hostname_nodes in ip_hostname_map.items():
        if len(hostname_nodes) > 1:
            # Multiple different hostnames for same IP
            all_hostnames = list(hostname_nodes.keys())
            if len(all_hostnames) > 1:
                # This might be fine (multiple aliases), but flag if inconsistent
                hostname_counts = {h: len(nodes) for h, nodes in hostname_nodes.items()}
                max_count = max(hostname_counts.values())
                total_nodes = len(all_hosts_files)

                # If some hostnames are only on some nodes, warn
                for hostname, count in hostname_counts.items():
                    if count < total_nodes and count < max_count:
                        issues.append({
                            'level': 'warning',
                            'type': 'inconsistent_hostname',
                            'message': f'Hostname "{hostname}" for {ip} only present on {count}/{total_nodes} nodes'
                        })
                        break  # Only report once per IP

    return issues

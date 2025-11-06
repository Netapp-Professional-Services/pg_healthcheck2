"""
ClickHouse OS-Level Log Analysis Check (SSH-based)

Analyzes ClickHouse server and keeper log files via SSH.
Similar to Instacollector's log file collection functionality.

Requirements:
- SSH access to ClickHouse nodes

Analyzes:
- ClickHouse server logs (errors, warnings, critical events)
- ClickHouse Keeper logs (if applicable)
- Recent error patterns and frequencies
"""

import logging
import re
from datetime import datetime
from collections import Counter
from plugins.common.check_helpers import require_ssh, CheckContentBuilder

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this check."""
    return 6  # Medium-high priority - log analysis provides diagnostic insight


def run_check_os_log_analysis(connector, settings):
    """
    Analyze ClickHouse log files on all nodes via SSH.

    Args:
        connector: ClickHouse connector instance with SSH support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "OS log analysis check")
    if not available:
        return skip_msg, skip_data

    # Add check header
    builder.h3("OS-Level Log Analysis (SSH)")
    builder.para(
        "Analysis of ClickHouse server and keeper log files via SSH. "
        "Identifies error patterns, warnings, and critical events."
    )

    try:
        ssh_hosts = connector.get_ssh_hosts()

        if not ssh_hosts:
            builder.warning("No SSH hosts configured.")
            structured_data["os_log_analysis"] = {"status": "skipped", "reason": "No SSH hosts"}
            return builder.build(), structured_data

        # Log file paths (customizable)
        server_log_paths = settings.get('clickhouse_server_log_paths', [
            '/var/log/clickhouse-server/clickhouse-server.log',
            '/var/log/clickhouse-server/clickhouse-server.err.log'
        ])

        keeper_log_paths = settings.get('clickhouse_keeper_log_paths', [
            '/var/log/clickhouse-keeper/clickhouse-keeper.log',
            '/var/log/clickhouse-keeper/clickhouse-keeper.err.log'
        ])

        # How many lines to analyze
        log_lines_to_analyze = settings.get('clickhouse_log_lines_to_analyze', 5000)

        all_node_logs = []
        errors = []

        # Collect and analyze logs from each node
        for ssh_host in ssh_hosts:
            node_logs = _analyze_node_logs(
                connector, ssh_host, server_log_paths, keeper_log_paths, log_lines_to_analyze
            )

            if 'error' in node_logs:
                errors.append(f"{ssh_host}: {node_logs['error']}")
            else:
                all_node_logs.append(node_logs)

        # Display results
        if all_node_logs:
            _display_log_analysis(builder, all_node_logs, settings)

        if errors:
            builder.h4("⚠️ Collection Errors")
            for error in errors:
                builder.para(f"• {error}")
            builder.blank()

        # Structured data
        structured_data["os_log_analysis"] = {
            "status": "success",
            "nodes_analyzed": len(all_node_logs),
            "errors": len(errors),
            "log_data": all_node_logs
        }

    except Exception as e:
        logger.error(f"OS log analysis check failed: {e}", exc_info=True)
        builder.error(f"Check failed: {e}")
        structured_data["os_log_analysis"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _analyze_node_logs(connector, ssh_host, server_log_paths, keeper_log_paths, log_lines):
    """
    Analyze log files from a single node via SSH.

    Returns:
        dict: Log analysis from the node
    """
    data = {
        'host': ssh_host,
        'node_id': connector.ssh_host_to_node.get(ssh_host, ssh_host)
    }

    try:
        ssh_manager = connector.get_ssh_manager(ssh_host)
        if not ssh_manager:
            return {'host': ssh_host, 'error': 'No SSH manager available'}

        ssh_manager.ensure_connected()

        # Analyze server logs
        data['server_logs'] = _analyze_logs(ssh_manager, server_log_paths, log_lines, 'server')

        # Analyze keeper logs (if present)
        data['keeper_logs'] = _analyze_logs(ssh_manager, keeper_log_paths, log_lines, 'keeper')

    except Exception as e:
        return {'host': ssh_host, 'error': str(e)}

    return data


def _analyze_logs(ssh_manager, log_paths, log_lines, log_type):
    """
    Analyze a set of log files.

    Returns:
        dict: Log analysis results
    """
    analysis = {
        'log_type': log_type,
        'files_analyzed': [],
        'total_lines': 0,
        'error_count': 0,
        'warning_count': 0,
        'critical_count': 0,
        'error_patterns': Counter(),
        'recent_errors': [],
        'recent_critical': []
    }

    for log_path in log_paths:
        try:
            # Check if log file exists
            check_cmd = f"test -f {log_path} && echo 'exists' || echo 'not_found'"
            check_out, _, _ = ssh_manager.execute_command(check_cmd)

            if 'not_found' in check_out:
                logger.debug(f"Log file {log_path} not found")
                continue

            # Get last N lines from log
            tail_cmd = f"tail -n {log_lines} {log_path}"
            log_content, stderr, exit_code = ssh_manager.execute_command(tail_cmd)

            if exit_code != 0:
                logger.warning(f"Failed to read {log_path}: {stderr}")
                continue

            analysis['files_analyzed'].append(log_path)

            # Analyze log content
            lines = log_content.strip().split('\n')
            analysis['total_lines'] += len(lines)

            for line in lines:
                # Count severity levels
                if re.search(r'\b(ERROR|Error|error)\b', line):
                    analysis['error_count'] += 1
                    _add_error_pattern(analysis, line)

                    # Store recent errors (last 10)
                    if len(analysis['recent_errors']) < 10:
                        analysis['recent_errors'].append(_clean_log_line(line))

                if re.search(r'\b(WARN|Warning|warning)\b', line):
                    analysis['warning_count'] += 1

                if re.search(r'\b(CRITICAL|Critical|critical|FATAL|Fatal)\b', line):
                    analysis['critical_count'] += 1

                    # Store recent critical (last 5)
                    if len(analysis['recent_critical']) < 5:
                        analysis['recent_critical'].append(_clean_log_line(line))

        except Exception as e:
            logger.warning(f"Error analyzing {log_path}: {e}")
            continue

    # Convert Counter to sorted list of tuples
    analysis['error_patterns'] = analysis['error_patterns'].most_common(10)

    return analysis


def _add_error_pattern(analysis, log_line):
    """Extract and count error patterns from log line."""
    # Try to extract meaningful error patterns

    # Pattern 1: Exception class names
    exception_match = re.search(r'((?:[A-Z][a-z]+)+Exception|(?:[A-Z][a-z]+)+Error)', log_line)
    if exception_match:
        analysis['error_patterns'][exception_match.group(1)] += 1
        return

    # Pattern 2: Error codes
    code_match = re.search(r'Code:\s*(\d+)', log_line)
    if code_match:
        analysis['error_patterns'][f"Code {code_match.group(1)}"] += 1
        return

    # Pattern 3: Generic error keywords
    for keyword in ['Connection refused', 'Timeout', 'Out of memory', 'Disk full',
                    'Too many open files', 'Permission denied', 'Cannot allocate']:
        if keyword in log_line:
            analysis['error_patterns'][keyword] += 1
            return

    # Pattern 4: Extract first few words after ERROR/Error
    error_context = re.search(r'(?:ERROR|Error|error)[:\s]+([^:,\.]{5,50})', log_line)
    if error_context:
        context = error_context.group(1).strip()
        # Truncate and count
        analysis['error_patterns'][context[:40]] += 1


def _clean_log_line(line):
    """Clean log line for display (truncate, remove timestamp if too long)."""
    # Remove common timestamp patterns to save space
    line = re.sub(r'^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s*', '', line)
    line = re.sub(r'^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]\s*', '', line)

    # Truncate if too long
    if len(line) > 200:
        return line[:197] + "..."

    return line


def _display_log_analysis(builder, all_node_logs, settings):
    """Display log analysis results in the report."""

    # Aggregate statistics across all nodes
    total_errors = 0
    total_warnings = 0
    total_critical = 0
    nodes_with_errors = []
    nodes_with_critical = []

    all_error_patterns = Counter()

    for node in all_node_logs:
        node_id = node['node_id']

        # Server logs
        server = node.get('server_logs', {})
        server_errors = server.get('error_count', 0)
        server_critical = server.get('critical_count', 0)

        # Keeper logs
        keeper = node.get('keeper_logs', {})
        keeper_errors = keeper.get('error_count', 0)
        keeper_critical = keeper.get('critical_count', 0)

        node_total_errors = server_errors + keeper_errors
        node_total_critical = server_critical + keeper_critical

        total_errors += node_total_errors
        total_warnings += server.get('warning_count', 0) + keeper.get('warning_count', 0)
        total_critical += node_total_critical

        if node_total_errors > 0:
            nodes_with_errors.append((node_id, node_total_errors))

        if node_total_critical > 0:
            nodes_with_critical.append((node_id, node_total_critical))

        # Aggregate error patterns
        for pattern, count in server.get('error_patterns', []):
            all_error_patterns[pattern] += count
        for pattern, count in keeper.get('error_patterns', []):
            all_error_patterns[pattern] += count

    # 1. Summary
    builder.h4("Log Analysis Summary")

    if total_critical > 0:
        builder.critical(
            f"🔴 **{total_critical} critical/fatal messages found across {len(nodes_with_critical)} node(s)**\n\n"
            "Immediate investigation required."
        )

    if total_errors > 100:
        builder.warning(
            f"⚠️ **{total_errors} errors found across {len(nodes_with_errors)} node(s)**\n\n"
            "Review error patterns below."
        )

    summary_table = [{
        "Metric": "Total Log Lines Analyzed",
        "Count": sum(n.get('server_logs', {}).get('total_lines', 0) +
                    n.get('keeper_logs', {}).get('total_lines', 0) for n in all_node_logs)
    }, {
        "Metric": "Total Errors",
        "Count": total_errors
    }, {
        "Metric": "Total Warnings",
        "Count": total_warnings
    }, {
        "Metric": "Total Critical/Fatal",
        "Count": total_critical
    }, {
        "Metric": "Nodes with Errors",
        "Count": len(nodes_with_errors)
    }]

    builder.table(summary_table)
    builder.blank()

    # 2. Per-Node Summary
    builder.h4("Per-Node Log Statistics")

    node_table = []
    for node in all_node_logs:
        server = node.get('server_logs', {})
        keeper = node.get('keeper_logs', {})

        server_files = len(server.get('files_analyzed', []))
        keeper_files = len(keeper.get('files_analyzed', []))

        node_table.append({
            "Node": node['node_id'],
            "Server Files": server_files,
            "Keeper Files": keeper_files,
            "Errors": server.get('error_count', 0) + keeper.get('error_count', 0),
            "Warnings": server.get('warning_count', 0) + keeper.get('warning_count', 0),
            "Critical": server.get('critical_count', 0) + keeper.get('critical_count', 0)
        })

    builder.table(node_table)
    builder.blank()

    # 3. Top Error Patterns
    if all_error_patterns:
        builder.h4("Top Error Patterns (All Nodes)")

        pattern_table = []
        for pattern, count in all_error_patterns.most_common(15):
            pattern_table.append({
                "Error Pattern": pattern,
                "Occurrences": count
            })

        builder.table(pattern_table)
        builder.blank()

    # 4. Recent Critical Messages
    has_critical = any(
        len(n.get('server_logs', {}).get('recent_critical', [])) > 0 or
        len(n.get('keeper_logs', {}).get('recent_critical', [])) > 0
        for n in all_node_logs
    )

    if has_critical:
        builder.h4("🔴 Recent Critical/Fatal Messages")

        for node in all_node_logs:
            server_critical = node.get('server_logs', {}).get('recent_critical', [])
            keeper_critical = node.get('keeper_logs', {}).get('recent_critical', [])

            if server_critical or keeper_critical:
                builder.para(f"**Node: {node['node_id']}**")

                if server_critical:
                    builder.para("*Server logs:*")
                    for msg in server_critical:
                        builder.para(f"  • {msg}")

                if keeper_critical:
                    builder.para("*Keeper logs:*")
                    for msg in keeper_critical:
                        builder.para(f"  • {msg}")

                builder.blank()

    # 5. Recent Errors (sample)
    builder.h4("Recent Error Messages (Sample)")
    builder.para("Showing up to 5 recent errors per node for quick diagnosis.")
    builder.blank()

    for node in all_node_logs[:3]:  # Show first 3 nodes
        server_errors = node.get('server_logs', {}).get('recent_errors', [])
        keeper_errors = node.get('keeper_logs', {}).get('recent_errors', [])

        if server_errors or keeper_errors:
            builder.para(f"**Node: {node['node_id']}**")

            if server_errors:
                for msg in server_errors[:5]:
                    builder.para(f"  • {msg}")

            if len(all_node_logs) > 3:
                builder.para(f"_...and {len(all_node_logs) - 3} more nodes_")

            builder.blank()
            break  # Only show one node to keep report concise

    # 6. Recommendations
    recommendations = _generate_recommendations(all_node_logs, total_errors, total_critical, settings)

    if recommendations['critical'] or recommendations['high']:
        builder.recs(recommendations)
    elif total_errors == 0 and total_critical == 0:
        builder.success("✅ No errors or critical messages found in recent logs.")
    else:
        builder.success("Logs analyzed successfully. Review error patterns above if needed.")


def _generate_recommendations(all_node_logs, total_errors, total_critical, settings):
    """Generate recommendations based on log analysis."""
    recs = {
        "critical": [],
        "high": [],
        "general": []
    }

    if total_critical > 0:
        recs["critical"].extend([
            f"{total_critical} critical/fatal messages found - investigate immediately",
            "Review recent critical messages above for root cause",
            "Check system resources (disk, memory) for capacity issues",
            "Verify cluster configuration and connectivity"
        ])

    if total_errors > 500:
        recs["critical"].append(
            f"Very high error count ({total_errors}) - indicates systemic issues"
        )
    elif total_errors > 100:
        recs["high"].append(
            f"Elevated error count ({total_errors}) - review error patterns"
        )

    # Analyze error patterns for specific recommendations
    all_error_patterns = Counter()
    for node in all_node_logs:
        for pattern, count in node.get('server_logs', {}).get('error_patterns', []):
            all_error_patterns[pattern] += count
        for pattern, count in node.get('keeper_logs', {}).get('error_patterns', []):
            all_error_patterns[pattern] += count

    for pattern, count in all_error_patterns.most_common(5):
        if 'memory' in pattern.lower():
            recs["high"].append(f"Memory-related errors detected: '{pattern}' ({count}x) - increase memory or reduce query complexity")
        elif 'connection' in pattern.lower() or 'timeout' in pattern.lower():
            recs["high"].append(f"Connection issues detected: '{pattern}' ({count}x) - check network and firewall")
        elif 'disk' in pattern.lower():
            recs["high"].append(f"Disk-related errors: '{pattern}' ({count}x) - check disk space and I/O")
        elif 'permission' in pattern.lower():
            recs["high"].append(f"Permission errors: '{pattern}' ({count}x) - verify file permissions")

    # General recommendations
    recs["general"].extend([
        "Set up log aggregation and alerting for critical errors",
        "Configure log rotation to prevent disk space issues",
        "Monitor error rates over time to detect anomalies",
        "Review ClickHouse server configuration for optimization",
        "Consider enabling detailed query logging for troubleshooting",
        "Regularly review system.error_log table for historical error trends"
    ])

    return recs

"""
System log analysis check for Cassandra.

Analyzes system.log to detect:
- Error patterns and counts
- Warning patterns
- Slow query indicators
- Exception types
- Startup/shutdown events

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def parse_system_log(content, max_lines=10000):
    """
    Parse Cassandra system.log content.

    Args:
        content: Raw system.log content
        max_lines: Maximum lines to analyze

    Returns:
        dict: Parsed log analysis
    """
    result = {
        'error_count': 0,
        'warn_count': 0,
        'info_count': 0,
        'error_patterns': defaultdict(int),
        'warning_patterns': defaultdict(int),
        'exceptions': defaultdict(int),
        'slow_queries': 0,
        'udf_warnings': 0,
        'compaction_errors': 0,
        'gossip_issues': 0,
        'recent_errors': [],
        'recent_warnings': [],
        'lines_analyzed': 0
    }

    if not content:
        return result

    lines = content.strip().split('\n')
    lines_to_analyze = lines[-max_lines:] if len(lines) > max_lines else lines

    for line in lines_to_analyze:
        result['lines_analyzed'] += 1

        # Determine log level
        if ' ERROR ' in line:
            result['error_count'] += 1

            # Categorize error
            if 'Exception' in line:
                exc_match = re.search(r'(\w+Exception)', line)
                if exc_match:
                    result['exceptions'][exc_match.group(1)] += 1

            if 'compaction' in line.lower():
                result['compaction_errors'] += 1
                result['error_patterns']['Compaction error'] += 1
            elif 'timeout' in line.lower():
                result['error_patterns']['Timeout'] += 1
            elif 'gossip' in line.lower():
                result['gossip_issues'] += 1
                result['error_patterns']['Gossip issue'] += 1
            elif 'out of memory' in line.lower() or 'OutOfMemory' in line:
                result['error_patterns']['Out of memory'] += 1
            elif 'disk' in line.lower() or 'space' in line.lower():
                result['error_patterns']['Disk/space issue'] += 1
            else:
                result['error_patterns']['Other error'] += 1

            # Keep recent errors
            if len(result['recent_errors']) < 10:
                # Extract timestamp and message
                timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
                timestamp = timestamp_match.group(1) if timestamp_match else 'Unknown'
                # Truncate long lines
                msg = line[:200] + '...' if len(line) > 200 else line
                result['recent_errors'].append({
                    'timestamp': timestamp,
                    'message': msg
                })

        elif ' WARN ' in line:
            result['warn_count'] += 1

            # Categorize warnings
            if 'ran longer than' in line.lower():
                result['slow_queries'] += 1
                result['warning_patterns']['Slow query/function'] += 1
            elif 'UDFunction' in line or 'udf' in line.lower():
                result['udf_warnings'] += 1
                result['warning_patterns']['UDF performance'] += 1
            elif 'batch' in line.lower() and 'size' in line.lower():
                result['warning_patterns']['Batch size warning'] += 1
            elif 'timeout' in line.lower():
                result['warning_patterns']['Timeout warning'] += 1
            elif 'heap' in line.lower() or 'memory' in line.lower():
                result['warning_patterns']['Memory warning'] += 1
            elif 'compaction' in line.lower():
                result['warning_patterns']['Compaction warning'] += 1
            else:
                result['warning_patterns']['Other warning'] += 1

            # Keep recent warnings (excluding very common ones)
            if len(result['recent_warnings']) < 10:
                timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
                timestamp = timestamp_match.group(1) if timestamp_match else 'Unknown'
                msg = line[:200] + '...' if len(line) > 200 else line
                result['recent_warnings'].append({
                    'timestamp': timestamp,
                    'message': msg
                })

        elif ' INFO ' in line:
            result['info_count'] += 1

    # Convert defaultdicts to regular dicts
    result['error_patterns'] = dict(result['error_patterns'])
    result['warning_patterns'] = dict(result['warning_patterns'])
    result['exceptions'] = dict(result['exceptions'])

    return result


def run_check_system_log(connector, settings):
    """
    Analyze Cassandra system.log for issues.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("System Log Analysis")
    builder.para("Analyzing `system.log` for errors, warnings, and operational issues.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "System log analysis")
    if not available:
        return skip_msg, skip_data

    try:
        # Get system.log from nodes
        results = connector.execute_ssh_on_all_hosts(
            "tail -5000 /var/log/cassandra/system.log 2>/dev/null || cat system.log 2>/dev/null",
            "Get system.log"
        )

        all_analyses = []
        total_errors = 0
        total_warnings = 0
        all_error_patterns = defaultdict(int)
        all_warning_patterns = defaultdict(int)
        all_exceptions = defaultdict(int)

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            analysis = parse_system_log(result['output'])
            analysis['node'] = node_ip
            all_analyses.append(analysis)

            total_errors += analysis['error_count']
            total_warnings += analysis['warn_count']

            for pattern, count in analysis['error_patterns'].items():
                all_error_patterns[pattern] += count
            for pattern, count in analysis['warning_patterns'].items():
                all_warning_patterns[pattern] += count
            for exc, count in analysis['exceptions'].items():
                all_exceptions[exc] += count

        if not all_analyses:
            builder.note("System logs not available.")
            return builder.build(), {'status': 'no_data'}

        # Summary
        if total_errors > 100:
            builder.critical(f"**{total_errors} errors** found in system logs!")
        elif total_errors > 10:
            builder.warning(f"**{total_errors} errors** found in system logs")
        elif total_errors > 0:
            builder.note(f"{total_errors} errors found in system logs")
        else:
            builder.success("No errors in recent system logs.")
        builder.blank()

        if total_warnings > 1000:
            builder.warning(f"**{total_warnings} warnings** - high warning rate indicates operational issues")
        elif total_warnings > 0:
            builder.text(f"**Warnings:** {total_warnings}")
        builder.blank()

        # Error patterns
        if all_error_patterns:
            builder.h4("Error Patterns")
            pattern_table = []
            for pattern, count in sorted(all_error_patterns.items(), key=lambda x: x[1], reverse=True):
                pattern_table.append({
                    'Pattern': pattern,
                    'Count': count
                })
            builder.table(pattern_table)
            builder.blank()

        # Exception types
        if all_exceptions:
            builder.h4("Exception Types")
            exc_table = []
            for exc, count in sorted(all_exceptions.items(), key=lambda x: x[1], reverse=True)[:10]:
                exc_table.append({
                    'Exception': exc,
                    'Count': count
                })
            builder.table(exc_table)
            builder.blank()

        # Warning patterns
        if all_warning_patterns and total_warnings > 0:
            builder.h4("Warning Patterns")
            warn_table = []
            for pattern, count in sorted(all_warning_patterns.items(), key=lambda x: x[1], reverse=True):
                warn_table.append({
                    'Pattern': pattern,
                    'Count': count
                })
            builder.table(warn_table)
            builder.blank()

        # Slow query analysis
        total_slow = sum(a['slow_queries'] for a in all_analyses)
        total_udf = sum(a['udf_warnings'] for a in all_analyses)
        if total_slow > 0 or total_udf > 0:
            builder.h4("Performance Warnings")
            if total_slow > 0:
                builder.warning(f"**{total_slow} slow query/function warnings** detected")
            if total_udf > 0:
                builder.warning(f"**{total_udf} UDF performance warnings** - user-defined functions running slow")
            builder.blank()

        # Recent errors (from first node with errors)
        for analysis in all_analyses:
            if analysis['recent_errors']:
                builder.h4("Recent Errors (Sample)")
                for err in analysis['recent_errors'][:5]:
                    builder.text(f"- `{err['timestamp']}`: {err['message'][:100]}...")
                builder.blank()
                break

        # Recommendations
        recommendations = []
        if 'Out of memory' in all_error_patterns:
            recommendations.append("Out of memory errors detected - increase heap size or reduce load")
        if 'Timeout' in all_error_patterns or 'Timeout warning' in all_warning_patterns:
            recommendations.append("Timeout issues detected - review timeout settings and cluster load")
        if total_slow > 100:
            recommendations.append("High number of slow queries - review query patterns and indexes")
        if total_udf > 100:
            recommendations.append("UDF performance issues - optimize user-defined functions")
        if 'Compaction error' in all_error_patterns:
            recommendations.append("Compaction errors - check disk space and compaction settings")
        if 'Gossip issue' in all_error_patterns:
            recommendations.append("Gossip issues detected - check network connectivity between nodes")

        if recommendations:
            builder.h4("Recommendations")
            builder.tip("**Based on log analysis:**")
            for rec in recommendations:
                builder.text(f"- {rec}")
            builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_analyses),
                'total_errors': total_errors,
                'total_warnings': total_warnings,
                'total_slow_queries': total_slow,
                'total_udf_warnings': total_udf,
                'has_errors': total_errors > 0,
                'has_critical_errors': total_errors > 100,
                'has_oom_errors': 'Out of memory' in all_error_patterns,
                'has_timeout_issues': 'Timeout' in all_error_patterns or 'Timeout warning' in all_warning_patterns,
                'has_compaction_errors': 'Compaction error' in all_error_patterns,
                'has_gossip_issues': 'Gossip issue' in all_error_patterns,
                'error_patterns': dict(all_error_patterns),
                'exception_types': dict(all_exceptions)
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"System log check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"System log check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

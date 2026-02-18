"""
Cassandra environment check for Cassandra.

Analyzes cassandra-env.sh file to detect:
- Heap size configuration (MAX_HEAP_SIZE, HEAP_NEWSIZE)
- JMX settings
- GC configuration overrides
- Environment-specific tuning

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 6


def parse_size_to_mb(size_str):
    """
    Parse size string to megabytes.

    Args:
        size_str: Size string like "8G", "8192M", "8192"

    Returns:
        int: Size in MB, or None if unable to parse
    """
    if not size_str:
        return None

    size_str = size_str.strip().upper()

    match = re.match(r'^(\d+(?:\.\d+)?)\s*([GMKT]?)B?$', size_str)
    if match:
        value = float(match.group(1))
        unit = match.group(2)

        if unit == 'G':
            return int(value * 1024)
        elif unit == 'T':
            return int(value * 1024 * 1024)
        elif unit == 'K':
            return int(value / 1024)
        elif unit == 'M' or unit == '':
            return int(value)

    return None


def parse_cassandra_env(content):
    """
    Parse cassandra-env.sh file content.

    Args:
        content: Raw cassandra-env.sh content

    Returns:
        dict: Parsed environment settings
    """
    result = {
        'max_heap_size': None,
        'heap_newsize': None,
        'jmx_port': None,
        'local_jmx': None,
        'heap_dump_dir': None,
        'gc_settings': [],
        'jvm_opts': [],
        'custom_settings': [],
        'uses_calculated_heap': False
    }

    if not content:
        return result

    lines = content.strip().split('\n')

    for line in lines:
        # Remove leading/trailing whitespace but preserve the line structure
        stripped = line.strip()

        # Skip comments and empty lines
        if not stripped or stripped.startswith('#'):
            continue

        # Look for MAX_HEAP_SIZE assignments
        heap_match = re.search(r'MAX_HEAP_SIZE["\']?=["\'"]?([^"\'"\s]+)', stripped)
        if heap_match:
            value = heap_match.group(1)
            if '$' not in value and value not in ['""', "''"]:
                result['max_heap_size'] = value
            elif '${' in value or '$max_heap' in value.lower():
                result['uses_calculated_heap'] = True

        # Look for HEAP_NEWSIZE assignments
        newsize_match = re.search(r'HEAP_NEWSIZE["\']?=["\'"]?([^"\'"\s]+)', stripped)
        if newsize_match:
            value = newsize_match.group(1)
            if '$' not in value and value not in ['""', "''"]:
                result['heap_newsize'] = value

        # JMX port
        jmx_match = re.search(r'JMX_PORT["\']?=["\'"]?(\d+)', stripped)
        if jmx_match:
            result['jmx_port'] = int(jmx_match.group(1))

        # LOCAL_JMX
        if 'LOCAL_JMX=' in stripped:
            if 'yes' in stripped.lower():
                result['local_jmx'] = True
            elif 'no' in stripped.lower():
                result['local_jmx'] = False

        # Heap dump directory
        dump_match = re.search(r'CASSANDRA_HEAPDUMP_DIR["\']?=["\'"]?([^"\'"\s]+)', stripped)
        if dump_match:
            result['heap_dump_dir'] = dump_match.group(1)

        # JVM_OPTS additions (GC and other settings)
        if 'JVM_OPTS=' in stripped and '-X' in stripped:
            # Extract the JVM option
            opt_match = re.search(r'(-X[^\s"\']+)', stripped)
            if opt_match:
                if 'GC' in stripped or 'gc' in stripped:
                    result['gc_settings'].append(opt_match.group(1))
                else:
                    result['jvm_opts'].append(opt_match.group(1))

    return result


def run_check_cassandra_env(connector, settings):
    """
    Audit cassandra-env.sh configuration.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("Cassandra Environment Audit")
    builder.para("Analyzing `cassandra-env.sh` for heap and environment settings.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "Cassandra env audit")
    if not available:
        return skip_msg, skip_data

    try:
        # Get cassandra-env.sh from nodes
        results = connector.execute_ssh_on_all_hosts(
            "cat /etc/cassandra/cassandra-env.sh || cat /etc/cassandra/conf/cassandra-env.sh",
            "Get cassandra-env.sh"
        )

        all_configs = []
        issues = []

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            config = parse_cassandra_env(result['output'])
            config['node'] = node_ip
            all_configs.append(config)

        if not all_configs:
            builder.note("cassandra-env.sh not available.")
            return builder.build(), {'status': 'no_data'}

        # Use first config for display
        config = all_configs[0]

        # Heap settings
        builder.h4("Heap Configuration")

        if config['max_heap_size']:
            heap_mb = parse_size_to_mb(config['max_heap_size'])
            builder.text(f"**MAX_HEAP_SIZE:** {config['max_heap_size']}")
            if heap_mb:
                if heap_mb > 32768:
                    builder.warning("Heap larger than 32GB may cause long GC pauses")
                    issues.append({
                        'type': 'large_heap',
                        'detail': f"Heap size {config['max_heap_size']} exceeds 32GB",
                        'severity': 'warning'
                    })
                elif heap_mb < 4096:
                    builder.warning("Heap smaller than 4GB may cause frequent GC")
                    issues.append({
                        'type': 'small_heap',
                        'detail': f"Heap size {config['max_heap_size']} is less than 4GB",
                        'severity': 'warning'
                    })
        elif config['uses_calculated_heap']:
            builder.text("**MAX_HEAP_SIZE:** Calculated automatically based on system memory")
        else:
            builder.text("**MAX_HEAP_SIZE:** Not explicitly set")
            issues.append({
                'type': 'heap_not_set',
                'detail': 'MAX_HEAP_SIZE not explicitly configured',
                'severity': 'info'
            })

        if config['heap_newsize']:
            builder.text(f"**HEAP_NEWSIZE:** {config['heap_newsize']}")
        builder.blank()

        # JMX Settings
        builder.h4("JMX Configuration")
        if config['jmx_port']:
            builder.text(f"**JMX Port:** {config['jmx_port']}")
        else:
            builder.text("**JMX Port:** 7199 (default)")

        if config['local_jmx'] is True:
            builder.text("**Local JMX:** Enabled (recommended for security)")
        elif config['local_jmx'] is False:
            builder.warning("**Local JMX:** Disabled - JMX accessible remotely")
            builder.text("_Consider enabling LOCAL_JMX=yes for security_")
            issues.append({
                'type': 'remote_jmx',
                'detail': 'JMX is accessible remotely',
                'severity': 'warning'
            })
        builder.blank()

        # Heap dump
        if config['heap_dump_dir']:
            builder.text(f"**Heap Dump Directory:** {config['heap_dump_dir']}")
            builder.blank()

        # GC and JVM settings
        if config['gc_settings']:
            builder.h4("GC Settings (from cassandra-env.sh)")
            seen = set()
            for setting in config['gc_settings']:
                if setting not in seen:
                    seen.add(setting)
                    builder.text(f"- `{setting}`")
            builder.blank()

        # Recommendations
        recommendations = []
        if not config['max_heap_size'] and not config['uses_calculated_heap']:
            recommendations.append("Set MAX_HEAP_SIZE explicitly for predictable memory usage")
        if config['local_jmx'] is False:
            recommendations.append("Enable LOCAL_JMX=yes to restrict JMX to localhost")
        if config['max_heap_size']:
            heap_mb = parse_size_to_mb(config['max_heap_size'])
            if heap_mb and heap_mb > 32768:
                recommendations.append("Consider reducing heap to 31GB max to enable compressed oops")

        if recommendations:
            builder.h4("Recommendations")
            builder.tip("**Environment configuration improvements:**")
            for rec in recommendations:
                builder.text(f"- {rec}")
            builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_configs),
                'max_heap_size': config['max_heap_size'],
                'heap_newsize': config['heap_newsize'],
                'heap_mb': parse_size_to_mb(config['max_heap_size']) if config['max_heap_size'] else None,
                'jmx_port': config['jmx_port'] or 7199,
                'local_jmx': config['local_jmx'],
                'remote_jmx_enabled': config['local_jmx'] is False,
                'heap_explicitly_set': config['max_heap_size'] is not None,
                'uses_calculated_heap': config['uses_calculated_heap'],
                'issue_count': len(issues),
                'issues': issues
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"Cassandra env check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"Cassandra env check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

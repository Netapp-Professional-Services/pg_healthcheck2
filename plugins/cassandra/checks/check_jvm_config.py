"""
JVM configuration check for Cassandra.

Analyzes jvm.options file to detect:
- GC algorithm settings (CMS vs G1)
- JVM flags and their implications
- Deprecated or problematic options
- Missing recommended settings

Works with both live SSH and Instacollector imported data.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 6


def parse_jvm_options(content):
    """
    Parse jvm.options file content.

    Args:
        content: Raw jvm.options file content

    Returns:
        dict: Parsed JVM settings
    """
    result = {
        'gc_algorithm': 'unknown',
        'heap_settings': {},
        'gc_settings': [],
        'other_flags': [],
        'deprecated_flags': [],
        'raw_flags': []
    }

    if not content:
        return result

    # Known deprecated/problematic flags
    deprecated = {
        '-XX:+UseParNewGC': 'Deprecated in Java 9+, use G1GC instead',
        '-XX:+UseConcMarkSweepGC': 'Deprecated in Java 9+, consider G1GC',
        '-XX:+CMSParallelRemarkEnabled': 'CMS is deprecated',
        '-XX:+UseCMSInitiatingOccupancyOnly': 'CMS is deprecated',
        '-XX:CMSInitiatingOccupancyFraction': 'CMS is deprecated',
        '-XX:+PrintGCDetails': 'Use -Xlog:gc* in Java 9+',
        '-XX:+PrintGCDateStamps': 'Use -Xlog:gc* in Java 9+',
        '-XX:+PrintHeapAtGC': 'Use -Xlog:gc* in Java 9+',
        '-XX:+PrintTenuringDistribution': 'Use -Xlog:gc* in Java 9+',
        '-XX:+PrintGCApplicationStoppedTime': 'Use -Xlog:gc* in Java 9+',
    }

    lines = content.strip().split('\n')

    for line in lines:
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith('#'):
            continue

        # Must start with - to be a JVM flag
        if not line.startswith('-'):
            continue

        result['raw_flags'].append(line)

        # Detect GC algorithm
        if 'UseG1GC' in line:
            result['gc_algorithm'] = 'G1GC'
        elif 'UseConcMarkSweepGC' in line or 'CMS' in line:
            result['gc_algorithm'] = 'CMS'
        elif 'UseZGC' in line:
            result['gc_algorithm'] = 'ZGC'
        elif 'UseShenandoahGC' in line:
            result['gc_algorithm'] = 'Shenandoah'

        # Parse heap settings
        if line.startswith('-Xms'):
            result['heap_settings']['initial_heap'] = line[4:]
        elif line.startswith('-Xmx'):
            result['heap_settings']['max_heap'] = line[4:]
        elif line.startswith('-Xmn'):
            result['heap_settings']['young_gen'] = line[4:]

        # Check for deprecated flags
        for dep_flag, reason in deprecated.items():
            if dep_flag in line or line.startswith(dep_flag.split('=')[0] + '='):
                result['deprecated_flags'].append({
                    'flag': line,
                    'reason': reason
                })
                break
        else:
            # GC-related settings
            if 'GC' in line or 'gc' in line.lower() or 'Heap' in line:
                result['gc_settings'].append(line)
            else:
                result['other_flags'].append(line)

    return result


def run_check_jvm_config(connector, settings):
    """
    Audit JVM configuration from jvm.options.

    Args:
        connector: Database connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder()
    builder.h3("JVM Configuration Audit")
    builder.para("Analyzing `jvm.options` for JVM settings and potential issues.")

    # Check for data availability
    available, skip_msg, skip_data = require_ssh(connector, "JVM config audit")
    if not available:
        return skip_msg, skip_data

    try:
        # Get jvm.options from nodes
        results = connector.execute_ssh_on_all_hosts(
            "cat /etc/cassandra/jvm.options || cat /etc/cassandra/conf/jvm.options",
            "Get JVM options"
        )

        all_configs = []
        issues = []

        for result in results:
            if not result.get('success') or not result.get('output'):
                continue

            node_ip = result['host']
            config = parse_jvm_options(result['output'])
            config['node'] = node_ip
            all_configs.append(config)

        if not all_configs:
            builder.note("JVM options file not available.")
            return builder.build(), {'status': 'no_data'}

        # Use first config for display (should be same across nodes)
        config = all_configs[0]

        # Summary
        builder.text(f"**GC Algorithm:** {config['gc_algorithm'].upper()}")
        if config['heap_settings']:
            heap_info = []
            if 'initial_heap' in config['heap_settings']:
                heap_info.append(f"Initial: {config['heap_settings']['initial_heap']}")
            if 'max_heap' in config['heap_settings']:
                heap_info.append(f"Max: {config['heap_settings']['max_heap']}")
            if heap_info:
                builder.text(f"**Heap:** {', '.join(heap_info)}")
        builder.blank()

        # GC Algorithm analysis
        if config['gc_algorithm'] == 'CMS':
            builder.warning("**CMS garbage collector detected** - deprecated in Java 9+")
            builder.blank()

            builder.text("*Why Migrate to G1GC:*")
            builder.blank()
            builder.text("- CMS is removed in Java 14+, limiting upgrade path")
            builder.text("- G1GC provides more predictable pause times")
            builder.text("- Better handling of large heaps (>4GB)")
            builder.text("- Reduced tuning complexity")
            builder.blank()

            builder.text("*Migration Steps:*")
            builder.blank()
            builder.text("1. **Backup current jvm.options:**")
            builder.text("   `cp /etc/cassandra/jvm.options /etc/cassandra/jvm.options.backup`")
            builder.blank()
            builder.text("2. **Edit jvm.options** - Comment out CMS flags and add G1GC:")
            builder.text("   ```")
            builder.text("   # Comment out these CMS-specific flags:")
            builder.text("   # -XX:+UseParNewGC")
            builder.text("   # -XX:+UseConcMarkSweepGC")
            builder.text("   # -XX:+CMSParallelRemarkEnabled")
            builder.text("   # -XX:CMSInitiatingOccupancyFraction=75")
            builder.text("   # -XX:+UseCMSInitiatingOccupancyOnly")
            builder.text("   ")
            builder.text("   # Add G1GC configuration:")
            builder.text("   -XX:+UseG1GC")
            builder.text("   -XX:MaxGCPauseMillis=500")
            builder.text("   -XX:G1RSetUpdatingPauseTimePercent=5")
            builder.text("   -XX:InitiatingHeapOccupancyPercent=70")
            builder.text("   -XX:ParallelGCThreads=4")
            builder.text("   -XX:ConcGCThreads=4")
            builder.text("   ```")
            builder.blank()
            builder.text("3. **Test on one node first** (rolling restart approach)")
            builder.text("4. **Monitor GC behavior:** `nodetool gcstats` and `/var/log/cassandra/gc.log`")
            builder.text("5. **Roll out to remaining nodes** after validating performance")
            builder.blank()

            issues.append({
                'type': 'deprecated_gc',
                'detail': 'CMS garbage collector is deprecated',
                'severity': 'warning'
            })
        elif config['gc_algorithm'] == 'G1GC':
            builder.success("G1GC garbage collector configured (recommended)")
            builder.blank()
        elif config['gc_algorithm'] == 'unknown':
            builder.note("GC algorithm not explicitly set - JVM default will be used")
            builder.blank()

        # Deprecated flags
        if config['deprecated_flags']:
            builder.h4("Deprecated JVM Flags")
            builder.warning(f"**{len(config['deprecated_flags'])} deprecated flag(s) found:**")
            builder.blank()
            for flag in config['deprecated_flags']:
                builder.text(f"- `{flag['flag']}`")
                builder.text(f"  _{flag['reason']}_")
                issues.append({
                    'type': 'deprecated_flag',
                    'flag': flag['flag'],
                    'reason': flag['reason'],
                    'severity': 'warning'
                })
            builder.blank()

        # GC Settings
        if config['gc_settings']:
            builder.h4("GC Settings")
            # Deduplicate
            seen = set()
            for setting in config['gc_settings']:
                if setting not in seen:
                    seen.add(setting)
                    builder.text(f"- `{setting}`")
            builder.blank()

        # Key JVM flags
        important_flags = [f for f in config['other_flags'] if any(
            x in f for x in ['Xss', 'MaxDirectMemory', 'agent', 'javaagent', 'cassandra']
        )]
        if important_flags:
            builder.h4("Notable JVM Flags")
            seen = set()
            for flag in important_flags:
                if flag not in seen:
                    seen.add(flag)
                    builder.text(f"- `{flag}`")
            builder.blank()

        # Recommendations
        recommendations = []
        if config['gc_algorithm'] == 'CMS':
            recommendations.append("Migrate from CMS to G1GC for Java 9+ compatibility")
        if config['deprecated_flags']:
            recommendations.append("Remove or update deprecated JVM flags")
        if not config['heap_settings'].get('max_heap'):
            recommendations.append("Explicitly set -Xmx in jvm.options for predictable memory usage")

        if recommendations:
            builder.h4("Recommendations")
            builder.tip("**JVM configuration improvements:**")
            for rec in recommendations:
                builder.text(f"- {rec}")
            builder.blank()

        # Structured data
        structured_data = {
            'status': 'success',
            'data': {
                'nodes_checked': len(all_configs),
                'gc_algorithm': config['gc_algorithm'],
                'has_deprecated_flags': len(config['deprecated_flags']) > 0,
                'deprecated_flag_count': len(config['deprecated_flags']),
                'uses_cms': config['gc_algorithm'] == 'CMS',
                'uses_g1gc': config['gc_algorithm'] == 'G1GC',
                'heap_initial': config['heap_settings'].get('initial_heap'),
                'heap_max': config['heap_settings'].get('max_heap'),
                'total_flags': len(config['raw_flags']),
                'issues': issues
            }
        }

        return builder.build(), structured_data

    except Exception as e:
        logger.error(f"JVM config check failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        builder.error(f"JVM config check failed: {e}")
        return builder.build(), {'status': 'error', 'error': str(e)}

"""
JVM statistics check for Cassandra brokers.

Monitors JVM heap usage, garbage collection activity, and memory pool
statistics across all broker nodes. This module serves as a template
pattern for JVM monitoring that can be adapted for other databases
like Cassandra, MongoDB, etc.

Key Metrics Monitored:
- Heap memory usage (Eden, Survivor, Old Generation)
- Non-heap memory (Metaspace, Code Cache)
- GC activity (count, time, frequency)
- Memory pool utilization

Template Pattern Features:
- Reusable parsing functions
- Configurable thresholds
- Multi-host execution
- Structured data for rules engine
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 8


def parse_jstat_gc(output):
    """
    Parse jstat -gc output into structured data.
    
    Template function that can be reused across database types.
    
    Args:
        output: Raw jstat -gc output
        
    Returns:
        dict: Parsed GC statistics or None if parsing fails
    """
    try:
        lines = output.strip().split('\n')
        if len(lines) < 2:
            return None
        
        # Header line has column names
        headers = lines[0].split()
        # Data line has values
        values = lines[1].split()
        
        if len(headers) != len(values):
            logger.warning(f"jstat header/value mismatch: {len(headers)} vs {len(values)}")
            return None
        
        # Create dict from headers and values
        gc_data = {}
        for i, header in enumerate(headers):
            try:
                gc_data[header] = float(values[i])
            except ValueError:
                gc_data[header] = values[i]
        
        return gc_data
        
    except Exception as e:
        logger.error(f"Error parsing jstat -gc output: {e}")
        return None


def parse_jstat_gcutil(output):
    """
    Parse jstat -gcutil output into structured data.
    
    Template function for percentage-based GC stats.
    
    Args:
        output: Raw jstat -gcutil output
        
    Returns:
        dict: Parsed GC utilization statistics or None
    """
    try:
        lines = output.strip().split('\n')
        if len(lines) < 2:
            return None
        
        headers = lines[0].split()
        values = lines[1].split()
        
        if len(headers) != len(values):
            return None
        
        gcutil_data = {}
        for i, header in enumerate(headers):
            try:
                gcutil_data[header] = float(values[i])
            except ValueError:
                gcutil_data[header] = values[i]
        
        return gcutil_data
        
    except Exception as e:
        logger.error(f"Error parsing jstat -gcutil output: {e}")
        return None


def calculate_heap_usage(gc_data):
    """
    Calculate heap memory usage from jstat data.
    
    Template function for heap calculations.
    
    Args:
        gc_data: Parsed jstat -gc data
        
    Returns:
        dict: Heap usage statistics
    """
    try:
        # Young generation (Eden + Survivor spaces)
        eden_used = gc_data.get('EU', 0)  # Eden Used
        survivor0_used = gc_data.get('S0U', 0)  # Survivor 0 Used
        survivor1_used = gc_data.get('S1U', 0)  # Survivor 1 Used
        young_used = eden_used + survivor0_used + survivor1_used
        
        eden_capacity = gc_data.get('EC', 0)  # Eden Capacity
        survivor0_capacity = gc_data.get('S0C', 0)
        survivor1_capacity = gc_data.get('S1C', 0)
        young_capacity = eden_capacity + survivor0_capacity + survivor1_capacity
        
        # Old generation
        old_used = gc_data.get('OU', 0)  # Old Used
        old_capacity = gc_data.get('OC', 0)  # Old Capacity
        
        # Total heap
        heap_used = young_used + old_used
        heap_capacity = young_capacity + old_capacity
        
        # Metaspace (non-heap)
        metaspace_used = gc_data.get('MU', 0)  # Metaspace Used
        metaspace_capacity = gc_data.get('MC', 0)  # Metaspace Capacity
        
        return {
            'young_used_kb': young_used,
            'young_capacity_kb': young_capacity,
            'young_util_percent': (young_used / young_capacity * 100) if young_capacity > 0 else 0,
            'old_used_kb': old_used,
            'old_capacity_kb': old_capacity,
            'old_util_percent': (old_used / old_capacity * 100) if old_capacity > 0 else 0,
            'heap_used_kb': heap_used,
            'heap_capacity_kb': heap_capacity,
            'heap_util_percent': (heap_used / heap_capacity * 100) if heap_capacity > 0 else 0,
            'metaspace_used_kb': metaspace_used,
            'metaspace_capacity_kb': metaspace_capacity,
            'metaspace_util_percent': (metaspace_used / metaspace_capacity * 100) if metaspace_capacity > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Error calculating heap usage: {e}")
        return None


def calculate_gc_metrics(gc_data, gcutil_data):
    """
    Calculate GC performance metrics.

    Template function for GC analysis.

    Args:
        gc_data: Parsed jstat -gc data
        gcutil_data: Parsed jstat -gcutil data

    Returns:
        dict: GC performance metrics
    """
    try:
        # Young GC (Minor GC)
        ygc_count = gc_data.get('YGC', 0)  # Young GC count
        ygc_time = gc_data.get('YGCT', 0)  # Young GC time (seconds)

        # Full GC (Major GC)
        fgc_count = gc_data.get('FGC', 0)  # Full GC count
        fgc_time = gc_data.get('FGCT', 0)  # Full GC time (seconds)

        # Total
        total_gc_time = ygc_time + fgc_time
        total_gc_count = ygc_count + fgc_count

        # Calculate averages
        avg_ygc_time = (ygc_time / ygc_count * 1000) if ygc_count > 0 else 0  # Convert to ms
        avg_fgc_time = (fgc_time / fgc_count * 1000) if fgc_count > 0 else 0

        return {
            'young_gc_count': int(ygc_count),
            'young_gc_time_sec': ygc_time,
            'avg_young_gc_time_ms': avg_ygc_time,
            'full_gc_count': int(fgc_count),
            'full_gc_time_sec': fgc_time,
            'avg_full_gc_time_ms': avg_fgc_time,
            'total_gc_time_sec': total_gc_time,
            'total_gc_count': int(total_gc_count),
            'gc_time_percent': gcutil_data.get('GCT', 0) if gcutil_data else 0
        }

    except Exception as e:
        logger.error(f"Error calculating GC metrics: {e}")
        return None


def parse_gc_log_events(gc_log_output):
    """
    Parse GC log output to detect Full GC events.

    Supports both formats:
    - Old (Pre-Java 9): [Full GC (Allocation Failure) 2019-10-30T11:13:00.920-0100: 6.399: [CMS: 43711K->43711K(43712K), 0.1417937 secs]
    - New (Java 9+): [2019-10-30T11:13:00.920-0100][info][gc] GC(123) Pause Full (Allocation Failure) 43711K->43711K(43712K) 141.793ms

    Args:
        gc_log_output: Raw GC log content (last N lines)

    Returns:
        list: List of Full GC event dictionaries
    """
    full_gc_events = []

    try:
        lines = gc_log_output.strip().split('\n')

        for line in lines:
            event = None

            # Try Java 9+ unified logging format first
            # Pattern: [timestamp][level][gc] GC(N) Pause Full (Cause) heapK->heapK(totalK) timeMs
            if 'Pause Full' in line or 'pause full' in line.lower():
                event = {'raw_line': line, 'format': 'unified'}

                # Extract timestamp from [timestamp] tag
                timestamp_match = re.search(r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)[^\]]*\]', line)
                if timestamp_match:
                    event['timestamp'] = timestamp_match.group(1)[:19]  # Strip milliseconds for consistency
                else:
                    event['timestamp'] = 'Unknown'

                # Extract cause from Pause Full (Cause) or Full (Cause)
                cause_match = re.search(r'(?:Pause )?Full\s*\(([^)]+)\)', line, re.IGNORECASE)
                if cause_match:
                    event['cause'] = cause_match.group(1)
                else:
                    event['cause'] = 'Unknown'

                # Extract pause time (milliseconds in unified logging)
                # Patterns: 141.793ms, 0.142s, 142ms
                pause_match = re.search(r'(\d+\.?\d*)ms\b', line)
                if pause_match:
                    pause_ms = float(pause_match.group(1))
                    event['pause_time_ms'] = pause_ms
                    event['pause_time_sec'] = pause_ms / 1000
                else:
                    # Try seconds format
                    pause_match_sec = re.search(r'(\d+\.?\d*)s\b', line)
                    if pause_match_sec:
                        pause_sec = float(pause_match_sec.group(1))
                        event['pause_time_sec'] = pause_sec
                        event['pause_time_ms'] = pause_sec * 1000
                    else:
                        event['pause_time_ms'] = 0
                        event['pause_time_sec'] = 0

                # Extract heap before->after (e.g., "43711K->43711K(43712K)" or "42M->41M(64M)")
                heap_match = re.search(r'(\d+)([KMG])->(\d+)([KMG])\((\d+)([KMG])\)', line)
                if heap_match:
                    before_val = int(heap_match.group(1))
                    before_unit = heap_match.group(2)
                    after_val = int(heap_match.group(3))
                    after_unit = heap_match.group(4)
                    total_val = int(heap_match.group(5))
                    total_unit = heap_match.group(6)

                    # Convert to KB
                    def to_kb(val, unit):
                        if unit == 'K': return val
                        if unit == 'M': return val * 1024
                        if unit == 'G': return val * 1024 * 1024
                        return val

                    event['heap_before_kb'] = to_kb(before_val, before_unit)
                    event['heap_after_kb'] = to_kb(after_val, after_unit)
                    event['heap_total_kb'] = to_kb(total_val, total_unit)
                    event['heap_reclaimed_kb'] = event['heap_before_kb'] - event['heap_after_kb']
                    event['heap_reclaimed_pct'] = (event['heap_reclaimed_kb'] / event['heap_before_kb'] * 100) if event['heap_before_kb'] > 0 else 0
                else:
                    event['heap_before_kb'] = 0
                    event['heap_after_kb'] = 0
                    event['heap_total_kb'] = 0
                    event['heap_reclaimed_kb'] = 0
                    event['heap_reclaimed_pct'] = 0

            # Try old format (pre-Java 9)
            elif '[Full GC' in line:
                event = {'raw_line': line, 'format': 'legacy'}

                # Extract cause (e.g., "Allocation Failure", "Metadata GC Threshold", "System.gc()")
                cause_match = re.search(r'\[Full GC \(([^)]+)\)', line)
                if cause_match:
                    event['cause'] = cause_match.group(1)
                else:
                    event['cause'] = 'Unknown'

                # Extract timestamp
                timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', line)
                if timestamp_match:
                    event['timestamp'] = timestamp_match.group(1)
                else:
                    event['timestamp'] = 'Unknown'

                # Extract pause time (e.g., "0.1417937 secs")
                pause_match = re.search(r'(\d+\.\d+)\s+secs?\]', line)
                if pause_match:
                    pause_time = float(pause_match.group(1))
                    event['pause_time_sec'] = pause_time
                    event['pause_time_ms'] = pause_time * 1000
                else:
                    event['pause_time_sec'] = 0
                    event['pause_time_ms'] = 0

                # Extract heap before->after (e.g., "43711K->43711K(43712K)")
                heap_match = re.search(r'(\d+)K->(\d+)K\((\d+)K\)', line)
                if heap_match:
                    event['heap_before_kb'] = int(heap_match.group(1))
                    event['heap_after_kb'] = int(heap_match.group(2))
                    event['heap_total_kb'] = int(heap_match.group(3))
                    event['heap_reclaimed_kb'] = event['heap_before_kb'] - event['heap_after_kb']
                    event['heap_reclaimed_pct'] = (event['heap_reclaimed_kb'] / event['heap_before_kb'] * 100) if event['heap_before_kb'] > 0 else 0
                else:
                    event['heap_before_kb'] = 0
                    event['heap_after_kb'] = 0
                    event['heap_total_kb'] = 0
                    event['heap_reclaimed_kb'] = 0
                    event['heap_reclaimed_pct'] = 0

            if event:
                full_gc_events.append(event)

    except Exception as e:
        logger.error(f"Error parsing GC log events: {e}")

    return full_gc_events


def run_check_jvm_stats(connector, settings):
    """
    Checks JVM memory and GC statistics on all Cassandra nodes via SSH.

    Monitors JVM heap usage, garbage collection activity, Full GC events,
    and production-readiness of JVM monitoring configuration.

    Args:
        connector: Database connector with multi-host SSH support
        settings: Configuration settings with thresholds

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    adoc_content = ["=== JVM Memory & GC Statistics (All Nodes)", ""]
    structured_data = {}

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "JVM statistics check")
    if not available:
        return skip_msg, skip_data
    
    try:
        # === STEP 1: GET THRESHOLDS ===
        heap_warning_percent = settings.get('cassandra_jvm_heap_warning_percent', 75)
        heap_critical_percent = settings.get('cassandra_jvm_heap_critical_percent', 90)
        old_gen_warning_percent = settings.get('cassandra_jvm_old_gen_warning_percent', 80)
        old_gen_critical_percent = settings.get('cassandra_jvm_old_gen_critical_percent', 90)
        metaspace_warning_percent = settings.get('cassandra_jvm_metaspace_warning_percent', 85)
        metaspace_critical_percent = settings.get('cassandra_jvm_metaspace_critical_percent', 95)
        fgc_warning_count = settings.get('cassandra_jvm_fgc_warning_count', 10)
        fgc_critical_count = settings.get('cassandra_jvm_fgc_critical_count', 50)
        
        # === STEP 2: EXECUTE ON ALL HOSTS ===
        gc_log_lines = settings.get('cassandra_gc_log_lines', 500)  # Number of log lines to check

        command = f"""
# Find Cassandra Java process
CASSANDRA_PID=$(ps aux | grep -i cassandra | grep java | grep -v grep | awk '{{print $2}}' | head -1)

# Get full username without truncation - ps -o user= doesn't truncate
if [ -n "$CASSANDRA_PID" ]; then
    CASSANDRA_USER=$(ps -o user= -p $CASSANDRA_PID 2>/dev/null | tr -d ' ')
fi

# Fallback to ps aux if above fails
if [ -z "$CASSANDRA_USER" ]; then
    CASSANDRA_USER=$(ps aux | grep -i cassandra | grep java | grep -v grep | awk '{{print $1}}' | head -1)
fi

if [ -z "$CASSANDRA_PID" ]; then
    echo "ERROR: Cassandra process not found"
    exit 1
fi

if [ -z "$CASSANDRA_USER" ]; then
    CASSANDRA_USER=cassandra
fi

echo "CASSANDRA_PID=$CASSANDRA_PID"
echo "CASSANDRA_USER=$CASSANDRA_USER"

# Get GC stats (run as Cassandra user if not already that user)
echo "=== GC_STATS ==="
CURRENT_USER=$(whoami)
echo "CURRENT_USER=$CURRENT_USER"

# Try jstat - need to run as the same user as Cassandra process
if [ "$CURRENT_USER" = "$CASSANDRA_USER" ]; then
    jstat -gc $CASSANDRA_PID 2>&1
else
    # Try with sudo first
    if sudo -n -u $CASSANDRA_USER jstat -gc $CASSANDRA_PID 2>&1; then
        true  # Success
    else
        # Sudo failed, try direct (may fail with permission error but we'll catch it)
        echo "WARNING: Cannot run jstat as $CASSANDRA_USER user. Trying direct access..."
        jstat -gc $CASSANDRA_PID 2>&1 || echo "JSTAT_FAILED: Operation not permitted - need to run as $CASSANDRA_USER user"
    fi
fi

# Get GC summary
echo "=== GC_UTIL ==="
if [ "$CURRENT_USER" = "$CASSANDRA_USER" ]; then
    jstat -gcutil $CASSANDRA_PID 2>&1
else
    # Try with sudo first
    if sudo -n -u $CASSANDRA_USER jstat -gcutil $CASSANDRA_PID 2>&1; then
        true  # Success
    else
        # Sudo failed, try direct (may fail with permission error but we'll catch it)
        echo "WARNING: Cannot run jstat as $CASSANDRA_USER user. Trying direct access..."
        jstat -gcutil $CASSANDRA_PID 2>&1 || echo "JSTAT_FAILED: Operation not permitted - need to run as $CASSANDRA_USER user"
    fi
fi

# Check JVM configuration for production readiness
echo "=== JVM_CONFIG ==="
# Get full Java command line to analyze JVM options
JVM_CMDLINE=$(cat /proc/$CASSANDRA_PID/cmdline 2>/dev/null | tr '\\0' ' ')
echo "JVM_COMMAND_LINE_START"
echo "$JVM_CMDLINE"
echo "JVM_COMMAND_LINE_END"

# Check for PerfDisableSharedMem (prevents jstat)
if echo "$JVM_CMDLINE" | grep -q "PerfDisableSharedMem"; then
    echo "PERF_SHARED_MEM_DISABLED=true"
else
    echo "PERF_SHARED_MEM_DISABLED=false"
fi

# Check if GC logging is enabled (legacy or unified)
if echo "$JVM_CMDLINE" | grep -qE "(-Xlog:gc|-Xloggc:|-XX:\\+PrintGC)"; then
    echo "GC_LOGGING_ENABLED=true"
else
    echo "GC_LOGGING_ENABLED=false"
fi

# Find and parse GC log file
echo "=== GC_LOG ==="
# Common GC log locations
GC_LOG_PATHS="/var/log/cassandra/gc.log /var/log/cassandra/gc.log.0 /opt/cassandra/logs/gc.log"
GC_LOG_FILE=""

for LOG_PATH in $GC_LOG_PATHS; do
    if [ -f "$LOG_PATH" ]; then
        GC_LOG_FILE="$LOG_PATH"
        break
    fi
done

# Also try to find via Java process command line
if [ -z "$GC_LOG_FILE" ]; then
    CMDLINE_LOG=$(ps aux | grep $CASSANDRA_PID | grep -oP '(?<=-Xloggc:)[^ ]+' | head -1)
    if [ -z "$CMDLINE_LOG" ]; then
        # Try unified logging format
        CMDLINE_LOG=$(echo "$JVM_CMDLINE" | grep -oP '(?<=-Xlog:.*file=)[^:,\\s]+' | head -1)
    fi
    if [ -n "$CMDLINE_LOG" ] && [ -f "$CMDLINE_LOG" ]; then
        GC_LOG_FILE="$CMDLINE_LOG"
    fi
fi

if [ -n "$GC_LOG_FILE" ]; then
    echo "GC_LOG_FILE=$GC_LOG_FILE"
    # Get last N lines and look for Full GC events
    tail -n {gc_log_lines} "$GC_LOG_FILE" 2>/dev/null | grep -E "\\[Full GC" || echo "NO_FULL_GC_EVENTS"
else
    echo "GC_LOG_NOT_FOUND"
fi

"""
        
        results = connector.execute_ssh_on_all_hosts(
            command,
            "JVM statistics check"
        )
        
        # === STEP 3: PARSE RESULTS ===
        all_jvm_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []
        
        for result in results:
            host = result['host']
            broker_id = result['node_id']
            
            if not result['success']:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': result.get('error', 'Unknown error')
                })
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"Could not collect JVM stats on {host} (Broker {broker_id}): {result.get('error')}\n"
                    f"====\n\n"
                )
                continue
            
            # Parse output
            output = result['output'].strip()
            if not output or "ERROR" in output:
                error_msg = "Cassandra process not found" if "not found" in output else "Unknown error"
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': error_msg
                })
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"Could not collect JVM stats on {host} (Broker {broker_id}): {error_msg}\n"
                    f"====\n\n"
                )
                continue
            
            # Extract PID
            pid_match = re.search(r'CASSANDRA_PID=(\d+)', output)
            cassandra_pid = pid_match.group(1) if pid_match else 'unknown'

            # Extract sections FIRST (before permission check, so we can still get JVM config data)
            gc_stats_section = re.search(r'=== GC_STATS ===\n(.*?)\n===', output, re.DOTALL)
            gc_util_section = re.search(r'=== GC_UTIL ===\n(.*?)(?:\n===|$)', output, re.DOTALL)
            jvm_config_section = re.search(r'=== JVM_CONFIG ===\n(.*?)\n===', output, re.DOTALL)
            gc_log_section = re.search(r'=== GC_LOG ===\n(.*?)$', output, re.DOTALL)

            # Parse JVM configuration (production readiness)
            perf_shared_mem_disabled = False
            gc_logging_enabled = True  # Default assume it's enabled
            if jvm_config_section:
                jvm_config_content = jvm_config_section.group(1)
                if 'PERF_SHARED_MEM_DISABLED=true' in jvm_config_content:
                    perf_shared_mem_disabled = True
                if 'GC_LOGGING_ENABLED=false' in jvm_config_content:
                    gc_logging_enabled = False

            # Parse GC log for Full GC events (even if jstat fails)
            full_gc_events = []
            gc_log_file = None
            if gc_log_section:
                gc_log_content = gc_log_section.group(1)
                # Extract GC log file path
                gc_log_file_match = re.search(r'GC_LOG_FILE=(.+)', gc_log_content)
                if gc_log_file_match:
                    gc_log_file = gc_log_file_match.group(1).strip()
                # Parse Full GC events if found
                if 'NO_FULL_GC_EVENTS' not in gc_log_content and 'GC_LOG_NOT_FOUND' not in gc_log_content:
                    full_gc_events = parse_gc_log_events(gc_log_content)

            # Check for jstat permission error
            if 'JSTAT_FAILED' in output or 'Operation not permitted' in output:
                # Extract Cassandra user from output
                cassandra_user_match = re.search(r'CASSANDRA_USER=(\S+)', output)
                cassandra_user = cassandra_user_match.group(1) if cassandra_user_match else 'cassandra'

                # Get current SSH user from output or settings
                current_user_match = re.search(r'CURRENT_USER=(\S+)', output)
                ssh_user = current_user_match.group(1) if current_user_match else settings.get('ssh_user', 'your-ssh-user')

                # Store minimal info for production readiness checks (using already-parsed data)
                minimal_info = {
                    'host': host,
                    'broker_id': broker_id,
                    'cassandra_pid': cassandra_pid,
                    'gc_log_file': gc_log_file,
                    'full_gc_events': full_gc_events,
                    'full_gc_event_count_recent': len(full_gc_events),
                    'perf_shared_mem_disabled': perf_shared_mem_disabled,
                    'gc_logging_enabled': gc_logging_enabled,
                    'jstat_failed': True
                }
                all_jvm_data.append(minimal_info)

                error_msg = (
                    f"jstat permission denied - SSH user '{ssh_user}' cannot access Java process owned by '{cassandra_user}'. "
                    f"Grant sudo access: 'sudo visudo' and add: '{ssh_user} ALL=({cassandra_user}) NOPASSWD: /usr/bin/jstat'"
                )
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': error_msg
                })
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"**Permission Issue on {host} (Broker {broker_id})**\n\n"
                    f"Cannot run `jstat` to collect JVM statistics. The SSH user '{ssh_user}' needs permission to run jstat as the '{cassandra_user}' user.\n\n"
                    f"**Fix**: Grant sudo access by adding this line to `/etc/sudoers` (via `sudo visudo`):\n\n"
                    f"```\n"
                    f"{ssh_user} ALL=({cassandra_user}) NOPASSWD: /usr/bin/jstat\n"
                    f"```\n\n"
                    f"Or run the health check as the '{cassandra_user}' user directly.\n"
                    f"====\n\n"
                )
                continue

            # Parse jstat data
            gc_data = None
            gcutil_data = None

            if gc_stats_section:
                gc_stats_content = gc_stats_section.group(1)
                # Check if output contains error messages
                if 'MonitorException' not in gc_stats_content and 'Could not attach' not in gc_stats_content:
                    gc_data = parse_jstat_gc(gc_stats_content)

            if gc_util_section:
                gc_util_content = gc_util_section.group(1)
                # Check if output contains error messages
                if 'MonitorException' not in gc_util_content and 'Could not attach' not in gc_util_content:
                    gcutil_data = parse_jstat_gcutil(gc_util_content)

            # If jstat parsing failed, add minimal info and skip (different from permission error above)
            if not gc_data or not gcutil_data:
                # Store minimal info for production readiness checks
                minimal_info = {
                    'host': host,
                    'broker_id': broker_id,
                    'cassandra_pid': cassandra_pid,
                    'gc_log_file': gc_log_file,
                    'full_gc_events': full_gc_events,
                    'full_gc_event_count_recent': len(full_gc_events),
                    'perf_shared_mem_disabled': perf_shared_mem_disabled,
                    'gc_logging_enabled': gc_logging_enabled,
                    'jstat_failed': True
                }
                all_jvm_data.append(minimal_info)

                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': 'Could not parse jstat output'
                })
                continue

            # Calculate metrics
            heap_usage = calculate_heap_usage(gc_data)
            gc_metrics = calculate_gc_metrics(gc_data, gcutil_data)
            
            if not heap_usage or not gc_metrics:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': 'Could not calculate JVM metrics'
                })
                continue
            
            # Combine all data
            jvm_info = {
                'host': host,
                'broker_id': broker_id,
                'cassandra_pid': cassandra_pid,
                'gc_log_file': gc_log_file,
                'full_gc_events': full_gc_events,
                'full_gc_event_count_recent': len(full_gc_events),
                'perf_shared_mem_disabled': perf_shared_mem_disabled,
                'gc_logging_enabled': gc_logging_enabled,
                **heap_usage,
                **gc_metrics
            }
            all_jvm_data.append(jvm_info)
            
            # === STEP 4: INTERPRET - Check Thresholds ===
            broker_has_issues = False
            
            # Check heap usage
            if jvm_info['heap_util_percent'] >= heap_critical_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**Critical Heap Memory Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Heap Usage:** {jvm_info['heap_util_percent']:.1f}% (threshold: {heap_critical_percent}%)\n"
                    f"* **Heap Used:** {jvm_info['heap_used_kb']/1024:.1f} MB / {jvm_info['heap_capacity_kb']/1024:.1f} MB\n"
                    f"* **Old Gen:** {jvm_info['old_util_percent']:.1f}%\n"
                    f"* **Full GC Count:** {jvm_info['full_gc_count']}\n\n"
                    f"**⚠️ Risk of OutOfMemoryError! Immediate action required.**\n"
                    f"====\n\n"
                )
            
            elif jvm_info['heap_util_percent'] >= heap_warning_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in warning_brokers:
                    warning_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"**High Heap Memory Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Heap Usage:** {jvm_info['heap_util_percent']:.1f}% (threshold: {heap_warning_percent}%)\n"
                    f"* **Heap Used:** {jvm_info['heap_used_kb']/1024:.1f} MB / {jvm_info['heap_capacity_kb']/1024:.1f} MB\n"
                    f"====\n\n"
                )
            
            # Check Old Generation
            if jvm_info['old_util_percent'] >= old_gen_critical_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**Critical Old Generation Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Old Gen:** {jvm_info['old_util_percent']:.1f}% (threshold: {old_gen_critical_percent}%)\n"
                    f"* **Full GC Count:** {jvm_info['full_gc_count']}\n"
                    f"* **Avg Full GC Time:** {jvm_info['avg_full_gc_time_ms']:.1f} ms\n\n"
                    f"**Old generation is nearly full - risk of Full GC storms!**\n"
                    f"====\n\n"
                )
            
            # Check Full GC activity
            if jvm_info['full_gc_count'] >= fgc_critical_count:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**Excessive Full GC Activity**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Full GC Count:** {jvm_info['full_gc_count']} (threshold: {fgc_critical_count})\n"
                    f"* **Full GC Time:** {jvm_info['full_gc_time_sec']:.2f} seconds total\n"
                    f"* **Avg Full GC:** {jvm_info['avg_full_gc_time_ms']:.1f} ms\n\n"
                    f"**Frequent Full GCs indicate memory pressure!**\n"
                    f"====\n\n"
                )
            
            # Check Metaspace
            if jvm_info['metaspace_util_percent'] >= metaspace_critical_percent:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                adoc_content.append(
                    f"[WARNING]\n====\n"
                    f"**High Metaspace Usage**\n\n"
                    f"* **Broker:** {broker_id} ({host})\n"
                    f"* **Metaspace:** {jvm_info['metaspace_util_percent']:.1f}% (threshold: {metaspace_critical_percent}%)\n"
                    f"* **Used:** {jvm_info['metaspace_used_kb']/1024:.1f} MB / {jvm_info['metaspace_capacity_kb']/1024:.1f} MB\n"
                    f"====\n\n"
                )
        
        # === STEP 5: PRODUCTION READINESS WARNINGS ===
        nodes_with_perf_disabled = [jvm for jvm in all_jvm_data if jvm.get('perf_shared_mem_disabled')]
        nodes_without_gc_logging = [jvm for jvm in all_jvm_data if not jvm.get('gc_logging_enabled', True)]

        if nodes_with_perf_disabled or nodes_without_gc_logging:
            adoc_content.append("==== ⚠️ Production Monitoring Configuration Issues\n\n")

            if nodes_with_perf_disabled:
                adoc_content.append("[WARNING]\n====\n")
                adoc_content.append(f"**{len(nodes_with_perf_disabled)} node(s) have `-XX:+PerfDisableSharedMem` enabled**\n\n")
                adoc_content.append("This JVM option disables performance data sharing, which prevents monitoring tools like `jstat` from working.\n\n")
                adoc_content.append("**Affected nodes:**\n\n")
                for jvm in nodes_with_perf_disabled:
                    adoc_content.append(f"* {jvm['broker_id']} ({jvm['host']})\n")
                adoc_content.append("\n**Production Impact:**\n\n")
                adoc_content.append("* Cannot use `jstat` for live JVM monitoring\n")
                adoc_content.append("* Monitoring tools cannot attach to JVM process\n")
                adoc_content.append("* Troubleshooting memory issues becomes much harder\n\n")
                adoc_content.append("**Recommendation:**\n\n")
                adoc_content.append("* Remove `-XX:+PerfDisableSharedMem` from JVM options\n")
                adoc_content.append("* Or add `-XX:-PerfDisableSharedMem` to explicitly enable it\n")
                adoc_content.append("* Restart Cassandra nodes after making changes\n")
                adoc_content.append("====\n\n")

            if nodes_without_gc_logging:
                adoc_content.append("[WARNING]\n====\n")
                adoc_content.append(f"**{len(nodes_without_gc_logging)} node(s) do not have GC logging enabled**\n\n")
                adoc_content.append("GC logging is essential for troubleshooting memory and performance issues in production.\n\n")
                adoc_content.append("**Affected nodes:**\n\n")
                for jvm in nodes_without_gc_logging:
                    adoc_content.append(f"* {jvm['broker_id']} ({jvm['host']})\n")
                adoc_content.append("\n**Production Impact:**\n\n")
                adoc_content.append("* Cannot analyze Full GC events that cause latency spikes\n")
                adoc_content.append("* Difficult to diagnose memory pressure and OutOfMemory issues\n")
                adoc_content.append("* No historical GC data for performance troubleshooting\n\n")
                adoc_content.append("**Recommendation (Java 11+):**\n\n")
                adoc_content.append("```\n")
                adoc_content.append("-Xlog:gc=info,heap*=trace,age*=debug,safepoint=info,promotion*=trace:file=/var/log/cassandra/gc.log:time,uptime,pid,tid,level:filecount=10,filesize=10M\n")
                adoc_content.append("```\n\n")
                adoc_content.append("**Recommendation (Java 8):**\n\n")
                adoc_content.append("```\n")
                adoc_content.append("-Xloggc:/var/log/cassandra/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M\n")
                adoc_content.append("```\n")
                adoc_content.append("====\n\n")

        # === STEP 6: SUMMARY TABLES ===
        # Filter out entries where jstat failed (they only have config data, not metrics)
        jvm_data_with_metrics = [jvm for jvm in all_jvm_data if not jvm.get('jstat_failed', False)]

        if jvm_data_with_metrics:
            adoc_content.append("==== Heap Memory Summary\n\n")
            adoc_content.append("|===\n")
            adoc_content.append("|Broker|Host|Heap Used (MB)|Heap Max (MB)|Heap %|Old Gen %|Metaspace %\n")

            for jvm in sorted(jvm_data_with_metrics, key=lambda x: x['heap_util_percent'], reverse=True):
                heap_indicator = ""
                if jvm['heap_util_percent'] >= heap_critical_percent:
                    heap_indicator = "🔴 "
                elif jvm['heap_util_percent'] >= heap_warning_percent:
                    heap_indicator = "⚠️ "
                
                old_indicator = ""
                if jvm['old_util_percent'] >= old_gen_critical_percent:
                    old_indicator = "🔴 "
                elif jvm['old_util_percent'] >= old_gen_warning_percent:
                    old_indicator = "⚠️ "
                
                adoc_content.append(
                    f"|{jvm['broker_id']}|{jvm['host']}|"
                    f"{jvm['heap_used_kb']/1024:.1f}|{jvm['heap_capacity_kb']/1024:.1f}|"
                    f"{heap_indicator}{jvm['heap_util_percent']:.1f}|"
                    f"{old_indicator}{jvm['old_util_percent']:.1f}|"
                    f"{jvm['metaspace_util_percent']:.1f}\n"
                )
            adoc_content.append("|===\n\n")
            
            adoc_content.append("==== Garbage Collection Summary\n\n")
            adoc_content.append("|===\n")
            adoc_content.append("|Broker|Host|Young GC Count|Avg Young GC (ms)|Full GC Count|Avg Full GC (ms)|Total GC Time (s)\n")

            for jvm in sorted(jvm_data_with_metrics, key=lambda x: x['full_gc_count'], reverse=True):
                fgc_indicator = ""
                if jvm['full_gc_count'] >= fgc_critical_count:
                    fgc_indicator = "🔴 "
                elif jvm['full_gc_count'] >= fgc_warning_count:
                    fgc_indicator = "⚠️ "
                
                adoc_content.append(
                    f"|{jvm['broker_id']}|{jvm['host']}|"
                    f"{jvm['young_gc_count']}|{jvm['avg_young_gc_time_ms']:.1f}|"
                    f"{fgc_indicator}{jvm['full_gc_count']}|{jvm['avg_full_gc_time_ms']:.1f}|"
                    f"{jvm['total_gc_time_sec']:.2f}\n"
                )
            adoc_content.append("|===\n\n")

            # === FULL GC EVENTS ANALYSIS ===
            # Collect all Full GC events across brokers
            all_full_gc_events = []
            brokers_with_full_gc = []

            for jvm in all_jvm_data:
                if jvm.get('full_gc_events'):
                    for event in jvm['full_gc_events']:
                        event['broker_id'] = jvm['broker_id']
                        event['host'] = jvm['host']
                        all_full_gc_events.append(event)
                    if jvm['broker_id'] not in brokers_with_full_gc:
                        brokers_with_full_gc.append(jvm['broker_id'])

            if all_full_gc_events:
                adoc_content.append("==== Full GC Events (From Log Files)\n\n")

                # Count by cause
                cause_counts = {}
                for event in all_full_gc_events:
                    cause = event.get('cause', 'Unknown')
                    cause_counts[cause] = cause_counts.get(cause, 0) + 1

                # Show critical warning if Full GCs found
                total_full_gc_events = len(all_full_gc_events)
                adoc_content.append(f"[IMPORTANT]\n====\n")
                adoc_content.append(f"**⚠️ Detected {total_full_gc_events} Full GC event(s) in recent log history (last {gc_log_lines} lines)**\n\n")
                adoc_content.append(f"Full GCs pause the entire application and can cause timeouts and latency spikes.\n\n")
                adoc_content.append(f"**Affected Brokers:** {', '.join(brokers_with_full_gc)}\n\n")

                adoc_content.append(f"**Full GC Causes:**\n\n")
                for cause, count in sorted(cause_counts.items(), key=lambda x: x[1], reverse=True):
                    adoc_content.append(f"* **{cause}**: {count} event(s)\n")
                adoc_content.append(f"====\n\n")

                # Show detailed events table (limited to recent events)
                recent_events = sorted(all_full_gc_events, key=lambda x: x.get('timestamp', ''), reverse=True)[:20]

                adoc_content.append(f"**Recent Full GC Events (Last 20):**\n\n")
                adoc_content.append("|===\n")
                adoc_content.append("|Broker|Timestamp|Cause|Pause Time (ms)|Heap Before→After|Reclaimed\n")

                for event in recent_events:
                    broker = event.get('broker_id', 'N/A')
                    timestamp = event.get('timestamp', 'Unknown')
                    cause = event.get('cause', 'Unknown')
                    pause_ms = event.get('pause_time_ms', 0)
                    heap_before_mb = event.get('heap_before_kb', 0) / 1024
                    heap_after_mb = event.get('heap_after_kb', 0) / 1024
                    reclaimed_pct = event.get('heap_reclaimed_pct', 0)

                    # Color code by pause time
                    pause_indicator = ""
                    if pause_ms > 1000:  # > 1 second
                        pause_indicator = "🔴 "
                    elif pause_ms > 500:  # > 500ms
                        pause_indicator = "⚠️ "

                    adoc_content.append(
                        f"|{broker}|{timestamp}|{cause}|"
                        f"{pause_indicator}{pause_ms:.0f}|"
                        f"{heap_before_mb:.0f}→{heap_after_mb:.0f} MB|"
                        f"{reclaimed_pct:.1f}%\n"
                    )

                adoc_content.append("|===\n\n")

                # Add explanations for common causes
                adoc_content.append("[TIP]\n====\n")
                adoc_content.append("**Common Full GC Causes:**\n\n")
                adoc_content.append("* **Allocation Failure**: Heap is full and cannot allocate new objects - *most critical, indicates insufficient heap*\n")
                adoc_content.append("* **Metadata GC Threshold**: Metaspace (class metadata) is full - review metaspace sizing\n")
                adoc_content.append("* **System.gc()**: Explicit GC called by code - usually unnecessary, disable with -XX:+DisableExplicitGC\n")
                adoc_content.append("* **Ergonomics**: JVM adaptive sizing triggered GC - review GC tuning parameters\n")
                adoc_content.append("* **Heap Inspection**: jmap/jcmd heap dump triggered GC - expected for diagnostics\n\n")
                adoc_content.append("**⚠️ If you see frequent 'Allocation Failure' Full GCs, increase heap size immediately!**\n")
                adoc_content.append("====\n\n")

            elif all_jvm_data:
                # No Full GC events found
                adoc_content.append("==== Full GC Events (From Log Files)\n\n")
                adoc_content.append("[NOTE]\n====\n")
                adoc_content.append(f"✅ No Full GC events detected in recent log history (last {gc_log_lines} lines).\n\n")
                adoc_content.append(f"This is excellent - Full GCs can cause application pauses and should be avoided.\n")
                adoc_content.append("====\n\n")

        # === STEP 6: ERROR SUMMARY ===
        if errors:
            adoc_content.append("==== Collection Errors\n\n")
            adoc_content.append("[WARNING]\n====\n")
            adoc_content.append(f"Could not collect JVM stats from {len(errors)} broker(s):\n\n")
            for error in errors:
                adoc_content.append(f"* Broker {error['broker_id']} ({error['host']}): {error['error']}\n")
            adoc_content.append("====\n\n")
        
        # === STEP 7: RECOMMENDATIONS ===
        if issues_found:
            adoc_content.append("==== Recommendations\n\n")
            adoc_content.append("[TIP]\n====\n")
            
            if critical_brokers:
                adoc_content.append("**🔴 Critical Priority (Immediate Action):**\n\n")
                adoc_content.append("* **Increase heap size:** Add -Xmx and -Xms JVM options (recommend 6-8GB for Cassandra)\n")
                adoc_content.append("* **Tune GC settings:** Consider G1GC with appropriate pause time goals\n")
                adoc_content.append("* **Review memory leaks:** Check for growing old generation over time\n")
                adoc_content.append("* **Monitor Full GCs:** Frequent Full GCs indicate insufficient heap\n")
                adoc_content.append("* **Check broker workload:** High message rates may require more memory\n\n")
            
            if warning_brokers:
                adoc_content.append("**⚠️ High Priority (Plan Optimization):**\n\n")
                adoc_content.append("* **Monitor trends:** Track heap usage over time for growth patterns\n")
                adoc_content.append("* **Review broker configuration:** Optimize buffer sizes and batch configs\n")
                adoc_content.append("* **Plan heap increase:** Add memory before reaching critical levels\n")
                adoc_content.append("* **GC tuning:** Consider switching to G1GC if using CMS/Parallel\n\n")
            
            adoc_content.append("**📋 General Best Practices:**\n\n")
            adoc_content.append("* Set heap size: 6GB minimum, 8-12GB for production (but < 32GB to use compressed oops)\n")
            adoc_content.append("* Use G1GC: `-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35`\n")
            adoc_content.append("* Set Xms = Xmx: Prevent heap resizing overhead\n")
            adoc_content.append("* Monitor GC logs: Enable GC logging for troubleshooting\n")
            adoc_content.append("* Consider off-heap caching: Reduce heap pressure for page cache\n")
            adoc_content.append("====\n")
        elif jvm_data_with_metrics:
            # Only say "healthy" if we actually collected metrics
            adoc_content.append("[NOTE]\n====\n")
            adoc_content.append("✅ JVM memory and GC performance is healthy across all brokers.\n\n")
            adoc_content.append(f"All brokers show heap usage below {heap_warning_percent}% with acceptable GC activity.\n")
            adoc_content.append("====\n")
        else:
            # No metrics collected at all
            adoc_content.append("[NOTE]\n====\n")
            adoc_content.append("ℹ️ Unable to assess JVM health - no metrics collected.\n\n")
            adoc_content.append("This is typically due to permission issues or missing monitoring tools. ")
            adoc_content.append("See the error messages above for details.\n")
            adoc_content.append("====\n")
        
        # === STEP 8: STRUCTURED DATA ===
        structured_data["jvm_stats"] = {
            "status": "success",
            "brokers_checked": len([r for r in results if r['success']]),
            "brokers_with_errors": len(errors),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "heap_warning_percent": heap_warning_percent,
                "heap_critical_percent": heap_critical_percent,
                "old_gen_warning_percent": old_gen_warning_percent,
                "old_gen_critical_percent": old_gen_critical_percent,
                "fgc_warning_count": fgc_warning_count,
                "fgc_critical_count": fgc_critical_count
            },
            "errors": errors,
            "data": all_jvm_data
        }
        
        # Collect Full GC event stats for rules
        all_full_gc_events_for_rules = []
        for jvm in all_jvm_data:
            if jvm.get('full_gc_events'):
                all_full_gc_events_for_rules.extend(jvm['full_gc_events'])

        full_gc_cause_counts = {}
        allocation_failure_count = 0
        for event in all_full_gc_events_for_rules:
            cause = event.get('cause', 'Unknown')
            full_gc_cause_counts[cause] = full_gc_cause_counts.get(cause, 0) + 1
            if cause == 'Allocation Failure':
                allocation_failure_count += 1

        # Summary for aggregate rules
        structured_data["jvm_summary"] = {
            "status": "success",
            "data": [{
                "total_brokers_checked": len([r for r in results if r['success']]),
                "critical_broker_count": len(critical_brokers),
                "warning_broker_count": len(warning_brokers),
                "max_heap_util": max([jvm['heap_util_percent'] for jvm in jvm_data_with_metrics], default=0),
                "max_old_gen_util": max([jvm['old_util_percent'] for jvm in jvm_data_with_metrics], default=0),
                "max_fgc_count": max([jvm['full_gc_count'] for jvm in jvm_data_with_metrics], default=0),
                "total_fgc_count": sum([jvm['full_gc_count'] for jvm in jvm_data_with_metrics]),
                "total_full_gc_events_from_logs": len(all_full_gc_events_for_rules),
                "full_gc_allocation_failures": allocation_failure_count,
                "full_gc_causes": full_gc_cause_counts
            }]
        }

        # Add detailed Full GC events for rules
        if all_full_gc_events_for_rules:
            structured_data["full_gc_events"] = {
                "status": "success",
                "data": all_full_gc_events_for_rules
            }
        
    except Exception as e:
        import traceback
        logger.error(f"JVM stats check failed: {e}\n{traceback.format_exc()}")
        error_msg = f"[ERROR]\n====\nJVM statistics check failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["jvm_stats"] = {
            "status": "error",
            "details": str(e)
        }

    return "\n".join(adoc_content), structured_data

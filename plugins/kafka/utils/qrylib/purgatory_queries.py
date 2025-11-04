"""Query functions for purgatory operations."""
import json


def get_purgatory_query(connector):
    """
    Returns query for checking Kafka purgatory sizes via SSH + JMX on all brokers.
    
    Collects both Fetch and Produce purgatory metrics using JMX. Requires:
    - JMX enabled on brokers (default port 9999)
    - kafka-run-class available in PATH or configured via kafka_run_class_path
    
    The command uses timeout to limit JmxTool execution (which runs indefinitely)
    and collects multiple samples over 3 seconds to ensure data is available.
    
    Optional sudo support: Set kafka_jmx_use_sudo=true if needed for file permissions
    or organizational policy (typically not required since JMX is network-based).
    """
    # Get configuration from connector settings
    settings = getattr(connector, 'settings', {})
    jmx_port = settings.get('kafka_jmx_port', 9999)
    kafka_run_class = settings.get('kafka_run_class_path', 'kafka-run-class')
    
    # Optional sudo support (typically not needed for JMX)
    use_sudo = settings.get('kafka_jmx_use_sudo', False)
    sudo_user = settings.get('kafka_jmx_sudo_user', 'kafka')
    
    # Build command prefix with or without sudo
    if use_sudo:
        cmd_prefix = f"sudo -u {sudo_user} {kafka_run_class}"
    else:
        cmd_prefix = kafka_run_class
    
    # Build the command to collect both purgatory metrics
    command = f"""
# Collect Fetch Purgatory Size
echo "=== FETCH_PURGATORY ==="
timeout 3 {cmd_prefix} kafka.tools.JmxTool \\
  --object-name "kafka.server:type=DelayedFetchMetrics,name=DelayedFetchRequestsPurgatory,fetcherType=consumer" \\
  --attributes Value \\
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:{jmx_port}/jmxrmi \\
  --reporting-interval 1000 \\
  2>/dev/null

# Collect Produce Purgatory Size
echo "=== PRODUCE_PURGATORY ==="
timeout 3 {cmd_prefix} kafka.tools.JmxTool \\
  --object-name "kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Produce" \\
  --attributes Value \\
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:{jmx_port}/jmxrmi \\
  --reporting-interval 1000 \\
  2>/dev/null
""".strip()
    
    return json.dumps({
        "operation": "purgatory_size",
        "command": command
    })

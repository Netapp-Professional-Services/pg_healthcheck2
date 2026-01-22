"""System limits queries for Kafka brokers (shell commands)."""

__all__ = [
    'get_kafka_process_limits_query',
    'get_swappiness_query'
]

import json


def get_kafka_process_limits_query(connector):
    """
    Returns JSON request to read system limits from /proc/<pid>/limits.
    
    Finds the Kafka process and reads its limits file.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "KAFKA_PID=$(pgrep -f 'kafka\\.Kafka' | head -1); if [ -n \"$KAFKA_PID\" ]; then cat /proc/$KAFKA_PID/limits 2>/dev/null || echo 'ERROR: Could not read limits'; else echo 'ERROR: Kafka process not found'; fi"
    })


def get_swappiness_query(connector):
    """
    Returns JSON request to get vm.swappiness value.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "sysctl -n vm.swappiness 2>/dev/null || echo 'ERROR: Could not read swappiness'"
    })

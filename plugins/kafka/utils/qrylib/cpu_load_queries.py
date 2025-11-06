"""CPU load queries for Kafka brokers (shell commands)."""

__all__ = [
    'get_cpu_load_query',
    'get_proc_stat_query'
]

import json


def get_cpu_load_query(connector):
    """
    Returns JSON request for CPU load average via 'uptime' command.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "uptime"
    })


def get_proc_stat_query(connector):
    """
    Returns JSON request for CPU statistics via '/proc/stat'.

    Useful for calculating CPU utilization percentage.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "cat /proc/stat | head -n 1"
    })

"""Memory usage queries for Kafka brokers (shell commands)."""

__all__ = [
    'get_memory_usage_query',
    'get_proc_meminfo_query'
]

import json


def get_memory_usage_query(connector):
    """
    Returns JSON request for memory usage via 'free -m' command.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "free -m"
    })


def get_proc_meminfo_query(connector):
    """
    Returns JSON request for detailed memory info via '/proc/meminfo'.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "cat /proc/meminfo"
    })

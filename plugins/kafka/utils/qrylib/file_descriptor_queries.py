"""File descriptor queries for Kafka brokers (shell commands)."""

__all__ = [
    'get_file_descriptor_limit_query',
    'get_file_descriptor_usage_query',
    'get_kafka_process_fd_query'
]

import json


def get_file_descriptor_limit_query(connector):
    """
    Returns JSON request for file descriptor limit via 'ulimit -n'.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "ulimit -n"
    })


def get_file_descriptor_usage_query(connector):
    """
    Returns JSON request for current file descriptor usage.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "lsof -n | wc -l"
    })


def get_kafka_process_fd_query(connector):
    """
    Returns JSON request for Kafka broker process file descriptor count.

    Finds the Kafka process and counts its open file descriptors.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "lsof -p $(pgrep -f 'kafka\\.Kafka') 2>/dev/null | wc -l || echo '0'"
    })

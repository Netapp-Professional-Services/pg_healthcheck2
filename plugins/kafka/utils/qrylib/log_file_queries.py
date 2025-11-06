"""Broker log file queries for Kafka (shell commands)."""

__all__ = [
    'get_server_log_query',
    'get_controller_log_query',
    'get_state_change_log_query',
    'get_gc_log_query',
    'get_log_dir_query'
]

import json


def get_server_log_query(connector, num_lines=1000):
    """
    Returns JSON request for reading recent lines from server.log.

    Args:
        connector: Kafka connector instance
        num_lines: Number of recent lines to read (default: 1000)

    Returns:
        str: JSON string with operation and command
    """
    # Try common log file locations
    command = (
        f"for path in "
        "/var/log/kafka/server.log "
        "/opt/kafka/logs/server.log "
        "/usr/local/kafka/logs/server.log "
        "$(find /var/log -name 'server.log' 2>/dev/null | grep kafka | head -1) "
        "$(find /opt -name 'server.log' 2>/dev/null | grep kafka | head -1); "
        f"do [ -f \"$path\" ] && tail -n {num_lines} \"$path\" && exit 0; done; "
        "echo 'ERROR: server.log not found'"
    )

    return json.dumps({
        "operation": "shell",
        "command": command
    })


def get_controller_log_query(connector, num_lines=1000):
    """
    Returns JSON request for reading recent lines from controller.log.

    Args:
        connector: Kafka connector instance
        num_lines: Number of recent lines to read (default: 1000)

    Returns:
        str: JSON string with operation and command
    """
    command = (
        f"for path in "
        "/var/log/kafka/controller.log "
        "/opt/kafka/logs/controller.log "
        "/usr/local/kafka/logs/controller.log "
        "$(find /var/log -name 'controller.log' 2>/dev/null | grep kafka | head -1) "
        "$(find /opt -name 'controller.log' 2>/dev/null | grep kafka | head -1); "
        f"do [ -f \"$path\" ] && tail -n {num_lines} \"$path\" && exit 0; done; "
        "echo 'controller.log not found'"
    )

    return json.dumps({
        "operation": "shell",
        "command": command
    })


def get_state_change_log_query(connector, num_lines=1000):
    """
    Returns JSON request for reading recent lines from state-change.log.

    Args:
        connector: Kafka connector instance
        num_lines: Number of recent lines to read (default: 1000)

    Returns:
        str: JSON string with operation and command
    """
    command = (
        f"for path in "
        "/var/log/kafka/state-change.log "
        "/opt/kafka/logs/state-change.log "
        "/usr/local/kafka/logs/state-change.log "
        "$(find /var/log -name 'state-change.log' 2>/dev/null | grep kafka | head -1) "
        "$(find /opt -name 'state-change.log' 2>/dev/null | grep kafka | head -1); "
        f"do [ -f \"$path\" ] && tail -n {num_lines} \"$path\" && exit 0; done; "
        "echo 'state-change.log not found'"
    )

    return json.dumps({
        "operation": "shell",
        "command": command
    })


def get_gc_log_query(connector, num_lines=500):
    """
    Returns JSON request for reading recent lines from GC log.

    Args:
        connector: Kafka connector instance
        num_lines: Number of recent lines to read (default: 500)

    Returns:
        str: JSON string with operation and command
    """
    # Find GC log by looking for common patterns
    command = (
        f"for path in "
        "/var/log/kafka/kafkaServer-gc.log "
        "/opt/kafka/logs/kafkaServer-gc.log "
        "/var/log/kafka/*gc*.log "
        "$(find /var/log -name '*gc*.log' 2>/dev/null | grep kafka | head -1) "
        "$(find /opt -name '*gc*.log' 2>/dev/null | grep kafka | head -1); "
        f"do [ -f \"$path\" ] && tail -n {num_lines} \"$path\" && exit 0; done; "
        "echo 'GC log not found'"
    )

    return json.dumps({
        "operation": "shell",
        "command": command
    })


def get_log_dir_query(connector):
    """
    Returns JSON request for finding Kafka log directory.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    command = (
        "for path in "
        "/var/log/kafka "
        "/opt/kafka/logs "
        "/usr/local/kafka/logs; "
        "do [ -d \"$path\" ] && echo \"$path\" && ls -lh \"$path\" && exit 0; done; "
        "echo 'Kafka log directory not found'"
    )

    return json.dumps({
        "operation": "shell",
        "command": command
    })

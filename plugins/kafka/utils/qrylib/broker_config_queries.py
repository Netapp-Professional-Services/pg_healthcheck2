"""Broker configuration queries for Kafka."""

__all__ = [
    'get_broker_config_query',
    'get_server_properties_query',
    'get_kafka_env_query'
]

import json
import shlex


def get_broker_config_query(connector, broker_id: int):
    """Returns query for broker configuration via Admin API."""
    return json.dumps({
        "operation": "broker_config",
        "broker_id": broker_id
    })


def get_server_properties_query(connector):
    """
    Returns JSON request for reading server.properties via SSH using the path
    from config (kafka_config_file_path). This path is mandatory when SSH is used.

    Args:
        connector: Kafka connector instance (must have settings['kafka_config_file_path'])

    Returns:
        str: JSON string with operation and command
    """
    path = connector.settings.get('kafka_config_file_path')
    if not path:
        command = "echo 'ERROR: kafka_config_file_path not set in config'"
    else:
        # Safe for shell: path is from config
        path_quoted = shlex.quote(path)
        command = f"if [ -f {path_quoted} ]; then cat {path_quoted}; else echo 'ERROR: server.properties not found at {path}'; fi"

    return json.dumps({
        "operation": "shell",
        "command": command
    })


def get_kafka_env_query(connector):
    """
    Returns JSON request for reading Kafka JVM environment settings via SSH.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    # Get JVM settings from running Kafka process
    command = (
        "ps aux | grep -i 'kafka.Kafka' | grep -v grep | head -1"
    )

    return json.dumps({
        "operation": "shell",
        "command": command
    })

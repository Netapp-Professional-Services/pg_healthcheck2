"""Broker configuration queries for Kafka."""

__all__ = [
    'get_broker_config_query',
    'get_server_properties_query',
    'get_kafka_env_query'
]

import json


def get_broker_config_query(connector, broker_id: int):
    """Returns query for broker configuration via Admin API."""
    return json.dumps({
        "operation": "broker_config",
        "broker_id": broker_id
    })


def get_server_properties_query(connector):
    """
    Returns JSON request for reading server.properties configuration via SSH.

    Tries common Kafka installation paths.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON string with operation and command
    """
    # Try common paths for server.properties
    command = (
        "for path in "
        "/etc/kafka/server.properties "
        "/opt/kafka/config/server.properties "
        "/usr/local/kafka/config/server.properties "
        "$(find /opt -name 'server.properties' 2>/dev/null | grep kafka | head -1); "
        "do [ -f \"$path\" ] && cat \"$path\" && exit 0; done; "
        "echo 'ERROR: server.properties not found'"
    )

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
        "ps aux | grep -i 'kafka\\.Kafka' | grep -v grep | head -1"
    )

    return json.dumps({
        "operation": "shell",
        "command": command
    })

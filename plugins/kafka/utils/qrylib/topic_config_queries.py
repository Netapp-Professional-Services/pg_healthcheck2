"""
Query library for Kafka topic configuration operations.

Provides JSON-encoded queries for retrieving and analyzing topic configurations
via the Kafka Admin API.
"""

import json


def get_topic_config_query(connector, topic_name: str):
    """
    Returns JSON query for getting config of a specific topic.

    Args:
        connector: Kafka connector instance
        topic_name (str): Name of the topic to query

    Returns:
        str: JSON-encoded query for topic configuration
    """
    return json.dumps({
        "operation": "topic_config",
        "topic": topic_name
    })


def get_list_topics_query(connector):
    """
    Returns JSON request for listing all topics in the cluster.

    Uses the Kafka Admin API to retrieve all topic names.

    Args:
        connector: Kafka connector instance

    Returns:
        str: JSON-encoded query for topic listing
    """
    return json.dumps({
        "operation": "admin_list_topics"
    })


def get_topic_metadata_query(connector, topic_name):
    """
    Returns JSON request for retrieving topic partition information.

    Gets partition metadata including:
    - Partition IDs
    - Leader broker for each partition
    - Replica assignments
    - ISR (In-Sync Replicas)

    Args:
        connector: Kafka connector instance
        topic_name (str): Name of the topic to query

    Returns:
        str: JSON-encoded query for topic partition metadata
    """
    return json.dumps({
        "operation": "describe_topics",
        "topics": [topic_name]
    })

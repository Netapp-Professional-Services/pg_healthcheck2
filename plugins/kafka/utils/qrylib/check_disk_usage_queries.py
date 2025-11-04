"""Query functions for disk usage operations."""

import json


def get_disk_usage_query(connector):
    """
    Returns query for checking disk usage via SSH on all brokers.
    
    This query triggers SSH execution on all configured hosts to check
    disk space for Kafka data directories.
    """
    return json.dumps({
        "operation": "disk_usage",
        "command": "df -h | grep -E '(/data|/var/lib/kafka|/kafka|/opt/kafka)'"
    })

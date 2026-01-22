"""Tombstone metrics queries for Cassandra (nodetool)."""

__all__ = [
    'get_tombstone_metrics_query'
]

import json


def get_tombstone_metrics_query(connector):
    """
    Returns JSON request for tombstone metrics using version-appropriate nodetool command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    if hasattr(connector, 'version_info'):
        major = connector.version_info.get('major_version', 3)
        if major >= 4:
            return json.dumps({
                "operation": "nodetool",
                "command": "tablestats"
            })
        else:
            return json.dumps({
                "operation": "nodetool",
                "command": "cfstats"
            })
    # Default to 3.x
    return json.dumps({
        "operation": "nodetool",
        "command": "cfstats"
    })

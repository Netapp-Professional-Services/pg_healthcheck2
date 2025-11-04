"""Temporary files queries for Cassandra (shell commands)."""

__all__ = [
    'get_check_temporary_files_query'
]

import json


def get_check_temporary_files_query(connector, data_dir='/var/lib/cassandra/data'):
    """
    Returns JSON request for finding temporary files in Cassandra data directory.
    
    Args:
        connector: Cassandra connector instance
        data_dir: Path to Cassandra data directory (default: /var/lib/cassandra/data)
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": f"find {data_dir} -name '*tmp*' -type f"
    })
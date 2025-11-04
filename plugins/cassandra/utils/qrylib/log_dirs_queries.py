import json

def get_describe_log_dirs_query(connector):
    """
    Returns a nodetool command to get Cassandra data directories.

    For Cassandra, we use 'nodetool info' which shows data file locations.
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "info"
    })

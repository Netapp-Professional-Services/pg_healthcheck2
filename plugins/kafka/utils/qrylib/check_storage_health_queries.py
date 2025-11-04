import json

def get_describe_log_dirs_query(connector):
    """Returns a JSON query to describe log directories for all brokers."""
    return json.dumps({
        "operation": "describe_log_dirs",
        "broker_ids": []
    })

import json

def get_broker_config_query(connector, broker_id: int):
    """Returns query for broker configuration."""
    return json.dumps({
        "operation": "broker_config",
        "broker_id": broker_id
    })

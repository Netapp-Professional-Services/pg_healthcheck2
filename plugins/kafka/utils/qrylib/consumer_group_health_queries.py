import json

def get_describe_consumer_groups_query(connector):
    """Returns query for describing all consumer groups."""
    return json.dumps({
        "operation": "describe_consumer_groups",
        "group_ids": []
    })

def get_all_consumer_lag_query(connector):
    """Returns query for consumer lag across all groups."""
    return json.dumps({
        "operation": "consumer_lag",
        "group_id": "*"
    })
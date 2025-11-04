import json

def get_describe_consumer_groups_query(connector):
    """Returns query for describing all consumer groups."""
    return json.dumps({
        "operation": "describe_consumer_groups",
        "group_ids": []
    })

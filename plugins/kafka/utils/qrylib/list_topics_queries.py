import json

def get_list_topics_query(connector):
    """Returns query for listing all topics."""
    return json.dumps({
        "operation": "list_topics"
    })

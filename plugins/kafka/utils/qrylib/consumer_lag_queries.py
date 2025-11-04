import json

def get_all_consumer_lag_query(connector):
    return json.dumps({
        "operation": "consumer_lag",
        "group_id": "*"
    })

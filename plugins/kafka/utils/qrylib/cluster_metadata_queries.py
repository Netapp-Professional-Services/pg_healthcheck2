import json

def get_cluster_metadata_query(connector):
    """Returns query for cluster metadata."""
    return json.dumps({
        "operation": "cluster_metadata"
    })

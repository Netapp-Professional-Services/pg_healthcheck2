import json

def get_cluster_metadata_query(connector):
    return json.dumps({"operation": "cluster_metadata"})

def get_partition_distribution_query(connector):
    return json.dumps({"operation": "describe_log_dirs", "broker_ids": []})

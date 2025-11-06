from collections import Counter
from plugins.kafka.utils.qrylib.partition_balance_queries import get_cluster_metadata_query, get_partition_distribution_query

def get_weight():
    return 7

def run_partition_balance(connector, settings):
    adoc_content = ["=== Partition Balance Across Brokers", ""]
    structured_data = {}
    broker_data = []
    try:
        cluster_query = get_cluster_metadata_query(connector)
        cluster_formatted, cluster_raw = connector.execute_query(cluster_query, return_raw=True)
        if "[ERROR]" in cluster_formatted:
            adoc_content.append(cluster_formatted)
            structured_data["partition_balance"] = {"status": "error", "data": []}
            return "\n".join(adoc_content), structured_data
        brokers = cluster_raw.get('brokers', [])
        num_brokers = len(brokers)
        if num_brokers == 0:
            adoc_content.append("[NOTE]\n====\nNo brokers found in the cluster.\n====\n")
            structured_data["partition_balance"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data

        logs_query = get_partition_distribution_query(connector)
        logs_formatted, logs_raw = connector.execute_query(logs_query, return_raw=True)
        if "[ERROR]" in logs_formatted:
            adoc_content.append(logs_formatted)
            structured_data["partition_balance"] = {"status": "error", "data": []}
            return "\n".join(adoc_content), structured_data

        # Check if logs_raw is an error dict instead of a list
        if isinstance(logs_raw, dict) and 'error' in logs_raw:
            error_msg = f"[ERROR]\n====\nFailed to get partition distribution: {logs_raw['error']}\n====\n"
            adoc_content.append(error_msg)
            structured_data["partition_balance"] = {"status": "error", "data": []}
            return "\n".join(adoc_content), structured_data

        broker_replica_counts = Counter(item.get('broker_id') for item in logs_raw)
        total_replicas = sum(broker_replica_counts.values())
        average = total_replicas / num_brokers if num_brokers > 0 else 0

        for broker in brokers:
            bid = broker.get('id')
            count = broker_replica_counts.get(bid, 0)
            deviation = (abs(count - average) / average * 100) if average > 0 else 0
            broker_data.append({"broker_id": bid, "replica_count": count, "deviation": deviation})

        max_dev = max([d['deviation'] for d in broker_data] + [0])
        threshold = settings.get('imbalance_threshold_percent', 10)

        if max_dev > threshold:
            adoc_content.append(f"[WARNING]\n====\n**Imbalance Detected:** Maximum deviation {max_dev:.1f}% exceeds threshold {threshold}%. This may lead to uneven load and hotspots.\n====\n")
            adoc_content.append("\n==== Recommendations")
            adoc_content.append("[TIP]\n====\n* **Best Practice:** Ensure even distribution of partition replicas for optimal performance.\n* **Remediation:** Use the kafka-reassign-partitions tool to rebalance partitions across brokers.\n* **Monitoring:** Regularly check partition distribution after cluster changes.\n====\n")
        else:
            adoc_content.append("[NOTE]\n====\nPartition replicas are balanced across brokers.\n====\n")

        adoc_content.append("==== Broker Replica Distribution")
        adoc_content.append("| Broker ID | Host | Replica Count | Deviation (%)")
        adoc_content.append("|===")
        sorted_data = sorted(broker_data, key=lambda x: x['broker_id'])
        for item in sorted_data:
            host = next((b['host'] for b in brokers if b['id'] == item['broker_id']), 'unknown')
            adoc_content.append(f"| {item['broker_id']} | {host} | {item['replica_count']} | {item['deviation']:.2f}")
        adoc_content.append("|===")

        structured_data["partition_balance"] = {"status": "success", "data": broker_data}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["partition_balance"] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data
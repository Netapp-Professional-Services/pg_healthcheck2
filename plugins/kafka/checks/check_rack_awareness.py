from plugins.kafka.utils.qrylib.cluster_metadata_queries import get_cluster_metadata_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8


def run_rack_awareness_check(connector, settings):
    """
    Performs the health check analysis for rack-awareness.

    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings

    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = [
        "=== Rack-awareness",
        ""
    ]
    structured_data = {}

    try:
        query = get_cluster_metadata_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            # Query execution failed
            adoc_content.append(formatted)
            structured_data["rack_awareness"] = {"status": "error", "data": raw}
        else:
            brokers = raw.get('brokers', [])
            node_count_per_rack = {}

            if brokers:
                for broker in brokers:
                    rack = broker.get('rack', 'N/A')
                    if rack in node_count_per_rack:
                        node_count_per_rack[rack] += 1
                    else:
                        node_count_per_rack[rack] = 1

            if node_count_per_rack:
                distinct_number_of_racks = len(node_count_per_rack)
                if distinct_number_of_racks==1 or node_count_per_rack.__contains__(None):
                    adoc_content.append(f"[WARNING]\n====\nRack-awareness is not properly for the cluster.\n"
                                        "====\n")

                    adoc_content.append("\n==== Recommendations")
                    adoc_content.append("[TIP]\n====\n"
                                        "**Configuration:** Enable rack awareness in the Kafka cluster by setting `broker.rack` property in the server.properties for each broker. \n"
                                        "Select Kafka racks by referring the physical racks where Kafka broker VMs are deployed. \n"
                                        "====\n")
                    adoc_content.append("[INFO]\n====\n"
                                        "The existing topic partition replicas will not be reassigned automatically after enabling rack awareness," 
                                        "only newly created topics will be considered for rack-aware replica assignment. "
                                        "You can use the `kafka-reassign-partitions.sh` to reassign the existing partition replica assignment to match the racks. "
                                        "\n====\n")
                elif distinct_number_of_racks>1:
                    adoc_content.append(f"[NOTE]\n====\nBrokers in the Kafka cluster are configured on {distinct_number_of_racks} distinct racks.\n\n")
                    adoc_content.append( "====\n")
                    structured_data["rack_awareness"] = {
                        "status": "success",
                        "data": raw,
                        "racks_used": distinct_number_of_racks
                    }

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["rack_awareness"] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data
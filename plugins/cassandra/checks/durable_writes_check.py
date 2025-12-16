from plugins.cassandra.utils.qrylib.qry_durable_writes import get_durable_writes_query
from plugins.cassandra.utils.keyspace_filter import filter_user_keyspaces
from plugins.common.check_helpers import format_check_header, safe_execute_query, format_recommendations, format_data_as_table

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - data loss risk

def run_durable_writes_check(connector, settings):
    """
    Analyzes durable_writes setting for user keyspaces.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Durable Writes Analysis",
        "Checking durable_writes setting for all user keyspaces."
    )
    structured_data = {}
    
    query = get_durable_writes_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Durable writes query")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["durable_writes"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Filter out system keyspaces using centralized filter
    user_keyspaces = filter_user_keyspaces(raw, settings)
    
    if not user_keyspaces:
        adoc_content.append("[NOTE]\n====\nNo user keyspaces found.\n====\n")
        structured_data["durable_writes"] = {"status": "success", "data": []}
        return "\n".join(adoc_content), structured_data
    
    # Find keyspaces with durable_writes false
    false_durable = [ks for ks in user_keyspaces if not ks.get('durable_writes', True)]
    
    # Format filtered data for display (only user keyspaces)
    filtered_table = format_data_as_table(
        user_keyspaces,
        columns=['keyspace_name', 'durable_writes', 'replication']
    )

    if false_durable:
        adoc_content.append(
            f"[CRITICAL]\n====\n"
            f"**{len(false_durable)} user keyspace(s)** have durable_writes set to false.\n"
            "This increases risk of data loss on commitlog failure.\n"
            "====\n"
        )
        adoc_content.append(filtered_table)

        recommendations = [
            "For each affected keyspace, execute: ALTER KEYSPACE keyspace_name WITH durable_writes = true",
            "Verify commitlog disk space and configuration in cassandra.yaml",
            "Consider using SSD for commitlog to improve write durability"
        ]
        adoc_content.extend(format_recommendations(recommendations))

        status_result = "critical"
    else:
        adoc_content.append(
            "[NOTE]\n====\n"
            "All user keyspaces have durable_writes enabled.\n"
            "====\n"
        )
        adoc_content.append(filtered_table)
        status_result = "success"
    
    structured_data["durable_writes"] = {
        "status": status_result,
        "data": user_keyspaces,
        "false_durable_count": len(false_durable),
        "keyspaces_with_false": [ks['keyspace_name'] for ks in false_durable]
    }
    
    return "\n".join(adoc_content), structured_data
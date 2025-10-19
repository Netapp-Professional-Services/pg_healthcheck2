from plugins.cassandra.utils.qrylib.qry_keyspace_replication_check import get_keyspace_replication_query

from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query


def get_weight():
    return 7


def run_keyspace_replication_check(connector, settings):
    """
    Analyzes keyspace replication strategies.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Keyspace Replication Strategy Analysis",
        "Analyzing replication strategies for all user keyspaces."
    )
    structured_data = {}
    
    try:
        query = get_keyspace_replication_query(connector)
        success, formatted, raw = safe_execute_query(connector, query, "Keyspace replication query")
        
        if not success:
            adoc_content.append(formatted)
            structured_data["replication"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces in Python
        system_keyspaces = {'system', 'system_schema', 'system_traces', 
                           'system_auth', 'system_distributed', 'system_views'}
        user_keyspaces = [ks for ks in raw 
                          if ks.get('keyspace_name') not in system_keyspaces]
        
        if not user_keyspaces:
            adoc_content.append("[NOTE]\n====\nNo user keyspaces found.\n====\n")
            structured_data["replication"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Analyze replication strategies
        simple_strategy_keyspaces = []
        for ks in user_keyspaces:
            replication = ks.get('replication', {})
            if 'SimpleStrategy' in replication.get('class', ''):
                simple_strategy_keyspaces.append(ks['keyspace_name'])
        
        if simple_strategy_keyspaces:
            adoc_content.append("[WARNING]\n====\n"
                              f"**{len(simple_strategy_keyspaces)} keyspace(s)** "
                              "using SimpleStrategy (not recommended for production).\n"
                              "====\n")
            adoc_content.append(formatted)
            
            recommendations = [
                "Plan maintenance window to alter keyspaces to NetworkTopologyStrategy",
                "Calculate appropriate replication factor per datacenter (typically RF=3)",
                "After altering replication, run 'nodetool repair' to ensure data consistency"
            ]
            adoc_content.extend(format_recommendations(recommendations))
        else:
            adoc_content.append("[NOTE]\n====\n"
                              "All user keyspaces use NetworkTopologyStrategy.\n"
                              "====\n")
        
        structured_data["replication"] = {
            "status": "success",
            "data": user_keyspaces,
            "simple_strategy_count": len(simple_strategy_keyspaces)
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nReplication check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["replication"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
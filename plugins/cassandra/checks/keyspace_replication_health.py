from plugins.cassandra.utils.qrylib.qry_keyspace_replication_health import get_keyspace_replication_health_query
from plugins.cassandra.utils.keyspace_filter import filter_user_keyspaces
from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query, format_data_as_table

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High: replication configuration impacts availability

def run_keyspace_replication_health(connector, settings):
    """
    Analyzes keyspace replication strategies and factors for health.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Keyspace Replication Health Analysis",
        "Analyzing replication strategies and factors for all user keyspaces."
    )
    structured_data = {}
    
    try:
        query = get_keyspace_replication_health_query(connector)
        success, formatted, raw = safe_execute_query(connector, query, "Keyspace replication query")
        
        if not success:
            adoc_content.append(formatted)
            structured_data["replication"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces using centralized filter
        user_keyspaces = filter_user_keyspaces(raw, settings)
        
        if not user_keyspaces:
            adoc_content.append("[NOTE]\n====\nNo user keyspaces found.\n====\n")
            structured_data["replication"] = {"status": "success", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Analyze replication health
        issues = []
        simple_strategy_keyspaces = []
        low_rf_keyspaces = []
        for ks in user_keyspaces:
            keyspace_name = ks['keyspace_name']
            replication = ks.get('replication', {})
            strategy_class = replication.get('class', '')
            
            if 'SimpleStrategy' in strategy_class:
                simple_strategy_keyspaces.append(keyspace_name)
                issues.append(f"Keyspace '{keyspace_name}' uses SimpleStrategy (not recommended for production).")
            elif 'NetworkTopologyStrategy' in strategy_class:
                # Check min RF across DCs
                min_rf = float('inf')
                has_rf = False
                for dc, rf_val in replication.items():
                    if dc != 'class' and isinstance(rf_val, int):
                        min_rf = min(min_rf, rf_val)
                        has_rf = True
                if has_rf and min_rf < 2:
                    low_rf_keyspaces.append((keyspace_name, min_rf))
                    issues.append(f"Keyspace '{keyspace_name}' has low replication factor {min_rf} (minimum should be 3 for production).")

        # Format filtered data for display (only user keyspaces)
        filtered_table = format_data_as_table(
            user_keyspaces,
            columns=['keyspace_name', 'replication']
        )
        adoc_content.append(filtered_table)
        
        if issues:
            adoc_content.append("[WARNING]\n====\nReplication health issues detected:\n")
            for issue in issues:
                adoc_content.append(f"* {issue}")
            adoc_content.append("====\n")
            
            recommendations = [
                "For SimpleStrategy keyspaces, alter to NetworkTopologyStrategy with RF=3 per DC.",
                "Increase low RF to at least 3: ALTER KEYSPACE <name> WITH replication = {'class': 'NetworkTopologyStrategy', '<dc>': 3}",
                "After changes, run 'nodetool repair -full' to ensure data consistency across the cluster."
            ]
            adoc_content.extend(format_recommendations(recommendations))
            
            status_result = "warning"
        else:
            adoc_content.append("[NOTE]\n====\nAll user keyspaces have healthy replication configurations (NetworkTopologyStrategy with RF >= 2).\n====\n")
            status_result = "success"
        
        structured_data["replication"] = {
            "status": status_result,
            "data": user_keyspaces,
            "simple_strategy_count": len(simple_strategy_keyspaces),
            "low_rf_count": len(low_rf_keyspaces),
            "total_user_keyspaces": len(user_keyspaces)
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nReplication health check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["replication"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
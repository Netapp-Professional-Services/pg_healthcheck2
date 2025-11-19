from plugins.cassandra.utils.qrylib.qry_keyspace_replication_strategy import get_keyspace_replication_strategy_query
from plugins.common.check_helpers import format_check_header, safe_execute_query, format_recommendations


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High - replication configuration impacts availability


def run_keyspace_replication_strategy(connector, settings):
    """
    Verifies that all user-defined keyspaces use NetworkTopologyStrategy
    and reports their replication factors per datacenter.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Keyspace Replication Strategy Analysis",
        "Verifying that all user-defined keyspaces use NetworkTopologyStrategy "
        "and reporting replication factors per datacenter."
    )
    structured_data = {}
    
    try:
        query = get_keyspace_replication_strategy_query(connector)
        success, formatted, raw = safe_execute_query(
            connector, query, "Keyspace replication strategy"
        )
        
        if not success:
            adoc_content.append(formatted)
            structured_data["replication_strategy"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces in Python
        system_keyspaces = [ 'system', 'system_schema', 'system_traces', 'system_auth', 'system_distributed', 'system_views' ]
        user_keyspaces = [
            ks for ks in raw
            if ks.get('keyspace_name') not in system_keyspaces
        ]
        
        if not user_keyspaces:
            adoc_content.append("[NOTE]\n====\nNo user-defined keyspaces found.\n====\n")
            structured_data["replication_strategy"] = {
                "status": "success",
                "data": [],
                "issues": 0
            }
            return "\n".join(adoc_content), structured_data
        
        # Analyze strategies
        problematic_keyspaces = []
        healthy_keyspaces = []
        for ks in user_keyspaces:
            replication = ks.get('replication', {})
            strategy_class = replication.get('class', '')
            if 'NetworkTopologyStrategy' in strategy_class:
                # Extract RF per DC
                dc_rf = {k: v for k, v in replication.items() if k != 'class'}
                ks['dc_replication_factors'] = dc_rf
                healthy_keyspaces.append(ks)
            else:
                # Includes SimpleStrategy or others
                problematic_keyspaces.append(ks)
        
        # Report results
        adoc_content.append(formatted)
        
        if problematic_keyspaces:
            adoc_content.append(
                f"[WARNING]\n====\n"
                f"**{len(problematic_keyspaces)} user keyspace(s)** "
                f"do not use NetworkTopologyStrategy.\n====\n"
            )
            
            recommendations = [
                "Plan a maintenance window to alter problematic keyspaces:",
                "ALTER KEYSPACE {keyspace_name} WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};  # Adjust DCs and RF as needed",
                "After altering, run 'nodetool repair -full {keyspace_name}' to ensure data consistency across the cluster",
                "Verify datacenter names in cassandra-rackdc.properties match your topology"
            ]
            adoc_content.extend(format_recommendations(recommendations))
            
            status_result = "warning"
        else:
            adoc_content.append(
                f"[NOTE]\n====\n"
                f"All {len(user_keyspaces)} user keyspace(s) use NetworkTopologyStrategy.\n====\n"
            )
            
            # Report RF per DC for healthy ones
            adoc_content.append("\n==== Replication Factors per Datacenter")
            adoc_content.append("|===\n|Keyspace|Datacenter|Replication Factor")
            for ks in healthy_keyspaces:
                for dc, rf in ks.get('dc_replication_factors', {}).items():
                    adoc_content.append(f"|{ks['keyspace_name']}|{dc}|{rf}")
            adoc_content.append("|===\n")
            
            # Check for low RF (optional best practice)
            low_rf_keyspaces = [
                ks for ks in healthy_keyspaces
                if any(int(rf) < 2 for rf in ks.get('dc_replication_factors', {}).values())
            ]
            if low_rf_keyspaces:
                adoc_content.append(
                    f"[IMPORTANT]\n====\n"
                    f"**{len(low_rf_keyspaces)} keyspace(s)** have datacenter(s) "
                    f"with replication factor < 2 (not recommended for production).\n====\n"
                )
            
            status_result = "success"
        
        structured_data["replication_strategy"] = {
            "status": status_result,
            "data": user_keyspaces,
            "healthy_count": len(healthy_keyspaces),
            "problematic_count": len(problematic_keyspaces),
            "problematic_keyspaces": [ks['keyspace_name'] for ks in problematic_keyspaces]
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nReplication strategy check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["replication_strategy"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data

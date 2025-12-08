from plugins.cassandra.utils.qrylib.qry_udf_aggregates import get_functions_query, get_aggregates_query
from plugins.cassandra.utils.keyspace_filter import KeyspaceFilter
from plugins.common.check_helpers import format_check_header, safe_execute_query, format_recommendations

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 5  # Medium: configuration recommendation for UDF languages

def run_udf_aggregates_check(connector, settings):
    """
    Analyzes user-defined functions and aggregates, flagging Java-based implementations.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "User-Defined Functions and Aggregates Analysis",
        "Querying system_schema.functions and system_schema.aggregates to list all UDFs and aggregates, flagging any using the 'java' language."
    )
    structured_data = {}
    
    # Create keyspace filter for filtering UDFs/aggregates
    ks_filter = KeyspaceFilter(settings)
    
    # Query functions
    query_funcs = get_functions_query(connector)
    success_funcs, formatted_funcs, raw_funcs = safe_execute_query(
        connector, query_funcs, "UDF functions query"
    )
    
    if not success_funcs:
        adoc_content.append(formatted_funcs)
        structured_data["functions"] = {"status": "error", "data": raw_funcs}
        # Continue to aggregates
    else:
        user_funcs = [
            {"type": "function", **f}
            for f in raw_funcs
            if not ks_filter.is_excluded(f.get('keyspace_name'))
        ]
        structured_data["functions"] = {
            "status": "success",
            "data": user_funcs
        }
        adoc_content.append(formatted_funcs)
    
    # Query aggregates
    query_aggs = get_aggregates_query(connector)
    success_aggs, formatted_aggs, raw_aggs = safe_execute_query(
        connector, query_aggs, "UDF aggregates query"
    )
    
    if not success_aggs:
        adoc_content.append(formatted_aggs)
        structured_data["aggregates"] = {"status": "error", "data": raw_aggs}
        # Return with partial data
    else:
        user_aggs = [
            {"type": "aggregate", **a}
            for a in raw_aggs
            if not ks_filter.is_excluded(a.get('keyspace_name'))
        ]
        structured_data["aggregates"] = {
            "status": "success",
            "data": user_aggs
        }
        adoc_content.append(formatted_aggs)
    
    # Only analyze if both succeeded
    if success_funcs and success_aggs:
        all_user_udfs = user_funcs + user_aggs
        
        if not all_user_udfs:
            adoc_content.append("[NOTE]\n====\nNo user-defined functions or aggregates found.\n====\n")
            structured_data["overall_status"] = "success"
            structured_data["java_count"] = 0
            return "\n".join(adoc_content), structured_data
        
        java_udfs = [u for u in all_user_udfs if u.get('language') == 'java']
        
        if java_udfs:
            adoc_content.append(
                f"[WARNING]\n====\n"
                f"**{len(java_udfs)} user-defined { 'function' if len([u for u in java_udfs if u['type']=='function']) == len(java_udfs) else 'function/aggregate' }s** use the 'java' language, which may introduce security and maintenance risks.\n====\n"
            )
            
            # Table of Java UDFs
            adoc_content.append("\n==== Java-Based UDFs/Aggregates")
            adoc_content.append("|===\n|Keyspace|Name|Language|Return Type")
            for u in java_udfs:
                adoc_content.append(
                    f"|{u['keyspace_name']}|{u['function_name']}|{u['language']}|{u.get('return_type', 'N/A')}"
                )
            adoc_content.append("|===\n")
            
            recommendations = [
                "Rewrite Java UDFs/aggregates using safer languages like JavaScript or Python for better security and easier debugging.",
                "If Java is necessary, perform thorough code review and security auditing.",
                "Remove unused UDFs/aggregates to minimize potential vulnerabilities.",
                "Test application queries after any UDF modifications to ensure functionality."
            ]
            adoc_content.extend(format_recommendations(recommendations))
            
            structured_data["overall_status"] = "warning"
            structured_data["java_count"] = len(java_udfs)
        else:
            adoc_content.append(
                f"[NOTE]\n====\n"
                f"All {len(all_user_udfs)} user-defined functions and aggregates use safe languages (no Java detected).\n====\n"
            )
            structured_data["overall_status"] = "success"
            structured_data["java_count"] = 0
    else:
        structured_data["overall_status"] = "error"
    
    return "\n".join(adoc_content), structured_data
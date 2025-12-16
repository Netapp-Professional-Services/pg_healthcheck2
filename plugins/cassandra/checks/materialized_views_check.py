from plugins.cassandra.utils.qrylib.qry_materialized_views import get_materialized_views_query
from plugins.cassandra.utils.keyspace_filter import filter_user_keyspaces
from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query, format_data_as_table

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 5  # Medium: Performance concerns with materialized views

def run_materialized_views_check(connector, settings):
    """
    Performs the health check analysis for materialized views.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Materialized Views Analysis",
        "Querying system_schema.views to list all materialized views, which can introduce performance overhead."
    )
    structured_data = {}
    
    try:
        query = get_materialized_views_query(connector)
        success, formatted, raw = safe_execute_query(connector, query, "Materialized views query")
        
        if not success:
            adoc_content.append(formatted)
            structured_data["materialized_views"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
        
        # Filter out system keyspaces using centralized filter
        user_views = filter_user_keyspaces(raw, settings)
        
        if not user_views:
            adoc_content.append("[NOTE]\n====\nNo user materialized views found.\n====\n")
            structured_data["materialized_views"] = {"status": "success", "data": [], "count": 0}
            return "\n".join(adoc_content), structured_data
        
        # Analyze: Count views, group by keyspace if needed
        view_count = len(user_views)

        # Format filtered data for display (only user views)
        filtered_table = format_data_as_table(
            user_views,
            columns=['keyspace_name', 'view_name', 'base_table_name']
        )
        adoc_content.append(filtered_table)
        
        if view_count > 10:  # Arbitrary threshold for warning
            adoc_content.append("[WARNING]\n====\n"
                              f"**{view_count} materialized view(s)** found across user keyspaces. "
                              "High numbers can introduce significant performance overhead during writes.\n"
                              "====\n")
            
            recommendations = [
                "Review materialized views for necessity - consider denormalizing application logic instead",
                "Monitor write performance on tables with views using nodetool tpstats",
                "For high-write tables, evaluate dropping unused views: DROP MATERIALIZED VIEW keyspace.view_name",
                "Ensure base tables have appropriate clustering keys to optimize view maintenance"
            ]
            adoc_content.extend(format_recommendations(recommendations))
            status_result = "warning"
        else:
            adoc_content.append("[NOTE]\n====\n"
                              f"{view_count} materialized view(s) found. Low count is generally acceptable.\n"
                              "====\n")
            status_result = "success"
        
        structured_data["materialized_views"] = {
            "status": status_result,
            "data": user_views,
            "count": view_count,
            "high_count": view_count > 10
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nMaterialized views check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["materialized_views"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
from plugins.common.check_helpers import CheckContentBuilder
from plugins.kafka.utils.qrylib.list_topics_queries import get_list_topics_query

def get_weight():
    return 4

def run_check_topic_count_and_naming(connector, settings):
    """
    Analyzes topic count and naming conventions.
    
    Checks for:
    - Topic count exceeding threshold
    - Topics starting with digits
    - Topics containing spaces
    - Topics containing hyphens (best practice: use underscores)
    - Topics containing uppercase letters (best practice: lowercase only)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    try:
        builder.h3("Topic Count and Naming Conventions")
        
        query = get_list_topics_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            builder.add(formatted)
            structured_data["topic_analysis"] = {"status": "error", "data": raw}
            return builder.build(), structured_data
        
        if not raw or not raw.get('topics'):
            builder.note("No topics found. System is healthy.")
            structured_data["topic_analysis"] = {
                "status": "success",
                "data": [{
                    "total_count": 0,
                    "topics": [],
                    "max_threshold": settings.get('kafka_max_topics', 100)
                }]
            }
            return builder.build(), structured_data
        
        total_count = raw.get('count', 0)
        topics = raw.get('topics', [])
        max_topics = settings.get('kafka_max_topics', 100)
        
        # === COLLECT FACTS: Analyze naming violations ===
        def has_naming_violation(topic_name):
            """Check if topic name violates naming conventions."""
            if not topic_name:
                return False
            return (
                topic_name[0].isdigit() or      # Starts with digit
                ' ' in topic_name or             # Contains space
                '-' in topic_name or             # Contains hyphen (prefer underscores)
                topic_name != topic_name.lower() # Contains uppercase (prefer lowercase)
            )
        
        invalid_names = [t for t in topics if has_naming_violation(t)]
        
        # Categorize violations by type for detailed reporting
        starts_with_digit = [t for t in topics if t and t[0].isdigit()]
        contains_space = [t for t in topics if ' ' in t]
        contains_hyphen = [t for t in topics if '-' in t]
        contains_uppercase = [t for t in topics if t != t.lower()]
        
        issues_found = total_count > max_topics or invalid_names
        
        # === INTERPRET FACTS: Report issues ===
        if total_count > max_topics:
            builder.warning_issue(
                "Topic Proliferation",
                {
                    "Current Count": total_count,
                    "Threshold": max_topics,
                    "Impact": "May lead to performance degradation"
                }
            )
        
        if invalid_names:
            builder.warning(
                f"**Naming Violations:** {len(invalid_names)} topics do not follow naming conventions."
            )
            
            # Show violations by type in tables
            if starts_with_digit:
                builder.h4(f"Topics Starting with Digit ({len(starts_with_digit)})")
                builder.para("*Issue: Topic names should start with a letter*")
                
                # Show top 10
                for topic in starts_with_digit[:10]:
                    builder.text(f"* `{topic}`")
                if len(starts_with_digit) > 10:
                    builder.para(f"_... and {len(starts_with_digit) - 10} more_")
                builder.blank()
            
            if contains_space:
                builder.h4(f"Topics Containing Spaces ({len(contains_space)})")
                builder.para("*Issue: Spaces cause problems with many tools*")
                
                for topic in contains_space[:10]:
                    builder.text(f"* `{topic}`")
                if len(contains_space) > 10:
                    builder.para(f"_... and {len(contains_space) - 10} more_")
                builder.blank()
            
            if contains_hyphen:
                builder.h4(f"Topics Containing Hyphens ({len(contains_hyphen)})")
                builder.para("*Best practice: Use underscores instead of hyphens*")
                
                for topic in contains_hyphen[:10]:
                    suggested = topic.replace('-', '_')
                    builder.text(f"* `{topic}` → suggested: `{suggested}`")
                if len(contains_hyphen) > 10:
                    builder.para(f"_... and {len(contains_hyphen) - 10} more_")
                builder.blank()
            
            if contains_uppercase:
                builder.h4(f"Topics Containing Uppercase ({len(contains_uppercase)})")
                builder.para("*Best practice: Use lowercase for consistency*")
                
                for topic in contains_uppercase[:10]:
                    suggested = topic.lower()
                    builder.text(f"* `{topic}` → suggested: `{suggested}`")
                if len(contains_uppercase) > 10:
                    builder.para(f"_... and {len(contains_uppercase) - 10} more_")
                builder.blank()
        
        # Summary section
        builder.h4("Topic Summary")
        
        summary_data = {
            "Total Topics": total_count,
            "Threshold": max_topics,
            "Topics with Violations": len(invalid_names),
            "Starts with Digit": len(starts_with_digit),
            "Contains Spaces": len(contains_space),
            "Contains Hyphens": len(contains_hyphen),
            "Contains Uppercase": len(contains_uppercase)
        }
        builder.dict_table(summary_data, "Metric", "Count")
        
        # Recommendations
        if issues_found:
            recommendations = {}
            
            if total_count > max_topics:
                recommendations["high"] = [
                    "**Implement topic creation policies** - Require approval for new topics",
                    "**Review and consolidate** unnecessary or duplicate topics",
                    "**Set up alerts** for topic count approaching limits"
                ]
            
            if invalid_names:
                recommendations["high"] = recommendations.get("high", [])
                recommendations["high"].extend([
                    "**Establish naming standards** - Use lowercase letters, digits, underscores, and dots only",
                    "**Create a topic naming guide** - Document conventions and share with teams",
                    "**Consider renaming** critical topics during maintenance windows"
                ])
            
            recommendations["general"] = [
                "Use lowercase letters, digits, underscores (_), and dots (.) only",
                "Start topic names with a letter, not a digit",
                "Avoid hyphens - they can cause issues with metrics systems like Prometheus",
                "Keep names descriptive but concise (e.g., `user_events`, `order_processing`)",
                "Use prefixes for organization (e.g., `prod_`, `staging_`)",
                "Regularly review topic list for compliance",
                "Implement automated naming validation in CI/CD pipelines"
            ]
            
            builder.recs(recommendations)
        else:
            builder.success(
                "All topics follow naming conventions and count is within limits.\n\n"
                f"Total topics: {total_count} (threshold: {max_topics})"
            )
        
        # === STRUCTURED DATA: Full details ===
        topic_details = []
        for topic_name in topics:
            starts_digit = topic_name and len(topic_name) > 0 and topic_name[0].isdigit()
            has_space = ' ' in topic_name
            has_hyphen = '-' in topic_name
            has_uppercase = topic_name != topic_name.lower()
            
            topic_details.append({
                "topic": topic_name,
                "starts_with_digit": starts_digit,
                "contains_space": has_space,
                "contains_hyphen": has_hyphen,
                "contains_uppercase": has_uppercase,
                "has_violation": starts_digit or has_space or has_hyphen or has_uppercase
            })
        
        # Store both summary and individual topic data
        structured_data["topic_analysis"] = {
            "status": "success",
            "data": [{
                "total_count": total_count,
                "max_threshold": max_topics,
                "violation_count": len(invalid_names),
                "violations_by_type": {
                    "starts_with_digit": len(starts_with_digit),
                    "contains_space": len(contains_space),
                    "contains_hyphen": len(contains_hyphen),
                    "contains_uppercase": len(contains_uppercase)
                },
                "exceeds_threshold": total_count > max_topics
            }]
        }
        
        structured_data["topic_details"] = {
            "status": "success",
            "data": topic_details
        }
    
    except Exception as e:
        import traceback
        from logging import getLogger
        logger = getLogger(__name__)
        logger.error(f"Topic count/naming check failed: {e}\n{traceback.format_exc()}")
        
        builder.error(f"Check failed: {e}")
        structured_data["topic_analysis"] = {"status": "error", "details": str(e)}
    
    return builder.build(), structured_data

"""
Trend Analysis Module
Generates AI-powered engagement recommendations from historical health check data.
"""

import json
from flask import current_app
import psycopg2
import psycopg2.extras
from datetime import datetime


def get_accessible_companies_list(db_config, accessible_company_ids):
    """
    Fetches companies accessible by the user for dropdown by calling
    the get_accessible_companies_list SQL function.
    """
    if not accessible_company_ids:
        return []
     
    conn = None
    companies = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor() # No DictCursor needed

        # Call the new database function
        query = "SELECT * FROM get_accessible_companies_list(%s);"
        
        cursor.execute(query, (accessible_company_ids,))
        
        # Format the (id, company_name) tuples into the desired list of dicts
        companies = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]
        
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching companies: {e}")
    finally:
        if conn:
            conn.close()
    return companies


def get_trend_data(db_config, company_id, days, accessible_company_ids):
    """
    Aggregates trend data for AI analysis by calling the get_trend_data SQL function.
    Enforces access control via accessible_company_ids.
    """
    # 1. --- CRITICAL: Security check STAYS in the application layer ---
    if company_id not in accessible_company_ids:
        current_app.logger.warning(f"Access denied: company {company_id} not accessible")
        return None
     
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        # We don't need DictCursor since we're just getting one JSON field,
        # but it's fine to leave it. psycopg2 handles JSON decoding automatically.
        cursor = conn.cursor() 
         
        # 2. Call the single database function
        query = "SELECT get_trend_data(%s, %s);"
         
        cursor.execute(query, (company_id, days))
        
        result = cursor.fetchone()
        
        # 3. Return the JSON object (which psycopg2 auto-decodes into a Python dict)
        if result and result[0]:
            return result[0]
        else:
            # This handles the case where the company_id was valid
            # but the SQL function returned NULL (e.g., company not found)
            return None
         
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching trend data: {e}")
        return None
    finally:
        if conn:
            conn.close()

def build_engagement_prompt(trend_data, template_text):
    """Builds the AI prompt from template and trend data."""
    
    # Format recurring issues section
    recurring_section = ""
    if trend_data['recurring_issues']:
        recurring_section = "| Rule Name | Severity | Occurrences | Status | Days Present |\n"
        recurring_section += "|-----------|----------|-------------|--------|---------------|\n"
        for issue in trend_data['recurring_issues'][:15]:  # Top 15
            status = "🔴 UNRESOLVED" if issue['in_latest_run'] else "✅ Resolved"
            
            # FIX: Parse ISO timestamp strings to datetime objects
            try:
                # Handle both with and without 'Z' suffix
                last_seen_str = issue['last_seen'].replace('Z', '+00:00') if isinstance(issue['last_seen'], str) else issue['last_seen']
                first_seen_str = issue['first_seen'].replace('Z', '+00:00') if isinstance(issue['first_seen'], str) else issue['first_seen']
                
                last_seen = datetime.fromisoformat(last_seen_str)
                first_seen = datetime.fromisoformat(first_seen_str)
                days_present = (last_seen - first_seen).days
            except (ValueError, AttributeError) as e:
                current_app.logger.warning(f"Error parsing dates for issue {issue.get('rule_name')}: {e}")
                days_present = 0
            
            recurring_section += f"| {issue['rule_name'][:40]} | {issue['severity']} | {issue['occurrences']} | {status} | {days_present} days |\n"
    else:
        recurring_section = "✅ No recurring issues detected!"
    
    # Format health score trend
    health_section = ""
    if trend_data['health_score_trend']:
        scores = trend_data['health_score_trend']
        if len(scores) >= 2:
            first_score = scores[0]['calculated_score']
            last_score = scores[-1]['calculated_score']
            trend_direction = "📈 Improving" if last_score > first_score else "📉 Degrading" if last_score < first_score else "➡️ Stable"
            health_section = f"{trend_direction} (from {first_score} to {last_score})\n\n"
        health_section += "Recent scores: " + ", ".join([f"{s['calculated_score']}" for s in scores[-10:]])
    else:
        health_section = "No health score data available"
    
    # Format cross-tech patterns
    cross_tech_section = ""
    if trend_data['cross_technology_patterns']:
        cross_tech_section = "| Technology | Unique Issues | Total Triggers | Critical | High | Medium |\n"
        cross_tech_section += "|------------|---------------|----------------|----------|------|--------|\n"
        for tech in trend_data['cross_technology_patterns']:
            cross_tech_section += f"| {tech['db_technology']} | {tech['unique_issues']} | {tech['total_triggered_rules']} | {tech['critical_count']} | {tech['high_count']} | {tech['medium_count']} |\n"
    else:
        cross_tech_section = "Single technology deployment"
    
    # Replace template variables
    prompt = template_text.format(
        company_name=trend_data['company_info']['company_name'],
        days=trend_data['time_period']['days'],
        total_runs=trend_data['summary']['total_runs'],
        technologies=", ".join(trend_data['summary']['technologies']),
        first_run=trend_data['time_period']['first_run'],
        last_run=trend_data['time_period']['last_run'],
        recurring_issues_section=recurring_section,
        health_score_section=health_section,
        cross_tech_section=cross_tech_section
    )
    
    return prompt


def generate_trend_analysis(db_config, company_id, days, profile_id, template_id, accessible_company_ids, user_id):
    """
    Main orchestration function - generates trend analysis.
    Returns: (success: bool, result: dict or error_message: str)
    """
    from .ai_connector import get_ai_recommendation
    from .database import fetch_prompt_template_content
    
    # 1. Get trend data
    trend_data = get_trend_data(db_config, company_id, days, accessible_company_ids)
    if not trend_data:
        return False, "Failed to retrieve trend data or access denied"
    
    # 2. Get prompt template
    template_text = fetch_prompt_template_content(db_config, template_id)
    if not template_text:
        return False, "Prompt template not found"
    
    # 3. Build prompt
    prompt = build_engagement_prompt(trend_data, template_text)
    
    # 4. Generate AI content
    try:
        ai_result = get_ai_recommendation(prompt, profile_id)
        if not ai_result or ai_result.startswith("Error:"):
            return False, f"AI generation failed: {ai_result}"
        
        # 5. Save result
        analysis_id = save_trend_analysis(
            db_config=db_config,
            user_id=user_id,
            company_id=company_id,
            analysis_period_days=days,
            persona='consulting',
            template_id=template_id,
            ai_content=ai_result,
            # trend_data_snapshot=trend_data, <-- This line is removed
            profile_id=profile_id
        )
        
        if not analysis_id:
            return False, "Failed to save trend analysis"
        
        return True, {
            "analysis_id": analysis_id,
            "content": ai_result,
            "company_name": trend_data['company_info']['company_name']
        }
        
    except Exception as e:
        current_app.logger.error(f"Error generating trend analysis: {e}")
        return False, str(e)

def save_trend_analysis(db_config, user_id, company_id, analysis_period_days, persona, template_id, ai_content, profile_id):
    """
    Saves trend analysis result by calling the 
    save_trend_analysis SQL function.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # The logic for report_name, report_type, and encryption
        # is now handled by the SQL function.
        query = "SELECT * FROM save_trend_analysis(%s, %s, %s, %s, %s, %s, %s);"

        cursor.execute(query, (
            user_id,
            company_id,
            analysis_period_days,
            persona,
            template_id,
            ai_content,
            profile_id
        ))

        analysis_id = cursor.fetchone()[0]
        conn.commit()
        return analysis_id

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        current_app.logger.error(f"Database error saving trend analysis: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_trend_analyses_history(db_config, accessible_company_ids, limit=50):
    """
    Fetches recent trend analyses for display by calling the 
    get_trend_analyses_history SQL function.
    """
    if not accessible_company_ids:
        return []
     
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Call the PostgreSQL function directly
        # psycopg2 handles converting the Python list to a PG ARRAY
        query = "SELECT * FROM get_trend_analyses_history(%s, %s);"

        cursor.execute(query, (accessible_company_ids, limit))
        return [dict(row) for row in cursor.fetchall()]

    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching trend analyses: {e}")
        return []
    finally:
        if conn:
            conn.close()

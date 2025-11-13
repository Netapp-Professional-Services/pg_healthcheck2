"""
Database functions for Analysis schemas.

This module provides Python wrappers for the analysis stored procedures,
following the patterns established in database.py.
"""

import psycopg2
import psycopg2.extras
from flask import current_app


def get_accessible_schemas(db_config):
    """
    Get list of analysis schemas the current user can access.

    Args:
        db_config: Database configuration dictionary

    Returns:
        list: List of dicts with schema_name and display_name
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM get_accessible_analysis_schemas()")
        schemas = cursor.fetchall()
        return schemas
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching accessible schemas: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_migration_candidates(db_config, limit=50, min_frequency=50000, days=30):
    """
    Get high-frequency write workloads that may benefit from Kafka migration.

    Args:
        db_config: Database configuration dictionary
        limit: Maximum number of results to return
        min_frequency: Minimum calls per hour threshold
        days: Number of days to look back

    Returns:
        list: List of migration candidate dicts
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT * FROM get_migration_candidates(%s, %s, %s)",
            [limit, min_frequency, days]
        )
        candidates = cursor.fetchall()
        return candidates
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching migration candidates: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_write_volume_trends(db_config, company_id, days=90):
    """
    Get write volume growth trends for a specific company.

    Args:
        db_config: Database configuration dictionary
        company_id: Company ID to fetch trends for
        days: Number of days to look back

    Returns:
        list: List of trend data dicts with check_date, totals, etc.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT * FROM get_write_volume_trends(%s, %s)",
            [company_id, days]
        )
        trends = cursor.fetchall()
        return trends
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching write volume trends: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_migration_pipeline_summary(db_config):
    """
    Get executive summary of migration opportunities.

    Args:
        db_config: Database configuration dictionary

    Returns:
        dict: Summary with counts, revenue estimates, top company
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM get_migration_pipeline_summary()")
        summary = cursor.fetchone()
        return summary if summary else {}
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching migration pipeline summary: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_customer_technology_footprint(db_config, days=90):
    """
    Get technology usage footprint across all customers.

    Args:
        db_config: Database configuration dictionary
        days: Number of days to look back

    Returns:
        list: List of customer technology usage dicts
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT * FROM get_customer_technology_footprint(%s)",
            [days]
        )
        footprint = cursor.fetchall()
        return footprint
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching customer technology footprint: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_query_details(db_config, run_id, query_text):
    """
    Get detailed metrics for a specific query from a health check run.

    Args:
        db_config: Database configuration dictionary
        run_id: Health check run ID
        query_text: Query text to search for (partial match)

    Returns:
        dict: Query details with all metrics, or None if not found
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT * FROM get_query_details(%s, %s)",
            [run_id, query_text]
        )
        details = cursor.fetchone()
        return details
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching query details: {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_consulting_opportunities_summary(db_config):
    """
    Get executive summary of consulting engagement opportunities.

    Args:
        db_config: Database configuration dictionary

    Returns:
        dict: Summary with counts, revenue estimates, top opportunity type
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM get_consulting_opportunities_summary()")
        summary = cursor.fetchone()
        return summary if summary else {}
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching consulting opportunities summary: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_consulting_opportunities(db_config, limit=50):
    """
    Get all consulting engagement opportunities from triggered rules.

    Args:
        db_config: Database configuration dictionary
        limit: Maximum number of results to return

    Returns:
        list: List of consulting opportunity dicts
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            """
            SELECT * FROM consulting_analysis.consulting_opportunities_from_rules
            ORDER BY
                CASE consulting_priority
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    ELSE 4
                END,
                triggered_rule_count DESC,
                run_timestamp DESC
            LIMIT %s
            """,
            [limit]
        )
        opportunities = cursor.fetchall()
        return opportunities
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching consulting opportunities: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_consulting_executive_summary(db_config):
    """
    Get executive-level summary of consulting opportunities by type.

    Args:
        db_config: Database configuration dictionary

    Returns:
        list: List of consulting type summaries
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM consulting_analysis.executive_consulting_summary")
        summary = cursor.fetchall()
        return summary
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching consulting executive summary: {e}")
        return []
    finally:
        if conn:
            conn.close()

"""
Query library for Aurora version upgrade analysis.

This module provides queries to get current Aurora version information
from the database itself.
"""

def get_current_version_query():
    """
    Get the current PostgreSQL version string.

    Returns:
        str: SQL query to get version
    """
    return "SELECT version();"


def get_aurora_version_query():
    """
    Get the Aurora-specific version if available.

    Returns:
        str: SQL query to get Aurora version
    """
    return "SELECT aurora_version();"

"""CQL queries for compaction pending tasks in Cassandra."""

__all__ = [
    'get_compaction_pending_tasks_query'
]


def get_compaction_pending_tasks_query(connector):
    """
    Returns CQL query for CompactionExecutor pending tasks from system_views.thread_pools.

    Args:
        connector: Cassandra connector instance

    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT pending_tasks FROM system_views.thread_pools WHERE name = 'CompactionExecutor';
    """

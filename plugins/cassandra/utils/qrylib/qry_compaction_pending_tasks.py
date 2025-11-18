import json

"""Nodetool queries for compaction pending tasks in Cassandra."""

__all__ = [
    'get_nodetool_compactionstats_query'
]

def get_nodetool_compactionstats_query(connector):
    """
    Returns query of system_view.thread_pools for CompactionExecutor Pending tasks.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return """
	select pending_tasks  from system_views.thread_pools where name ='CompactionExecutor';
    """ 
    })

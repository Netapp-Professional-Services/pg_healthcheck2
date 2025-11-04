"""Query functions for iostat operations."""

import json


def get_iostat_query(connector):
    """
    Returns query for checking disk I/O statistics via SSH on all brokers.
    
    Uses iostat to gather disk performance metrics including throughput,
    IOPS, utilization, and I/O wait times.
    """
    return json.dumps({
        "operation": "iostat",
        "command": "iostat -x 1 2 | tail -n +3"  # Run twice, skip first (boot stats)
    })

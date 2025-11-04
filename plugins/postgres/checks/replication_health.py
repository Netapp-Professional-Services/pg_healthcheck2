"""
Replication Health Check

Comprehensive analysis of physical and logical replication health, including:
- Streaming replication status and lag
- Replication slot health and inactive slots
- Logical subscription status

Patroni-aware with role-based checks and context.
"""

import logging
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.qrylib.replication_health import (
    get_physical_replication_query,
    get_replication_slots_query,
    get_subscription_stats_query,
    get_wal_receiver_query
)

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module."""
    return 4


def _is_standby_node(connector) -> bool:
    """
    Check if this PostgreSQL node is a standby (in recovery).

    Args:
        connector: Database connector

    Returns:
        True if standby, False if primary
    """
    try:
        query = "SELECT pg_is_in_recovery();"
        _, raw = connector.execute_query(query, return_raw=True)

        # Raw result is a list of dictionaries
        if isinstance(raw, list) and len(raw) > 0:
            # Get first row
            row = raw[0]
            if isinstance(row, dict):
                # Dictionary format: {'pg_is_in_recovery': True/False}
                return bool(row.get('pg_is_in_recovery', False))
            elif isinstance(row, (list, tuple)) and len(row) > 0:
                # Tuple/list format: (True/False,)
                return bool(row[0])

        return False
    except Exception as e:
        # If we can't determine, assume primary to avoid hiding slot errors
        logger.warning(f"Could not determine if node is standby: {e}")
        return False


def run_replication_health(connector, settings):
    """
    Performs a comprehensive check of physical and logical replication,
    and the health of replication slots.

    Patroni-aware: Adapts behavior based on cluster role (primary/standby)
    and provides context for Patroni-managed environments.

    Args:
        connector: Database connector with environment detection
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder()
    builder.h3("Replication Health Analysis")

    structured_data = {}

    # Detect environment
    is_patroni = (connector.environment == 'patroni')

    # Detect if this is a standby by querying pg_is_in_recovery()
    is_standby = _is_standby_node(connector)
    is_primary = not is_standby

    # Add Patroni context at the top if detected
    if is_patroni:
        builder.note(
            "**Patroni Environment Detected**\n\n"
            "This cluster is managed by Patroni. Basic replication status is shown below.\n\n"
            "For comprehensive topology, failover history, and cluster health, see dedicated Patroni checks:\n\n"
            "• Patroni Topology - cluster-wide replication lag and node status\n"
            "• Patroni Health Status - node health across the cluster\n"
            "• Patroni Failover History - stability and failover patterns"
        )
        builder.blank()

    # Show Node Role
    _add_node_role_section(builder, is_primary, is_standby, is_patroni)

    # Physical Replication Status
    _check_physical_replication(builder, connector, structured_data, is_patroni, is_primary)

    # Replication Slot Health
    _check_replication_slots(builder, connector, structured_data, is_patroni, is_primary)

    # Logical Replication Subscriptions
    _check_logical_subscriptions(builder, connector, structured_data)

    return builder.build(), structured_data


def _add_node_role_section(builder: CheckContentBuilder, is_primary: bool, is_standby: bool, is_patroni: bool):
    """
    Add node role section to clarify which node is being checked.

    Args:
        builder: CheckContentBuilder instance
        is_primary: Whether this is the primary node
        is_standby: Whether this is a standby node
        is_patroni: Whether Patroni is detected
    """
    builder.h4("Node Role")

    if is_primary:
        role = "Primary"
        status = "Read/Write"
    elif is_standby:
        role = "Standby"
        status = "Read-Only (Recovery Mode)"
    else:
        role = "Unknown"
        status = "Unable to determine"

    role_data = [
        {'Attribute': 'Role', 'Value': role},
        {'Attribute': 'Status', 'Value': status}
    ]

    if is_patroni:
        role_data.append({'Attribute': 'Management', 'Value': 'Patroni'})

    builder.table(role_data)
    builder.blank()


def _check_wal_receiver(
    builder: CheckContentBuilder,
    connector,
    structured_data: dict,
    is_patroni: bool
):
    """
    Check WAL receiver status on standby nodes (incoming replication).

    Args:
        builder: CheckContentBuilder instance
        connector: Database connector
        structured_data: Dictionary to store structured findings
        is_patroni: Whether Patroni is detected
    """
    try:
        query = get_wal_receiver_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        # Check for errors
        if "[ERROR]" in formatted:
            builder.text("**Incoming Replication from Primary:**")
            builder.blank()
            builder.text(formatted)
            structured_data["wal_receiver_status"] = {"status": "error", "data": None}
            return

        # No WAL receiver running
        if not raw:
            builder.warning(
                "**Incoming Replication**: No WAL receiver process is running.\n\n"
                "This standby is not receiving replication from a primary. "
                "This may indicate a configuration issue or broken replication."
            )
            structured_data["wal_receiver_status"] = {"status": "success", "data": []}
        else:
            # Show WAL receiver status
            wal_receiver = raw[0] if isinstance(raw, list) else raw
            status = wal_receiver.get('status', 'unknown')

            builder.text("**Incoming Replication from Primary:**")
            builder.blank()

            # Check replication health
            if status == 'streaming':
                last_msg_age = wal_receiver.get('last_msg_age_seconds')
                if last_msg_age is not None and last_msg_age < 30:
                    builder.text("✓ Replication is **healthy** and actively streaming from primary.")
                elif last_msg_age is not None and last_msg_age >= 30:
                    builder.warning(
                        f"**Replication Delay Warning**\n\n"
                        f"Last message received {last_msg_age:.1f} seconds ago. "
                        f"Replication may be lagging or stalled."
                    )
                else:
                    builder.text("✓ Replication status: **streaming**")
            else:
                builder.warning(f"**Replication Status**: {status}\n\nReplication is not in streaming mode.")

            builder.blank()
            builder.text(formatted)
            structured_data["wal_receiver_status"] = {"status": "success", "data": raw}

    except Exception as e:
        builder.critical_issue(
            "WAL Receiver Check Failed",
            [f"Could not analyze WAL receiver status: {str(e)}"]
        )
        structured_data["wal_receiver_status"] = {"status": "error", "error": str(e)}


def _check_physical_replication(
    builder: CheckContentBuilder,
    connector,
    structured_data: dict,
    is_patroni: bool,
    is_primary: bool
):
    """
    Check physical streaming replication status.

    Args:
        builder: CheckContentBuilder instance
        connector: Database connector
        structured_data: Dictionary to store structured findings
        is_patroni: Whether Patroni is detected
        is_primary: Whether this is the primary node
    """
    builder.h4("Physical Replication (Streaming)")

    try:
        # On standby nodes, first show incoming replication (WAL receiver)
        if not is_primary:
            _check_wal_receiver(builder, connector, structured_data, is_patroni)
            builder.blank()

        # Then show outgoing replication (pg_stat_replication)
        # On primary: shows replication to standbys
        # On standby: shows cascading replication (if any)
        query = get_physical_replication_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        # Check for errors
        if "[ERROR]" in formatted:
            builder.text(formatted)
            structured_data["physical_replication_status"] = {"status": "error", "data": None}
            return

        # No active replication
        if not raw:
            if is_primary:
                builder.note("**Outgoing Replication**: No standbys are currently connected.")
            else:
                builder.note("**Cascading Replication**: This standby is not replicating to other standbys.")
            structured_data["physical_replication_status"] = {"status": "success", "data": []}
        else:
            # Show replication status
            if is_primary:
                builder.text("**Outgoing Replication to Standbys:**")
            else:
                builder.text("**Cascading Replication to Other Standbys:**")
            builder.blank()
            builder.warning(
                "**Review Replication Lag**\n\n"
                "Significant replication lag can indicate network issues, resource contention, "
                "or heavy write workload."
            )
            builder.blank()
            builder.text(formatted)
            structured_data["physical_replication_status"] = {"status": "success", "data": raw}

        # Add Patroni context
        if is_patroni:
            builder.blank()
            builder.tip(
                "**Patroni Manages Replication Setup**\n\n"
                "Patroni automatically configures streaming replication between nodes.\n\n"
                "For detailed cluster-wide lag analysis and failover readiness, "
                "see the **Patroni Topology** check."
            )

        builder.blank()

    except Exception as e:
        builder.critical_issue(
            "Physical Replication Check Failed",
            [f"Could not analyze physical replication: {str(e)}"]
        )
        structured_data["physical_replication_status"] = {"status": "error", "error": str(e)}


def _check_replication_slots(
    builder: CheckContentBuilder,
    connector,
    structured_data: dict,
    is_patroni: bool,
    is_primary: bool
):
    """
    Check replication slot health.

    Args:
        builder: CheckContentBuilder instance
        connector: Database connector
        structured_data: Dictionary to store structured findings
        is_patroni: Whether Patroni is detected
        is_primary: Whether this is the primary node
    """
    builder.h4("Replication Slot Health")

    try:
        query = get_replication_slots_query(connector)
        formatted, raw_slots = connector.execute_query(query, return_raw=True)

        # Check if we hit "recovery is in progress" error
        if "[ERROR]" in formatted and "recovery is in progress" in formatted.lower():
            _handle_standby_slot_query(builder, is_patroni)
            structured_data["replication_slots_summary"] = {"status": "skipped", "reason": "standby_node"}
            return

        # Check for other errors
        if not isinstance(raw_slots, list):
            if "[ERROR]" in formatted:
                builder.text(formatted)
            raw_slots = []
            structured_data["replication_slots_summary"] = {"status": "error", "data": None}
            return

        # No slots found
        if not raw_slots:
            builder.note("No replication slots found.")
            if is_patroni and is_primary:
                builder.blank()
                builder.text("_Note: Patroni may create replication slots with names like `patroni_*` for cluster management._")
            structured_data["replication_slots_summary"] = {"status": "success", "data": {"total_slots": 0, "inactive_slots_count": 0}}
            structured_data["replication_slots_details"] = {"status": "success", "data": []}
            builder.blank()
            return

        # Show slots
        builder.text(formatted)
        builder.blank()

        # Analyze inactive slots
        inactive_slots = [s for s in raw_slots if isinstance(s, dict) and not s.get('active')]
        summary = {"total_slots": len(raw_slots), "inactive_slots_count": len(inactive_slots)}
        structured_data["replication_slots_summary"] = {"status": "success", "data": summary}
        structured_data["replication_slots_details"] = {"status": "success", "data": raw_slots}

        if inactive_slots:
            _handle_inactive_slots(builder, inactive_slots, is_patroni)

        # Add Patroni context
        if is_patroni:
            builder.blank()
            builder.note(
                "**Patroni Slot Management**\n\n"
                "Patroni may create its own replication slots (prefixed with `patroni_`).\n\n"
                "These are automatically managed and should not be manually modified or dropped."
            )

        builder.blank()

    except Exception as e:
        builder.critical_issue(
            "Replication Slot Check Failed",
            [f"Could not analyze replication slots: {str(e)}"]
        )
        structured_data["replication_slots_summary"] = {"status": "error", "error": str(e)}


def _handle_standby_slot_query(builder: CheckContentBuilder, is_patroni: bool):
    """
    Handle replication slot query on standby node.

    Args:
        builder: CheckContentBuilder instance
        is_patroni: Whether Patroni is detected
    """
    if is_patroni:
        builder.note(
            "**Replication Slots on Standby**\n\n"
            "Replication slot queries are only available on the primary node.\n\n"
            "This node is a standby in recovery mode. Patroni manages replication slots on the primary.\n\n"
            "To inspect slots, see the **Patroni Topology** check which queries all nodes in the cluster."
        )
    else:
        builder.note(
            "**Replication Slots on Standby**\n\n"
            "Replication slot queries are only available on the primary node.\n\n"
            "This node is a standby in recovery mode. To inspect replication slots, "
            "run this check against the primary node."
        )


def _handle_inactive_slots(builder: CheckContentBuilder, inactive_slots: list, is_patroni: bool):
    """
    Handle inactive replication slots warning.

    Args:
        builder: CheckContentBuilder instance
        inactive_slots: List of inactive slot dictionaries
        is_patroni: Whether Patroni is detected
    """
    slot_names = [s.get('slot_name', 'unknown') for s in inactive_slots]
    patroni_slots = [name for name in slot_names if 'patroni' in name.lower()]
    other_slots = [name for name in slot_names if 'patroni' not in name.lower()]

    if other_slots:
        # Critical issue for non-Patroni slots
        details = [
            f"**{len(other_slots)} inactive replication slot(s) found:**",
            "",
            "Inactive slots: " + ", ".join(f"`{s}`" for s in other_slots),
            "",
            "**Impact:** Inactive replication slots prevent PostgreSQL from removing old WAL files. "
            "This will eventually fill the disk and cause an outage.",
            "",
            "**Action Required:** Drop any unused replication slots immediately:",
            "",
            "```sql",
            "-- Verify slot is unused before dropping",
            f"SELECT * FROM pg_replication_slots WHERE slot_name = '<slot_name>';",
            "",
            "-- Drop unused slot",
            "SELECT pg_drop_replication_slot('<slot_name>');",
            "```"
        ]
        builder.critical_issue("Inactive Replication Slots Detected", details)

    if patroni_slots and is_patroni:
        # Info note for Patroni-managed slots
        builder.blank()
        builder.note(
            f"**Patroni-Managed Slots:** {len(patroni_slots)} inactive Patroni slot(s) detected: "
            + ", ".join(f"`{s}`" for s in patroni_slots) +
            "\n\nThese are managed by Patroni. Inactive Patroni slots may indicate:\n\n"
            "• A node that was removed from the cluster\n"
            "• A temporary network issue\n"
            "• A standby that is being rebuilt\n\n"
            "Check the **Patroni Topology** and **Patroni DCS Health** checks for cluster status."
        )


def _check_logical_subscriptions(
    builder: CheckContentBuilder,
    connector,
    structured_data: dict
):
    """
    Check logical replication subscription status.

    Args:
        builder: CheckContentBuilder instance
        connector: Database connector
        structured_data: Dictionary to store structured findings
    """
    try:
        query = get_subscription_stats_query(connector)
        if not query:
            # Logical replication not supported in this version
            return

        builder.h4("Logical Subscription Status")
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            builder.text(formatted)
            structured_data["logical_subscription_status"] = {"status": "error", "data": None}
        elif not raw:
            builder.note("No logical replication subscriptions found.")
            structured_data["logical_subscription_status"] = {"status": "success", "data": []}
        else:
            builder.text(formatted)
            structured_data["logical_subscription_status"] = {"status": "success", "data": raw}

        builder.blank()

    except Exception as e:
        builder.critical_issue(
            "Logical Subscription Check Failed",
            [f"Could not analyze logical subscriptions: {str(e)}"]
        )
        structured_data["logical_subscription_status"] = {"status": "error", "error": str(e)}

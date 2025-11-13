"""
Kafka User Permissions Check

Validates the authenticated user's ACL permissions to determine what operations
are available for health checks. This helps identify why certain checks may fail
or return incomplete results.

Checks:
- Topic permissions (LIST, DESCRIBE, READ)
- Consumer Group permissions (DESCRIBE, READ)
- Cluster permissions (DESCRIBE, ALTER_CONFIGS)
- What health check features are available with current permissions
"""

from plugins.common.check_helpers import CheckContentBuilder
import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 10  # Critical - explains why other checks may fail


def run(connector, settings):
    """
    Check what permissions the authenticated user has.

    Args:
        connector: Kafka connector instance
        settings: Configuration settings

    Returns:
        tuple: (asciidoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("User Permissions Check")

    findings = {
        'status': 'success',
        'authenticated_user': settings.get('sasl_username', 'unknown'),
        'permissions': {},
        'limitations': [],
        'available_checks': [],
        'unavailable_checks': []
    }

    try:
        # Test various operations to determine permissions
        permissions = _test_permissions(connector, settings)
        findings['permissions'] = permissions

        # Determine what's available vs limited
        limitations, available_checks, unavailable_checks = _analyze_capabilities(permissions)
        findings['limitations'] = limitations
        findings['available_checks'] = available_checks
        findings['unavailable_checks'] = unavailable_checks

        # Build report
        _build_report(builder, findings)

    except Exception as e:
        logger.error(f"Error checking permissions: {e}", exc_info=True)
        builder.error(f"Failed to check permissions: {e}")
        findings['status'] = 'error'
        findings['error'] = str(e)

    return builder.build(), {'user_permissions': findings}


def _test_permissions(connector, settings):
    """
    Test various Kafka operations to determine user permissions.

    Returns:
        dict: Permissions test results
    """
    permissions = {
        'topics': {
            'list': False,
            'describe': False,
            'describe_configs': False
        },
        'consumer_groups': {
            'list': False,
            'describe': False
        },
        'cluster': {
            'describe': False
        }
    }

    # Test 1: LIST topics
    try:
        topics = connector.admin_client.list_topics()
        permissions['topics']['list'] = True
        permissions['topics']['topic_count'] = len(topics)
        permissions['topics']['user_topic_count'] = len([t for t in topics if not t.startswith('__')])
        logger.info(f"✅ LIST topics permission confirmed ({len(topics)} topics)")
    except Exception as e:
        logger.warning(f"❌ LIST topics failed: {e}")
        permissions['topics']['list_error'] = str(e)

    # Test 2: DESCRIBE topics (via cluster metadata)
    try:
        cluster = connector.admin_client._client.cluster
        cluster.request_update()
        described_topics = list(cluster.topics(exclude_internal_topics=False))
        permissions['topics']['describe'] = len(described_topics) > 0
        permissions['topics']['described_topic_count'] = len(described_topics)

        if len(described_topics) > 0:
            logger.info(f"✅ DESCRIBE topics permission confirmed ({len(described_topics)} topics)")
        else:
            logger.warning(f"⚠️  DESCRIBE topics appears limited (0 topics returned, but {permissions['topics'].get('topic_count', 0)} exist)")
    except Exception as e:
        logger.warning(f"❌ DESCRIBE topics failed: {e}")
        permissions['topics']['describe'] = False
        permissions['topics']['describe_error'] = str(e)

    # Test 3: DESCRIBE topic configs
    if permissions['topics']['list'] and permissions['topics']['topic_count'] > 0:
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            topics = connector.admin_client.list_topics()
            # Try to describe first non-internal topic
            test_topic = next((t for t in topics if not t.startswith('__')), None)

            if test_topic:
                config_resource = ConfigResource(ConfigResourceType.TOPIC, test_topic)
                configs = connector.admin_client.describe_configs([config_resource])
                permissions['topics']['describe_configs'] = True
                logger.info(f"✅ DESCRIBE_CONFIGS topics permission confirmed")
        except Exception as e:
            logger.warning(f"❌ DESCRIBE_CONFIGS topics failed: {e}")
            permissions['topics']['describe_configs'] = False
            permissions['topics']['describe_configs_error'] = str(e)

    # Test 4: LIST consumer groups
    try:
        groups = connector.admin_client.list_consumer_groups()
        permissions['consumer_groups']['list'] = True
        permissions['consumer_groups']['group_count'] = len(groups)
        logger.info(f"✅ LIST consumer groups permission confirmed ({len(groups)} groups)")
    except Exception as e:
        logger.warning(f"❌ LIST consumer groups failed: {e}")
        permissions['consumer_groups']['list_error'] = str(e)

    # Test 5: DESCRIBE consumer groups
    if permissions['consumer_groups']['list'] and permissions['consumer_groups']['group_count'] > 0:
        try:
            groups = connector.admin_client.list_consumer_groups()
            if groups:
                test_group = groups[0][0] if isinstance(groups[0], tuple) else groups[0]
                descriptions = connector.admin_client.describe_consumer_groups([test_group])
                permissions['consumer_groups']['describe'] = True
                logger.info(f"✅ DESCRIBE consumer groups permission confirmed")
        except Exception as e:
            logger.warning(f"❌ DESCRIBE consumer groups failed: {e}")
            permissions['consumer_groups']['describe'] = False
            permissions['consumer_groups']['describe_error'] = str(e)

    # Test 6: DESCRIBE cluster
    try:
        cluster = connector.admin_client._client.cluster
        cluster.request_update()
        brokers = list(cluster.brokers())
        permissions['cluster']['describe'] = len(brokers) > 0
        permissions['cluster']['broker_count'] = len(brokers)
        logger.info(f"✅ DESCRIBE cluster permission confirmed ({len(brokers)} brokers)")
    except Exception as e:
        logger.warning(f"❌ DESCRIBE cluster failed: {e}")
        permissions['cluster']['describe_error'] = str(e)

    return permissions


def _analyze_capabilities(permissions):
    """
    Analyze what health check features are available based on permissions.

    Returns:
        tuple: (limitations, available_checks, unavailable_checks)
    """
    limitations = []
    available_checks = []
    unavailable_checks = []

    # Topic-related checks
    if permissions['topics']['list']:
        available_checks.append("Topic count and naming")
    else:
        unavailable_checks.append("Topic count and naming")
        limitations.append("Cannot list topics - all topic-based checks unavailable")

    if permissions['topics']['describe']:
        available_checks.append("In-Sync Replica (ISR) health")
        available_checks.append("Under-replicated partitions")
        available_checks.append("Partition distribution")
    else:
        unavailable_checks.append("In-Sync Replica (ISR) health")
        unavailable_checks.append("Under-replicated partitions")
        unavailable_checks.append("Partition distribution")

        if permissions['topics']['list']:
            # Has LIST but not DESCRIBE
            limitations.append(
                f"Can list topics ({permissions['topics'].get('topic_count', 0)} found) "
                f"but cannot describe them - DESCRIBE permission required for replication checks"
            )

    if permissions['topics']['describe_configs']:
        available_checks.append("Topic configuration analysis")
    else:
        unavailable_checks.append("Topic configuration analysis")
        limitations.append("Cannot describe topic configs - DESCRIBE_CONFIGS permission required")

    # Consumer group checks
    if permissions['consumer_groups']['list']:
        available_checks.append("Consumer group listing")
    else:
        unavailable_checks.append("Consumer group listing")
        limitations.append("Cannot list consumer groups")

    if permissions['consumer_groups']['describe']:
        available_checks.append("Consumer lag analysis")
    else:
        unavailable_checks.append("Consumer lag analysis")
        if permissions['consumer_groups']['list']:
            limitations.append(
                f"Can list consumer groups ({permissions['consumer_groups'].get('group_count', 0)} found) "
                f"but cannot describe them - DESCRIBE permission required for lag analysis"
            )

    # Cluster checks
    if permissions['cluster']['describe']:
        available_checks.append("Cluster overview")
        available_checks.append("Broker information")
    else:
        unavailable_checks.append("Cluster overview")
        unavailable_checks.append("Broker information")
        limitations.append("Cannot describe cluster - basic cluster info may be limited")

    return limitations, available_checks, unavailable_checks


def _build_report(builder, findings):
    """Build the AsciiDoc report content."""

    permissions = findings['permissions']
    limitations = findings['limitations']
    available_checks = findings['available_checks']
    unavailable_checks = findings['unavailable_checks']
    user = findings['authenticated_user']

    # Summary
    if len(limitations) == 0:
        builder.success(f"✅ User '{user}' has all required permissions for comprehensive health checks")
    elif len(available_checks) > len(unavailable_checks):
        builder.warning(f"⚠️ User '{user}' has partial permissions - some checks will be limited")
    else:
        builder.critical(f"🚫 User '{user}' has insufficient permissions - most checks unavailable")

    builder.blank()

    # Permission Details
    builder.text("*Permission Test Results:*")
    builder.blank()

    # Topics
    builder.text("**Topics:**")
    builder.text(f"• LIST: {_format_permission(permissions['topics']['list'])}")
    if permissions['topics']['list']:
        builder.text(f"  - Found {permissions['topics'].get('topic_count', 0)} total topics "
                    f"({permissions['topics'].get('user_topic_count', 0)} user topics)")

    builder.text(f"• DESCRIBE: {_format_permission(permissions['topics']['describe'])}")
    if permissions['topics']['describe']:
        builder.text(f"  - Can describe {permissions['topics'].get('described_topic_count', 0)} topics")
    elif permissions['topics']['list'] and permissions['topics'].get('topic_count', 0) > 0:
        builder.text(f"  - ⚠️ {permissions['topics'].get('topic_count', 0)} topics exist but cannot be described")

    builder.text(f"• DESCRIBE_CONFIGS: {_format_permission(permissions['topics']['describe_configs'])}")
    builder.blank()

    # Consumer Groups
    builder.text("**Consumer Groups:**")
    builder.text(f"• LIST: {_format_permission(permissions['consumer_groups']['list'])}")
    if permissions['consumer_groups']['list']:
        builder.text(f"  - Found {permissions['consumer_groups'].get('group_count', 0)} consumer groups")

    builder.text(f"• DESCRIBE: {_format_permission(permissions['consumer_groups']['describe'])}")
    builder.blank()

    # Cluster
    builder.text("**Cluster:**")
    builder.text(f"• DESCRIBE: {_format_permission(permissions['cluster']['describe'])}")
    if permissions['cluster']['describe']:
        builder.text(f"  - Can see {permissions['cluster'].get('broker_count', 0)} brokers")
    builder.blank()

    # Limitations
    if limitations:
        builder.text("*Limitations Detected:*")
        builder.blank()
        for i, limitation in enumerate(limitations, 1):
            builder.text(f"{i}. {limitation}")
        builder.blank()

    # Available Checks
    if available_checks:
        builder.text(f"*Available Health Checks ({len(available_checks)}):*")
        builder.blank()
        for check in sorted(available_checks):
            builder.text(f"✅ {check}")
        builder.blank()

    # Unavailable Checks
    if unavailable_checks:
        builder.text(f"*Unavailable Health Checks ({len(unavailable_checks)}):*")
        builder.blank()
        for check in sorted(unavailable_checks):
            builder.text(f"❌ {check}")
        builder.blank()

    # Recommendations
    if limitations:
        builder.text("*Recommendations:*")
        builder.blank()
        builder.text("To enable full health check capabilities, grant the following ACLs:")
        builder.blank()

        if not permissions['topics']['describe']:
            builder.text("```bash")
            builder.text(f"# Grant DESCRIBE permission on all topics")
            builder.text(f"kafka-acls --bootstrap-server <broker> \\")
            builder.text(f"  --add --allow-principal User:{user} \\")
            builder.text(f"  --operation DESCRIBE --topic '*'")
            builder.text("```")
            builder.blank()

        if not permissions['topics']['describe_configs']:
            builder.text("```bash")
            builder.text(f"# Grant DESCRIBE_CONFIGS permission on all topics")
            builder.text(f"kafka-acls --bootstrap-server <broker> \\")
            builder.text(f"  --add --allow-principal User:{user} \\")
            builder.text(f"  --operation DESCRIBE_CONFIGS --topic '*'")
            builder.text("```")
            builder.blank()

        if not permissions['consumer_groups']['describe']:
            builder.text("```bash")
            builder.text(f"# Grant DESCRIBE permission on all consumer groups")
            builder.text(f"kafka-acls --bootstrap-server <broker> \\")
            builder.text(f"  --add --allow-principal User:{user} \\")
            builder.text(f"  --operation DESCRIBE --group '*'")
            builder.text("```")
            builder.blank()

        builder.text("*Note:* For Instaclustr managed clusters, configure ACLs via the Instaclustr console → Access Control.")


def _format_permission(has_permission):
    """Format permission status with emoji."""
    return "✅ Allowed" if has_permission else "❌ Denied"

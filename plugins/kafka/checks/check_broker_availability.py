from plugins.common.check_helpers import CheckContentBuilder
from plugins.kafka.utils.qrylib.cluster_metadata_queries import get_cluster_metadata_query
from plugins.kafka.utils.qrylib.describe_topics_queries import get_describe_topics_query
import logging

logger = logging.getLogger(__name__)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 10  # Critical: service availability threats

def run_check_broker_availability(connector, settings):
    """
    Performs the broker availability health check.
    
    Checks broker availability by examining cluster metadata and comparing
    configured brokers against those responding to API calls.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    try:
        builder.h3("Broker Availability")
        
        # Get list of all brokers from cluster metadata
        metadata_query = get_cluster_metadata_query(connector)
        formatted, raw = connector.execute_query(metadata_query, return_raw=True)
        
        # Debug logging
        logger.info(f"Broker availability check - metadata query result type: {type(raw)}")
        logger.info(f"Broker availability check - metadata raw: {raw}")
        
        if "[ERROR]" in formatted or not raw or isinstance(raw, dict) and 'error' in raw:
            builder.add(formatted)
            structured_data["broker_availability"] = {"status": "error", "data": [], "details": str(raw)}
            return builder.build(), structured_data
        
        # Extract broker info
        if isinstance(raw, dict):
            brokers = raw.get('brokers', [])
            controller_id = raw.get('controller_id', -1)
            cluster_id = raw.get('cluster_id', 'Unknown')
        else:
            builder.error(f"Unexpected metadata format: {type(raw)}")
            structured_data["broker_availability"] = {
                "status": "error",
                "details": f"Expected dict, got {type(raw).__name__}"
            }
            return builder.build(), structured_data
        
        if not brokers:
            builder.warning("No brokers found in cluster metadata. This may indicate a connection or API issue.")
            structured_data["broker_availability"] = {
                "status": "success",
                "data": [],
                "count": 0,
                "available_count": 0,
                "unavailable_count": 0,
                "controller_id": controller_id,
                "controller_available": False,
                "under_replicated_partitions": 0
            }
            return builder.build(), structured_data
        
        all_broker_ids = {b['id'] for b in brokers}
        
        # Get topic descriptions for under-replicated partition info
        topics_query = get_describe_topics_query(connector)
        topics_formatted, topics_raw = connector.execute_query(topics_query, return_raw=True)
        
        # Count under-replicated partitions
        total_under_replicated = 0
        if isinstance(topics_raw, list):
            for topic_info in topics_raw:
                total_under_replicated += topic_info.get('under_replicated_partitions', 0)
        
        # Use cluster internal API to find which brokers are actually responding
        cluster = connector.admin_client._client.cluster
        cluster.request_update()  # Force metadata refresh
        
        # Get all broker nodes known to the cluster
        cluster_broker_nodes = cluster.brokers()
        available_broker_ids = {node.nodeId for node in cluster_broker_nodes}
        
        # Compare configured brokers vs available brokers
        brokers_data = []
        unavailable = []
        
        for b in brokers:
            broker_id = b['id']
            host = b['host']
            port = b['port']
            
            # Check if broker is in the live broker list
            if broker_id in available_broker_ids:
                status = "available"
            else:
                status = "unavailable"
                unavailable.append(broker_id)
            
            brokers_data.append({
                "broker_id": broker_id,
                "host": host,
                "port": port,
                "status": status
            })
        
        # Check controller availability
        controller_broker = next((d for d in brokers_data if d['broker_id'] == controller_id), None)
        controller_available = controller_broker and controller_broker['status'] == 'available' if controller_id != -1 else False
        
        # === INTERPRET FACTS ===
        
        if unavailable:
            builder.critical_issue(
                f"Broker Unavailability Detected",
                {
                    "Unavailable Brokers": ', '.join(map(str, unavailable)),
                    "Count": f"{len(unavailable)} of {len(brokers)} brokers",
                    "Impact": f"{total_under_replicated} under-replicated partitions" if total_under_replicated > 0 else "Checking ISR status..."
                }
            )
        
        if controller_id != -1 and not controller_available:
            builder.critical_issue(
                "Controller Broker Unavailable",
                {
                    "Controller ID": controller_id,
                    "Impact": "Cluster operations like topic creation and partition reassignment will fail"
                }
            )
        
        if not unavailable and controller_available:
            builder.success(
                "All configured brokers are available and responding.\n\n"
                f"Cluster: {cluster_id}, Brokers: {len(brokers)}, Controller: Broker {controller_id}"
            )
        
        # Show broker status table
        builder.h4("Broker Status")
        
        broker_rows = []
        for d in sorted(brokers_data, key=lambda x: x['broker_id']):
            status_icon = "✅" if d['status'] == 'available' else "❌"
            is_controller = " (Controller)" if d['broker_id'] == controller_id else ""
            
            broker_rows.append({
                "Status": status_icon,
                "Broker ID": d['broker_id'],
                "Host": d['host'],
                "Port": d['port'],
                "Role": f"{d['status'].title()}{is_controller}"
            })
        
        builder.table(broker_rows)
        
        # Show under-replication impact if relevant
        if unavailable and total_under_replicated > 0:
            builder.para(f"**Impact:** {total_under_replicated} partition(s) are under-replicated due to broker unavailability.")
        
        # Recommendations - only if issues detected
        if unavailable or not controller_available:
            recommendations = {}
            
            if unavailable:
                recommendations["critical"] = [
                    "**Immediately investigate unavailable brokers** - Check system status and logs",
                    "**Verify broker process is running** - SSH to broker and check `systemctl status kafka` or equivalent",
                    "**Check broker logs** - Look in /var/log/kafka or kafka/logs/ for errors",
                    "**Common causes to investigate:**",
                    "  - Process crashed or was killed (OOM, disk full)",
                    "  - Network connectivity issues",
                    "  - Disk I/O problems or disk full",
                    "  - JVM heap exhaustion or GC issues",
                    "**Restart broker if needed** - After fixing root cause"
                ]
            
            if not controller_available:
                if "critical" not in recommendations:
                    recommendations["critical"] = []
                recommendations["critical"].extend([
                    "**Controller unavailable** - Cluster cannot perform admin operations",
                    "**If controller broker is down** - Follow broker restart procedures above",
                    "**Kafka will elect new controller** - Once broker is back online"
                ])
            
            recommendations["general"] = [
                "Set up alerts for broker status changes to catch issues immediately",
                "Monitor broker JVM metrics (heap usage, GC frequency)",
                "Monitor disk space on all broker hosts (alert at 70%)",
                "Implement automated health checks for broker processes",
                "Document broker restart procedures for on-call teams",
                "Keep broker logs in centralized logging system for faster debugging"
            ]
            
            builder.recs(recommendations)
        
        # === STRUCTURED DATA ===
        structured_data["broker_availability"] = {
            "status": "success",
            "cluster_id": cluster_id,
            "data": brokers_data,
            "count": len(brokers),
            "available_count": len(brokers) - len(unavailable),
            "unavailable_count": len(unavailable),
            "unavailable_broker_ids": unavailable,
            "controller_id": controller_id,
            "controller_available": controller_available,
            "under_replicated_partitions": total_under_replicated
        }
    
    except Exception as e:
        import traceback
        logger.error(f"Broker availability check failed: {e}\n{traceback.format_exc()}")
        
        builder.error(f"Check failed: {e}")
        structured_data["broker_availability"] = {
            "status": "error",
            "details": str(e)
        }
    
    return builder.build(), structured_data

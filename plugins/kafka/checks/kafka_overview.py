"""
Kafka Overview Check
Provides high-level information about the Kafka cluster including version, broker count, and cluster ID.
"""

import logging

logger = logging.getLogger(__name__)


def get_weight():
    """Returns the importance score for this module."""
    return 10


def run_kafka_overview(connector, settings):
    """
    Provides an overview of the Kafka cluster, including version, broker count, 
    cluster ID, and controller information.
    """
    adoc_content = [
        "=== Kafka Cluster Overview",
        "Provides a high-level overview of the Kafka cluster."
    ]
    structured_data = {}
    
    checks_to_run = [
        ("Cluster Information", _get_cluster_info, "version_info"),
        ("Broker Details", _get_broker_details, "broker_info"),
        ("Controller Status", _get_controller_info, "controller_info")
    ]
    
    for title, check_func, data_key in checks_to_run:
        try:
            adoc_content.append(f"\n==== {title}")
            result_adoc, result_data = check_func(connector, settings)
            adoc_content.append(result_adoc)
            structured_data[data_key] = {"status": "success", "data": result_data}
            
        except Exception as e:
            error_msg = f"\n[ERROR]\n====\nCould not execute check for '{title}': {e}\n====\n"
            logger.error(f"Check '{title}' failed: {e}", exc_info=True)
            adoc_content.append(error_msg)
            structured_data[data_key] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data


def _get_cluster_info(connector, settings):
    """Gets cluster-level metadata including version, cluster ID, and broker count."""
    try:
        cluster_metadata = connector.admin_client.describe_cluster()
        
        # Extract version
        version = _detect_kafka_version(connector, cluster_metadata)
        
        brokers = cluster_metadata.get('brokers', [])
        broker_count = len(brokers)
        cluster_id = cluster_metadata.get('cluster_id', 'N/A')
        
        # Controller is just an integer in the response
        controller_id = cluster_metadata.get('controller_id', 'N/A')
        
        result_data = [{
            'version': version,
            'cluster_id': cluster_id,
            'broker_count': broker_count,
            'controller_id': controller_id
        }]
        
        # Format as AsciiDoc table
        adoc = ["\n[cols=\"1,3\"]", "|==="]
        adoc.append(f"| Kafka Version | {version}")
        adoc.append(f"| Cluster ID | {cluster_id}")
        adoc.append(f"| Broker Count | {broker_count}")
        adoc.append(f"| Controller | Broker {controller_id}")
        adoc.append("|===")
        
        return "\n".join(adoc), result_data
        
    except Exception as e:
        logger.error(f"_get_cluster_info failed: {e}", exc_info=True)
        return f"[ERROR] Could not fetch cluster info: {e}", []


def _get_broker_details(connector, settings):
    """Gets detailed information about each broker."""
    try:
        cluster_metadata = connector.admin_client.describe_cluster()
        
        brokers = []
        for node in cluster_metadata.get('brokers', []):
            # KRaft kafka-python uses 'node_id' field
            broker_info = {
                'broker_id': node.get('node_id', node.get('id', node.get('broker_id', 'N/A'))),
                'host': node.get('host', 'N/A'),
                'port': node.get('port', 'N/A'),
                'rack': node.get('rack') or 'default'
            }
            brokers.append(broker_info)
        
        result_data = brokers
        
        if brokers:
            adoc = ["\n.Broker List"]
            adoc.append("[cols=\"1,2,1,1\"]")
            adoc.append("|===")
            adoc.append("| Broker ID | Host | Port | Rack")
            for broker in brokers:
                adoc.append(f"| {broker['broker_id']} | {broker['host']} | {broker['port']} | {broker['rack']}")
            adoc.append("|===")
            return "\n".join(adoc), result_data
        else:
            return "\n[WARNING] No brokers found in cluster", []
        
    except Exception as e:
        logger.error(f"_get_broker_details failed: {e}", exc_info=True)
        return f"[ERROR] Could not fetch broker details: {e}", []


def _get_controller_info(connector, settings):
    """Gets information about the current controller."""
    try:
        cluster_metadata = connector.admin_client.describe_cluster()
        
        # In KRaft, controller_id is just an integer
        controller_id = cluster_metadata.get('controller_id')
        
        if controller_id is not None:
            # Find the broker with this ID to get host/port
            brokers = cluster_metadata.get('brokers', [])
            controller_broker = next((b for b in brokers if b.get('node_id') == controller_id), None)
            
            if controller_broker:
                result_data = [{
                    'controller_id': controller_id,
                    'controller_host': controller_broker.get('host', 'N/A'),
                    'controller_port': controller_broker.get('port', 'N/A')
                }]
                
                adoc = ["\n[cols=\"1,3\"]", "|==="]
                adoc.append(f"| Controller Broker ID | {controller_id}")
                adoc.append(f"| Host | {controller_broker.get('host')}")
                adoc.append(f"| Port | {controller_broker.get('port')}")
                adoc.append("|===")
            else:
                result_data = [{'controller_id': controller_id, 'status': 'Controller broker not found in broker list'}]
                adoc = [f"\n[NOTE] Controller: Broker {controller_id}"]
        else:
            result_data = [{'status': 'No controller information available'}]
            adoc = ["\n[NOTE] KRaft mode: Controller information not available"]
        
        return "\n".join(adoc), result_data
        
    except Exception as e:
        logger.error(f"_get_controller_info failed: {e}", exc_info=True)
        return f"[ERROR] Could not fetch controller info: {e}", []

def _detect_kafka_version(connector, cluster_metadata):
    """Helper to detect Kafka version using multiple methods."""
    
    # Method 1: Check config.yaml settings (most reliable if configured)
    try:
        if hasattr(connector, 'settings'):
            kafka_version = connector.settings.get('kafka_version') or connector.settings.get('version')
            if kafka_version:
                logger.info(f"Found Kafka version from config.yaml: {kafka_version}")
                return kafka_version
    except Exception as e:
        logger.debug(f"Could not get version from settings: {e}")
    
    # Method 2: Try from broker configs
    try:
        from kafka.admin import ConfigResource, ConfigResourceType
        brokers = cluster_metadata.get('brokers', [])
        
        if brokers:
            broker_id = brokers[0].get('node_id')
            
            if broker_id is not None:
                configs = connector.admin_client.describe_configs([
                    ConfigResource(ConfigResourceType.BROKER, str(broker_id))
                ])
                
                for resource, config in configs.items():
                    for key in ['inter.broker.protocol.version', 'log.message.format.version']:
                        if key in config:
                            version = config[key].value
                            logger.info(f"Found version from broker config[{key}]: {version}")
                            return version
    except Exception as e:
        logger.debug(f"Could not get version from broker configs: {e}")
    
    # Method 3: Try from AdminClient's internal API version
    try:
        if hasattr(connector.admin_client, '_client'):
            client = connector.admin_client._client
            
            if hasattr(client, 'config'):
                api_ver = client.config.get('api_version')
                if api_ver and api_ver != (0, 0, 0):
                    logger.info(f"Found api_version from client config: {api_ver}")
                    if isinstance(api_ver, tuple):
                        return ".".join(map(str, api_ver))
                    return str(api_ver)
    except Exception as e:
        logger.debug(f"Could not get version from client api_version: {e}")
    
    # Method 4: Try via SSH if available
    try:
        if hasattr(connector, 'has_ssh_support') and connector.has_ssh_support():
            brokers = cluster_metadata.get('brokers', [])
            if brokers:
                broker_host = brokers[0].get('host')
                
                result = connector.execute_ssh_command(
                    f"kafka-broker-api-versions --bootstrap-server {broker_host}:9092 2>/dev/null | head -1",
                    broker_host
                )
                
                if result and 'kafka' in result.lower():
                    import re
                    match = re.search(r'(\d+\.\d+\.\d+)', result)
                    if match:
                        version = match.group(1)
                        logger.info(f"Found version from SSH command: {version}")
                        return version
    except Exception as e:
        logger.debug(f"Could not get version via SSH: {e}")
    
    # Fallback
    logger.warning("Could not detect exact Kafka version - configure 'kafka_version' in config.yaml")
    return "Unknown"


def _old_detect_kafka_version(connector, cluster_metadata):
    """Helper to detect Kafka version using multiple methods."""
    
    # Method 1: Try from broker configs (most reliable for exact version)
    try:
        from kafka.admin import ConfigResource, ConfigResourceType
        brokers = cluster_metadata.get('brokers', [])
        
        if brokers:
            # Use node_id field (KRaft format)
            broker_id = brokers[0].get('node_id')
            
            if broker_id is not None:
                configs = connector.admin_client.describe_configs([
                    ConfigResource(ConfigResourceType.BROKER, str(broker_id))
                ])
                
                # Try multiple config keys
                for resource, config in configs.items():
                    for key in ['inter.broker.protocol.version', 'log.message.format.version']:
                        if key in config:
                            version = config[key].value
                            logger.info(f"Found version from broker config[{key}]: {version}")
                            return version
    except Exception as e:
        logger.debug(f"Could not get version from broker configs: {e}")
    
    # Method 2: Try from AdminClient's internal API version
    try:
        if hasattr(connector.admin_client, '_client'):
            client = connector.admin_client._client
            
            # Check for api_version in config
            if hasattr(client, 'config'):
                api_ver = client.config.get('api_version')
                if api_ver and api_ver != (0, 0, 0):
                    logger.info(f"Found api_version from client config: {api_ver}")
                    if isinstance(api_ver, tuple):
                        return ".".join(map(str, api_ver))
                    return str(api_ver)
    except Exception as e:
        logger.debug(f"Could not get version from client api_version: {e}")
    
    # Method 3: Check if connector has SSH access - query broker directly
    try:
        if hasattr(connector, 'has_ssh_support') and connector.has_ssh_support():
            brokers = cluster_metadata.get('brokers', [])
            if brokers:
                # Try to get version from broker server.properties or logs
                broker_host = brokers[0].get('host')
                
                # Execute kafka-broker-api-versions via SSH
                result = connector.execute_ssh_command(
                    f"kafka-broker-api-versions --bootstrap-server {broker_host}:9092 2>/dev/null | head -1",
                    broker_host
                )
                
                if result and 'kafka' in result.lower():
                    # Parse version from output
                    import re
                    match = re.search(r'(\d+\.\d+\.\d+)', result)
                    if match:
                        version = match.group(1)
                        logger.info(f"Found version from SSH command: {version}")
                        return version
    except Exception as e:
        logger.debug(f"Could not get version via SSH: {e}")
    
    # Fallback: Return generic KRaft version
    logger.warning("Could not detect exact Kafka version")
    return "3.x (KRaft)"

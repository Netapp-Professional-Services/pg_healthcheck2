"""KRaft Controller Health Check"""
import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)

def get_weight():
    return 8

def check_prometheus_controller_health(connector, settings):
    builder = CheckContentBuilder()
    builder.h3("Controller Health - KRaft (Prometheus)")
    
    if not settings.get('instaclustr_prometheus_enabled'):
        return builder.build(), {'status': 'skipped', 'reason': 'Prometheus not enabled', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
    
    try:
        from plugins.common.prometheus_client import get_instaclustr_client
        client = get_instaclustr_client(cluster_id=settings['instaclustr_cluster_id'], username=settings['instaclustr_prometheus_username'], api_key=settings['instaclustr_prometheus_api_key'], prometheus_base_url=settings.get('instaclustr_prometheus_base_url'))
        all_metrics = client.scrape_all_nodes()
        
        if not all_metrics:
            builder.error("❌ No metrics")
            return builder.build(), {'status': 'error', 'error_message': 'No metrics', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        active_controllers = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_active_controller_count_kraft$')
        fenced_brokers = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_fenced_broker_count_kraft$')
        metadata_errors = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_metadata_error_count_kraft$')
        
        if not (active_controllers or fenced_brokers or metadata_errors):
            builder.text("ℹ️  Controller metrics not available (cluster may not use KRaft)")
            return builder.build(), {'status': 'info', 'message': 'Not KRaft or metrics unavailable', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        broker_data = {}
        for metric in active_controllers:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            broker_data[node_id] = {'node_id': node_id, 'public_ip': target_labels.get('PublicIp', 'unknown'), 'active_controllers': int(metric['value'])}
        
        for metric in fenced_brokers:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['fenced_brokers'] = int(metric['value'])
        
        for metric in metadata_errors:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['metadata_errors'] = int(metric['value'])
        
        if not broker_data:
            builder.text("ℹ️  No controller data")
            return builder.build(), {'status': 'info', 'message': 'No data', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        node_data = list(broker_data.values())
        total_fenced = sum(b.get('fenced_brokers', 0) for b in node_data)
        total_errors = sum(b.get('metadata_errors', 0) for b in node_data)
        
        if total_fenced > 0 or total_errors > 0:
            status, severity = 'warning', 7
            message = f"⚠️  Controller issues: {total_fenced} fenced brokers, {total_errors} metadata errors"
        else:
            status, severity = 'healthy', 0
            message = f"✅ Controller health good (KRaft mode)"
        
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_controller': {'status': status, 'data': node_data, 'metadata': {'source': 'prometheus', 'broker_count': len(node_data)}},
            'cluster_aggregate': {'total_fenced_brokers': total_fenced, 'total_metadata_errors': total_errors, 'broker_count': len(node_data)}
        }
        
        if status == 'warning':
            builder.warning(message)
        else:
            builder.success(message)
        
        builder.blank()
        builder.text("*Controller Status:*")
        builder.text(f"- Fenced Brokers: {total_fenced}")
        builder.text(f"- Metadata Errors: {total_errors}")
        builder.text(f"- Brokers Monitored: {len(node_data)}")
        
        return builder.build(), findings
    
    except Exception as e:
        logger.error(f"Controller health check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

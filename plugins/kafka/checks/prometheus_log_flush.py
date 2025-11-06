"""Log Flush Performance Check"""
import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)

def get_weight():
    return 7

def check_prometheus_log_flush(connector, settings):
    builder = CheckContentBuilder()
    builder.h3("Log Flush Performance (Prometheus)")
    
    if not settings.get('instaclustr_prometheus_enabled'):
        return builder.build(), {'status': 'skipped', 'reason': 'Prometheus not enabled', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
    
    try:
        from plugins.common.prometheus_client import get_instaclustr_client
        client = get_instaclustr_client(cluster_id=settings['instaclustr_cluster_id'], username=settings['instaclustr_prometheus_username'], api_key=settings['instaclustr_prometheus_api_key'], prometheus_base_url=settings.get('instaclustr_prometheus_base_url'))
        all_metrics = client.scrape_all_nodes()
        
        if not all_metrics:
            builder.error("❌ No metrics")
            return builder.build(), {'status': 'error', 'error_message': 'No metrics', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        flush_rate = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_log_flush_rate(_kraft)?$')
        flush_time = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_log_flush_time(_kraft)?_milliseconds$')
        
        if not (flush_rate or flush_time):
            builder.text("ℹ️  Log flush metrics not available")
            return builder.build(), {'status': 'info', 'message': 'Metrics not available', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        broker_data = {}
        for metric in flush_rate:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id not in broker_data:
                broker_data[node_id] = {'node_id': node_id, 'public_ip': target_labels.get('PublicIp', 'unknown')}
            broker_data[node_id]['flush_rate'] = round(metric['value'], 2)
        
        for metric in flush_time:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['flush_time_ms'] = round(metric['value'], 2)
        
        if not broker_data:
            builder.error("❌ No data")
            return builder.build(), {'status': 'error', 'error_message': 'No data', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        node_data = list(broker_data.values())
        status, severity = 'info', 0
        message = f"Log flush metrics collected from {len(node_data)} brokers"
        
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_flush': {'status': status, 'data': node_data, 'metadata': {'source': 'prometheus', 'broker_count': len(node_data)}},
            'cluster_aggregate': {'avg_flush_rate': round(sum(b.get('flush_rate', 0) for b in node_data) / len(node_data), 2) if node_data else 0, 'broker_count': len(node_data)}
        }
        
        builder.text(f"ℹ️  {message}")
        builder.blank()
        builder.text("*Per-Broker Flush Metrics:*")
        for broker in node_data:
            builder.text(f"- Broker {broker['node_id'][:8]}: Rate {broker.get('flush_rate', 0)}/s, Time {broker.get('flush_time_ms', 0)}ms")
        
        return builder.build(), findings
    
    except Exception as e:
        logger.error(f"Log flush check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

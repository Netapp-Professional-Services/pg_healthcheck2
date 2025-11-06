"""Request Handler Saturation Check"""
import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)

def get_weight():
    return 8

def check_prometheus_request_handler(connector, settings):
    builder = CheckContentBuilder()
    builder.h3("Request Handler Saturation (Prometheus)")
    
    if not settings.get('instaclustr_prometheus_enabled'):
        return builder.build(), {'status': 'skipped', 'reason': 'Prometheus not enabled', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
    
    try:
        from plugins.common.prometheus_client import get_instaclustr_client
        client = get_instaclustr_client(cluster_id=settings['instaclustr_cluster_id'], username=settings['instaclustr_prometheus_username'], api_key=settings['instaclustr_prometheus_api_key'], prometheus_base_url=settings.get('instaclustr_prometheus_base_url'))
        all_metrics = client.scrape_all_nodes()
        
        if not all_metrics:
            builder.error("❌ No metrics")
            return builder.build(), {'status': 'error', 'error_message': 'No metrics', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        handler_idle = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_request_handler_avg_idle_percent$')
        
        if not handler_idle:
            builder.error("❌ Request handler metrics not found")
            return builder.build(), {'status': 'error', 'error_message': 'Metrics not found', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        critical_threshold = settings.get('kafka_handler_idle_critical_pct', 10)
        warning_threshold = settings.get('kafka_handler_idle_warning_pct', 30)
        
        broker_data = {}
        for metric in handler_idle:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            idle_pct = round(metric['value'], 1)
            broker_data[node_id] = {'node_id': node_id, 'public_ip': target_labels.get('PublicIp', 'unknown'), 'handler_idle_pct': idle_pct}
        
        if not broker_data:
            builder.error("❌ No data")
            return builder.build(), {'status': 'error', 'error_message': 'No data', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        node_data = list(broker_data.values())
        critical_brokers = [b for b in node_data if b['handler_idle_pct'] < critical_threshold]
        warning_brokers = [b for b in node_data if critical_threshold <= b['handler_idle_pct'] < warning_threshold]
        
        if critical_brokers:
            status, severity = 'critical', 9
            message = f"🔴 {len(critical_brokers)} broker(s) with critical request handler saturation (<{critical_threshold}% idle)"
        elif warning_brokers:
            status, severity = 'warning', 7
            message = f"⚠️  {len(warning_brokers)} broker(s) with high request handler load (<{warning_threshold}% idle)"
        else:
            status, severity = 'healthy', 0
            message = f"✅ Request handlers healthy across {len(node_data)} brokers"
        
        avg_idle = sum(b['handler_idle_pct'] for b in node_data) / len(node_data)
        
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_handler': {'status': status, 'data': node_data, 'metadata': {'source': 'prometheus', 'broker_count': len(node_data)}},
            'cluster_aggregate': {'avg_handler_idle_pct': round(avg_idle, 1), 'broker_count': len(node_data)}
        }
        
        if status == 'critical':
            builder.critical(message)
            builder.blank()
            builder.text("Low idle % means request handlers are saturated - broker is overloaded")
        elif status == 'warning':
            builder.warning(message)
        else:
            builder.success(message)
        
        builder.blank()
        builder.text("*Cluster Summary:*")
        builder.text(f"- Avg Handler Idle: {round(avg_idle, 1)}%")
        builder.text(f"- Saturated Brokers: {len(critical_brokers)} critical, {len(warning_brokers)} warning")
        
        return builder.build(), findings
    
    except Exception as e:
        logger.error(f"Request handler check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

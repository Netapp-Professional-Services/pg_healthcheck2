"""Replica Fetcher Health Check"""
import logging
from datetime import datetime
from plugins.common.check_helpers import CheckContentBuilder

logger = logging.getLogger(__name__)

def get_weight():
    return 8

def check_prometheus_replica_fetcher(connector, settings):
    builder = CheckContentBuilder()
    builder.h3("Replica Fetcher Health (Prometheus)")
    
    if not settings.get('instaclustr_prometheus_enabled'):
        return builder.build(), {'status': 'skipped', 'reason': 'Prometheus not enabled', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
    
    try:
        from plugins.common.prometheus_client import get_instaclustr_client
        client = get_instaclustr_client(cluster_id=settings['instaclustr_cluster_id'], username=settings['instaclustr_prometheus_username'], api_key=settings['instaclustr_prometheus_api_key'], prometheus_base_url=settings.get('instaclustr_prometheus_base_url'))
        all_metrics = client.scrape_all_nodes()
        
        if not all_metrics:
            builder.error("❌ No metrics")
            return builder.build(), {'status': 'error', 'error_message': 'No metrics', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        failed_parts = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_replica_fetcher_failed_partitions_count$')
        max_lag = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_replica_fetcher_max_lag$')
        min_fetch_rate = client.filter_metrics(all_metrics, name_pattern=r'^ic_node_replica_fetcher_min_fetch_rate$')
        
        if not (failed_parts or max_lag or min_fetch_rate):
            builder.text("ℹ️  Replica fetcher metrics not available")
            return builder.build(), {'status': 'info', 'message': 'Metrics not available', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        broker_data = {}
        for metric in failed_parts:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id not in broker_data:
                broker_data[node_id] = {'node_id': node_id, 'public_ip': target_labels.get('PublicIp', 'unknown')}
            broker_data[node_id]['failed_partitions'] = int(metric['value'])
        
        for metric in max_lag:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['max_lag'] = round(metric['value'], 2)
        
        for metric in min_fetch_rate:
            target_labels = metric.get('target_labels', {})
            node_id = target_labels.get('NodeId', 'unknown')
            if node_id in broker_data:
                broker_data[node_id]['min_fetch_rate'] = round(metric['value'], 2)
        
        if not broker_data:
            builder.error("❌ No data")
            return builder.build(), {'status': 'error', 'error_message': 'No data', 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}
        
        node_data = list(broker_data.values())
        critical_brokers = [b for b in node_data if b.get('failed_partitions', 0) > 10]
        warning_brokers = [b for b in node_data if 0 < b.get('failed_partitions', 0) <= 10]
        
        if critical_brokers:
            status, severity = 'critical', 9
            message = f"🔴 {len(critical_brokers)} broker(s) with critical replica fetcher issues"
        elif warning_brokers:
            status, severity = 'warning', 7
            message = f"⚠️  {len(warning_brokers)} broker(s) with replica fetcher issues"
        else:
            status, severity = 'healthy', 0
            message = f"✅ Replica fetchers healthy across {len(node_data)} brokers"
        
        findings = {
            'status': status,
            'severity': severity,
            'message': message,
            'per_broker_fetcher': {'status': status, 'data': node_data, 'metadata': {'source': 'prometheus', 'broker_count': len(node_data)}},
            'cluster_aggregate': {'total_failed_partitions': sum(b.get('failed_partitions', 0) for b in node_data), 'broker_count': len(node_data)}
        }
        
        if status == 'critical':
            builder.critical(message)
        elif status == 'warning':
            builder.warning(message)
        else:
            builder.success(message)
        
        builder.blank()
        builder.text("*Cluster Summary:*")
        builder.text(f"- Brokers with Issues: {len(critical_brokers) + len(warning_brokers)}/{len(node_data)}")
        builder.text(f"- Total Failed Partitions: {sum(b.get('failed_partitions', 0) for b in node_data)}")
        
        if critical_brokers or warning_brokers:
            builder.blank()
            builder.text("*Brokers with Replica Fetcher Issues:*")
            for broker in critical_brokers + warning_brokers:
                builder.text(f"- Broker {broker['node_id'][:8]}: {broker.get('failed_partitions', 0)} failed partitions, max lag: {broker.get('max_lag', 'N/A')}")
        
        return builder.build(), findings
    
    except Exception as e:
        logger.error(f"Replica fetcher check failed: {e}", exc_info=True)
        builder.error(f"❌ Check failed: {str(e)}")
        return builder.build(), {'status': 'error', 'error_message': str(e), 'data': [], 'metadata': {'source': 'prometheus', 'timestamp': datetime.utcnow().isoformat() + 'Z'}}

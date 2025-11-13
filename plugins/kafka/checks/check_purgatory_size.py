"""
Purgatory Size check for Kafka brokers.

Monitors the size of Fetch and Produce purgatories across all broker nodes.
Purgatory metrics help identify potential performance bottlenecks and consumer/producer lag.

Key Metrics Monitored:
- Fetch Purgatory Size: Number of fetch requests waiting in purgatory
- Produce Purgatory Size: Number of produce requests waiting in purgatory

Fetch Purgatory Context:
Fetch requests are added to purgatory when there is not enough data to fulfill
the request (determined by fetch.min.bytes in consumer configuration). Requests
wait until fetch.wait.max.ms is reached or enough data becomes available.

Produce Purgatory Context:
Produce requests are added to purgatory when request.required.acks is set to -1
or 'all'. Requests wait until the partition leader receives acknowledgement from
all its followers. Growing purgatory size may indicate overloaded replicas.
"""

from plugins.common.check_helpers import require_ssh, CheckContentBuilder
from plugins.kafka.utils.qrylib.purgatory_queries import get_purgatory_query
import logging
import json

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 7


def run_check_purgatory_size(connector, settings):
    """
    Checks purgatory sizes on all Kafka brokers via SSH + JMX.
    
    Collects both Fetch and Produce purgatory metrics using JMX and analyzes
    them to identify consumer/producer performance issues.
    
    Args:
        connector: Kafka connector with multi-host SSH support
        settings: Configuration settings with thresholds
    
    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    builder.h3("Purgatory Size Monitoring (All Brokers)")

    # Check SSH availability
    available, skip_msg, skip_data = require_ssh(connector, "Purgatory size check")
    if not available:
        builder.add(skip_msg)
        return builder.build(), skip_data

    # Check JMX availability
    if not connector.has_jmx():
        current_strategy = connector.metric_collection_strategy
        details = connector.metric_collection_details

        builder.note(f"ℹ️ Purgatory metrics require direct JMX access")
        builder.blank()

        if current_strategy == 'local_prometheus':
            builder.text(f"*Current metric collection:* Prometheus JMX Exporter (port {details.get('port', 7500)})")
            builder.blank()
            builder.text("*Note:* Purgatory metrics are not typically exported by Prometheus JMX Exporter.")
            builder.text("If you need purgatory monitoring, enable direct JMX access:")
        elif current_strategy == 'instaclustr_prometheus':
            builder.text("*Current metric collection:* Instaclustr Prometheus API")
            builder.blank()
            builder.text("*Note:* Purgatory metrics may be available through Instaclustr API.")
            builder.text("Contact Instaclustr support for purgatory metric availability.")
            return builder.build(), {
                'status': 'skipped',
                'reason': 'instaclustr_prometheus_check_instaclustr_api',
                'details': 'Using Instaclustr - purgatory metrics may be in their API'
            }
        else:
            reason = details.get('reason', 'JMX not available')
            builder.text(f"*Status:* {reason}")
            builder.blank()
            builder.text("*How to Enable JMX:*")

        builder.blank()
        builder.text("Add to Kafka startup script or systemd service:")
        builder.text("```bash")
        builder.text("export KAFKA_JMX_OPTS=\"-Dcom.sun.management.jmxremote \\")
        builder.text("  -Dcom.sun.management.jmxremote.authenticate=false \\")
        builder.text("  -Dcom.sun.management.jmxremote.ssl=false \\")
        builder.text("  -Dcom.sun.management.jmxremote.port=9999\"")
        builder.text("```")
        builder.blank()
        builder.text("Then restart Kafka brokers.")
        return builder.build(), {
            'status': 'skipped',
            'reason': 'jmx_not_available',
            'current_strategy': current_strategy,
            'details': details
        }
    
    try:
        # === STEP 1: GET THRESHOLDS ===
        fetch_warning_threshold = settings.get('kafka_fetch_purgatory_warning', 100)
        fetch_critical_threshold = settings.get('kafka_fetch_purgatory_critical', 500)
        produce_warning_threshold = settings.get('kafka_produce_purgatory_warning', 100)
        produce_critical_threshold = settings.get('kafka_produce_purgatory_critical', 500)
        
        # === STEP 2: GET QUERY FROM QUERY LIBRARY ===
        query_json = get_purgatory_query(connector)
        query_data = json.loads(query_json)
        command = query_data['command']
        
        # === STEP 3: EXECUTE ON ALL HOSTS ===
        results = connector.execute_ssh_on_all_hosts(command, "Purgatory size check")
        
        # === STEP 4: PARSE RESULTS ===
        all_purgatory_data = []
        issues_found = False
        critical_brokers = []
        warning_brokers = []
        errors = []
        
        for result in results:
            host = result['host']
            broker_id = result['node_id']
            
            if not result['success']:
                error_msg = result.get('error', 'Unknown error')

                # Check for common JMX issues
                if 'exit 124' in error_msg or 'timeout' in error_msg.lower():
                    error_msg = "JMX connection timeout - JMX may not be enabled on port 9999"
                elif 'exit 127' in error_msg:
                    error_msg = "kafka-run-class.sh not found - check kafka_run_class_path setting"

                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': error_msg
                })
                continue
            
            output = result['output'].strip()
            if not output:
                builder.warning(f"No purgatory data returned from {host} (Broker {broker_id})")
                continue
            
            # Debug logging
            logger.debug(f"Raw output from {host} (Broker {broker_id}):\n{output[:500]}")
            
            # Parse the output to extract both metrics
            fetch_purgatory_size = None
            produce_purgatory_size = None
            
            sections = output.split('===')
            for section in sections:
                if 'FETCH_PURGATORY' in section:
                    # Find data lines (after the header line with "time")
                    # JmxTool outputs multiple samples, we want the last one
                    lines = [l.strip() for l in section.split('\n') if l.strip()]
                    data_lines = []
                    for line in lines:
                        if ',' in line and 'time' not in line.lower():
                            data_lines.append(line)
                    
                    # Use the last data line (most recent value)
                    if data_lines:
                        try:
                            parts = data_lines[-1].split(',')
                            if len(parts) >= 2:
                                # Handle both formats: "value" or just value
                                value_str = parts[-1].strip().strip('"')
                                fetch_purgatory_size = int(float(value_str))
                        except (ValueError, IndexError) as e:
                            logger.debug(f"Could not parse fetch purgatory line: {data_lines[-1]} - {e}")
                
                elif 'PRODUCE_PURGATORY' in section:
                    lines = [l.strip() for l in section.split('\n') if l.strip()]
                    data_lines = []
                    for line in lines:
                        if ',' in line and 'time' not in line.lower():
                            data_lines.append(line)
                    
                    if data_lines:
                        try:
                            parts = data_lines[-1].split(',')
                            if len(parts) >= 2:
                                value_str = parts[-1].strip().strip('"')
                                produce_purgatory_size = int(float(value_str))
                        except (ValueError, IndexError) as e:
                            logger.debug(f"Could not parse produce purgatory line: {data_lines[-1]} - {e}")
            
            if fetch_purgatory_size is None or produce_purgatory_size is None:
                errors.append({
                    'host': host,
                    'broker_id': broker_id,
                    'error': 'Could not parse purgatory metrics from JMX output'
                })
                builder.warning(
                    f"Could not parse purgatory metrics on {host} (Broker {broker_id})"
                )
                continue
            
            purgatory_info = {
                'broker_id': broker_id,
                'host': host,
                'fetch_purgatory_size': fetch_purgatory_size,
                'produce_purgatory_size': produce_purgatory_size
            }
            all_purgatory_data.append(purgatory_info)
        
        # === STEP 5: ISSUE DETECTION ===
        for purg in all_purgatory_data:
            broker_id = purg['broker_id']
            host = purg['host']
            broker_has_issues = False
            
            # Check Fetch Purgatory Size
            if purg['fetch_purgatory_size'] >= fetch_critical_threshold:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                builder.critical_issue(
                    "Critical Fetch Purgatory Size",
                    {
                        "Broker": f"{broker_id} ({host})",
                        "Fetch Purgatory Size": f"{purg['fetch_purgatory_size']} requests (threshold: {fetch_critical_threshold})",
                        "Impact": "Consumers may be waiting for data due to fetch.min.bytes settings",
                        "Warning": "High fetch purgatory size indicates consumers are experiencing delays!"
                    }
                )
            elif purg['fetch_purgatory_size'] >= fetch_warning_threshold:
                issues_found = True
                if broker_id not in warning_brokers:
                    warning_brokers.append(broker_id)
                
                builder.warning_issue(
                    "Elevated Fetch Purgatory Size",
                    {
                        "Broker": f"{broker_id} ({host})",
                        "Fetch Purgatory Size": f"{purg['fetch_purgatory_size']} requests (threshold: {fetch_warning_threshold})",
                        "Note": "Monitor for increasing trend"
                    }
                )
            
            # Check Produce Purgatory Size
            if purg['produce_purgatory_size'] >= produce_critical_threshold:
                issues_found = True
                broker_has_issues = True
                if broker_id not in critical_brokers:
                    critical_brokers.append(broker_id)
                
                builder.critical_issue(
                    "Critical Produce Purgatory Size",
                    {
                        "Broker": f"{broker_id} ({host})",
                        "Produce Purgatory Size": f"{purg['produce_purgatory_size']} requests (threshold: {produce_critical_threshold})",
                        "Impact": "Producers waiting for replica acknowledgements (acks=all/-1)",
                        "Warning": "Growing purgatory suggests overloaded replicas - risk of producer timeouts!"
                    }
                )
            elif purg['produce_purgatory_size'] >= produce_warning_threshold:
                issues_found = True
                if broker_id not in warning_brokers:
                    warning_brokers.append(broker_id)
                
                builder.warning_issue(
                    "Elevated Produce Purgatory Size",
                    {
                        "Broker": f"{broker_id} ({host})",
                        "Produce Purgatory Size": f"{purg['produce_purgatory_size']} requests (threshold: {produce_warning_threshold})",
                        "Note": "Monitor for increasing trend - may indicate replica lag"
                    }
                )
        
        # === STEP 6: SUMMARY TABLE ===
        if all_purgatory_data:
            builder.h4("Purgatory Size Summary")
            
            builder.text("|===")
            builder.text("|Broker|Host|Fetch Purgatory Size|Produce Purgatory Size")
            
            for purg in sorted(all_purgatory_data, 
                             key=lambda x: x['fetch_purgatory_size'] + x['produce_purgatory_size'], 
                             reverse=True):
                fetch_indicator = ""
                if purg['fetch_purgatory_size'] >= fetch_critical_threshold:
                    fetch_indicator = "🔴 "
                elif purg['fetch_purgatory_size'] >= fetch_warning_threshold:
                    fetch_indicator = "⚠️ "
                
                produce_indicator = ""
                if purg['produce_purgatory_size'] >= produce_critical_threshold:
                    produce_indicator = "🔴 "
                elif purg['produce_purgatory_size'] >= produce_warning_threshold:
                    produce_indicator = "⚠️ "
                
                builder.text(
                    f"|{purg['broker_id']}|{purg['host']}|"
                    f"{fetch_indicator}{purg['fetch_purgatory_size']}|"
                    f"{produce_indicator}{purg['produce_purgatory_size']}"
                )
            builder.text("|===")
            builder.blank()
        
        # === STEP 7: ERROR SUMMARY ===
        if errors:
            builder.h4("Collection Errors")
            error_list = [f"Broker {e['broker_id']} ({e['host']}): {e['error']}" for e in errors]
            builder.warning(
                f"Could not collect purgatory metrics from {len(errors)} broker(s):\n\n" +
                "\n".join(f"* {e}" for e in error_list)
            )
            builder.blank()

            # Check if all errors are JMX-related
            jmx_errors = [e for e in errors if 'JMX' in e['error'] or 'timeout' in e['error'].lower()]
            if len(jmx_errors) == len(errors):
                builder.text("*How to Enable JMX on Kafka Brokers:*")
                builder.blank()
                builder.text("Add the following to your Kafka startup script or systemd service:")
                builder.text("```bash")
                builder.text("export KAFKA_JMX_OPTS=\"-Dcom.sun.management.jmxremote \\")
                builder.text("  -Dcom.sun.management.jmxremote.authenticate=false \\")
                builder.text("  -Dcom.sun.management.jmxremote.ssl=false \\")
                builder.text("  -Dcom.sun.management.jmxremote.port=9999\"")
                builder.text("```")
                builder.blank()
                builder.text("Then restart Kafka brokers.")
                builder.blank()
                builder.text("*Alternative:* Use Prometheus JMX Exporter for purgatory metrics (port 7500)")
                builder.blank()
        
        # === STEP 8: RECOMMENDATIONS ===
        if issues_found:
            builder.recs({
                "critical": [
                    "**Increase cluster capacity:** Add more brokers or scale up existing broker resources",
                    "**Review producer configuration:** Check if acks=all is necessary for all topics",
                    "**Monitor replica lag:** High produce purgatory indicates replica synchronization issues",
                    "**Check consumer configuration:** Adjust fetch.min.bytes and fetch.wait.max.ms for fetch delays",
                    "**Investigate slow replicas:** Use kafka-topics --describe --under-replicated-partitions"
                ] if critical_brokers else None,
                "high": [
                    "**Monitor purgatory trends:** Track metrics over time to identify growing patterns",
                    "**Review partition replication:** Check if replication factor matches cluster size",
                    "**Optimize consumer fetch settings:** Balance between latency and throughput",
                    "**Consider async producers:** Use acks=1 for less critical data to reduce purgatory load"
                ] if warning_brokers else None,
                "general": [
                    "Fetch Purgatory: Affected by fetch.min.bytes and fetch.wait.max.ms consumer configs",
                    "Produce Purgatory: Primarily affected when using acks=all/-1 with slow replicas",
                    "Monitor replica lag: Use kafka-consumer-groups --describe to check lag metrics",
                    "JMX monitoring: Enable JMX on all brokers for metric collection",
                    "Baseline metrics: Establish normal purgatory size ranges for your workload"
                ]
            })
        else:
            builder.success(
                f"✅ Purgatory sizes are healthy across all brokers.\n\n"
                f"All brokers show fetch purgatory below {fetch_warning_threshold} requests "
                f"and produce purgatory below {produce_warning_threshold} requests."
            )
        
        # === STEP 9: STRUCTURED DATA ===
        structured_data["purgatory_size"] = {
            "status": "success",
            "brokers_checked": len([r for r in results if r['success']]),
            "brokers_with_errors": len(errors),
            "critical_brokers": critical_brokers,
            "warning_brokers": warning_brokers,
            "thresholds": {
                "fetch_warning_threshold": fetch_warning_threshold,
                "fetch_critical_threshold": fetch_critical_threshold,
                "produce_warning_threshold": produce_warning_threshold,
                "produce_critical_threshold": produce_critical_threshold
            },
            "errors": errors,
            "data": all_purgatory_data
        }
        
        # Summary for aggregate rules
        structured_data["purgatory_summary"] = {
            "status": "success",
            "data": [{
                "total_brokers_checked": len([r for r in results if r['success']]),
                "critical_broker_count": len(critical_brokers),
                "warning_broker_count": len(warning_brokers),
                "critical_brokers": critical_brokers,
                "warning_brokers": warning_brokers,
                "has_cluster_wide_issue": len(critical_brokers) >= 2,
                "max_fetch_purgatory": max([p['fetch_purgatory_size'] for p in all_purgatory_data], default=0),
                "max_produce_purgatory": max([p['produce_purgatory_size'] for p in all_purgatory_data], default=0),
                "total_fetch_purgatory": sum([p['fetch_purgatory_size'] for p in all_purgatory_data]),
                "total_produce_purgatory": sum([p['produce_purgatory_size'] for p in all_purgatory_data])
            }]
        }
        
    except Exception as e:
        import traceback
        logger.error(f"Purgatory size check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Purgatory size check failed: {e}")
        structured_data["purgatory_size"] = {
            "status": "error",
            "details": str(e)
        }
    
    return builder.build(), structured_data

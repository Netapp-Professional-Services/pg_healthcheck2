"""
Patroni DCS (Distributed Configuration Store) Health Check

Monitors the health of the DCS backend used by Patroni for leader election,
configuration storage, and cluster coordination.

Supports:
- etcd (most common)
- Consul
- ZooKeeper
- Kubernetes (limited - detects but doesn't deeply monitor)

The DCS is critical infrastructure - issues here directly cause Patroni failovers
and cluster instability.

Data Sources:
- Patroni /config endpoint - detect DCS type and connection details
- DCS-specific health APIs
- DCS metrics and status endpoints

Output:
- DCS type and connection information
- Cluster member health
- Performance metrics (latency, disk usage)
- Quorum status
- Actionable recommendations for DCS issues
"""

import logging
import requests
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.patroni_client import create_patroni_client_from_settings
from plugins.postgres.utils.patroni_helpers import (
    skip_if_not_patroni,
    build_error_result
)

logger = logging.getLogger(__name__)


def check_patroni_dcs_health(connector, settings: Dict) -> Tuple[str, Dict]:
    """
    Check the health of Patroni's DCS backend.

    Detects which DCS is in use and performs appropriate health checks.

    Args:
        connector: PostgreSQL connector with environment detection
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Patroni DCS Health Check")

    # Skip if not Patroni
    skip_result = skip_if_not_patroni(connector)
    if skip_result:
        return skip_result

    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        # Detect DCS type and configuration
        dcs_info = _detect_dcs(settings)

        if not dcs_info.get('success'):
            return build_error_result(
                'patroni_dcs_health',
                dcs_info.get('error', 'Could not detect DCS configuration'),
                builder
            )

        dcs_type = dcs_info.get('type')
        dcs_config = dcs_info.get('config', {})

        # Perform DCS-specific health checks
        health_data = None
        if dcs_type == 'etcd':
            health_data = _check_etcd_health(dcs_config, settings)
        elif dcs_type == 'consul':
            health_data = _check_consul_health(dcs_config, settings)
        elif dcs_type == 'zookeeper':
            health_data = _check_zookeeper_health(dcs_config, settings)
        elif dcs_type == 'kubernetes':
            health_data = _check_kubernetes_dcs(dcs_config, settings)
        else:
            return build_error_result(
                'patroni_dcs_health',
                f"Unsupported DCS type: {dcs_type}",
                builder
            )

        if not health_data.get('success'):
            return build_error_result(
                'patroni_dcs_health',
                health_data.get('error', f'Could not check {dcs_type} health'),
                builder
            )

        # Analyze health data
        analysis = _analyze_dcs_health(dcs_type, health_data)

        # Build output with actionable advice
        _build_dcs_health_output(builder, dcs_type, dcs_config, health_data, analysis)

        # Build findings for trend storage
        findings = {
            'patroni_dcs_health': {
                'status': 'success',
                'timestamp': timestamp,
                'dcs_type': dcs_type,
                'dcs_config': dcs_config,
                'health': health_data.get('health', {}),
                'analysis': analysis,
                'source': 'dcs_api'
            }
        }

        return builder.build(), findings

    except Exception as e:
        logger.error(f"Failed to check Patroni DCS health: {e}", exc_info=True)
        return build_error_result(
            'patroni_dcs_health',
            str(e),
            builder
        )


def _detect_dcs(settings: Dict) -> Dict:
    """
    Detect which DCS Patroni is using.

    Args:
        settings: Configuration dictionary

    Returns:
        Dictionary with DCS type and configuration
    """
    client = create_patroni_client_from_settings(settings)
    if not client:
        return {'success': False, 'error': 'Could not create Patroni client - check configuration'}

    try:
        success, result = client.get_config()
        client.close()

        if not success:
            return {'success': False, 'error': 'Could not fetch Patroni config'}

        config = result.get('data', {})

        # Detect DCS type from config keys
        if 'etcd' in config or 'etcd3' in config:
            etcd_config = config.get('etcd') or config.get('etcd3', {})
            return {
                'success': True,
                'type': 'etcd',
                'config': etcd_config
            }
        elif 'consul' in config:
            return {
                'success': True,
                'type': 'consul',
                'config': config.get('consul', {})
            }
        elif 'zookeeper' in config:
            return {
                'success': True,
                'type': 'zookeeper',
                'config': config.get('zookeeper', {})
            }
        elif 'kubernetes' in config:
            return {
                'success': True,
                'type': 'kubernetes',
                'config': config.get('kubernetes', {})
            }
        else:
            # Try to infer from settings if not in Patroni config
            if settings.get('etcd_host') or settings.get('etcd_port'):
                return {
                    'success': True,
                    'type': 'etcd',
                    'config': {
                        'host': settings.get('etcd_host', 'localhost'),
                        'port': settings.get('etcd_port', 2379)
                    }
                }

            return {'success': False, 'error': 'Could not detect DCS type from configuration'}

    except Exception as e:
        logger.debug(f"Could not detect DCS: {e}")
        return {'success': False, 'error': str(e)}


def _check_etcd_health(dcs_config: Dict, settings: Dict) -> Dict:
    """
    Check etcd cluster health.

    Args:
        dcs_config: etcd configuration from Patroni
        settings: Configuration dictionary

    Returns:
        Dictionary with health status and metrics
    """
    # Get etcd connection details
    etcd_host = dcs_config.get('host') or settings.get('etcd_host', 'localhost')
    etcd_hosts = dcs_config.get('hosts') or dcs_config.get('host')

    # Handle multiple hosts
    if isinstance(etcd_hosts, str):
        # Could be comma-separated or single host
        if ',' in etcd_hosts:
            hosts = [h.strip() for h in etcd_hosts.split(',')]
        else:
            hosts = [etcd_hosts]
    elif isinstance(etcd_hosts, list):
        hosts = etcd_hosts
    else:
        hosts = [etcd_host]

    # Extract port
    etcd_port = dcs_config.get('port') or settings.get('etcd_port', 2379)
    timeout = settings.get('dcs_timeout', 5)

    health_results = []
    member_list = []

    for host in hosts:
        # Clean host (remove port if included)
        if ':' in host:
            host, port_str = host.split(':', 1)
            try:
                etcd_port = int(port_str)
            except:
                pass

        endpoint = f"http://{host}:{etcd_port}"

        try:
            # Check /health endpoint
            health_resp = requests.get(
                f"{endpoint}/health",
                timeout=timeout
            )

            health_status = health_resp.json() if health_resp.status_code == 200 else {}

            # Try to get member list (only from first responding host)
            if not member_list:
                try:
                    members_resp = requests.get(
                        f"{endpoint}/v3/cluster/member/list",
                        timeout=timeout,
                        headers={'Content-Type': 'application/json'},
                        json={}
                    )
                    if members_resp.status_code == 200:
                        member_list = members_resp.json().get('members', [])
                except Exception as e:
                    logger.debug(f"Could not get member list from {endpoint}: {e}")

            # Try to get metrics
            metrics = {}
            try:
                metrics_resp = requests.get(
                    f"{endpoint}/metrics",
                    timeout=timeout
                )
                if metrics_resp.status_code == 200:
                    metrics = _parse_etcd_metrics(metrics_resp.text)
            except Exception as e:
                logger.debug(f"Could not get metrics from {endpoint}: {e}")

            health_results.append({
                'endpoint': endpoint,
                'healthy': health_status.get('health') == 'true' or health_resp.status_code == 200,
                'status': health_status,
                'metrics': metrics,
                'reachable': True
            })

        except requests.exceptions.RequestException as e:
            logger.debug(f"Could not connect to etcd at {endpoint}: {e}")
            health_results.append({
                'endpoint': endpoint,
                'healthy': False,
                'reachable': False,
                'error': str(e)
            })

    if not health_results:
        return {'success': False, 'error': 'Could not reach any etcd endpoints'}

    return {
        'success': True,
        'health': {
            'endpoints': health_results,
            'members': member_list,
            'cluster_healthy': all(h.get('healthy') for h in health_results if h.get('reachable'))
        }
    }


def _parse_etcd_metrics(metrics_text: str) -> Dict:
    """
    Parse Prometheus-format etcd metrics.

    Args:
        metrics_text: Metrics in Prometheus text format

    Returns:
        Dictionary of parsed metrics
    """
    metrics = {}

    # Parse key metrics
    for line in metrics_text.split('\n'):
        if line.startswith('#') or not line.strip():
            continue

        try:
            # Simple parsing of key=value metrics
            if 'etcd_server_has_leader' in line:
                metrics['has_leader'] = '1' in line.split()[-1]
            elif 'etcd_mvcc_db_total_size_in_bytes' in line:
                metrics['db_size_bytes'] = int(float(line.split()[-1]))
            elif 'etcd_disk_backend_commit_duration_seconds' in line and 'sum' in line:
                metrics['disk_commit_latency'] = float(line.split()[-1])
        except Exception as e:
            logger.debug(f"Could not parse metric line: {line}: {e}")
            continue

    return metrics


def _check_consul_health(dcs_config: Dict, settings: Dict) -> Dict:
    """
    Check Consul cluster health.

    Args:
        dcs_config: Consul configuration from Patroni
        settings: Configuration dictionary

    Returns:
        Dictionary with health status and metrics
    """
    consul_host = dcs_config.get('host') or settings.get('consul_host', 'localhost')
    consul_port = dcs_config.get('port') or settings.get('consul_port', 8500)
    timeout = settings.get('dcs_timeout', 5)

    endpoint = f"http://{consul_host}:{consul_port}"

    try:
        # Check agent health
        health_resp = requests.get(
            f"{endpoint}/v1/agent/self",
            timeout=timeout
        )

        if health_resp.status_code != 200:
            return {'success': False, 'error': f'Consul returned status {health_resp.status_code}'}

        agent_info = health_resp.json()

        # Get cluster members
        members_resp = requests.get(
            f"{endpoint}/v1/agent/members",
            timeout=timeout
        )
        members = members_resp.json() if members_resp.status_code == 200 else []

        # Get leader info
        leader_resp = requests.get(
            f"{endpoint}/v1/status/leader",
            timeout=timeout
        )
        leader = leader_resp.json() if leader_resp.status_code == 200 else None

        return {
            'success': True,
            'health': {
                'agent': agent_info,
                'members': members,
                'leader': leader,
                'cluster_healthy': len([m for m in members if m.get('Status') == 1]) > 0
            }
        }

    except requests.exceptions.RequestException as e:
        logger.debug(f"Could not connect to Consul at {endpoint}: {e}")
        return {'success': False, 'error': str(e)}


def _check_zookeeper_health(dcs_config: Dict, settings: Dict) -> Dict:
    """
    Check ZooKeeper ensemble health.

    Args:
        dcs_config: ZooKeeper configuration from Patroni
        settings: Configuration dictionary

    Returns:
        Dictionary with health status and metrics
    """
    import socket

    zk_hosts = dcs_config.get('hosts') or settings.get('zookeeper_hosts', 'localhost:2181')
    timeout = settings.get('dcs_timeout', 5)

    # Parse hosts
    if isinstance(zk_hosts, str):
        hosts = [h.strip() for h in zk_hosts.split(',')]
    else:
        hosts = zk_hosts

    health_results = []

    for host_port in hosts:
        if ':' in host_port:
            host, port = host_port.rsplit(':', 1)
            port = int(port)
        else:
            host = host_port
            port = 2181

        try:
            # Send 'ruok' command (are you ok?)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            sock.sendall(b'ruok')
            response = sock.recv(4).decode('utf-8')
            sock.close()

            is_ok = response == 'imok'

            # Try to get stats
            stats = {}
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                sock.connect((host, port))
                sock.sendall(b'stat')
                stat_data = sock.recv(4096).decode('utf-8')
                sock.close()

                # Parse basic stats
                for line in stat_data.split('\n'):
                    if 'Mode:' in line:
                        stats['mode'] = line.split(':', 1)[1].strip()
                    elif 'Connections:' in line:
                        stats['connections'] = int(line.split(':', 1)[1].strip())
            except Exception as e:
                logger.debug(f"Could not get ZK stats: {e}")

            health_results.append({
                'endpoint': f"{host}:{port}",
                'healthy': is_ok,
                'stats': stats,
                'reachable': True
            })

        except Exception as e:
            logger.debug(f"Could not connect to ZooKeeper at {host}:{port}: {e}")
            health_results.append({
                'endpoint': f"{host}:{port}",
                'healthy': False,
                'reachable': False,
                'error': str(e)
            })

    if not health_results:
        return {'success': False, 'error': 'Could not reach any ZooKeeper nodes'}

    return {
        'success': True,
        'health': {
            'endpoints': health_results,
            'cluster_healthy': any(h.get('healthy') for h in health_results)
        }
    }


def _check_kubernetes_dcs(dcs_config: Dict, settings: Dict) -> Dict:
    """
    Check Kubernetes DCS (limited checks - mainly just detection).

    Args:
        dcs_config: Kubernetes configuration from Patroni
        settings: Configuration dictionary

    Returns:
        Dictionary with basic information
    """
    # For Kubernetes, Patroni uses ConfigMaps/Endpoints for DCS
    # We can't easily check from outside the cluster
    return {
        'success': True,
        'health': {
            'type': 'kubernetes',
            'namespace': dcs_config.get('namespace', 'default'),
            'labels': dcs_config.get('labels', {}),
            'note': 'Kubernetes DCS health is managed by k8s - check API server and etcd health'
        }
    }


def _analyze_dcs_health(dcs_type: str, health_data: Dict) -> Dict:
    """
    Analyze DCS health and identify issues.

    Args:
        dcs_type: Type of DCS (etcd, consul, zookeeper)
        health_data: Health check results

    Returns:
        Analysis dictionary with issues and recommendations
    """
    issues = []
    health_score = 100
    health = health_data.get('health', {})

    if dcs_type == 'etcd':
        endpoints = health.get('endpoints', [])
        members = health.get('members', [])

        # Check endpoint health
        unhealthy_endpoints = [e for e in endpoints if not e.get('healthy')]
        if unhealthy_endpoints:
            issues.append({
                'severity': 'critical' if len(unhealthy_endpoints) == len(endpoints) else 'high',
                'type': 'etcd_endpoints_unhealthy',
                'message': f"{len(unhealthy_endpoints)} of {len(endpoints)} etcd endpoint(s) are unhealthy",
                'recommendation': 'Check etcd logs, disk space, and network connectivity. Ensure etcd processes are running.'
            })
            health_score -= 30

        # Check for leader
        has_leader = any(e.get('metrics', {}).get('has_leader') for e in endpoints)
        if not has_leader and endpoints:
            issues.append({
                'severity': 'critical',
                'type': 'etcd_no_leader',
                'message': 'etcd cluster has no leader - cluster is not operational',
                'recommendation': 'URGENT: Check etcd quorum. Ensure majority of members are online. Review etcd logs for election issues.'
            })
            health_score -= 50

        # Check database size
        for endpoint in endpoints:
            db_size = endpoint.get('metrics', {}).get('db_size_bytes', 0)
            if db_size > 8 * 1024 * 1024 * 1024:  # 8GB
                issues.append({
                    'severity': 'warning',
                    'type': 'etcd_large_database',
                    'message': f'etcd database is very large ({db_size / (1024**3):.2f} GB)',
                    'recommendation': 'Consider compacting and defragmenting etcd database. Review Patroni history retention settings.'
                })
                health_score -= 15

    elif dcs_type == 'consul':
        members = health.get('members', [])
        alive_members = [m for m in members if m.get('Status') == 1]

        if len(alive_members) < len(members) / 2:
            issues.append({
                'severity': 'critical',
                'type': 'consul_no_quorum',
                'message': f'Only {len(alive_members)} of {len(members)} Consul members are alive - may not have quorum',
                'recommendation': 'URGENT: Check Consul server health and network connectivity. Ensure quorum is maintained.'
            })
            health_score -= 40

        if not health.get('leader'):
            issues.append({
                'severity': 'critical',
                'type': 'consul_no_leader',
                'message': 'Consul cluster has no leader',
                'recommendation': 'Check Consul raft logs and ensure quorum. Verify network connectivity between servers.'
            })
            health_score -= 50

    elif dcs_type == 'zookeeper':
        endpoints = health.get('endpoints', [])
        healthy_endpoints = [e for e in endpoints if e.get('healthy')]

        if len(healthy_endpoints) < len(endpoints) / 2:
            issues.append({
                'severity': 'critical',
                'type': 'zookeeper_no_quorum',
                'message': f'Only {len(healthy_endpoints)} of {len(endpoints)} ZooKeeper nodes are healthy - may not have quorum',
                'recommendation': 'URGENT: Check ZooKeeper logs and network. Ensure majority of ensemble is online.'
            })
            health_score -= 40

    return {
        'health_score': max(0, health_score),
        'issues': issues
    }


def _build_dcs_health_output(
    builder: CheckContentBuilder,
    dcs_type: str,
    dcs_config: Dict,
    health_data: Dict,
    analysis: Dict
):
    """
    Build AsciiDoc output for DCS health with actionable advice.

    Args:
        builder: CheckContentBuilder instance
        dcs_type: Type of DCS
        dcs_config: DCS configuration
        health_data: Health check results
        analysis: Analysis dictionary
    """
    health = health_data.get('health', {})
    health_score = analysis['health_score']

    # DCS Type and Configuration
    builder.text(f"*DCS Type*: {dcs_type.upper()}")
    builder.blank()

    # Health Score
    builder.text("*DCS Health Score*")
    builder.blank()
    builder.text(f"Score: *{health_score}/100*")
    builder.blank()

    if health_score >= 90:
        builder.note("DCS cluster is healthy and operating normally.")
    elif health_score >= 70:
        builder.warning("DCS cluster has minor issues that should be investigated.")
    else:
        builder.critical("**DCS cluster has serious issues that require immediate attention.**\n\nDCS problems directly cause Patroni failovers and cluster instability.")

    builder.blank()

    # DCS-specific output
    if dcs_type == 'etcd':
        _build_etcd_output(builder, health, analysis)
    elif dcs_type == 'consul':
        _build_consul_output(builder, health, analysis)
    elif dcs_type == 'zookeeper':
        _build_zookeeper_output(builder, health, analysis)
    elif dcs_type == 'kubernetes':
        _build_kubernetes_output(builder, health)

    # Issues and recommendations - group by severity
    issues = analysis.get('issues', [])
    if issues:
        # Group issues by severity
        critical_issues = [i for i in issues if i.get('severity') == 'critical']
        high_issues = [i for i in issues if i.get('severity') == 'high']
        warning_issues = [i for i in issues if i.get('severity') == 'warning']
        info_issues = [i for i in issues if i.get('severity') == 'info']

        # Format issue details for admonition blocks
        if critical_issues:
            details = []
            for issue in critical_issues:
                details.append(f"*{issue['type'].replace('_', ' ').title()}*")
                details.append(f"{issue['message']}")
                details.append(f"_Action_: {issue['recommendation']}")
            builder.critical_issue("Critical DCS Issues", details)

        if high_issues:
            details = []
            for issue in high_issues:
                details.append(f"*{issue['type'].replace('_', ' ').title()}*")
                details.append(f"{issue['message']}")
                details.append(f"_Action_: {issue['recommendation']}")
            builder.warning_issue("High Priority DCS Issues", details)

        if warning_issues:
            details = []
            for issue in warning_issues:
                details.append(f"*{issue['type'].replace('_', ' ').title()}*")
                details.append(f"{issue['message']}")
                details.append(f"_Action_: {issue['recommendation']}")
            builder.warning_issue("DCS Warnings", details)

        if info_issues:
            details = []
            for issue in info_issues:
                details.append(f"*{issue['type'].replace('_', ' ').title()}*")
                details.append(f"{issue['message']}")
                details.append(f"_Action_: {issue['recommendation']}")
            builder.note(f"**DCS Information**\n\n" + "\n\n".join(details))

    # General DCS maintenance tips
    builder.text("*DCS Maintenance Best Practices*")
    builder.blank()

    if dcs_type == 'etcd':
        builder.tip("**etcd Maintenance:**\n\n• Regular backups: `etcdctl snapshot save`\n• Monitor disk usage and compact regularly\n• Keep etcd version up to date\n• Use SSD storage for best performance\n• Monitor network latency between members")
    elif dcs_type == 'consul':
        builder.tip("**Consul Maintenance:**\n\n• Regular snapshots: `consul snapshot save`\n• Monitor raft logs and disk usage\n• Keep Consul version up to date\n• Ensure gossip encryption is enabled\n• Monitor WAN latency for multi-DC setups")
    elif dcs_type == 'zookeeper':
        builder.tip("**ZooKeeper Maintenance:**\n\n• Regular backups of data directory\n• Monitor transaction log size\n• Configure autopurge for old snapshots\n• Keep ZooKeeper version up to date\n• Use separate disk for transaction logs")


def _build_etcd_output(builder: CheckContentBuilder, health: Dict, analysis: Dict):
    """Build etcd-specific output."""
    endpoints = health.get('endpoints', [])
    members = health.get('members', [])

    builder.text("*etcd Cluster Status*")
    builder.blank()

    # Endpoints table
    endpoint_table = []
    for endpoint in endpoints:
        status = "Healthy" if endpoint.get('healthy') else "Unhealthy"
        if not endpoint.get('reachable'):
            status = "Unreachable"

        metrics = endpoint.get('metrics', {})
        db_size = metrics.get('db_size_bytes', 0)
        db_size_mb = db_size / (1024 * 1024) if db_size > 0 else 0

        endpoint_table.append({
            'Endpoint': endpoint['endpoint'],
            'Status': status,
            'Has Leader': 'Yes' if metrics.get('has_leader') else 'No',
            'DB Size': f"{db_size_mb:.2f} MB" if db_size > 0 else "N/A"
        })

    if endpoint_table:
        builder.table(endpoint_table)
        builder.blank()

    # Member information
    if members:
        builder.text(f"*Cluster Members*: {len(members)}")
        builder.blank()


def _build_consul_output(builder: CheckContentBuilder, health: Dict, analysis: Dict):
    """Build Consul-specific output."""
    members = health.get('members', [])
    leader = health.get('leader')

    builder.text("*Consul Cluster Status*")
    builder.blank()

    if leader:
        builder.text(f"Leader: `{leader}`")
        builder.blank()

    # Members table
    if members:
        member_table = []
        for member in members:
            status = "Alive" if member.get('Status') == 1 else "Failed"
            member_table.append({
                'Name': member.get('Name', 'Unknown'),
                'Address': member.get('Addr', 'Unknown'),
                'Status': status,
                'Role': member.get('Tags', {}).get('role', 'N/A')
            })

        builder.table(member_table)
        builder.blank()


def _build_zookeeper_output(builder: CheckContentBuilder, health: Dict, analysis: Dict):
    """Build ZooKeeper-specific output."""
    endpoints = health.get('endpoints', [])

    builder.text("*ZooKeeper Ensemble Status*")
    builder.blank()

    # Endpoints table
    endpoint_table = []
    for endpoint in endpoints:
        status = "OK" if endpoint.get('healthy') else "Unhealthy"
        if not endpoint.get('reachable'):
            status = "Unreachable"

        stats = endpoint.get('stats', {})

        endpoint_table.append({
            'Endpoint': endpoint['endpoint'],
            'Status': status,
            'Mode': stats.get('mode', 'N/A'),
            'Connections': str(stats.get('connections', 'N/A'))
        })

    if endpoint_table:
        builder.table(endpoint_table)
        builder.blank()


def _build_kubernetes_output(builder: CheckContentBuilder, health: Dict):
    """Build Kubernetes DCS output."""
    builder.text("*Kubernetes DCS*")
    builder.blank()
    builder.text(f"Namespace: `{health.get('namespace', 'default')}`")
    builder.blank()
    builder.note("Patroni uses Kubernetes ConfigMaps/Endpoints for DCS.\n\nCheck Kubernetes API server and underlying etcd health using kubectl and k8s monitoring tools.")

"""
Patroni REST API Client

Provides a client for interacting with Patroni's REST API to discover
cluster topology, member status, and configuration.

Patroni REST API Documentation:
- GET /cluster - Full cluster information
- GET /leader - Current leader
- GET /replica - Replica nodes
- GET /health - Node health status
- GET /patroni - Node-specific Patroni info
"""

import logging
import requests
from typing import Dict, List, Optional, Tuple
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class PatroniClient:
    """
    Client for Patroni REST API interactions.

    Patroni exposes a REST API (default port 8008) that provides
    cluster topology and member status information.
    """

    def __init__(self, base_url: str, timeout: int = 5):
        """
        Initialize Patroni client.

        Args:
            base_url: Base URL for Patroni API (e.g., http://host:8008)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self._session = requests.Session()

    def get_cluster_topology(self) -> Tuple[bool, Dict]:
        """
        Get full cluster topology from Patroni /cluster endpoint.

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            response = self._session.get(
                f"{self.base_url}/cluster",
                timeout=self.timeout
            )

            if response.status_code == 200:
                data = response.json()
                logger.debug(f"Successfully retrieved cluster topology: {data.get('members', [])} members")
                return True, {
                    'status': 'success',
                    'data': data,
                    'source': 'patroni_api'
                }
            else:
                logger.warning(f"Patroni API returned status {response.status_code}")
                return False, {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}',
                    'source': 'patroni_api'
                }

        except RequestException as e:
            logger.debug(f"Could not reach Patroni API: {e}")
            return False, {
                'status': 'error',
                'error': str(e),
                'source': 'patroni_api'
            }

    def get_node_status(self, endpoint: str = '/patroni') -> Tuple[bool, Dict]:
        """
        Get status of a specific Patroni node.

        Args:
            endpoint: API endpoint to query (default: /patroni)

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            response = self._session.get(
                f"{self.base_url}{endpoint}",
                timeout=self.timeout
            )

            if response.status_code in [200, 503]:  # 503 = replica in Patroni
                data = response.json()
                return True, {
                    'status': 'success',
                    'data': data,
                    'http_status': response.status_code
                }
            else:
                return False, {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}'
                }

        except RequestException as e:
            logger.debug(f"Could not get node status: {e}")
            return False, {
                'status': 'error',
                'error': str(e)
            }

    def get_leader_info(self) -> Tuple[bool, Dict]:
        """
        Get current cluster leader information.

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            response = self._session.get(
                f"{self.base_url}/leader",
                timeout=self.timeout
            )

            if response.status_code == 200:
                # Response is plain text with leader name
                leader_name = response.text.strip()
                return True, {
                    'status': 'success',
                    'leader': leader_name
                }
            else:
                return False, {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}'
                }

        except RequestException as e:
            logger.debug(f"Could not get leader info: {e}")
            return False, {
                'status': 'error',
                'error': str(e)
            }

    def get_health_status(self) -> Tuple[bool, Dict]:
        """
        Get PostgreSQL health status from Patroni /health endpoint.

        This endpoint returns 200 if PostgreSQL is running and accepting connections,
        503 if PostgreSQL is down or not accepting connections.

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            response = self._session.get(
                f"{self.base_url}/health",
                timeout=self.timeout
            )

            # Both 200 and 503 are valid responses
            if response.status_code in [200, 503]:
                is_healthy = response.status_code == 200

                return True, {
                    'status': 'success',
                    'healthy': is_healthy,
                    'http_status': response.status_code,
                    'source': 'patroni_api'
                }
            else:
                return False, {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}',
                    'source': 'patroni_api'
                }

        except RequestException as e:
            logger.debug(f"Could not get health status: {e}")
            return False, {
                'status': 'error',
                'error': str(e),
                'source': 'patroni_api'
            }

    def get_liveness(self) -> Tuple[bool, Dict]:
        """
        Get Patroni liveness status from /liveness endpoint.

        This endpoint returns 200 if Patroni is running and has a heartbeat.
        Used for Kubernetes liveness probes.

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            response = self._session.get(
                f"{self.base_url}/liveness",
                timeout=self.timeout
            )

            if response.status_code == 200:
                return True, {
                    'status': 'success',
                    'alive': True,
                    'http_status': response.status_code,
                    'source': 'patroni_api'
                }
            else:
                return True, {
                    'status': 'success',
                    'alive': False,
                    'http_status': response.status_code,
                    'source': 'patroni_api'
                }

        except RequestException as e:
            logger.debug(f"Could not get liveness status: {e}")
            return False, {
                'status': 'error',
                'error': str(e),
                'source': 'patroni_api'
            }

    def get_readiness(self, lag_threshold_mb: Optional[int] = None) -> Tuple[bool, Dict]:
        """
        Get Patroni readiness status from /readiness endpoint.

        This endpoint returns 200 if the node is ready to accept traffic.
        For replicas, this checks if replication lag is within acceptable limits.
        Used for Kubernetes readiness probes.

        Args:
            lag_threshold_mb: Optional lag threshold in MB to pass as parameter

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            url = f"{self.base_url}/readiness"
            params = {}
            if lag_threshold_mb is not None:
                # Patroni expects lag in bytes
                params['lag'] = lag_threshold_mb * 1024 * 1024

            response = self._session.get(
                url,
                params=params,
                timeout=self.timeout
            )

            # 200 = ready, 503 = not ready
            if response.status_code in [200, 503]:
                is_ready = response.status_code == 200

                return True, {
                    'status': 'success',
                    'ready': is_ready,
                    'http_status': response.status_code,
                    'source': 'patroni_api'
                }
            else:
                return False, {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}',
                    'source': 'patroni_api'
                }

        except RequestException as e:
            logger.debug(f"Could not get readiness status: {e}")
            return False, {
                'status': 'error',
                'error': str(e),
                'source': 'patroni_api'
            }

    def get_history(self) -> Tuple[bool, Dict]:
        """
        Get cluster failover/switchover history from /history endpoint.

        Returns a timeline of leadership changes and failover events.

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            response = self._session.get(
                f"{self.base_url}/history",
                timeout=self.timeout
            )

            if response.status_code == 200:
                data = response.json()
                logger.debug(f"Successfully retrieved failover history: {len(data)} events")
                return True, {
                    'status': 'success',
                    'data': data,
                    'source': 'patroni_api'
                }
            else:
                logger.warning(f"Patroni API returned status {response.status_code}")
                return False, {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}',
                    'source': 'patroni_api'
                }

        except RequestException as e:
            logger.debug(f"Could not get history: {e}")
            return False, {
                'status': 'error',
                'error': str(e),
                'source': 'patroni_api'
            }

    def get_config(self) -> Tuple[bool, Dict]:
        """
        Get Patroni dynamic configuration from /config endpoint.

        Returns the current dynamic configuration stored in DCS
        (etcd/Consul/ZooKeeper).

        Returns:
            tuple: (success: bool, data: dict)
        """
        try:
            response = self._session.get(
                f"{self.base_url}/config",
                timeout=self.timeout
            )

            if response.status_code == 200:
                data = response.json()
                logger.debug(f"Successfully retrieved Patroni configuration")
                return True, {
                    'status': 'success',
                    'data': data,
                    'source': 'patroni_api'
                }
            else:
                logger.warning(f"Patroni API returned status {response.status_code}")
                return False, {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}',
                    'source': 'patroni_api'
                }

        except RequestException as e:
            logger.debug(f"Could not get config: {e}")
            return False, {
                'status': 'error',
                'error': str(e),
                'source': 'patroni_api'
            }

    def get_metrics(self) -> Tuple[bool, str]:
        """
        Get Prometheus-format metrics from /metrics endpoint.

        Returns raw Prometheus metrics text format.

        Returns:
            tuple: (success: bool, metrics_text: str)
        """
        try:
            response = self._session.get(
                f"{self.base_url}/metrics",
                timeout=self.timeout
            )

            if response.status_code == 200:
                metrics_text = response.text
                logger.debug(f"Successfully retrieved Patroni metrics")
                return True, metrics_text
            else:
                logger.warning(f"Patroni API returned status {response.status_code}")
                return False, f"Error: HTTP {response.status_code}"

        except RequestException as e:
            logger.debug(f"Could not get metrics: {e}")
            return False, f"Error: {str(e)}"

    def close(self):
        """Close the session."""
        self._session.close()


def create_patroni_client_from_settings(settings: Dict) -> Optional[PatroniClient]:
    """
    Create a Patroni client from settings configuration.

    IMPORTANT: This function creates a client for Patroni's REST API (default port 8008).
    When connecting through a proxy (PgBouncer/HAProxy), we need to use the actual
    Patroni node's host, NOT the proxy host.

    Priority order:
    1. patroni_api_url (explicit override)
    2. patroni_direct.host (actual Patroni node, bypasses proxy)
    3. host (primary connection host - may be proxy, use with warning)

    Args:
        settings: Configuration dictionary

    Returns:
        PatroniClient instance or None if not configured
    """
    patroni_port = settings.get('patroni_port', 8008)
    patroni_timeout = settings.get('patroni_timeout', 5)

    # Priority 1: Allow explicit API URL override
    if settings.get('patroni_api_url'):
        base_url = settings['patroni_api_url']
        logger.debug(f"Using explicit patroni_api_url: {base_url}")

    # Priority 2: Use patroni_direct host (actual Patroni node, bypasses proxy)
    elif settings.get('patroni_direct', {}).get('host'):
        patroni_host = settings['patroni_direct']['host']
        base_url = f"http://{patroni_host}:{patroni_port}"
        logger.debug(f"Using patroni_direct.host for API: {base_url}")

    # Priority 3: Fall back to primary host (may be proxy - issue warning)
    elif settings.get('host'):
        patroni_host = settings['host']
        base_url = f"http://{patroni_host}:{patroni_port}"

        # Warn if this might be a proxy host
        if settings.get('pgbouncer_host') or settings.get('haproxy_host'):
            logger.warning(
                f"Using proxy host ({patroni_host}) for Patroni REST API. "
                f"This may return incorrect cluster status. Consider configuring "
                f"patroni_direct.host with the actual Patroni node address."
            )
        logger.debug(f"Using primary host for API: {base_url}")

    else:
        logger.warning("Cannot create Patroni client: no host configured")
        return None

    return PatroniClient(base_url, timeout=patroni_timeout)

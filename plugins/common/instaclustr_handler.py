"""
NetApp Instaclustr connection management for database health checks.

Provides a reusable manager for Instaclustr cluster health and metrics.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import yaml

try:
    import requests
    from requests.exceptions import RequestException, HTTPError, Timeout
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    # Define placeholder exceptions so decorators can reference them
    class RequestException(Exception):
        """Placeholder when requests is not installed."""
        pass
    class HTTPError(Exception):
        """Placeholder when requests is not installed."""
        pass
    class Timeout(Exception):
        """Placeholder when requests is not installed."""
        pass

from .retry_utils import retry_on_failure

logger = logging.getLogger(__name__)


class InstaclustrSupportMixin:
    """Mixin for Instaclustr support detection."""
    
    def has_instaclustr_support(self) -> bool:
        """Check if Instaclustr operations are supported."""
        return (hasattr(self, 'instaclustr_manager') and 
                self.instaclustr_manager is not None and 
                self.instaclustr_manager.is_configured())

    def get_instaclustr_skip_message(self, operation_name: Optional[str] = None) -> Tuple[str, Dict]:
        """Generate a skip message for Instaclustr-dependent checks."""
        op_text = f" for {operation_name}" if operation_name else ""
        adoc_message = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires Instaclustr access{op_text}.\n\n"
            "Configure in settings or config/instaclustr_credentials.yaml:\n\n"
            "* `instaclustr_api_key`: API key\n"
            "* `instaclustr_cluster_id`: Cluster ID\n\n"
            "See https://www.instaclustr.com/support/documentation/ for API setup.\n"
            "====\n"
        )
        structured_data = {
            "status": "skipped",
            "reason": "Instaclustr not configured",
            "required_settings": ["instaclustr_api_key", "instaclustr_cluster_id"]
        }
        return adoc_message, structured_data


class InstaclustrConnectionManager:
    """
    Manages Instaclustr API connections for cluster health checks.
    
    Uses REST API v2 (e.g., /monitoring/v2 for metrics).
    """
    
    BASE_URL = "https://api.instaclustr.com"

    def __init__(self, settings: Dict):
        """
        Initialize Instaclustr connection manager.
        
        Args:
            settings: Dictionary with configuration:
                - instaclustr_api_key: API key
                - instaclustr_cluster_id: Cluster ID
        """
        if not REQUESTS_AVAILABLE:
            raise ImportError(
                "requests library is required for Instaclustr support. "
                "Install it with: pip install requests"
            )
        
        self.settings = settings
        self.session = None
        self._load_credentials()
        self._validate_settings()
        self.connect()

    def _load_credentials(self):
        """Load credentials from config/instaclustr_credentials.yaml."""
        config_yaml = Path('config') / 'instaclustr_credentials.yaml'
        
        if config_yaml.exists():
            try:
                with open(config_yaml, 'r') as f:
                    creds = yaml.safe_load(f)
                
                self.settings.update({
                    'instaclustr_api_key': creds.get('instaclustr_api_key'),
                    'instaclustr_cluster_id': creds.get('instaclustr_cluster_id')
                })
                logger.info(f"Loaded Instaclustr credentials from {config_yaml}")
            except Exception as e:
                logger.warning(f"Failed to load config/instaclustr_credentials.yaml: {e}")

    def _validate_settings(self):
        """Validate required settings."""
        required = ['instaclustr_api_key', 'instaclustr_cluster_id']
        missing = [key for key in required if not self.settings.get(key)]
        
        if missing:
            raise ValueError(
                f"Missing Instaclustr settings: {', '.join(missing)}. "
                "Configure in settings or config/instaclustr_credentials.yaml"
            )

    def connect(self) -> None:
        """Initialize requests session with API key."""
        try:
            self.session = requests.Session()
            self.session.headers.update({
                'Authorization': f"Bearer {self.settings['instaclustr_api_key']}",
                'Content-Type': 'application/json'
            })
            
            # Set default timeout
            self.session.timeout = self.settings.get('instaclustr_timeout', 30)
            
            logger.info(
                f"✅ Instaclustr API session initialized for "
                f"cluster {self.settings['instaclustr_cluster_id']}"
            )
            
        except Exception as e:
            logger.error(f"Instaclustr API initialization failed: {e}")
            raise ConnectionError(f"Could not initialize Instaclustr session: {e}")

    def is_configured(self) -> bool:
        """Check if session is initialized."""
        return self.session is not None

    @retry_on_failure(max_attempts=3, delay=1, exceptions=(RequestException,))
    def get_cluster_details(self) -> Optional[Dict]:
        """
        Fetch cluster details from Instaclustr API.
        
        Returns:
            Dict with cluster details or None if error
        """
        cluster_id = self.settings['instaclustr_cluster_id']
        
        try:
            response = self.session.get(
                f"{self.BASE_URL}/cluster-management/v2/resources/clusters/{cluster_id}",
                timeout=self.settings.get('instaclustr_timeout', 30)
            )
            
            response.raise_for_status()
            cluster = response.json()
            
            return {
                "cluster_id": cluster.get('id'),
                "name": cluster.get('name'),
                "status": cluster.get('status'),
                "data_centres": len(cluster.get('dataCentres', [])),
                "nodes": [node['id'] for node in cluster.get('nodes', [])],
                "node_count": len(cluster.get('nodes', []))
            }
        
        except HTTPError as e:
            if e.response.status_code == 401:
                logger.error(f"Instaclustr authentication failed: Invalid API key")
            elif e.response.status_code == 403:
                logger.error(f"Instaclustr permission denied for cluster {cluster_id}")
            elif e.response.status_code == 404:
                logger.warning(f"Instaclustr cluster not found: {cluster_id}")
            else:
                logger.warning(f"Instaclustr HTTP error: {e.response.status_code}")
        
        except Timeout:
            logger.error(f"Instaclustr API timeout fetching cluster details")
        
        except RequestException as e:
            logger.warning(f"Could not fetch Instaclustr cluster details: {e}")
        
        except Exception as e:
            logger.error(f"Unexpected error fetching cluster details: {e}")
        
        return None

    def get_metrics(self, metric_type: str = 'health', hours: int = 24) -> Dict:
        """
        Fetch cluster health or performance metrics.
        
        Args:
            metric_type: Type of metric to fetch ('health', 'performance', etc.)
            hours: Number of hours of historical data
        
        Returns:
            Dict with metric data
        """
        cluster_id = self.settings['instaclustr_cluster_id']
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        try:
            # Apply retry logic
            @retry_on_failure(
                max_attempts=3,
                delay=1,
                exceptions=(RequestException,),
                log_attempts=True
            )
            def fetch_metrics():
                response = self.session.get(
                    f"{self.BASE_URL}/monitoring/v2/clusters/{cluster_id}/metrics",
                    params={
                        'metric': metric_type,
                        'start': start_time.isoformat(),
                        'end': end_time.isoformat()
                    },
                    timeout=self.settings.get('instaclustr_timeout', 30)
                )
                response.raise_for_status()
                return response.json()
            
            metrics = fetch_metrics()
            
            return {
                "metric_type": metric_type,
                "value": metrics.get('value', 'N/A'),
                "timestamp": metrics.get('timestamp', datetime.utcnow().isoformat()),
                "note": metrics.get('note', ''),
                "status": "success"
            }
        
        except HTTPError as e:
            # Classify HTTP errors
            if e.response.status_code == 401:
                logger.error(f"Instaclustr authentication failed for metrics")
                note = "Authentication failed: Invalid API key"
                status = "auth_error"
            
            elif e.response.status_code == 403:
                logger.error(f"Instaclustr permission denied for metrics")
                note = "Permission denied"
                status = "permission_error"
            
            elif e.response.status_code == 429:
                logger.warning(f"Instaclustr rate limit reached")
                note = "Rate limited"
                status = "rate_limited"
            
            elif e.response.status_code in (500, 502, 503, 504):
                logger.warning(f"Instaclustr service error: {e.response.status_code}")
                note = f"Service error ({e.response.status_code})"
                status = "service_error"
            
            else:
                logger.warning(f"Instaclustr HTTP error: {e.response.status_code}")
                note = f"HTTP error ({e.response.status_code})"
                status = "error"
            
            return {
                "metric_type": metric_type,
                "value": "N/A",
                "note": note,
                "status": status
            }
        
        except Timeout:
            logger.error(f"Instaclustr API timeout fetching metrics")
            return {
                "metric_type": metric_type,
                "value": "N/A",
                "note": "API timeout",
                "status": "timeout"
            }
        
        except RequestException as e:
            logger.warning(f"Could not fetch Instaclustr metrics: {e}")
            return {
                "metric_type": metric_type,
                "value": "N/A",
                "note": f"Request failed: {str(e)}",
                "status": "error"
            }
        
        except Exception as e:
            logger.error(f"Unexpected error fetching Instaclustr metrics: {e}")
            return {
                "metric_type": metric_type,
                "value": "N/A",
                "note": f"Unexpected error: {str(e)}",
                "status": "error"
            }

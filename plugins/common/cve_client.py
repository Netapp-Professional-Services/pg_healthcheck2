"""
NVD API client for retrieving CVE information.

Handles communication with the NIST National Vulnerability Database (NVD) API v2.0.
Includes rate limiting, pagination, and response parsing.

API Documentation: https://nvd.nist.gov/developers/vulnerabilities
"""
import requests
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class NVDClient:
    """
    Client for NIST National Vulnerability Database API v2.0.

    Provides methods to query CVEs by CPE identifier with automatic
    rate limiting and pagination support.

    Rate Limits:
        - Unauthenticated: 5 requests per 30 seconds
        - Authenticated: 50 requests per 30 seconds

    Example:
        client = NVDClient(api_key="your-key")
        cves = client.get_cves_by_cpe("cpe:2.3:a:postgresql:postgresql:16.2")
        for cve in cves:
            parsed = client.parse_cve_response(cve)
            print(f"{parsed['cve_id']}: {parsed['severity']}")
    """

    BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"

    # Rate limits (requests per 30 seconds)
    RATE_LIMIT_UNAUTHENTICATED = 5
    RATE_LIMIT_AUTHENTICATED = 50
    RATE_WINDOW = 30  # seconds

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        """
        Initialize NVD API client.

        Args:
            api_key: Optional NVD API key for higher rate limits
            timeout: Request timeout in seconds (default: 30)
        """
        self.api_key = api_key
        self.timeout = timeout
        self.rate_limit = self.RATE_LIMIT_AUTHENTICATED if api_key else self.RATE_LIMIT_UNAUTHENTICATED
        self._request_times = []

        logger.info(f"NVD client initialized (authenticated: {bool(api_key)}, rate limit: {self.rate_limit}/30s)")

    def _rate_limit_sleep(self):
        """Enforce rate limiting by sleeping if necessary."""
        now = time.time()

        # Remove requests older than rate window
        self._request_times = [t for t in self._request_times if now - t < self.RATE_WINDOW]

        # Check if we've hit the rate limit
        if len(self._request_times) >= self.rate_limit:
            # Calculate how long to sleep
            oldest_request = self._request_times[0]
            sleep_time = self.RATE_WINDOW - (now - oldest_request) + 1

            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
                self._request_times = []

        self._request_times.append(time.time())

    def get_cves_by_cpe(self, cpe_string: str, results_per_page: int = 50, max_results: Optional[int] = None, use_virtual_match: bool = True) -> List[Dict]:
        """
        Query CVEs by CPE identifier with automatic pagination.

        Args:
            cpe_string: CPE 2.3 identifier (e.g., "cpe:2.3:a:postgresql:postgresql:16.2")
            results_per_page: Max results per API call (default: 50, max: 2000)
            max_results: Optional limit on total results to retrieve
            use_virtual_match: Use virtualMatchString instead of cpeName (catches version ranges)

        Returns:
            List of CVE dictionaries from NVD API

        Raises:
            Exception: If API request fails

        Note:
            virtualMatchString is recommended as it catches CVEs with version ranges
            (e.g., "16.0 to 16.5") that don't match exact version CPE queries.
        """
        all_cves = []
        start_index = 0

        param_type = 'virtualMatchString' if use_virtual_match else 'cpeName'
        logger.info(f"Querying NVD for CPE: {cpe_string} (using {param_type})")

        while True:
            # Enforce rate limiting
            self._rate_limit_sleep()

            # Build request parameters
            # Use virtualMatchString for better version range matching
            params = {
                param_type: cpe_string,
                'resultsPerPage': min(results_per_page, 2000),  # API max is 2000
                'startIndex': start_index
            }

            # Add API key header if available
            headers = {}
            if self.api_key:
                headers['apiKey'] = self.api_key

            try:
                logger.debug(f"NVD API request: startIndex={start_index}, resultsPerPage={params['resultsPerPage']}")

                response = requests.get(
                    self.BASE_URL,
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )
                response.raise_for_status()

                data = response.json()
                vulnerabilities = data.get('vulnerabilities', [])

                if not vulnerabilities:
                    logger.debug("No more vulnerabilities in response, pagination complete")
                    break

                all_cves.extend(vulnerabilities)
                logger.debug(f"Retrieved {len(vulnerabilities)} CVEs (total: {len(all_cves)})")

                # Check if we've reached max_results
                if max_results and len(all_cves) >= max_results:
                    logger.info(f"Reached max_results limit: {max_results}")
                    all_cves = all_cves[:max_results]
                    break

                # Check if there are more results
                total_results = data.get('totalResults', 0)
                if start_index + len(vulnerabilities) >= total_results:
                    logger.debug(f"Retrieved all {total_results} results")
                    break

                start_index += len(vulnerabilities)

            except requests.exceptions.Timeout:
                raise Exception(f"NVD API request timed out after {self.timeout}s")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    raise Exception("NVD API access forbidden - check API key or rate limits")
                elif e.response.status_code == 503:
                    raise Exception("NVD API service unavailable - try again later")
                else:
                    raise Exception(f"NVD API HTTP error: {e.response.status_code} - {e.response.text}")
            except requests.exceptions.RequestException as e:
                raise Exception(f"NVD API request failed: {e}")
            except ValueError as e:
                raise Exception(f"Failed to parse NVD API response: {e}")

        logger.info(f"Retrieved {len(all_cves)} total CVEs for {cpe_string}")
        return all_cves

    def parse_cve_response(self, cve_data: Dict) -> Dict:
        """
        Parse NVD CVE response into simplified structure.

        Args:
            cve_data: Raw CVE data from NVD API

        Returns:
            {
                'cve_id': str,
                'published': str (ISO format),
                'last_modified': str (ISO format),
                'description': str,
                'severity': str (CRITICAL/HIGH/MEDIUM/LOW/UNKNOWN),
                'cvss_score': float,
                'cvss_vector': str,
                'cvss_version': str (v3.1/v3.0/v2.0),
                'references': List[str],
                'cpe_matches': List[str]
            }
        """
        cve = cve_data.get('cve', {})

        # Extract basic info
        cve_id = cve.get('id', 'N/A')
        published = cve.get('published', '')
        last_modified = cve.get('lastModified', '')

        # Extract description (prefer English)
        descriptions = cve.get('descriptions', [])
        description = 'No description available'
        for desc in descriptions:
            if desc.get('lang') == 'en':
                description = desc.get('value', 'No description available')
                break

        # Truncate long descriptions
        if len(description) > 500:
            description = description[:497] + '...'

        # Extract CVSS metrics (prefer v3.1 > v3.0 > v2.0)
        metrics = cve.get('metrics', {})
        cvss_score = 0.0
        cvss_vector = ''
        cvss_version = 'unknown'
        severity = 'UNKNOWN'

        # Try CVSS v3.1 first (most recent)
        if 'cvssMetricV31' in metrics and metrics['cvssMetricV31']:
            cvss_data = metrics['cvssMetricV31'][0]['cvssData']
            cvss_score = cvss_data.get('baseScore', 0.0)
            cvss_vector = cvss_data.get('vectorString', '')
            cvss_version = 'v3.1'
            severity = cvss_data.get('baseSeverity', 'UNKNOWN')

        # Fall back to CVSS v3.0
        elif 'cvssMetricV30' in metrics and metrics['cvssMetricV30']:
            cvss_data = metrics['cvssMetricV30'][0]['cvssData']
            cvss_score = cvss_data.get('baseScore', 0.0)
            cvss_vector = cvss_data.get('vectorString', '')
            cvss_version = 'v3.0'
            severity = cvss_data.get('baseSeverity', 'UNKNOWN')

        # Fall back to CVSS v2.0 (legacy)
        elif 'cvssMetricV2' in metrics and metrics['cvssMetricV2']:
            cvss_data = metrics['cvssMetricV2'][0]['cvssData']
            cvss_score = cvss_data.get('baseScore', 0.0)
            cvss_vector = cvss_data.get('vectorString', '')
            cvss_version = 'v2.0'

            # Map v2 score to severity categories (v2 doesn't have severity labels)
            if cvss_score >= 9.0:
                severity = 'CRITICAL'
            elif cvss_score >= 7.0:
                severity = 'HIGH'
            elif cvss_score >= 4.0:
                severity = 'MEDIUM'
            elif cvss_score > 0.0:
                severity = 'LOW'
            else:
                severity = 'UNKNOWN'

        # Extract reference URLs
        references = []
        for ref in cve.get('references', []):
            url = ref.get('url', '')
            if url:
                references.append(url)

        # Limit to first 10 references
        references = references[:10]

        # Extract CPE matches (vulnerable configurations)
        cpe_matches = []
        configurations = cve.get('configurations', [])
        for config in configurations:
            for node in config.get('nodes', []):
                for match in node.get('cpeMatch', []):
                    if match.get('vulnerable'):
                        cpe_criteria = match.get('criteria', '')
                        if cpe_criteria:
                            cpe_matches.append(cpe_criteria)

        return {
            'cve_id': cve_id,
            'published': published,
            'last_modified': last_modified,
            'description': description,
            'severity': severity,
            'cvss_score': cvss_score,
            'cvss_vector': cvss_vector,
            'cvss_version': cvss_version,
            'references': references,
            'cpe_matches': cpe_matches
        }

    def get_rate_limit_info(self) -> Dict:
        """
        Get current rate limit status.

        Returns:
            {
                'limit': int (requests per 30s),
                'window': int (seconds),
                'current_usage': int,
                'authenticated': bool
            }
        """
        now = time.time()
        recent_requests = [t for t in self._request_times if now - t < self.RATE_WINDOW]

        return {
            'limit': self.rate_limit,
            'window': self.RATE_WINDOW,
            'current_usage': len(recent_requests),
            'authenticated': bool(self.api_key)
        }

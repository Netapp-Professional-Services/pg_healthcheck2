"""
CVE check support mixin for database connectors.

Provides a reusable interface for querying CVE information from NVD
for database technologies and their extensions/plugins.

Usage:
    class PostgresConnector(CVECheckMixin):
        def __init__(self, settings):
            self.settings = settings
            self.version_info = {'version_string': '16.2', 'major_version': 16}
            self.initialize_cve_support()

        def get_installed_extensions(self):
            # Query database for installed extensions
            return [{'name': 'postgis', 'version': '3.4.0'}]
"""
import logging
from typing import Dict, List, Optional
from plugins.common.cve_client import NVDClient
from plugins.common.cpe_mapper import CPEMapper

logger = logging.getLogger(__name__)


class CVECheckMixin:
    """
    Mixin class that provides CVE checking functionality for database connectors.

    Requirements for connectors using this mixin:
        1. Must have `version_info` dict with 'version_string' and 'major_version'
        2. Should implement `get_technology_name_for_cve()` (default: uses technology_name)
        3. Optionally implement `get_installed_extensions()` for extension CVE checks

    Provides:
        - `initialize_cve_support()`: Setup CVE checking
        - `has_cve_support()`: Check if CVE functionality is available
        - `get_core_cves()`: Retrieve CVEs for core database
        - `get_extension_cves()`: Retrieve CVEs for installed extensions

    Example:
        connector = PostgresConnector(settings)
        if connector.has_cve_support():
            core_results = connector.get_core_cves()
            if core_results['status'] == 'success':
                print(f"Found {core_results['total_cves']} CVEs")
    """

    def initialize_cve_support(self, api_key: Optional[str] = None):
        """
        Initialize CVE support - call from connector __init__.

        Args:
            api_key: Optional NVD API key for higher rate limits
                    If not provided, will check settings['nvd_api_key']

        Note:
            Without API key: 5 requests per 30 seconds
            With API key: 50 requests per 30 seconds
        """
        # Get API key from settings if not provided
        if not api_key and hasattr(self, 'settings'):
            api_key = self.settings.get('nvd_api_key')

        try:
            self._nvd_client = NVDClient(api_key=api_key)
            self._cve_enabled = True
            logger.info(f"CVE support initialized (authenticated: {bool(api_key)})")
        except Exception as e:
            logger.warning(f"Failed to initialize CVE support: {e}")
            self._nvd_client = None
            self._cve_enabled = False

    def has_cve_support(self) -> bool:
        """
        Check if CVE checking is available.

        Returns:
            True if CVE client is initialized and ready
        """
        return (
            hasattr(self, '_cve_enabled') and
            self._cve_enabled and
            hasattr(self, '_nvd_client') and
            self._nvd_client is not None
        )

    def _filter_irrelevant_cves(self, cves: List[Dict], major_version: int, tech_name: str) -> List[Dict]:
        """
        Filter out false positive CVEs that don't actually affect the core technology.

        Filters based on:
        - Publication date (too old to be relevant)
        - Description content (mentions extensions/bindings)
        - CPE matches (non-core components)

        Args:
            cves: List of parsed CVE dicts
            major_version: Major version number of the technology
            tech_name: Technology name (e.g., 'postgres')

        Returns:
            Filtered list of relevant CVEs
        """
        from datetime import datetime
        import re

        if not cves:
            return []

        filtered_cves = []

        # Technology-specific keywords for EXTERNAL extensions/bindings
        # These indicate the CVE affects a NON-CORE component, not the core technology
        # NOTE: PL/Perl, PL/Python, PL/pgSQL are CORE PostgreSQL components - don't filter them!
        extension_keywords = {
            'postgres': [
                'ocaml', 'pl/php', 'plphp',  # PL/php is external extension
                # NOTE: pl/perl, pl/python are CORE, not extensions - removed from filter
                'psycopg', 'pg_partman', 'jdbc', 'odbc', 'npgsql', 'node-postgres',
                'pg-promise', 'python-psycopg', 'ruby-pg', 'php-pgsql',
                'bindings', 'driver', 'client library', 'adapter'
            ],
            'cassandra': ['driver', 'client', 'connector', 'jdbc', 'python-driver'],
            'kafka': ['client', 'python-kafka', 'kafka-python', 'librdkafka'],
            'mysql': ['connector', 'jdbc', 'odbc', 'python-mysql', 'mysqlclient'],
            'mongodb': ['driver', 'pymongo', 'motor', 'mongoose']
        }

        tech_keywords = extension_keywords.get(tech_name, [])

        # Get minimum publication year based on major version
        # For PostgreSQL: rough mapping of major version to release year
        version_year_mapping = {
            'postgres': {
                17: 2024, 16: 2023, 15: 2022, 14: 2021, 13: 2020,
                12: 2019, 11: 2018, 10: 2017, 9: 2010
            },
            # Add more mappings as needed
        }

        min_year = version_year_mapping.get(tech_name, {}).get(major_version, 2015)
        # Subtract 2 years to catch CVEs from previous versions that might affect current
        min_year = min_year - 2

        logger.debug(f"Filtering CVEs: min_year={min_year}, major_version={major_version}")

        for cve in cves:
            # Extract publication year
            published = cve.get('published', '')
            try:
                pub_year = int(published[:4]) if published else 0
            except (ValueError, IndexError):
                pub_year = 0

            # Filter 1: Too old (published before version era)
            if pub_year > 0 and pub_year < min_year:
                logger.debug(f"Filtered {cve.get('cve_id')}: too old ({pub_year} < {min_year})")
                continue

            # Filter 2: Description mentions extension/binding keywords
            description = cve.get('description', '').lower()
            is_extension = False
            for keyword in tech_keywords:
                if keyword.lower() in description:
                    logger.debug(f"Filtered {cve.get('cve_id')}: extension keyword '{keyword}' in description")
                    is_extension = True
                    break

            if is_extension:
                continue

            # Filter 3: CPE matches show non-core components
            cpe_matches = cve.get('cpe_matches', [])
            if cpe_matches:
                # Check if ANY CPE match is for the core technology
                has_core_match = False
                for cpe in cpe_matches:
                    cpe_lower = cpe.lower()
                    # Core technology should be in the CPE
                    if f':{tech_name}:{tech_name}:' in cpe_lower or f':{tech_name}:' in cpe_lower:
                        has_core_match = True
                        break

                # If no core matches, check if all matches are for other components
                if not has_core_match:
                    all_other_components = True
                    for cpe in cpe_matches:
                        cpe_lower = cpe.lower()
                        # Check if this is clearly a different component
                        is_other = any(keyword in cpe_lower for keyword in tech_keywords)
                        if not is_other:
                            all_other_components = False
                            break

                    if all_other_components:
                        logger.debug(f"Filtered {cve.get('cve_id')}: all CPE matches are non-core components")
                        continue

            # CVE passed all filters - include it
            filtered_cves.append(cve)

        logger.info(f"CVE filtering: {len(cves)} → {len(filtered_cves)} (removed {len(cves) - len(filtered_cves)} false positives)")

        return filtered_cves

    def get_technology_name_for_cve(self) -> str:
        """
        Get the technology name for CVE lookups.

        Override this method if the technology_name differs from CPE naming.

        Returns:
            Technology name (e.g., 'postgres', 'cassandra')

        Example:
            # PostgreSQL uses 'postgres' as technology_name
            def get_technology_name_for_cve(self):
                return 'postgres'  # Maps to CPE vendor 'postgresql'
        """
        if hasattr(self, 'technology_name'):
            return self.technology_name
        return 'unknown'

    def get_installed_extensions(self) -> List[Dict[str, str]]:
        """
        Get list of installed extensions/plugins for CVE checking.

        Override this method in connector to support extension CVE checks.

        Returns:
            List of dicts: [{'name': 'postgis', 'version': '3.4.0'}, ...]

        Example (PostgreSQL):
            def get_installed_extensions(self):
                query = "SELECT extname, extversion FROM pg_extension"
                results = self.execute_query(query)
                return [{'name': row[0], 'version': row[1]} for row in results]
        """
        return []

    def get_core_cves(self, max_results: Optional[int] = None) -> Dict:
        """
        Retrieve CVEs for the core database technology.

        Args:
            max_results: Optional limit on number of CVEs to retrieve

        Returns:
            {
                'status': 'success'|'error'|'unavailable',
                'technology': str,
                'version': str,
                'major_version': int,
                'cpe_string': str,
                'cves': List[Dict],
                'total_cves': int,
                'severity_counts': {'critical': int, 'high': int, 'medium': int, 'low': int},
                'error': str (if status='error')
            }

        Example:
            result = connector.get_core_cves()
            if result['status'] == 'success':
                print(f"Found {result['total_cves']} CVEs")
                for cve in result['cves']:
                    print(f"{cve['cve_id']}: {cve['severity']} ({cve['cvss_score']})")
        """
        if not self.has_cve_support():
            return {
                'status': 'unavailable',
                'error': 'CVE support not initialized. Call initialize_cve_support() first.'
            }

        # Extract version info from connector
        if not hasattr(self, 'version_info'):
            return {
                'status': 'error',
                'error': 'Connector does not provide version_info attribute'
            }

        version_string = self.version_info.get('version_string', '')
        major_version = self.version_info.get('major_version', 0)

        if not version_string or major_version == 0:
            return {
                'status': 'error',
                'error': f'Could not determine version (version_string="{version_string}", major_version={major_version})'
            }

        # Get technology name
        tech_name = self.get_technology_name_for_cve()

        # Check if technology is supported in CPE registry
        if not CPEMapper.is_supported(tech_name):
            return {
                'status': 'unavailable',
                'error': f'Technology "{tech_name}" not in CPE registry. Supported: {", ".join(CPEMapper.get_supported_technologies())}'
            }

        try:
            # Build CPE string with exact version
            # We use virtualMatchString parameter which catches CVEs with version ranges
            cpe_string = CPEMapper.build_cpe_string(tech_name, version_string)

            logger.info(f"Querying NVD for CVEs affecting {tech_name} {version_string}")

            # Query NVD API
            raw_cves = self._nvd_client.get_cves_by_cpe(cpe_string, max_results=max_results)

            # Parse CVEs
            parsed_cves = []
            for raw_cve in raw_cves:
                try:
                    parsed = self._nvd_client.parse_cve_response(raw_cve)
                    parsed_cves.append(parsed)
                except Exception as e:
                    logger.warning(f"Failed to parse CVE: {e}")

            # Filter out false positives
            parsed_cves = self._filter_irrelevant_cves(parsed_cves, major_version, tech_name)

            # Count by severity
            severity_counts = {
                'critical': 0,
                'high': 0,
                'medium': 0,
                'low': 0,
                'unknown': 0
            }

            for cve in parsed_cves:
                severity = cve.get('severity', 'UNKNOWN').lower()
                if severity in severity_counts:
                    severity_counts[severity] += 1
                else:
                    severity_counts['unknown'] += 1

            logger.info(f"Found {len(parsed_cves)} CVEs for {tech_name} {version_string}")

            return {
                'status': 'success',
                'technology': tech_name,
                'version': version_string,
                'major_version': major_version,
                'cpe_string': cpe_string,
                'cves': parsed_cves,
                'total_cves': len(parsed_cves),
                'severity_counts': severity_counts
            }

        except ValueError as e:
            # CPE mapping error
            logger.error(f"CPE mapping error: {e}")
            return {
                'status': 'error',
                'technology': tech_name,
                'version': version_string,
                'error': str(e)
            }
        except Exception as e:
            # API or network error
            logger.error(f"Error retrieving CVEs: {e}")
            return {
                'status': 'error',
                'technology': tech_name,
                'version': version_string,
                'error': str(e)
            }

    def get_extension_cves(self, max_results_per_extension: Optional[int] = None) -> List[Dict]:
        """
        Retrieve CVEs for installed extensions/plugins.

        Calls get_installed_extensions() to discover extensions, then
        queries NVD for each extension.

        Args:
            max_results_per_extension: Optional limit per extension

        Returns:
            List of CVE result dicts (same structure as get_core_cves())

        Example:
            extension_results = connector.get_extension_cves()
            for ext_result in extension_results:
                if ext_result['status'] == 'success':
                    print(f"{ext_result['extension_name']}: {ext_result['total_cves']} CVEs")
        """
        if not self.has_cve_support():
            logger.debug("CVE support not available, skipping extension CVE check")
            return []

        # Get installed extensions from connector
        extensions = self.get_installed_extensions()

        if not extensions:
            logger.debug("No extensions found to check for CVEs")
            return []

        logger.info(f"Checking CVEs for {len(extensions)} extension(s)")

        results = []

        for ext in extensions:
            ext_name = ext.get('name', 'unknown')
            ext_version = ext.get('version', '')

            if not ext_version:
                logger.warning(f"Extension '{ext_name}' has no version, skipping CVE check")
                results.append({
                    'status': 'skipped',
                    'extension_name': ext_name,
                    'error': 'No version information available'
                })
                continue

            # Check if extension is in CPE registry
            if not CPEMapper.is_supported(ext_name):
                logger.debug(f"Extension '{ext_name}' not in CPE registry, skipping")
                results.append({
                    'status': 'unavailable',
                    'extension_name': ext_name,
                    'version': ext_version,
                    'error': f'No CPE mapping for extension "{ext_name}"'
                })
                continue

            try:
                # Build CPE string for extension
                cpe_string = CPEMapper.build_cpe_string(ext_name, ext_version)

                logger.debug(f"Querying CVEs for extension {ext_name} {ext_version}")

                # Query NVD API
                raw_cves = self._nvd_client.get_cves_by_cpe(cpe_string, max_results=max_results_per_extension)

                # Parse CVEs
                parsed_cves = []
                for raw_cve in raw_cves:
                    try:
                        parsed = self._nvd_client.parse_cve_response(raw_cve)
                        parsed_cves.append(parsed)
                    except Exception as e:
                        logger.warning(f"Failed to parse CVE for {ext_name}: {e}")

                # Count by severity
                severity_counts = {
                    'critical': sum(1 for cve in parsed_cves if cve.get('severity') == 'CRITICAL'),
                    'high': sum(1 for cve in parsed_cves if cve.get('severity') == 'HIGH'),
                    'medium': sum(1 for cve in parsed_cves if cve.get('severity') == 'MEDIUM'),
                    'low': sum(1 for cve in parsed_cves if cve.get('severity') == 'LOW'),
                    'unknown': sum(1 for cve in parsed_cves if cve.get('severity') == 'UNKNOWN')
                }

                results.append({
                    'status': 'success',
                    'extension_name': ext_name,
                    'version': ext_version,
                    'cpe_string': cpe_string,
                    'cves': parsed_cves,
                    'total_cves': len(parsed_cves),
                    'severity_counts': severity_counts
                })

                logger.info(f"Found {len(parsed_cves)} CVEs for extension {ext_name} {ext_version}")

            except Exception as e:
                logger.error(f"Error checking CVEs for extension '{ext_name}': {e}")
                results.append({
                    'status': 'error',
                    'extension_name': ext_name,
                    'version': ext_version,
                    'error': str(e)
                })

        return results

    def get_all_cves(self, max_core_results: Optional[int] = None, max_extension_results: Optional[int] = None) -> Dict:
        """
        Convenience method to get both core and extension CVEs.

        Args:
            max_core_results: Optional limit for core CVEs
            max_extension_results: Optional limit per extension

        Returns:
            {
                'core': Dict (from get_core_cves()),
                'extensions': List[Dict] (from get_extension_cves()),
                'summary': {
                    'total_cves': int,
                    'core_cves': int,
                    'extension_cves': int,
                    'critical_count': int,
                    'high_count': int,
                    'medium_count': int,
                    'low_count': int
                }
            }
        """
        core_result = self.get_core_cves(max_results=max_core_results)
        extension_results = self.get_extension_cves(max_results_per_extension=max_extension_results)

        # Calculate summary
        core_cves = core_result.get('total_cves', 0) if core_result.get('status') == 'success' else 0
        extension_cves = sum(
            ext.get('total_cves', 0)
            for ext in extension_results
            if ext.get('status') == 'success'
        )

        core_severity = core_result.get('severity_counts', {}) if core_result.get('status') == 'success' else {}

        critical_count = core_severity.get('critical', 0)
        high_count = core_severity.get('high', 0)
        medium_count = core_severity.get('medium', 0)
        low_count = core_severity.get('low', 0)

        for ext in extension_results:
            if ext.get('status') == 'success':
                ext_severity = ext.get('severity_counts', {})
                critical_count += ext_severity.get('critical', 0)
                high_count += ext_severity.get('high', 0)
                medium_count += ext_severity.get('medium', 0)
                low_count += ext_severity.get('low', 0)

        return {
            'core': core_result,
            'extensions': extension_results,
            'summary': {
                'total_cves': core_cves + extension_cves,
                'core_cves': core_cves,
                'extension_cves': extension_cves,
                'critical_count': critical_count,
                'high_count': high_count,
                'medium_count': medium_count,
                'low_count': low_count
            }
        }

    def get_cve_rate_limit_info(self) -> Dict:
        """
        Get current NVD API rate limit status.

        Returns:
            {
                'limit': int,
                'window': int (seconds),
                'current_usage': int,
                'authenticated': bool
            }
        """
        if not self.has_cve_support():
            return {
                'error': 'CVE support not initialized'
            }

        return self._nvd_client.get_rate_limit_info()

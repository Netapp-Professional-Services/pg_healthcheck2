"""
CPE (Common Platform Enumeration) mapper for database technologies.

Maps database technology metadata to CPE 2.3 identifiers for use with
vulnerability databases like NVD.

CPE 2.3 Format:
    cpe:2.3:part:vendor:product:version:update:edition:language:sw_edition:target_sw:target_hw:other

Documentation: https://csrc.nist.gov/publications/detail/nistir/7695/final
"""
import re
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class CPEMapper:
    """
    Maps database technology metadata to CPE 2.3 identifiers.

    For databases, we typically use the format:
        cpe:2.3:a:vendor:product:version:*:*:*:*:*:*:*

    Where:
        - part: 'a' (application)
        - vendor: Database vendor name
        - product: Database product name
        - version: Specific version (e.g., "16.2")
        - All other fields: wildcards (*)

    Example:
        # PostgreSQL 16.2
        cpe = CPEMapper.build_cpe_string('postgres', '16.2')
        # Returns: "cpe:2.3:a:postgresql:postgresql:16.2:*:*:*:*:*:*:*"

        # Query all PostgreSQL 16.x versions
        pattern = CPEMapper.build_cpe_range_query('postgres', 16)
        # Returns: "cpe:2.3:a:postgresql:postgresql:16.*:*:*:*:*:*:*:*"
    """

    # Technology metadata registry
    # Maps technology names to CPE vendor/product identifiers
    TECH_REGISTRY = {
        'postgres': {
            'vendor': 'postgresql',
            'product': 'postgresql',
            'part': 'a',
            'aliases': ['postgresql']
        },
        'postgresql': {
            'vendor': 'postgresql',
            'product': 'postgresql',
            'part': 'a'
        },
        'cassandra': {
            'vendor': 'apache',
            'product': 'cassandra',
            'part': 'a'
        },
        'kafka': {
            'vendor': 'apache',
            'product': 'kafka',
            'part': 'a'
        },
        'mysql': {
            'vendor': 'oracle',
            'product': 'mysql',
            'part': 'a',
            'aliases': ['mariadb']
        },
        'mariadb': {
            'vendor': 'mariadb',
            'product': 'mariadb',
            'part': 'a'
        },
        'mongodb': {
            'vendor': 'mongodb',
            'product': 'mongodb',
            'part': 'a'
        },
        'redis': {
            'vendor': 'redis',
            'product': 'redis',
            'part': 'a'
        },
        'valkey': {
            'vendor': 'valkey',
            'product': 'valkey',
            'part': 'a'
        },
        'opensearch': {
            'vendor': 'opensearch',
            'product': 'opensearch',
            'part': 'a',
            'aliases': ['elasticsearch']
        },
        'elasticsearch': {
            'vendor': 'elastic',
            'product': 'elasticsearch',
            'part': 'a'
        },
        'clickhouse': {
            'vendor': 'clickhouse',
            'product': 'clickhouse',
            'part': 'a'
        },
        # Common PostgreSQL extensions
        'postgis': {
            'vendor': 'postgis',
            'product': 'postgis',
            'part': 'a'
        },
        'timescaledb': {
            'vendor': 'timescale',
            'product': 'timescaledb',
            'part': 'a'
        },
        'citus': {
            'vendor': 'microsoft',
            'product': 'citus',
            'part': 'a'
        },
        'pg_partman': {
            'vendor': 'pgpartman_project',
            'product': 'pg_partman',
            'part': 'a'
        },
        # Common Cassandra-related
        'dse': {
            'vendor': 'datastax',
            'product': 'datastax_enterprise',
            'part': 'a'
        }
    }

    @classmethod
    def normalize_technology_name(cls, technology: str) -> str:
        """
        Normalize technology name to match registry.

        Args:
            technology: Technology name (e.g., 'PostgreSQL', 'POSTGRES', 'postgres')

        Returns:
            Normalized lowercase name
        """
        return technology.lower().strip()

    @classmethod
    def normalize_version(cls, version: str) -> str:
        """
        Normalize version string for CPE usage.

        Removes common suffixes and prefixes:
        - "PostgreSQL 16.2" -> "16.2"
        - "v16.2" -> "16.2"
        - "16.2-debian" -> "16.2"

        Args:
            version: Raw version string

        Returns:
            Normalized version string
        """
        # Remove common prefixes
        version = re.sub(r'^(v|version|release)\s*', '', version, flags=re.IGNORECASE)

        # Extract major.minor pattern (e.g., "16.2" from "PostgreSQL 16.2 (Debian...)")
        match = re.search(r'(\d+\.\d+(?:\.\d+)?)', version)
        if match:
            return match.group(1)

        # Fallback: return cleaned version
        return version.strip()

    @classmethod
    def is_supported(cls, technology: str) -> bool:
        """
        Check if technology is in the CPE registry.

        Args:
            technology: Technology name

        Returns:
            True if technology is supported
        """
        tech_lower = cls.normalize_technology_name(technology)
        return tech_lower in cls.TECH_REGISTRY

    @classmethod
    def get_vendor_product(cls, technology: str) -> Optional[Tuple[str, str]]:
        """
        Get vendor and product names for a technology.

        Args:
            technology: Technology name

        Returns:
            (vendor, product) tuple or None if not found
        """
        tech_lower = cls.normalize_technology_name(technology)

        if tech_lower not in cls.TECH_REGISTRY:
            return None

        meta = cls.TECH_REGISTRY[tech_lower]
        return (meta['vendor'], meta['product'])

    @classmethod
    def build_cpe_string(cls, technology: str, version: str, **kwargs) -> str:
        """
        Build CPE 2.3 string for a database technology.

        Args:
            technology: Technology name (e.g., 'postgres', 'cassandra')
            version: Version string (e.g., '16.2', '4.1.0')
            **kwargs: Optional CPE components:
                - update: Update/patch level
                - edition: Edition (e.g., 'enterprise')
                - language: Language
                - sw_edition: Software edition
                - target_sw: Target software platform
                - target_hw: Target hardware platform
                - other: Other information

        Returns:
            CPE 2.3 string

        Raises:
            ValueError: If technology is not in registry

        Example:
            >>> CPEMapper.build_cpe_string('postgres', '16.2')
            'cpe:2.3:a:postgresql:postgresql:16.2:*:*:*:*:*:*:*'
        """
        tech_lower = cls.normalize_technology_name(technology)

        if tech_lower not in cls.TECH_REGISTRY:
            raise ValueError(
                f"Technology '{technology}' not in CPE registry. "
                f"Supported: {', '.join(sorted(cls.TECH_REGISTRY.keys()))}"
            )

        meta = cls.TECH_REGISTRY[tech_lower]
        normalized_version = cls.normalize_version(version)

        # Build CPE string with wildcards for unspecified components
        cpe = f"cpe:2.3:{meta['part']}:{meta['vendor']}:{meta['product']}:{normalized_version}"

        # Add optional components (default to wildcard)
        cpe += f":{kwargs.get('update', '*')}"
        cpe += f":{kwargs.get('edition', '*')}"
        cpe += f":{kwargs.get('language', '*')}"
        cpe += f":{kwargs.get('sw_edition', '*')}"
        cpe += f":{kwargs.get('target_sw', '*')}"
        cpe += f":{kwargs.get('target_hw', '*')}"
        cpe += f":{kwargs.get('other', '*')}"

        logger.debug(f"Built CPE string: {cpe}")
        return cpe

    @classmethod
    def build_cpe_range_query(cls, technology: str, major_version: int, include_minor: Optional[int] = None) -> str:
        """
        Build CPE query pattern for a version range.

        Useful for querying all CVEs affecting a major version family.

        Args:
            technology: Technology name
            major_version: Major version number (e.g., 16 for PostgreSQL 16.x)
            include_minor: Optional minor version (e.g., 2 for 16.2.x)

        Returns:
            CPE pattern string with wildcards

        Example:
            >>> CPEMapper.build_cpe_range_query('postgres', 16)
            'cpe:2.3:a:postgresql:postgresql:16.*:*:*:*:*:*:*:*'

            >>> CPEMapper.build_cpe_range_query('postgres', 16, 2)
            'cpe:2.3:a:postgresql:postgresql:16.2.*:*:*:*:*:*:*:*'
        """
        tech_lower = cls.normalize_technology_name(technology)

        if tech_lower not in cls.TECH_REGISTRY:
            raise ValueError(f"Technology '{technology}' not in CPE registry")

        meta = cls.TECH_REGISTRY[tech_lower]

        # Build version pattern
        if include_minor is not None:
            version_pattern = f"{major_version}.{include_minor}.*"
        else:
            version_pattern = f"{major_version}.*"

        cpe_pattern = f"cpe:2.3:{meta['part']}:{meta['vendor']}:{meta['product']}:{version_pattern}:*:*:*:*:*:*:*"

        logger.debug(f"Built CPE range query: {cpe_pattern}")
        return cpe_pattern

    @classmethod
    def extract_version_from_cpe(cls, cpe_string: str) -> Optional[str]:
        """
        Extract version component from CPE string.

        Args:
            cpe_string: CPE 2.3 string

        Returns:
            Version string or None if parsing fails

        Example:
            >>> CPEMapper.extract_version_from_cpe('cpe:2.3:a:postgresql:postgresql:16.2:*:*:*:*:*:*:*')
            '16.2'
        """
        parts = cpe_string.split(':')
        if len(parts) >= 6:
            return parts[5]  # Version is 6th component (0-indexed: 5)
        return None

    @classmethod
    def parse_cpe_string(cls, cpe_string: str) -> Dict[str, str]:
        """
        Parse CPE 2.3 string into components.

        Args:
            cpe_string: CPE 2.3 string

        Returns:
            Dictionary of CPE components

        Example:
            >>> CPEMapper.parse_cpe_string('cpe:2.3:a:postgresql:postgresql:16.2:*:*:*:*:*:*:*')
            {
                'cpe_version': '2.3',
                'part': 'a',
                'vendor': 'postgresql',
                'product': 'postgresql',
                'version': '16.2',
                'update': '*',
                'edition': '*',
                ...
            }
        """
        parts = cpe_string.split(':')

        if len(parts) < 6:
            raise ValueError(f"Invalid CPE string: {cpe_string}")

        # Pad with wildcards if needed
        while len(parts) < 13:
            parts.append('*')

        return {
            'cpe_version': parts[1],
            'part': parts[2],
            'vendor': parts[3],
            'product': parts[4],
            'version': parts[5],
            'update': parts[6],
            'edition': parts[7],
            'language': parts[8],
            'sw_edition': parts[9],
            'target_sw': parts[10],
            'target_hw': parts[11],
            'other': parts[12] if len(parts) > 12 else '*'
        }

    @classmethod
    def get_supported_technologies(cls) -> List[str]:
        """
        Get list of all supported technologies in registry.

        Returns:
            Sorted list of technology names
        """
        return sorted(cls.TECH_REGISTRY.keys())

    @classmethod
    def add_custom_technology(cls, tech_name: str, vendor: str, product: str, part: str = 'a', aliases: Optional[List[str]] = None):
        """
        Add a custom technology to the registry at runtime.

        Useful for plugins that need to register extensions or custom databases.

        Args:
            tech_name: Technology identifier (lowercase)
            vendor: CPE vendor name
            product: CPE product name
            part: CPE part (default: 'a' for application)
            aliases: Optional list of alias names

        Example:
            >>> CPEMapper.add_custom_technology('my_extension', 'mycompany', 'my_extension')
        """
        tech_lower = tech_name.lower()

        if tech_lower in cls.TECH_REGISTRY:
            logger.warning(f"Technology '{tech_name}' already exists in registry, overwriting")

        cls.TECH_REGISTRY[tech_lower] = {
            'vendor': vendor,
            'product': product,
            'part': part,
            'aliases': aliases or []
        }

        logger.info(f"Added custom technology to CPE registry: {tech_name}")

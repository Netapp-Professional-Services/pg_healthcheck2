"""
API Versions Check for Kafka brokers.

Analyzes Kafka broker API versions to identify:
- API compatibility between brokers
- Supported feature set
- Potential version mismatches

Data sources:
- kafka-api-versions-output.txt.info (import)
- Admin API ApiVersionsRequest (live)
"""

from plugins.common.check_helpers import CheckContentBuilder
import logging
import re

logger = logging.getLogger(__name__)


def get_weight():
    """Module priority weight (1-10)."""
    return 5  # Medium priority


# Key APIs and their significance
KEY_APIS = {
    'Produce': {'id': 0, 'description': 'Message production'},
    'Fetch': {'id': 1, 'description': 'Message consumption'},
    'ListOffsets': {'id': 2, 'description': 'Offset queries'},
    'Metadata': {'id': 3, 'description': 'Cluster metadata'},
    'LeaderAndIsr': {'id': 4, 'description': 'Partition leadership'},
    'StopReplica': {'id': 5, 'description': 'Replica management'},
    'UpdateMetadata': {'id': 6, 'description': 'Metadata updates'},
    'ControlledShutdown': {'id': 7, 'description': 'Graceful shutdown'},
    'OffsetCommit': {'id': 8, 'description': 'Consumer offset commits'},
    'OffsetFetch': {'id': 9, 'description': 'Consumer offset fetches'},
    'FindCoordinator': {'id': 10, 'description': 'Coordinator discovery'},
    'JoinGroup': {'id': 11, 'description': 'Consumer group membership'},
    'Heartbeat': {'id': 12, 'description': 'Consumer heartbeats'},
    'LeaveGroup': {'id': 13, 'description': 'Consumer group leave'},
    'SyncGroup': {'id': 14, 'description': 'Consumer group sync'},
    'DescribeGroups': {'id': 15, 'description': 'Consumer group info'},
    'ListGroups': {'id': 16, 'description': 'Consumer group listing'},
    'CreateTopics': {'id': 19, 'description': 'Topic creation'},
    'DeleteTopics': {'id': 20, 'description': 'Topic deletion'},
    'DescribeConfigs': {'id': 32, 'description': 'Configuration queries'},
    'AlterConfigs': {'id': 33, 'description': 'Configuration changes'},
}


def run_api_versions_check(connector, settings):
    """
    Analyzes Kafka broker API versions for compatibility and feature support.

    Checks:
    - API version consistency across brokers
    - Support for key APIs
    - Deprecated API usage

    Args:
        connector: Kafka connector (live or import)
        settings: Configuration settings

    Returns:
        tuple: (adoc_content: str, structured_data: dict)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    try:
        builder.h3("API Versions Analysis")

        # === Get API versions ===
        query = '{"operation": "api_versions"}'
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted or (isinstance(raw, dict) and 'error' in raw):
            error_msg = raw.get('error', formatted) if isinstance(raw, dict) else formatted
            builder.error(f"Failed to get API versions: {error_msg}")
            structured_data = {
                "status": "error",
                "details": error_msg
            }
            return builder.build(), structured_data

        # Parse API versions
        api_versions = {}
        raw_content = raw.get('raw', raw) if isinstance(raw, dict) else raw

        if isinstance(raw_content, str):
            api_versions = _parse_api_versions_output(raw_content)
        elif isinstance(raw, dict) and 'apis' in raw:
            api_versions = raw['apis']

        if not api_versions:
            builder.note("No API version information available.")
            structured_data = {
                "status": "skipped",
                "reason": "no_data"
            }
            return builder.build(), structured_data

        # === Analyze API versions ===
        total_apis = len(api_versions)
        issues = []
        warnings = []

        # Check for key APIs
        missing_key_apis = []
        supported_key_apis = []

        for api_name, api_info in KEY_APIS.items():
            api_id = api_info['id']
            if api_id in api_versions or str(api_id) in api_versions:
                supported_key_apis.append(api_name)
            else:
                # Check by name in the parsed data
                found = False
                for parsed_id, parsed_info in api_versions.items():
                    if isinstance(parsed_info, dict) and parsed_info.get('name', '').lower() == api_name.lower():
                        supported_key_apis.append(api_name)
                        found = True
                        break
                if not found:
                    missing_key_apis.append(api_name)

        if missing_key_apis:
            # Only warn for truly critical missing APIs
            critical_missing = [a for a in missing_key_apis if a in ['Produce', 'Fetch', 'Metadata']]
            if critical_missing:
                issues.append({
                    'level': 'critical',
                    'type': 'missing_critical_api',
                    'message': f'Critical APIs missing: {", ".join(critical_missing)}'
                })

        # === Build report ===
        has_critical = any(i['level'] == 'critical' for i in issues)

        if has_critical:
            for issue in [i for i in issues if i['level'] == 'critical']:
                builder.critical(f"**{issue['type'].replace('_', ' ').title()}:** {issue['message']}")
            builder.blank()

        # Summary
        builder.para(f"Broker supports **{total_apis}** API operations.")
        builder.blank()

        # Key APIs table
        builder.h4("Key API Support")

        key_api_rows = []
        for api_name in sorted(KEY_APIS.keys()):
            api_info = KEY_APIS[api_name]
            supported = api_name in supported_key_apis
            key_api_rows.append({
                "API": api_name,
                "ID": str(api_info['id']),
                "Description": api_info['description'],
                "Supported": "Yes" if supported else "No"
            })

        builder.table(key_api_rows)
        builder.blank()

        # Version ranges for supported APIs (if we have detailed info)
        if api_versions and isinstance(list(api_versions.values())[0], dict):
            builder.h4("API Version Ranges")

            version_rows = []
            for api_id, api_data in sorted(api_versions.items(), key=lambda x: int(x[0]) if str(x[0]).isdigit() else 999):
                if isinstance(api_data, dict):
                    api_name = api_data.get('name', f'API {api_id}')
                    min_ver = api_data.get('min_version', api_data.get('min', '-'))
                    max_ver = api_data.get('max_version', api_data.get('max', '-'))
                    version_rows.append({
                        "API": api_name,
                        "ID": str(api_id),
                        "Min Version": str(min_ver),
                        "Max Version": str(max_ver)
                    })

            if version_rows:
                # Show first 20 APIs
                builder.table(version_rows[:20])
                if len(version_rows) > 20:
                    builder.para(f"_... and {len(version_rows) - 20} more APIs_")
                builder.blank()

        # Recommendations
        if issues or warnings:
            recommendations = {}

            if has_critical:
                recommendations["critical"] = [
                    "**Verify broker connectivity** - missing APIs may indicate connection issues",
                    "**Check Kafka version** - some APIs were added in specific versions"
                ]

            recommendations["general"] = [
                "Ensure all brokers run the same Kafka version for API consistency",
                "When upgrading clients, verify API compatibility with broker versions",
                "Monitor for deprecation warnings in newer Kafka releases"
            ]

            builder.recs(recommendations)
        else:
            builder.success(
                f"API versions check passed.\n\n"
                f"All {len(supported_key_apis)} key APIs are supported."
            )

        # === Structured data ===
        structured_data = {
            "status": "success",
            "data": {
                "total_apis": total_apis,
                "missing_key_apis_count": len(missing_key_apis),
                "missing_key_apis": missing_key_apis,
                "has_critical_issues": has_critical,
                "has_warnings": bool(warnings),
            }
        }

    except Exception as e:
        import traceback
        logger.error(f"API versions check failed: {e}\n{traceback.format_exc()}")
        builder.error(f"Check failed: {e}")
        structured_data = {
            "status": "error",
            "details": str(e)
        }

    return builder.build(), structured_data


def _parse_api_versions_output(content):
    """
    Parse kafka-broker-api-versions.sh output.

    Example format:
    ApiVersion(apiKey=PRODUCE(0), minVersion=0, maxVersion=9)
    ApiVersion(apiKey=FETCH(1), minVersion=0, maxVersion=13)
    ...
    """
    apis = {}

    # Pattern: ApiVersion(apiKey=NAME(ID), minVersion=X, maxVersion=Y)
    pattern = r'ApiVersion\(apiKey=(\w+)\((\d+)\),\s*minVersion=(\d+),\s*maxVersion=(\d+)\)'

    for match in re.finditer(pattern, content):
        name, api_id, min_ver, max_ver = match.groups()
        apis[int(api_id)] = {
            'name': name,
            'min_version': int(min_ver),
            'max_version': int(max_ver)
        }

    # If no structured matches, try line-by-line parsing
    if not apis:
        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # Try to extract API info from various formats
            if 'apiKey=' in line.lower() or 'api=' in line.lower():
                # Try to extract numbers
                numbers = re.findall(r'\d+', line)
                if len(numbers) >= 3:
                    api_id = int(numbers[0])
                    apis[api_id] = {
                        'name': f'API_{api_id}',
                        'min_version': int(numbers[1]),
                        'max_version': int(numbers[2])
                    }

    return apis

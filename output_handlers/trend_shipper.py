"""Handles shipping of health check results to a persistent backend.

This module is responsible for taking the final results of a health check
and sending them to a storage destination for trend analysis. It is driven
by the `config/trends.yaml` file and acts as a dispatcher, supporting
different backends like PostgreSQL or a generic HTTP API.
"""

import yaml
import psycopg2
import requests
import json
import re
from decimal import Decimal
from datetime import datetime, timedelta
from utils.json_utils import UniversalJSONEncoder, safe_json_dumps

def load_config(config_path='config/trends.yaml'):
    """Loads the trend shipper configuration from a YAML file.

    Args:
        config_path (str, optional): The path to the trend shipper
            configuration file. Defaults to 'config/trends.yaml'.

    Returns:
        dict | None: A dictionary with the configuration if the file is
        found and valid, otherwise None.
    """

    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print("Log: trends.yaml not found. Skipping trend analysis.")
        return None
    except Exception as e:
        print(f"Error loading trends.yaml: {e}")
        return None


def _extract_db_version(structured_findings):
    """Extract database version from structured findings.

    Args:
        structured_findings (dict): The structured findings dictionary

    Returns:
        str: The database version string, or None if not found
    """
    # Look for version in common locations across different technologies
    version_locations = [
        ('db_metadata', 'version'),  # ClickHouse connector.get_db_metadata()
        ('db_version', None),  # Direct field
        ('database_version', None),
        ('version', None),
        ('server_info', 'version'),
        ('cluster_info', 'version'),
        ('system_info', 'version'),
    ]

    for location in version_locations:
        if len(location) == 2 and location[1]:
            # Nested lookup
            parent = structured_findings.get(location[0], {})
            if isinstance(parent, dict):
                version = parent.get(location[1])
                if version:
                    return str(version)
        else:
            # Top-level lookup
            version = structured_findings.get(location[0])
            if version:
                return str(version)

    return None


def _parse_version_components(version_string):
    """Parse version string into major and minor components.

    Args:
        version_string (str): Version string like "16.3", "4.1.5", "25.1.2.15"

    Returns:
        tuple: (major, minor) version numbers, or (None, None) if parsing fails
    """
    if not version_string:
        return None, None

    # Extract numeric components using regex
    match = re.match(r'(\d+)\.(\d+)', str(version_string))
    if match:
        try:
            return int(match.group(1)), int(match.group(2))
        except ValueError:
            pass

    return None, None


def _extract_cluster_name(target_info, structured_findings):
    """Extract cluster name from target info or findings.

    Args:
        target_info (dict): Target system information
        structured_findings (dict): The structured findings dictionary

    Returns:
        str: The cluster name, or None if not found
    """
    # Check target_info first
    cluster_name = target_info.get('cluster_name')
    if cluster_name:
        return cluster_name

    # Check structured findings
    name_locations = [
        ('db_metadata', 'cluster_name'),  # ClickHouse connector.get_db_metadata()
        ('cluster_name', None),
        ('cluster_info', 'name'),
        ('cluster_info', 'cluster_name'),
        ('system_info', 'cluster_name'),
    ]

    for location in name_locations:
        if len(location) == 2 and location[1]:
            parent = structured_findings.get(location[0], {})
            if isinstance(parent, dict):
                name = parent.get(location[1])
                if name:
                    return str(name)
        else:
            name = structured_findings.get(location[0])
            if name:
                return str(name)

    return None


def _extract_node_count(structured_findings):
    """Extract node count from structured findings.

    Args:
        structured_findings (dict): The structured findings dictionary

    Returns:
        int: The number of nodes, or None if not found
    """
    count_locations = [
        ('db_metadata', 'nodes'),  # ClickHouse connector.get_db_metadata()
        ('node_count', None),
        ('cluster_info', 'node_count'),
        ('cluster_info', 'nodes'),
        ('cluster_size', None),
    ]

    for location in count_locations:
        if len(location) == 2 and location[1]:
            parent = structured_findings.get(location[0], {})
            if isinstance(parent, dict):
                count = parent.get(location[1])
                if count is not None:
                    try:
                        return int(count)
                    except (ValueError, TypeError):
                        pass
        else:
            count = structured_findings.get(location[0])
            if count is not None:
                try:
                    return int(count)
                except (ValueError, TypeError):
                    pass

    return None


def _build_infrastructure_metadata(target_info, structured_findings):
    """Build infrastructure metadata JSONB from available data.

    Args:
        target_info (dict): Target system information
        structured_findings (dict): The structured findings dictionary

    Returns:
        dict: Infrastructure metadata dictionary
    """
    metadata = {}

    # Add cloud provider info if available
    if 'cloud_provider' in target_info:
        metadata['cloud_provider'] = target_info['cloud_provider']
    if 'region' in target_info:
        metadata['region'] = target_info['region']
    if 'availability_zone' in target_info:
        metadata['availability_zone'] = target_info['availability_zone']

    # Add instance/resource info
    if 'instance_type' in target_info:
        metadata['instance_type'] = target_info['instance_type']

    # Extract from structured findings
    infra_locations = [
        'infrastructure_info',
        'cloud_metadata',
        'deployment_info',
    ]

    for location in infra_locations:
        info = structured_findings.get(location, {})
        if isinstance(info, dict):
            metadata.update(info)

    return metadata if metadata else None


def _calculate_health_score(analysis_results):
    """Calculate overall health score from triggered rules.

    Score is calculated as: 100 - (sum of severity scores)
    Capped at 0 minimum.

    Args:
        analysis_results (dict): Results from generate_dynamic_prompt()

    Returns:
        float: Health score from 0.0 to 100.0, or None if no analysis results
    """
    if not analysis_results:
        return None

    total_deductions = 0

    # Critical issues: 10 points each
    critical_count = len(analysis_results.get('critical_issues', []))
    total_deductions += critical_count * 10

    # High priority issues: 7 points each
    high_count = len(analysis_results.get('high_priority_issues', []))
    total_deductions += high_count * 7

    # Medium priority issues: 5 points each
    medium_count = len(analysis_results.get('medium_priority_issues', []))
    total_deductions += medium_count * 5

    # Low priority issues: 2 points each
    low_count = len(analysis_results.get('low_priority_issues', []))
    total_deductions += low_count * 2

    health_score = max(0.0, 100.0 - total_deductions)
    return health_score


def _store_triggered_rules(cursor, run_id, analysis_results):
    """Store which rules were triggered during analysis.
    
    This function extracts the triggered rules from the analysis results
    and inserts them into the health_check_triggered_rules table for
    future trend analysis and querying.
    
    Args:
        cursor: PostgreSQL cursor object
        run_id (int): The ID of the health check run
        analysis_results (dict): The results from generate_dynamic_prompt(),
            containing critical_issues, high_priority_issues, and 
            medium_priority_issues lists.
    
    Returns:
        int: Total number of triggered rules stored
    """
    
    if not analysis_results:
        return 0
    
    insert_sql = """
        INSERT INTO health_check_triggered_rules (
            run_id, rule_config_name, metric_name, severity_level, 
            severity_score, reasoning, recommendations, triggered_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    total_stored = 0
    
    # Process critical issues
    for issue in analysis_results.get('critical_issues', []):
        try:
            cursor.execute(insert_sql, (
                run_id,
                issue.get('rule_config_name', 'unknown'),
                issue.get('metric', 'unknown'),
                'critical',
                issue.get('analysis', {}).get('score', 10),
                issue.get('analysis', {}).get('reasoning', ''),
                json.dumps(issue.get('analysis', {}).get('recommendations', [])),
                json.dumps(issue.get('data', {}))
            ))
            total_stored += 1
        except Exception as e:
            print(f"Warning: Failed to store critical issue for run {run_id}: {e}")
    
    # Process high priority issues
    for issue in analysis_results.get('high_priority_issues', []):
        try:
            cursor.execute(insert_sql, (
                run_id,
                issue.get('rule_config_name', 'unknown'),
                issue.get('metric', 'unknown'),
                'high',
                issue.get('analysis', {}).get('score', 7),
                issue.get('analysis', {}).get('reasoning', ''),
                json.dumps(issue.get('analysis', {}).get('recommendations', [])),
                json.dumps(issue.get('data', {}))
            ))
            total_stored += 1
        except Exception as e:
            print(f"Warning: Failed to store high priority issue for run {run_id}: {e}")
    
    # Process medium priority issues
    for issue in analysis_results.get('medium_priority_issues', []):
        try:
            cursor.execute(insert_sql, (
                run_id,
                issue.get('rule_config_name', 'unknown'),
                issue.get('metric', 'unknown'),
                'medium',
                issue.get('analysis', {}).get('score', 5),
                issue.get('analysis', {}).get('reasoning', ''),
                json.dumps(issue.get('analysis', {}).get('recommendations', [])),
                json.dumps(issue.get('data', {}))
            ))
            total_stored += 1
        except Exception as e:
            print(f"Warning: Failed to store medium priority issue for run {run_id}: {e}")
    
    return total_stored


def ship_to_database(db_config, target_info, findings_json, structured_findings, adoc_content, analysis_results=None):
    """Connects to PostgreSQL and inserts the health check data.

    This function handles the entire database transaction, including creating
    a company record if it doesn't exist and inserting the full findings,
    report, and execution context into the `health_check_runs` table.
    
    NEW: Also stores triggered rules from analysis_results for trend analysis.

    Args:
        db_config (dict): Database connection parameters for psycopg2.
        target_info (dict): Information about the target system being analyzed.
        findings_json (str): The complete structured findings, pre-serialized
            as a JSON string for database insertion.
        structured_findings (dict): The structured findings as a Python dict,
            used to extract execution context metadata.
        adoc_content (str): The full AsciiDoc report content to be stored.
        analysis_results (dict, optional): Results from generate_dynamic_prompt()
            containing triggered rules. If provided, rules will be stored in
            health_check_triggered_rules table.

    Returns:
        None
    """

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Log: Successfully connected to PostgreSQL for trend shipping.")

        company_name = target_info.get('company_name', 'Default Company')
        cursor.execute("SELECT get_or_create_company(%s);", (company_name,))
        company_id = cursor.fetchone()[0]

        db_type = target_info.get('db_type', 'unknown')
        host = target_info.get('host', 'unknown')
        port = target_info.get('port', 0)
        database = target_info.get('database', 'unknown')
        
        # Extract execution context from the structured findings
        context = structured_findings.get('execution_context', {})
        run_by_user = context.get('run_by_user', 'unknown')
        run_from_host = context.get('run_from_host', 'unknown')
        tool_version = context.get('tool_version', 'unknown')
        prompt_template_name = structured_findings.get('prompt_template_name')

        ai_context = context.get('ai_execution_metrics')
        ai_context_json = json.dumps(ai_context) if ai_context else None

        # Extract metadata for new columns
        db_version = _extract_db_version(structured_findings)
        db_version_major, db_version_minor = _parse_version_components(db_version)
        cluster_name = _extract_cluster_name(target_info, structured_findings)
        node_count = _extract_node_count(structured_findings)
        infrastructure_metadata = _build_infrastructure_metadata(target_info, structured_findings)
        health_score = _calculate_health_score(analysis_results)

        # Convert infrastructure_metadata to JSON if present
        infra_json = json.dumps(infrastructure_metadata) if infrastructure_metadata else None

        # Log extracted metadata for debugging
        print(f"Log: Extracted metadata - Version: {db_version}, Major: {db_version_major}, Minor: {db_version_minor}")
        print(f"Log: Cluster: {cluster_name}, Nodes: {node_count}, Health Score: {health_score}")

        # Insert health check run with enhanced metadata
        insert_query = """
        INSERT INTO health_check_runs (
            company_id, db_technology, target_host, target_port, target_db_name,
            findings, prompt_template_name, run_by_user, run_from_host, tool_version,
            report_adoc, ai_execution_context,
            db_version, db_version_major, db_version_minor, cluster_name, node_count,
            infrastructure_metadata, health_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        cursor.execute(insert_query, (
            company_id, db_type, host, port, database, findings_json,
            prompt_template_name, run_by_user, run_from_host, tool_version,
            adoc_content, ai_context_json,
            db_version, db_version_major, db_version_minor, cluster_name, node_count,
            infra_json, health_score
        ))
        
        # Get the ID of the newly inserted run
        run_id = cursor.fetchone()[0]
        print(f"Log: Successfully inserted health check run with ID: {run_id}")

        # NEW: Store triggered rules if analysis results are provided
        if analysis_results:
            print("Log: Storing triggered rules for trend analysis...")
            rules_stored = _store_triggered_rules(cursor, run_id, analysis_results)
            print(f"Log: Stored {rules_stored} triggered rules for run {run_id}")
            
            # Log summary of issues
            critical_count = len(analysis_results.get('critical_issues', []))
            high_count = len(analysis_results.get('high_priority_issues', []))
            medium_count = len(analysis_results.get('medium_priority_issues', []))
            print(f"Log: Issue summary - Critical: {critical_count}, High: {high_count}, Medium: {medium_count}")

        conn.commit()
        print("Log: Successfully shipped all findings and the AsciiDoc report to the database.")

    except psycopg2.Error as e:
        print(f"Error: Failed to ship data to PostgreSQL. {e}")
        if conn: 
            conn.rollback()
    except Exception as e:
        print(f"Error: Unexpected error during database shipping. {e}")
        if conn:
            conn.rollback()
    finally:
        if conn: 
            conn.close()


def ship_to_api(api_config, target_info, findings, adoc_content, analysis_results=None):
    """Sends health check data and the AsciiDoc report to an API endpoint.

    Args:
        api_config (dict): Configuration for the API, including:
            - endpoint_url (str): The API endpoint URL
            - api_key (str, optional): API key for authentication
            - timeout (int, optional): Request timeout in seconds (default: 30)
        target_info (dict): Information about the target system.
        findings (dict): The complete structured findings dictionary.
        adoc_content (str): The full AsciiDoc report content.
        analysis_results (dict, optional): Results from generate_dynamic_prompt()
            containing triggered rules and issue lists. Defaults to None.

    Returns:
        None
    """

    try:
        headers = {'Content-Type': 'application/json'}

        # Add API key to headers if provided
        api_key = api_config.get('api_key')
        if api_key:
            headers['X-API-Key'] = api_key

        full_payload = {
            'target_info': target_info,
            'findings': findings,
            'report_adoc': adoc_content
        }

        # Include analysis_results if provided (contains triggered rules for trend analysis)
        if analysis_results:
            full_payload['analysis_results'] = analysis_results

        timeout = api_config.get('timeout', 30)

        response = requests.post(
            api_config['endpoint_url'],
            headers=headers,
            data=safe_json_dumps(full_payload),
            timeout=timeout
        )
        response.raise_for_status()

        result = response.json()

        # Handle different response codes
        if response.status_code == 201:
            print(f"✅ Findings submitted successfully!")
            print(f"   Submission ID: {result.get('submission_id', 'N/A')}")
            if 'submissions_remaining' in result and result['submissions_remaining'] is not None:
                print(f"   Submissions remaining: {result['submissions_remaining']}")
        elif response.status_code == 202:
            print(f"✅ Findings accepted for processing!")
            print(f"   Submission ID: {result.get('submission_id', 'N/A')}")
            print(f"   Status: Queued for processing")

        print(f"Log: Successfully sent raw findings to API. Status: {response.status_code}")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f"❌ Authentication failed: Invalid or missing API key")
        elif e.response.status_code == 402:
            print(f"⚠️  Trial key limit reached. Upgrade to continue.")
            print(f"   Visit: https://health.instaclustr.com/pricing")
        elif e.response.status_code == 503:
            print(f"⚠️  Service temporarily unavailable. Please try again later.")
        else:
            print(f"❌ API error: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to ship data to API. {e}")


def run(structured_findings, target_info, adoc_content=None, analysis_results=None):
    """Main entry point for the trend shipper module.

    This function is called by the main application after a health check is
    complete. It loads the `trends.yaml` configuration and, based on the
    specified `destination`, calls the appropriate function to ship the results.
    
    NEW: Accepts analysis_results parameter to store triggered rules.

    Args:
        structured_findings (dict): The final dictionary of all structured
            findings from the health check.
        target_info (dict): A dictionary containing metadata about the target
            system (e.g., host, db_type, company_name).
        adoc_content (str, optional): The full AsciiDoc report. Defaults to None.
        analysis_results (dict, optional): Results from generate_dynamic_prompt()
            containing triggered rules and issue lists. Defaults to None.

    Returns:
        None
    """

    print("--- Trend Shipper Module Started ---")
    config = load_config()
    if not config:
        print("--- Trend Shipper Module Finished (No Config) ---")
        return

    destination = config.get('destination')

    if destination == "postgresql":
        findings_as_json = safe_json_dumps(structured_findings)
        ship_to_database(
            config.get('database'),
            target_info,
            findings_as_json,
            structured_findings,
            adoc_content,
            analysis_results  # Pass analysis results for rule tracking
        )
    elif destination == "api":
        ship_to_api(
            config.get('api'),
            target_info,
            structured_findings,
            adoc_content,
            analysis_results  # Pass analysis results for rule tracking
        )
    else:
        print(f"Error: Unknown trend storage destination '{destination}'.")
        
    print("--- Trend Shipper Module Finished ---")

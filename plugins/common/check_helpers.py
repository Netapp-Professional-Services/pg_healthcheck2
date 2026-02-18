"""
Common helper functions for health checks.

Provides utilities that are useful across all database health checks,
regardless of the underlying database technology.
"""

import logging
from typing import Tuple, Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)



def require_ssh(connector, operation_name, verbose=False):
    """
    Check if SSH is available for the connector, or if nodetool data is pre-loaded.

    Works with:
    - Old pattern: single ssh_manager
    - New pattern: multi-host SSHSupportMixin
    - Diagnostic imports: pre-loaded nodetool data (no SSH needed)

    Args:
        connector: The database connector instance
        operation_name: Human-readable name of the operation requiring SSH
        verbose: If True, include full configuration instructions in skip message.
                If False (default), return a concise skip message.

    Returns:
        tuple: (available: bool, skip_message: str, skip_data: dict)
            - If SSH or nodetool data is available: (True, None, None)
            - If neither is available: (False, adoc_skip_message, structured_data)

    Example:
        available, skip_msg, skip_data = require_ssh(connector, "disk usage check")
        if not available:
            return skip_msg, skip_data

        # Proceed with SSH operations...
    """
    # CHECK FOR DIAGNOSTIC IMPORTS: If connector has pre-loaded nodetool data,
    # we can proceed even without SSH (data comes from imported files)
    if hasattr(connector, 'has_nodetool_data') and callable(connector.has_nodetool_data):
        if connector.has_nodetool_data():
            return True, None, None

    # NEW PATTERN: Check if connector uses SSHSupportMixin
    if hasattr(connector, 'has_ssh_support') and callable(connector.has_ssh_support):
        if not connector.has_ssh_support():
            # Use mixin's skip message method if available
            if hasattr(connector, 'get_ssh_skip_message'):
                return (False,) + connector.get_ssh_skip_message(operation_name, verbose=verbose)
            else:
                # Fallback message
                if verbose:
                    skip_msg = (
                        f"[IMPORTANT]\n"
                        f"====\n"
                        f"{operation_name} requires SSH access, which is not configured.\n"
                        f"====\n"
                    )
                else:
                    skip_msg = f"_Skipped: SSH not configured for {operation_name}._\n"
                skip_data = {"status": "skipped", "reason": "SSH not configured"}
                return False, skip_msg, skip_data

        # SSH is available
        return True, None, None

    # OLD PATTERN: Check for single ssh_manager (backward compatibility)
    if not hasattr(connector, 'ssh_manager') or connector.ssh_manager is None:
        if verbose:
            skip_msg = (
                f"[IMPORTANT]\n"
                f"====\n"
                f"{operation_name} requires SSH access, which is not configured.\n\n"
                f"Configure the following in your settings:\n\n"
                f"* `ssh_host`: Hostname or IP address\n"
                f"* `ssh_user`: SSH username\n"
                f"* `ssh_key_file` OR `ssh_password`: Authentication method\n"
                f"====\n"
            )
        else:
            skip_msg = f"_Skipped: SSH not configured for {operation_name}._\n"
        skip_data = {"status": "skipped", "reason": "SSH not configured"}
        return False, skip_msg, skip_data

    # Old pattern SSH is available
    return True, None, None


def get_metric_collection_error_message(connector, metric_name: str = "metrics") -> str:
    """
    Generate an accurate error message when metric collection fails.

    This function checks what's actually available (SSH, Instaclustr Prometheus, etc.)
    and generates a precise error message instead of the generic "SSH unavailable" message.

    Args:
        connector: Database connector instance
        metric_name: Name of the metric that failed to collect (for context)

    Returns:
        Formatted error message string explaining what was tried and why it failed
    """
    # Check SSH availability
    ssh_available, _, _ = require_ssh(connector, "metric collection")

    # Check Instaclustr Prometheus
    instaclustr_enabled = getattr(connector, 'settings', {}).get('instaclustr_prometheus_enabled', False)

    # Check metric collection strategy
    strategy = getattr(connector, 'metric_collection_strategy', None)
    strategy_details = getattr(connector, 'metric_collection_details', {})

    # Build accurate error message
    lines = [
        f"⚠️ Could not collect {metric_name}\n",
        "*Tried collection methods:*"
    ]

    # Method 1: Instaclustr Prometheus
    if instaclustr_enabled:
        lines.append("1. Instaclustr Prometheus API - Configured but metric not available or query failed")
    else:
        lines.append("1. Instaclustr Prometheus API - Not configured (set `instaclustr_prometheus_enabled: true`)")

    # Method 2: Local Prometheus
    if ssh_available:
        if strategy == 'local_prometheus':
            lines.append("2. Local Prometheus JMX exporter - Found but metric not exposed or query failed")
        else:
            lines.append("2. Local Prometheus JMX exporter - Not found (JMX exporter not running on port 7500)")
    else:
        lines.append("2. Local Prometheus JMX exporter - SSH unavailable (required for local Prometheus)")

    # Method 3: Standard JMX
    if ssh_available:
        if strategy == 'jmx':
            lines.append("3. Standard JMX - Found but metric query failed")
        elif strategy_details.get('reason') == 'JMX not enabled on port 9999':
            lines.append("3. Standard JMX - Not enabled (JMX remote port 9999 not configured)")
        else:
            lines.append("3. Standard JMX - Not available (JMX remote port 9999 not configured)")
    else:
        lines.append("3. Standard JMX - SSH unavailable (required for JMX access)")

    lines.append("")
    lines.append("*To enable monitoring, configure one of:*")

    if not instaclustr_enabled:
        lines.append("• Instaclustr Prometheus: Set `instaclustr_prometheus_enabled: true`")

    if ssh_available:
        if strategy != 'local_prometheus':
            lines.append("• Local Prometheus exporter: Ensure JMX exporter running on brokers (port 7500)")
        if strategy != 'jmx' and 'JMX not enabled' in str(strategy_details.get('reason', '')):
            lines.append("• Standard JMX: Enable JMX on port 9999 (add JVM options to Kafka startup)")
    else:
        lines.append("• Configure SSH access: Set `ssh_hosts`, `ssh_user`, and `ssh_key_file`")
        lines.append("• Then enable either Local Prometheus exporter or Standard JMX")

    return "\n".join(lines)


def require_aws(connector: Any, operation_name: Optional[str] = None) -> Tuple[bool, str, Dict]:
    """
    Check if AWS support is available and return appropriate response if not.
    
    Args:
        connector: Database connector with AWSSupportMixin
        operation_name: Optional description of what AWS is needed for
    
    Returns:
        tuple: (aws_available: bool, skip_message: str, skip_data: dict)
        
    Example:
        aws_ok, skip_msg, skip_data = require_aws(connector, "CloudWatch metrics")
        if not aws_ok:
            return skip_msg, skip_data
        # Continue with check...
    """
    if hasattr(connector, 'has_aws_support') and connector.has_aws_support():
        return True, "", {}
    
    # AWS not available
    if hasattr(connector, 'get_aws_skip_message'):
        skip_msg, skip_data = connector.get_aws_skip_message(operation_name)
    else:
        # Fallback if connector doesn't have the mixin
        op_text = f" for {operation_name}" if operation_name else ""
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires AWS access{op_text}.\n\n"
            "Configure the following in your settings:\n\n"
            "* `aws_region`: AWS region (e.g., 'us-east-1')\n"
            "* `aws_access_key_id`: AWS access key\n"
            "* `aws_secret_access_key`: AWS secret key\n"
            "====\n"
        )
        skip_data = {
            "status": "skipped",
            "reason": "AWS not configured",
            "required_settings": ["aws_region", "aws_access_key_id", "aws_secret_access_key"]
        }
    
    return False, skip_msg, skip_data


def require_azure(connector: Any, operation_name: Optional[str] = None) -> Tuple[bool, str, Dict]:
    """
    Check if Azure support is available and return appropriate response if not.
    
    Args:
        connector: Database connector with AzureSupportMixin
        operation_name: Optional description of what Azure is needed for
    
    Returns:
        tuple: (azure_available: bool, skip_message: str, skip_data: dict)
        
    Example:
        azure_ok, skip_msg, skip_data = require_azure(connector, "Azure Monitor metrics")
        if not azure_ok:
            return skip_msg, skip_data
        # Continue with check...
    """
    if hasattr(connector, 'has_azure_support') and connector.has_azure_support():
        return True, "", {}
    
    # Azure not available
    if hasattr(connector, 'get_azure_skip_message'):
        skip_msg, skip_data = connector.get_azure_skip_message(operation_name)
    else:
        # Fallback if connector doesn't have the mixin
        op_text = f" for {operation_name}" if operation_name else ""
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires Azure access{op_text}.\n\n"
            "Configure credentials via:\n"
            "* Azure CLI (`az login`)\n"
            "* Environment variables: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`\n"
            "* `config/azure_credentials.yaml` with `subscription_id`, `client_id`, `client_secret`, `tenant_id`\n"
            "====\n"
        )
        skip_data = {
            "status": "skipped",
            "reason": "Azure not configured",
            "required_settings": ["subscription_id", "resource_group", "server_name"]
        }
    
    return False, skip_msg, skip_data


def require_instaclustr(connector: Any, operation_name: Optional[str] = None) -> Tuple[bool, str, Dict]:
    """
    Check if Instaclustr support is available and return appropriate response if not.

    Args:
        connector: Database connector with InstaclustrSupportMixin
        operation_name: Optional description of what Instaclustr is needed for

    Returns:
        tuple: (instaclustr_available: bool, skip_message: str, skip_data: dict)

    Example:
        ic_ok, skip_msg, skip_data = require_instaclustr(connector, "cluster metrics")
        if not ic_ok:
            return skip_msg, skip_data
        # Continue with check...
    """
    if hasattr(connector, 'has_instaclustr_support') and connector.has_instaclustr_support():
        return True, "", {}

    # Instaclustr not available
    if hasattr(connector, 'get_instaclustr_skip_message'):
        skip_msg, skip_data = connector.get_instaclustr_skip_message(operation_name)
    else:
        # Fallback if connector doesn't have the mixin
        op_text = f" for {operation_name}" if operation_name else ""
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires Instaclustr access{op_text}.\n\n"
            "Configure in settings or config/instaclustr_credentials.yaml:\n"
            "* `instaclustr_api_key`: API key\n"
            "* `instaclustr_cluster_id`: Cluster ID\n"
            "====\n"
        )
        skip_data = {
            "status": "skipped",
            "reason": "Instaclustr not configured",
            "required_settings": ["instaclustr_api_key", "instaclustr_cluster_id"]
        }

    return False, skip_msg, skip_data


def require_prometheus(settings: Dict[str, Any], operation_name: Optional[str] = None,
                       verbose: bool = False) -> Tuple[bool, str, Dict]:
    """
    Check if Prometheus/Instaclustr Prometheus is enabled and return appropriate response if not.

    Args:
        settings: Configuration dictionary with Prometheus settings
        operation_name: Optional description of what Prometheus is needed for
        verbose: If True, include full configuration instructions in skip message.
                If False (default), return a concise skip message.

    Returns:
        tuple: (prometheus_available: bool, skip_message: str, skip_data: dict)

    Example:
        prom_ok, skip_msg, skip_data = require_prometheus(settings, "JVM heap metrics")
        if not prom_ok:
            builder.add(skip_msg)
            return builder.build(), skip_data
        # Continue with check...
    """
    # Check if Instaclustr Prometheus is enabled
    if settings.get('instaclustr_prometheus_enabled'):
        return True, "", {}

    # Prometheus not enabled
    op_text = f" for {operation_name}" if operation_name else ""

    if verbose:
        skip_msg = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires Prometheus access{op_text}.\n\n"
            "Configure the following in your settings:\n\n"
            "**Instaclustr Prometheus:**\n"
            "* `instaclustr_prometheus_enabled`: true\n"
            "* `instaclustr_cluster_id`: Your cluster ID\n"
            "* `instaclustr_prometheus_username`: Prometheus username\n"
            "* `instaclustr_prometheus_api_key`: Prometheus API key\n"
            "* `instaclustr_prometheus_base_url`: Prometheus endpoint URL\n"
            "====\n"
        )
    else:
        # Concise message (default)
        skip_msg = f"_Skipped: Prometheus not enabled{op_text}._\n"

    skip_data = {
        "status": "skipped",
        "reason": "Prometheus not enabled",
        "required_settings": [
            "instaclustr_prometheus_enabled",
            "instaclustr_cluster_id",
            "instaclustr_prometheus_username",
            "instaclustr_prometheus_api_key",
            "instaclustr_prometheus_base_url"
        ]
    }

    return False, skip_msg, skip_data


def format_check_header(check_name: str, description: str, 
                        requires_ssh: bool = False,
                        requires_aws: bool = False,
                        requires_azure: bool = False,
                        requires_instaclustr: bool = False) -> List[str]:
    """
    Generate a standard check header with title and description.
    
    Args:
        check_name: Name of the check
        description: Description of what the check does
        requires_ssh: If True, adds SSH requirement note
        requires_aws: If True, adds AWS requirement note
        requires_azure: If True, adds Azure requirement note
        requires_instaclustr: If True, adds Instaclustr requirement note
    
    Returns:
        list: Lines of AsciiDoc content
    """
    header = [
        f"=== {check_name}",
        "",
        description
    ]
    
    # Add requirement notes if needed
    requirements = []
    if requires_ssh:
        requirements.append("SSH access to the database server")
    if requires_aws:
        requirements.append("AWS credentials with CloudWatch read permissions")
    if requires_azure:
        requirements.append("Azure credentials with Monitor read permissions")
    if requires_instaclustr:
        requirements.append("Instaclustr API access")
    
    if requirements:
        header.extend([
            "",
            "[NOTE]",
            "====",
            "**Requirements:**",
            ""
        ])
        for req in requirements:
            header.append(f"* {req}")
        header.append("====")
    
    return header


def format_recommendations(recommendations: List[str]) -> List[str]:
    """
    Format a list of recommendations in standard AsciiDoc format.

    Args:
        recommendations: List of recommendation strings

    Returns:
        list: Formatted AsciiDoc lines
    """
    if not recommendations:
        return []

    output = [
        "",
        "==== Recommendations",
        "[TIP]",
        "===="
    ]

    for rec in recommendations:
        # Ensure each recommendation is a bullet point
        if not rec.startswith('*'):
            rec = f"* {rec}"
        output.append(rec)

    output.append("====")
    output.append("")

    return output


def format_data_as_table(data: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> str:
    """
    Format a list of dictionaries as an AsciiDoc table.

    This is useful when you need to display filtered data instead of raw query results.

    Args:
        data: List of dictionaries to format
        columns: Optional list of column names to include (in order).
                 If None, uses all keys from first row.

    Returns:
        str: Formatted AsciiDoc table string

    Example:
        >>> data = [{'keyspace_name': 'my_app', 'replication': '3'}]
        >>> print(format_data_as_table(data))
        |===
        | keyspace_name | replication
        | my_app | 3
        |===
    """
    if not data:
        return ""

    # Determine columns
    if columns is None:
        columns = list(data[0].keys())

    lines = []
    lines.append("|===")

    # Header row
    header = "| " + " | ".join(str(col) for col in columns)
    lines.append(header)
    lines.append("")  # Blank line after header for AsciiDoc

    # Data rows
    for row in data:
        values = []
        for col in columns:
            val = row.get(col, "")
            # Handle complex types (dicts, lists)
            if isinstance(val, (dict, list)):
                val = str(val)
            values.append(str(val) if val is not None else "")
        lines.append("| " + " | ".join(values))

    lines.append("|===")

    return "\n".join(lines)


def safe_execute_query(
    connector: Any,
    query: str,
    error_context: str = "Query",
    params: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, Union[List[Dict], Dict, Any]]:
    """
    Safely execute a query with standardized error handling.
    
    Args:
        connector: Database connector with execute_query method
        query: Query to execute (SQL, CQL, JSON command, etc.)
        error_context: Context for error messages (e.g., "Nodetool command", "CQL query")
        params: Optional query parameters for parameterized queries
    
    Returns:
        tuple: (success: bool, formatted: str, raw_data: Any)
            - success: True if query executed without errors
            - formatted: AsciiDoc formatted output string
            - raw_data: Structured data (list of dicts, dict, or error dict)
    
    Example:
        success, formatted, raw = safe_execute_query(
            connector, 
            "SELECT * FROM system.peers",
            "System peers query"
        )
        if not success:
            adoc_content.append(formatted)
            structured_data["result"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data
    """
    try:
        # Execute query with or without parameters
        if params:
            formatted, raw = connector.execute_query(query, params, return_raw=True)
        else:
            formatted, raw = connector.execute_query(query, return_raw=True)
        
        # Check for errors in formatted output
        if "[ERROR]" in formatted or "[CAUTION]" in formatted:
            logger.warning(f"{error_context} returned error: {formatted[:100]}...")
            return False, formatted, raw
        
        return True, formatted, raw
        
    except AttributeError as e:
        # Connector doesn't have execute_query method
        logger.error(f"Connector missing execute_query method: {e}", exc_info=True)
        error_msg = (
            "[CAUTION]\n"
            "====\n"
            f"Internal error: Connector does not support query execution.\n"
            f"Details: {str(e)}\n"
            "====\n"
        )
        error_data = {
            "error": "AttributeError",
            "message": str(e),
            "error_context": error_context,
            "query": query[:200] if query else None
        }
        return False, error_msg, error_data
    
    except Exception as e:
        logger.error(f"{error_context} failed: {e}", exc_info=True)
        error_msg = (
            "[CAUTION]\n"
            "====\n"
            f"{error_context} failed: {str(e)}\n"
            "====\n"
        )
        error_data = {
            "error": type(e).__name__,
            "message": str(e),
            "error_context": error_context,
            "query": query[:200] if query else None
        }
        return False, error_msg, error_data


def merge_structured_data(base_data: Dict[str, Any], 
                          new_data: Dict[str, Any],
                          section_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Safely merge new data into base structured data dictionary.
    
    Useful for combining results from multiple queries in a single check.
    
    Args:
        base_data: Base dictionary to merge into
        new_data: New data to add
        section_name: Optional section name to nest new_data under
    
    Returns:
        Updated base_data dictionary (also modifies in place)
    
    Example:
        structured_data = {}
        
        # First query
        success, formatted, raw = safe_execute_query(...)
        structured_data = merge_structured_data(
            structured_data,
            {"node_status": raw},
            "cluster_health"
        )
        
        # Second query
        success, formatted, raw = safe_execute_query(...)
        structured_data = merge_structured_data(
            structured_data,
            {"compaction_stats": raw},
            "cluster_health"
        )
        
        # Result: structured_data = {
        #     "cluster_health": {
        #         "node_status": [...],
        #         "compaction_stats": {...}
        #     }
        # }
    """
    if section_name:
        # Create section if it doesn't exist
        if section_name not in base_data:
            base_data[section_name] = {}
        
        # Merge into section
        base_data[section_name].update(new_data)
    else:
        # Merge directly into base
        base_data.update(new_data)
    
    return base_data


def calculate_percentage(value: Union[int, float], 
                        total: Union[int, float],
                        decimal_places: int = 1) -> Optional[float]:
    """
    Safely calculate percentage, handling zero division.
    
    Args:
        value: Numerator value
        total: Denominator value
        decimal_places: Number of decimal places to round to
    
    Returns:
        Percentage as float, or None if total is 0
    
    Example:
        pct = calculate_percentage(75, 100)  # Returns 75.0
        pct = calculate_percentage(1, 3, 2)  # Returns 33.33
        pct = calculate_percentage(5, 0)     # Returns None
    """
    if total == 0 or total is None:
        return None
    
    try:
        percentage = (float(value) / float(total)) * 100
        return round(percentage, decimal_places)
    except (ValueError, TypeError, ZeroDivisionError):
        logger.warning(f"Could not calculate percentage: {value}/{total}")
        return None


def format_bytes(bytes_value: int, decimal_places: int = 2) -> str:
    """
    Format bytes into human-readable string (KB, MB, GB, TB).
    
    Args:
        bytes_value: Number of bytes
        decimal_places: Number of decimal places to round to
    
    Returns:
        Formatted string (e.g., "1.50 GB")
    
    Example:
        format_bytes(1024)           # "1.00 KB"
        format_bytes(1536)           # "1.50 KB"
        format_bytes(1073741824)     # "1.00 GB"
        format_bytes(0)              # "0 B"
    """
    if bytes_value == 0:
        return "0 B"
    
    if bytes_value < 0:
        return f"{bytes_value} B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    value = float(bytes_value)
    
    while value >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    
    return f"{value:.{decimal_places}f} {units[unit_index]}"


# ============================================================================
# CheckContentBuilder - Optional builder for cleaner check code
# ============================================================================
# BACKWARD COMPATIBILITY GUARANTEE:
# - This is purely additive - all existing checks continue to work unchanged
# - All existing helper functions remain unchanged and fully supported
# - Opt-in only - no migration required
# ============================================================================


class CheckContentBuilder:
    """
    Optional builder for constructing check AsciiDoc content with minimal typing.
    
    Reduces boilerplate in check modules by providing fluent methods for common
    patterns. Integrates seamlessly with existing helpers.
    
    BACKWARD COMPATIBILITY:
    - Purely additive - existing checks work unchanged
    - Opt-in only - use for new checks or when refactoring
    - Works with all existing helpers (format_check_header, safe_execute_query, etc.)
    
    Quick Example:
        builder = CheckContentBuilder(connector.formatter)
        builder.h3("Disk Usage Check")
        builder.para("Checking disk usage across all brokers...")
        
        if critical:
            builder.critical("Broker 1 at 95% capacity!")
        
        builder.h4("Summary")
        builder.table(disk_data)
        builder.recs(["Increase disk space", "Enable log cleanup"])
        
        return builder.build(), structured_data
    
    Advanced Example:
        # Mix with existing helpers
        builder.add_header("Schema Check", "Description", requires_ssh=True)
        
        success, formatted, raw = safe_execute_query(...)
        builder.add(formatted)  # Add pre-formatted content
        
        if not success:
            return builder.build(), structured_data
        
        builder.critical_block(
            "Schema Inconsistency",
            ["Version mismatch detected", "3 nodes out of sync"]
        )
    """
    
    def __init__(self, formatter=None):
        """
        Initialize builder.
        
        Args:
            formatter: Optional AsciiDocFormatter instance (for table formatting)
        """
        self._lines = []
        self._formatter = formatter
    
    # ========================================================================
    # Core Methods - Accept pre-formatted content
    # ========================================================================
    
    def add(self, content):
        """
        Add pre-formatted content (from connectors, existing helpers, etc.).
        
        Args:
            content: String or list of strings to add
        
        Returns:
            self (for chaining)
        
        Example:
            success, formatted, raw = safe_execute_query(...)
            builder.add(formatted)
        """
        if content:
            if isinstance(content, list):
                self._lines.extend(content)
            else:
                self._lines.append(str(content))
        return self
    
    def add_lines(self, lines):
        """
        Add multiple lines from existing helpers.
        
        Args:
            lines: List of strings
        
        Returns:
            self (for chaining)
        
        Example:
            header = format_check_header("Title", "Desc")
            builder.add_lines(header)
        """
        if lines:
            if isinstance(lines, list):
                self._lines.extend(lines)
            else:
                self._lines.append(str(lines))
        return self
    
    def blank(self):
        """Add blank line for spacing."""
        self._lines.append("")
        return self
    
    # ========================================================================
    # Headers - Minimal typing
    # ========================================================================
    
    def h3(self, text):
        """
        Add level 3 header (===).
        
        Args:
            text: Header text
        
        Returns:
            self (for chaining)
        """
        self._lines.append(f"=== {text}")
        self._lines.append("")
        return self
    
    def h4(self, text):
        """
        Add level 4 header (====).
        
        Args:
            text: Header text
        
        Returns:
            self (for chaining)
        """
        self._lines.append(f"==== {text}")
        self._lines.append("")
        return self
    
    def add_header(self, check_name, description, 
                   requires_ssh=False, requires_aws=False,
                   requires_azure=False, requires_instaclustr=False):
        """
        Add standard check header using existing format_check_header().
        
        Args:
            check_name: Name of the check
            description: Description text
            requires_ssh: SSH requirement flag
            requires_aws: AWS requirement flag
            requires_azure: Azure requirement flag
            requires_instaclustr: Instaclustr requirement flag
        
        Returns:
            self (for chaining)
        
        Example:
            builder.add_header("Disk Check", "Monitors disk usage", requires_ssh=True)
        """
        header_lines = format_check_header(
            check_name, description,
            requires_ssh=requires_ssh,
            requires_aws=requires_aws,
            requires_azure=requires_azure,
            requires_instaclustr=requires_instaclustr
        )
        self._lines.extend(header_lines)
        return self
    
    # ========================================================================
    # Text Content
    # ========================================================================
    
    def para(self, text):
        """
        Add paragraph text with trailing blank line.
        
        Args:
            text: Paragraph text
        
        Returns:
            self (for chaining)
        """
        if text:
            self._lines.append(str(text))
            self._lines.append("")
        return self
    
    def text(self, text):
        """
        Add text without trailing blank line.
        
        Args:
            text: Text to add
        
        Returns:
            self (for chaining)
        """
        if text:
            self._lines.append(str(text))
        return self
    
    # ========================================================================
    # Admonition Blocks - Super short names
    # ========================================================================
    
    def note(self, message):
        """
        Add NOTE admonition block.
        
        Args:
            message: Note message
        
        Returns:
            self (for chaining)
        """
        self._lines.append("[NOTE]")
        self._lines.append("====")
        self._lines.append(str(message))
        self._lines.append("====")
        self._lines.append("")
        return self
    
    def tip(self, message):
        """
        Add TIP admonition block.
        
        Args:
            message: Tip message
        
        Returns:
            self (for chaining)
        """
        self._lines.append("[TIP]")
        self._lines.append("====")
        self._lines.append(str(message))
        self._lines.append("====")
        self._lines.append("")
        return self
    
    def warning(self, message):
        """
        Add WARNING admonition block.
        
        Args:
            message: Warning message
        
        Returns:
            self (for chaining)
        """
        self._lines.append("[WARNING]")
        self._lines.append("====")
        self._lines.append(str(message))
        self._lines.append("====")
        self._lines.append("")
        return self
    
    def critical(self, message):
        """
        Add IMPORTANT/CRITICAL admonition block.
        
        Args:
            message: Critical message
        
        Returns:
            self (for chaining)
        """
        self._lines.append("[IMPORTANT]")
        self._lines.append("====")
        self._lines.append(str(message))
        self._lines.append("====")
        self._lines.append("")
        return self
    
    def error(self, message):
        """
        Add CAUTION/ERROR admonition block.
        
        Args:
            message: Error message
        
        Returns:
            self (for chaining)
        """
        self._lines.append("[CAUTION]")
        self._lines.append("====")
        self._lines.append(str(message))
        self._lines.append("====")
        self._lines.append("")
        return self
    
    # ========================================================================
    # Structured Admonition Blocks - For detailed issues
    # ========================================================================
    
    def issue(self, title, details, level="IMPORTANT"):
        """
        Add structured issue block with title and key-value details.
        
        Args:
            title: Issue title (bold)
            details: Dict of key-value pairs or list of strings
            level: Admonition level (IMPORTANT, WARNING, CAUTION, NOTE)
        
        Returns:
            self (for chaining)
        
        Example:
            builder.issue(
                "Critical Heap Usage",
                {
                    "Broker": "1 (192.168.1.113)",
                    "Heap": "95.3% (threshold: 90%)",
                    "Status": "CRITICAL"
                }
            )
        """
        self._lines.append(f"[{level}]")
        self._lines.append("====")
        self._lines.append(f"**{title}**")
        self._lines.append("")
        
        if isinstance(details, dict):
            for key, value in details.items():
                self._lines.append(f"* **{key}:** {value}")
        elif isinstance(details, list):
            for item in details:
                if not item.startswith('*'):
                    item = f"* {item}"
                self._lines.append(item)
        else:
            self._lines.append(str(details))
        
        self._lines.append("====")
        self._lines.append("")
        return self
    
    def critical_issue(self, title, details):
        """Shortcut for critical issue (IMPORTANT level)."""
        return self.issue(title, details, "IMPORTANT")
    
    def warning_issue(self, title, details):
        """Shortcut for warning issue (WARNING level)."""
        return self.issue(title, details, "WARNING")
    
    # ========================================================================
    # Tables - Use existing formatter
    # ========================================================================
    
    def table(self, data):
        """
        Add table from list of dicts using AsciiDocFormatter.
        
        Args:
            data: List of dictionaries (keys are column headers)
        
        Returns:
            self (for chaining)
        
        Example:
            builder.table([
                {"Broker": 1, "Usage": "95%", "Status": "CRITICAL"},
                {"Broker": 2, "Usage": "65%", "Status": "OK"}
            ])
        """
        if self._formatter:
            table_str = self._formatter.format_table(data)
            self._lines.append(table_str)
            self._lines.append("")
        else:
            # Fallback: basic table
            if data and len(data) > 0:
                headers = list(data[0].keys())
                self._lines.append("|===")
                self._lines.append("|" + "|".join(headers))
                for row in data:
                    values = [str(row.get(h, "")) for h in headers]
                    self._lines.append("|" + "|".join(values))
                self._lines.append("|===")
                self._lines.append("")
        return self
    
    def dict_table(self, data, key_header="Key", value_header="Value"):
        """
        Add two-column table from dictionary.
        
        Args:
            data: Dictionary to display
            key_header: Header for key column
            value_header: Header for value column
        
        Returns:
            self (for chaining)
        """
        if self._formatter:
            table_str = self._formatter.format_dict_as_table(data, key_header, value_header)
            self._lines.append(table_str)
            self._lines.append("")
        else:
            # Fallback
            self._lines.append("|===")
            self._lines.append(f"|{key_header}|{value_header}")
            for k, v in data.items():
                self._lines.append(f"|{k}|{v}")
            self._lines.append("|===")
            self._lines.append("")
        return self
    
    def table_with_indicators(self, headers, rows, indicator_col=None, 
                             warning_threshold=None, critical_threshold=None):
        """
        Add table with status indicators (🔴/⚠️) based on thresholds.
        
        Args:
            headers: List of column header strings
            rows: List of row data (each row is a list matching headers)
            indicator_col: Index of column to add indicators to (optional)
            warning_threshold: Value for warning indicator
            critical_threshold: Value for critical indicator
        
        Returns:
            self (for chaining)
        
        Example:
            builder.table_with_indicators(
                headers=["Broker", "Usage %"],
                rows=[[1, 95], [2, 65], [3, 78]],
                indicator_col=1,
                warning_threshold=70,
                critical_threshold=90
            )
        """
        self._lines.append("|===")
        self._lines.append("|" + "|".join(headers))
        
        for row in rows:
            # Add indicators if specified
            if indicator_col is not None and warning_threshold is not None:
                value = row[indicator_col]
                indicator = ""
                if critical_threshold and value >= critical_threshold:
                    indicator = "🔴 "
                elif value >= warning_threshold:
                    indicator = "⚠️ "
                
                # Insert indicator
                row_copy = list(row)
                row_copy[indicator_col] = f"{indicator}{value}"
                self._lines.append("|" + "|".join(str(x) for x in row_copy))
            else:
                self._lines.append("|" + "|".join(str(x) for x in row))
        
        self._lines.append("|===")
        self._lines.append("")
        return self
    
    # ========================================================================
    # Recommendations - Minimal typing
    # ========================================================================
    
    def recs(self, recommendations, title="Recommendations"):
        """
        Add recommendations section (ultra-short alias).
        
        Args:
            recommendations: List of recommendation strings or dict with priorities
            title: Section title (default: "Recommendations")
        
        Returns:
            self (for chaining)
        
        Simple Example:
            builder.recs([
                "Increase heap size",
                "Enable GC logging"
            ])
        
        Structured Example:
            builder.recs({
                "critical": ["Immediate action needed", "Fix now"],
                "high": ["Plan optimization"],
                "general": ["Best practices"]
            })
        """
        if not recommendations:
            return self
        
        self._lines.append(f"==== {title}")
        self._lines.append("")
        self._lines.append("[TIP]")
        self._lines.append("====")
        
        if isinstance(recommendations, dict):
            # Structured recommendations by priority
            if "critical" in recommendations and recommendations["critical"]:
                self._lines.append("**🔴 Critical Priority (Immediate Action):**")
                self._lines.append("")
                for rec in recommendations["critical"]:
                    if not rec.startswith('*'):
                        rec = f"* {rec}"
                    self._lines.append(rec)
                self._lines.append("")
            
            if "high" in recommendations and recommendations["high"]:
                self._lines.append("**⚠️ High Priority (Plan Optimization):**")
                self._lines.append("")
                for rec in recommendations["high"]:
                    if not rec.startswith('*'):
                        rec = f"* {rec}"
                    self._lines.append(rec)
                self._lines.append("")
            
            if "general" in recommendations and recommendations["general"]:
                self._lines.append("**📋 General Best Practices:**")
                self._lines.append("")
                for rec in recommendations["general"]:
                    if not rec.startswith('*'):
                        rec = f"* {rec}"
                    self._lines.append(rec)
        
        elif isinstance(recommendations, list):
            # Simple list of recommendations
            for rec in recommendations:
                if not rec.startswith('*'):
                    rec = f"* {rec}"
                self._lines.append(rec)
        
        self._lines.append("====")
        self._lines.append("")
        return self
    
    def recommendations(self, recommendations, title="Recommendations"):
        """Alias for recs() with full name for clarity."""
        return self.recs(recommendations, title)
    
    # ========================================================================
    # Literal/Code Blocks
    # ========================================================================
    
    def literal(self, text, language="text"):
        """
        Add literal/code block.
        
        Args:
            text: Literal text content
            language: Source language for syntax highlighting (default: text)
        
        Returns:
            self (for chaining)
        """
        self._lines.append(f"[source,{language}]")
        self._lines.append("----")
        self._lines.append(str(text))
        self._lines.append("----")
        self._lines.append("")
        return self
    
    def code(self, code, language="bash"):
        """
        Add code block (alias for literal with bash default).
        
        Args:
            code: Code content
            language: Programming language (default: bash)
        
        Returns:
            self (for chaining)
        """
        return self.literal(code, language)
    
    # ========================================================================
    # Quick Status Messages
    # ========================================================================
    
    def success(self, message="✅ All checks passed. No issues detected."):
        """Add success note with checkmark."""
        return self.note(message)
    
    def skip(self, reason):
        """Add skip message."""
        return self.note(f"Check skipped: {reason}")
    
    # ========================================================================
    # Build Final Output
    # ========================================================================
    
    def build(self):
        """
        Build final AsciiDoc string.
        
        Returns:
            String with proper line joining
        """
        return "\n".join(self._lines)
    
    # ========================================================================
    # Convenience: Full section patterns
    # ========================================================================
    
    def summary_section(self, title, data, status_message=None):
        """
        Add complete summary section with table.
        
        Args:
            title: Section title
            data: Table data (list of dicts)
            status_message: Optional status note to add after table
        
        Returns:
            self (for chaining)
        """
        self.h4(title)
        self.table(data)
        if status_message:
            self.note(status_message)
        return self

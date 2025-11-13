"""
Security middleware and validation functions for the trends_app.

This module provides optional security enhancements that can be enabled
via configuration. All features are opt-in to maintain flexibility for
development and different deployment scenarios.
"""

import json
from functools import wraps
from flask import request, jsonify, current_app, redirect
from datetime import datetime, timedelta


# ============================================================================
# CONFIGURATION DEFAULTS
# ============================================================================

SECURITY_DEFAULTS = {
    'enforce_https': False,              # HTTPS enforcement (disable for dev)
    'max_request_size_mb': 16,           # Maximum request payload size
    'max_json_depth': 10,                # Maximum JSON nesting depth
    'max_json_keys': 10000,              # Maximum total keys in JSON
    'rate_limit_enabled': False,         # Rate limiting (requires redis)
    'rate_limit_per_minute': 100,        # Requests per minute per API key
    'rate_limit_per_hour': 1000,         # Requests per hour per API key
    'log_failed_auth': True,             # Log failed authentication attempts
    'allowed_db_types': None,            # Whitelist of db_types (None = all)
    'ip_whitelist_enabled': False,       # IP-based restrictions per API key
}


def get_security_config():
    """
    Load security configuration from app config or use defaults.

    Configuration can be set in trends.yaml under 'security' section:

    security:
      enforce_https: false  # true for production
      max_request_size_mb: 16
      max_json_depth: 10
      rate_limit_enabled: true
      rate_limit_per_minute: 100
    """
    from .utils import load_trends_config

    config = load_trends_config()
    security_config = config.get('security', {})

    # Merge with defaults
    result = SECURITY_DEFAULTS.copy()
    result.update(security_config)

    return result


# ============================================================================
# HTTPS ENFORCEMENT (Optional)
# ============================================================================

def enforce_https_middleware():
    """
    Middleware to enforce HTTPS connections.

    Only enforces if:
    1. security.enforce_https is True in config
    2. Not in testing mode
    3. Request is not already secure

    This allows developers to work over HTTP in local environments
    while enforcing HTTPS in production.
    """
    security_config = get_security_config()

    # Skip if disabled or in testing
    if not security_config['enforce_https']:
        return None

    if current_app.config.get('TESTING'):
        return None

    # Skip if already secure
    if request.is_secure:
        return None

    # Skip if explicitly disabled for this request (internal calls)
    if request.environ.get('werkzeug.server.shutdown'):
        return None

    # Redirect HTTP to HTTPS
    if request.url.startswith('http://'):
        url = request.url.replace('http://', 'https://', 1)
        current_app.logger.warning(
            f"Redirecting insecure request: {request.remote_addr} -> {request.path}"
        )
        return redirect(url, code=301)

    return None


# ============================================================================
# REQUEST SIZE VALIDATION
# ============================================================================

def validate_request_size():
    """
    Validate request payload size.

    Returns:
        tuple: (is_valid, error_response)
        - is_valid: True if valid, False otherwise
        - error_response: Flask response object if invalid, None otherwise
    """
    security_config = get_security_config()
    max_size_bytes = security_config['max_request_size_mb'] * 1024 * 1024

    content_length = request.content_length

    if content_length and content_length > max_size_bytes:
        current_app.logger.warning(
            f"Request size too large: {content_length} bytes from {request.remote_addr}"
        )
        return False, jsonify({
            "error": "Payload too large",
            "message": f"Request must be smaller than {security_config['max_request_size_mb']}MB",
            "received_mb": round(content_length / (1024 * 1024), 2)
        }), 413

    return True, None


# ============================================================================
# JSON COMPLEXITY VALIDATION
# ============================================================================

def validate_json_complexity(data, max_depth=None, max_keys=None, _current_depth=0, _key_count=None):
    """
    Validate JSON structure complexity to prevent DoS attacks.

    Checks:
    1. Maximum nesting depth (prevents stack overflow)
    2. Maximum total keys (prevents memory exhaustion)

    Args:
        data: The data structure to validate
        max_depth: Maximum nesting depth (None = use config)
        max_keys: Maximum total keys (None = use config)
        _current_depth: Internal recursion counter
        _key_count: Internal key counter (list)

    Returns:
        tuple: (is_valid, error_message)

    Example:
        >>> validate_json_complexity({"a": {"b": {"c": 1}}}, max_depth=2)
        (False, "JSON nested too deeply (depth: 3, max: 2)")
    """
    security_config = get_security_config()

    if max_depth is None:
        max_depth = security_config['max_json_depth']
    if max_keys is None:
        max_keys = security_config['max_json_keys']

    # Initialize key counter on first call
    if _key_count is None:
        _key_count = [0]

    # Check depth
    if _current_depth > max_depth:
        return False, f"JSON nested too deeply (depth: {_current_depth}, max: {max_depth})"

    # Check key count
    if _key_count[0] > max_keys:
        return False, f"JSON has too many keys (count: {_key_count[0]}, max: {max_keys})"

    # Recurse through structure
    if isinstance(data, dict):
        _key_count[0] += len(data)
        for value in data.values():
            is_valid, error = validate_json_complexity(
                value, max_depth, max_keys, _current_depth + 1, _key_count
            )
            if not is_valid:
                return False, error

    elif isinstance(data, list):
        for item in data:
            is_valid, error = validate_json_complexity(
                item, max_depth, max_keys, _current_depth + 1, _key_count
            )
            if not is_valid:
                return False, error

    return True, None


# ============================================================================
# TYPE AND VALUE VALIDATION
# ============================================================================

def validate_target_info(target_info):
    """
    Validate target_info structure with type and value checking.

    Args:
        target_info (dict): Target information from submission

    Returns:
        tuple: (is_valid, error_message)
    """
    # Type checks
    if not isinstance(target_info.get('db_type'), str):
        return False, "target_info.db_type must be a string"

    if not isinstance(target_info.get('host'), str):
        return False, "target_info.host must be a string"

    if not isinstance(target_info.get('port'), int):
        return False, "target_info.port must be an integer"

    if not isinstance(target_info.get('database'), str):
        return False, "target_info.database must be a string"

    # Value validation
    port = target_info['port']
    if not (1 <= port <= 65535):
        return False, f"target_info.port must be between 1-65535 (got: {port})"

    host = target_info['host']
    if len(host) > 255:
        return False, f"target_info.host too long (max: 255 chars, got: {len(host)})"

    if len(host.strip()) == 0:
        return False, "target_info.host cannot be empty"

    db_type = target_info['db_type']
    security_config = get_security_config()
    allowed_types = security_config['allowed_db_types']

    if allowed_types and db_type not in allowed_types:
        return False, f"target_info.db_type '{db_type}' not in allowed list: {allowed_types}"

    database = target_info['database']
    if len(database) > 255:
        return False, f"target_info.database too long (max: 255 chars, got: {len(database)})"

    return True, None


def validate_submission_payload(data):
    """
    Comprehensive validation of the entire submission payload.

    Args:
        data (dict): The complete submission payload

    Returns:
        tuple: (is_valid, error_message)
    """
    # Check top-level structure
    if not isinstance(data.get('target_info'), dict):
        return False, "target_info must be a dictionary"

    if not isinstance(data.get('findings'), dict):
        return False, "findings must be a dictionary"

    if not isinstance(data.get('report_adoc'), str):
        return False, "report_adoc must be a string"

    # Optional analysis_results
    if 'analysis_results' in data and data['analysis_results'] is not None:
        if not isinstance(data['analysis_results'], dict):
            return False, "analysis_results must be a dictionary if provided"

    # Validate target_info
    is_valid, error = validate_target_info(data['target_info'])
    if not is_valid:
        return False, error

    # Validate JSON complexity
    is_valid, error = validate_json_complexity(data['findings'])
    if not is_valid:
        return False, f"findings: {error}"

    if 'analysis_results' in data and data['analysis_results']:
        is_valid, error = validate_json_complexity(data['analysis_results'])
        if not is_valid:
            return False, f"analysis_results: {error}"

    # Validate report size
    if len(data['report_adoc']) > 10 * 1024 * 1024:  # 10MB max for report
        return False, "report_adoc too large (max: 10MB)"

    return True, None


# ============================================================================
# FAILED AUTHENTICATION LOGGING
# ============================================================================

def log_failed_authentication(api_key_prefix, reason="invalid"):
    """
    Log failed authentication attempts for security monitoring.

    Args:
        api_key_prefix (str): First 8 characters of attempted key
        reason (str): Reason for failure (invalid, expired, revoked)
    """
    security_config = get_security_config()

    if not security_config['log_failed_auth']:
        return

    # Log to application logger
    current_app.logger.warning(
        f"Failed API authentication: "
        f"key_prefix={api_key_prefix}, "
        f"reason={reason}, "
        f"ip={request.remote_addr}, "
        f"user_agent={request.headers.get('User-Agent', 'unknown')}"
    )

    # TODO: Store in database for analysis
    # This would require a new table: api_auth_failures
    # For now, logging to file is sufficient for monitoring


# ============================================================================
# IP WHITELIST VALIDATION (Optional)
# ============================================================================

def validate_ip_whitelist(api_key_id):
    """
    Check if request IP is whitelisted for this API key.

    This is an optional feature that can be enabled per API key.
    Useful for restricting API keys to specific networks/servers.

    Args:
        api_key_id (int): The API key ID

    Returns:
        tuple: (is_allowed, error_message)
    """
    security_config = get_security_config()

    if not security_config['ip_whitelist_enabled']:
        return True, None

    # TODO: Implement IP whitelist checking
    # This would require:
    # 1. New table: api_key_ip_whitelist (api_key_id, ip_address/CIDR)
    # 2. Query to check if request.remote_addr matches any entry
    # 3. Support for CIDR notation (e.g., 192.168.1.0/24)

    # For now, just return True (feature not implemented)
    return True, None


# ============================================================================
# RATE LIMITING DECORATOR (Optional)
# ============================================================================

def rate_limit_check():
    """
    Check if request exceeds rate limits.

    This is a placeholder for flask-limiter integration.
    Rate limiting is optional and requires Redis.

    Returns:
        tuple: (is_allowed, error_response)
    """
    security_config = get_security_config()

    if not security_config['rate_limit_enabled']:
        return True, None

    # TODO: Implement with flask-limiter
    # Current approach: Log only
    current_app.logger.debug(
        f"Rate limit check (not enforced): {request.remote_addr} -> {request.path}"
    )

    return True, None


# ============================================================================
# SECURITY DECORATOR (Combines all checks)
# ============================================================================

def secure_api_endpoint(f):
    """
    Decorator that applies all enabled security checks to an API endpoint.

    Checks (in order):
    1. Request size limit
    2. Rate limiting (if enabled)
    3. IP whitelist (if enabled)

    Usage:
        @bp.route('/api/submit-health-check', methods=['POST'])
        @secure_api_endpoint
        @require_api_key
        def submit_health_check():
            ...

    Note: This should be applied BEFORE @require_api_key to catch
    oversized requests before authentication.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check 1: Request size
        is_valid, error_response = validate_request_size()
        if not is_valid:
            return error_response

        # Check 2: Rate limiting
        is_allowed, error_response = rate_limit_check()
        if not is_allowed:
            return error_response

        # Check 3: IP whitelist (requires API key to be validated first)
        # This is handled after @require_api_key decorator

        return f(*args, **kwargs)

    return decorated_function


# ============================================================================
# SECURITY STATUS ENDPOINT
# ============================================================================

def get_security_status():
    """
    Get current security configuration status.

    Returns:
        dict: Security settings and their states
    """
    security_config = get_security_config()

    return {
        "https_enforcement": {
            "enabled": security_config['enforce_https'],
            "description": "Redirects HTTP requests to HTTPS"
        },
        "request_size_limits": {
            "enabled": True,  # Always enabled
            "max_size_mb": security_config['max_request_size_mb']
        },
        "json_complexity_limits": {
            "enabled": True,  # Always enabled
            "max_depth": security_config['max_json_depth'],
            "max_keys": security_config['max_json_keys']
        },
        "rate_limiting": {
            "enabled": security_config['rate_limit_enabled'],
            "per_minute": security_config['rate_limit_per_minute'],
            "per_hour": security_config['rate_limit_per_hour']
        },
        "failed_auth_logging": {
            "enabled": security_config['log_failed_auth']
        },
        "db_type_whitelist": {
            "enabled": security_config['allowed_db_types'] is not None,
            "allowed_types": security_config['allowed_db_types']
        },
        "ip_whitelist": {
            "enabled": security_config['ip_whitelist_enabled']
        }
    }

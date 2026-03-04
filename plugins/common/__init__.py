"""
Common utilities shared across all database plugins.

This module provides reusable components for:
- SSH connection management
- Shell command execution
- Output formatting (AsciiDoc tables, etc.)
- Common parsers (nodetool, df, ps, etc.)
- Check helpers and utilities
- Cloud platform integrations (AWS, Azure, Instaclustr) - optional
- Retry logic for API calls
"""

from .ssh_handler import SSHConnectionManager
from .shell_executor import ShellExecutor
from .output_formatters import AsciiDocFormatter
from .parsers import NodetoolParser, ShellCommandParser
from .ssh_mixin import SSHSupportMixin
from .check_helpers import (
    require_ssh,
    require_aws,
    require_azure,
    require_instaclustr,
    format_check_header,
    format_recommendations,
    safe_execute_query,
    merge_structured_data,
    calculate_percentage,
    format_bytes
)
from .retry_utils import retry_on_failure, should_retry_error

# Cloud integrations are optional (require boto3, azure-identity, etc.)
try:
    from .aws_handler import AWSConnectionManager, AWSSupportMixin
except ImportError:
    AWSConnectionManager = None
    AWSSupportMixin = None

try:
    from .azure_handler import AzureConnectionManager, AzureSupportMixin
except ImportError:
    AzureConnectionManager = None
    AzureSupportMixin = None

try:
    from .instaclustr_handler import InstaclustrConnectionManager, InstaclustrSupportMixin
except ImportError:
    InstaclustrConnectionManager = None
    InstaclustrSupportMixin = None

__all__ = [
    # SSH Infrastructure
    'SSHConnectionManager',
    'SSHSupportMixin',
    
    # Command Execution
    'ShellExecutor',
    
    # Parsers
    'NodetoolParser',
    'ShellCommandParser',
    
    # Formatters
    'AsciiDocFormatter',
    
    # Check Helpers
    'require_ssh',
    'require_aws',
    'require_azure',
    'require_instaclustr',
    'format_check_header',
    'format_recommendations',
    'safe_execute_query',
    'merge_structured_data',
    'calculate_percentage',
    'format_bytes',
    
    # Cloud Integrations
    'AWSConnectionManager',
    'AWSSupportMixin',
    'AzureConnectionManager',
    'AzureSupportMixin',
    'InstaclustrConnectionManager',
    'InstaclustrSupportMixin',
    
    # Retry Utilities
    'retry_on_failure',
    'should_retry_error'
]

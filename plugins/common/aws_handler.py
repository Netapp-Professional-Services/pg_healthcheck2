"""
AWS connection management for database health checks.

Provides a reusable AWS connection manager for RDS and CloudWatch metrics.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
import yaml

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    # Define placeholder exceptions so decorators can reference them
    class ClientError(Exception):
        """Placeholder when boto3 is not installed."""
        pass
    class BotoCoreError(Exception):
        """Placeholder when boto3 is not installed."""
        pass

from .retry_utils import retry_on_failure

logger = logging.getLogger(__name__)


class AWSSupportMixin:
    """Mixin for AWS support detection in database connectors."""
    
    def has_aws_support(self) -> bool:
        """Check if AWS operations are supported."""
        if not hasattr(self, 'aws_manager'):
            return False
        return self.aws_manager is not None and self.aws_manager.is_configured()

    def get_aws_skip_message(self, operation_name: Optional[str] = None) -> tuple:
        """Generate a skip message for AWS-dependent checks."""
        op_text = f" for {operation_name}" if operation_name else ""
        adoc_message = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires AWS access{op_text}.\n\n"
            "Configure the following in your settings:\n\n"
            "* `aws_region`: AWS region (e.g., 'us-east-1')\n"
            "* `aws_access_key_id`: AWS access key\n"
            "* `aws_secret_access_key`: AWS secret key\n\n"
            "Required permissions:\n\n"
            "* `cloudwatch:GetMetricStatistics`\n"
            "* `rds:DescribeDBInstances` (if using RDS)\n"
            "====\n"
        )
        structured_data = {
            "status": "skipped",
            "reason": "AWS not configured",
            "required_settings": ["aws_region", "aws_access_key_id", "aws_secret_access_key"],
            "required_permissions": ["cloudwatch:GetMetricStatistics", "rds:DescribeDBInstances"]
        }
        return adoc_message, structured_data


class AWSConnectionManager:
    """
    Manages AWS connections for RDS and CloudWatch metrics.
    
    Uses boto3 for AWS API interactions.
    """
    
    def __init__(self, settings: Dict):
        """
        Initialize AWS connection manager.
        
        Args:
            settings: Dictionary with AWS configuration:
                - aws_region: AWS region
                - aws_access_key_id: AWS access key
                - aws_secret_access_key: AWS secret key
        """
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 library is required for AWS support. "
                "Install it with: pip install boto3"
            )
        
        self.settings = settings
        self.rds_client = None
        self.cloudwatch_client = None
        self._load_credentials()
        self._validate_settings()
        self.connect()

    def _load_credentials(self):
        """Load credentials from config file or use existing settings."""
        # Try to load from config/aws_credentials.yaml
        config_yaml = Path('config') / 'aws_credentials.yaml'
        
        if config_yaml.exists():
            try:
                with open(config_yaml, 'r') as f:
                    creds = yaml.safe_load(f)
                
                # Update settings with credentials from file
                self.settings.update({
                    'aws_region': creds.get('aws_region', self.settings.get('aws_region')),
                    'aws_access_key_id': creds.get('aws_access_key_id'),
                    'aws_secret_access_key': creds.get('aws_secret_access_key')
                })
                logger.info(f"Loaded AWS credentials from {config_yaml}")
            except Exception as e:
                logger.warning(f"Failed to load config/aws_credentials.yaml: {e}")
        
        # Set default region if not specified
        self.settings.setdefault('aws_region', 'us-east-1')

    def _validate_settings(self):
        """Validate that required AWS settings are present."""
        required = ['aws_region', 'aws_access_key_id', 'aws_secret_access_key']
        missing = [key for key in required if not self.settings.get(key)]
        
        if missing:
            raise ValueError(
                f"Missing required AWS settings: {', '.join(missing)}. "
                "Configure in settings or config/aws_credentials.yaml"
            )

    def connect(self) -> None:
        """Initialize AWS clients."""
        try:
            self.rds_client = boto3.client(
                'rds',
                region_name=self.settings['aws_region'],
                aws_access_key_id=self.settings['aws_access_key_id'],
                aws_secret_access_key=self.settings['aws_secret_access_key']
            )
            
            self.cloudwatch_client = boto3.client(
                'cloudwatch',
                region_name=self.settings['aws_region'],
                aws_access_key_id=self.settings['aws_access_key_id'],
                aws_secret_access_key=self.settings['aws_secret_access_key']
            )
            
            logger.info(f"✅ AWS clients initialized for region {self.settings['aws_region']}")
            
        except Exception as e:
            logger.error(f"AWS connection failed: {e}")
            raise ConnectionError(f"Could not initialize AWS clients: {e}")

    def is_configured(self) -> bool:
        """Check if AWS clients are initialized."""
        return self.rds_client is not None and self.cloudwatch_client is not None

    @retry_on_failure(max_attempts=3, delay=1, exceptions=(ClientError, BotoCoreError))
    def get_instance_details(self, db_identifier: str) -> Optional[Dict]:
        """
        Fetch RDS instance details.
        
        Args:
            db_identifier: RDS instance identifier
        
        Returns:
            Dict with instance details or None if not found
        """
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=db_identifier
            )
            
            if response['DBInstances']:
                instance = response['DBInstances'][0]
                return {
                    "instance_id": instance.get('DBInstanceIdentifier'),
                    "instance_class": instance.get('DBInstanceClass'),
                    "engine": instance.get('Engine'),
                    "engine_version": instance.get('EngineVersion'),
                    "allocated_storage_gb": instance.get('AllocatedStorage'),
                    "status": instance.get('DBInstanceStatus'),
                    "endpoint": instance.get('Endpoint', {}).get('Address'),
                    "port": instance.get('Endpoint', {}).get('Port'),
                    "availability_zone": instance.get('AvailabilityZone'),
                    "multi_az": instance.get('MultiAZ')
                }
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'DBInstanceNotFound':
                logger.warning(f"RDS instance not found: {db_identifier}")
            elif error_code in ('AccessDenied', 'UnauthorizedOperation'):
                logger.error(f"Permission denied for RDS instance {db_identifier}: {e}")
            else:
                logger.warning(f"Could not fetch RDS instance details for {db_identifier}: {e}")
        
        except Exception as e:
            logger.error(f"Unexpected error fetching RDS instance details: {e}")
        
        return None

    def get_cloudwatch_metrics(self, 
                              dimensions: List[Dict], 
                              metrics_to_fetch: List[Dict], 
                              hours: int = 24, 
                              period: int = 3600) -> Dict:
        """
        Fetch CloudWatch metrics with retry logic and error classification.
        
        Args:
            dimensions: CloudWatch dimensions (e.g., [{'Name': 'DBInstanceIdentifier', 'Value': 'mydb'}])
            metrics_to_fetch: List of metric configs with Namespace, MetricName, Statistic, Unit
            hours: Number of hours of historical data to fetch
            period: Metric aggregation period in seconds
        
        Returns:
            Dict mapping metric names to their values and metadata
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        fetched_metrics = {}
        
        for metric_info in metrics_to_fetch:
            metric_name = metric_info['MetricName']
            
            try:
                # Apply retry decorator to the API call
                @retry_on_failure(
                    max_attempts=3, 
                    delay=1, 
                    exceptions=(ClientError,),
                    log_attempts=True
                )
                def fetch_metric():
                    return self.cloudwatch_client.get_metric_statistics(
                        Namespace=metric_info['Namespace'],
                        MetricName=metric_name,
                        Dimensions=dimensions,
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=period,
                        Statistics=[metric_info['Statistic']],
                        Unit=metric_info['Unit']
                    )
                
                response = fetch_metric()
                
                if response['Datapoints']:
                    # Get latest datapoint
                    latest_datapoint = sorted(
                        response['Datapoints'],
                        key=lambda x: x['Timestamp'],
                        reverse=True
                    )[0]
                    
                    value = latest_datapoint.get(metric_info['Statistic'])
                    
                    fetched_metrics[metric_name] = {
                        "value": value,
                        "unit": metric_info['Unit'],
                        "statistic": metric_info['Statistic'],
                        "timestamp": latest_datapoint['Timestamp'].isoformat(),
                        "status": "success"
                    }
                else:
                    fetched_metrics[metric_name] = {
                        "value": "N/A",
                        "note": "No data points found.",
                        "status": "no_data"
                    }
            
            except ClientError as e:
                error_code = e.response['Error']['Code']
                
                # Classify errors for better handling
                if error_code in ('AccessDeniedException', 'UnauthorizedOperation'):
                    logger.error(f"AWS permission error for {metric_name}: {error_code}")
                    note = f"Permission denied: {error_code}"
                    status = "permission_error"
                
                elif error_code in ('Throttling', 'RequestLimitExceeded'):
                    logger.warning(f"AWS rate limit for {metric_name}: {error_code}")
                    note = f"Rate limited: {error_code}"
                    status = "rate_limited"
                
                elif error_code == 'InvalidParameterValue':
                    logger.error(f"Invalid parameter for {metric_name}: {e}")
                    note = f"Invalid parameters: {error_code}"
                    status = "invalid_parameters"
                
                else:
                    logger.warning(f"AWS error for {metric_name}: {error_code}")
                    note = f"Error: {error_code}"
                    status = "error"
                
                fetched_metrics[metric_name] = {
                    "value": "N/A",
                    "note": note,
                    "error_code": error_code,
                    "status": status
                }
            
            except Exception as e:
                logger.error(f"Unexpected error fetching {metric_name}: {e}")
                fetched_metrics[metric_name] = {
                    "value": "N/A",
                    "note": f"Unexpected error: {str(e)}",
                    "status": "error"
                }
        
        return fetched_metrics

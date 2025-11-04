import boto3
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def discover_rds_proxy(aws_region, db_identifier, cluster_id=None):
    """Discovers RDS Proxy associated with a DB instance or cluster.

    Searches for RDS Proxies that target the specified database instance
    or cluster. This enables automatic proxy discovery without manual configuration.

    Args:
        aws_region (str): The AWS region where the RDS resources are located.
        db_identifier (str): The DBInstanceIdentifier to search for.
        cluster_id (str, optional): The DBClusterIdentifier to search for.

    Returns:
        str | None: The DBProxyName if found, otherwise None.
    """
    rds = boto3.client('rds', region_name=aws_region)
    try:
        # List all DB proxies in the region
        response = rds.describe_db_proxies()

        for proxy in response.get('DBProxies', []):
            proxy_name = proxy.get('DBProxyName')

            # Get the targets for this proxy
            try:
                targets_response = rds.describe_db_proxy_targets(DBProxyName=proxy_name)

                for target in targets_response.get('Targets', []):
                    # Check if this target matches our instance or cluster
                    target_arn = target.get('TargetArn', '')

                    # Match by instance identifier
                    if db_identifier and db_identifier in target_arn:
                        logger.info(f"Discovered RDS Proxy '{proxy_name}' for instance '{db_identifier}'")
                        return proxy_name

                    # Match by cluster identifier
                    if cluster_id and cluster_id in target_arn:
                        logger.info(f"Discovered RDS Proxy '{proxy_name}' for cluster '{cluster_id}'")
                        return proxy_name

            except Exception as e:
                logger.debug(f"Could not check targets for proxy '{proxy_name}': {e}")
                continue

    except Exception as e:
        logger.debug(f"Could not discover RDS Proxy: {e}")

    return None

def get_instance_details(aws_region, db_identifier):
    """Fetches RDS instance details like instance class and storage.

    Uses the boto3 client to call the `describe_db_instances` API
    endpoint and retrieve key metadata for the specified database instance.

    Args:
        aws_region (str): The AWS region where the RDS instance is located
            (e.g., "us-east-1").
        db_identifier (str): The unique DBInstanceIdentifier for the RDS
            database instance.

    Returns:
        dict | None: A dictionary containing instance details if found,
        otherwise None. The dictionary includes 'instance_class' and
        'allocated_storage_gb'.
    """
    rds = boto3.client('rds', region_name=aws_region)
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
        if response['DBInstances']:
            instance = response['DBInstances'][0]
            # This can be expanded later to fetch RAM size based on instance class
            return {
                "instance_class": instance.get('DBInstanceClass'),
                "allocated_storage_gb": instance.get('AllocatedStorage')
            }
    except Exception as e:
        print(f"Warning: Could not fetch RDS instance details for {db_identifier}: {e}")
    return None

def get_cloudwatch_metrics(aws_region, dimensions, metrics_to_fetch, hours=24, period=3600):
    """Fetches a set of specified metrics from Amazon CloudWatch.

    This function iterates through a list of desired metrics, queries the
    `GetMetricStatistics` API for each one over a defined time period,
    and returns the latest datapoint found for each. It gracefully handles
    cases where a metric may not exist for a given resource.

    Args:
        aws_region (str): The AWS region to query (e.g., "us-east-1").
        dimensions (list[dict]): The dimensions to filter the metrics, e.g.,
            `[{'Name': 'DBInstanceIdentifier', 'Value': 'my-db-instance'}]`.
        metrics_to_fetch (list[dict]): A list of dictionaries, each defining
            a metric. Each dict must contain 'Namespace', 'MetricName',
            'Statistic', and 'Unit'.
        hours (int): The number of hours back from now to query. Defaults to 24.
        period (int): The granularity of the data points in seconds.
            Defaults to 3600 (1 hour).

    Returns:
        dict: A dictionary where keys are the MetricNames and values are objects
              containing the latest datapoint's value, unit, statistic, and
              timestamp.
    """
    cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    
    fetched_metrics = {}
    for metric_info in metrics_to_fetch:
        try:
            # Build parameters for get_metric_statistics
            params = {
                'Namespace': metric_info['Namespace'],
                'MetricName': metric_info['MetricName'],
                'Dimensions': dimensions,
                'StartTime': start_time,
                'EndTime': end_time,
                'Period': period,
                'Statistics': [metric_info['Statistic']]
            }

            # Only add Unit if it's specified (some metrics like VolumeBytesUsed don't use it)
            if metric_info.get('Unit'):
                params['Unit'] = metric_info['Unit']

            response = cloudwatch.get_metric_statistics(**params)
            
            if response['Datapoints']:
                # Get the most recent datapoint
                latest_datapoint = sorted(response['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[0]
                value = latest_datapoint.get(metric_info['Statistic'])
                # Determine unit - use specified unit or infer from metric name
                unit = metric_info.get('Unit')
                if not unit:
                    # Infer unit for metrics without explicit unit specification
                    if 'Bytes' in metric_info['MetricName']:
                        unit = 'Bytes'
                    else:
                        unit = 'None'

                fetched_metrics[metric_info['MetricName']] = {
                    "value": value,
                    "unit": unit,
                    "statistic": metric_info['Statistic'],
                    "timestamp": latest_datapoint['Timestamp'].isoformat()
                }
            else:
                fetched_metrics[metric_info['MetricName']] = {"value": "N/A", "note": "No data points found."}
        except Exception as e:
            # Gracefully handle missing metrics (e.g., AuroraReplicaLag on a non-Aurora instance)
            fetched_metrics[metric_info['MetricName']] = {"value": "N/A", "note": f"Could not fetch: {e}"}
            print(f"Warning: Could not fetch CloudWatch metric {metric_info['MetricName']}: {e}")
            
    return fetched_metrics

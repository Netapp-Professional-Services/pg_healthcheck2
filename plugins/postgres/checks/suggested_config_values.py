"""
PostgreSQL Configuration Tuning Recommendations

Analyzes current PostgreSQL configuration and provides PGTune-style recommendations
based on actual system resources (RAM, CPU). Handles Aurora/RDS and bare-metal differently.
"""

from plugins.common.check_helpers import CheckContentBuilder, require_ssh
import re

def get_weight():
    """Returns the importance score for this module."""
    return 7  # Configuration tuning is important for performance


def run_suggested_config_values(connector, settings):
    """
    Analyzes configuration and provides PGTune-style recommendations.

    For Aurora/RDS: Shows current settings with notes about AWS management
    For Bare-Metal: Detects system resources and calculates optimal values

    Args:
        connector: PostgreSQL connector
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("Configuration Tuning Recommendations")

    # Get PostgreSQL version
    pg_major_version = connector.version_info.get('major_version', 0)
    builder.add(f"_Analysis for PostgreSQL {pg_major_version}_")
    builder.blank()

    # Check if Aurora/RDS
    is_aurora = hasattr(connector, 'environment') and connector.environment in ['aurora', 'rds']

    if is_aurora:
        return _handle_aurora_config(builder, connector, structured_data)
    else:
        return _handle_baremetal_config(builder, connector, settings, pg_major_version, structured_data)


def _handle_aurora_config(builder, connector, structured_data):
    """Handle Aurora/RDS configuration display."""
    builder.h4("Aurora/RDS Configuration")

    builder.note("In AWS Aurora/RDS, most performance parameters are automatically managed by AWS based on your instance class. "
                 "Tuning should be done through DB Parameter Groups in the AWS console.")

    # Get current settings to display
    settings_to_show = [
        'shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size',
        'max_connections', 'checkpoint_completion_target', 'max_wal_size', 'random_page_cost'
    ]

    settings_placeholders = ', '.join([f"'{s}'" for s in settings_to_show])
    config_query = f"SELECT name, setting, unit FROM pg_settings WHERE name IN ({settings_placeholders}) ORDER BY name;"

    formatted_result, raw_settings = connector.execute_query(config_query, return_raw=True)

    if "[ERROR]" not in formatted_result and raw_settings:
        builder.h4("Current Settings (AWS-Managed)")

        settings_table = []
        for setting in raw_settings:
            name = setting['name']
            value_str = setting['setting']
            unit_str = setting.get('unit', '')

            # Format display value
            display_value = _format_setting_value(value_str, unit_str)

            settings_table.append({
                'Setting': name,
                'Current Value': display_value,
                'Managed By': 'AWS RDS'
            })

        builder.table(settings_table)

        recommendations = [
            "Most settings are optimized by AWS based on your instance class",
            "To adjust settings, modify the DB Parameter Group in AWS Console",
            "Consider upgrading instance class for better performance instead of tuning",
            "Review CloudWatch metrics to identify actual bottlenecks before changing settings"
        ]
        builder.recs(recommendations, title="Aurora Tuning Notes")

    structured_data['environment'] = 'aurora'
    structured_data['current_settings'] = raw_settings if raw_settings else []
    structured_data['status'] = 'success'

    return builder.build(), structured_data


def _handle_baremetal_config(builder, connector, settings, pg_major_version, structured_data):
    """Handle bare-metal/EC2 configuration analysis with PGTune recommendations."""

    # Check if SSH is available
    ssh_available, skip_msg, skip_data = require_ssh(connector, "system resource detection")
    if not ssh_available:
        builder.warning("SSH access is required to detect system resources and calculate optimal settings.")
        builder.add(skip_msg)
        return builder.build(), skip_data

    # Detect system resources
    ssh_hosts = connector.get_ssh_hosts()
    if not ssh_hosts:
        builder.warning("No SSH hosts configured. Cannot detect system resources for configuration recommendations.")
        structured_data['status'] = 'error'
        structured_data['error'] = 'no_ssh_hosts'
        return builder.build(), structured_data

    ssh_host = ssh_hosts[0]
    ssh_manager = connector.get_ssh_manager(ssh_host)

    builder.h4("Detecting System Resources...")

    system_resources = _detect_system_resources(ssh_manager)

    if not system_resources['detected']:
        builder.warning("Could not detect system resources. Unable to calculate optimal configuration values.")
        structured_data['status'] = 'error'
        structured_data['error'] = 'resource_detection_failed'
        return builder.build(), structured_data

    # Display detected resources
    resource_info = [
        f"- **Total RAM**: {system_resources['ram_gb']:.1f} GB ({system_resources['ram_mb']:.0f} MB)",
        f"- **CPU Cores**: {system_resources['cpu_cores']}",
    ]
    if system_resources['storage_type']:
        resource_info.append(f"- **Storage Type**: {system_resources['storage_type']}")

    builder.add_lines(resource_info)
    builder.blank()

    structured_data['system_resources'] = system_resources

    # Get current settings
    settings_to_check = [
        'shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size',
        'max_connections', 'checkpoint_completion_target', 'max_wal_size', 'min_wal_size',
        'random_page_cost', 'effective_io_concurrency', 'wal_buffers',
        'default_statistics_target', 'max_worker_processes', 'max_parallel_workers_per_gather'
    ]

    settings_placeholders = ', '.join([f"'{s}'" for s in settings_to_check])
    config_query = f"SELECT name, setting, unit, boot_val, reset_val FROM pg_settings WHERE name IN ({settings_placeholders}) ORDER BY name;"

    formatted_result, raw_settings = connector.execute_query(config_query, return_raw=True)

    if "[ERROR]" in formatted_result or not raw_settings:
        builder.critical("Could not retrieve current configuration settings.")
        structured_data['status'] = 'error'
        return builder.build(), structured_data

    # Parse current settings
    current_config = {}
    for setting in raw_settings:
        name = setting['name']
        value_str = setting['setting']
        unit_str = setting.get('unit', '')
        current_config[name] = {
            'value': value_str,
            'unit': unit_str,
            'display': _format_setting_value(value_str, unit_str)
        }

    # Determine workload type (default to 'mixed', could be configurable)
    workload_type = settings.get('workload_type', 'mixed')

    # Calculate PGTune recommendations
    recommendations = _calculate_pgtune_recommendations(
        system_resources,
        workload_type,
        pg_major_version,
        current_config
    )

    structured_data['current_config'] = current_config
    structured_data['recommendations'] = recommendations
    structured_data['workload_type'] = workload_type
    structured_data['status'] = 'success'

    # Add helper fields for rules to reference
    for setting_name, rec in recommendations.items():
        structured_data['recommendations'][setting_name]['_system_ram_gb'] = system_resources['ram_gb']
        structured_data['recommendations'][setting_name]['_cpu_cores'] = system_resources['cpu_cores']
        structured_data['recommendations'][setting_name]['_storage_type'] = system_resources['storage_type']
        structured_data['recommendations'][setting_name]['current_value'] = current_config.get(setting_name, {}).get('value', '0')

    # Display results
    builder.h4("Configuration Analysis")

    builder.add(f"**Workload Type**: {workload_type.title()} _(default, configurable via `workload_type` setting)_")
    builder.blank()

    # Create comparison table
    comparison_table = []
    has_recommendations = False

    for setting_name, rec in recommendations.items():
        current_val = current_config.get(setting_name, {}).get('display', 'Unknown')
        recommended_val = rec['recommended_display']

        # Determine status
        if rec['needs_change']:
            has_recommendations = True
            status = '⚠️ Review'
            gap = rec.get('gap_display', '')
        else:
            status = '✓ OK'
            gap = ''

        comparison_table.append({
            'Setting': setting_name,
            'Current': current_val,
            'Recommended': recommended_val,
            'Status': status,
            'Reasoning': rec['reasoning'][:60] + '...' if len(rec['reasoning']) > 60 else rec['reasoning']
        })

    builder.table(comparison_table)
    builder.blank()

    # Show detailed recommendations
    if has_recommendations:
        builder.h4("Recommended Changes")

        changes_needed = {k: v for k, v in recommendations.items() if v['needs_change']}

        for setting_name, rec in changes_needed.items():
            builder.add(f"**{setting_name}**")
            builder.add(f"- Current: `{current_config.get(setting_name, {}).get('display', 'Unknown')}`")
            builder.add(f"- Recommended: `{rec['recommended_display']}`")
            builder.add(f"- Reasoning: {rec['reasoning']}")
            builder.blank()

        # Provide PostgreSQL configuration instructions
        builder.h4("How to Apply Changes")

        config_instructions = [
            "Edit `postgresql.conf` (usually in data directory or `/etc/postgresql/`)",
            "Add or update the settings listed above",
            "Reload configuration: `SELECT pg_reload_conf();` or `sudo systemctl reload postgresql`",
            "Some settings require a restart: `sudo systemctl restart postgresql`",
            "Settings requiring restart: `shared_buffers`, `max_connections`, `max_worker_processes`",
            "Always test configuration changes in a non-production environment first",
            "Monitor performance after changes using `pg_stat_statements` and system metrics"
        ]

        for instruction in config_instructions:
            builder.add(f"* {instruction}")

        builder.blank()

    else:
        builder.note("Your configuration values are within recommended ranges for your system resources and workload type.")

    return builder.build(), structured_data


def _detect_system_resources(ssh_manager):
    """
    Detect system resources via SSH.

    Returns:
        dict: System resource information (RAM, CPU, storage type)
    """
    resources = {
        'detected': False,
        'ram_mb': 0,
        'ram_gb': 0,
        'cpu_cores': 0,
        'storage_type': None
    }

    try:
        # Detect RAM
        stdout, stderr, exit_code = ssh_manager.execute_command("cat /proc/meminfo | grep MemTotal")
        if exit_code == 0:
            # Parse: MemTotal:       16384000 kB
            match = re.search(r'MemTotal:\s+(\d+)\s+kB', stdout)
            if match:
                ram_kb = int(match.group(1))
                resources['ram_mb'] = ram_kb / 1024
                resources['ram_gb'] = ram_kb / (1024 * 1024)

        # Detect CPU cores
        stdout, stderr, exit_code = ssh_manager.execute_command("nproc")
        if exit_code == 0:
            resources['cpu_cores'] = int(stdout.strip())

        # Try to detect storage type (SSD vs HDD)
        # Check if root device is rotational (0=SSD, 1=HDD)
        stdout, stderr, exit_code = ssh_manager.execute_command(
            "lsblk -d -o name,rota | grep -v loop | tail -n +2 | head -1 | awk '{print $2}'"
        )
        if exit_code == 0 and stdout.strip():
            rota = stdout.strip()
            if rota == '0':
                resources['storage_type'] = 'SSD'
            elif rota == '1':
                resources['storage_type'] = 'HDD'

        # Mark as detected if we got RAM and CPU
        if resources['ram_mb'] > 0 and resources['cpu_cores'] > 0:
            resources['detected'] = True

    except Exception as e:
        pass

    return resources


def _calculate_pgtune_recommendations(system_resources, workload_type, pg_version, current_config):
    """
    Calculate PGTune-style configuration recommendations.

    Based on: https://pgtune.leopard.in.ua/
    """
    ram_mb = system_resources['ram_mb']
    ram_gb = system_resources['ram_gb']
    cpu_cores = system_resources['cpu_cores']
    storage_type = system_resources['storage_type'] or 'SSD'  # Default to SSD

    recommendations = {}

    # shared_buffers: 25% of RAM, capped at 16GB for most workloads
    # Web: 25% up to 8GB, DW: 25% up to 16GB, OLTP: 25% up to 8GB
    if workload_type == 'datawarehouse':
        shared_buffers_mb = min(ram_mb * 0.25, 16 * 1024)
    else:
        shared_buffers_mb = min(ram_mb * 0.25, 8 * 1024)

    recommendations['shared_buffers'] = _make_recommendation(
        'shared_buffers',
        shared_buffers_mb,
        'MB',
        current_config,
        f"25% of system RAM (capped for {workload_type} workload)"
    )

    # effective_cache_size: 75% of RAM
    effective_cache_size_mb = ram_mb * 0.75

    recommendations['effective_cache_size'] = _make_recommendation(
        'effective_cache_size',
        effective_cache_size_mb,
        'MB',
        current_config,
        "75% of RAM - estimates OS cache for query planning"
    )

    # maintenance_work_mem: RAM/16, capped at 2GB
    maintenance_work_mem_mb = min(ram_mb / 16, 2048)

    recommendations['maintenance_work_mem'] = _make_recommendation(
        'maintenance_work_mem',
        maintenance_work_mem_mb,
        'MB',
        current_config,
        "Speeds up VACUUM, CREATE INDEX, and other maintenance"
    )

    # work_mem: Depends on workload and max_connections
    # Formula: (RAM - shared_buffers) / (max_connections * 3)
    max_connections = int(current_config.get('max_connections', {}).get('value', 100))
    work_mem_mb = max((ram_mb - shared_buffers_mb) / (max_connections * 3), 4)

    if workload_type == 'datawarehouse':
        work_mem_mb = max(work_mem_mb, 32)  # DW needs more for complex queries

    recommendations['work_mem'] = _make_recommendation(
        'work_mem',
        work_mem_mb,
        'MB',
        current_config,
        f"Per-operation memory, based on RAM and {max_connections} connections"
    )

    # checkpoint_completion_target: 0.9 for all workloads
    recommendations['checkpoint_completion_target'] = _make_recommendation(
        'checkpoint_completion_target',
        0.9,
        None,
        current_config,
        "Spreads checkpoint I/O over more time"
    )

    # wal_buffers: 3% of shared_buffers, capped at 16MB
    wal_buffers_mb = min(shared_buffers_mb * 0.03, 16)

    recommendations['wal_buffers'] = _make_recommendation(
        'wal_buffers',
        wal_buffers_mb,
        'MB',
        current_config,
        "WAL write buffer, 3% of shared_buffers"
    )

    # max_wal_size: Depends on workload
    if workload_type == 'datawarehouse':
        max_wal_size_mb = 8192  # 8GB for DW
    elif workload_type == 'oltp':
        max_wal_size_mb = 4096  # 4GB for OLTP
    else:
        max_wal_size_mb = 2048  # 2GB for web/mixed

    recommendations['max_wal_size'] = _make_recommendation(
        'max_wal_size',
        max_wal_size_mb,
        'MB',
        current_config,
        f"Checkpoint frequency control for {workload_type} workload"
    )

    # min_wal_size: 25% of max_wal_size
    min_wal_size_mb = max_wal_size_mb * 0.25

    recommendations['min_wal_size'] = _make_recommendation(
        'min_wal_size',
        min_wal_size_mb,
        'MB',
        current_config,
        "Minimum WAL size to keep"
    )

    # random_page_cost: SSD vs HDD
    if storage_type == 'SSD':
        random_page_cost = 1.1
        reasoning = "SSD storage - encourages index usage"
    else:
        random_page_cost = 4.0
        reasoning = "HDD storage - default value"

    recommendations['random_page_cost'] = _make_recommendation(
        'random_page_cost',
        random_page_cost,
        None,
        current_config,
        reasoning
    )

    # effective_io_concurrency: SSD vs HDD
    if storage_type == 'SSD':
        effective_io_concurrency = 200
        reasoning = "SSD can handle many concurrent I/O operations"
    else:
        effective_io_concurrency = 2
        reasoning = "HDD has limited concurrent I/O capability"

    recommendations['effective_io_concurrency'] = _make_recommendation(
        'effective_io_concurrency',
        effective_io_concurrency,
        None,
        current_config,
        reasoning
    )

    # default_statistics_target: Higher for DW, moderate for others
    if workload_type == 'datawarehouse':
        default_statistics_target = 500
    else:
        default_statistics_target = 100

    recommendations['default_statistics_target'] = _make_recommendation(
        'default_statistics_target',
        default_statistics_target,
        None,
        current_config,
        "Query planner statistics detail level"
    )

    # Parallel query settings (PG 9.6+)
    if pg_version >= 10:
        # max_worker_processes: number of CPU cores
        recommendations['max_worker_processes'] = _make_recommendation(
            'max_worker_processes',
            cpu_cores,
            None,
            current_config,
            f"One worker per CPU core ({cpu_cores} cores detected)"
        )

        # max_parallel_workers_per_gather: half of CPU cores, min 2
        max_parallel_workers = max(int(cpu_cores / 2), 2)

        recommendations['max_parallel_workers_per_gather'] = _make_recommendation(
            'max_parallel_workers_per_gather',
            max_parallel_workers,
            None,
            current_config,
            f"Parallel query workers (half of {cpu_cores} cores)"
        )

    return recommendations


def _make_recommendation(setting_name, recommended_value, unit, current_config, reasoning):
    """
    Create a recommendation dict comparing current vs recommended value.
    """
    current = current_config.get(setting_name, {})
    current_value = current.get('value', '0')
    current_unit = current.get('unit', unit)

    # Convert current value to same unit as recommended
    if unit == 'MB':
        current_mb = _parse_memory_to_mb(current_value, current_unit)
        needs_change = abs(current_mb - recommended_value) > (recommended_value * 0.1)  # 10% tolerance
        gap = recommended_value - current_mb
        gap_display = f"{gap:+.0f} MB" if needs_change else ""
        recommended_display = f"{recommended_value:.0f} MB"
    elif unit is None:
        # Numeric or float values
        current_float = float(current_value) if current_value else 0
        if isinstance(recommended_value, float):
            needs_change = abs(current_float - recommended_value) > 0.05
            gap_display = f"{recommended_value - current_float:+.2f}" if needs_change else ""
            recommended_display = f"{recommended_value:.1f}"
        else:
            needs_change = current_float != recommended_value
            gap_display = f"{recommended_value - int(current_float):+d}" if needs_change else ""
            recommended_display = str(recommended_value)
    else:
        needs_change = False
        gap_display = ""
        recommended_display = str(recommended_value)

    return {
        'recommended_value': recommended_value,
        'recommended_display': recommended_display,
        'needs_change': needs_change,
        'gap_display': gap_display,
        'reasoning': reasoning
    }


def _parse_memory_to_mb(value_str, unit_str):
    """Parse PostgreSQL memory setting to megabytes."""
    if not value_str or not value_str.replace('.', '').isdigit():
        return 0

    value = float(value_str)

    if not unit_str:
        # No unit means it's in server's default unit (usually 8kB blocks)
        return (value * 8) / 1024

    if unit_str == 'kB':
        return value / 1024
    elif unit_str == 'MB':
        return value
    elif unit_str == 'GB':
        return value * 1024
    elif 'kB' in unit_str:  # Handles '8kB', '16kB', etc.
        block_size_kb = int(re.findall(r'\d+', unit_str)[0])
        return (value * block_size_kb) / 1024

    return 0


def _format_setting_value(value_str, unit_str):
    """Format a setting value for display."""
    if not unit_str:
        return value_str

    if unit_str in ['kB', 'MB', 'GB']:
        # Convert to MB for consistent display
        mb = _parse_memory_to_mb(value_str, unit_str)
        if mb >= 1024:
            return f"{mb / 1024:.1f} GB"
        else:
            return f"{mb:.0f} MB"
    elif '8kB' in unit_str or '16kB' in unit_str:
        # Block-based units
        mb = _parse_memory_to_mb(value_str, unit_str)
        if mb >= 1024:
            return f"{mb / 1024:.1f} GB"
        else:
            return f"{mb:.0f} MB"
    else:
        return f"{value_str} {unit_str}".strip()

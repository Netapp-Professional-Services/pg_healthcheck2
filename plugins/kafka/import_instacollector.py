#!/usr/bin/env python3
"""
Kafka Instacollector Import Tool

Generates health check reports from Instaclustr Instacollector exports.
This allows offline analysis of Kafka cluster health without live access.

Usage:
    # Using a config file (recommended):
    python -m plugins.kafka.import_instacollector --config config/kafka_import.yaml

    # Using CLI arguments:
    python -m plugins.kafka.import_instacollector /path/to/instacollector-export [options]

    # From tarball
    python -m plugins.kafka.import_instacollector ./InstaCollection_kafka.tar.gz

    # From extracted directory
    python -m plugins.kafka.import_instacollector ./instacollector_export/

    # Auto-detect most recent export in a directory:
    python -m plugins.kafka.import_instacollector /path/to/uploads/

    # With company name for trends tracking
    python -m plugins.kafka.import_instacollector ./export.tar.gz --company "Acme Corp"

Smart Path Detection:
    When given a directory path, the tool will automatically find the most recent
    Instacollector export by looking for files/directories matching:
    - InstaCollection_*.tar.gz
    - Directories containing IP-named subdirectories

The Instacollector tool is provided by Instaclustr for collecting Kafka diagnostics.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from datetime import datetime

import yaml

# Ensure project root is in path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from plugins.kafka.instacollector_connector import KafkaInstacollectorConnector
from plugins.kafka.instacollector_loader import find_most_recent_collection
from plugins.kafka import KafkaPlugin
from utils.report_builder import ReportBuilder
from utils.json_utils import UniversalJSONEncoder


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate health check report from Kafka Instacollector export',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        'diagnostic_path',
        nargs='?',
        help='Path to Instacollector export (tarball or extracted directory)'
    )

    parser.add_argument(
        '--config',
        help='Path to YAML config file'
    )

    parser.add_argument(
        '--output-dir', '-o',
        help='Output directory for report (default: adoc_out/<company_name>)'
    )

    parser.add_argument(
        '--company', '-c',
        help='Company name for report identification'
    )

    parser.add_argument(
        '--report-config', '-r',
        help='Custom report configuration file'
    )

    parser.add_argument(
        '--ship-trends',
        action='store_true',
        default=None,
        help='Ship results to trends database'
    )

    parser.add_argument(
        '--trends-config',
        help='Path to trends configuration file (default: config/trends.yaml)'
    )

    parser.add_argument(
        '--json-only',
        action='store_true',
        help='Output only structured JSON findings (no AsciiDoc)'
    )

    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress progress output'
    )

    return parser.parse_args()


def load_config(config_path):
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ Error: Config file not found: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"❌ Error: Invalid YAML in config file: {e}")
        sys.exit(1)


def merge_args_with_config(args, config):
    """Merge CLI arguments with config file settings."""
    class MergedArgs:
        pass

    merged = MergedArgs()

    # diagnostic_path: CLI > config
    merged.diagnostic_path = args.diagnostic_path or config.get('diagnostic_path')

    # company: CLI > config
    merged.company = args.company or config.get('company_name', 'kafka_import')

    # output_dir: CLI > config
    merged.output_dir = args.output_dir or config.get('output_dir')

    # report_config: CLI > config
    merged.report_config = args.report_config or config.get('report_config')

    # ship_trends: CLI > config
    if args.ship_trends is not None:
        merged.ship_trends = args.ship_trends
    else:
        merged.ship_trends = config.get('trend_storage_enabled', False)

    # trends_config: CLI > config
    merged.trends_config = args.trends_config or config.get('trends_config', 'config/trends.yaml')

    # Other flags
    merged.json_only = args.json_only
    merged.quiet = args.quiet
    merged.full_config = config

    return merged


def resolve_diagnostic_path(path_str, quiet=False):
    """
    Resolve the diagnostic path, handling:
    1. Direct path to tarball
    2. Direct path to extracted directory
    3. Directory containing Instacollector exports (finds most recent)
    """
    if not path_str:
        return None

    path = Path(path_str)

    if not path.exists():
        return None

    # Case 1: Direct path to a tarball
    if path.is_file() and (path.suffix in ('.gz', '.tgz') or path.name.endswith('.tar.gz')):
        return path

    # Case 2: Direct path to a directory with Instacollector data
    if path.is_dir():
        ip_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')

        # Check if it looks like an Instacollector export
        has_node_dirs = any(
            ip_pattern.match(item.name) and item.is_dir()
            for item in path.iterdir()
        )
        if has_node_dirs:
            return path

        # Check subdirectories
        for subdir in path.iterdir():
            if subdir.is_dir():
                has_node_dirs = any(
                    ip_pattern.match(item.name) and item.is_dir()
                    for item in subdir.iterdir()
                )
                if has_node_dirs:
                    if not quiet:
                        print(f"📁 Found Instacollector data in: {subdir.name}")
                    return subdir

        # Case 3: Find most recent
        latest = find_most_recent_collection(path)
        if latest:
            if not quiet:
                print(f"📁 Using most recent: {latest.name}")
            return latest

        return path

    return path


def main():
    """Main entry point."""
    raw_args = parse_args()

    # Load config file if specified
    if raw_args.config:
        config = load_config(raw_args.config)
        args = merge_args_with_config(raw_args, config)
    else:
        config = {}
        args = merge_args_with_config(raw_args, config)
        if not args.company:
            args.company = 'kafka_import'
        args.full_config = {}

    # Validate diagnostic path
    if not args.diagnostic_path:
        print("❌ Error: No diagnostic path provided.")
        print("   Either specify a path as argument or use --config with diagnostic_path in YAML")
        print("\nExamples:")
        print("   python -m plugins.kafka.import_instacollector /path/to/InstaCollection.tar.gz")
        print("   python -m plugins.kafka.import_instacollector --config config/kafka_import.yaml")
        sys.exit(1)

    # Resolve diagnostic path
    diagnostic_path = resolve_diagnostic_path(args.diagnostic_path, args.quiet)

    if not diagnostic_path:
        print(f"❌ Error: Path not found: {args.diagnostic_path}")
        sys.exit(1)

    if not diagnostic_path.exists():
        print(f"❌ Error: Resolved path not found: {diagnostic_path}")
        sys.exit(1)

    # Setup settings
    settings = {
        'company_name': args.company,
        'show_qry': args.full_config.get('show_qry', False),
        'row_limit': args.full_config.get('row_limit', 100),
    }

    # Add AI settings if present
    if args.full_config.get('ai_analyze'):
        settings['ai_analyze'] = True
        settings['ai_provider'] = args.full_config.get('ai_provider')
        settings['ai_endpoint'] = args.full_config.get('ai_endpoint')
        settings['ai_model'] = args.full_config.get('ai_model')
        settings['ai_api_key'] = args.full_config.get('ai_api_key')

    # Add NVD API key if present
    if args.full_config.get('nvd_api_key'):
        settings['nvd_api_key'] = args.full_config.get('nvd_api_key')

    # Create connector and load data
    if not args.quiet:
        print(f"Loading Kafka Instacollector data from: {diagnostic_path}")

    try:
        connector = KafkaInstacollectorConnector(settings, str(diagnostic_path))
        connector.connect()
    except Exception as e:
        print(f"❌ Error loading Kafka Instacollector data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Get plugin and report definition
    plugin = KafkaPlugin()

    if args.report_config:
        report_sections = plugin.get_report_definition(args.report_config)
    else:
        # Use instacollector-specific report
        instacollector_report = Path(__file__).parent / "reports" / "instacollector.py"
        report_sections = plugin.get_report_definition(str(instacollector_report))

    # Build report
    if not args.quiet:
        print("Generating health check report...")

    try:
        builder = ReportBuilder(connector, settings, plugin, report_sections, '2.1.0')
        content, findings = builder.build()
    except Exception as e:
        print(f"❌ Error building report: {e}")
        import traceback
        traceback.print_exc()
        connector.disconnect()
        sys.exit(1)

    # Determine output directory
    sanitized_company_name = re.sub(r'\W+', '_', args.company.lower()).strip('_')
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = project_root / 'adoc_out' / sanitized_company_name

    output_dir.mkdir(parents=True, exist_ok=True)

    # Write outputs
    if not args.json_only:
        adoc_path = output_dir / 'health_check.adoc'
        with open(adoc_path, 'w') as f:
            f.write(content)
        if not args.quiet:
            print(f"✅ AsciiDoc report: {adoc_path}")

    json_path = output_dir / 'structured_health_check_findings.json'
    with open(json_path, 'w') as f:
        json.dump(findings, f, indent=2, cls=UniversalJSONEncoder)
    if not args.quiet:
        print(f"✅ Structured findings: {json_path}")

    # Show summary
    if not args.quiet:
        _print_summary(findings, connector)

    # Ship to trends if requested
    if args.ship_trends:
        _ship_to_trends(args, settings, connector, findings, content, plugin)

    connector.disconnect()

    if not args.quiet:
        print("\n✅ Import complete!")


def _print_summary(findings, connector):
    """Print summary of findings."""
    print("\n" + "=" * 60)
    print("REPORT SUMMARY")
    print("=" * 60)

    metadata = connector.get_db_metadata()
    print(f"Cluster ID: {metadata.get('db_name', 'Unknown')}")
    print(f"Kafka Version: {metadata.get('version', 'Unknown')}")
    print(f"Mode: {metadata.get('environment_details', {}).get('kafka_mode', 'Unknown').upper()}")
    print(f"Brokers: {metadata.get('environment_details', {}).get('node_count', 'Unknown')}")
    print(f"Checks executed: {len(findings)}")

    # Evaluate rules
    _evaluate_rules(findings)


def _evaluate_rules(findings):
    """Evaluate rules and show triggered ones."""
    rules_dir = project_root / 'plugins' / 'kafka' / 'rules'

    if not rules_dir.exists():
        return

    all_rules = {}
    for rule_file in rules_dir.glob('*.json'):
        try:
            with open(rule_file) as f:
                all_rules[rule_file.name] = json.load(f)
        except Exception:
            continue

    triggered = []

    for check_name, check_data in findings.items():
        if not isinstance(check_data, dict) or 'data' not in check_data:
            continue

        data = check_data.get('data', {})
        if not isinstance(data, dict):
            continue

        for rule_file, rule_sets in all_rules.items():
            for rule_name, rule_config in rule_sets.items():
                if not isinstance(rule_config, dict):
                    continue

                for rule in rule_config.get('rules', []):
                    expression = rule.get('expression', '')
                    try:
                        result = eval(expression, {
                            'data': data, 'len': len, 'set': set,
                            'any': any, 'all': all, 'True': True, 'False': False
                        })
                        if result:
                            triggered.append({
                                'level': rule.get('level', 'info'),
                                'score': rule.get('score', 0),
                                'reasoning': rule.get('reasoning', '')
                            })
                    except:
                        pass

    if triggered:
        level_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        triggered.sort(key=lambda x: (level_order.get(x['level'], 5), -x['score']))

        print(f"\n⚡ Rules Triggered: {len(triggered)}")

        levels = {}
        for t in triggered:
            levels[t['level']] = levels.get(t['level'], 0) + 1

        icons = {'critical': '🔴', 'high': '🟠', 'medium': '🟡', 'low': '🔵', 'info': '⚪'}
        for level in ['critical', 'high', 'medium', 'low', 'info']:
            if level in levels:
                print(f"   {icons.get(level, '•')} {level.upper()}: {levels[level]}")


def _ship_to_trends(args, settings, connector, findings, adoc_content, plugin):
    """Ship results to trends database."""
    try:
        from output_handlers import trend_shipper
        from utils.json_utils import safe_json_dumps
        from utils.dynamic_prompt_generator import generate_dynamic_prompt

        trends_config_path = Path(args.trends_config)
        if not trends_config_path.exists():
            print(f"⚠️  Trends config not found: {trends_config_path}")
            return

        with open(trends_config_path) as f:
            trends_config = yaml.safe_load(f)

        destination = trends_config.get('destination', 'postgresql')

        db_metadata = connector.get_db_metadata()

        target_info = {
            'company_name': args.company,
            'db_type': 'kafka',
            'host': db_metadata.get('environment_details', {}).get('source_path', 'imported'),
            'port': 0,
            'database': db_metadata.get('db_name', 'unknown'),
        }

        # Add execution context
        import socket
        import getpass
        findings['execution_context'] = {
            'run_by_user': getpass.getuser(),
            'run_from_host': socket.gethostname(),
            'tool_version': '2.1.0',
            'import_source': str(args.diagnostic_path),
            'collection_date': db_metadata.get('environment_details', {}).get('collection_date'),
        }

        findings['db_metadata'] = {
            'version': db_metadata.get('version'),
            'cluster_id': db_metadata.get('db_name'),
            'nodes': db_metadata.get('environment_details', {}).get('node_count', 1),
        }

        # Evaluate rules
        analysis_rules = plugin.get_rules_config()
        analysis_results = None
        if analysis_rules:
            try:
                analysis_results = generate_dynamic_prompt(
                    findings, settings, analysis_rules, db_metadata, plugin, verbose=False
                )
                triggered_count = analysis_results.get('total_issues', 0)
                if triggered_count > 0:
                    print(f"   Evaluated {triggered_count} triggered rules for storage")
            except Exception as e:
                print(f"⚠️  Rule evaluation failed: {e}")

        findings_json = safe_json_dumps(findings)

        print("\nShipping to trends database...")

        if destination == 'postgresql':
            db_config = {
                'host': trends_config['database']['host'],
                'port': trends_config['database']['port'],
                'dbname': trends_config['database']['dbname'],
                'user': trends_config['database']['user'],
                'password': trends_config['database']['password'],
                'sslmode': trends_config['database'].get('sslmode', 'prefer'),
            }

            trend_shipper.ship_to_database(
                db_config=db_config,
                target_info=target_info,
                findings_json=findings_json,
                structured_findings=findings,
                adoc_content=adoc_content,
                analysis_results=analysis_results
            )
        elif destination == 'api':
            api_config = trends_config.get('api', {})
            trend_shipper.ship_to_api(
                api_config=api_config,
                target_info=target_info,
                findings=findings,
                adoc_content=adoc_content,
                analysis_results=analysis_results
            )
        else:
            print(f"⚠️  Unknown destination: {destination}")
            return

        print("✅ Results shipped to trends database")

    except ImportError as e:
        print(f"⚠️  Could not import trend_shipper: {e}")
    except Exception as e:
        import traceback
        print(f"⚠️  Failed to ship to trends: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    main()

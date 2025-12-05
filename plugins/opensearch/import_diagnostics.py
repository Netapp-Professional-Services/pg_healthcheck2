#!/usr/bin/env python3
"""
OpenSearch Diagnostic Import Tool

Generates health check reports from Elastic/OpenSearch support-diagnostics exports.
This allows offline analysis of cluster health without live access.

Usage:
    python -m plugins.opensearch.import_diagnostics /path/to/diagnostic-export [options]

    # From zip file
    python -m plugins.opensearch.import_diagnostics ./api-diagnostics-20251120-145536.zip

    # From extracted directory
    python -m plugins.opensearch.import_diagnostics ./api-diagnostics-20251120-145536/

    # With custom output directory
    python -m plugins.opensearch.import_diagnostics ./export.zip --output-dir ./reports

    # With company name for trends tracking
    python -m plugins.opensearch.import_diagnostics ./export.zip --company "Acme Corp"

    # Ship to trends database
    python -m plugins.opensearch.import_diagnostics ./export.zip --ship-trends

The support-diagnostics tool can be obtained from:
    https://github.com/elastic/support-diagnostics
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from datetime import datetime

# Ensure project root is in path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from plugins.opensearch.diagnostic_connector import DiagnosticConnector
from plugins.opensearch import OpenSearchPlugin
from utils.report_builder import ReportBuilder
from utils.json_utils import UniversalJSONEncoder


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate health check report from OpenSearch/Elasticsearch support-diagnostics export',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        'diagnostic_path',
        help='Path to diagnostic export (zip file or extracted directory)'
    )

    parser.add_argument(
        '--output-dir', '-o',
        help='Output directory for report (default: adoc_out/<company_name>)'
    )

    parser.add_argument(
        '--company', '-c',
        default='opensearch_import',
        help='Company name for report identification (default: opensearch_import)'
    )

    parser.add_argument(
        '--report-config', '-r',
        help='Custom report configuration file (default: uses plugin default)'
    )

    parser.add_argument(
        '--ship-trends',
        action='store_true',
        help='Ship results to trends database (requires trends config)'
    )

    parser.add_argument(
        '--trends-config',
        default='config/trends.yaml',
        help='Path to trends configuration file'
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


def main():
    """Main entry point."""
    args = parse_args()

    # Validate diagnostic path
    diagnostic_path = Path(args.diagnostic_path)
    if not diagnostic_path.exists():
        print(f"❌ Error: Path not found: {diagnostic_path}")
        sys.exit(1)

    # Setup settings
    settings = {
        'company_name': args.company,
        'show_qry': False,
        'row_limit': 100,
    }

    # Create connector and load diagnostic data
    if not args.quiet:
        print(f"Loading diagnostic data from: {diagnostic_path}")

    try:
        connector = DiagnosticConnector(settings, str(diagnostic_path))
        connector.connect()
    except Exception as e:
        print(f"❌ Error loading diagnostic data: {e}")
        sys.exit(1)

    # Get plugin and report definition
    plugin = OpenSearchPlugin()

    if args.report_config:
        report_sections = plugin.get_report_definition(args.report_config)
    else:
        report_sections = plugin.get_report_definition(None)

    # Build report
    if not args.quiet:
        print("Generating health check report...")

    try:
        builder = ReportBuilder(connector, settings, plugin, report_sections, '2.1.0')
        content, findings = builder.build()
    except Exception as e:
        print(f"❌ Error building report: {e}")
        connector.disconnect()
        sys.exit(1)

    # Determine output directory (sanitize company name like main.py does)
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
    """Print summary of findings and sales signals."""
    print("\n" + "=" * 60)
    print("REPORT SUMMARY")
    print("=" * 60)

    # Cluster info
    metadata = connector.get_db_metadata()
    print(f"Cluster: {metadata.get('db_name', 'Unknown')}")
    print(f"Version: {metadata.get('version', 'Unknown')}")
    print(f"Checks executed: {len(findings)}")

    # Count sales signals
    sales_signals = []
    for check_name, check_data in findings.items():
        if isinstance(check_data, dict) and 'data' in check_data:
            data = check_data['data']
            for key, value in data.items():
                if value is True and any(word in key.lower() for word in
                    ['opportunity', 'gap', 'upsell', 'consulting', 'unused', 'insufficient']):
                    sales_signals.append((check_name, key))

    if sales_signals:
        print(f"\n🔥 Sales/Consulting Signals: {len(sales_signals)}")
        for check, signal in sales_signals:
            print(f"   • {signal} ({check})")

    # Evaluate rules
    _evaluate_rules(findings)


def _evaluate_rules(findings):
    """Evaluate rules and show triggered ones."""
    rules_dir = project_root / 'plugins' / 'opensearch' / 'rules'

    if not rules_dir.exists():
        return

    all_rules = {}
    for rule_file in rules_dir.glob('*.json'):
        with open(rule_file) as f:
            all_rules[rule_file.name] = json.load(f)

    triggered = []

    for check_name, check_data in findings.items():
        if not isinstance(check_data, dict) or 'data' not in check_data:
            continue

        data = check_data.get('data', {})

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
        # Sort by severity
        level_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        triggered.sort(key=lambda x: (level_order.get(x['level'], 5), -x['score']))

        print(f"\n⚡ Rules Triggered: {len(triggered)}")

        # Count by level
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
        import yaml
        from output_handlers import trend_shipper
        from utils.json_utils import safe_json_dumps
        from utils.dynamic_prompt_generator import generate_dynamic_prompt

        # Load trends config
        trends_config_path = Path(args.trends_config)
        if not trends_config_path.exists():
            print(f"⚠️  Trends config not found: {trends_config_path}")
            return

        with open(trends_config_path) as f:
            trends_config = yaml.safe_load(f)

        destination = trends_config.get('destination', 'postgresql')

        # Get metadata from connector
        db_metadata = connector.get_db_metadata()

        # Build target_info for the shipper
        target_info = {
            'company_name': args.company,
            'db_type': 'opensearch',
            'host': db_metadata.get('environment_details', {}).get('source_path', 'imported'),
            'port': 0,
            'database': db_metadata.get('db_name', 'unknown'),
        }

        # Add execution context to findings
        import socket
        import getpass
        findings['execution_context'] = {
            'run_by_user': getpass.getuser(),
            'run_from_host': socket.gethostname(),
            'tool_version': '2.1.0',
            'import_source': str(args.diagnostic_path),
            'collection_date': db_metadata.get('environment_details', {}).get('collection_date'),
        }

        # Add db_metadata for trend_shipper extraction
        findings['db_metadata'] = {
            'version': db_metadata.get('version'),
            'cluster_name': db_metadata.get('db_name'),
            'nodes': len(connector.cluster_nodes) if connector.cluster_nodes else 1,
        }

        # Evaluate rules using generate_dynamic_prompt to get properly structured analysis_results
        # This is needed to store triggered rules in health_check_triggered_rules table
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
                print(f"⚠️  Rule evaluation failed (rules will not be stored): {e}")

        # Serialize findings to JSON string (after adding metadata)
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

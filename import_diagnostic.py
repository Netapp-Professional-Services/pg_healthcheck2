#!/usr/bin/env python3
"""
Import and analyze Elastic/OpenSearch support-diagnostics exports.

This script imports diagnostic exports from the Elastic support-diagnostics tool
and runs them through our health check system, including:
- All existing OpenSearch health checks
- Rules engine for severity scoring
- AI analysis (if configured)
- Trend shipping for historical tracking

Usage:
    python import_diagnostic.py --diagnostic /path/to/api-diagnostics-20251120-145536.zip --config config/opensearch.yaml

    # Or with an extracted directory
    python import_diagnostic.py --diagnostic /path/to/api-diagnostics-20251120-145536/ --config config/opensearch.yaml

The config file should specify:
    - company_name: Client name for trends tracking
    - ai_analyze: true/false for AI recommendations
    - trend_storage_enabled: true/false for shipping to trends database
"""

import yaml
import sys
import importlib
import json
import re
import logging
import argparse
import getpass
import socket
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.json_utils import UniversalJSONEncoder
from utils.dynamic_prompt_generator import generate_dynamic_prompt
from utils.run_recommendation import run_recommendation
from utils.report_builder import ReportBuilder
from output_handlers import trend_shipper
from plugins.opensearch.diagnostic_connector import DiagnosticConnector

try:
    APP_VERSION = (Path(__file__).parent / "VERSION").read_text().strip()
except FileNotFoundError:
    APP_VERSION = "unknown"


class DiagnosticImporter:
    """Imports and analyzes diagnostic exports using the health check system."""

    def __init__(self, config_file: str, diagnostic_path: str, report_config_file: str = None):
        """
        Initialize the diagnostic importer.

        Args:
            config_file: Path to configuration YAML file
            diagnostic_path: Path to diagnostic zip or directory
            report_config_file: Optional custom report configuration
        """
        self.settings = self.load_settings(config_file)
        self.diagnostic_path = diagnostic_path
        self.app_version = APP_VERSION

        # Force db_type to opensearch for plugin loading
        self.settings['db_type'] = 'opensearch'

        # Load the OpenSearch plugin
        self.active_plugin = self._load_opensearch_plugin()
        if not self.active_plugin:
            raise ValueError("Failed to load OpenSearch plugin")

        # Get report definition
        self.report_sections = self.active_plugin.get_report_definition(report_config_file)

        # Create diagnostic connector (instead of live connector)
        self.connector = DiagnosticConnector(self.settings, diagnostic_path)

        # Set up output paths
        self.paths = self.get_paths()
        self.adoc_content = ""
        self.all_structured_findings = {}
        self.analysis_output = {}

    def _load_opensearch_plugin(self):
        """Load the OpenSearch plugin."""
        try:
            from plugins.opensearch import OpenSearchPlugin
            return OpenSearchPlugin()
        except ImportError as e:
            print(f"❌ Failed to load OpenSearch plugin: {e}")
            return None

    def load_settings(self, config_file: str) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r') as f:
                settings = yaml.safe_load(f)

            # Ensure required settings have defaults
            settings.setdefault('company_name', 'Diagnostic Import')
            settings.setdefault('ai_analyze', False)
            settings.setdefault('generate_report', True)

            return settings
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"Error loading settings from {config_file}: {e}")
            sys.exit(1)

    def get_paths(self) -> dict:
        """Generate output paths for report artifacts."""
        workdir = Path.cwd()
        sanitized_company_name = re.sub(r'\W+', '_', self.settings['company_name'].lower()).strip('_')
        return {'adoc_out': workdir / 'adoc_out' / sanitized_company_name}

    def run_import(self):
        """
        Run the diagnostic import and analysis.

        This loads the diagnostic data, runs health checks against it,
        generates reports, and optionally ships to trends.
        """
        print(f"\n{'=' * 60}")
        print(f"Diagnostic Import Tool v{APP_VERSION}")
        print(f"{'=' * 60}")
        print(f"\nImporting: {self.diagnostic_path}")
        print(f"Company: {self.settings.get('company_name')}")

        # Connect (load diagnostic data)
        try:
            self.connector.connect()
        except ConnectionError as e:
            print(f"\n❌ Failed to load diagnostic data: {e}")
            sys.exit(1)

        # Get collection timestamp from diagnostic metadata
        metadata = self.connector.get_db_metadata()
        collection_date = metadata.get('environment_details', {}).get('collection_date', 'Unknown')
        print(f"\nDiagnostic collected: {collection_date}")

        # Run the report builder (executes all health checks)
        print(f"\n--- Running Health Checks Against Imported Data ---")
        builder = ReportBuilder(
            self.connector,
            self.settings,
            self.active_plugin,
            self.report_sections,
            self.app_version
        )
        self.adoc_content, self.all_structured_findings = builder.build()

        # Add import-specific metadata
        self.all_structured_findings['import_metadata'] = {
            'source': 'support-diagnostics-import',
            'diagnostic_path': str(self.diagnostic_path),
            'collection_date': collection_date,
            'import_date': datetime.now().astimezone().isoformat(),
            'diagnostic_version': metadata.get('environment_details', {}).get('diagnostic_version'),
        }

        # Run AI analysis if configured
        ai_execution_metrics = {}
        if self.settings.get('ai_analyze', False):
            ai_execution_metrics = self.run_ai_analysis()

        # Generate and embed metadata
        self.generate_and_embed_metadata(ai_execution_metrics)

        # Ship to trends if configured
        if self.settings.get('trend_storage_enabled', False):
            try:
                print("\n--- Shipping to Trends Database ---")
                trend_shipper.run(
                    self.all_structured_findings,
                    self.settings,
                    self.adoc_content,
                    analysis_results=self.analysis_output
                )
            except Exception as e:
                print(f"⚠️  Warning: Trend shipper failed: {e}")
        else:
            print("\n--- Trend shipping disabled (trend_storage_enabled: false) ---")

        # Save structured findings
        self.save_structured_findings()

        # Cleanup
        self.connector.disconnect()

        print(f"\n{'=' * 60}")
        print("Import Complete!")
        print(f"{'=' * 60}")

    def generate_and_embed_metadata(self, ai_execution_metrics: dict = None):
        """Generate summarized findings and embed metadata."""
        if ai_execution_metrics is None:
            ai_execution_metrics = {}

        if not self.analysis_output:
            print("\n--- Generating Summarized Findings ---")
            analysis_rules = self.active_plugin.get_rules_config()
            db_metadata = self.connector.get_db_metadata()
            self.analysis_output = generate_dynamic_prompt(
                self.all_structured_findings,
                self.settings,
                analysis_rules,
                db_metadata,
                self.active_plugin
            )

        # Add db_metadata
        if hasattr(self.connector, 'get_db_metadata'):
            db_metadata = self.connector.get_db_metadata()
            if db_metadata:
                self.all_structured_findings['db_metadata'] = db_metadata

        self.all_structured_findings['summarized_findings'] = self.analysis_output.get('summarized_findings', {})
        self.all_structured_findings['prompt_template_name'] = self.settings.get('prompt_template', 'default_prompt.j2')
        self.all_structured_findings['execution_context'] = {
            'tool_version': self.app_version,
            'run_by_user': getpass.getuser(),
            'run_from_host': socket.gethostname(),
            'import_mode': True,
            'ai_execution_metrics': ai_execution_metrics
        }

    def run_ai_analysis(self) -> dict:
        """Generate AI analysis and return execution metrics."""
        if not self.analysis_output:
            self.generate_and_embed_metadata()

        print("\n--- Sending Prompt to AI for Analysis ---")
        full_prompt = self.analysis_output.get('prompt')

        ai_metrics = {}
        if full_prompt:
            print(f"[DEBUG] Prompt length: {len(full_prompt)} characters")
            ai_adoc, ai_metrics = run_recommendation(self.settings, full_prompt)
            self.adoc_content += f"\n\n{ai_adoc}"
        else:
            print("⚠️  Warning: Prompt generation failed; skipping AI analysis.")
        return ai_metrics

    def save_structured_findings(self):
        """Save structured findings to JSON file."""
        output_path = self.paths['adoc_out'] / "structured_health_check_findings.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.all_structured_findings, f, indent=2, cls=UniversalJSONEncoder)
        print(f"\n✅ Structured findings saved to: {output_path}")

    def write_adoc(self, output_file: str):
        """Write AsciiDoc report to file."""
        output_path = self.paths['adoc_out'] / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(self.adoc_content)
        print(f"✅ Report saved to: {output_path}")


def main():
    """Parse arguments and run the diagnostic import."""
    parser = argparse.ArgumentParser(
        description='Import and analyze Elastic/OpenSearch support-diagnostics exports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Import a zip file
    python import_diagnostic.py --diagnostic ./api-diagnostics-20251120-145536.zip --config config/opensearch_import.yaml

    # Import an extracted directory
    python import_diagnostic.py --diagnostic ./api-diagnostics-20251120-145536/ --config config/opensearch_import.yaml

    # Import with custom output name
    python import_diagnostic.py --diagnostic ./diagnostic.zip --config config/opensearch.yaml --output client_report.adoc
        """
    )
    parser.add_argument(
        '--diagnostic', '-d',
        required=True,
        help='Path to diagnostic zip file or extracted directory'
    )
    parser.add_argument(
        '--config', '-c',
        required=True,
        help='Path to configuration YAML file'
    )
    parser.add_argument(
        '--report-config',
        help='Path to custom report configuration file'
    )
    parser.add_argument(
        '--output', '-o',
        default='health_check.adoc',
        help='Output report filename (default: health_check.adoc)'
    )
    parser.add_argument(
        '--no-report',
        action='store_true',
        help='Skip generating AsciiDoc report'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Configure logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Validate diagnostic path
    diagnostic_path = Path(args.diagnostic)
    if not diagnostic_path.exists():
        print(f"❌ Diagnostic path does not exist: {args.diagnostic}")
        sys.exit(1)

    # Run import
    importer = DiagnosticImporter(args.config, args.diagnostic, args.report_config)
    importer.run_import()

    # Generate report
    if not args.no_report and importer.settings.get('generate_report', True):
        importer.write_adoc(args.output)
    else:
        print("\n📋 AsciiDoc report generation skipped")


if __name__ == '__main__':
    main()

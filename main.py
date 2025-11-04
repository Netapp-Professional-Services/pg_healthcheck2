#!/usr/bin/env python3
"""
Main entrypoint for the Database Health Check Tool.

This script initializes the application, discovers plugins, parses command-line
arguments, and orchestrates the execution of health checks, report generation,
and AI analysis based on the provided configuration.
"""

import yaml
import sys
import importlib
from pathlib import Path
from datetime import datetime, timedelta
import json
from decimal import Decimal
import re
import logging
import argparse
import pkgutil
import socket
import getpass
from utils.json_utils import UniversalJSONEncoder
from utils.dynamic_prompt_generator import generate_dynamic_prompt
from utils.run_recommendation import run_recommendation
from utils.report_builder import ReportBuilder
from plugins.base import BasePlugin
from output_handlers import trend_shipper

try:
    APP_VERSION = (Path(__file__).parent / "VERSION").read_text().strip()
except FileNotFoundError:
    APP_VERSION = "unknown"

def discover_plugins():
    """Finds and loads all available plugins from the 'plugins' directory.

    This function iterates through the subdirectories of the 'plugins' folder,
    imports them as modules, and looks for classes that inherit from BasePlugin.
    It instantiates each found plugin and returns a dictionary mapping the
    plugin's technology name to its instance.

    Returns:
        dict: A dictionary of loaded plugin instances, keyed by technology name.
    """
    plugins_path = Path(__file__).parent / "plugins"
    discovered_plugins = {}
    for _, name, _ in pkgutil.iter_modules([str(plugins_path)]):
        if name != "base":
            try:
                module = importlib.import_module(f'plugins.{name}')
                for item_name in dir(module):
                    item = getattr(module, item_name)
                    if isinstance(item, type) and issubclass(item, BasePlugin) and item is not BasePlugin:
                        try:
                            plugin_instance = item()
                            discovered_plugins[plugin_instance.technology_name] = plugin_instance
                            print(f"✅ Discovered and loaded plugin: {plugin_instance.technology_name}")
                        except Exception as e:
                            print(f"⚠️  Warning: Could not instantiate plugin '{name}'. Error: {e}. Skipping.")
            except ImportError as e:
                print(f"⚠️  Warning: Could not import plugin '{name}'. Missing dependency: {e}. Skipping.")
            except Exception as e:
                print(f"⚠️  Warning: Failed to load plugin '{name}' due to an unexpected error: {e}. Skipping.")
    return discovered_plugins

class HealthCheck:
    """Orchestrates the entire health check process from start to finish."""
    def __init__(self, config_file, report_config_file=None):
        """Initializes the HealthCheck application.

        Args:
            config_file (str): Path to the main `config.yaml` file.
            report_config_file (str, optional): Path to a custom report
                configuration file. If not provided, the default for the
                selected plugin will be used.
        """
        self.settings = self.load_settings(config_file)
        self.app_version = APP_VERSION
        self.available_plugins = discover_plugins()
        active_tech = self.settings.get('db_type')
        self.active_plugin = self.available_plugins.get(active_tech)

        if not self.active_plugin:
            raise ValueError(f"Unsupported or missing db_type: '{active_tech}'. Available plugins: {list(self.available_plugins.keys())}")

        self.report_sections = self.active_plugin.get_report_definition(report_config_file)
        self.connector = self.active_plugin.get_connector(self.settings)
        self.paths = self.get_paths()
        self.adoc_content = ""
        self.all_structured_findings = {}
        self.analysis_output = {}

    def load_settings(self, config_file):
        """Loads the main YAML configuration file."""
        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"Error loading settings from {config_file}: {e}")
            sys.exit(1)

    def get_paths(self):
        """Generates the output paths for report artifacts."""
        workdir = Path.cwd()
        sanitized_company_name = re.sub(r'\W+', '_', self.settings['company_name'].lower()).strip('_')
        return { 'adoc_out': workdir / 'adoc_out' / sanitized_company_name }

    def run_report(self):
        """Orchestrates the main health check process.

        This method connects to the database, runs the report builder to
        collect data, optionally triggers AI analysis, embeds metadata, ships
        the data to a trend analysis platform, and saves the final output.
        """
        self.connector.connect()

        builder = ReportBuilder(self.connector, self.settings, self.active_plugin, self.report_sections, self.app_version)
        self.adoc_content, self.all_structured_findings = builder.build()
        
        ai_execution_metrics = {}
        if self.settings.get('ai_analyze', False):
            ai_execution_metrics = self.run_ai_analysis()

        # Always generate and embed metadata before shipping and saving.
        self.generate_and_embed_metadata(ai_execution_metrics)

        try:
            print("\n--- Handing off findings to Trend Shipper ---")
            # Pass analysis_output to store triggered rules for trend analysis
            trend_shipper.run(
                self.all_structured_findings, 
                self.settings, 
                self.adoc_content,
                analysis_results=self.analysis_output  # NEW: Pass analysis results for rule tracking
            )
        except Exception as e:
            print(f"CRITICAL: The trend shipper module failed with an unexpected error: {e}")

        self.save_structured_findings()
        self.connector.disconnect()

    def generate_and_embed_metadata(self, ai_execution_metrics={}):
        """Generates summarized findings and embeds all metadata into the findings object.""" 
        if not self.analysis_output:
            print("\n--- Generating Summarized Findings for Historical Record ---")
            analysis_rules = self.active_plugin.get_rules_config()
            db_metadata = self.connector.get_db_metadata()
            self.analysis_output = generate_dynamic_prompt(self.all_structured_findings, self.settings, analysis_rules, db_metadata, self.active_plugin)
        
        self.all_structured_findings['summarized_findings'] = self.analysis_output.get('summarized_findings', {})
        self.all_structured_findings['prompt_template_name'] = self.settings.get('prompt_template', 'default_prompt.j2')
        self.all_structured_findings['execution_context'] = {
            'tool_version': self.app_version,
            'run_by_user': getpass.getuser(),
            'run_from_host': socket.gethostname(),
            'ai_execution_metrics': ai_execution_metrics
        }

    def run_ai_analysis(self):
        """Generates a prompt, sends it to the AI, and returns execution metrics.

        Returns:
            dict: A dictionary containing metrics about the AI query execution,
                  such as token count and execution time.
        """
        if not self.analysis_output:
             # This will populate self.analysis_output
             self.generate_and_embed_metadata()
        
        print("\n--- Sending Prompt to AI for Analysis ---")
        full_prompt = self.analysis_output.get('prompt')

        # === ADD THIS DEBUGGING LINE ===
        print(f"[DEBUG] Prompt length being passed to run_recommendation: {len(full_prompt)} characters.")
        # ===============================

        ai_metrics = {}
        if full_prompt:
            ai_adoc, ai_metrics = run_recommendation(self.settings, full_prompt)
            self.adoc_content += f"\n\n{ai_adoc}"
        else:
            print("Warning: Prompt generation failed; skipping AI analysis.")
        return ai_metrics

    def save_structured_findings(self):
        """Saves the final structured findings object to a JSON file."""
        output_path = self.paths['adoc_out'] / "structured_health_check_findings.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.all_structured_findings, f, indent=2, cls=UniversalJSONEncoder)
        print(f"\nStructured health check findings saved to: {output_path}")

    def write_adoc(self, output_file):
        """Writes the generated AsciiDoc content to the final report file."""
        output_path = self.paths['adoc_out'] / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(self.adoc_content)

def main():
    """Parses command line arguments and runs the health check."""
    parser = argparse.ArgumentParser(description='Database Health Check Tool')
    parser.add_argument('--config', default='config/config.yaml', help='Path to configuration file')
    parser.add_argument('--report-config', help='Path to a custom report configuration file.')
    parser.add_argument('--output', default='health_check.adoc', help='Output file name')
    args = parser.parse_args()
    
    print(f"--- Running Health Check Tool v{APP_VERSION} ---")
    health_check = HealthCheck(args.config, args.report_config)
    
    settings = health_check.load_settings(args.config)
    generate_report_flag = settings.get('generate_report', True)
    
    health_check.run_report()

    if generate_report_flag:
        health_check.write_adoc(args.output)
        print(f"\nHealth check completed successfully!")
        print(f"Report generated: {health_check.paths['adoc_out'] / args.output}")
    else:
        print("\nHealth check data collection completed. AsciiDoc report generation was skipped as per 'generate_report: false' in config.")

if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).parent))
    main()

"""Offline AI Analysis Processor.

This script provides a command-line interface to re-run the AI analysis
portion of a health check using a pre-existing findings file. It is useful
for testing different prompt templates, using different AI models, or
debugging the analysis process without needing to re-connect to the database
and re-run the entire health check.
"""

import json
import argparse
from pathlib import Path
import sys
import yaml
import pkgutil
import importlib

# Add the project root to the path to allow for correct module imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.dynamic_prompt_generator import generate_dynamic_prompt
from utils.run_recommendation import run_recommendation
from plugins.base import BasePlugin

def discover_plugins():
    """Finds and loads all available plugins from the 'plugins' directory.

    This function iterates through the subdirectories of the 'plugins' folder,
    imports them as modules, and looks for classes that inherit from BasePlugin.
    It instantiates each found plugin and returns a dictionary mapping the
    plugin's technology name to its instance.

    Returns:
        dict: A dictionary of loaded plugin instances, keyed by technology name.
    """

    plugins_path = Path(__file__).parent.parent / "plugins"
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

def main():
    """Executes the offline AI analysis process.

    This main function orchestrates the entire offline workflow:
    1.  Parses command-line arguments for config, findings, and output files.
    2.  Loads the settings and the pre-existing structured findings JSON.
    3.  Discovers and loads the appropriate plugin based on the `db_type`.
    4.  Generates a dynamic prompt using the findings and rules.
    5.  Calls the AI recommendation engine with the generated prompt.
    6.  Saves the resulting AsciiDoc report to the specified output file.
    """

    parser = argparse.ArgumentParser(description='Offline AI Analysis Processor')
    parser.add_argument('--config', required=True, help='Path to the configuration file (e.g., config/config.yaml)')
    parser.add_argument('--findings', required=True, help='Path to the structured_health_check_findings.json file')
    parser.add_argument('--template', help='Path to a specific Jinja2 prompt template to use (overrides config)')
    parser.add_argument('--output', default='ai_recommendations.adoc', help='Output file for the AI-generated report')
    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            settings = yaml.safe_load(f)
        with open(args.findings, 'r') as f:
            all_structured_findings = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        sys.exit(1)

    if args.template:
        settings['prompt_template'] = Path(args.template).name

    available_plugins = discover_plugins()
    active_tech = settings.get('db_type')
    active_plugin = available_plugins.get(active_tech)

    if not active_plugin:
        raise ValueError(f"Unsupported or missing db_type: '{active_tech}'.")

    print(f"\n--- Starting AI Analysis for '{active_tech}' using offline data ---")

    analysis_rules = active_plugin.get_rules_config()

    # --- Agnostic Metadata Extraction ---
    db_version = active_plugin.get_db_version_from_findings(all_structured_findings)
    db_name = active_plugin.get_db_name_from_findings(all_structured_findings)

    # Create metadata dict for offline analysis (no environment detection available)
    db_metadata = {
        'version': db_version,
        'db_name': db_name,
        'environment': 'unknown',
        'environment_details': {}
    }

    settings['ai_run_integrated'] = True

    dynamic_analysis = generate_dynamic_prompt(all_structured_findings, settings, analysis_rules, db_metadata, active_plugin)
    full_prompt = dynamic_analysis['prompt']

    ai_adoc, _ = run_recommendation(settings, full_prompt)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(ai_adoc)

    print(f"\n✅ AI analysis complete.\nReport saved to: {output_path}")

if __name__ == '__main__':
    main()

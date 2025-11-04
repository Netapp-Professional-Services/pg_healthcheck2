#!/usr/bin/env python3
"""
Standalone Rule Testing Utility

This script loads a structured findings file and runs the AI analysis
logic against it to test the application of rule files and preview the final AI prompt.
"""
import json
import argparse
import sys
from pathlib import Path
import pkgutil
import importlib

# Add the project root to the path for correct module imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.dynamic_prompt_generator import generate_dynamic_prompt
from plugins.base import BasePlugin

def discover_plugins():
    """Finds and loads all available plugins."""
    project_root = Path(__file__).parent.parent
    plugins_path = project_root / "plugins"
    
    discovered_plugins = {}
    for _, name, _ in pkgutil.iter_modules([str(plugins_path)]):
        if name != "base":
            try:
                module = importlib.import_module(f'plugins.{name}')
                for item_name in dir(module):
                    item = getattr(module, item_name)
                    if isinstance(item, type) and issubclass(item, BasePlugin) and item is not BasePlugin:
                        plugin_instance = item()
                        discovered_plugins[plugin_instance.technology_name] = plugin_instance
            except Exception as e:
                print(f"⚠️ Warning: Failed to load plugin '{name}': {e}. Skipping.")
    return discovered_plugins

def main():
    parser = argparse.ArgumentParser(description='Health Check Rule and Prompt Tester')
    parser.add_argument('--findings', required=True, help='Path to the structured_health_check_findings.json file')
    parser.add_argument('--db-type', required=True, help='The database technology to test (e.g., "postgres")')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose debugging output for rule evaluation.')
    parser.add_argument('--prompt-template', help='Path to a specific Jinja2 prompt template to use (overrides the default).')
    parser.add_argument('--output-prompt', help='File path to save the final, rendered AI prompt.')
    args = parser.parse_args()

    # --- Load Structured Findings ---
    try:
        with open(args.findings, 'r') as f:
            all_structured_findings = json.load(f)
    except Exception as e:
        print(f"❌ Error loading findings file: {e}")
        sys.exit(1)

    # --- Discover and Activate the Correct Plugin ---
    available_plugins = discover_plugins()
    active_plugin = available_plugins.get(args.db_type)

    if not active_plugin:
        print(f"❌ Error: Could not find or load a plugin for db_type: '{args.db_type}'")
        sys.exit(1)

    print(f"✅ Plugin '{args.db_type}' loaded successfully.")

    # --- Load the Rules and Weights from the Plugin ---
    analysis_rules = active_plugin.get_rules_config()
    module_weights = active_plugin.get_module_weights()

    if not analysis_rules:
        print("❌ Error: No analysis rules were found by the plugin.")
        sys.exit(1)

    print(f"✅ Loaded {len(analysis_rules)} rule configurations.")
    
    # --- NEW: Check for Missing Weights ---
    modules_in_findings = set(all_structured_findings.keys())
    modules_with_weights = set(module_weights.keys())
    missing_weights = modules_in_findings - modules_with_weights
    
    if missing_weights:
        print("\n⚠️ Warning: The following modules have data but are missing a weight definition in the plugin:")
        for module in sorted(list(missing_weights)):
            print(f"  - {module}")
    
    print("\n--- Running Rule Analysis ---")

    # --- Generate Analysis and Statistics ---
    dummy_settings = {'row_limit': 10, 'is_aurora': True}
    if args.prompt_template:
        dummy_settings['prompt_template'] = Path(args.prompt_template).name

    # Extract database metadata for prompt generation
    db_version = all_structured_findings.get("postgres_overview", {}).get("version_info", {}).get("data", [{}])[0].get("version", "N/A")
    db_name = all_structured_findings.get("postgres_overview", {}).get("database_size", {}).get("data", [{}])[0].get("database", "N/A")

    # Create metadata dict for offline analysis (no environment detection available)
    db_metadata = {
        'version': db_version,
        'db_name': db_name,
        'environment': 'unknown',
        'environment_details': {}
    }

    analysis_output = generate_dynamic_prompt(
        all_structured_findings,
        dummy_settings,
        analysis_rules,
        db_metadata,
        active_plugin,
        verbose=args.verbose
    )

    # --- Save the Rendered Prompt if Requested ---
    if args.output_prompt:
        try:
            output_path = Path(args.output_prompt)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(analysis_output['prompt'])
            print(f"\n✅ Final AI prompt saved to: {output_path}")
        except Exception as e:
            print(f"❌ Error saving prompt file: {e}")

    # --- Print the Results ---
    stats = analysis_output.get('rule_application_stats', {})
    triggered_rules = {k: v for k, v in stats.items() if v.get('triggered', 0) > 0}
    error_rules = {k: v for k, v in stats.items() if v.get('errors', 0) > 0}

    print("\n--- ✅ Analysis Complete ---")
    print(f"\n📈 Rule Application Statistics:")
    print(json.dumps(stats, indent=2))

    if triggered_rules:
        print("\n🔥 Triggered Rules:")
        print(json.dumps(triggered_rules, indent=2))

    if error_rules:
        print("\n❌ Rules with Errors:")
        print(json.dumps(error_rules, indent=2))

if __name__ == '__main__':
    main()

import importlib.util
from pathlib import Path
import json

# --- Import the components of this plugin ---
from .connector import PostgresConnector
# No longer importing the static rules config: from .rules.analysis_rules import METRIC_ANALYSIS_CONFIG

# --- Import the base class it must implement ---
from plugins.base import BasePlugin


class PostgresPlugin(BasePlugin):
    """The PostgreSQL implementation of the plugin interface."""

    @property
    def technology_name(self):
        return "postgres"

    def get_connector(self, settings):
        """Returns an instance of the PostgreSQL connector."""
        return PostgresConnector(settings)

    def get_rules_config(self):
        """
        Dynamically discovers and loads all .json rule files
        from the 'rules' directory with validation.
        """
        from utils.rule_validator import validate_and_load_rules
        import logging

        logger = logging.getLogger(__name__)
        all_rules = {}
        # Assumes this script is in plugins/postgres/
        rules_dir = Path(__file__).parent / 'rules'

        if not rules_dir.is_dir():
            logger.warning(f"Rules directory not found at {rules_dir}")
            return {}

        # Iterate over every .json file in the rules directory
        for rule_file in rules_dir.glob('*.json'):
            try:
                with open(rule_file, 'r') as f:
                    loaded_rules = json.load(f)

                # Validate and filter rules
                validated_rules = validate_and_load_rules(loaded_rules, str(rule_file))

                if validated_rules:
                    all_rules.update(validated_rules)

            except json.JSONDecodeError as e:
                logger.error(f"Rule file {rule_file.name} has invalid JSON: {e}")
            except IOError as e:
                logger.error(f"Could not read rule file {rule_file.name}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error loading rule file {rule_file.name}: {e}")

        return all_rules

    def get_template_path(self) -> Path:
        """Returns the path to this plugin's templates directory."""
        return Path(__file__).parent / "templates"

    def get_report_definition(self, report_config_file=None):
        """
        Dynamically loads a report definition from a file.
        Falls back to the default if no file is specified.
        """
        if report_config_file:
            config_path = Path(report_config_file)
        else:
            # Default location for the standard report definition
            config_path = Path(__file__).parent / "reports" / "default.py"

        if not config_path.is_file():
            raise FileNotFoundError(f"Report configuration file not found: {config_path}")

        # Dynamically load the module from the specified path
        spec = importlib.util.spec_from_file_location("report_config_module", config_path)
        report_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(report_module)
        
        return getattr(report_module, 'REPORT_SECTIONS')


    def get_module_weights(self) -> dict:
        """
        Dynamically discovers the importance score for each check module by
        calling the 'get_weight()' function within the module itself.
        """
        weights = {}
        report_sections = self.get_report_definition()

        for section in report_sections:
            # Safely get the module name, skipping sections that are not modules
            module_name = section.get('module')
            if not module_name:
                continue

            try:
                spec = importlib.util.find_spec(f"plugins.postgres.checks.{module_name}")
                if spec:
                    check_module = spec.loader.load_module()
                    if hasattr(check_module, 'get_weight'):
                        weights[module_name] = check_module.get_weight()
            except Exception as e:
                print(f"⚠️ Warning: Could not dynamically load weight for module '{module_name}'. Error: {e}")
        
        return weights


    def get_db_version_from_findings(self, findings: dict) -> str:
        """Extracts the PostgreSQL version from the findings."""
        try:
            # PostgreSQL-specific path to the version info
            return findings.get("postgres_overview", {}).get("version_info", {}).get("data", [{}])[0].get("version", "N/A")
        except (IndexError, AttributeError):
            return "N/A"

    def get_db_name_from_findings(self, findings: dict) -> str:
        """
        Extracts the PostgreSQL database name from the findings.
        This ensures historical accuracy for the offline processor.
        """
        try:
            # PostgreSQL-specific path to the database name
            return findings.get("postgres_overview", {}).get("database_size", {}).get("data", [{}])[0].get("database", "N/A")
        except (IndexError, AttributeError):
            return "N/A"

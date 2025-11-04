import importlib.util
from pathlib import Path
import json
from plugins.base import BasePlugin
from .connector import CassandraConnector

class CassandraPlugin(BasePlugin):
    """The Cassandra implementation of the plugin interface."""

    @property
    def technology_name(self):
        return "cassandra"

    def get_connector(self, settings):
        """Returns an instance of the Cassandra connector."""
        return CassandraConnector(settings)

    def get_rules_config(self):
        """
        Dynamically discovers and loads all .json rule files
        from the 'rules' directory with validation.

        Returns:
            dict: All rules merged into a single dictionary (only valid rules)
        """
        from utils.rule_validator import validate_and_load_rules
        import logging

        logger = logging.getLogger(__name__)
        all_rules = {}
        rules_dir = Path(__file__).parent / 'rules'

        if not rules_dir.is_dir():
            logger.warning(f"Rules directory not found at {rules_dir}")
            return {}

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

        logger.info(f"Loaded {len(all_rules)} rule metric(s) from {rules_dir}")
        return all_rules

    def get_report_definition(self, report_config_file=None):
        """
        Dynamically loads a report definition from a Python file.
        Falls back to reports/default.py if not specified.
        
        Args:
            report_config_file: Optional path to custom report file
            
        Returns:
            list: REPORT_SECTIONS list from the report module
        """
        if report_config_file:
            config_path = Path(report_config_file)
        else:
            config_path = Path(__file__).parent / "reports" / "default.py"

        if not config_path.is_file():
            raise FileNotFoundError(f"Report configuration file not found: {config_path}")

        # Dynamically import the report module
        spec = importlib.util.spec_from_file_location("report_config_module", config_path)
        report_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(report_module)
        
        return getattr(report_module, 'REPORT_SECTIONS')

    def get_template_path(self) -> Path:
        """Returns the path to this plugin's templates directory."""
        return Path(__file__).parent / "templates"

    # Optional: Override these methods for enhanced functionality
    def get_module_weights(self) -> dict:
        """
        Dynamically discovers check weights by importing each check module.
        Override this if you want custom weight calculation logic.
        """
        weights = {}
        try:
            report_sections = self.get_report_definition()
            for section in report_sections:
                module_name = section.get('module')
                if not module_name:
                    continue
                
                try:
                    # Extract just the module name from full path
                    if 'plugins.cassandra.checks.' in module_name:
                        short_name = module_name.split('.')[-1]
                        spec = importlib.util.find_spec(f"plugins.cassandra.checks.{short_name}")
                        if spec:
                            check_module = spec.loader.load_module()
                            if hasattr(check_module, 'get_weight'):
                                weights[short_name] = check_module.get_weight()
                except Exception as e:
                    print(f"⚠️ Could not load weight for '{module_name}': {e}")
        except Exception:
            pass  # Return empty dict if report loading fails
        
        return weights

    def get_db_version_from_findings(self, findings: dict) -> str:
        """
        Extracts the Cassandra version from the findings.
        Looks in check_node_info check results.
        """
        try:
            # Cassandra-specific path to version info
            node_info = findings.get("check_node_info", {}).get("node_info", {})
            if node_info.get("status") == "success":
                data = node_info.get("data", [{}])
                if data and len(data) > 0:
                    return data[0].get("release_version", "N/A")
            return "N/A"
        except (IndexError, AttributeError, KeyError):
            return "N/A"

    def get_db_name_from_findings(self, findings: dict) -> str:
        """
        Extracts the Cassandra keyspace from findings.
        For Cassandra, keyspace serves as the "database name".
        """
        try:
            node_info = findings.get("check_node_info", {}).get("node_info", {})
            if node_info.get("status") == "success":
                data = node_info.get("data", [{}])
                if data and len(data) > 0:
                    keyspace = data[0].get("keyspace")
                    return keyspace if keyspace else "system"
            return "system"
        except (IndexError, AttributeError, KeyError):
            return "system"

from pathlib import Path
import json
from plugins.base import BasePlugin
from .connector import ClickHouseConnector
from .reports.default import get_report_definition as get_clickhouse_report_definition

class ClickHousePlugin(BasePlugin):
    @property
    def technology_name(self):
        return "clickhouse"

    def get_connector(self, settings):
        return ClickHouseConnector(settings)

    def get_report_definition(self, report_config_file=None):
        return get_clickhouse_report_definition()

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

    def get_template_path(self):
        return Path(__file__).parent.parent / "clickhouse" / "templates"

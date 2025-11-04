"""
Runtime validation for health check rule files.

Validates rule JSON structure to ensure they have required fields and proper format.
Logs warnings for issues but does not halt execution.
"""

import logging
from typing import Dict, List, Any, Tuple

logger = logging.getLogger(__name__)


def validate_rule_structure(rule_data: Dict[str, Any], rule_file: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validates the structure of a rule JSON file.

    Supports two formats:

    1. Expression-based format (used by dynamic_prompt_generator.py):
    {
        "config_name": {
            "metric_keywords": ["keyword1", "keyword2"],
            "rules": [
                {
                    "expression": "data.get('value') > 10",
                    "level": "critical",
                    "score": 9,
                    "reasoning": "...",
                    "recommendations": ["..."]
                }
            ]
        }
    }

    2. Simple threshold format:
    {
        "metric_name": {
            "critical": {
                "threshold": 90,
                "reasoning": "...",
                "recommendations": ["..."]
            }
        }
    }

    Args:
        rule_data: Parsed JSON rule data
        rule_file: File path for logging purposes

    Returns:
        tuple: (validated_rules, list_of_warnings)
            - validated_rules: Rules that passed validation (may be empty)
            - list_of_warnings: List of validation warning messages
    """
    warnings = []
    validated_rules = {}

    if not isinstance(rule_data, dict):
        warnings.append(f"Rule file '{rule_file}' root must be a dictionary, got {type(rule_data).__name__}")
        return {}, warnings

    if not rule_data:
        warnings.append(f"Rule file '{rule_file}' is empty")
        return {}, warnings

    for config_name, config_data in rule_data.items():
        if not isinstance(config_data, dict):
            warnings.append(
                f"Rule '{rule_file}' config '{config_name}': Expected dict, got {type(config_data).__name__}"
            )
            continue

        # Detect which format this rule uses
        has_metric_keywords = 'metric_keywords' in config_data
        has_rules_array = 'rules' in config_data

        if has_metric_keywords or has_rules_array:
            # Expression-based format (dynamic prompt generator format)
            validated_config, config_warnings = _validate_expression_format(
                config_name, config_data, rule_file
            )
            warnings.extend(config_warnings)
            if validated_config:
                validated_rules[config_name] = validated_config
        else:
            # Simple threshold format
            validated_config, config_warnings = _validate_threshold_format(
                config_name, config_data, rule_file
            )
            warnings.extend(config_warnings)
            if validated_config:
                validated_rules[config_name] = validated_config

    return validated_rules, warnings


def _validate_expression_format(config_name: str, config_data: Dict[str, Any], rule_file: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validates expression-based rule format used by dynamic_prompt_generator.py.

    Args:
        config_name: Name of the rule configuration
        config_data: Rule configuration data
        rule_file: File path for logging

    Returns:
        tuple: (validated_config, list_of_warnings)
    """
    warnings = []
    field_issues = []

    # Validate metric_keywords
    metric_keywords = config_data.get('metric_keywords')
    if not metric_keywords:
        field_issues.append("'metric_keywords' is required")
    elif not isinstance(metric_keywords, list):
        field_issues.append(f"'metric_keywords' must be list, got {type(metric_keywords).__name__}")
    elif not metric_keywords:
        field_issues.append("'metric_keywords' cannot be empty list")
    else:
        for i, kw in enumerate(metric_keywords):
            if not isinstance(kw, str):
                field_issues.append(f"metric_keywords[{i}] must be string, got {type(kw).__name__}")
                break

    # Validate rules array
    rules = config_data.get('rules')
    if not rules:
        field_issues.append("'rules' array is required")
    elif not isinstance(rules, list):
        field_issues.append(f"'rules' must be list, got {type(rules).__name__}")
    elif not rules:
        field_issues.append("'rules' cannot be empty list")
    else:
        valid_levels = ['critical', 'high', 'medium', 'low', 'info', 'warning']
        for i, rule in enumerate(rules):
            if not isinstance(rule, dict):
                field_issues.append(f"rules[{i}] must be dict, got {type(rule).__name__}")
                continue

            # Check required fields in each rule
            required_rule_fields = ['expression', 'level', 'score', 'reasoning', 'recommendations']
            missing = [f for f in required_rule_fields if f not in rule]
            if missing:
                field_issues.append(f"rules[{i}] missing fields: {', '.join(missing)}")
                continue

            # Validate field types
            if not isinstance(rule['expression'], str):
                field_issues.append(f"rules[{i}].expression must be string")
            elif not rule['expression'].strip():
                field_issues.append(f"rules[{i}].expression cannot be empty")

            if rule['level'] not in valid_levels:
                field_issues.append(f"rules[{i}].level must be one of {valid_levels}, got '{rule['level']}'")

            if not isinstance(rule['score'], (int, float)):
                field_issues.append(f"rules[{i}].score must be number")

            if not isinstance(rule['reasoning'], str):
                field_issues.append(f"rules[{i}].reasoning must be string")
            elif not rule['reasoning'].strip():
                field_issues.append(f"rules[{i}].reasoning cannot be empty")

            if not isinstance(rule['recommendations'], list):
                field_issues.append(f"rules[{i}].recommendations must be list")
            elif not rule['recommendations']:
                field_issues.append(f"rules[{i}].recommendations cannot be empty list")

    if field_issues:
        for issue in field_issues:
            warnings.append(f"Rule '{rule_file}' config '{config_name}': {issue}")
        return {}, warnings

    return config_data, warnings


def _validate_threshold_format(config_name: str, config_data: Dict[str, Any], rule_file: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validates simple threshold-based rule format.

    Args:
        config_name: Name of the rule configuration
        config_data: Rule configuration data
        rule_file: File path for logging

    Returns:
        tuple: (validated_config, list_of_warnings)
    """
    warnings = []
    valid_severities = ['critical', 'high', 'medium', 'low', 'warning']
    required_fields = ['threshold', 'reasoning', 'recommendations']

    validated_severities = {}

    for severity, rule_config in config_data.items():
        if severity not in valid_severities:
            warnings.append(
                f"Rule '{rule_file}' config '{config_name}': Unknown severity '{severity}'. "
                f"Valid severities: {', '.join(valid_severities)}"
            )
            continue

        if not isinstance(rule_config, dict):
            warnings.append(
                f"Rule '{rule_file}' config '{config_name}' severity '{severity}': "
                f"Expected dict, got {type(rule_config).__name__}"
            )
            continue

        # Check required fields
        missing_fields = [field for field in required_fields if field not in rule_config]
        if missing_fields:
            warnings.append(
                f"Rule '{rule_file}' config '{config_name}' severity '{severity}': "
                f"Missing required fields: {', '.join(missing_fields)}"
            )
            continue

        # Validate field types
        field_issues = []

        threshold = rule_config['threshold']
        if threshold is not None and not isinstance(threshold, (int, float, str)):
            field_issues.append(f"threshold should be number/string/null, got {type(threshold).__name__}")

        reasoning = rule_config['reasoning']
        if not isinstance(reasoning, str):
            field_issues.append(f"reasoning must be string, got {type(reasoning).__name__}")
        elif not reasoning.strip():
            field_issues.append("reasoning cannot be empty")

        recommendations = rule_config['recommendations']
        if not isinstance(recommendations, list):
            field_issues.append(f"recommendations must be list, got {type(recommendations).__name__}")
        elif not recommendations:
            field_issues.append("recommendations cannot be empty list")

        if field_issues:
            warnings.append(
                f"Rule '{rule_file}' config '{config_name}' severity '{severity}': "
                f"Field validation failed: {'; '.join(field_issues)}"
            )
            continue

        validated_severities[severity] = rule_config

    if not validated_severities:
        warnings.append(
            f"Rule '{rule_file}' config '{config_name}': No valid severity levels found"
        )
        return {}, warnings

    return validated_severities, warnings


def validate_and_load_rules(rule_data: Dict[str, Any], rule_file: str) -> Dict[str, Any]:
    """
    Validates and loads rule data, logging warnings for any issues.

    This is the main entry point for rule validation during health check execution.

    Args:
        rule_data: Parsed JSON rule data
        rule_file: File path for logging purposes

    Returns:
        dict: Validated rules (may be empty if all rules are invalid)
    """
    validated_rules, warnings = validate_rule_structure(rule_data, rule_file)

    # Log all warnings but don't raise exceptions
    for warning in warnings:
        logger.warning(f"Rule validation: {warning}")

    if not validated_rules and rule_data:
        logger.error(
            f"Rule file '{rule_file}' has no valid rules. "
            f"Health check will continue without rule-based analysis for this file."
        )
    elif warnings:
        logger.info(
            f"Rule file '{rule_file}' loaded with {len(validated_rules)} valid metric(s). "
            f"Encountered {len(warnings)} validation issue(s)."
        )
    else:
        logger.debug(f"Rule file '{rule_file}' validated successfully with {len(validated_rules)} metric(s)")

    return validated_rules


def validate_rules_directory(rules_dir: str) -> Tuple[int, int, List[str]]:
    """
    Validates all JSON rule files in a directory.

    Useful for batch validation or testing.

    Args:
        rules_dir: Path to directory containing rule JSON files

    Returns:
        tuple: (total_files, valid_files, list_of_all_warnings)
    """
    import os
    import json

    total_files = 0
    valid_files = 0
    all_warnings = []

    if not os.path.exists(rules_dir):
        logger.warning(f"Rules directory does not exist: {rules_dir}")
        return 0, 0, [f"Directory not found: {rules_dir}"]

    for filename in os.listdir(rules_dir):
        if not filename.endswith('.json'):
            continue

        total_files += 1
        rule_file = os.path.join(rules_dir, filename)

        try:
            with open(rule_file, 'r') as f:
                rule_data = json.load(f)

            validated_rules, warnings = validate_rule_structure(rule_data, rule_file)
            all_warnings.extend(warnings)

            if validated_rules:
                valid_files += 1

        except json.JSONDecodeError as e:
            warning = f"Rule file '{rule_file}' has invalid JSON: {e}"
            all_warnings.append(warning)
            logger.error(warning)
        except Exception as e:
            warning = f"Rule file '{rule_file}' failed to load: {e}"
            all_warnings.append(warning)
            logger.error(warning)

    return total_files, valid_files, all_warnings

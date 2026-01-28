"""
Defines the ReportBuilder class, which is responsible for constructing
health check reports by executing a series of predefined actions.
"""

import importlib
import inspect
from pathlib import Path
from datetime import datetime

class ReportBuilder:
    """Handles the construction of the health check report.

    This class processes a report definition structure, dynamically executes
    check modules, reads static template parts, and assembles the final
    AsciiDoc report content and structured JSON data.

    Attributes:
        connector (object): The active database connector instance.
        settings (dict): The main application settings.
        active_plugin (object): The instantiated plugin for the target technology.
        report_sections (list): The list of sections that define the report structure.
        app_version (str): The current version of the application.
        adoc_content (list): A list of AsciiDoc strings that are built up during the process.
        all_structured_findings (dict): A dictionary that collects all structured
            data from the executed check modules.
    """

    def __init__(self, connector, settings, active_plugin, report_sections, app_version):
        """Initializes the ReportBuilder.

        Args:
            connector (object): The active database connector.
            settings (dict): The main application settings.
            active_plugin (object): The instantiated plugin for the target technology.
            report_sections (list): The report definition structure, which is a
                list of section dictionaries.
            app_version (str): The current version of the application, used for
                populating placeholders in report templates.
        """

        self.connector = connector
        self.settings = settings
        self.active_plugin = active_plugin
        self.report_sections = report_sections
        self.app_version = app_version # <-- Store the app version
        self.adoc_content = []
        self.all_structured_findings = {}

    def build(self):
        """Builds the full report by iterating through sections and actions.

        This is the main public method of the class. It orchestrates the entire
        report generation process based on the `report_sections` definition
        provided during initialization.

        Returns:
            tuple[str, dict]: A tuple where the first element is the complete
            AsciiDoc report content as a single string, and the second is a
            dictionary containing all structured findings from the checks.
        """

        for section in self.report_sections:
            if section.get('title'):
                self.adoc_content.append(f"== {section['title']}")
            for action in section['actions']:
                action_type = action.get('type')
                if action_type == 'module':
                    content = self._run_module(action['module'], action['function'])
                    self.adoc_content.append(content)
                elif action_type in ['header', 'comments']:
                    content = self._read_report_part(action['file'])
                    self.adoc_content.append(content)
        
        return "\n\n".join(self.adoc_content), self.all_structured_findings

    def _run_module(self, module_name, function_name):
        """Dynamically imports and executes a function from a check module.

        It captures the AsciiDoc and structured data output from the function
        and stores the structured data in the `all_structured_findings`
        instance attribute.

        Args:
            module_name (str): The full, importable path to the module
                (e.g., 'plugins.postgres.checks.overview').
            function_name (str): The name of the function to execute within the module.

        Returns:
            str: The AsciiDoc content string returned by the executed function,
                 or a formatted error string if the module fails.
        """

        try:
            module = importlib.import_module(module_name)
            func = getattr(module, function_name)

            # Check if function accepts all_findings parameter (for executive summary)
            sig = inspect.signature(func)
            if 'all_findings' in sig.parameters:
                adoc_content, structured_data = func(self.connector, self.settings, all_findings=self.all_structured_findings)
            else:
                adoc_content, structured_data = func(self.connector, self.settings)

            key = module_name.split('.')[-1]
            self.all_structured_findings[key] = structured_data
            return adoc_content
        except Exception as e:
            key = module_name.split('.')[-1]
            error_msg = f"[ERROR]\n====\nModule {module_name}.{function_name} failed: {e}\n====\n"
            self.all_structured_findings[key] = {"status": "error", "error": str(e)}
            return error_msg

    def _read_report_part(self, filename):
        """Reads a static text file from the plugin's template directory.

        This method constructs a path to a file within the active plugin's
        'templates/report_parts/' directory, reads its content, and performs
        placeholder substitution for values from settings, the current date,
        and the application version.

        Args:
            filename (str): The name of the file to read.

        Returns:
            str: The content of the file with placeholders replaced, or a
                 formatted error string if the file cannot be read.
        """

        try:
            template_path = self.active_plugin.get_template_path()
            file_path = template_path / "report_parts" / filename
            
            with open(file_path, 'r') as f:
                content = f.read()

            # Replace standard placeholders
            for key, value in self.settings.items():
                content = content.replace(f'${{{key.upper()}}}', str(value))
            
            # --- NEW: Replace dynamic and version placeholders ---
            content = content.replace('${CURRENT_DATE}', datetime.utcnow().strftime('%Y-%m-%d'))
            content = content.replace('${APP_VERSION}', self.app_version)
            
            return content
        except FileNotFoundError:
            return f"[ERROR]\n====\nReport part file '{filename}' not found in plugin's templates/report_parts/ directory.\n====\n"
        except Exception as e:
            return f"[ERROR]\n====\nCould not read report part file '{filename}': {e}\n====\n"

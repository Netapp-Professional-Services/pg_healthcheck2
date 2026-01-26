"""
Output formatting utilities for health check results.

Provides consistent formatting across all database plugins:
- AsciiDoc tables
- Literal blocks
- Shell command output detection and formatting
- Admonition blocks (NOTE, WARNING, CRITICAL, ERROR, TIP)
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class AsciiDocFormatter:
    """
    Formats data into AsciiDoc markup for reports.
    
    Supports:
    - Tables from list of dicts
    - Shell command output (detects tabular vs literal)
    - Literal blocks for raw text
    - Admonition blocks for messages
    """
    
    # Commands that typically produce tabular output
    TABULAR_COMMANDS = [
        'df', 'ps', 'free', 'top', 'netstat', 'ss', 'lsof',
        'iostat', 'vmstat', 'mpstat', 'sar', 'du'
    ]
    
    def format_table(self, data: List[Dict[str, Any]]) -> str:
        """
        Formats a list of dictionaries as an AsciiDoc table.
        
        Args:
            data: List of dicts where keys are column names
        
        Returns:
            AsciiDoc table string
        """
        if not data:
            return "[NOTE]\n====\nNo data to display.\n====\n"
        
        # Get columns from first row
        columns = list(data[0].keys())
        
        # Build table
        table_lines = ['|===']
        table_lines.append('|' + '|'.join(columns))
        
        for row in data:
            row_values = [str(row.get(col, '')) for col in columns]
            # Escape special characters
            row_values = [self._escape_asciidoc(v) for v in row_values]
            table_lines.append('|' + '|'.join(row_values))
        
        table_lines.append('|===')
        
        return '\n'.join(table_lines)
    
    def format_shell_output(self, command: str, output: str) -> str:
        """
        Formats shell command output intelligently.
        
        Detects if output is tabular (df, ps, etc.) and formats accordingly.
        Otherwise returns as literal block.
        
        Args:
            command: The shell command that was executed
            output: Raw output from the command
        
        Returns:
            Formatted AsciiDoc string
        """
        if not output or not output.strip():
            return self.format_note("No output from command.")
        
        # Check if this is a tabular command
        is_tabular = any(cmd in command.lower() for cmd in self.TABULAR_COMMANDS)
        
        if is_tabular and '\n' in output:
            lines = output.strip().split('\n')
            
            # Try to detect header line
            if lines and len(lines) > 1:
                header_line = lines[0]
                header_parts = header_line.split()
                
                # If header has multiple columns, treat as table
                if len(header_parts) >= 2:
                    return self._parse_tabular_output(lines, header_parts)
        
        # Fallback: literal block
        return self.format_literal(output)
    
    def _parse_tabular_output(self, lines: List[str], header: List[str]) -> str:
        """
        Parses tabular shell output into AsciiDoc table.
        
        Args:
            lines: All output lines (including header)
            header: Parsed header columns
        
        Returns:
            AsciiDoc table
        """
        table_lines = ['|===']
        table_lines.append('|' + '|'.join(header))
        
        # Parse data lines
        for line in lines[1:]:
            if not line.strip():
                continue
            
            # Split by whitespace, but limit to header column count
            parts = line.split(None, len(header) - 1)
            
            # Handle insufficient columns
            if len(parts) < len(header):
                # Pad with empty strings
                parts.extend([''] * (len(header) - len(parts)))
            elif len(parts) > len(header):
                # Truncate extra columns (shouldn't happen but be safe)
                parts = parts[:len(header)]
            
            # Escape special characters
            parts = [self._escape_asciidoc(part) for part in parts]
            
            table_lines.append('|' + '|'.join(parts))
        
        table_lines.append('|===')
        
        return '\n'.join(table_lines)
    
    def _escape_asciidoc(self, text: str) -> str:
        """
        Escape special AsciiDoc characters in text.
        
        Args:
            text: Text to escape
        
        Returns:
            Escaped text safe for AsciiDoc tables
        """
        # Escape pipe characters (table delimiter)
        text = text.replace('|', '\\|')
        
        # Escape square brackets (can trigger macros)
        text = text.replace('[', '\\[')
        text = text.replace(']', '\\]')
        
        # Escape backslashes that aren't already escaping something
        # This is tricky - only escape standalone backslashes
        # We'll do a simple approach: if backslash isn't followed by |, [, or ]
        # then escape it
        result = []
        i = 0
        while i < len(text):
            if text[i] == '\\':
                # Check if next char is already an escape sequence we created
                if i + 1 < len(text) and text[i + 1] in ('|', '[', ']', '\\'):
                    # Already escaped, keep as is
                    result.append(text[i])
                else:
                    # Standalone backslash, escape it
                    result.append('\\\\')
                i += 1
            else:
                result.append(text[i])
                i += 1
        
        return ''.join(result)
    
    def format_literal(self, text: str) -> str:
        """
        Formats text as a literal block.
        
        Args:
            text: Raw text to format
        
        Returns:
            AsciiDoc literal block
        """
        if not text:
            text = "(empty)"
        
        return f"[source,text]\n----\n{text}\n----\n"
    
    def format_note(self, message: str) -> str:
        """
        Formats a note/info message.
        
        Args:
            message: Note text
        
        Returns:
            AsciiDoc NOTE admonition block
        """
        return f"[NOTE]\n====\n{message}\n====\n"
    
    def format_warning(self, message: str) -> str:
        """
        Formats a warning message.
        
        Args:
            message: Warning text
        
        Returns:
            AsciiDoc WARNING admonition block
        """
        return f"[WARNING]\n====\n{message}\n====\n"
    
    def format_critical(self, message: str) -> str:
        """
        Formats a critical/important message.
        
        Args:
            message: Critical message text
        
        Returns:
            AsciiDoc IMPORTANT admonition block
        """
        return f"[IMPORTANT]\n====\n{message}\n====\n"
    
    def format_error(self, error: str) -> str:
        """
        Formats an error message.
        
        Args:
            error: Error text
        
        Returns:
            AsciiDoc CAUTION admonition block
        """
        return f"[CAUTION]\n====\n{error}\n====\n"
    
    def format_tip(self, message: str) -> str:
        """
        Formats a tip/recommendation message.
        
        Args:
            message: Tip text
        
        Returns:
            AsciiDoc TIP admonition block
        """
        return f"[TIP]\n====\n{message}\n====\n"
    
    def format_nodetool_status(self, nodes: List[Dict]) -> str:
        """
        Formats nodetool status output as an AsciiDoc table.
        
        Args:
            nodes: List of node dictionaries from NodetoolParser
        
        Returns:
            Formatted AsciiDoc table
        """
        if not nodes:
            return self.format_note("No nodes found.")
        
        # Create simplified view for display
        display_nodes = []
        for node in nodes:
            display_nodes.append({
                'DC': node.get('datacenter', 'unknown'),
                'Status': node.get('status', '?'),
                'State': node.get('state', '?'),
                'Address': node.get('address', 'unknown'),
                'Load': node.get('load', '0 B'),
                'Owns': f"{node.get('owns_effective_percent', 0):.1f}%",
                'Host ID': node.get('host_id', 'unknown')[:8] + '...',  # Truncate
                'Rack': node.get('rack', 'unknown')
            })
        
        return self.format_table(display_nodes)
    
    def format_nodetool_tpstats(self, pools: List[Dict]) -> str:
        """
        Formats nodetool tpstats output as an AsciiDoc table.
        
        Args:
            pools: List of thread pool dictionaries from NodetoolParser
        
        Returns:
            Formatted AsciiDoc table
        """
        if not pools:
            return self.format_note("No thread pool statistics found.")
        
        return self.format_table(pools)
    
    def format_nodetool_compactionstats(self, stats: Dict) -> str:
        """
        Formats nodetool compactionstats output.
        
        Args:
            stats: Compaction stats dictionary from NodetoolParser
        
        Returns:
            Formatted AsciiDoc string
        """
        lines = []
        
        pending = stats.get('pending_tasks', 0)
        active = stats.get('active_compactions', [])
        
        lines.append(f"**Pending Compaction Tasks:** {pending}")
        lines.append("")
        
        if active:
            lines.append(f"**Active Compactions:** {len(active)}")
            lines.append("")
            lines.append(self.format_table(active))
        else:
            lines.append("**Active Compactions:** None")
        
        return '\n'.join(lines)
    
    def format_dict_as_table(self, data: Dict[str, Any],
                            key_header: str = "Key",
                            value_header: str = "Value") -> str:
        """
        Formats a dictionary as a two-column AsciiDoc table.

        Args:
            data: Dictionary to format
            key_header: Header for key column
            value_header: Header for value column

        Returns:
            AsciiDoc table string
        """
        if not data:
            return self.format_note("No data to display.")

        table_lines = ['|===']
        table_lines.append(f'|{key_header}|{value_header}')

        for key, value in data.items():
            key_str = self._escape_asciidoc(str(key))
            value_str = self._escape_asciidoc(str(value))
            table_lines.append(f'|{key_str}|{value_str}')

        table_lines.append('|===')

        return '\n'.join(table_lines)

    def truncate_field(self, data: List[Dict[str, Any]],
                      field_name: str,
                      max_length: int = 120,
                      suffix: str = '...') -> List[Dict[str, Any]]:
        """
        Truncates a specific field in a list of dictionaries for display.

        Common use case: Truncating long query text, error messages, or descriptions
        in tables while preserving full data in structured findings.

        Args:
            data: List of dictionaries
            field_name: Name of the field to truncate
            max_length: Maximum length before truncation (default: 120)
            suffix: String to append when truncated (default: '...')

        Returns:
            New list with truncated field values (original data unchanged)

        Example:
            >>> data = [{'query': 'SELECT * FROM very_long_table_name...', 'time': 1.5}]
            >>> truncated = formatter.truncate_field(data, 'query', max_length=50)
            >>> # truncated[0]['query'] = 'SELECT * FROM very_long_table_name...'[:47] + '...'
        """
        if not data:
            return []

        truncated_data = []
        for row in data:
            truncated_row = row.copy()

            if field_name in truncated_row and truncated_row[field_name] is not None:
                field_value = str(truncated_row[field_name])

                if len(field_value) > max_length:
                    # Truncate and add suffix
                    truncate_at = max_length - len(suffix)
                    truncated_row[field_name] = field_value[:truncate_at] + suffix

            truncated_data.append(truncated_row)

        return truncated_data

    def format_table_with_truncation(self, data: List[Dict[str, Any]],
                                    truncate_fields: Dict[str, int] = None) -> str:
        """
        Formats a table with automatic field truncation.

        Convenience method that combines truncation and table formatting
        for common display scenarios (e.g., query result tables).

        Args:
            data: List of dictionaries to format
            truncate_fields: Dict mapping field names to max lengths
                           Example: {'query': 120, 'description': 80}
                           If None, no truncation is performed

        Returns:
            AsciiDoc table string

        Example:
            >>> queries = [
            ...     {'query': 'SELECT * FROM very_long...', 'time_ms': 1500},
            ...     {'query': 'UPDATE users SET...', 'time_ms': 300}
            ... ]
            >>> table = formatter.format_table_with_truncation(
            ...     queries,
            ...     truncate_fields={'query': 50}
            ... )
        """
        if not data:
            return self.format_note("No data to display.")

        # If no truncation specified, just format normally
        if not truncate_fields:
            return self.format_table(data)

        # Apply truncation to each specified field
        display_data = data
        for field_name, max_length in truncate_fields.items():
            display_data = self.truncate_field(display_data, field_name, max_length)

        return self.format_table(display_data)

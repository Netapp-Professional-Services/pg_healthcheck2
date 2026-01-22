from plugins.cassandra.utils.qrylib.qry_temporary_files import get_temporary_files_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 6  # Medium - potential disk waste and operational issues


def run_temporary_files_check(connector, settings):
    """
    Checks for temporary files in the Cassandra data directory.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Temporary Files in Data Directory",
        "Scanning for temporary files in Cassandra data directory using shell commands.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["temp_files"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute shell command
    query = get_temporary_files_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "find temporary files")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["temp_files"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Parse shell output
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    lines = output.strip().split('\n')
    temp_files = [line.strip() for line in lines if line.strip() and not line.startswith('find:')]
    
    if not temp_files:
        adoc_content.append("[NOTE]\n====\nNo temporary files found in data directory.\n====\n")
        structured_data["temp_files"] = {"status": "success", "data": [], "count": 0}
        return "\n".join(adoc_content), structured_data
    
    # Report findings
    adoc_content.append(
        f"[WARNING]\n====\n"
        f"**{len(temp_files)} temporary file(s)** found in data directory. "
        "These may indicate failed operations and consume disk space.\n"
        "====\n"
    )
    adoc_content.append(formatted)
    
    # List files if not too many
    if len(temp_files) <= 10:
        adoc_content.append("\n==== Temporary Files Found")
        for file_path in temp_files:
            adoc_content.append(f"* {file_path}")
    else:
        adoc_content.append(f"\nFirst 10 files: {', '.join(temp_files[:10])}")
    
    recommendations = [
        "Review and remove unnecessary temp files: 'rm -f <file_path>' (ensure safe to delete)",
        "Investigate source: check /var/log/cassandra/system.log for failed compactions, repairs, or streaming",
        "Monitor data directory regularly: add cron job for 'find /var/lib/cassandra/data -name '*tmp*' -delete' if appropriate",
        "Check disk usage with 'df -h /var/lib/cassandra' to assess impact"
    ]
    adoc_content.extend(format_recommendations(recommendations))
    
    structured_data["temp_files"] = {
        "status": "warning",
        "data": temp_files,
        "count": len(temp_files)
    }
    
    return "\n".join(adoc_content), structured_data
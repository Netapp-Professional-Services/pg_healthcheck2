# Diagnostic Data Import Pattern

This document describes the design pattern for importing offline diagnostic data (from tools like Instacollector, diagnostic bundles, etc.) into the health check system. Following this pattern ensures code reuse and minimizes maintenance overhead.

## Core Principle: Interface Compatibility

**The import connector MUST implement the same interface as the live connector.**

This allows all existing health checks to work unchanged with imported data. The connector acts as an adapter that translates pre-loaded file data into the same format returned by live API calls.

## Pattern Overview

```
┌─────────────────────────┐     ┌─────────────────────────┐
│   Live Connector        │     │   Import Connector      │
│   (KafkaConnector)      │     │ (KafkaInstacollector-   │
│                         │     │        Connector)       │
├─────────────────────────┤     ├─────────────────────────┤
│ - execute_query()       │     │ - execute_query()       │
│ - execute_ssh_on_all()  │     │ - execute_ssh_on_all()  │
│ - has_ssh_support()     │     │ - has_ssh_support()     │
│ - get_ssh_manager()     │     │ - get_ssh_manager()     │
│ - version_info          │     │ - version_info          │
│ - cluster_metadata      │     │ - cluster_metadata      │
└──────────┬──────────────┘     └──────────┬──────────────┘
           │                               │
           │ Same Interface                │
           ▼                               ▼
┌─────────────────────────────────────────────────────────┐
│                    Health Checks                        │
│   (check_isr_health.py, check_consumer_lag.py, etc.)    │
│                                                         │
│   Checks use connector interface - don't need to know   │
│   whether data comes from live connection or import     │
└─────────────────────────────────────────────────────────┘
```

## Implementation Checklist

### 1. Loader Class (data_loader.py)

Create a loader class that:
- Extracts tarballs (handles nested tarballs common in diagnostic exports)
- Parses technology-specific output files
- Provides accessor methods for different data types
- Handles multi-node data (e.g., `get_data(node=node_ip)`)

```python
class KafkaInstacollectorLoader:
    def load(self, path: str) -> bool:
        """Load and parse diagnostic export."""

    def get_kafka_output(self, command: str, node: str = None):
        """Get parsed Kafka CLI output."""

    def get_config(self, filename: str, node: str = None):
        """Get configuration file contents."""

    def get_log_files(self, pattern: str, node: str = None):
        """Get log files matching pattern."""
```

### 2. Import Connector (import_connector.py)

The connector MUST implement:

#### execute_query() - Same operations and return format as live connector
```python
def execute_query(self, query, params=None, return_raw=False):
    """
    Execute operations by reading from loaded files.

    Returns data in EXACT SAME FORMAT as live connector.
    """
    operation = json.loads(query).get('operation')

    if operation == 'describe_topics':
        # Transform loaded data to live connector format
        return self._describe_topics(return_raw)
    elif operation == 'consumer_lag':
        return self._get_consumer_lag(return_raw)
    # ... same operations as live connector
```

#### SSH simulation methods
```python
def has_ssh_support(self) -> bool:
    """Return True - we simulate SSH via loaded files."""
    return True

def get_ssh_hosts(self) -> List[str]:
    """Return list of node IPs from loaded data."""
    return self.cluster_nodes

def get_ssh_manager(self, host: str = None):
    """Return SimulatedSSHManager for command execution."""
    return SimulatedSSHManager(self.loader, host)

def execute_ssh_on_all_hosts(self, command: str, description: str) -> List[Dict]:
    """
    Simulate SSH command execution.

    Maps commands to loaded files:
    - 'cat /proc/meminfo' → mem.info
    - 'cat /proc/cpuinfo' → cpu.info
    - 'iostat' → io_stat.info
    - etc.
    """
```

#### Metadata properties
```python
@property
def version_info(self) -> Dict:
    """Return version info extracted from loaded data."""

def get_db_metadata(self) -> Dict:
    """Return cluster metadata for trends database."""
```

### 3. SimulatedSSHManager Class

For checks that use `get_ssh_manager()`:

```python
class SimulatedSSHManager:
    """Simulates SSH connection using loaded files."""

    def __init__(self, loader, host: str):
        self.loader = loader
        self.host = host
        self.connected = True

    def ensure_connected(self) -> bool:
        return True

    def execute_command(self, command: str, timeout: int = 30) -> tuple:
        """
        Simulate command execution.

        Returns: (stdout, stderr, exit_code)
        """
        # Map command to loaded file
        if 'meminfo' in command.lower():
            output = self.loader.get_raw_file('mem.info', self.host)
            return (output, '', 0) if output else ('', 'Not available', 1)
```

### 4. Report Definition (reports/import.py)

Use the SAME checks as the live report:

```python
REPORT_SECTIONS = [
    {
        "title": "Topic Health",
        "actions": [
            # Use existing check - NOT a duplicate _ic.py version
            {'type': 'module', 'module': 'plugins.kafka.checks.check_isr_health',
             'function': 'run_check_isr_health'},
        ]
    },
]
```

## What NOT to Do

### DON'T: Create duplicate check files

```
# BAD - Creates maintenance burden
plugins/kafka/checks/check_isr_health.py      # For live
plugins/kafka/checks/check_isr_health_ic.py   # For import - DUPLICATE!
```

### DO: Implement compatible connector interface

```
# GOOD - Same check works for both
plugins/kafka/checks/check_isr_health.py      # Works with both connectors
```

## Data Format Compatibility

The import connector must return data in the EXACT SAME format as the live connector:

```python
# Live connector _describe_topics() returns:
[
    {
        'topic': 'topic_name',
        'partitions': 3,
        'replication_factor': 3,
        'under_replicated_partitions': 0
    },
    ...
]

# Import connector _describe_topics() MUST return same format:
def _describe_topics(self, topic_names, return_raw):
    topics_data = self.loader.get_kafka_output('topics')

    # Transform loaded format to live connector format
    raw_results = []
    for topic in topics_data.get('topics', []):
        raw_results.append({
            'topic': topic.get('name'),
            'partitions': topic.get('partition_count'),
            'replication_factor': topic.get('replication_factor'),
            'under_replicated_partitions': self._count_under_replicated(topic)
        })

    return (formatted, raw_results) if return_raw else formatted
```

## Examples in Codebase

| Technology | Live Connector | Import Connector | Pattern |
|------------|---------------|------------------|---------|
| Cassandra | `connector.py` | `instacollector_connector.py` | SSH command mapping |
| OpenSearch | `connector.py` | `diagnostic_connector.py` | Operation file mapping |
| Kafka | `connector.py` | `instacollector_connector.py` | Query operation mapping |

## Testing Import Connectors

1. Run import with test data:
```bash
python plugins/kafka/import_instacollector.py /path/to/export.tar.gz --company test
```

2. Compare output structure with live run to verify format compatibility

3. Ensure same rules trigger for similar data patterns

## Adding New Import Support

1. Create loader class for the diagnostic format
2. Create import connector implementing live connector interface
3. Create report definition using existing checks
4. Test thoroughly with real diagnostic exports
5. Document any limitations (data not available in exports)

## Limitations to Document

Some data may not be available in diagnostic exports:
- Real-time metrics (must use snapshot values)
- Some SSH commands (lsof, process listings)
- Dynamic cluster state changes

Document these limitations in the connector and gracefully handle missing data.

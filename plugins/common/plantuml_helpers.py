"""
PlantUML Diagram Helpers

Provides reusable utilities for generating PlantUML diagrams in health check reports.
Supports common diagram patterns: cluster topologies, replication flows, datacenter layouts.

Usage:
    from plugins.common.plantuml_helpers import ClusterTopologyDiagram

    diagram = ClusterTopologyDiagram(title="My Cluster")
    diagram.add_leader_node("node1", "192.168.1.1:5432", state="running", metrics={"lag": "0 MB"})
    diagram.add_replica_node("node2", "192.168.1.2:5432", state="streaming", sync_mode="async")
    diagram.add_replication("node1", "node2", sync=False)

    plantuml_code = diagram.generate()
"""

from typing import Dict, List, Optional, Tuple, Any
from enum import Enum


class NodeState(Enum):
    """Standard node health states with associated colors."""
    HEALTHY = ("#90EE90", "✅")  # Light green
    WARNING = ("#FFD700", "⚠️")   # Yellow/Gold
    CRITICAL = ("#FF6B6B", "❌")  # Red
    UNKNOWN = ("#D3D3D3", "❓")   # Light gray


class NodeRole(Enum):
    """Common node roles across database systems."""
    LEADER = ("LEADER", "#90EE90")
    REPLICA = ("REPLICA", "#87CEEB")
    COORDINATOR = ("COORDINATOR", "#DDA0DD")
    BROKER = ("BROKER", "#87CEEB")
    ARBITER = ("ARBITER", "#F0E68C")
    SEED = ("SEED", "#98FB98")


class PlantUMLBase:
    """Base class for PlantUML diagram generators."""

    def __init__(self, title: str = "Cluster Topology"):
        """
        Initialize base diagram.

        Args:
            title: Diagram title
        """
        self.title = title
        self.preamble: List[str] = []
        self.body: List[str] = []
        self.legend_items: List[Tuple[str, str]] = []
        self._setup_defaults()

    def _setup_defaults(self):
        """Setup default PlantUML styles and settings."""
        # Define color constants for reference (not using !define anymore)
        self.colors = {
            'LEADER_COLOR': '#90EE90',      # Light green
            'REPLICA_COLOR': '#87CEEB',     # Sky blue
            'WARNING_COLOR': '#FFD700',     # Gold
            'CRITICAL_COLOR': '#FF6B6B',    # Light red
            'UNKNOWN_COLOR': '#D3D3D3',     # Light gray
            'COORDINATOR_COLOR': '#DDA0DD', # Plum
            'BROKER_COLOR': '#87CEEB',      # Sky blue
            'ARBITER_COLOR': '#F0E68C',     # Khaki
            'SEED_COLOR': '#98FB98'         # Pale green
        }

        self.preamble = [
            "@startuml",
            "",
            "skinparam componentStyle rectangle",
            "skinparam shadowing false",
            "skinparam DefaultFontName Arial",
            "skinparam ArrowThickness 2",
            "",
            f"title {self.title}",
            ""
        ]

    def add_custom_color(self, name: str, hex_color: str):
        """
        Add a custom color definition.

        Args:
            name: Color name (e.g., "CUSTOM_BLUE")
            hex_color: Hex color code (e.g., "#1E90FF")
        """
        self.colors[name] = hex_color

    def add_legend_item(self, color_ref: str, description: str):
        """
        Add an item to the legend.

        Args:
            color_ref: Color reference (e.g., "LEADER_COLOR")
            description: Description text
        """
        self.legend_items.append((color_ref, description))

    def _build_legend(self) -> List[str]:
        """Build legend section."""
        if not self.legend_items:
            return []

        lines = [
            "",
            "legend right",
            "  |= Color |= Meaning |"
        ]

        for color_ref, description in self.legend_items:
            # Resolve color reference to actual hex code
            hex_color = self.colors.get(color_ref, color_ref)
            lines.append(f"  | <back:{hex_color}>   </back> | {description} |")

        lines.append("endlegend")
        return lines

    def generate(self) -> str:
        """
        Generate complete PlantUML diagram code.

        Returns:
            Complete PlantUML code as string
        """
        lines = self.preamble + self.body + self._build_legend() + ["", "@enduml"]
        return "\n".join(lines)


class ClusterTopologyDiagram(PlantUMLBase):
    """
    Generator for cluster topology diagrams.

    Supports leader-follower, multi-master, and ring topologies.
    Commonly used for: PostgreSQL Patroni, MySQL replication, MongoDB replica sets.
    """

    def __init__(self, title: str = "Cluster Topology", show_legend: bool = True):
        """
        Initialize cluster topology diagram.

        Args:
            title: Diagram title
            show_legend: Whether to show the legend
        """
        super().__init__(title)
        self.nodes: Dict[str, Dict] = {}
        self.connections: List[Dict] = []
        self.show_legend = show_legend

        if show_legend:
            self._setup_default_legend()

    def _setup_default_legend(self):
        """Setup default legend items for cluster topologies."""
        self.add_legend_item("LEADER_COLOR", "Leader Node (Healthy)")
        self.add_legend_item("REPLICA_COLOR", "Replica Node (Healthy)")
        self.add_legend_item("WARNING_COLOR", "Warning State")
        self.add_legend_item("CRITICAL_COLOR", "Critical State")

    def add_node(
        self,
        node_id: str,
        label: str,
        role: str = "NODE",
        state: str = "healthy",
        details: Optional[Dict[str, Any]] = None,
        color: Optional[str] = None
    ):
        """
        Add a node to the topology.

        Args:
            node_id: Unique node identifier (used for connections)
            label: Display label for the node
            role: Node role (LEADER, REPLICA, COORDINATOR, etc.)
            state: Node state (healthy, warning, critical, unknown)
            details: Additional details to display (dict of key-value pairs)
            color: Custom color override (e.g., "LEADER_COLOR")
        """
        # Determine color based on state and role
        if color is None:
            if state.lower() in ['warning', 'degraded']:
                color = "WARNING_COLOR"
            elif state.lower() in ['critical', 'down', 'failed']:
                color = "CRITICAL_COLOR"
            elif state.lower() == 'unknown':
                color = "UNKNOWN_COLOR"
            else:
                # Use role-based color for healthy nodes
                role_colors = {
                    'LEADER': 'LEADER_COLOR',
                    'REPLICA': 'REPLICA_COLOR',
                    'COORDINATOR': 'COORDINATOR_COLOR',
                    'BROKER': 'BROKER_COLOR',
                    'ARBITER': 'ARBITER_COLOR',
                    'SEED': 'SEED_COLOR'
                }
                color = role_colors.get(role.upper(), 'REPLICA_COLOR')

        self.nodes[node_id] = {
            'label': label,
            'role': role,
            'state': state,
            'details': details or {},
            'color': color
        }

    def add_leader_node(
        self,
        node_id: str,
        address: str,
        state: str = "running",
        metrics: Optional[Dict[str, Any]] = None
    ):
        """
        Convenience method to add a leader node.

        Args:
            node_id: Unique node identifier
            address: Node address (host:port)
            state: Node state
            metrics: Additional metrics to display
        """
        details = {'State': state}
        if metrics:
            details.update(metrics)

        self.add_node(
            node_id=node_id,
            label=f"{node_id}\\n{address}\\n**[LEADER]**",
            role="LEADER",
            state=state,
            details=details
        )

    def add_replica_node(
        self,
        node_id: str,
        address: str,
        state: str = "streaming",
        sync_mode: str = "async",
        lag: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ):
        """
        Convenience method to add a replica node.

        Args:
            node_id: Unique node identifier
            address: Node address (host:port)
            state: Node state
            sync_mode: Replication mode (sync/async)
            lag: Replication lag display
            metrics: Additional metrics to display
        """
        details = {
            'State': state,
            'Mode': sync_mode.upper()
        }
        if lag:
            details['Lag'] = lag
        if metrics:
            details.update(metrics)

        label_parts = [node_id, address, f"**[REPLICA - {sync_mode.upper()}]**"]

        self.add_node(
            node_id=node_id,
            label="\\n".join(label_parts),
            role="REPLICA",
            state=state,
            details=details
        )

    def add_replication(
        self,
        source_id: str,
        target_id: str,
        sync: bool = False,
        label: Optional[str] = None,
        bidirectional: bool = False
    ):
        """
        Add a replication connection between nodes.

        Args:
            source_id: Source node ID
            target_id: Target node ID
            sync: Whether this is synchronous replication
            label: Optional label for the arrow
            bidirectional: Whether replication is bidirectional
        """
        self.connections.append({
            'source': source_id,
            'target': target_id,
            'sync': sync,
            'label': label or (f"{'sync' if sync else 'async'} replication"),
            'bidirectional': bidirectional
        })

    def generate(self) -> str:
        """Generate the complete diagram."""
        # Build node definitions
        for node_id, node_info in self.nodes.items():
            label = node_info['label']
            color = node_info['color']

            # Add details to label
            if node_info['details']:
                detail_lines = [f"{k}: {v}" for k, v in node_info['details'].items()]
                label += "\\n" + "\\n".join(detail_lines)

            # Resolve color reference to actual hex code
            hex_color = self.colors.get(color, color)
            self.body.append(f'component "{label}" as {node_id} {hex_color}')

        if self.nodes:
            self.body.append("")

        # Build connections
        for conn in self.connections:
            source = conn['source']
            target = conn['target']
            arrow = "-->" if conn['sync'] else "..>"

            if conn['bidirectional']:
                arrow = "<" + arrow

            label = conn['label']
            self.body.append(f"{source} {arrow} {target} : {label}")

        return super().generate()


class RingTopologyDiagram(PlantUMLBase):
    """
    Generator for ring topology diagrams.

    Commonly used for: Cassandra, Riak, DynamoDB-style systems.
    """

    def __init__(self, title: str = "Ring Topology", show_tokens: bool = True):
        """
        Initialize ring topology diagram.

        Args:
            title: Diagram title
            show_tokens: Whether to show token ranges
        """
        super().__init__(title)
        self.nodes: List[Dict] = []
        self.show_tokens = show_tokens
        self._setup_default_legend()

    def _setup_default_legend(self):
        """Setup default legend for ring topologies."""
        self.add_legend_item("SEED_COLOR", "Seed Node")
        self.add_legend_item("REPLICA_COLOR", "Normal Node")
        self.add_legend_item("WARNING_COLOR", "Node with Issues")

    def add_node(
        self,
        node_id: str,
        address: str,
        datacenter: str,
        rack: str,
        is_seed: bool = False,
        token_range: Optional[str] = None,
        state: str = "UP",
        load: Optional[str] = None
    ):
        """
        Add a node to the ring.

        Args:
            node_id: Node identifier
            address: Node address
            datacenter: Datacenter name
            rack: Rack name
            is_seed: Whether this is a seed node
            token_range: Token range (e.g., "0...1000")
            state: Node state (UP, DOWN, etc.)
            load: Node load/data size
        """
        color = "SEED_COLOR" if is_seed else "REPLICA_COLOR"
        if state.upper() != "UP":
            color = "WARNING_COLOR"

        self.nodes.append({
            'id': node_id,
            'address': address,
            'datacenter': datacenter,
            'rack': rack,
            'is_seed': is_seed,
            'token_range': token_range,
            'state': state,
            'load': load,
            'color': color
        })

    def generate(self) -> str:
        """Generate the ring topology diagram."""
        # Group nodes by datacenter
        dcs: Dict[str, List[Dict]] = {}
        for node in self.nodes:
            dc = node['datacenter']
            if dc not in dcs:
                dcs[dc] = []
            dcs[dc].append(node)

        # Build datacenter packages
        for dc_name, dc_nodes in dcs.items():
            self.body.append(f"package \"{dc_name}\" {{")

            # Group by rack
            racks: Dict[str, List[Dict]] = {}
            for node in dc_nodes:
                rack = node['rack']
                if rack not in racks:
                    racks[rack] = []
                racks[rack].append(node)

            for rack_name, rack_nodes in racks.items():
                self.body.append(f"  package \"{rack_name}\" {{")

                for node in rack_nodes:
                    label_parts = [
                        node['id'],
                        node['address'],
                        f"State: {node['state']}"
                    ]

                    if node['is_seed']:
                        label_parts.insert(2, "**[SEED]**")

                    if self.show_tokens and node['token_range']:
                        label_parts.append(f"Tokens: {node['token_range']}")

                    if node['load']:
                        label_parts.append(f"Load: {node['load']}")

                    label = "\\n".join(label_parts)
                    # Resolve color reference to actual hex code
                    hex_color = self.colors.get(node['color'], node['color'])
                    self.body.append(f'    component "{label}" as {node["id"]} {hex_color}')

                self.body.append("  }")

            self.body.append("}")
            self.body.append("")

        # Add ring connections (each node to next)
        if len(self.nodes) > 1:
            for i in range(len(self.nodes)):
                current = self.nodes[i]
                next_node = self.nodes[(i + 1) % len(self.nodes)]
                self.body.append(f"{current['id']} ..> {next_node['id']}")

        return super().generate()


class DatacenterLayoutDiagram(PlantUMLBase):
    """
    Generator for multi-datacenter deployment diagrams.

    Shows geographical distribution and inter-DC connections.
    """

    def __init__(self, title: str = "Multi-Datacenter Layout"):
        """
        Initialize datacenter layout diagram.

        Args:
            title: Diagram title
        """
        super().__init__(title)
        self.datacenters: Dict[str, Dict] = {}
        self.dc_connections: List[Tuple[str, str, str]] = []

    def add_datacenter(
        self,
        dc_id: str,
        name: str,
        region: str,
        node_count: int,
        replication_factor: Optional[int] = None
    ):
        """
        Add a datacenter.

        Args:
            dc_id: Datacenter identifier
            name: Datacenter name
            region: Geographic region
            node_count: Number of nodes in DC
            replication_factor: Replication factor for this DC
        """
        self.datacenters[dc_id] = {
            'name': name,
            'region': region,
            'node_count': node_count,
            'rf': replication_factor
        }

    def add_dc_connection(self, source_dc: str, target_dc: str, label: str = "replication"):
        """
        Add a connection between datacenters.

        Args:
            source_dc: Source datacenter ID
            target_dc: Target datacenter ID
            label: Connection label
        """
        self.dc_connections.append((source_dc, target_dc, label))

    def generate(self) -> str:
        """Generate the datacenter layout diagram."""
        for dc_id, dc_info in self.datacenters.items():
            label_parts = [
                f"**{dc_info['name']}**",
                f"Region: {dc_info['region']}",
                f"Nodes: {dc_info['node_count']}"
            ]

            if dc_info['rf']:
                label_parts.append(f"RF: {dc_info['rf']}")

            label = "\\n".join(label_parts)
            self.body.append(f'cloud "{label}" as {dc_id}')

        self.body.append("")

        for source, target, label in self.dc_connections:
            self.body.append(f"{source} ..> {target} : {label}")

        return super().generate()


def embed_diagram_in_adoc(plantuml_code: str, diagram_id: str, format: str = "svg") -> str:
    """
    Wrap PlantUML code in AsciiDoc syntax.

    Args:
        plantuml_code: PlantUML diagram code
        diagram_id: Unique identifier for the diagram
        format: Output format (svg, png)

    Returns:
        Complete AsciiDoc block with PlantUML code
    """
    return f"[plantuml, {diagram_id}, {format}]\n----\n{plantuml_code}\n----"

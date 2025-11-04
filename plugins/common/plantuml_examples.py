"""
PlantUML Helpers - Usage Examples

This file demonstrates how to use the PlantUML helpers for common scenarios
across different database technologies.

See plantuml_helpers.py for the implementation.
"""

from plugins.common.plantuml_helpers import (
    ClusterTopologyDiagram,
    RingTopologyDiagram,
    DatacenterLayoutDiagram,
    embed_diagram_in_adoc
)


# ============================================================================
# Example 1: PostgreSQL Patroni Cluster (Leader-Follower)
# ============================================================================
def example_patroni_cluster():
    """Example: PostgreSQL Patroni cluster with leader and replicas."""
    diagram = ClusterTopologyDiagram(title="PostgreSQL Patroni Cluster")

    # Add leader node
    diagram.add_leader_node(
        node_id="pg1",
        address="192.168.1.10:5432",
        state="running",
        metrics={"Timeline": 1, "Version": "16.10"}
    )

    # Add synchronous replica
    diagram.add_replica_node(
        node_id="pg2",
        address="192.168.1.11:5432",
        state="streaming",
        sync_mode="sync",
        lag="0 MB",
        metrics={"Timeline": 1}
    )

    # Add async replica
    diagram.add_replica_node(
        node_id="pg3",
        address="192.168.1.12:5432",
        state="streaming",
        sync_mode="async",
        lag="5.2 MB",
        metrics={"Timeline": 1}
    )

    # Add replication connections
    diagram.add_replication("pg1", "pg2", sync=True)
    diagram.add_replication("pg1", "pg3", sync=False)

    plantuml_code = diagram.generate()
    return embed_diagram_in_adoc(plantuml_code, "patroni-example", "svg")


# ============================================================================
# Example 2: MySQL Master-Slave Replication
# ============================================================================
def example_mysql_replication():
    """Example: MySQL master-slave replication topology."""
    diagram = ClusterTopologyDiagram(title="MySQL Replication Topology")

    # Master node
    diagram.add_node(
        node_id="master",
        label="mysql-master\\n10.0.1.10:3306\\n**[MASTER]**",
        role="LEADER",
        state="healthy",
        details={
            "Binlog": "mysql-bin.000042",
            "Position": "1547",
            "Threads": "150"
        }
    )

    # Slave 1 (healthy)
    diagram.add_node(
        node_id="slave1",
        label="mysql-slave-1\\n10.0.1.11:3306\\n**[SLAVE]**",
        role="REPLICA",
        state="healthy",
        details={
            "Seconds Behind": "0",
            "IO Running": "Yes",
            "SQL Running": "Yes"
        }
    )

    # Slave 2 (lagging)
    diagram.add_node(
        node_id="slave2",
        label="mysql-slave-2\\n10.0.1.12:3306\\n**[SLAVE]**",
        role="REPLICA",
        state="warning",
        details={
            "Seconds Behind": "45",
            "IO Running": "Yes",
            "SQL Running": "Yes"
        }
    )

    diagram.add_replication("master", "slave1", sync=False, label="async replication (0s lag)")
    diagram.add_replication("master", "slave2", sync=False, label="async replication (45s lag)")

    plantuml_code = diagram.generate()
    return embed_diagram_in_adoc(plantuml_code, "mysql-replication", "svg")


# ============================================================================
# Example 3: MongoDB Replica Set
# ============================================================================
def example_mongodb_replica_set():
    """Example: MongoDB replica set topology."""
    diagram = ClusterTopologyDiagram(title="MongoDB Replica Set")

    # Primary
    diagram.add_node(
        node_id="primary",
        label="mongo-rs-0\\nmongo0.example.com:27017\\n**[PRIMARY]**",
        role="LEADER",
        state="healthy",
        details={
            "State": "PRIMARY",
            "OptimeDate": "2025-11-03 16:00:00",
            "Uptime": "45d"
        }
    )

    # Secondary 1
    diagram.add_node(
        node_id="secondary1",
        label="mongo-rs-1\\nmongo1.example.com:27017\\n**[SECONDARY]**",
        role="REPLICA",
        state="healthy",
        details={
            "State": "SECONDARY",
            "Replication Lag": "0s",
            "Priority": "1"
        }
    )

    # Secondary 2
    diagram.add_node(
        node_id="secondary2",
        label="mongo-rs-2\\nmongo2.example.com:27017\\n**[SECONDARY]**",
        role="REPLICA",
        state="healthy",
        details={
            "State": "SECONDARY",
            "Replication Lag": "2s",
            "Priority": "1"
        }
    )

    # Arbiter (no data)
    diagram.add_node(
        node_id="arbiter",
        label="mongo-arbiter\\nmongo-arb.example.com:27017\\n**[ARBITER]**",
        role="ARBITER",
        state="healthy",
        details={"State": "ARBITER", "Votes": "1"}
    )

    # Connections
    diagram.add_replication("primary", "secondary1", sync=False)
    diagram.add_replication("primary", "secondary2", sync=False)

    plantuml_code = diagram.generate()
    return embed_diagram_in_adoc(plantuml_code, "mongodb-replica-set", "svg")


# ============================================================================
# Example 4: Cassandra Ring Topology (Multi-DC)
# ============================================================================
def example_cassandra_ring():
    """Example: Cassandra ring topology across multiple datacenters."""
    diagram = RingTopologyDiagram(
        title="Cassandra Ring Topology",
        show_tokens=True
    )

    # DC1 nodes
    diagram.add_node(
        node_id="dc1_node1",
        address="10.1.1.10:9042",
        datacenter="DC1",
        rack="rack1",
        is_seed=True,
        token_range="-9223...−4611",
        state="UP",
        load="150 GB"
    )

    diagram.add_node(
        node_id="dc1_node2",
        address="10.1.1.11:9042",
        datacenter="DC1",
        rack="rack2",
        is_seed=False,
        token_range="-4611...0",
        state="UP",
        load="145 GB"
    )

    diagram.add_node(
        node_id="dc1_node3",
        address="10.1.1.12:9042",
        datacenter="DC1",
        rack="rack3",
        is_seed=False,
        token_range="0...4611",
        state="UP",
        load="148 GB"
    )

    # DC2 nodes
    diagram.add_node(
        node_id="dc2_node1",
        address="10.2.1.10:9042",
        datacenter="DC2",
        rack="rack1",
        is_seed=True,
        token_range="4611...9223",
        state="UP",
        load="152 GB"
    )

    diagram.add_node(
        node_id="dc2_node2",
        address="10.2.1.11:9042",
        datacenter="DC2",
        rack="rack2",
        is_seed=False,
        token_range="9223...13835",
        state="DEGRADED",  # Node with issues
        load="180 GB"
    )

    plantuml_code = diagram.generate()
    return embed_diagram_in_adoc(plantuml_code, "cassandra-ring", "svg")


# ============================================================================
# Example 5: Kafka Broker Cluster
# ============================================================================
def example_kafka_cluster():
    """Example: Kafka broker cluster with controller."""
    diagram = ClusterTopologyDiagram(title="Kafka Broker Cluster")

    # Controller (broker that's also controller)
    diagram.add_node(
        node_id="broker1",
        label="kafka-broker-1\\n10.0.1.20:9092\\n**[CONTROLLER]**",
        role="COORDINATOR",
        state="healthy",
        details={
            "Controller": "Yes",
            "Active Controllers": "1",
            "Topics": "250",
            "Partitions": "5000"
        }
    )

    # Regular brokers
    diagram.add_node(
        node_id="broker2",
        label="kafka-broker-2\\n10.0.1.21:9092\\n**[BROKER]**",
        role="BROKER",
        state="healthy",
        details={
            "Controller": "No",
            "Partitions": "4950"
        }
    )

    diagram.add_node(
        node_id="broker3",
        label="kafka-broker-3\\n10.0.1.22:9092\\n**[BROKER]**",
        role="BROKER",
        state="healthy",
        details={
            "Controller": "No",
            "Partitions": "5010"
        }
    )

    # Note: Kafka doesn't have replication between brokers in the traditional sense,
    # but we can show the cluster membership
    diagram.body.append("")
    diagram.body.append("note right of broker1")
    diagram.body.append("  Controller manages:")
    diagram.body.append("  • Partition leadership")
    diagram.body.append("  • Broker metadata")
    diagram.body.append("  • Topic configurations")
    diagram.body.append("end note")

    plantuml_code = diagram.generate()
    return embed_diagram_in_adoc(plantuml_code, "kafka-cluster", "svg")


# ============================================================================
# Example 6: Multi-Region Deployment (Cassandra/DynamoDB style)
# ============================================================================
def example_multi_region_deployment():
    """Example: Multi-region deployment with cross-region replication."""
    diagram = DatacenterLayoutDiagram(title="Multi-Region Deployment")

    # Add datacenters/regions
    diagram.add_datacenter(
        dc_id="us_east",
        name="US East",
        region="us-east-1",
        node_count=6,
        replication_factor=3
    )

    diagram.add_datacenter(
        dc_id="us_west",
        name="US West",
        region="us-west-2",
        node_count=6,
        replication_factor=3
    )

    diagram.add_datacenter(
        dc_id="eu_central",
        name="EU Central",
        region="eu-central-1",
        node_count=3,
        replication_factor=2
    )

    diagram.add_datacenter(
        dc_id="ap_southeast",
        name="AP Southeast",
        region="ap-southeast-1",
        node_count=3,
        replication_factor=2
    )

    # Add cross-region connections
    diagram.add_dc_connection("us_east", "us_west", "cross-region replication")
    diagram.add_dc_connection("us_east", "eu_central", "cross-region replication")
    diagram.add_dc_connection("us_west", "ap_southeast", "cross-region replication")

    plantuml_code = diagram.generate()
    return embed_diagram_in_adoc(plantuml_code, "multi-region", "svg")


# ============================================================================
# Example 7: Redis Sentinel (HA Setup)
# ============================================================================
def example_redis_sentinel():
    """Example: Redis with Sentinel for high availability."""
    diagram = ClusterTopologyDiagram(title="Redis Sentinel HA")

    # Master
    diagram.add_node(
        node_id="redis_master",
        label="redis-master\\n10.0.1.30:6379\\n**[MASTER]**",
        role="LEADER",
        state="healthy",
        details={
            "Role": "master",
            "Connected Slaves": "2",
            "Uptime": "30d"
        }
    )

    # Replicas
    diagram.add_node(
        node_id="redis_slave1",
        label="redis-slave-1\\n10.0.1.31:6379\\n**[REPLICA]**",
        role="REPLICA",
        state="healthy",
        details={
            "Role": "slave",
            "Offset": "1234567890",
            "Lag": "0s"
        }
    )

    diagram.add_node(
        node_id="redis_slave2",
        label="redis-slave-2\\n10.0.1.32:6379\\n**[REPLICA]**",
        role="REPLICA",
        state="healthy",
        details={
            "Role": "slave",
            "Offset": "1234567850",
            "Lag": "1s"
        }
    )

    # Sentinel nodes (monitoring)
    diagram.add_node(
        node_id="sentinel1",
        label="sentinel-1\\n10.0.1.40:26379\\n**[SENTINEL]**",
        role="COORDINATOR",
        state="healthy",
        details={"Monitored Masters": "1"}
    )

    diagram.add_replication("redis_master", "redis_slave1", sync=False)
    diagram.add_replication("redis_master", "redis_slave2", sync=False)

    # Add note about Sentinels
    diagram.body.append("")
    diagram.body.append("note bottom of sentinel1")
    diagram.body.append("  Sentinels monitor master/replicas")
    diagram.body.append("  and perform automatic failover")
    diagram.body.append("  (3 sentinels recommended)")
    diagram.body.append("end note")

    plantuml_code = diagram.generate()
    return embed_diagram_in_adoc(plantuml_code, "redis-sentinel", "svg")


# ============================================================================
# Integration with CheckContentBuilder
# ============================================================================
def example_integration_with_check_builder():
    """
    Example: How to integrate PlantUML diagrams with CheckContentBuilder.

    This is the typical pattern used in health check modules.
    """
    from plugins.common.check_helpers import CheckContentBuilder

    builder = CheckContentBuilder()
    builder.h3("Cluster Topology")

    # Add summary information
    builder.text("*Cluster:* my-production-cluster")
    builder.text("*Total Nodes:* 3")
    builder.text("*Health Score:* 95/100")
    builder.blank()

    # Create and add PlantUML diagram
    diagram = ClusterTopologyDiagram(title="Production Cluster")
    diagram.add_leader_node("node1", "10.0.1.1:5432", state="running")
    diagram.add_replica_node("node2", "10.0.1.2:5432", state="streaming", sync_mode="async", lag="0 MB")

    diagram.add_replication("node1", "node2", sync=False)

    plantuml_code = diagram.generate()
    adoc_block = embed_diagram_in_adoc(plantuml_code, "my-cluster-topology", "svg")

    # Add diagram to builder (split by lines)
    for line in adoc_block.split('\n'):
        builder.text(line)

    builder.blank()

    # Add additional details
    builder.text("*Node Details:*")
    builder.text("• Node 1: Leader, healthy")
    builder.text("• Node 2: Replica, 0 MB lag")

    return builder.build()


# ============================================================================
# Quick Reference
# ============================================================================
"""
Quick Reference for PlantUML Helpers:

1. ClusterTopologyDiagram - Leader/follower, master/slave patterns
   Use for: PostgreSQL, MySQL, MongoDB, Redis, etc.

   diagram = ClusterTopologyDiagram(title="My Cluster")
   diagram.add_leader_node("node1", "host:port", state="running")
   diagram.add_replica_node("node2", "host:port", sync_mode="async", lag="5 MB")
   diagram.add_replication("node1", "node2", sync=False)
   plantuml = diagram.generate()

2. RingTopologyDiagram - Distributed hash ring patterns
   Use for: Cassandra, Riak, consistent hashing systems

   diagram = RingTopologyDiagram(title="Cassandra Ring")
   diagram.add_node("node1", "host:port", "DC1", "rack1", is_seed=True, token_range="0...1000")
   plantuml = diagram.generate()

3. DatacenterLayoutDiagram - Multi-region deployments
   Use for: Cross-region replication, geo-distributed systems

   diagram = DatacenterLayoutDiagram(title="Multi-Region")
   diagram.add_datacenter("us_east", "US East", "us-east-1", node_count=6, replication_factor=3)
   diagram.add_dc_connection("us_east", "us_west", "replication")
   plantuml = diagram.generate()

4. Embedding in AsciiDoc:

   adoc_block = embed_diagram_in_adoc(plantuml_code, "unique-diagram-id", "svg")

   Then add to CheckContentBuilder:
   for line in adoc_block.split('\\n'):
       builder.text(line)

Node States:
- "healthy" / "running" / "streaming" → Green
- "warning" / "degraded" → Yellow
- "critical" / "down" / "failed" → Red
- "unknown" → Gray

Node Roles:
- LEADER - Primary/master node (green)
- REPLICA - Secondary/slave node (blue)
- COORDINATOR - Controller/coordinator (purple)
- BROKER - Kafka-style broker (blue)
- ARBITER - Voting-only node (yellow)
- SEED - Cassandra seed node (light green)
"""

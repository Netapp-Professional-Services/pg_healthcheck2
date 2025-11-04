#!/usr/bin/env python3
"""
Cassandra Test Data Generator & Load Tester

Creates realistic test data and schema configurations to exercise health checks.
Supports multiple scenarios to test different check behaviors.

Usage:
    python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario all
    python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario optimal
    python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario problematic
    python cassandra_test_data_generator.py --config ../../../config/cassandra_instaclustr.yaml --scenario load-test --count 100000
"""

import argparse
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from faker import Faker


class CassandraTestDataGenerator:
    """Generates test data and schema configurations for Cassandra health check testing."""

    def __init__(self, config_file):
        """Initialize with configuration."""
        self.config = self._load_config(config_file)
        self.cluster = None
        self.session = None
        self.faker = Faker()
        self.stats = {
            'tables_created': 0,
            'keyspaces_created': 0,
            'rows_inserted': 0,
            'indexes_created': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

    def _load_config(self, config_file):
        """Load YAML configuration."""
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def connect(self):
        """Connect to Cassandra cluster."""
        try:
            hosts = self.config.get('hosts', ['localhost'])
            port = self.config.get('port', 9042)
            user = self.config.get('user')
            password = self.config.get('password')

            auth_provider = None
            if user and password:
                auth_provider = PlainTextAuthProvider(username=user, password=password)

            self.cluster = Cluster(
                contact_points=hosts,
                port=port,
                auth_provider=auth_provider
            )

            self.session = self.cluster.connect()
            self.session.row_factory = dict_factory

            # Test connection
            result = list(self.session.execute("SELECT release_version FROM system.local"))
            version = result[0]['release_version'] if result else 'Unknown'

            print(f"✅ Connected to Cassandra {version}")
            print(f"   Hosts: {', '.join(hosts)}")
            return True

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def disconnect(self):
        """Disconnect from Cassandra."""
        if self.cluster:
            self.cluster.shutdown()
            print("✅ Disconnected from Cassandra")

    def create_test_keyspace(self, keyspace_name='test_health_check', replication_factor=3):
        """Create a test keyspace."""
        try:
            # Get datacenter from config or detect from system.local
            local_dc = self.config.get('local_dc') or self.config.get('datacenter')

            if not local_dc:
                # Try to detect DC from system.local
                result = list(self.session.execute("SELECT data_center FROM system.local"))
                if result:
                    local_dc = result[0]['data_center']

            if local_dc:
                replication_config = f"'class': 'NetworkTopologyStrategy', '{local_dc}': {replication_factor}"
            else:
                replication_config = f"'class': 'SimpleStrategy', 'replication_factor': {replication_factor}"

            cql = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH replication = {{{replication_config}}}
            AND durable_writes = true
            """

            self.session.execute(cql)
            print(f"✅ Created keyspace: {keyspace_name}")
            self.stats['keyspaces_created'] += 1
            return True

        except Exception as e:
            print(f"❌ Failed to create keyspace: {e}")
            self.stats['errors'] += 1
            return False

    def run_scenario(self, scenario='all', count=1000):
        """Run a specific test data scenario."""
        self.stats['start_time'] = time.time()

        scenarios = {
            'optimal': self.scenario_optimal_config,
            'problematic': self.scenario_problematic_config,
            'secondary_indexes': self.scenario_secondary_indexes,
            'mixed_compaction': self.scenario_mixed_compaction,
            'ttl_tables': self.scenario_ttl_tables,
            'load_test': lambda: self.scenario_load_test(count)
        }

        if scenario == 'all':
            print("\n🚀 Running ALL scenarios...\n")
            for name, func in scenarios.items():
                if name != 'load_test':  # Skip load test in 'all'
                    print(f"\n{'='*60}")
                    print(f"Scenario: {name}")
                    print(f"{'='*60}")
                    func()
        elif scenario in scenarios:
            print(f"\n🚀 Running scenario: {scenario}\n")
            scenarios[scenario]()
        else:
            print(f"❌ Unknown scenario: {scenario}")
            print(f"Available scenarios: {', '.join(scenarios.keys())}, all")
            return False

        self.stats['end_time'] = time.time()
        self._print_stats()
        return True

    def scenario_optimal_config(self):
        """Create tables with optimal configurations."""
        keyspace = 'test_optimal'
        self.create_test_keyspace(keyspace)
        self.session.set_keyspace(keyspace)

        # Table 1: Optimal STCS table
        print("  Creating optimal STCS table...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                created_at TIMESTAMP,
                last_login TIMESTAMP
            ) WITH
                compaction = {'class': 'SizeTieredCompactionStrategy'}
                AND bloom_filter_fp_chance = 0.01
        """)
        self.stats['tables_created'] += 1

        # Table 2: Optimal LCS table
        print("  Creating optimal LCS table...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id UUID,
                user_id UUID,
                amount DECIMAL,
                timestamp TIMESTAMP,
                description TEXT,
                PRIMARY KEY (user_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
                AND compaction = {'class': 'LeveledCompactionStrategy'}
                AND bloom_filter_fp_chance = 0.01
        """)
        self.stats['tables_created'] += 1

        # Insert some test data
        print("  Inserting test data...")
        self._insert_user_data(keyspace, 'user_profiles', 100)

        print("  ✅ Optimal scenario complete")

    def scenario_problematic_config(self):
        """Create tables with problematic configurations to trigger warnings."""
        keyspace = 'test_problematic'
        self.create_test_keyspace(keyspace)
        self.session.set_keyspace(keyspace)

        # Table 1: High bloom filter FP
        print("  Creating table with high bloom filter FP...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS high_bloom_fp (
                id UUID PRIMARY KEY,
                data TEXT
            ) WITH bloom_filter_fp_chance = 0.15
        """)
        self.stats['tables_created'] += 1

        # Table 2: Table with default settings (for baseline comparison)
        # Note: In Cassandra 4.0+, read_repair_chance properties were removed
        print("  Creating table with default settings...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS default_settings (
                id UUID PRIMARY KEY,
                data TEXT
            )
        """)
        self.stats['tables_created'] += 1

        # Table 3: Table with CDC enabled (change data capture)
        # Note: In Cassandra 4.0+, read_repair_chance properties were removed
        print("  Creating table with CDC enabled...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS cdc_enabled (
                id UUID PRIMARY KEY,
                data TEXT,
                timestamp TIMESTAMP
            ) WITH cdc = true
        """)
        self.stats['tables_created'] += 1

        # Table 4: Low index intervals
        print("  Creating table with low index intervals...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS low_index_intervals (
                id UUID PRIMARY KEY,
                data TEXT
            ) WITH
                min_index_interval = 32
                AND max_index_interval = 256
        """)
        self.stats['tables_created'] += 1

        print("  ✅ Problematic scenario complete")

    def scenario_secondary_indexes(self):
        """Create tables with various secondary index configurations."""
        keyspace = 'test_indexes'
        self.create_test_keyspace(keyspace)
        self.session.set_keyspace(keyspace)

        # Table with standard secondary index
        print("  Creating table with standard secondary index...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS users_indexed (
                user_id UUID PRIMARY KEY,
                email TEXT,
                status TEXT,
                country TEXT
            )
        """)
        self.stats['tables_created'] += 1

        # Create secondary index on low-cardinality column
        self.session.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_status
            ON users_indexed (status)
        """)
        self.stats['indexes_created'] += 1
        print("    - Created index on 'status' (low cardinality - good)")

        # Create secondary index on high-cardinality column (problematic)
        self.session.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_email
            ON users_indexed (email)
        """)
        self.stats['indexes_created'] += 1
        print("    - Created index on 'email' (high cardinality - problematic)")

        print("  ✅ Secondary indexes scenario complete")

    def scenario_mixed_compaction(self):
        """Create tables with different compaction strategies."""
        keyspace = 'test_compaction'
        self.create_test_keyspace(keyspace)
        self.session.set_keyspace(keyspace)

        # STCS table
        print("  Creating STCS table...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS stcs_table (
                id UUID PRIMARY KEY,
                data TEXT
            ) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
        """)
        self.stats['tables_created'] += 1

        # LCS table
        print("  Creating LCS table...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS lcs_table (
                id UUID,
                timestamp TIMESTAMP,
                data TEXT,
                PRIMARY KEY (id, timestamp)
            ) WITH compaction = {'class': 'LeveledCompactionStrategy'}
        """)
        self.stats['tables_created'] += 1

        # TWCS table (time window)
        print("  Creating TWCS table...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS twcs_table (
                sensor_id UUID,
                timestamp TIMESTAMP,
                value DOUBLE,
                PRIMARY KEY (sensor_id, timestamp)
            ) WITH compaction = {
                'class': 'TimeWindowCompactionStrategy',
                'compaction_window_unit': 'DAYS',
                'compaction_window_size': '1'
            }
        """)
        self.stats['tables_created'] += 1

        print("  ✅ Mixed compaction scenario complete")

    def scenario_ttl_tables(self):
        """Create tables with TTL configurations."""
        keyspace = 'test_ttl'
        self.create_test_keyspace(keyspace)
        self.session.set_keyspace(keyspace)

        # Short TTL table (1 day)
        print("  Creating short TTL table (1 day)...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS session_data (
                session_id UUID PRIMARY KEY,
                user_id UUID,
                data TEXT,
                created_at TIMESTAMP
            ) WITH default_time_to_live = 86400
        """)
        self.stats['tables_created'] += 1

        # Medium TTL table (7 days)
        print("  Creating medium TTL table (7 days)...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS cache_data (
                key TEXT PRIMARY KEY,
                value TEXT,
                cached_at TIMESTAMP
            ) WITH default_time_to_live = 604800
        """)
        self.stats['tables_created'] += 1

        # Long TTL table (90 days)
        print("  Creating long TTL table (90 days)...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS archived_logs (
                log_id UUID PRIMARY KEY,
                message TEXT,
                timestamp TIMESTAMP
            ) WITH default_time_to_live = 7776000
        """)
        self.stats['tables_created'] += 1

        print("  ✅ TTL tables scenario complete")

    def scenario_load_test(self, count=10000):
        """Insert large amount of test data."""
        keyspace = 'test_load'
        self.create_test_keyspace(keyspace)
        self.session.set_keyspace(keyspace)

        # Create table for load testing
        print("  Creating load test table...")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS load_test_data (
                id UUID,
                partition_key INT,
                timestamp TIMESTAMP,
                user_id UUID,
                event_type TEXT,
                data TEXT,
                metadata MAP<TEXT, TEXT>,
                PRIMARY KEY (partition_key, timestamp, id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """)
        self.stats['tables_created'] += 1

        # Insert data in batches
        print(f"  Inserting {count} rows...")
        batch_size = 100
        inserted = 0

        import uuid

        for i in range(0, count, batch_size):
            batch_count = min(batch_size, count - i)

            for j in range(batch_count):
                row_id = uuid.uuid4()
                partition = random.randint(1, 100)
                timestamp = datetime.now() - timedelta(hours=random.randint(0, 168))
                user_id = uuid.uuid4()
                event_type = random.choice(['login', 'logout', 'purchase', 'view', 'click'])
                data = self.faker.text(max_nb_chars=200)
                metadata = {
                    'source': random.choice(['web', 'mobile', 'api']),
                    'version': f'1.{random.randint(0, 10)}.{random.randint(0, 20)}'
                }

                try:
                    self.session.execute("""
                        INSERT INTO load_test_data (id, partition_key, timestamp, user_id, event_type, data, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (row_id, partition, timestamp, user_id, event_type, data, metadata))

                    inserted += 1
                    self.stats['rows_inserted'] += 1

                except Exception as e:
                    print(f"    ⚠️  Insert failed: {e}")
                    self.stats['errors'] += 1

            if (inserted % 1000) == 0:
                print(f"    - Inserted {inserted}/{count} rows...")

        print(f"  ✅ Load test complete: {inserted} rows inserted")

    def _insert_user_data(self, keyspace, table, count=100):
        """Insert realistic user data."""
        import uuid

        for i in range(count):
            user_id = uuid.uuid4()
            username = self.faker.user_name()
            email = self.faker.email()
            created_at = datetime.now() - timedelta(days=random.randint(1, 365))
            last_login = datetime.now() - timedelta(hours=random.randint(1, 168))

            try:
                self.session.execute(f"""
                    INSERT INTO {keyspace}.{table} (user_id, username, email, created_at, last_login)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, username, email, created_at, last_login))

                self.stats['rows_inserted'] += 1

            except Exception as e:
                print(f"    ⚠️  Insert failed: {e}")
                self.stats['errors'] += 1

    def _print_stats(self):
        """Print execution statistics."""
        duration = self.stats['end_time'] - self.stats['start_time']

        print("\n" + "="*60)
        print("📊 Execution Statistics")
        print("="*60)
        print(f"Duration:           {duration:.2f} seconds")
        print(f"Keyspaces created:  {self.stats['keyspaces_created']}")
        print(f"Tables created:     {self.stats['tables_created']}")
        print(f"Indexes created:    {self.stats['indexes_created']}")
        print(f"Rows inserted:      {self.stats['rows_inserted']}")
        print(f"Errors:             {self.stats['errors']}")

        if self.stats['rows_inserted'] > 0:
            rows_per_sec = self.stats['rows_inserted'] / duration
            print(f"Insert rate:        {rows_per_sec:.2f} rows/sec")

        print("="*60)


def main():
    parser = argparse.ArgumentParser(description='Cassandra Test Data Generator & Load Tester')
    parser.add_argument('--config', required=True, help='Path to Cassandra config file')
    parser.add_argument('--scenario', default='all',
                        choices=['all', 'optimal', 'problematic', 'secondary_indexes',
                                'mixed_compaction', 'ttl_tables', 'load_test'],
                        help='Test scenario to run')
    parser.add_argument('--count', type=int, default=10000,
                        help='Number of rows for load test scenario')

    args = parser.parse_args()

    # Initialize generator
    generator = CassandraTestDataGenerator(args.config)

    # Connect
    if not generator.connect():
        return 1

    try:
        # Run scenario
        success = generator.run_scenario(args.scenario, args.count)
        return 0 if success else 1

    finally:
        generator.disconnect()


if __name__ == '__main__':
    sys.exit(main())

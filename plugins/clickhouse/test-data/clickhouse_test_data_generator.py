#!/usr/bin/env python3
"""
ClickHouse Test Data Generator & Load Tester

Creates realistic test data and schema configurations to exercise health checks.
Supports multiple scenarios to test different check behaviors.

Usage:
    python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario all
    python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario optimal
    python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario problematic
    python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario analytics --count 100000
    python clickhouse_test_data_generator.py --config ../../../config/clickhouse_ic_test.yaml --scenario time_series --count 50000
"""

import argparse
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import uuid

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import yaml
import clickhouse_connect
from faker import Faker


class ClickHouseTestDataGenerator:
    """Generates test data and schema configurations for ClickHouse health check testing."""

    def __init__(self, config_file):
        """Initialize with configuration."""
        self.config = self._load_config(config_file)
        self.client = None
        self.faker = Faker()
        self.stats = {
            'tables_created': 0,
            'databases_created': 0,
            'rows_inserted': 0,
            'views_created': 0,
            'dictionaries_created': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

    def _load_config(self, config_file):
        """Load YAML configuration."""
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def connect(self):
        """Connect to ClickHouse cluster."""
        try:
            # Get connection parameters
            hosts = self.config.get('hosts', ['localhost'])
            port = self.config.get('http_port', 8123)
            user = self.config.get('user', 'default')
            password = self.config.get('password', '')
            database = self.config.get('database', 'default')
            secure = self.config.get('secure', False)

            # Use first host
            host = hosts[0] if isinstance(hosts, list) else hosts

            # Connect
            self.client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                secure=secure
            )

            # Test connection
            result = self.client.query("SELECT version()")
            version = result.result_set[0][0] if result.result_set else 'Unknown'

            print(f"✅ Connected to ClickHouse {version}")
            print(f"   Host: {host}:{port}")
            return True

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def disconnect(self):
        """Disconnect from ClickHouse."""
        if self.client:
            self.client.close()
            print("✅ Disconnected from ClickHouse")

    def create_test_database(self, database_name='test_health_check'):
        """Create a test database."""
        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            print(f"✅ Created database: {database_name}")
            self.stats['databases_created'] += 1
            return True

        except Exception as e:
            print(f"❌ Failed to create database: {e}")
            self.stats['errors'] += 1
            return False

    def run_scenario(self, scenario='all', count=10000):
        """Run a specific test data scenario."""
        self.stats['start_time'] = time.time()

        scenarios = {
            'optimal': self.scenario_optimal_config,
            'problematic': self.scenario_problematic_config,
            'mixed_engines': self.scenario_mixed_engines,
            'time_series': lambda: self.scenario_time_series(count),
            'analytics': lambda: self.scenario_analytics_workload(count),
            'materialized_views': self.scenario_materialized_views,
            'dictionaries': self.scenario_dictionaries,
            'partitioned_tables': self.scenario_partitioned_tables
        }

        if scenario == 'all':
            print("\n🚀 Running ALL scenarios...\n")
            for name, func in scenarios.items():
                if name not in ['analytics', 'time_series']:  # Skip heavy load tests in 'all'
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
        database = 'test_optimal'
        self.create_test_database(database)

        # Table 1: Optimal MergeTree for user profiles
        print("  Creating optimal MergeTree table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.user_profiles (
                user_id UUID,
                username String,
                email String,
                created_at DateTime,
                last_login DateTime,
                plan_type LowCardinality(String),
                is_active UInt8
            )
            ENGINE = MergeTree()
            ORDER BY (user_id, created_at)
            SETTINGS index_granularity = 8192
        """)
        self.stats['tables_created'] += 1

        # Table 2: Optimal time-series table with partitioning
        print("  Creating optimal partitioned events table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.events (
                event_id UUID,
                user_id UUID,
                event_type LowCardinality(String),
                event_time DateTime,
                properties Map(String, String),
                value Float64
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_time)
            ORDER BY (event_type, event_time, user_id)
            SETTINGS index_granularity = 8192
        """)
        self.stats['tables_created'] += 1

        # Insert some test data
        print("  Inserting test data...")
        self._insert_user_profiles(database, 'user_profiles', 1000)

        print("  ✅ Optimal scenario complete")

    def scenario_problematic_config(self):
        """Create tables with problematic configurations to trigger warnings."""
        database = 'test_problematic'
        self.create_test_database(database)

        # Table 1: Missing ORDER BY (problematic)
        print("  Creating table without ORDER BY (problematic)...")
        try:
            self.client.command(f"""
                CREATE TABLE IF NOT EXISTS {database}.unordered_table (
                    id UUID,
                    data String,
                    timestamp DateTime
                )
                ENGINE = MergeTree()
                ORDER BY tuple()
            """)
            self.stats['tables_created'] += 1
        except Exception as e:
            print(f"    ⚠️  Table creation note: {e}")

        # Table 2: Poor index granularity
        print("  Creating table with poor index granularity...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.poor_granularity (
                id UInt64,
                data String
            )
            ENGINE = MergeTree()
            ORDER BY id
            SETTINGS index_granularity = 256
        """)
        self.stats['tables_created'] += 1

        # Table 3: Table with many columns (potential performance issue)
        print("  Creating table with many columns...")
        columns = ", ".join([f"col{i} String" for i in range(50)])
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.many_columns (
                id UInt64,
                {columns}
            )
            ENGINE = MergeTree()
            ORDER BY id
        """)
        self.stats['tables_created'] += 1

        print("  ✅ Problematic scenario complete")

    def scenario_mixed_engines(self):
        """Create tables with different MergeTree engine variants."""
        database = 'test_engines'
        self.create_test_database(database)

        # ReplacingMergeTree
        print("  Creating ReplacingMergeTree table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.user_state (
                user_id UUID,
                state String,
                updated_at DateTime,
                version UInt64
            )
            ENGINE = ReplacingMergeTree(version)
            ORDER BY user_id
        """)
        self.stats['tables_created'] += 1

        # SummingMergeTree
        print("  Creating SummingMergeTree table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.metrics_summary (
                date Date,
                metric_name LowCardinality(String),
                value Float64,
                count UInt64
            )
            ENGINE = SummingMergeTree()
            ORDER BY (date, metric_name)
        """)
        self.stats['tables_created'] += 1

        # AggregatingMergeTree
        print("  Creating AggregatingMergeTree table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.aggregated_stats (
                date Date,
                user_id UUID,
                total_value AggregateFunction(sum, Float64),
                avg_value AggregateFunction(avg, Float64),
                count_value AggregateFunction(count, UInt64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY (date, user_id)
        """)
        self.stats['tables_created'] += 1

        # CollapsingMergeTree
        print("  Creating CollapsingMergeTree table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.collapsing_data (
                id UUID,
                value String,
                timestamp DateTime,
                sign Int8
            )
            ENGINE = CollapsingMergeTree(sign)
            ORDER BY (id, timestamp)
        """)
        self.stats['tables_created'] += 1

        print("  ✅ Mixed engines scenario complete")

    def scenario_time_series(self, count=50000):
        """Create and populate time-series tables."""
        database = 'test_time_series'
        self.create_test_database(database)

        # Metrics table
        print("  Creating metrics table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.metrics (
                timestamp DateTime,
                metric_name LowCardinality(String),
                host LowCardinality(String),
                value Float64,
                tags Map(String, String)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (metric_name, host, timestamp)
            TTL timestamp + INTERVAL 30 DAY
        """)
        self.stats['tables_created'] += 1

        # Insert time-series data
        print(f"  Inserting {count} time-series metrics...")
        self._insert_time_series_data(database, 'metrics', count)

        print("  ✅ Time-series scenario complete")

    def scenario_analytics_workload(self, count=100000):
        """Create analytics tables and populate with realistic data."""
        database = 'test_analytics'
        self.create_test_database(database)

        # Page views table
        print("  Creating page views table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.page_views (
                timestamp DateTime,
                user_id UUID,
                session_id UUID,
                page_url String,
                referrer String,
                user_agent String,
                country LowCardinality(String),
                device_type LowCardinality(String),
                duration_ms UInt32
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (timestamp, user_id)
        """)
        self.stats['tables_created'] += 1

        # Insert analytics data
        print(f"  Inserting {count} page view records...")
        self._insert_analytics_data(database, 'page_views', count)

        print("  ✅ Analytics workload scenario complete")

    def scenario_materialized_views(self):
        """Create materialized views for aggregations."""
        database = 'test_views'
        self.create_test_database(database)

        # Base table
        print("  Creating base table for views...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.raw_events (
                timestamp DateTime,
                event_type String,
                user_id UUID,
                value Float64
            )
            ENGINE = MergeTree()
            ORDER BY timestamp
        """)
        self.stats['tables_created'] += 1

        # Materialized view for hourly aggregation
        print("  Creating hourly aggregation materialized view...")
        self.client.command(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.hourly_stats
            ENGINE = SummingMergeTree()
            ORDER BY (hour, event_type)
            AS SELECT
                toStartOfHour(timestamp) as hour,
                event_type,
                sum(value) as total_value,
                count() as event_count
            FROM {database}.raw_events
            GROUP BY hour, event_type
        """)
        self.stats['views_created'] += 1

        # Insert some test data to populate the view
        print("  Inserting test events to populate view...")
        self._insert_events(database, 'raw_events', 5000)

        print("  ✅ Materialized views scenario complete")

    def scenario_dictionaries(self):
        """Create dictionary configurations."""
        database = 'test_dictionaries'
        self.create_test_database(database)

        # Source table for dictionary
        print("  Creating dictionary source table...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.country_codes (
                code String,
                name String,
                continent String,
                population UInt64
            )
            ENGINE = MergeTree()
            ORDER BY code
        """)
        self.stats['tables_created'] += 1

        # Insert country data
        print("  Inserting country data...")
        countries = [
            ('US', 'United States', 'North America', 331000000),
            ('UK', 'United Kingdom', 'Europe', 67000000),
            ('DE', 'Germany', 'Europe', 83000000),
            ('FR', 'France', 'Europe', 67000000),
            ('JP', 'Japan', 'Asia', 126000000),
            ('CN', 'China', 'Asia', 1400000000),
            ('IN', 'India', 'Asia', 1380000000),
            ('BR', 'Brazil', 'South America', 212000000)
        ]

        for code, name, continent, population in countries:
            self.client.command(f"""
                INSERT INTO {database}.country_codes VALUES ('{code}', '{name}', '{continent}', {population})
            """)
            self.stats['rows_inserted'] += 1

        # Create dictionary
        print("  Creating flat dictionary...")
        try:
            self.client.command(f"""
                CREATE DICTIONARY IF NOT EXISTS {database}.country_dict (
                    code String,
                    name String,
                    continent String,
                    population UInt64
                )
                PRIMARY KEY code
                SOURCE(CLICKHOUSE(
                    HOST 'localhost'
                    DB '{database}'
                    TABLE 'country_codes'
                ))
                LAYOUT(FLAT())
                LIFETIME(MIN 300 MAX 360)
            """)
            self.stats['dictionaries_created'] += 1
        except Exception as e:
            print(f"    ⚠️  Dictionary creation note: {e}")

        print("  ✅ Dictionaries scenario complete")

    def scenario_partitioned_tables(self):
        """Create tables with various partitioning strategies."""
        database = 'test_partitions'
        self.create_test_database(database)

        # Monthly partitions
        print("  Creating table with monthly partitions...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.monthly_data (
                date Date,
                user_id UUID,
                value Float64
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, user_id)
        """)
        self.stats['tables_created'] += 1

        # Daily partitions
        print("  Creating table with daily partitions...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.daily_data (
                timestamp DateTime,
                metric_name String,
                value Float64
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(timestamp)
            ORDER BY (timestamp, metric_name)
            TTL timestamp + INTERVAL 7 DAY
        """)
        self.stats['tables_created'] += 1

        # Multi-level partitioning
        print("  Creating table with multi-level partitioning...")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.hierarchical_data (
                date Date,
                country String,
                city String,
                value Float64
            )
            ENGINE = MergeTree()
            PARTITION BY (toYYYYMM(date), country)
            ORDER BY (date, country, city)
        """)
        self.stats['tables_created'] += 1

        print("  ✅ Partitioned tables scenario complete")

    def _insert_user_profiles(self, database, table, count=1000):
        """Insert realistic user profile data."""
        data = []
        plan_types = ['free', 'basic', 'premium', 'enterprise']

        for i in range(count):
            data.append([
                str(uuid.uuid4()),
                self.faker.user_name(),
                self.faker.email(),
                datetime.now() - timedelta(days=random.randint(1, 365)),
                datetime.now() - timedelta(hours=random.randint(1, 168)),
                random.choice(plan_types),
                random.randint(0, 1)
            ])

            if len(data) >= 1000:
                self.client.insert(f"{database}.{table}", data, column_names=[
                    'user_id', 'username', 'email', 'created_at', 'last_login', 'plan_type', 'is_active'
                ])
                self.stats['rows_inserted'] += len(data)
                data = []

        if data:
            self.client.insert(f"{database}.{table}", data, column_names=[
                'user_id', 'username', 'email', 'created_at', 'last_login', 'plan_type', 'is_active'
            ])
            self.stats['rows_inserted'] += len(data)

    def _insert_time_series_data(self, database, table, count=50000):
        """Insert time-series metrics data."""
        metrics = ['cpu_usage', 'memory_usage', 'disk_io', 'network_tx', 'network_rx']
        hosts = [f'server-{i:03d}' for i in range(1, 11)]
        data = []

        for i in range(count):
            data.append([
                datetime.now() - timedelta(minutes=random.randint(0, 10080)),  # Last 7 days
                random.choice(metrics),
                random.choice(hosts),
                random.uniform(0, 100),
                {'region': random.choice(['us-east', 'us-west', 'eu-west']), 'env': random.choice(['prod', 'staging'])}
            ])

            if len(data) >= 10000:
                self.client.insert(f"{database}.{table}", data, column_names=[
                    'timestamp', 'metric_name', 'host', 'value', 'tags'
                ])
                self.stats['rows_inserted'] += len(data)
                print(f"    - Inserted {self.stats['rows_inserted']}/{count} rows...")
                data = []

        if data:
            self.client.insert(f"{database}.{table}", data, column_names=[
                'timestamp', 'metric_name', 'host', 'value', 'tags'
            ])
            self.stats['rows_inserted'] += len(data)

    def _insert_analytics_data(self, database, table, count=100000):
        """Insert analytics/page view data."""
        countries = ['US', 'UK', 'DE', 'FR', 'JP', 'CN', 'IN', 'BR']
        devices = ['desktop', 'mobile', 'tablet']
        pages = ['/home', '/products', '/about', '/contact', '/blog', '/pricing']
        data = []

        for i in range(count):
            data.append([
                datetime.now() - timedelta(hours=random.randint(0, 720)),  # Last 30 days
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                random.choice(pages),
                self.faker.url() if random.random() > 0.5 else '',
                self.faker.user_agent(),
                random.choice(countries),
                random.choice(devices),
                random.randint(100, 60000)
            ])

            if len(data) >= 10000:
                self.client.insert(f"{database}.{table}", data, column_names=[
                    'timestamp', 'user_id', 'session_id', 'page_url', 'referrer',
                    'user_agent', 'country', 'device_type', 'duration_ms'
                ])
                self.stats['rows_inserted'] += len(data)
                print(f"    - Inserted {self.stats['rows_inserted']}/{count} rows...")
                data = []

        if data:
            self.client.insert(f"{database}.{table}", data, column_names=[
                'timestamp', 'user_id', 'session_id', 'page_url', 'referrer',
                'user_agent', 'country', 'device_type', 'duration_ms'
            ])
            self.stats['rows_inserted'] += len(data)

    def _insert_events(self, database, table, count=5000):
        """Insert event data."""
        event_types = ['click', 'view', 'purchase', 'signup', 'logout']
        data = []

        for i in range(count):
            data.append([
                datetime.now() - timedelta(hours=random.randint(0, 72)),
                random.choice(event_types),
                str(uuid.uuid4()),
                random.uniform(0, 1000)
            ])

            if len(data) >= 1000:
                self.client.insert(f"{database}.{table}", data, column_names=[
                    'timestamp', 'event_type', 'user_id', 'value'
                ])
                self.stats['rows_inserted'] += len(data)
                data = []

        if data:
            self.client.insert(f"{database}.{table}", data, column_names=[
                'timestamp', 'event_type', 'user_id', 'value'
            ])
            self.stats['rows_inserted'] += len(data)

    def _print_stats(self):
        """Print execution statistics."""
        duration = self.stats['end_time'] - self.stats['start_time']

        print("\n" + "="*60)
        print("📊 Execution Statistics")
        print("="*60)
        print(f"Duration:           {duration:.2f} seconds")
        print(f"Databases created:  {self.stats['databases_created']}")
        print(f"Tables created:     {self.stats['tables_created']}")
        print(f"Views created:      {self.stats['views_created']}")
        print(f"Dictionaries:       {self.stats['dictionaries_created']}")
        print(f"Rows inserted:      {self.stats['rows_inserted']:,}")
        print(f"Errors:             {self.stats['errors']}")

        if self.stats['rows_inserted'] > 0:
            rows_per_sec = self.stats['rows_inserted'] / duration
            print(f"Insert rate:        {rows_per_sec:.2f} rows/sec")

        print("="*60)


def main():
    parser = argparse.ArgumentParser(description='ClickHouse Test Data Generator & Load Tester')
    parser.add_argument('--config', required=True, help='Path to ClickHouse config file')
    parser.add_argument('--scenario', default='all',
                        choices=['all', 'optimal', 'problematic', 'mixed_engines',
                                'time_series', 'analytics', 'materialized_views',
                                'dictionaries', 'partitioned_tables'],
                        help='Test scenario to run')
    parser.add_argument('--count', type=int, default=10000,
                        help='Number of rows for load test scenarios')

    args = parser.parse_args()

    # Initialize generator
    generator = ClickHouseTestDataGenerator(args.config)

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

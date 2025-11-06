#!/usr/bin/env python3
"""
ClickHouse Test Data Cleanup Script

Removes all test databases and data created by test data generator.

Usage:
    python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml
    python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml --database test_optimal
    python cleanup_test_data.py --config ../../../config/clickhouse_ic_test.yaml --all --confirm
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import yaml
import clickhouse_connect


class TestDataCleanup:
    """Cleanup utility for test data and databases."""

    # Test databases created by test data generator
    TEST_DATABASES = [
        'test_health_check',
        'test_optimal',
        'test_problematic',
        'test_engines',
        'test_time_series',
        'test_analytics',
        'test_views',
        'test_dictionaries',
        'test_partitions'
    ]

    def __init__(self, config_file):
        """Initialize with configuration."""
        self.config = self._load_config(config_file)
        self.client = None

    def _load_config(self, config_file):
        """Load YAML configuration."""
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def connect(self):
        """Connect to ClickHouse cluster."""
        try:
            hosts = self.config.get('hosts', ['localhost'])
            port = self.config.get('http_port', 8123)
            user = self.config.get('user', 'default')
            password = self.config.get('password', '')
            database = self.config.get('database', 'default')
            secure = self.config.get('secure', False)

            # Use first host
            host = hosts[0] if isinstance(hosts, list) else hosts

            self.client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                secure=secure
            )

            print(f"✅ Connected to ClickHouse")
            return True

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def disconnect(self):
        """Disconnect from ClickHouse."""
        if self.client:
            self.client.close()

    def list_test_databases(self):
        """List all test databases that exist."""
        try:
            result = self.client.query("SHOW DATABASES")

            existing_test_databases = []
            for row in result.result_set:
                db_name = row[0]
                if db_name in self.TEST_DATABASES:
                    existing_test_databases.append(db_name)

            return existing_test_databases

        except Exception as e:
            print(f"❌ Failed to list databases: {e}")
            return []

    def get_database_info(self, database):
        """Get information about a database."""
        try:
            # Get table count
            result = self.client.query(f"""
                SELECT count(*) as count
                FROM system.tables
                WHERE database = '{database}'
            """)
            table_count = result.result_set[0][0] if result.result_set else 0

            # Get total size
            result = self.client.query(f"""
                SELECT
                    formatReadableSize(sum(total_bytes)) as total_size,
                    sum(total_rows) as total_rows
                FROM system.tables
                WHERE database = '{database}'
            """)

            if result.result_set and result.result_set[0]:
                total_size = result.result_set[0][0]
                total_rows = result.result_set[0][1]
            else:
                total_size = '0 B'
                total_rows = 0

            return {
                'table_count': table_count,
                'total_size': total_size,
                'total_rows': total_rows
            }

        except Exception as e:
            print(f"  ⚠️  Could not get info for {database}: {e}")
            return None

    def drop_database(self, database, confirm=False):
        """Drop a database."""
        try:
            info = self.get_database_info(database)
            if info:
                print(f"\n  Database: {database}")
                print(f"    Tables: {info['table_count']}")
                print(f"    Total Size: {info['total_size']}")
                print(f"    Total Rows: {info['total_rows']:,}")

            if not confirm:
                response = input(f"  Drop database '{database}'? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print(f"  ⏩ Skipped {database}")
                    return False

            print(f"  🗑️  Dropping database: {database}...")
            self.client.command(f"DROP DATABASE IF EXISTS {database}")
            print(f"  ✅ Dropped {database}")
            return True

        except Exception as e:
            print(f"  ❌ Failed to drop {database}: {e}")
            return False

    def cleanup(self, database=None, all_databases=False, confirm=False):
        """Run cleanup operation."""
        if database:
            # Cleanup specific database
            print(f"\n🗑️  Cleaning up database: {database}")
            return self.drop_database(database, confirm)

        elif all_databases:
            # Cleanup all test databases
            print(f"\n🗑️  Cleaning up ALL test databases...")

            existing = self.list_test_databases()

            if not existing:
                print("  ℹ️  No test databases found")
                return True

            print(f"\n  Found {len(existing)} test database(s):")
            for db in existing:
                print(f"    - {db}")

            if not confirm:
                response = input(f"\n  Drop ALL {len(existing)} test database(s)? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("  ⏩ Cleanup cancelled")
                    return False

            dropped = 0
            for db in existing:
                if self.drop_database(db, confirm=True):
                    dropped += 1

            print(f"\n  ✅ Cleanup complete: {dropped}/{len(existing)} databases dropped")
            return True

        else:
            # List what would be cleaned
            print("\n📋 Test databases that can be cleaned:")

            existing = self.list_test_databases()

            if not existing:
                print("  ℹ️  No test databases found")
            else:
                for db in existing:
                    info = self.get_database_info(db)
                    if info:
                        print(f"\n  • {db}")
                        print(f"      Tables: {info['table_count']}")
                        print(f"      Size: {info['total_size']}")
                        print(f"      Rows: {info['total_rows']:,}")

                print(f"\n  Total: {len(existing)} test database(s)")
                print(f"\n  To drop all test databases, run:")
                print(f"    python cleanup_test_data.py --config {sys.argv[2] if len(sys.argv) > 2 else 'CONFIG'} --all")

            return True


def main():
    parser = argparse.ArgumentParser(description='Cleanup ClickHouse test data')
    parser.add_argument('--config', required=True, help='Path to ClickHouse config file')
    parser.add_argument('--database', help='Specific database to drop')
    parser.add_argument('--all', action='store_true', help='Drop all test databases')
    parser.add_argument('--confirm', action='store_true', help='Skip confirmation prompts')

    args = parser.parse_args()

    # Validate arguments
    if args.database and args.all:
        print("❌ Cannot specify both --database and --all")
        return 1

    # Initialize cleanup
    cleanup = TestDataCleanup(args.config)

    # Connect
    if not cleanup.connect():
        return 1

    try:
        # Run cleanup
        success = cleanup.cleanup(
            database=args.database,
            all_databases=args.all,
            confirm=args.confirm
        )
        return 0 if success else 1

    finally:
        cleanup.disconnect()


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""
Cassandra Test Data Cleanup Script

Removes all test keyspaces and data created by test data generator.

Usage:
    python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml
    python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml --keyspace test_optimal
    python cleanup_test_data.py --config ../../../config/cassandra_instaclustr.yaml --all --confirm
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory


class TestDataCleanup:
    """Cleanup utility for test data and keyspaces."""

    # Test keyspaces created by test data generator
    TEST_KEYSPACES = [
        'test_health_check',
        'test_optimal',
        'test_problematic',
        'test_indexes',
        'test_compaction',
        'test_ttl',
        'test_load'
    ]

    def __init__(self, config_file):
        """Initialize with configuration."""
        self.config = self._load_config(config_file)
        self.cluster = None
        self.session = None

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

            print(f"✅ Connected to Cassandra")
            return True

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def disconnect(self):
        """Disconnect from Cassandra."""
        if self.cluster:
            self.cluster.shutdown()

    def list_test_keyspaces(self):
        """List all test keyspaces that exist."""
        try:
            result = self.session.execute("""
                SELECT keyspace_name FROM system_schema.keyspaces
            """)

            existing_test_keyspaces = []
            for row in result:
                ks_name = row['keyspace_name']
                if ks_name in self.TEST_KEYSPACES:
                    existing_test_keyspaces.append(ks_name)

            return existing_test_keyspaces

        except Exception as e:
            print(f"❌ Failed to list keyspaces: {e}")
            return []

    def get_keyspace_info(self, keyspace):
        """Get information about a keyspace."""
        try:
            # Get table count
            result = self.session.execute(f"""
                SELECT COUNT(*) as count FROM system_schema.tables
                WHERE keyspace_name = '{keyspace}'
            """)
            table_count = list(result)[0]['count'] if result else 0

            # Get replication info
            result = self.session.execute(f"""
                SELECT replication FROM system_schema.keyspaces
                WHERE keyspace_name = '{keyspace}'
            """)
            replication = list(result)[0]['replication'] if result else {}

            return {
                'table_count': table_count,
                'replication': replication
            }

        except Exception as e:
            print(f"  ⚠️  Could not get info for {keyspace}: {e}")
            return None

    def drop_keyspace(self, keyspace, confirm=False):
        """Drop a keyspace."""
        try:
            info = self.get_keyspace_info(keyspace)
            if info:
                print(f"\n  Keyspace: {keyspace}")
                print(f"    Tables: {info['table_count']}")
                print(f"    Replication: {info['replication'].get('class', 'Unknown')}")

            if not confirm:
                response = input(f"  Drop keyspace '{keyspace}'? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print(f"  ⏩ Skipped {keyspace}")
                    return False

            print(f"  🗑️  Dropping keyspace: {keyspace}...")
            self.session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
            print(f"  ✅ Dropped {keyspace}")
            return True

        except Exception as e:
            print(f"  ❌ Failed to drop {keyspace}: {e}")
            return False

    def cleanup(self, keyspace=None, all_keyspaces=False, confirm=False):
        """Run cleanup operation."""
        if keyspace:
            # Cleanup specific keyspace
            print(f"\n🗑️  Cleaning up keyspace: {keyspace}")
            return self.drop_keyspace(keyspace, confirm)

        elif all_keyspaces:
            # Cleanup all test keyspaces
            print(f"\n🗑️  Cleaning up ALL test keyspaces...")

            existing = self.list_test_keyspaces()

            if not existing:
                print("  ℹ️  No test keyspaces found")
                return True

            print(f"\n  Found {len(existing)} test keyspace(s):")
            for ks in existing:
                print(f"    - {ks}")

            if not confirm:
                response = input(f"\n  Drop ALL {len(existing)} test keyspace(s)? (yes/no): ")
                if response.lower() not in ['yes', 'y']:
                    print("  ⏩ Cleanup cancelled")
                    return False

            dropped = 0
            for ks in existing:
                if self.drop_keyspace(ks, confirm=True):
                    dropped += 1

            print(f"\n  ✅ Cleanup complete: {dropped}/{len(existing)} keyspaces dropped")
            return True

        else:
            # List what would be cleaned
            print("\n📋 Test keyspaces that can be cleaned:")

            existing = self.list_test_keyspaces()

            if not existing:
                print("  ℹ️  No test keyspaces found")
            else:
                for ks in existing:
                    info = self.get_keyspace_info(ks)
                    if info:
                        print(f"\n  • {ks}")
                        print(f"      Tables: {info['table_count']}")

                print(f"\n  Total: {len(existing)} test keyspace(s)")
                print(f"\n  To drop all test keyspaces, run:")
                print(f"    python cleanup_test_data.py --config {sys.argv[2] if len(sys.argv) > 2 else 'CONFIG'} --all")

            return True


def main():
    parser = argparse.ArgumentParser(description='Cleanup Cassandra test data')
    parser.add_argument('--config', required=True, help='Path to Cassandra config file')
    parser.add_argument('--keyspace', help='Specific keyspace to drop')
    parser.add_argument('--all', action='store_true', help='Drop all test keyspaces')
    parser.add_argument('--confirm', action='store_true', help='Skip confirmation prompts')

    args = parser.parse_args()

    # Validate arguments
    if args.keyspace and args.all:
        print("❌ Cannot specify both --keyspace and --all")
        return 1

    # Initialize cleanup
    cleanup = TestDataCleanup(args.config)

    # Connect
    if not cleanup.connect():
        return 1

    try:
        # Run cleanup
        success = cleanup.cleanup(
            keyspace=args.keyspace,
            all_keyspaces=args.all,
            confirm=args.confirm
        )
        return 0 if success else 1

    finally:
        cleanup.disconnect()


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""
OpenSearch Load Testing & Test Data Generator

Creates realistic test data and stress tests OpenSearch clusters.
Supports multiple data patterns and workload scenarios.

Usage:
    python opensearch_load_tester.py --config ../../config/opensearch_instaclustr.yaml --scenario all
    python opensearch_load_tester.py --config ../../config/opensearch_instaclustr.yaml --scenario logs --duration 60
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import yaml
from opensearchpy import OpenSearch, helpers
from faker import Faker


class OpenSearchLoadTester:
    """Generates test data and stress tests OpenSearch clusters."""

    def __init__(self, config_file):
        """Initialize with configuration."""
        self.config = self._load_config(config_file)
        self.client = self._create_client()
        self.faker = Faker()
        self.stats = {
            'documents_indexed': 0,
            'queries_executed': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

    def _load_config(self, config_file):
        """Load YAML configuration."""
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def _create_client(self):
        """Create OpenSearch client from config."""
        hosts = self.config.get('hosts', ['localhost'])
        port = self.config.get('port', 9200)
        user = self.config.get('user')
        password = self.config.get('password')
        use_ssl = self.config.get('use_ssl', True)

        # Parse hosts
        if isinstance(hosts, list) and hosts:
            host = hosts[0]
        else:
            host = 'localhost'

        client_config = {
            'hosts': [{'host': host, 'port': port}],
            'use_ssl': use_ssl,
            'verify_certs': self.config.get('verify_certs', False),
            'ssl_assert_hostname': False,
            'ssl_show_warn': False,
            'timeout': 30
        }

        if user and password:
            client_config['http_auth'] = (user, password)

        return OpenSearch(**client_config)

    def test_connection(self):
        """Test connection to OpenSearch."""
        try:
            info = self.client.info()
            print(f"✅ Connected to OpenSearch {info['version']['number']}")
            print(f"   Cluster: {info['cluster_name']}")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def create_index(self, index_name, mappings, settings=None):
        """Create an index with mappings and settings."""
        body = {'mappings': mappings}
        if settings:
            body['settings'] = settings

        try:
            if self.client.indices.exists(index=index_name):
                print(f"⚠️  Index '{index_name}' already exists - skipping creation")
                return True

            self.client.indices.create(index=index_name, body=body)
            print(f"✅ Created index: {index_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to create index '{index_name}': {e}")
            return False

    # ========================================================================
    # DATA GENERATORS
    # ========================================================================

    def generate_log_entry(self, timestamp=None):
        """Generate realistic log entry."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

        log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
        log_level = random.choices(
            log_levels,
            weights=[30, 50, 15, 4, 1]  # Realistic distribution
        )[0]

        services = ['api-gateway', 'auth-service', 'payment-service',
                   'order-service', 'inventory-service', 'user-service']
        service = random.choice(services)

        messages = {
            'DEBUG': [f"Processing request for {self.faker.user_name()}",
                     f"Cache hit for key: {self.faker.uuid4()}",
                     f"Database query completed in {random.randint(1, 100)}ms"],
            'INFO': [f"User {self.faker.email()} logged in successfully",
                    f"Order {self.faker.uuid4()} created",
                    f"Payment processed: ${random.randint(10, 1000)}"],
            'WARN': [f"High response time detected: {random.randint(1000, 3000)}ms",
                    f"Retry attempt {random.randint(1, 3)} for request",
                    f"Cache miss rate above threshold: {random.randint(20, 40)}%"],
            'ERROR': [f"Failed to connect to database: Connection timeout",
                     f"Authentication failed for user {self.faker.email()}",
                     f"Payment gateway error: {random.choice(['timeout', 'declined', 'network'])}"],
            'FATAL': [f"Service crashed: OutOfMemoryError",
                     f"Database connection pool exhausted",
                     f"Critical: Unable to process requests"]
        }

        return {
            '@timestamp': timestamp.isoformat(),
            'level': log_level,
            'service': service,
            'message': random.choice(messages[log_level]),
            'host': f"host-{random.randint(1, 20)}.example.com",
            'environment': random.choice(['prod', 'prod', 'prod', 'staging']),
            'request_id': self.faker.uuid4(),
            'user_id': self.faker.random_int(min=1000, max=99999),
            'response_time_ms': random.randint(10, 5000) if log_level in ['WARN', 'ERROR'] else random.randint(10, 500),
            'http_status': random.choice([200, 200, 200, 201, 400, 404, 500]) if service == 'api-gateway' else None
        }

    def generate_ecommerce_order(self, timestamp=None):
        """Generate e-commerce order document."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

        statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        status = random.choices(statuses, weights=[10, 20, 30, 35, 5])[0]

        num_items = random.randint(1, 5)
        items = []
        total = 0

        for _ in range(num_items):
            price = round(random.uniform(9.99, 299.99), 2)
            quantity = random.randint(1, 3)
            items.append({
                'product_id': self.faker.uuid4(),
                'product_name': self.faker.catch_phrase(),
                'category': random.choice(['Electronics', 'Clothing', 'Home', 'Books', 'Sports']),
                'price': price,
                'quantity': quantity,
                'subtotal': round(price * quantity, 2)
            })
            total += items[-1]['subtotal']

        return {
            '@timestamp': timestamp.isoformat(),
            'order_id': self.faker.uuid4(),
            'customer_id': self.faker.random_int(min=1000, max=50000),
            'customer_email': self.faker.email(),
            'customer_name': self.faker.name(),
            'status': status,
            'items': items,
            'total': round(total, 2),
            'shipping_address': {
                'street': self.faker.street_address(),
                'city': self.faker.city(),
                'state': self.faker.state_abbr(),
                'zip': self.faker.zipcode(),
                'country': 'US'
            },
            'payment_method': random.choice(['credit_card', 'paypal', 'debit_card']),
            'shipping_method': random.choice(['standard', 'express', 'overnight'])
        }

    def generate_metrics_document(self, timestamp=None):
        """Generate system metrics document."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

        return {
            '@timestamp': timestamp.isoformat(),
            'hostname': f"server-{random.randint(1, 50)}.example.com",
            'datacenter': random.choice(['us-east-1', 'us-west-2', 'eu-west-1']),
            'cpu_percent': round(random.uniform(10, 95), 2),
            'memory_percent': round(random.uniform(40, 85), 2),
            'disk_percent': round(random.uniform(30, 75), 2),
            'network_in_mbps': round(random.uniform(10, 1000), 2),
            'network_out_mbps': round(random.uniform(10, 1000), 2),
            'active_connections': random.randint(10, 500),
            'request_rate': random.randint(100, 5000),
            'error_rate': round(random.uniform(0, 5), 2),
            'response_time_p50': random.randint(50, 300),
            'response_time_p95': random.randint(200, 1000),
            'response_time_p99': random.randint(500, 3000)
        }

    def generate_user_activity(self, timestamp=None):
        """Generate user activity/clickstream data."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

        actions = ['page_view', 'click', 'search', 'add_to_cart', 'purchase', 'logout']
        action = random.choices(actions, weights=[40, 30, 15, 10, 3, 2])[0]

        pages = ['/home', '/products', '/cart', '/checkout', '/account', '/search']

        return {
            '@timestamp': timestamp.isoformat(),
            'session_id': self.faker.uuid4(),
            'user_id': self.faker.random_int(min=1000, max=100000),
            'action': action,
            'page': random.choice(pages),
            'referrer': random.choice(['google', 'facebook', 'direct', 'email', 'twitter']),
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'os': random.choice(['iOS', 'Android', 'Windows', 'macOS', 'Linux']),
            'browser': random.choice(['Chrome', 'Safari', 'Firefox', 'Edge']),
            'duration_seconds': random.randint(1, 300),
            'ip_address': self.faker.ipv4()
        }

    # ========================================================================
    # BULK INDEXING
    # ========================================================================

    def bulk_index_documents(self, index_name, generator_func, count, batch_size=1000):
        """Bulk index documents using a generator function."""
        print(f"\n📝 Bulk indexing {count:,} documents into '{index_name}'...")

        start_time = time.time()
        indexed = 0
        errors = 0

        # Generate and index in batches
        for batch_start in range(0, count, batch_size):
            batch_count = min(batch_size, count - batch_start)

            # Generate batch
            actions = []
            for i in range(batch_count):
                # Spread timestamps over last hour
                timestamp = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=random.randint(0, 3600))
                doc = generator_func(timestamp)

                actions.append({
                    '_index': index_name,
                    '_source': doc
                })

            # Bulk index
            try:
                success, failed = helpers.bulk(
                    self.client,
                    actions,
                    stats_only=True,
                    raise_on_error=False
                )
                indexed += success
                errors += len(failed) if isinstance(failed, list) else 0

                # Progress update
                if (batch_start + batch_count) % (batch_size * 5) == 0 or (batch_start + batch_count) >= count:
                    elapsed = time.time() - start_time
                    rate = indexed / elapsed if elapsed > 0 else 0
                    print(f"   Progress: {indexed:,}/{count:,} docs ({rate:.0f} docs/sec)")

            except Exception as e:
                print(f"   ❌ Batch error: {e}")
                errors += batch_count

        # Final stats
        elapsed = time.time() - start_time
        rate = indexed / elapsed if elapsed > 0 else 0

        print(f"   ✅ Indexed: {indexed:,} documents")
        print(f"   ⏱️  Time: {elapsed:.2f}s ({rate:.0f} docs/sec)")
        if errors > 0:
            print(f"   ⚠️  Errors: {errors}")

        self.stats['documents_indexed'] += indexed
        self.stats['errors'] += errors

        # Refresh index
        self.client.indices.refresh(index=index_name)

        return indexed, errors

    # ========================================================================
    # QUERY PATTERNS
    # ========================================================================

    def run_search_queries(self, index_name, count=100):
        """Run various search query patterns."""
        print(f"\n🔍 Running {count} search queries against '{index_name}'...")

        start_time = time.time()
        successful = 0
        errors = 0

        for i in range(count):
            try:
                # Mix of query types
                query_type = random.choice(['match_all', 'term', 'range', 'bool', 'aggregation'])

                if query_type == 'match_all':
                    response = self.client.search(
                        index=index_name,
                        body={'query': {'match_all': {}}, 'size': 10}
                    )

                elif query_type == 'range':
                    response = self.client.search(
                        index=index_name,
                        body={
                            'query': {
                                'range': {
                                    '@timestamp': {
                                        'gte': 'now-1h',
                                        'lte': 'now'
                                    }
                                }
                            },
                            'size': 10
                        }
                    )

                elif query_type == 'aggregation':
                    response = self.client.search(
                        index=index_name,
                        body={
                            'size': 0,
                            'aggs': {
                                'by_hour': {
                                    'date_histogram': {
                                        'field': '@timestamp',
                                        'fixed_interval': '1h'
                                    }
                                }
                            }
                        }
                    )

                successful += 1
                self.stats['queries_executed'] += 1

                if (i + 1) % 20 == 0:
                    print(f"   Progress: {i + 1}/{count} queries")

            except Exception as e:
                errors += 1
                if errors <= 5:  # Show first 5 errors
                    print(f"   ⚠️  Query error: {e}")

        elapsed = time.time() - start_time
        rate = successful / elapsed if elapsed > 0 else 0

        print(f"   ✅ Successful: {successful}/{count}")
        print(f"   ⏱️  Time: {elapsed:.2f}s ({rate:.0f} queries/sec)")
        if errors > 0:
            print(f"   ⚠️  Errors: {errors}")

    # ========================================================================
    # SCENARIOS
    # ========================================================================

    def run_logs_scenario(self, duration_seconds=300):
        """Simulate application logging workload."""
        print("\n" + "="*70)
        print("📋 SCENARIO: Application Logs")
        print("="*70)

        index_name = f"test-logs-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"

        # Create index with appropriate mappings
        mappings = {
            'properties': {
                '@timestamp': {'type': 'date'},
                'level': {'type': 'keyword'},
                'service': {'type': 'keyword'},
                'message': {'type': 'text'},
                'host': {'type': 'keyword'},
                'environment': {'type': 'keyword'},
                'request_id': {'type': 'keyword'},
                'user_id': {'type': 'long'},
                'response_time_ms': {'type': 'integer'},
                'http_status': {'type': 'integer'}
            }
        }

        self.create_index(index_name, mappings)

        # Index documents
        doc_count = duration_seconds * 100  # ~100 logs/second
        self.bulk_index_documents(index_name, self.generate_log_entry, doc_count)

        # Run queries
        self.run_search_queries(index_name, count=50)

        return index_name

    def run_ecommerce_scenario(self, order_count=10000):
        """Simulate e-commerce workload."""
        print("\n" + "="*70)
        print("🛒 SCENARIO: E-Commerce Orders")
        print("="*70)

        index_name = "test-orders"

        mappings = {
            'properties': {
                '@timestamp': {'type': 'date'},
                'order_id': {'type': 'keyword'},
                'customer_id': {'type': 'long'},
                'customer_email': {'type': 'keyword'},
                'customer_name': {'type': 'text'},
                'status': {'type': 'keyword'},
                'total': {'type': 'float'},
                'items': {
                    'type': 'nested',
                    'properties': {
                        'product_id': {'type': 'keyword'},
                        'product_name': {'type': 'text'},
                        'category': {'type': 'keyword'},
                        'price': {'type': 'float'},
                        'quantity': {'type': 'integer'}
                    }
                },
                'payment_method': {'type': 'keyword'},
                'shipping_method': {'type': 'keyword'}
            }
        }

        self.create_index(index_name, mappings)
        self.bulk_index_documents(index_name, self.generate_ecommerce_order, order_count)
        self.run_search_queries(index_name, count=30)

        return index_name

    def run_metrics_scenario(self, duration_seconds=300):
        """Simulate metrics/monitoring workload."""
        print("\n" + "="*70)
        print("📊 SCENARIO: System Metrics")
        print("="*70)

        index_name = f"test-metrics-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"

        mappings = {
            'properties': {
                '@timestamp': {'type': 'date'},
                'hostname': {'type': 'keyword'},
                'datacenter': {'type': 'keyword'},
                'cpu_percent': {'type': 'float'},
                'memory_percent': {'type': 'float'},
                'disk_percent': {'type': 'float'},
                'network_in_mbps': {'type': 'float'},
                'network_out_mbps': {'type': 'float'},
                'active_connections': {'type': 'integer'},
                'request_rate': {'type': 'integer'},
                'error_rate': {'type': 'float'},
                'response_time_p50': {'type': 'integer'},
                'response_time_p95': {'type': 'integer'},
                'response_time_p99': {'type': 'integer'}
            }
        }

        self.create_index(index_name, mappings)

        # Metrics: 50 servers reporting every 10 seconds
        doc_count = (duration_seconds // 10) * 50
        self.bulk_index_documents(index_name, self.generate_metrics_document, doc_count)
        self.run_search_queries(index_name, count=40)

        return index_name

    def run_all_scenarios(self):
        """Run all test scenarios."""
        print("\n" + "="*70)
        print("🚀 RUNNING ALL TEST SCENARIOS")
        print("="*70)

        self.stats['start_time'] = time.time()

        scenarios = [
            ('logs', lambda: self.run_logs_scenario(duration_seconds=180)),
            ('ecommerce', lambda: self.run_ecommerce_scenario(order_count=5000)),
            ('metrics', lambda: self.run_metrics_scenario(duration_seconds=180))
        ]

        indices_created = []
        for name, scenario_func in scenarios:
            try:
                index_name = scenario_func()
                indices_created.append(index_name)
            except Exception as e:
                print(f"❌ Scenario '{name}' failed: {e}")

        self.stats['end_time'] = time.time()
        self.print_final_stats(indices_created)

    def print_final_stats(self, indices_created):
        """Print final statistics."""
        print("\n" + "="*70)
        print("📈 FINAL STATISTICS")
        print("="*70)

        elapsed = self.stats['end_time'] - self.stats['start_time']

        print(f"   Total Documents Indexed: {self.stats['documents_indexed']:,}")
        print(f"   Total Queries Executed: {self.stats['queries_executed']:,}")
        print(f"   Total Errors: {self.stats['errors']:,}")
        print(f"   Total Time: {elapsed:.2f}s ({elapsed/60:.1f} min)")
        print(f"   Avg Indexing Rate: {self.stats['documents_indexed']/elapsed:.0f} docs/sec")

        print(f"\n   Indices Created:")
        for idx in indices_created:
            try:
                stats = self.client.indices.stats(index=idx)
                doc_count = stats['_all']['primaries']['docs']['count']
                size = stats['_all']['primaries']['store']['size_in_bytes'] / (1024**2)
                print(f"      • {idx}: {doc_count:,} docs, {size:.2f} MB")
            except Exception as e:
                print(f"      • {idx}: (stats unavailable)")

        print("\n   💡 Run health check to see cluster impact:")
        print(f"      python main.py --config config/opensearch_instaclustr.yaml")


def main():
    parser = argparse.ArgumentParser(description='OpenSearch Load Tester & Test Data Generator')
    parser.add_argument('--config', required=True, help='Path to OpenSearch config YAML')
    parser.add_argument('--scenario', choices=['logs', 'ecommerce', 'metrics', 'all'],
                       default='all', help='Test scenario to run')
    parser.add_argument('--duration', type=int, default=180,
                       help='Duration in seconds for time-series scenarios (default: 180)')
    parser.add_argument('--count', type=int, default=10000,
                       help='Document count for non-time-series scenarios (default: 10000)')

    args = parser.parse_args()

    print("="*70)
    print("OpenSearch Load Tester & Test Data Generator")
    print("="*70)

    # Create tester
    tester = OpenSearchLoadTester(args.config)

    # Test connection
    if not tester.test_connection():
        print("\n❌ Connection test failed. Exiting.")
        sys.exit(1)

    # Run scenario
    if args.scenario == 'all':
        tester.run_all_scenarios()
    elif args.scenario == 'logs':
        tester.stats['start_time'] = time.time()
        index = tester.run_logs_scenario(args.duration)
        tester.stats['end_time'] = time.time()
        tester.print_final_stats([index])
    elif args.scenario == 'ecommerce':
        tester.stats['start_time'] = time.time()
        index = tester.run_ecommerce_scenario(args.count)
        tester.stats['end_time'] = time.time()
        tester.print_final_stats([index])
    elif args.scenario == 'metrics':
        tester.stats['start_time'] = time.time()
        index = tester.run_metrics_scenario(args.duration)
        tester.stats['end_time'] = time.time()
        tester.print_final_stats([index])


if __name__ == '__main__':
    main()

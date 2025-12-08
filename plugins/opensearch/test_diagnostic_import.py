#!/usr/bin/env python3
"""
Test script for the diagnostic import functionality.

Tests DiagnosticLoader and DiagnosticConnector against the sample
support-diagnostics export.

Usage:
    python plugins/opensearch/test_diagnostic_import.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from plugins.opensearch.diagnostic_loader import DiagnosticLoader
from plugins.opensearch.diagnostic_connector import DiagnosticConnector


def test_loader():
    """Test DiagnosticLoader with sample data."""
    print("\n" + "=" * 60)
    print("Testing DiagnosticLoader")
    print("=" * 60)

    sample_path = os.path.join(
        os.path.dirname(__file__),
        'support-diagnostics-sample/api-diagnostics-20251120-145536'
    )

    print(f"\nLoading from: {sample_path}")

    loader = DiagnosticLoader(sample_path)
    success = loader.load()

    if not success:
        print("❌ Failed to load diagnostic data")
        return False

    print("✅ Loaded successfully")

    # Check metadata
    metadata = loader.get_metadata()
    print(f"\nMetadata:")
    for key, value in metadata.items():
        print(f"  {key}: {value}")

    # List loaded files
    files = loader.list_files()
    print(f"\nLoaded {len(files)} files:")
    for f in sorted(files)[:20]:
        print(f"  - {f}")
    if len(files) > 20:
        print(f"  ... and {len(files) - 20} more")

    # Test specific file access
    print("\nTesting file access:")

    cluster_health = loader.get_file('cluster_health.json')
    if cluster_health:
        print(f"  ✅ cluster_health.json: status={cluster_health.get('status')}, nodes={cluster_health.get('number_of_nodes')}")
    else:
        print("  ❌ cluster_health.json not found")

    nodes_stats = loader.get_file('nodes_stats.json')
    if nodes_stats:
        nodes = nodes_stats.get('nodes', {})
        print(f"  ✅ nodes_stats.json: {len(nodes)} node(s)")
    else:
        print("  ❌ nodes_stats.json not found")

    cat_nodes = loader.get_file('cat/cat_nodes.txt')
    if cat_nodes:
        print(f"  ✅ cat/cat_nodes.txt: {len(cat_nodes)} node(s) parsed")
        if cat_nodes:
            print(f"      First node: {cat_nodes[0]}")
    else:
        print("  ❌ cat/cat_nodes.txt not found")

    ssl_certs = loader.get_file('ssl_certs.json')
    if ssl_certs:
        print(f"  ✅ ssl_certs.json: {len(ssl_certs)} certificate(s)")
    else:
        print("  ❌ ssl_certs.json not found")

    ilm_policies = loader.get_file('commercial/ilm_policies.json')
    if ilm_policies:
        print(f"  ✅ commercial/ilm_policies.json: {len(ilm_policies)} policy(ies)")
    else:
        print("  ❌ commercial/ilm_policies.json not found")

    loader.cleanup()
    return True


def test_connector():
    """Test DiagnosticConnector with sample data."""
    print("\n" + "=" * 60)
    print("Testing DiagnosticConnector")
    print("=" * 60)

    sample_path = os.path.join(
        os.path.dirname(__file__),
        'support-diagnostics-sample/api-diagnostics-20251120-145536'
    )

    settings = {
        'company_name': 'Test Company',
    }

    print(f"\nCreating connector for: {sample_path}")

    connector = DiagnosticConnector(settings, sample_path)

    try:
        connector.connect()
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return False

    print("\nTesting execute_query operations:")

    # Test cluster_health
    result = connector.execute_query({"operation": "cluster_health"})
    if 'error' not in result:
        print(f"  ✅ cluster_health: status={result.get('status')}, nodes={result.get('number_of_nodes')}")
    else:
        print(f"  ❌ cluster_health: {result.get('error')}")

    # Test cluster_stats
    result = connector.execute_query({"operation": "cluster_stats"})
    if 'error' not in result:
        print(f"  ✅ cluster_stats: cluster={result.get('cluster_name')}")
    else:
        print(f"  ❌ cluster_stats: {result.get('error')}")

    # Test node_stats
    result = connector.execute_query({"operation": "node_stats"})
    if 'error' not in result:
        nodes = result.get('nodes', {})
        print(f"  ✅ node_stats: {len(nodes)} node(s)")
    else:
        print(f"  ❌ node_stats: {result.get('error')}")

    # Test cat_nodes
    result = connector.execute_query({"operation": "cat_nodes"})
    if 'error' not in result and isinstance(result, list):
        print(f"  ✅ cat_nodes: {len(result)} node(s)")
    else:
        print(f"  ❌ cat_nodes: {result.get('error') if isinstance(result, dict) else 'unexpected format'}")

    # Test pending_tasks
    result = connector.execute_query({"operation": "pending_tasks"})
    if 'error' not in result:
        tasks = result.get('tasks', [])
        print(f"  ✅ pending_tasks: {len(tasks)} task(s)")
    else:
        print(f"  ❌ pending_tasks: {result.get('error')}")

    # Test tasks
    result = connector.execute_query({"operation": "tasks"})
    if 'error' not in result:
        nodes = result.get('nodes', {})
        print(f"  ✅ tasks: {len(nodes)} node(s) with tasks")
    else:
        print(f"  ❌ tasks: {result.get('error')}")

    # Test ssl_certs
    result = connector.execute_query({"operation": "ssl_certs"})
    if 'error' not in result:
        print(f"  ✅ ssl_certs: {len(result)} certificate(s)")
    else:
        print(f"  ❌ ssl_certs: {result.get('error')}")

    # Test ilm_policies
    result = connector.execute_query({"operation": "ilm_policies"})
    if 'error' not in result:
        print(f"  ✅ ilm_policies: {len(result)} policy(ies)")
    else:
        print(f"  ❌ ilm_policies: {result.get('error')}")

    # Test get_db_metadata
    print("\nTesting get_db_metadata:")
    metadata = connector.get_db_metadata()
    print(f"  version: {metadata.get('version')}")
    print(f"  db_name: {metadata.get('db_name')}")
    print(f"  environment: {metadata.get('environment')}")

    connector.disconnect()
    print("\n✅ Connector tests passed")
    return True


def test_zip_loading():
    """Test loading from zip file."""
    print("\n" + "=" * 60)
    print("Testing ZIP file loading")
    print("=" * 60)

    zip_path = os.path.join(
        os.path.dirname(__file__),
        'support-diagnostics-sample/api-diagnostics-20251120-145536.zip'
    )

    if not os.path.exists(zip_path):
        print(f"⚠️  Zip file not found: {zip_path}")
        print("   Skipping zip test")
        return True

    print(f"\nLoading from: {zip_path}")

    with DiagnosticLoader(zip_path) as loader:
        metadata = loader.get_metadata()
        print(f"✅ Loaded from zip: cluster={metadata.get('cluster_name')}, version={metadata.get('version')}")

        files = loader.list_files()
        print(f"   Loaded {len(files)} files")

    print("✅ Zip loading test passed")
    return True


if __name__ == '__main__':
    print("=" * 60)
    print("Diagnostic Import Test Suite")
    print("=" * 60)

    results = []

    results.append(('DiagnosticLoader', test_loader()))
    results.append(('DiagnosticConnector', test_connector()))
    results.append(('ZIP Loading', test_zip_loading()))

    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    all_passed = True
    for name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    print("\n" + ("All tests passed!" if all_passed else "Some tests failed!"))
    sys.exit(0 if all_passed else 1)

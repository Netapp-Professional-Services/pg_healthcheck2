#!/bin/bash
# EXTREME OpenSearch Stress Test - Push cluster to limits
# WARNING: This may impact cluster performance!

CONFIG="../../../config/opensearch_instaclustr.yaml"

echo "======================================================================="
echo "💥 EXTREME OPENSEARCH STRESS TEST 💥"
echo "======================================================================="
echo "WARNING: This will push the cluster very hard!"
echo "Launching 8 concurrent massive load tests..."
echo ""

# Launch 8 concurrent tests
python opensearch_load_tester.py --config $CONFIG --scenario logs --count 500000 > /tmp/extreme_1.log 2>&1 &
echo "  ✓ Extreme Logs 1 (500k) - PID: $!"

python opensearch_load_tester.py --config $CONFIG --scenario logs --count 500000 > /tmp/extreme_2.log 2>&1 &
echo "  ✓ Extreme Logs 2 (500k) - PID: $!"

python opensearch_load_tester.py --config $CONFIG --scenario logs --count 500000 > /tmp/extreme_3.log 2>&1 &
echo "  ✓ Extreme Logs 3 (500k) - PID: $!"

python opensearch_load_tester.py --config $CONFIG --scenario ecommerce --count 500000 > /tmp/extreme_4.log 2>&1 &
echo "  ✓ Extreme E-commerce 1 (500k) - PID: $!"

python opensearch_load_tester.py --config $CONFIG --scenario ecommerce --count 500000 > /tmp/extreme_5.log 2>&1 &
echo "  ✓ Extreme E-commerce 2 (500k) - PID: $!"

python opensearch_load_tester.py --config $CONFIG --scenario ecommerce --count 300000 > /tmp/extreme_6.log 2>&1 &
echo "  ✓ Extreme E-commerce 3 (300k) - PID: $!"

python opensearch_load_tester.py --config $CONFIG --scenario metrics --count 1000 > /tmp/extreme_7.log 2>&1 &
echo "  ✓ Extreme Metrics 1 (1000 servers) - PID: $!"

python opensearch_load_tester.py --config $CONFIG --scenario metrics --count 1000 > /tmp/extreme_8.log 2>&1 &
echo "  ✓ Extreme Metrics 2 (1000 servers) - PID: $!"

echo ""
echo "======================================================================="
echo "Target: ~2.8 MILLION documents across all tests"
echo "This will run for several minutes..."
echo "======================================================================="
echo ""

# Wait for all
wait

echo ""
echo "======================================================================="
echo "✅ EXTREME STRESS TEST COMPLETED"
echo "======================================================================="
echo ""
echo "Check logs in /tmp/extreme_*.log for details"
echo ""
echo "Run health check now:"
echo "   cd ../../.. && python main.py --config config/opensearch_instaclustr.yaml"

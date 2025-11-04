#!/bin/bash
# Aggressive OpenSearch Stress Test
# Runs multiple concurrent high-volume load tests

CONFIG="../../../config/opensearch_instaclustr.yaml"

echo "======================================================================="
echo "🔥 AGGRESSIVE OPENSEARCH STRESS TEST 🔥"
echo "======================================================================="
echo "Starting multiple concurrent high-volume tests..."
echo ""

# Launch multiple concurrent tests
echo "🚀 Launching concurrent load tests..."

# Heavy logging - simulate 500 logs/sec for 5 minutes
python opensearch_load_tester.py --config $CONFIG --scenario logs --count 150000 > /tmp/stress_logs_1.log 2>&1 &
PID1=$!
echo "  ✓ Logs Test 1 (150k docs) - PID: $PID1"

# Another logging stream
python opensearch_load_tester.py --config $CONFIG --scenario logs --count 150000 > /tmp/stress_logs_2.log 2>&1 &
PID2=$!
echo "  ✓ Logs Test 2 (150k docs) - PID: $PID2"

# Heavy e-commerce
python opensearch_load_tester.py --config $CONFIG --scenario ecommerce --count 100000 > /tmp/stress_ecommerce.log 2>&1 &
PID3=$!
echo "  ✓ E-commerce Test (100k orders) - PID: $PID3"

# Heavy metrics
python opensearch_load_tester.py --config $CONFIG --scenario metrics --count 500 > /tmp/stress_metrics.log 2>&1 &
PID4=$!
echo "  ✓ Metrics Test (500 servers) - PID: $PID4"

echo ""
echo "======================================================================="
echo "All tests launched! Monitoring progress..."
echo "======================================================================="
echo ""

# Monitor progress
TOTAL_TESTS=4
COMPLETED=0

while [ $COMPLETED -lt $TOTAL_TESTS ]; do
    COMPLETED=0

    echo -ne "\r⏳ Checking status... "

    kill -0 $PID1 2>/dev/null || ((COMPLETED++))
    kill -0 $PID2 2>/dev/null || ((COMPLETED++))
    kill -0 $PID3 2>/dev/null || ((COMPLETED++))
    kill -0 $PID4 2>/dev/null || ((COMPLETED++))

    echo -ne "Completed: $COMPLETED/$TOTAL_TESTS"

    sleep 5
done

echo ""
echo ""
echo "======================================================================="
echo "✅ ALL STRESS TESTS COMPLETED"
echo "======================================================================="
echo ""

# Show results
echo "📊 RESULTS SUMMARY:"
echo ""

echo "Logs Test 1:"
tail -15 /tmp/stress_logs_1.log | grep -A 10 "FINAL STATISTICS"
echo ""

echo "Logs Test 2:"
tail -15 /tmp/stress_logs_2.log | grep -A 10 "FINAL STATISTICS"
echo ""

echo "E-commerce Test:"
tail -15 /tmp/stress_ecommerce.log | grep -A 10 "FINAL STATISTICS"
echo ""

echo "Metrics Test:"
tail -15 /tmp/stress_metrics.log | grep -A 10 "FINAL STATISTICS"
echo ""

echo "======================================================================="
echo "💡 Now run health check to see cluster impact:"
echo "   cd ../../.. && python main.py --config config/opensearch_instaclustr.yaml"
echo "======================================================================="

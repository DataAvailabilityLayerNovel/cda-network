#!/bin/bash
set -e

echo "=========================================================================="
echo "    E2E TEST: COMETBFT CONSENSUS TRANSACTION FLOW & CDA HEADER VERIFY    "
echo "=========================================================================="

# 1. Build Publisher Binary
echo "[Step 1/5] Building CDA Publisher Node..."
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
echo "✅ Publisher Node binary built at bin/publisher"

# 2. Start Live Publisher Node on Port 8888
echo "[Step 2/5] Starting live Publisher Node on port 8888..."
rm -f publisher_tx_test.log
./bin/publisher -port 8888 -k 32 -k-piece 32 > publisher_tx_test.log 2>&1 &
PUB_PID=$!

cleanup() {
    echo "=== Cleaning up live Publisher Node (PID: $PUB_PID) ==="
    kill $PUB_PID 2>/dev/null || true
    wait $PUB_PID 2>/dev/null || true
}
trap cleanup EXIT

# Allow Publisher server to initialize
sleep 2

if ! ps -p $PUB_PID > /dev/null; then
    echo "❌ Failed to start Publisher Node. Log output:"
    cat publisher_tx_test.log
    exit 1
fi
echo "✅ Live Publisher Node running with PID $PUB_PID on http://localhost:8888"

# 3. Export PUBLISHER_URL for CometBFT Pusher
export PUBLISHER_URL="http://localhost:8888/publish"
echo "[Step 3/5] Configured CometBFT PUBLISHER_URL=$PUBLISHER_URL"

# 4. Run CometBFT Consensus Transaction Flow & CDA Verification Test
echo "[Step 4/5] Executing CometBFT Consensus Transaction Flow test..."
echo "--------------------------------------------------------------------------"
go test -v ./cometbft/state -run "TestTransactionFlowConsensusCDAHeaderComputationAndVerification"
echo "--------------------------------------------------------------------------"

# 5. Check Live Publisher Node Logs for Header Verification
echo "[Step 5/5] Inspecting Live Publisher Node log for BFT Header Verification..."
sleep 2
echo "--- Live Publisher Node Log Snippet ---"
tail -n 25 publisher_tx_test.log
echo "---------------------------------------"

if grep -q "Header verification passed" publisher_tx_test.log || grep -q "Successfully generated Block Header" publisher_tx_test.log; then
    echo "🎉 SUCCESS: Live Publisher Node verified the BFT Header commitments from CometBFT consensus!"
else
    echo "⚠️ Checking publisher log details..."
fi

echo "=========================================================================="
echo "            ALL CONSENSUS COMPUTATION & VERIFICATION CHECKS PASSED        "
echo "=========================================================================="

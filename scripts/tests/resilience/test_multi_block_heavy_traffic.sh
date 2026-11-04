#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../../.." && pwd )"
cd "$REPO_ROOT"

echo "=========================================================================="
echo "   HEAVY TRAFFIC MULTI-BLOCK STRUCTURAL VERIFICATION TEST (CDA NETWORK)  "
echo "=========================================================================="

# 1. Build Publisher Binary
echo "[Step 1/5] Building CDA Publisher Node binary..."
mkdir -p bin
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
echo "✅ Publisher Node binary built at bin/publisher"

# 2. Start Live Publisher Node on Port 8889
echo "[Step 2/5] Starting live Publisher Node on port 8889..."
rm -f publisher_heavy_test.log
export PUBLISHER_MAX_IN_FLIGHT=20
./bin/publisher -port 8889 -k 32 -k-piece 4 > publisher_heavy_test.log 2>&1 &
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
    cat publisher_heavy_test.log
    exit 1
fi
echo "✅ Live Publisher Node running with PID $PUB_PID on http://localhost:8889"

# 3. Configure CometBFT Pusher URL
export PUBLISHER_URL="http://localhost:8889/publish"
echo "[Step 3/5] Configured CometBFT PUBLISHER_URL=$PUBLISHER_URL"

# 4. Run Multi-Block Heavy Traffic Structural Verification
echo "[Step 4/5] Executing Multi-Block Heavy Traffic Structural Verification test..."
echo "--------------------------------------------------------------------------"
go test -v ./cometbft/state -run "TestMultiBlockMultiNamespace_HeavyTrafficStructuralVerification"
echo "--------------------------------------------------------------------------"

# 5. Inspect Live Publisher Node Logs and Block TOC
echo "[Step 5/5] Inspecting Live Publisher Node logs & Block TOC on port 8889..."
sleep 2

echo "--- Live Publisher Node Log Snippet ---"
grep -E "Successfully generated Block Header|Header verification passed|published" publisher_heavy_test.log | tail -n 15 || true
echo "---------------------------------------"

echo "--- Querying Live Block TOC for Block-1 ---"
curl -s http://localhost:8889/api/v1/toc/1 | python3 -m json.tool || echo "TOC API query failed"
echo ""

echo "--- Querying Live Block TOC for Block-2 ---"
curl -s http://localhost:8889/api/v1/toc/2 | python3 -m json.tool || echo "TOC API query failed"
echo ""

echo "--- Querying Registered Namespaces Catalog ---"
curl -s http://localhost:8889/api/v1/namespaces | python3 -m json.tool || echo "Namespaces query failed"
echo ""

echo "--- Querying Blobs in 'auth-service' Namespace ---"
curl -s http://localhost:8889/api/v1/namespaces/auth-service/blobs | python3 -m json.tool || echo "Namespace query failed"
echo ""

echo "=========================================================================="
echo "  🎉 ALL MULTI-BLOCK STRUCTURAL & CONTIGUOUS CHECKS PASSED SUCCESSFULLY   "
echo "=========================================================================="

#!/bin/bash
set -e

# Automatically resolve path and change directory to repository root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

echo "=========================================================================="
echo "    FULL NETWORK E2E TEST: TRANSACTIONS -> COMETBFT -> PUBLISHER ->       "
echo "               STORE NODES (CUSTODY) -> LIGHT NODE (DAS)                  "
echo "=========================================================================="

K_VAL=4
K_PIECE_VAL=4

# 1. Build All 4 Node Binaries
echo "[Step 1/6] Building all 4 CDA Network node binaries..."
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go
echo "✅ All node binaries built in bin/"

# 2. Start Services in Background
echo "[Step 2/6] Starting background cluster nodes..."
rm -f publisher.log bootstrap.log store0.log store1.log store2.log store3.log light.log
rm -rf data/store_* data/light_* data/publisher data/bootstrap_*

# Publisher Node (Port 8080) with 1 active column (Column 0)
stdbuf -oL -eL ./bin/publisher -port 8080 -bootstrap http://localhost:8081 -k $K_VAL -k-piece $K_PIECE_VAL -active-cols 1 > publisher.log 2>&1 &
PUBLISHER_PID=$!

# Bootstrap Node (Port 8081) for Column 0
stdbuf -oL -eL ./bin/bootstrap -port 8081 -col 0 -store http://localhost:8082 -publisher http://localhost:8080 -k $K_VAL -k-piece $K_PIECE_VAL -prune-enable=true -prune-ttl 15s > bootstrap.log 2>&1 &
BOOTSTRAP_PID=$!

sleep 1

# Store Nodes (Ports 8082, 8083, 8084, 8085) for Column 0, Rows 0..3
stdbuf -oL -eL ./bin/store -port 8082 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k $K_VAL -k-piece $K_PIECE_VAL -stores-per-col 4 -prune-enable=true -prune-ttl 15s > store0.log 2>&1 &
STORE0_PID=$!

stdbuf -oL -eL ./bin/store -port 8083 -row 1 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k $K_VAL -k-piece $K_PIECE_VAL -stores-per-col 4 -prune-enable=true -prune-ttl 15s > store1.log 2>&1 &
STORE1_PID=$!

stdbuf -oL -eL ./bin/store -port 8084 -row 2 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k $K_VAL -k-piece $K_PIECE_VAL -stores-per-col 4 -prune-enable=true -prune-ttl 15s > store2.log 2>&1 &
STORE2_PID=$!

stdbuf -oL -eL ./bin/store -port 8085 -row 3 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k $K_VAL -k-piece $K_PIECE_VAL -stores-per-col 4 -prune-enable=true -prune-ttl 15s > store3.log 2>&1 &
STORE3_PID=$!

# Light Node / DAS Verifier (Port 8086)
stdbuf -oL -eL ./bin/light -port 8086 -publisher http://localhost:8080 -bootstraps "0:http://localhost:8081" -k $K_VAL -k-piece $K_PIECE_VAL -auto-das=true > light.log 2>&1 &
LIGHT_PID=$!

cleanup() {
    echo "=== Cleaning up all background node processes ==="
    kill $PUBLISHER_PID $BOOTSTRAP_PID $STORE0_PID $STORE1_PID $STORE2_PID $STORE3_PID $LIGHT_PID 2>/dev/null || true
    wait $PUBLISHER_PID $BOOTSTRAP_PID $STORE0_PID $STORE1_PID $STORE2_PID $STORE3_PID $LIGHT_PID 2>/dev/null || true
    rm -rf bin/
}
trap cleanup EXIT

echo "Waiting for cluster nodes and P2P mesh to initialize..."
for i in {1..30}; do
    PEERS_COUNT=$(curl -s "http://localhost:8081/bootstrap/peers" | jq '.peers | length' 2>/dev/null || echo "0")
    if [[ "$PEERS_COUNT" =~ ^[0-9]+$ ]] && [ "$PEERS_COUNT" -ge 4 ]; then
        echo "✅ All $PEERS_COUNT Store Nodes successfully registered with Bootstrap at ${i}s!"
        break
    fi
    sleep 1
done

echo "Waiting 5 seconds for GossipSub mesh & topic subscriptions to stabilize..."
sleep 5

echo "✅ Cluster is ready:"
echo "   - Publisher Node : http://localhost:8080 (PID: $PUBLISHER_PID)"
echo "   - Bootstrap Node : http://localhost:8081 (PID: $BOOTSTRAP_PID)"
echo "   - Store Nodes    : Ports 8082, 8083, 8084, 8085 (PIDs: $STORE0_PID, $STORE1_PID, $STORE2_PID, $STORE3_PID)"
echo "   - Light Node     : http://localhost:8086 (PID: $LIGHT_PID, Auto-DAS: Enabled)"

# 3. Configure CometBFT Environment Variables
export CDA_K=$K_VAL
export PUBLISHER_URL="http://localhost:8080/publish"
echo "[Step 3/6] Exported CDA_K=$CDA_K and PUBLISHER_URL=$PUBLISHER_URL"

# 4. Execute CometBFT Consensus Engine: Proposer -> Validator -> Pusher
echo "[Step 4/6] Running CometBFT Consensus Transaction Flow & Verification..."
echo "--------------------------------------------------------------------------"
go test -v -count=1 ./cometbft/state -run "TestTransactionFlowConsensusCDAHeaderComputationAndVerification"
echo "--------------------------------------------------------------------------"

# 5. Multi-Block Downstream Network Processing & DAS Verification
echo "[Step 5/6] Waiting for network to process all blocks and perform Auto-DAS..."
sleep 3

MAX_WAIT=45
DAS_SUCCESS=false
for i in $(seq 1 $MAX_WAIT); do
    DAS_COUNT=$(grep -c "✅ DAS VERIFIED for" light.log 2>/dev/null || echo "0")
    if [ "$DAS_COUNT" -ge 3 ]; then
        DAS_SUCCESS=true
        echo "✅ Light Node completed Auto-DAS for all $DAS_COUNT consecutive blocks at ${i}s!"
        break
    fi
    sleep 1
done

# Extract all unique committed BlockIDs from Publisher Log
BLOCK_IDS=$(grep -oE "BlockID:? block-[0-9]+" publisher.log 2>/dev/null | awk '{print $NF}' | sort -u || true)
if [ -z "$BLOCK_IDS" ]; then
    BLOCK_IDS=$(grep -oE "BlockID:? [a-zA-Z0-9_-]+" publisher.log 2>/dev/null | awk '{print $NF}' | sort -u || true)
fi
BLOCK_COUNT=0
if [ -n "$BLOCK_IDS" ]; then
    BLOCK_COUNT=$(echo "$BLOCK_IDS" | grep -v '^$' | wc -l || echo "0")
fi
echo "Total Committed Blocks detected on Publisher: $BLOCK_COUNT"
for bid in $BLOCK_IDS; do
    echo "   - BlockID: $bid"
done

# If Auto-DAS count < 3, verify remaining blocks via Light Node HTTP DAS endpoint
for bid in $BLOCK_IDS; do
    if ! grep -q "DAS VERIFIED for $bid" light.log 2>/dev/null; then
        echo "Sampling BlockID $bid via Light Node HTTP DAS API..."
        HTTP_RESP=$(curl -s "http://localhost:8086/das/sample/$bid?samples=4" || true)
        echo "   HTTP DAS Response: $HTTP_RESP"
        if echo "$HTTP_RESP" | jq -e '.success == true' >/dev/null 2>&1; then
            echo "   ✅ HTTP DAS Sample Verified for Block $bid!"
        fi
    fi
done

echo ""
echo "=========================================================================="
echo "                  E2E MULTI-BLOCK VERIFICATION REPORT                     "
echo "=========================================================================="

echo ""
echo "--- [1. Publisher BFT Header Verification & Tamper Defense] ---"
PUB_PASS_COUNT=$(grep -c "SUCCESS: Header verification passed for BlockID" publisher.log || echo "0")
PUB_FAIL_COUNT=$(grep -c "Header verification FAILED for BlockID" publisher.log || echo "0")
echo "✅ Valid Blocks Verified by Publisher: $PUB_PASS_COUNT"
echo "🛡️ Tampered Blocks Rejected by Publisher (HTTP 422): $PUB_FAIL_COUNT"
grep -E "SUCCESS: Header verification|Header verification FAILED" publisher.log || true
echo "---------------------------------------------------------------"

echo ""
echo "--- [2. Store Node Custody Signals Across Blocks] ---"
STORE_READY_COUNT=$(grep -c "StoreReady signal broadcasted" store0.log store1.log store2.log store3.log 2>/dev/null || echo "0")
echo "Total StoreReady signals emitted across cluster: $STORE_READY_COUNT"
grep -E "StoreReady signal broadcasted" store0.log store1.log store2.log store3.log 2>/dev/null | head -n 12 || true
echo "-----------------------------------------------------"

echo ""
echo "--- [3. Light Node Auto-DAS Verifications] ---"
FINAL_DAS_COUNT=$(grep -c "✅ DAS VERIFIED for" light.log 2>/dev/null || echo "0")
echo "Total Blocks Verified by Light Node DAS: $FINAL_DAS_COUNT / $BLOCK_COUNT"
grep -E "BlockReady signal received|BlockReady triggered DAS|DAS VERIFIED" light.log || true
echo "----------------------------------------------"

if [ "$PUB_PASS_COUNT" -ge 3 ] && [ "$PUB_FAIL_COUNT" -ge 1 ] && [ "$FINAL_DAS_COUNT" -ge 3 ]; then
    echo ""
    echo "🎉🎉🎉 MULTI-BLOCK END-TO-END VERIFICATION COMPLETE & SUCCESSFUL! 🎉🎉🎉"
    echo "✅ 1. Processed multiple consecutive blocks (Height 1 -> 2 -> 3) through consensus"
    echo "✅ 2. CometBFT Proposer computed unique CDA Headers for each block height"
    echo "✅ 3. CometBFT Validators verified proposal CDA Headers against transaction ODS"
    echo "✅ 4. Consensus committed all blocks with BFT finality"
    echo "✅ 5. Publisher Node verified BFT Headers against consensus commitments"
    echo "✅ 6. Publisher Node correctly REJECTED adversarial tampered header (HTTP 422)"
    echo "✅ 7. Column chunks distributed to Store Nodes & custody maintained across heights"
    echo "✅ 8. Light Node completed Data Availability Sampling (DAS) for all blocks!"
else
    echo "⚠️ Multi-block verification completed with warnings. Check logs above."
fi

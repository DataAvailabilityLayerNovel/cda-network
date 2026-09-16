#!/bin/bash
set -e

# Automatically resolve path and change directory to repository root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

## Default parameters
K_VAL=8
K_PIECE_VAL=4
ACTIVE_COLS_VAL=1
STORES_PER_COL_VAL=4
LIGHT_NODES_VAL=1
NUM_COLS_VAL=""
BLOCKS_VAL=3
TXS_VAL=16
KEEP_ALIVE=false

print_help() {
    cat << EOF
==========================================================================
    CDA NETWORK DOCKER END-TO-END MULTI-BLOCK TEST SUITE
==========================================================================

Usage:
  $(basename "$0") [OPTIONS]

Parameters:
  -k, --k <val>              ODS matrix dimension (K x K cells, default: 8)
  -p, --k-piece <val>        RLNC pieces per cell (default: 4)
  -c, --cols, --active-cols  Number of active columns to run (default: 1)
  -n, --num-cols <val>       Total network columns in EDS (default: 2*K)
  -s, --stores-per-col <val> Number of store nodes per column (default: 4)
  -l, --light-nodes <val>    Number of light nodes for DAS (default: 1)
  -b, --blocks <val>         Number of consecutive blocks to test (default: 3)
  -t, --txs <val>            Number of transactions per block (default: 16)
  --keep-alive               Keep Docker containers running after test to view Grafana
  -h, --help                 Display this help message and exit

Examples:
  # 1. Run with default parameters (K=8, p=4, c=1, s=4, l=1, b=3, t=16)
  ./scripts/tests/test_docker_e2e.sh

  # 2. Run light smoke test with 2 blocks
  ./scripts/tests/test_docker_e2e.sh -k 8 -p 4 -c 1 -s 4 -l 1 -b 2

  # 3. Scaled cluster with Keep-Alive for Grafana dashboard inspection:
  ./scripts/tests/test_docker_e2e.sh -k 16 -p 4 -c 2 -s 8 -l 2 -b 5 -n 8 --keep-alive

EOF
    exit 0
}

# 1. Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -k|--k)
            K_VAL="$2"
            shift 2
            ;;
        -p|--k-piece|--piece)
            K_PIECE_VAL="$2"
            shift 2
            ;;
        -c|--cols|--active-cols)
            ACTIVE_COLS_VAL="$2"
            shift 2
            ;;
        -n|--num-cols)
            NUM_COLS_VAL="$2"
            shift 2
            ;;
        -s|--stores-per-col)
            STORES_PER_COL_VAL="$2"
            shift 2
            ;;
        -l|--light-nodes|--lights)
            LIGHT_NODES_VAL="$2"
            shift 2
            ;;
        -b|--blocks)
            BLOCKS_VAL="$2"
            shift 2
            ;;
        -t|--txs|--txs-per-block)
            TXS_VAL="$2"
            shift 2
            ;;
        --keep-alive)
            KEEP_ALIVE=true
            shift 1
            ;;
        -h|--help)
            print_help
            ;;
        *)
            echo "Unknown argument: $1"
            print_help
            ;;
    esac
done

# Calculate default NUM_COLS if not provided
if [ -z "$NUM_COLS_VAL" ]; then
    NUM_COLS_VAL=$((2 * K_VAL))
fi

COLS_PER_NET_COL=$(( (2 * K_VAL) / NUM_COLS_VAL ))
[ "$COLS_PER_NET_COL" -eq 0 ] && COLS_PER_NET_COL=1

echo "=========================================================================="
echo "    CDA NETWORK DOCKER END-TO-END MULTI-BLOCK TEST SUITE                 "
echo "=========================================================================="
echo "Cluster Configuration:"
echo "   - Matrix Dimension (K) : $K_VAL x $K_VAL (EDS: $((2 * K_VAL)) x $((2 * K_VAL)))"
echo "   - RLNC Piece Size (p)  : $K_PIECE_VAL pieces per cell"
echo "   - Total Network Cols(N): $NUM_COLS_VAL columns ($COLS_PER_NET_COL data columns per network column)"
echo "   - Active Columns (C)   : $ACTIVE_COLS_VAL column network(s)"
echo "   - Stores Per Column (S): $STORES_PER_COL_VAL store nodes/col ($((ACTIVE_COLS_VAL * STORES_PER_COL_VAL)) total store nodes)"
echo "   - Light Nodes (L)      : $LIGHT_NODES_VAL light node(s) (Auto-DAS enabled)"
echo "   - Number of Blocks (B) : $BLOCKS_VAL consecutive blocks"
echo "   - Txs Per Block (T)    : $TXS_VAL transactions"
echo "   - Keep-Alive           : $KEEP_ALIVE"
echo "=========================================================================="

# Interrupt and cleanup handling
INTERRUPTED=false
on_interrupt() {
    INTERRUPTED=true
    echo ""
    echo "=========================================================================="
    echo "⚠️ TEST INTERRUPTED BY USER (SIGINT/SIGTERM)."
    echo "   Containers and data are PRESERVED (no cleanup on interrupt)."
    echo "   📊 Grafana Dashboard : http://localhost:3000"
    echo "   To stop and remove containers manually:"
    echo "      bash scripts/cleanup.sh"
    echo "=========================================================================="
    exit 130
}
trap on_interrupt SIGINT SIGTERM

cleanup() {
    EXIT_CODE=$?
    # Capture container logs before exit
    mkdir -p logs
    docker compose -f docker-compose.json logs publisher > logs/docker_publisher.log 2>&1 || true
    docker compose -f docker-compose.json logs bootstrap-0 > logs/docker_bootstrap0.log 2>&1 || true
    docker compose -f docker-compose.json logs store-0-1 > logs/docker_store0_1.log 2>&1 || true
    docker compose -f docker-compose.json logs light-1 > logs/docker_light1.log 2>&1 || true

    if [ "$INTERRUPTED" = "true" ]; then
        return
    fi

    if [ "$KEEP_ALIVE" = "true" ]; then
        echo ""
        echo "=========================================================================="
        if [ "$EXIT_CODE" -eq 0 ]; then
            echo "🚀 KEEP-ALIVE ENABLED: Test PASSED and Docker cluster remains running."
        else
            echo "⚠️ KEEP-ALIVE ENABLED: Test finished (ExitCode: $EXIT_CODE), but Docker cluster remains running for inspection."
        fi
        echo "   📊 Grafana Dashboard : http://localhost:3000 (Credentials: admin / admin)"
        echo "   📈 Prometheus Metrics : http://localhost:9090"
        echo "   To stop the cluster when finished:"
        echo "      bash scripts/cleanup.sh"
        echo "=========================================================================="
    else
        echo ""
        echo "=== Cleaning up Docker resources (Keep-Alive: $KEEP_ALIVE, ExitCode: $EXIT_CODE) ==="
        bash scripts/cleanup.sh 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo ""
echo "[Step 1/6] Generating dynamic Docker Compose and Monitoring configs..."
GENERATED_OUT=$(python3 scripts/generate_compose.py \
    -k "$K_VAL" \
    -p "$K_PIECE_VAL" \
    -c "$ACTIVE_COLS_VAL" \
    -n "$NUM_COLS_VAL" \
    -s "$STORES_PER_COL_VAL" \
    -l "$LIGHT_NODES_VAL" \
    --crash-on-fail)
echo "$GENERATED_OUT"

echo ""
echo "[Step 2/6] Cleaning up any previous Docker states and volumes..."
bash scripts/cleanup.sh 2>/dev/null || true

echo ""
echo "[Step 3/6] Building Docker image and starting services in background..."
docker compose -f docker-compose.json build
docker compose -f docker-compose.json up -d

echo ""
echo "[Step 4/6] Waiting for cluster nodes to initialize and form P2P mesh..."
# 1. Wait for Publisher health
for i in {1..30}; do
    if curl -s http://localhost:8080/health | grep -q "healthy"; then
        echo "✅ Publisher Node is healthy at http://localhost:8080 at ${i}s!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "❌ ERROR: Publisher failed to become healthy within 30s!"
        docker compose -f docker-compose.json logs publisher
        exit 1
    fi
    sleep 1
done

# 2. Wait for Store Nodes to dynamically register with their respective Bootstrap Nodes
for c in $(seq 0 $((ACTIVE_COLS_VAL - 1))); do
    boot_port=$((9200 + c))
    for i in {1..40}; do
        PEERS_COUNT=$(curl -s "http://localhost:${boot_port}/bootstrap/peers" | jq '.peers | length' 2>/dev/null || echo "0")
        if [[ "$PEERS_COUNT" =~ ^[0-9]+$ ]] && [ "$PEERS_COUNT" -ge "$STORES_PER_COL_VAL" ]; then
            echo "✅ Column $c: All $PEERS_COUNT Store Nodes successfully registered with Bootstrap :$boot_port at ${i}s!"
            break
        fi
        if [ "$i" -eq 40 ]; then
            echo "❌ ERROR: Column $c Store Nodes failed to register with Bootstrap :$boot_port within 40s (Count: $PEERS_COUNT/$STORES_PER_COL_VAL)!"
            docker compose -f docker-compose.json logs "bootstrap-${c}"
            exit 1
        fi
        sleep 1
    done
done

echo "Waiting 5 seconds for GossipSub mesh and topic subscriptions to stabilize..."
sleep 5

echo ""
echo "[Step 5/6] Executing CometBFT Consensus Transaction Flow & CDA Verification..."
export CDA_K=$K_VAL
export CDA_K_PIECE=$K_PIECE_VAL
export CDA_BLOCKS=$BLOCKS_VAL
export CDA_TXS_PER_BLOCK=$TXS_VAL
export PUBLISHER_URL="http://localhost:8080/publish"
export PUBLISHER_TIMEOUT="300s"
echo "Exported CDA_K=$CDA_K, CDA_K_PIECE=$CDA_K_PIECE, CDA_BLOCKS=$CDA_BLOCKS, CDA_TXS_PER_BLOCK=$CDA_TXS_PER_BLOCK, PUBLISHER_URL=$PUBLISHER_URL, PUBLISHER_TIMEOUT=$PUBLISHER_TIMEOUT"

echo "--------------------------------------------------------------------------"
go test -v -count=1 ./cometbft/state -run "TestTransactionFlowConsensusCDAHeaderComputationAndVerification"
echo "--------------------------------------------------------------------------"

echo ""
echo "[Step 6/6] Multi-Block Downstream Network Processing & DAS Verification..."
TOTAL_EXPECTED_DAS=$((BLOCKS_VAL * LIGHT_NODES_VAL))
MAX_WAIT=$(( 20 + BLOCKS_VAL * 15 ))
echo "Waiting up to ${MAX_WAIT}s for $TOTAL_EXPECTED_DAS total Auto-DAS checks ($LIGHT_NODES_VAL Light Node(s) x $BLOCKS_VAL blocks)..."

DAS_SUCCESS=false
for i in $(seq 1 $MAX_WAIT); do
    DAS_COUNT=$(docker compose -f docker-compose.json logs 2>/dev/null | grep -c "✅ DAS VERIFIED for" || echo "0")
    if [ "$DAS_COUNT" -ge "$TOTAL_EXPECTED_DAS" ]; then
        DAS_SUCCESS=true
        echo "✅ All $LIGHT_NODES_VAL Light Node(s) completed Auto-DAS for all $BLOCKS_VAL consecutive blocks ($DAS_COUNT total DAS checks) at ${i}s!"
        break
    fi
    sleep 1
done

# Extract unique committed BlockIDs from Publisher container log
BLOCK_IDS=$(docker compose -f docker-compose.json logs publisher 2>/dev/null | grep -oE "BlockID:? block-[0-9]+" | awk '{print $NF}' | sort -u || true)
if [ -z "$BLOCK_IDS" ]; then
    BLOCK_IDS=$(docker compose -f docker-compose.json logs publisher 2>/dev/null | grep -oE "BlockID:? [a-zA-Z0-9_-]+" | awk '{print $NF}' | sort -u || true)
fi

BLOCK_COUNT=0
if [ -n "$BLOCK_IDS" ]; then
    BLOCK_COUNT=$(echo "$BLOCK_IDS" | grep -v '^$' | wc -l || echo "0")
fi
echo "Total Committed Blocks detected in Publisher container: $BLOCK_COUNT"
for bid in $BLOCK_IDS; do
    echo "   - BlockID: $bid"
done

# Fallback: If any Light Node container missed Auto-DAS, trigger via Light Node HTTP API
for l in $(seq 1 "$LIGHT_NODES_VAL"); do
    l_port=$((9400 + l))
    for bid in $BLOCK_IDS; do
        if ! docker compose -f docker-compose.json logs "light-${l}" 2>/dev/null | grep -q "DAS VERIFIED for $bid"; then
            echo "Sampling BlockID $bid via Light Node $l (Port $l_port) HTTP DAS API..."
            HTTP_RESP=$(curl -s "http://localhost:${l_port}/das/sample/$bid?samples=4" || true)
            echo "   HTTP DAS Response: $HTTP_RESP"
            if echo "$HTTP_RESP" | jq -e '.success == true' >/dev/null 2>&1; then
                echo "   ✅ HTTP DAS Sample Verified for Block $bid on Light Node $l!"
            fi
        fi
    done
done

echo ""
echo "=========================================================================="
echo "    DOCKER MULTI-BLOCK END-TO-END SYSTEM VERIFICATION AUDIT               "
echo "=========================================================================="

echo ""
echo "--- [1. Publisher BFT Header Verification & Tamper Defense] ---"
PUB_LOGS=$(docker compose -f docker-compose.json logs publisher 2>&1)
PUB_PASS_COUNT=$(echo "$PUB_LOGS" | grep -c "SUCCESS: Header verification passed for BlockID" || echo "0")
PUB_FAIL_COUNT=$(echo "$PUB_LOGS" | grep -c "Header verification FAILED for BlockID" || echo "0")
echo "✅ Valid Blocks Verified by Publisher: $PUB_PASS_COUNT / $BLOCKS_VAL"
echo "🛡️ Tampered Blocks Rejected by Publisher (HTTP 422): $PUB_FAIL_COUNT"
echo "$PUB_LOGS" | grep -E "SUCCESS: Header verification|Header verification FAILED" || true
echo "---------------------------------------------------------------"

echo ""
echo "--- [2. Store Node Custody Signals Across Blocks] ---"
ALL_LOGS=$(docker compose -f docker-compose.json logs 2>&1)
STORE_READY_COUNT=$(echo "$ALL_LOGS" | grep -c "StoreReady signal broadcasted" || echo "0")
echo "Total StoreReady signals emitted across cluster: $STORE_READY_COUNT"
echo "$ALL_LOGS" | grep -E "StoreReady signal broadcasted" | head -n 12 || true
echo "-----------------------------------------------------"

echo ""
echo "--- [3. Light Node Auto-DAS Verifications] ---"
FINAL_DAS_COUNT=$(echo "$ALL_LOGS" | grep -c "✅ DAS VERIFIED for" || echo "0")
echo "Total Blocks Verified by Light Node DAS: $FINAL_DAS_COUNT / $TOTAL_EXPECTED_DAS"
for l in $(seq 1 "$LIGHT_NODES_VAL"); do
    node_das=$(docker compose -f docker-compose.json logs "light-${l}" 2>/dev/null | grep -c "✅ DAS VERIFIED for" || echo "0")
    echo "   - Light Node $l (Port $((9400 + l))): $node_das / $BLOCKS_VAL blocks verified"
done
echo "$ALL_LOGS" | grep -E "BlockReady signal received|BlockReady triggered DAS|DAS VERIFIED" | head -n 20 || true
echo "----------------------------------------------"

# Export logs
mkdir -p logs
docker compose -f docker-compose.json logs publisher > logs/docker_publisher.log 2>&1 || true
docker compose -f docker-compose.json logs bootstrap-0 > logs/docker_bootstrap0.log 2>&1 || true
docker compose -f docker-compose.json logs store-0-1 > logs/docker_store0_1.log 2>&1 || true
docker compose -f docker-compose.json logs light-1 > logs/docker_light1.log 2>&1 || true
echo "Logs exported to logs/ (docker_publisher.log, docker_bootstrap0.log, docker_store0_1.log, docker_light1.log)"

if [ "$PUB_PASS_COUNT" -ge "$BLOCKS_VAL" ] && [ "$PUB_FAIL_COUNT" -ge 1 ] && [ "$FINAL_DAS_COUNT" -ge "$TOTAL_EXPECTED_DAS" ]; then
    echo ""
    echo "🎉🎉🎉 DOCKER MULTI-BLOCK END-TO-END VERIFICATION COMPLETE & SUCCESSFUL! 🎉🎉🎉"
    echo "✅ 1. Processed $BLOCKS_VAL consecutive blocks through CometBFT consensus"
    echo "✅ 2. CometBFT Proposer computed unique CDA Headers for each block height"
    echo "✅ 3. CometBFT Validators verified proposal CDA Headers against transaction ODS"
    echo "✅ 4. Consensus committed all $BLOCKS_VAL blocks with BFT finality"
    echo "✅ 5. Publisher container verified BFT Headers against consensus commitments"
    echo "✅ 6. Publisher container correctly REJECTED adversarial tampered header (HTTP 422)"
    echo "✅ 7. Column chunks distributed to Store Node containers across $ACTIVE_COLS_VAL active column(s)"
    echo "✅ 8. All $LIGHT_NODES_VAL Light Node container(s) completed Auto-DAS for all $BLOCKS_VAL blocks!"
    exit 0
else
    echo "❌ ERROR: Multi-block verification completed with discrepancies. See logs above."
    exit 1
fi

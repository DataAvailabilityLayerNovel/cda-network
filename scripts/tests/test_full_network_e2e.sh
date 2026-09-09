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

print_help() {
    cat << EOF
==========================================================================
    CDA NETWORK END-TO-END MULTI-BLOCK TEST SUITE
==========================================================================

Usage:
  $(basename "$0") [OPTIONS]
  $(basename "$0") [K] [K_PIECE] [BLOCKS] [TXS] [ACTIVE_COLS] [STORES_PER_COL] [LIGHT_NODES] [NUM_COLS]

Parameters:
  -k, --k <val>              ODS matrix dimension (K x K cells, default: 8)
  -p, --k-piece <val>        RLNC pieces per cell (default: 4)
  -c, --cols <val>           Number of active columns to run (default: 1)
  -n, --num-cols <val>       Total network columns in EDS (default: 2*K)
  -s, --stores-per-col <val> Number of store nodes per column (default: 4)
  -l, --light-nodes <val>    Number of light nodes for DAS (default: 1)
  -b, --blocks <val>         Number of consecutive blocks to test (default: 3)
  -t, --txs <val>            Number of transactions per block (default: 16)
  -h, --help                 Display this help message and exit

Examples:
  # 1. Run with default parameters (K=8, K_piece=4, Cols=1, Stores=4, Lights=1, Blocks=3, Txs=16)
  ./scripts/tests/test_full_network_e2e.sh

  # 2. Run with custom Store Nodes, Light Nodes, and explicit Num Cols
  ./scripts/tests/test_full_network_e2e.sh -s 2 -l 2 -n 16

  # 3. Scaled cluster: K=16, NumCols=32, 2 active columns, 4 stores/col, 3 light nodes, 5 blocks
  ./scripts/tests/test_full_network_e2e.sh -k 16 -n 32 -c 2 -s 4 -l 3 -b 5

  # 4. Positional arguments: [K] [K_PIECE] [BLOCKS] [TXS] [ACTIVE_COLS] [STORES_PER_COL] [LIGHT_NODES] [NUM_COLS]
  ./scripts/tests/test_full_network_e2e.sh 16 4 5 32 2 4 2 32

  # 5. Run with environment variables
  CDA_K=16 CDA_NUM_COLS=32 CDA_ACTIVE_COLS=2 CDA_STORES_PER_COL=4 CDA_LIGHT_NODES=2 ./scripts/tests/test_full_network_e2e.sh

EOF
    exit 0
}

# 1. Check environment variable defaults
[ -n "$CDA_K" ] && K_VAL="$CDA_K"
[ -n "$CDA_K_PIECE" ] && K_PIECE_VAL="$CDA_K_PIECE"
[ -n "$CDA_ACTIVE_COLS" ] && ACTIVE_COLS_VAL="$CDA_ACTIVE_COLS"
[ -n "$CDA_COLS" ] && ACTIVE_COLS_VAL="$CDA_COLS"
[ -n "$CDA_NUM_COLS" ] && NUM_COLS_VAL="$CDA_NUM_COLS"
[ -n "$CDA_STORES_PER_COL" ] && STORES_PER_COL_VAL="$CDA_STORES_PER_COL"
[ -n "$CDA_STORES" ] && STORES_PER_COL_VAL="$CDA_STORES"
[ -n "$CDA_LIGHT_NODES" ] && LIGHT_NODES_VAL="$CDA_LIGHT_NODES"
[ -n "$CDA_LIGHTS" ] && LIGHT_NODES_VAL="$CDA_LIGHTS"
[ -n "$CDA_LIGHT" ] && LIGHT_NODES_VAL="$CDA_LIGHT"
[ -n "$CDA_BLOCKS" ] && BLOCKS_VAL="$CDA_BLOCKS"
[ -n "$CDA_TXS_PER_BLOCK" ] && TXS_VAL="$CDA_TXS_PER_BLOCK"
[ -n "$CDA_TXS" ] && TXS_VAL="$CDA_TXS"

# 2. Parse command line arguments
if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    print_help
fi

if [[ "$1" =~ ^[0-9]+$ ]]; then
    # Positional arguments mode: [K] [K_PIECE] [BLOCKS] [TXS] [ACTIVE_COLS] [STORES_PER_COL] [LIGHT_NODES] [NUM_COLS]
    [ -n "$1" ] && K_VAL="$1"
    [ -n "$2" ] && K_PIECE_VAL="$2"
    [ -n "$3" ] && BLOCKS_VAL="$3"
    [ -n "$4" ] && TXS_VAL="$4"
    [ -n "$5" ] && ACTIVE_COLS_VAL="$5"
    [ -n "$6" ] && STORES_PER_COL_VAL="$6"
    [ -n "$7" ] && LIGHT_NODES_VAL="$7"
    [ -n "$8" ] && NUM_COLS_VAL="$8"
else
    # Flags mode
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
            -c|--cols|--col|--active-cols)
                ACTIVE_COLS_VAL="$2"
                shift 2
                ;;
            -n|--num-cols|--network-cols)
                NUM_COLS_VAL="$2"
                shift 2
                ;;
            -s|--stores-per-col|--stores|--store)
                STORES_PER_COL_VAL="$2"
                shift 2
                ;;
            -l|--light-nodes|--lights|--light)
                LIGHT_NODES_VAL="$2"
                shift 2
                ;;
            -b|--blocks|--block)
                BLOCKS_VAL="$2"
                shift 2
                ;;
            -t|--txs|--tx)
                TXS_VAL="$2"
                shift 2
                ;;
            -h|--help)
                print_help
                ;;
            *)
                echo "❌ Error: Unknown option '$1'"
                echo "Run '$0 --help' for usage details."
                exit 1
                ;;
        esac
    done
fi

if [ -n "$NUM_COLS_VAL" ]; then
    NUM_COLS="$NUM_COLS_VAL"
else
    NUM_COLS=$((2 * K_VAL))
fi

EDS_DIM=$((2 * K_VAL))
EDS_CELLS=$((EDS_DIM * EDS_DIM))
COLS_PER_NET_COL=$((EDS_DIM / NUM_COLS))
[ "$COLS_PER_NET_COL" -le 0 ] && COLS_PER_NET_COL=1

echo "=========================================================================="
echo "    FULL NETWORK E2E TEST: TRANSACTIONS -> COMETBFT -> PUBLISHER ->       "
echo "               STORE NODES (CUSTODY) -> LIGHT NODE (DAS)                  "
echo "=========================================================================="
echo ">> Target Configuration:"
echo "   - K (Matrix Dimension) : $K_VAL ($((K_VAL * K_VAL)) ODS cells, ${EDS_DIM}x${EDS_DIM} = $EDS_CELLS EDS cells)"
echo "   - K-Piece (RLNC Chunks): $K_PIECE_VAL pieces/cell"
echo "   - Total Network Cols   : $NUM_COLS column groups ($COLS_PER_NET_COL data col(s) per network col)"
echo "   - Active Columns       : $ACTIVE_COLS_VAL active column network(s)"
echo "   - Store Nodes          : $STORES_PER_COL_VAL nodes per column ($((ACTIVE_COLS_VAL * STORES_PER_COL_VAL)) total store nodes)"
echo "   - Light Nodes          : $LIGHT_NODES_VAL light node(s) running Auto-DAS"
echo "   - Number of Blocks     : $BLOCKS_VAL consecutive blocks"
echo "   - Txs Per Block        : $TXS_VAL transactions"
echo "=========================================================================="

# 1. Build All 4 Node Binaries
echo "[Step 1/6] Building all 4 CDA Network node binaries..."
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go
echo "✅ All node binaries built in bin/"

# 2. Start Services in Background
echo "[Step 2/6] Starting background cluster nodes..."
rm -f publisher.log light*.log bootstrap*.log store*.log
rm -f store_*.key *.key
rm -rf data/store_* data/light_* data/publisher data/bootstrap_*

BOOTSTRAP_PIDS=()
STORE_PIDS=()
LIGHT_PIDS=()
BOOTSTRAP_MAP_PARTS=()

for c in $(seq 0 $((ACTIVE_COLS_VAL - 1))); do
    boot_port=$((8100 + c * 100))
    store_base=$((boot_port + 1))
    start_col=$((c * COLS_PER_NET_COL))
    BOOTSTRAP_MAP_PARTS+=("${c}:http://localhost:${boot_port}")

    # Bootstrap Node for column network c (managing data columns start_col .. start_col + COLS_PER_NET_COL - 1)
    stdbuf -oL -eL ./bin/bootstrap -port $boot_port -col $start_col -store http://localhost:$store_base -publisher http://localhost:8080 -k $K_VAL -k-piece $K_PIECE_VAL -prune-enable=true -prune-ttl 15s > bootstrap${c}.log 2>&1 &
    BOOTSTRAP_PIDS+=($!)

    # STORES_PER_COL_VAL Store Nodes for column network c (Rows 0..STORES_PER_COL_VAL-1)
    for r in $(seq 0 $((STORES_PER_COL_VAL - 1))); do
        s_port=$((store_base + r))
        stdbuf -oL -eL ./bin/store -port $s_port -row $r -col $start_col -publisher http://localhost:8080 -bootstrap http://localhost:$boot_port -k $K_VAL -k-piece $K_PIECE_VAL -num-cols $NUM_COLS -stores-per-col $STORES_PER_COL_VAL -prune-enable=true -prune-ttl 15s > store${c}_${r}.log 2>&1 &
        STORE_PIDS+=($!)
    done
done

BOOTSTRAP_ARG=$(IFS=";"; echo "${BOOTSTRAP_MAP_PARTS[*]}")
sleep 1

# Publisher Node (Port 8080)
stdbuf -oL -eL ./bin/publisher -port 8080 -bootstrap "$BOOTSTRAP_ARG" -k $K_VAL -k-piece $K_PIECE_VAL -num-cols $NUM_COLS -active-cols $ACTIVE_COLS_VAL > publisher.log 2>&1 &
PUBLISHER_PID=$!

# Light Nodes (Ports 8086, 8087, ...)
for l in $(seq 0 $((LIGHT_NODES_VAL - 1))); do
    l_port=$((8086 + l))
    stdbuf -oL -eL ./bin/light -port $l_port -publisher http://localhost:8080 -bootstraps "$BOOTSTRAP_ARG" -k $K_VAL -k-piece $K_PIECE_VAL -num-cols $NUM_COLS -auto-das=true > light${l}.log 2>&1 &
    LIGHT_PIDS+=($!)
done
ln -sf light0.log light.log 2>/dev/null || true

cleanup() {
    echo "=== Cleaning up all background node processes ==="
    kill -9 $PUBLISHER_PID ${LIGHT_PIDS[@]} ${BOOTSTRAP_PIDS[@]} ${STORE_PIDS[@]} 2>/dev/null || true
    wait $PUBLISHER_PID ${LIGHT_PIDS[@]} ${BOOTSTRAP_PIDS[@]} ${STORE_PIDS[@]} 2>/dev/null || true
    rm -rf bin/
}
trap cleanup EXIT

echo "Waiting for cluster nodes and P2P mesh to initialize across $ACTIVE_COLS_VAL active column(s)..."
for c in $(seq 0 $((ACTIVE_COLS_VAL - 1))); do
    boot_port=$((8100 + c * 100))
    for i in {1..30}; do
        PEERS_COUNT=$(curl -s "http://localhost:${boot_port}/bootstrap/peers" | jq '.peers | length' 2>/dev/null || echo "0")
        if [[ "$PEERS_COUNT" =~ ^[0-9]+$ ]] && [ "$PEERS_COUNT" -ge "$STORES_PER_COL_VAL" ]; then
            echo "✅ Column $c: All $PEERS_COUNT Store Nodes successfully registered with Bootstrap :$boot_port at ${i}s!"
            break
        fi
        sleep 1
    done
done

echo "Waiting 5 seconds for GossipSub mesh & topic subscriptions to stabilize..."
sleep 5

echo "✅ Cluster is ready:"
echo "   - Publisher Node : http://localhost:8080 (PID: $PUBLISHER_PID, ActiveCols: $ACTIVE_COLS_VAL)"
echo "   - Bootstraps     : $BOOTSTRAP_ARG"
echo "   - Store Nodes    : ${#STORE_PIDS[@]} nodes running ($STORES_PER_COL_VAL per column across $ACTIVE_COLS_VAL columns)"
echo "   - Light Nodes    : ${#LIGHT_PIDS[@]} nodes running (Ports 8086..$((8086 + LIGHT_NODES_VAL - 1)), Auto-DAS: Enabled)"

# 3. Configure CometBFT Environment Variables
export CDA_K=$K_VAL
export CDA_K_PIECE=$K_PIECE_VAL
export CDA_BLOCKS=$BLOCKS_VAL
export CDA_TXS_PER_BLOCK=$TXS_VAL
export PUBLISHER_URL="http://localhost:8080/publish"
echo "[Step 3/6] Exported CDA_K=$CDA_K, CDA_K_PIECE=$CDA_K_PIECE, CDA_BLOCKS=$CDA_BLOCKS, CDA_TXS_PER_BLOCK=$CDA_TXS_PER_BLOCK"

# 4. Execute CometBFT Consensus Engine: Proposer -> Validator -> Pusher
echo "[Step 4/6] Running CometBFT Consensus Transaction Flow & Verification..."
echo "--------------------------------------------------------------------------"
go test -v -count=1 ./cometbft/state -run "TestTransactionFlowConsensusCDAHeaderComputationAndVerification"

# 5. Multi-Block Downstream Network Processing & DAS Verification
echo "[Step 5/6] Waiting for network to process all blocks and perform Auto-DAS..."
sleep 2

TOTAL_EXPECTED_DAS=$((BLOCKS_VAL * LIGHT_NODES_VAL))
MAX_WAIT=$(( 15 + BLOCKS_VAL * 12 ))
DAS_SUCCESS=false
for i in $(seq 1 $MAX_WAIT); do
    DAS_COUNT=$(grep -c "✅ DAS VERIFIED for" light*.log 2>/dev/null | awk -F: '{s+=$2} END {print s}' || echo "0")
    if [ "$DAS_COUNT" -ge "$TOTAL_EXPECTED_DAS" ]; then
        DAS_SUCCESS=true
        echo "✅ All $LIGHT_NODES_VAL Light Node(s) completed Auto-DAS for all $BLOCKS_VAL consecutive blocks ($DAS_COUNT total DAS checks) at ${i}s!"
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

# If any light node missed Auto-DAS, trigger via HTTP DAS endpoint
for l in $(seq 0 $((LIGHT_NODES_VAL - 1))); do
    l_port=$((8086 + l))
    l_log="light${l}.log"
    for bid in $BLOCK_IDS; do
        if ! grep -q "DAS VERIFIED for $bid" "$l_log" 2>/dev/null; then
            echo "Sampling BlockID $bid via Light Node $l (Port $l_port) HTTP DAS API..."
            HTTP_RESP=$(curl -s "http://localhost:${l_port}/das/sample/$bid?samples=4" || true)
            echo "   HTTP DAS Response: $HTTP_RESP"
            if echo "$HTTP_RESP" | jq -e '.success == true' >/dev/null 2>&1; then
                echo "   ✅ HTTP DAS Sample Verified for Block $bid on Light Node $l!"
            fi
        fi
    done
done

# 6. Verification of Metrics and Logs Across the Network Pipeline
echo ""
echo "=========================================================================="
echo "    [Step 6/6] MULTI-BLOCK END-TO-END SYSTEM VERIFICATION AUDIT          "
echo "=========================================================================="

echo ""
echo "--- [1. Publisher BFT Header Verification & Tamper Defense] ---"
PUB_PASS_COUNT=$(grep -c "SUCCESS: Header verification passed for BlockID" publisher.log || echo "0")
PUB_FAIL_COUNT=$(grep -c "Header verification FAILED for BlockID" publisher.log || echo "0")
echo "✅ Valid Blocks Verified by Publisher: $PUB_PASS_COUNT / $BLOCKS_VAL"
echo "🛡️ Tampered Blocks Rejected by Publisher (HTTP 422): $PUB_FAIL_COUNT"
grep -E "SUCCESS: Header verification|Header verification FAILED" publisher.log || true
echo "---------------------------------------------------------------"

echo ""
echo "--- [2. Store Node Custody Signals Across Blocks] ---"
STORE_READY_COUNT=$(grep -c "StoreReady signal broadcasted" store*.log 2>/dev/null | awk -F: '{s+=$2} END {print s}' || echo "0")
echo "Total StoreReady signals emitted across cluster: $STORE_READY_COUNT"
grep -E "StoreReady signal broadcasted" store*.log 2>/dev/null | head -n 12 || true
echo "-----------------------------------------------------"

echo ""
echo "--- [3. Light Node Auto-DAS Verifications] ---"
TOTAL_EXPECTED_DAS=$((BLOCKS_VAL * LIGHT_NODES_VAL))
FINAL_DAS_COUNT=$(grep -c "✅ DAS VERIFIED for" light*.log 2>/dev/null | awk -F: '{s+=$2} END {print s}' || echo "0")
echo "Total Blocks Verified by Light Node DAS: $FINAL_DAS_COUNT / $TOTAL_EXPECTED_DAS"
for l in $(seq 0 $((LIGHT_NODES_VAL - 1))); do
    l_log="light${l}.log"
    node_das=$(grep -c "✅ DAS VERIFIED for" "$l_log" 2>/dev/null || echo "0")
    echo "   - Light Node $l (Port $((8086 + l))): $node_das / $BLOCKS_VAL blocks verified"
done
grep -E "BlockReady signal received|BlockReady triggered DAS|DAS VERIFIED" light*.log || true
echo "----------------------------------------------"

if [ "$PUB_PASS_COUNT" -ge "$BLOCKS_VAL" ] && [ "$PUB_FAIL_COUNT" -ge 1 ] && [ "$FINAL_DAS_COUNT" -ge "$TOTAL_EXPECTED_DAS" ]; then
    echo ""
    echo "🎉🎉🎉 MULTI-BLOCK END-TO-END VERIFICATION COMPLETE & SUCCESSFUL! 🎉🎉🎉"
    echo "✅ 1. Processed $BLOCKS_VAL consecutive blocks (Height 1 -> ... -> $BLOCKS_VAL) through consensus"
    echo "✅ 2. CometBFT Proposer computed unique CDA Headers for each block height"
    echo "✅ 3. CometBFT Validators verified proposal CDA Headers against transaction ODS"
    echo "✅ 4. Consensus committed all $BLOCKS_VAL blocks with BFT finality"
    echo "✅ 5. Publisher Node verified BFT Headers against consensus commitments"
    echo "✅ 6. Publisher Node correctly REJECTED adversarial tampered header (HTTP 422)"
    echo "✅ 7. Column chunks distributed to Store Nodes across $ACTIVE_COLS_VAL active column(s) ($(($ACTIVE_COLS_VAL * STORES_PER_COL_VAL)) total store nodes)"
    echo "✅ 8. All $LIGHT_NODES_VAL Light Node(s) completed Data Availability Sampling (DAS) for all $BLOCKS_VAL blocks!"
else
    echo "⚠️ Multi-block verification completed with warnings. Check logs above."
    exit 1
fi

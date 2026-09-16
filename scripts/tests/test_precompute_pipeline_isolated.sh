#!/bin/bash
set -e

# Automatically resolve path and change directory to repository root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

## Default parameters
K_VAL=16
K_PIECE_VAL=4
ACTIVE_COLS_VAL=2
STORES_PER_COL_VAL=8
LIGHT_NODES_VAL=1
NUM_COLS_VAL=8
BLOCKS_VAL=3
KEEP_ALIVE=false

print_help() {
    cat << EOF
==========================================================================
    CDA NETWORK — ISOLATED PRE-COMPUTE PIPELINE VERIFICATION TEST
==========================================================================

Usage:
  $(basename "$0") [OPTIONS]

Parameters:
  -k, --k <val>              ODS matrix dimension (default: 16)
  -p, --k-piece <val>        RLNC pieces per cell (default: 4)
  -n, --num-cols <val>       Total network columns in EDS (default: 8)
  -c, --active-cols <val>    Number of active columns to run (default: 2)
  -s, --stores-per-col <val> Number of store nodes per column (default: 8)
  -l, --light-nodes <val>    Number of light nodes for DAS (default: 1)
  -b, --blocks <val>         Number of consecutive blocks to test (default: 3)
  --keep-alive               Keep Docker containers running after test
  -h, --help                 Display this help message and exit

Examples:
  ./scripts/tests/test_precompute_pipeline_isolated.sh -k 16 -c 2 -s 8 -b 3
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -k|--k) K_VAL="$2"; shift 2 ;;
        -p|--k-piece) K_PIECE_VAL="$2"; shift 2 ;;
        -n|--num-cols) NUM_COLS_VAL="$2"; shift 2 ;;
        -c|--active-cols) ACTIVE_COLS_VAL="$2"; shift 2 ;;
        -s|--stores-per-col) STORES_PER_COL_VAL="$2"; shift 2 ;;
        -l|--light-nodes) LIGHT_NODES_VAL="$2"; shift 2 ;;
        -b|--blocks) BLOCKS_VAL="$2"; shift 2 ;;
        --keep-alive) KEEP_ALIVE=true; shift 1 ;;
        -h|--help) print_help ;;
        *) echo "Unknown option: $1"; print_help ;;
    esac
done

echo "=========================================================================="
echo "    CDA NETWORK — ISOLATED PRE-COMPUTE PIPELINE VERIFICATION TEST        "
echo "=========================================================================="
echo "Cluster Configuration:"
echo "   - Matrix Dimension (K) : $K_VAL x $K_VAL (EDS: $((2 * K_VAL)) x $((2 * K_VAL)))"
echo "   - RLNC Piece Size (p)  : $K_PIECE_VAL pieces per cell"
echo "   - Total Network Cols(N): $NUM_COLS_VAL columns"
echo "   - Active Columns (C)   : $ACTIVE_COLS_VAL column network(s)"
echo "   - Stores Per Column (S): $STORES_PER_COL_VAL store nodes/col"
echo "   - Light Nodes (L)      : $LIGHT_NODES_VAL light node(s)"
echo "   - Number of Blocks (B) : $BLOCKS_VAL blocks (Pipeline 1-ahead)"
echo "   - Keep-Alive           : $KEEP_ALIVE"
echo "=========================================================================="

INTERRUPTED=false
on_interrupt() {
    INTERRUPTED=true
    echo "⚠️ TEST INTERRUPTED BY USER. Containers and logs preserved."
    exit 130
}
trap on_interrupt SIGINT SIGTERM

cleanup() {
    EXIT_CODE=$?
    mkdir -p logs
    docker compose -f docker-compose.json logs publisher > logs/docker_publisher.log 2>&1 || true
    docker compose -f docker-compose.json logs bootstrap-0 > logs/docker_bootstrap0.log 2>&1 || true
    docker compose -f docker-compose.json logs bootstrap-1 > logs/docker_bootstrap1.log 2>&1 || true

    if [ "$INTERRUPTED" = "true" ] || [ "$KEEP_ALIVE" = "true" ]; then
        echo "Cluster remains running for inspection (Keep-Alive: $KEEP_ALIVE)."
        return
    fi
    echo "=== Cleaning up Docker resources ==="
    bash scripts/cleanup.sh 2>/dev/null || true
}
trap cleanup EXIT

echo ""
echo "[Step 1/5] Checking existing cluster state..."
if ! curl -s http://localhost:8080/health | grep -q "healthy"; then
    echo "Cluster not running. Generating compose and starting cluster..."
    bash scripts/cleanup.sh 2>/dev/null || true
    python3 scripts/generate_compose.py \
        --cols "$NUM_COLS_VAL" \
        --active-cols "$ACTIVE_COLS_VAL" \
        --stores-per-col "$STORES_PER_COL_VAL" \
        --lights "$LIGHT_NODES_VAL" \
        --k "$K_VAL" \
        --k-piece "$K_PIECE_VAL" \
        --prune-enable \
        --prune-ttl 15s

    docker compose -f docker-compose.json up -d --build
    echo "Waiting for services to become healthy..."
    for i in {1..30}; do
        if curl -s http://localhost:8080/health | grep -q "healthy"; then
            echo "✅ Publisher healthy at ${i}s!"
            break
        fi
        sleep 1
    done
else
    echo "✅ Cluster is already active and healthy."
fi

echo ""
echo "[Step 2/5] Verifying Store Nodes registration with Bootstrap Nodes..."
for c in $(seq 0 $((ACTIVE_COLS_VAL - 1))); do
    boot_port=$((9200 + c))
    for i in {1..30}; do
        PEERS_COUNT=$(curl -s "http://localhost:${boot_port}/bootstrap/peers" | jq '.peers | length' 2>/dev/null || echo "0")
        if [[ "$PEERS_COUNT" =~ ^[0-9]+$ ]] && [ "$PEERS_COUNT" -ge "$STORES_PER_COL_VAL" ]; then
            echo "✅ Column $c: All $PEERS_COUNT Store Nodes registered with Bootstrap :$boot_port!"
            break
        fi
        sleep 1
    done
done

echo ""
echo "[Step 3/5] Running Pipelined 1-Block-Ahead Pre-Compute Benchmark..."
python3 scripts/test_pipeline_block_timing.py \
    --publisher http://localhost:8080 \
    --bootstrap http://localhost:9200 \
    --k "$K_VAL" \
    --count "$BLOCKS_VAL" \
    --timeout 120

echo ""
echo "[Step 4/5] Auditing Pre-Computation & In-Flight Buffer logs from Bootstrap Node..."
PRECOMPUTE_COUNT=$(docker compose -f docker-compose.json logs bootstrap-0 2>/dev/null | grep -c "\[Bootstrap Pre-computation\]" || echo "0")
WAIT_BUFFER_COUNT=$(docker compose -f docker-compose.json logs bootstrap-0 2>/dev/null | grep -c "\[Bootstrap Pipeline\] Pre-computed buffer ready" || echo "0")
DISPATCH_COUNT=$(docker compose -f docker-compose.json logs bootstrap-0 2>/dev/null | grep -c "Instantly dispatching pre-computed seeds" || echo "0")

echo "--------------------------------------------------------------------------"
echo "Bootstrap Node Pre-Compute Audit:"
echo "   - Pre-computed buffer rounds : $PRECOMPUTE_COUNT"
echo "   - Buffered wait events       : $WAIT_BUFFER_COUNT"
echo "   - Instant buffer dispatches  : $DISPATCH_COUNT"
echo "--------------------------------------------------------------------------"

if [ "$PRECOMPUTE_COUNT" -gt 0 ]; then
    echo "✅ SUCCESS: Bootstrap Pre-computation actively buffered seeds ahead of time!"
else
    echo "⚠️ Warning: No Pre-computation logs found in bootstrap-0."
fi

echo ""
echo "[Step 5/5] Auditing Store Nodes Completion..."
for c in $(seq 0 $((ACTIVE_COLS_VAL - 1))); do
    for s in $(seq 0 $((STORES_PER_COL_VAL - 1))); do
        port=$((9300 + c * 8 + s))
        COMPLETION_LINES=$(cat "data/store_${port}/completion.log" 2>/dev/null | wc -l || echo "0")
        echo "   - Store Node :$port (Col $c, Row $s): $COMPLETION_LINES blocks completed"
    done
done

echo ""
echo "🎉🎉🎉 ISOLATED PRE-COMPUTE PIPELINE TEST COMPLETE! 🎉🎉🎉"

#!/bin/bash
set -e

# Get the root directory of the repository
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

echo "=========================================================================="
echo "  TEST: Light Node Single-Seed Matrix Discovery & Multi-Column DAS Test   "
echo "=========================================================================="

echo "=== 1. Building Binaries ==="
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go

echo "=== 2. Creating Publisher Configuration (K=16, 8 Network Columns) ==="
cat <<EOF > publisher_config.json
{
  "api_port": 8080,
  "k": 16,
  "bootstrap_peers": {
    "0": "http://localhost:8200",
    "1": "http://localhost:8200",
    "2": "http://localhost:8200",
    "3": "http://localhost:8200",
    "4": "http://localhost:8201",
    "5": "http://localhost:8201",
    "6": "http://localhost:8201",
    "7": "http://localhost:8201",
    "8": "http://localhost:8202",
    "9": "http://localhost:8202",
    "10": "http://localhost:8202",
    "11": "http://localhost:8202",
    "12": "http://localhost:8203",
    "13": "http://localhost:8203",
    "14": "http://localhost:8203",
    "15": "http://localhost:8203",
    "16": "http://localhost:8204",
    "17": "http://localhost:8204",
    "18": "http://localhost:8204",
    "19": "http://localhost:8204",
    "20": "http://localhost:8205",
    "21": "http://localhost:8205",
    "22": "http://localhost:8205",
    "23": "http://localhost:8205",
    "24": "http://localhost:8206",
    "25": "http://localhost:8206",
    "26": "http://localhost:8206",
    "27": "http://localhost:8206",
    "28": "http://localhost:8207",
    "29": "http://localhost:8207",
    "30": "http://localhost:8207",
    "31": "http://localhost:8207"
  }
}
EOF

echo "=== 3. Starting Bootstrap Nodes (Bootstrap 0 is Seed, Bootstraps 1..7 register with Seed) ==="
BOOTSTRAP_PIDS=()

# Bootstrap 0 (Seed Node)
./bin/bootstrap -port 8200 -col 0 -publisher http://localhost:8080 -k 16 > bootstrap0.log 2>&1 &
BOOTSTRAP_PIDS+=($!)

sleep 1

# Bootstraps 1..7 (registered with Seed Node at localhost:8200)
for c in {1..7}; do
    port=$((8200 + c))
    col_id=$((c * 4))
    ./bin/bootstrap -port $port -col $col_id -seed http://localhost:8200 -publisher http://localhost:8080 -k 16 > bootstrap${c}.log 2>&1 &
    BOOTSTRAP_PIDS+=($!)
done

sleep 2

echo "=== 4. Starting Store Nodes across all 8 Column Subnets ==="
STORE_PIDS=()
STORE_PORTS=()
current_store_port=8300

for c in {0..7}; do
    col_id=$((c * 4))
    bootstrap_port=$((8200 + c))
    for s in {1..2}; do
        row=$((s - 1))
        ./bin/store -port $current_store_port -row $row -col $col_id -publisher http://localhost:8080 -bootstrap http://localhost:${bootstrap_port} -k 16 -myaddr http://localhost:${current_store_port} > store${c}_${s}.log 2>&1 &
        STORE_PIDS+=($!)
        STORE_PORTS+=($current_store_port)
        current_store_port=$((current_store_port + 1))
    done
done

# Publisher Node (Port 8080)
./bin/publisher -config publisher_config.json > publisher.log 2>&1 &
PUBLISHER_PID=$!

# ==============================================================================
# KEY TEST POINT:
# Light Node is configured with ONLY 1 Bootstrap Node (Column 0: http://localhost:8200)!
# It has NO knowledge in config of Bootstraps 1..7 (ports 8201..8207).
# ==============================================================================
echo "=== 5. Starting Light Node with ONLY 1 Single Seed Bootstrap (Port 8200) ==="
./bin/light -port 8499 -publisher http://localhost:8080 -bootstraps "0:http://localhost:8200" -k 16 -num-cols 8 > light_single_seed.log 2>&1 &
LIGHT_PID=$!

cleanup() {
    echo "=== Cleaning up test background processes ==="
    kill $PUBLISHER_PID "${BOOTSTRAP_PIDS[@]}" "${STORE_PIDS[@]}" $LIGHT_PID 2>/dev/null || true
    rm -rf bin/ publisher_config.json data/ *.key
    wait $PUBLISHER_PID "${BOOTSTRAP_PIDS[@]}" "${STORE_PIDS[@]}" $LIGHT_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting for services to establish P2P mesh and registrations..."
sleep 4

echo "=== 5.5. Verifying Dynamic Peer Registration across all 8 Bootstraps ==="
for c in {0..7}; do
    b_port=$((8200 + c))
    for attempt in {1..20}; do
        b_peers=$(curl -s "http://localhost:${b_port}/bootstrap/peers" | jq -c '.peers' 2>/dev/null || echo "[]")
        if [ "$b_peers" != "[]" ] && [ "$b_peers" != "null" ]; then
            echo "Bootstrap ${c} (Port ${b_port}) active peers: $b_peers"
            break
        fi
        sleep 0.5
    done
done

echo "=== 6. Publishing Block (16x16 ODS -> 32x32 EDS) ==="
PAYLOAD=$(python3 -c "
import json
data = ['%0128x' % (i + 1) for i in range(256)]
print(json.dumps({'block_id': 'test-single-seed-block', 'data': data}))
")

# Wait until publisher is responsive
for i in {1..30}; do
    if curl -s http://localhost:8080/publish > /dev/null 2>&1 || curl -s http://localhost:8080/health > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

PUB_RESP=$(curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:8080/publish)
echo "Publisher response: $PUB_RESP"

echo "Waiting for Store Nodes across all columns to finish coding..."
block_id="test-single-seed-block"

for i in {1..100}; do
    all_complete=true
    for port in "${STORE_PORTS[@]}"; do
        resp=$(curl -s "http://localhost:${port}/store/status/${block_id}")
        completed=$(echo "$resp" | jq -r '.completed' 2>/dev/null || echo "false")
        if [ "$completed" != "true" ]; then
            all_complete=false
            break
        fi
    done
    if [ "$all_complete" = "true" ]; then
        echo "All Store Nodes across all columns completed GossipSub and stored pieces! Took $((i * 200))ms."
        break
    fi
    sleep 0.2
done

echo "=========================================================================="
echo "=== 7. DAS Testing: Querying Cells in Different Columns via Single Seed ==="
echo "=========================================================================="

# Test sampling specific cells across different column subnets:
# Net Col 0 (c=0), Net Col 1 (c=4), Net Col 3 (c=12), Net Col 7 (c=28)
TEST_CELLS=("0,0" "1,4" "0,12" "1,28")

for cell in "${TEST_CELLS[@]}"; do
    IFS=',' read -r r c <<< "$cell"
    echo "--- Testing DAS on Cell [$r, $c] (Column Subnet $((c / 4))) ---"
    CELL_RESP=$(curl -s "http://localhost:8499/das/sample/${block_id}?row=${r}&col=${c}")
    CELL_SUCCESS=$(echo "$CELL_RESP" | jq -r '.success')
    echo "Response: $CELL_RESP"
    if [ "$CELL_SUCCESS" != "true" ]; then
        echo "[-] FAILED: DAS verification failed for Cell [$r, $c]!"
        exit 1
    fi
    echo "[+] SUCCESS: DAS verified Cell [$r, $c] via dynamic seed discovery!"
done

echo "=========================================================================="
echo "=== 8. DAS Testing: Full Matrix Sampling (?all=true, All 32 Columns) ==="
echo "=========================================================================="
FULL_RESP=$(curl -s "http://localhost:8499/das/sample/${block_id}?all=true")
FULL_SUCCESS=$(echo "$FULL_RESP" | jq -r '.success')
TOTAL_SAMPLED=$(echo "$FULL_RESP" | jq '.results | length')

echo "Full Matrix DAS Summary:"
echo "$FULL_RESP" | jq '{block_id: .block_id, success: .success, total_cells_sampled: (.results | length)}'

if [ "$FULL_SUCCESS" != "true" ] || [ "$TOTAL_SAMPLED" -lt 32 ]; then
    echo "[-] FAILED: Full Matrix DAS verification failed! Sampled: $TOTAL_SAMPLED"
    exit 1
fi

echo "=========================================================================="
echo "  RESULT: ALL DAS SAMPLING PASSED 100% WITH ONLY 1 SEED BOOTSTRAP NODE!  "
echo "=========================================================================="

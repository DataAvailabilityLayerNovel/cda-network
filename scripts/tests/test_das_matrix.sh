#!/bin/bash
set -e

# Get the root directory of the repository (parent of scripts/tests)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

echo "=== 1. Building Nodes ==="
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go

echo "=== 2. Creating Configuration Files ==="
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

echo "=== 3. Starting Services in Background ==="

BOOTSTRAP_PIDS=()
for c in {0..7}; do
    port=$((8200 + c))
    col_id=$((c * 4))
    ./bin/bootstrap -port $port -col $col_id -publisher http://localhost:8080 -k 16 > bootstrap${c}.log 2>&1 &
    BOOTSTRAP_PIDS+=($!)
done

# Wait for bootstraps to spin up and bind their ports
sleep 2

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

# --- Publisher Node (Port 8080) ---
./bin/publisher -config publisher_config.json > publisher.log 2>&1 &
PUBLISHER_PID=$!

# --- Light Nodes ---
# Light Node 1 (Port 8401)
./bin/light -port 8401 -publisher http://localhost:8080 -bootstraps "0:http://localhost:8200;1:http://localhost:8201;2:http://localhost:8202;3:http://localhost:8203;4:http://localhost:8204;5:http://localhost:8205;6:http://localhost:8206;7:http://localhost:8207" -k 16 > light1.log 2>&1 &
LIGHT1_PID=$!

# Light Node 2 (Port 8402)
./bin/light -port 8402 -publisher http://localhost:8080 -bootstraps "0:http://localhost:8200;1:http://localhost:8201;2:http://localhost:8202;3:http://localhost:8203;4:http://localhost:8204;5:http://localhost:8205;6:http://localhost:8206;7:http://localhost:8207" -k 16 > light2.log 2>&1 &
LIGHT2_PID=$!

# Ensure cleanup on exit
cleanup() {
    echo "=== Cleaning up background processes ==="
    kill $PUBLISHER_PID "${BOOTSTRAP_PIDS[@]}" "${STORE_PIDS[@]}" $LIGHT1_PID $LIGHT2_PID 2>/dev/null || true
    rm -rf bin/ publisher_config.json data/
    wait $PUBLISHER_PID "${BOOTSTRAP_PIDS[@]}" "${STORE_PIDS[@]}" $LIGHT1_PID $LIGHT2_PID 2>/dev/null || true
}
trap cleanup EXIT

# Allow servers to start
echo "Waiting for services to start..."
sleep 4

echo "=== 4. Verifying Dynamic Registration (Phase 1) ==="
# Retrieve bootstrap 0 active peers list
PEERS0=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo "Bootstrap Node 0 active peers list: $PEERS0"
# Verify Store Node 0-1 (8300) and 0-2 (8301) are both registered (checking their P2P ports: 18300, 18301)
if [[ "$PEERS0" != *"18300"* ]] || [[ "$PEERS0" != *"18301"* ]]; then
    echo "[-] ERROR: Dynamic registration failed!"
    exit 1
fi
echo "[+] Dynamic registration succeeded!"

echo "=== 5. Sending Publish Request to Publisher (ODS 16x16) ==="
PAYLOAD=$(python3 -c "
import json
data = ['%0128x' % (i + 1) for i in range(256)]
print(json.dumps({'block_id': 'test-block-matrix', 'data': data}))
")

echo "Publishing block to Publisher Node..."
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:8080/publish > /dev/null

echo "Waiting for Store Nodes to complete GossipSub and coding..."
block_id="test-block-matrix"

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
        echo "All Store Nodes have completed GossipSub and stored pieces! Took $((i * 200))ms."
        break
    fi
    sleep 0.2
done

echo "=== 6. Performing DAS Sampling on Light Node 1 (8401) for ALL EDS cells ==="
QUERY1_RESP=$(curl -s "http://localhost:8401/das/sample/test-block-matrix?all=true")
SUCCESS1=$(echo "$QUERY1_RESP" | jq -r '.success')

echo "Light Node 1 DAS Response Summary:"
echo "$QUERY1_RESP" | jq '{block_id: .block_id, success: .success, total_cells_sampled: (.results | length)}'

if [ "$SUCCESS1" != "true" ]; then
    echo "[-] ERROR: DAS Verification on Light Node 1 failed!"
    echo "Detailed Results (First 5 cells):"
    echo "$QUERY1_RESP" | jq '.results[:5]'
    exit 1
fi

echo "=== 7. Testing Graceful Leave Mid-Test ==="
# Stop Store Node 0-2 (Port 8301)
echo "Stopping Store Node 0-2 (Port 8301) gracefully..."
kill -15 ${STORE_PIDS[1]}
sleep 2

# Retrieve bootstrap 0 active peers list again
PEERS0_AFTER=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo "Bootstrap Node 0 active peers list after deregister: $PEERS0_AFTER"

# Verify Store Node 0-2 (P2P port 18301) is removed and 0-1 (18300) remains
if [[ "$PEERS0_AFTER" == *"18301"* ]]; then
    echo "[-] ERROR: Graceful deregistration failed (peer still present)!"
    exit 1
fi
if [[ "$PEERS0_AFTER" != *"18300"* ]]; then
    echo "[-] ERROR: Remaining peer was incorrectly removed!"
    exit 1
fi
echo "[+] Graceful deregistration succeeded!"

echo "=== 8. Triggering DAS Sampling after Node Leave (Verify Dynamic Routing) ==="
# Query Light Node 2 (8402) for a cell in Column 0
QUERY2_RESP=$(curl -s "http://localhost:8402/das/sample/test-block-matrix?row=0&col=0")
SUCCESS2=$(echo "$QUERY2_RESP" | jq -r '.success')

echo "Light Node 2 DAS Response (after Node 0-2 left):"
echo "$QUERY2_RESP" | jq .

if [ "$SUCCESS2" != "true" ]; then
    echo "[-] ERROR: DAS Verification failed after peer leave!"
    exit 1
fi

echo "[+] SUCCESS: Dynamic Join, Leave, and Dynamic Routing verification passed successfully!"

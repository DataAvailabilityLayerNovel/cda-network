#!/bin/bash
set -e

# Automatically resolve path and change directory to repository root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

# Default parameters
COLS=${1:-8}
STORES_PER_COL=${2:-8}
LIGHTS=${3:-2}
K=${4:-16}
K_PIECE=${5:-4}

echo "=== 0. Generating dynamic Docker Compose file ==="
GENERATED_OUT=$(python3 scripts/generate_compose.py --cols $COLS --stores-per-col $STORES_PER_COL --lights $LIGHTS --k $K --k-piece $K_PIECE --crash-on-fail)
echo "$GENERATED_OUT"

# Extract the store ports array from the script output
STORE_PORTS_STR=$(echo "$GENERATED_OUT" | grep "^STORE_PORTS=" | cut -d'=' -f2)
IFS=' ' read -r -a ports <<< "$STORE_PORTS_STR"

echo "=== 1. Cleaning up previous Docker Compose states ==="
docker compose -f docker-compose.json down -v 2>/dev/null || true

echo "=== 2. Building Docker Containers ==="
docker compose -f docker-compose.json build

echo "=== 3. Starting Services in Background ==="
docker compose -f docker-compose.json up -d

# Ensure cleanup on exit
cleanup() {
    echo "=== Cleaning up Docker resources ==="
    docker compose -f docker-compose.json down -v 2>/dev/null || true
}
# trap cleanup EXIT

echo "Waiting for services to start and register..."
# Poll Publisher health check
for i in {1..20}; do
    if curl -s http://localhost:8080/health | grep -q "healthy"; then
        echo "[+] Publisher is healthy!"
        break
    fi
    sleep 1
done

# Wait for registration to complete
sleep 5

echo "=== 4. Verifying Dynamic Registration inside Docker ==="
PEERS0=$(curl -s http://localhost:9200/bootstrap/peers | jq -c '.peers')
echo "Bootstrap Node 0 active peers list: $PEERS0"
if [[ "$PEERS0" != *"store-0-1"* ]] || [[ "$PEERS0" != *"store-0-2"* ]]; then
    echo "[-] ERROR: Dynamic registration inside Docker failed!"
    exit 1
fi
echo "[+] Dynamic registration inside Docker succeeded!"

echo "=== 5. Sending Publish Request to Publisher (ODS $K x $K) ==="
PAYLOAD=$(python3 -c "
import json, sys
k = int(sys.argv[1])
data = ['%0128x' % (i + 1) for i in range(k * k)]
print(json.dumps({'block_id': 'test-block-matrix', 'data': data}))
" $K)

echo "Publishing block to Publisher Node container..."
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:8080/publish > /dev/null

echo "Waiting for Store Nodes to complete GossipSub and coding..."
block_id="test-block-matrix"

for i in {1..10000}; do
    all_complete=true
    incomplete_port=""
    for port in "${ports[@]}"; do
        resp=$(curl -s "http://localhost:${port}/store/status/${block_id}")
        completed=$(echo "$resp" | jq -r '.completed' 2>/dev/null || echo "false")
        if [ "$completed" != "true" ]; then
            all_complete=false
            incomplete_port="$port"
            break
        fi
    done
    if [ "$all_complete" = "true" ]; then
        echo "All Store Nodes have completed GossipSub and stored pieces! Took $((i * 500))ms."
        break
    fi
    echo "[iter $i | $((i * 500))ms] Waiting... (last incomplete port: $incomplete_port)"
    if [ "$i" -eq 10000 ]; then
        echo "[-] ERROR: Store nodes did not complete within $((i * 500))ms timeout!"
        exit 1
    fi
    sleep 0.5
done

echo "=== 6. Performing DAS Sampling on Light Node 1 container for ALL EDS cells ==="
QUERY1_RESP=$(curl -s "http://localhost:9401/das/sample/test-block-matrix?all=true")
SUCCESS1=$(echo "$QUERY1_RESP" | jq -r '.success')

echo "Light Node 1 DAS Response Summary:"
echo "$QUERY1_RESP" | jq '{block_id: .block_id, success: .success, total_cells_sampled: (.results | length)}'

if [ "$SUCCESS1" != "true" ]; then
    echo "[-] ERROR: DAS Verification on Light Node 1 failed!"
    echo "First 20 failed cell details:"
    echo "$QUERY1_RESP" | jq '.results[] | select(.verified == false) | {row, col, error}' 2>/dev/null | head -n 20
    exit 1
fi

echo "=== 7. Testing Graceful Leave Mid-Test ==="
echo "Stopping store-0-2 container gracefully..."
docker compose -f docker-compose.json stop store-0-2
sleep 2

PEERS0_AFTER=$(curl -s http://localhost:9200/bootstrap/peers | jq -c '.peers')
echo "Bootstrap Node 0 active peers list after container stop: $PEERS0_AFTER"

if [[ "$PEERS0_AFTER" == *"store-0-2"* ]]; then
    echo "[-] ERROR: Graceful deregistration failed inside Docker!"
    exit 1
fi
if [[ "$PEERS0_AFTER" != *"store-0-1"* ]]; then
    echo "[-] ERROR: Remaining peer was incorrectly removed inside Docker!"
    exit 1
fi
echo "[+] Graceful deregistration inside Docker succeeded!"

echo "=== 8. Triggering DAS Sampling after Node Leave (Verify Dynamic Routing) ==="
QUERY2_RESP=$(curl -s "http://localhost:9402/das/sample/test-block-matrix?row=0&col=0")
SUCCESS2=$(echo "$QUERY2_RESP" | jq -r '.success')

echo "Light Node 2 DAS Response (after store-0-2 stopped):"
echo "$QUERY2_RESP" | jq .

if [ "$SUCCESS2" != "true" ]; then
    echo "[-] ERROR: DAS Verification failed in Docker after peer leave!"
    exit 1
fi

echo "=== 9. Exporting Representative Node Logs ==="
mkdir -p logs
docker compose -f docker-compose.json logs publisher > logs/docker_publisher.log 2>&1
docker compose -f docker-compose.json logs light-1 > logs/docker_light1.log 2>&1
docker compose -f docker-compose.json logs store-0-1 > logs/docker_store0_1.log 2>&1
docker compose -f docker-compose.json logs store-0-2 > logs/docker_store0_2.log 2>&1
docker compose -f docker-compose.json logs bootstrap-0 > logs/docker_bootstrap0.log 2>&1
docker compose -f docker-compose.json down -v
echo "[+] Representative logs exported to logs/ directory!"

echo "[+] SUCCESS: Containerized Matrix E2E verification passed successfully!"

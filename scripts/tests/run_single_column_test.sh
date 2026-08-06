#!/bin/bash
set -e

# Automatically resolve path and change directory to repository root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

# Configuration parameters for isolated single-column test
COLS=8
ACTIVE_COLS=1
STORES_PER_COL=8
LIGHTS=1
K=32
K_PIECE=8

echo "=== 0. Generating dynamic Docker Compose file with 1 active column and Pruning enabled ==="
GENERATED_OUT=$(python3 scripts/generate_compose.py --cols $COLS --active-cols $ACTIVE_COLS --stores-per-col $STORES_PER_COL --lights $LIGHTS --k $K --k-piece $K_PIECE --crash-on-fail --prune-enable --prune-ttl 15s)
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
python3 -c "
import json, sys
k = int(sys.argv[1])
data = ['%0128x' % (i + 1) for i in range(k * k)]
print(json.dumps({'block_id': 'test-block-matrix', 'data': data}))
" $K > cda_payload.json

echo "Publishing block to Publisher Node container..."
curl -s -X POST -H "Content-Type: application/json" -d @cda_payload.json http://localhost:8080/publish > /dev/null
rm -f cda_payload.json

echo "Waiting for Column 0 Store Nodes to complete GossipSub and coding..."
block_id="test-block-matrix"

start_time=$(date +%s)
for i in {1..10000}; do
    all_complete=true
    incomplete_port=""
    for port in "${ports[@]}"; do
        resp=$(curl -s --retry 3 --retry-delay 1 --retry-connrefused "http://localhost:${port}/store/status/${block_id}" || echo "{\"completed\":\"false\"}")
        completed=$(echo "$resp" | jq -r '.completed' 2>/dev/null || echo "false")
        if [ "$completed" != "true" ]; then
            all_complete=false
            incomplete_port="$port"
            break
        fi
    done
    if [ "$all_complete" = "true" ]; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "[+] SUCCESS: All Column 0 Store Nodes completed coding and achieved IsComplete!"
        echo "[+] Propagation & Coding completion time: ${duration} seconds."
        break
    fi
    echo "[iter $i | $((i * 3000))ms] Waiting... (last incomplete port: $incomplete_port)"
    if [ "$i" -eq 10000 ]; then
        echo "[-] ERROR: Store nodes did not complete within timeout!"
        exit 1
    fi
    sleep 3
done

echo "Waiting 25 seconds to guarantee that Store Nodes prune their raw pieces (TTL=15s)..."
sleep 30

echo "=== 6. Performing DAS Sampling on Light Node 1 container ==="
# Calculate N and cols_per_net_col dynamically
N=$((2 * K))
COLS_PER_NET_COL=$((N / COLS))
R_STEP=$((N / 4))

# We only sample custody cells in the columns handled by the active network column (0 to COLS_PER_NET_COL - 1).
for ((r = 0; r < N; r += R_STEP)); do
    for ((c = 0; c < COLS_PER_NET_COL; c++)); do
        resp=$(curl -s "http://localhost:9401/das/sample/test-block-matrix?row=${r}&col=${c}")
        success=$(echo "$resp" | jq -r '.success')
        if [ "$success" != "true" ]; then
            echo "[-] ERROR: DAS Verification failed for cell [${r}, ${c}]!"
            echo "$resp"
            exit 1
        fi
        echo "[+] Successfully sampled cell [${r}, ${c}]"
    done
done
echo "[+] All samples on active column succeeded!"

echo "=== 7. Testing Store Node Resiliency & Failover on Column 0 (Pruned Recovery) ==="
echo "Stopping the Custody Store Node for cell [0, 0]: store-0-1..."
docker compose -f docker-compose.json stop store-0-1
sleep 2

PEERS0_AFTER=$(curl -s http://localhost:9200/bootstrap/peers | jq -c '.peers')
echo "Bootstrap Node 0 active peers list after stopping store-0-1: $PEERS0_AFTER"

if [[ "$PEERS0_AFTER" == *"store-0-1"* ]]; then
    echo "[-] ERROR: Deregistration failed for store-0-1!"
    exit 1
fi
echo "[+] Deregistration of stopped store-0-1 succeeded!"

echo "=== 8. Triggering DAS Sampling after Custody Node Outage (Verify Recovery from Pruned Nodes) ==="
# We query cell [0, 0]. Since store-0-1 (the custody store) is offline, this forces the Light Node to query
# other store nodes (store-0-2 to store-0-8) which only hold the single recoded pieces (due to pruning),
# gather 4 independent pieces, and verify cell [0, 0].
resp=$(curl -s "http://localhost:9401/das/sample/test-block-matrix?row=0&col=0")
success=$(echo "$resp" | jq -r '.success')
if [ "$success" != "true" ]; then
    echo "[-] ERROR: DAS Verification failed for cell [0, 0] after custody node went offline!"
    echo "$resp"
    exit 1
fi
echo "[+] SUCCESS: Light Node successfully sampled and recovered cell [0, 0] by gathering recoded pieces from multiple pruned store nodes!"

echo "=== 9. Cleaning up resources ==="
docker compose -f docker-compose.json down -v
echo "[+] Cleaned up Docker resources successfully."
echo "[+] SUCCESS: Single column test verification passed successfully!"

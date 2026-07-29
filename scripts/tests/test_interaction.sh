#!/bin/bash
set -e

echo "=== 1. Building Nodes ==="
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go

echo "=== 2. Starting services in background ==="
# Store Node (Port 8082)
# Custody coordinate row = 0, col = 0, connects to Publisher (8080) and Bootstrap (8081)
./bin/store -port 8082 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k 8 > store.log 2>&1 &
STORE_PID=$!

# Bootstrap Node (Port 8081)
# ColumnID = 0, connects to Publisher (8080) and Store Node (8082)
./bin/bootstrap -port 8081 -col 0 -store http://localhost:8082 -publisher http://localhost:8080 -k 8 > bootstrap.log 2>&1 &
BOOTSTRAP_PID=$!

# Publisher Node (Port 8080)
# Connects to Column 0 Bootstrap Node (8081)
./bin/publisher -port 8080 -bootstrap http://localhost:8081 -k 8 > publisher.log 2>&1 &
PUBLISHER_PID=$!

# Ensure cleanup on exit
cleanup() {
    echo "=== Cleaning up background processes ==="
    kill $PUBLISHER_PID $BOOTSTRAP_PID $STORE_PID 2>/dev/null || true
    rm -rf bin/
    wait $PUBLISHER_PID $BOOTSTRAP_PID $STORE_PID 2>/dev/null || true
}
trap cleanup EXIT

# Allow servers to start
echo "Waiting for services to start..."
sleep 3

echo "=== 3. Sending Publish Request to Publisher (ODS) ==="
# ODS requires K * K = 16 cells. Each cell is 64-byte hex string (128 characters).
PAYLOAD='{
  "block_id": "test-block-1",
  "data": [
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
    "00000000000000000000000000000000000000000000AB0000000000000000000000000000000000000000000000000000000000000000000000000000000002",
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003",
    "00000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000004",
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005",
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006",
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000007",
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008",
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000009",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e",
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f",
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010"
  ]
}'

echo "Sending ODS Publish request to Publisher..."
PUB_RESP=$(curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:8080/publish)

echo "Publisher Response:"
echo "$PUB_RESP" | jq . 2>/dev/null || echo "$PUB_RESP"

if [[ "$PUB_RESP" != *"block_id"* ]]; then
  echo "[-] ERROR: Publisher response does not contain block_id!"
  exit 1
fi
if [[ "$PUB_RESP" != *"commits_root"* ]]; then
  echo "[-] ERROR: Publisher response does not contain commits_root!"
  exit 1
fi
echo "[+] SUCCESS: Block Header published successfully!"

sleep 2

echo "=== 4. Fetching Sync data from Bootstrap Node ==="
SYNC_RESP=$(curl -s http://localhost:8081/bootstrap/sync/test-block-1)

echo "Bootstrap Node Sync Response:"
echo "$SYNC_RESP" | jq . 2>/dev/null || echo "$SYNC_RESP"

if [[ "$SYNC_RESP" != *"proofs"* ]]; then
  echo "[-] ERROR: Bootstrap sync response does not contain proofs!"
  exit 1
fi

echo "=== 5. Verification Flow Audit ==="
echo "Filtering verifier checks from Bootstrap Node logs:"
grep -E "\[Verifier\]" bootstrap.log || true
echo "Filtering verifier checks from Store Node logs:"
grep -E "\[StoreNode\]|\[GossipSub\]" store.log || true
echo "--------------------------------------------------"

echo "=== 6. Full Logs Inspection ==="
echo "--- Publisher Node Log ---"
cat publisher.log
echo "--------------------------"
echo "--- Bootstrap Node Log ---"
cat bootstrap.log
echo "--------------------------"
echo "--- Store Node Log ---"
cat store.log
echo "--------------------------"

echo "[+] SUCCESS: Integration test passed successfully!"

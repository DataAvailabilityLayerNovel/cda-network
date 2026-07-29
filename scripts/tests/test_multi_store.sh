#!/bin/bash
set -e

echo "=== 1. Building Nodes ==="
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go

echo "=== 2. Starting services in background ==="
# Store Node 1 (Port 8082, Row 0, Col 0, peers with 8083 and 8084)
./bin/store -port 8082 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k 4 -peers http://localhost:8083,http://localhost:8084 > store1.log 2>&1 &
STORE1_PID=$!

# Store Node 2 (Port 8083, Row 0, Col 0, peers with 8082 and 8084)
./bin/store -port 8083 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k 4 -peers http://localhost:8082,http://localhost:8084 > store2.log 2>&1 &
STORE2_PID=$!

# Store Node 3 (Port 8084, Row 0, Col 0, peers with 8082 and 8083)
./bin/store -port 8084 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8081 -k 4 -peers http://localhost:8082,http://localhost:8083 > store3.log 2>&1 &
STORE3_PID=$!

# Bootstrap Node (Port 8081)
# ColumnID = 0, connects to Publisher (8080) and Store Node 1 (8082)
./bin/bootstrap -port 8081 -col 0 -store http://localhost:8082 -publisher http://localhost:8080 -k 4 > bootstrap.log 2>&1 &
BOOTSTRAP_PID=$!

# Publisher Node (Port 8080)
# Connects to Column 0 Bootstrap Node (8081)
./bin/publisher -port 8080 -bootstrap http://localhost:8081 -k 4 > publisher.log 2>&1 &
PUBLISHER_PID=$!

# Ensure cleanup on exit
cleanup() {
    echo "=== Cleaning up background processes ==="
    kill $PUBLISHER_PID $BOOTSTRAP_PID $STORE1_PID $STORE2_PID $STORE3_PID 2>/dev/null || true
    rm -rf bin/
    wait $PUBLISHER_PID $BOOTSTRAP_PID $STORE1_PID $STORE2_PID $STORE3_PID 2>/dev/null || true
}
trap cleanup EXIT

# Allow servers to start
echo "Waiting for services to start..."
sleep 3

echo "=== 3. Sending Publish Request to Publisher (ODS) ==="
# Since k=4, ODS requires k*k=16 cells. Each cell is 64-byte hex string (128 characters).
PAYLOAD='{
  "block_id": "test-block-2",
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

echo "Publishing block..."
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:8080/publish > /dev/null

echo "Waiting 5 seconds for block distribution, GossipSub, and P2P recoding..."
sleep 5

echo "=== 4. Querying Store Node 2 (8083) to pull pieces from peers & reconstruct cell [0, 0] ==="
# Store Node 2 only got 2 recoded pieces via gossip from Store Node 1.
# It does NOT have enough pieces (k=4) locally.
# It will request the 3 raw pieces from Store Node 1, filter out redundant ones, gather 5 pieces, and reconstruct cell [0, 0].
QUERY_RESP=$(curl -s http://localhost:8083/store/cell/retrieve/test-block-2/0/0)

echo "Query Response:"
echo "$QUERY_RESP" | jq .

RECOVERED=$(echo "$QUERY_RESP" | jq -r '.recovered')
DATA=$(echo "$QUERY_RESP" | jq -r '.data')
EXPECTED="00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"

if [ "$RECOVERED" != "true" ]; then
    echo "[-] ERROR: Cell recovery failed! recovered=false"
    exit 1
fi

if [ "$DATA" != "$EXPECTED" ]; then
    echo "[-] ERROR: Recovered data does not match expected original cell data!"
    echo "    Expected: $EXPECTED"
    echo "    Got:      $DATA"
    exit 1
fi

echo "[+] SUCCESS: Multi-Store Gossip, Retrieval, and Cell Reconstruction verified successfully!"

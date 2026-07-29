#!/bin/bash
set -e

echo "=== 1. Building Nodes ==="
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go

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
./bin/publisher -port 8080 -bootstrap http://localhost:8081 -k 4 > publisher.log 2>&1 &
PUBLISHER_PID=$!

# Light Node / DAS Verifier (Port 8085)
./bin/light -port 8085 -publisher http://localhost:8080 -stores http://localhost:8082,http://localhost:8083,http://localhost:8084 -k 4 > light.log 2>&1 &
LIGHT_PID=$!

# Ensure cleanup on exit
cleanup() {
    echo "=== Cleaning up background processes ==="
    kill $PUBLISHER_PID $BOOTSTRAP_PID $STORE1_PID $STORE2_PID $STORE3_PID $LIGHT_PID 2>/dev/null || true
    rm -rf bin/
    wait $PUBLISHER_PID $BOOTSTRAP_PID $STORE1_PID $STORE2_PID $STORE3_PID $LIGHT_PID 2>/dev/null || true
}
trap cleanup EXIT

# Allow servers to start
echo "Waiting for services to start..."
sleep 3

echo "=== 3. Sending Publish Request to Publisher (ODS) ==="
# Since k=4, ODS requires k*k=16 cells. Each cell is 64-byte hex string (128 characters).
PAYLOAD='{
  "block_id": "test-block-3",
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

echo "=== 4. Triggering DAS Sampling via Light Node (8085) ==="
# Light Node will sample 4 random cells in column 0, verify locally, and return JSON
QUERY_RESP=$(curl -s "http://localhost:8085/das/sample/test-block-3?samples=4")

echo "Light Node DAS Response:"
echo "$QUERY_RESP" | jq .

SUCCESS=$(echo "$QUERY_RESP" | jq -r '.success')

if [ "$SUCCESS" != "true" ]; then
    echo "[-] ERROR: DAS Verification failed!"
    exit 1
fi

echo "[+] SUCCESS: Data Availability Sampling (DAS) E2E verification passed successfully!"

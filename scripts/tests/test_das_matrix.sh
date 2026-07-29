#!/bin/bash
set -e

echo "=== 1. Building Nodes ==="
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go

echo "=== 2. Creating Configuration Files ==="
cat <<EOF > publisher_config.json
{
  "api_port": 8080,
  "k": 4,
  "bootstrap_peers": {
    "0": "http://localhost:8090",
    "1": "http://localhost:8090",
    "2": "http://localhost:8091",
    "3": "http://localhost:8091",
    "4": "http://localhost:8092",
    "5": "http://localhost:8092",
    "6": "http://localhost:8093",
    "7": "http://localhost:8093"
  }
}
EOF

echo "=== 3. Starting Services in Background ==="

# --- Network Column 0 (Handles data cols 0, 1) ---
# Bootstrap Node 0 (Port 8090)
./bin/bootstrap -port 8090 -col 0 -store http://localhost:8082 -publisher http://localhost:8080 -k 4 > bootstrap0.log 2>&1 &
BOOTSTRAP0_PID=$!
# Store Node 0-1 (Port 8082) - registers to Bootstrap 8090
./bin/store -port 8082 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8090 -k 4 -myaddr http://localhost:8082 > store0_1.log 2>&1 &
STORE0_1_PID=$!
# Store Node 0-2 (Port 8083) - registers to Bootstrap 8090
./bin/store -port 8083 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8090 -k 4 -myaddr http://localhost:8083 > store0_2.log 2>&1 &
STORE0_2_PID=$!

# --- Network Column 1 (Handles data cols 2, 3) ---
# Bootstrap Node 1 (Port 8091)
./bin/bootstrap -port 8091 -col 2 -store http://localhost:8084 -publisher http://localhost:8080 -k 4 > bootstrap1.log 2>&1 &
BOOTSTRAP1_PID=$!
# Store Node 1-1 (Port 8084) - registers to Bootstrap 8091
./bin/store -port 8084 -row 0 -col 2 -publisher http://localhost:8080 -bootstrap http://localhost:8091 -k 4 -myaddr http://localhost:8084 > store1_1.log 2>&1 &
STORE1_1_PID=$!
# Store Node 1-2 (Port 8085) - registers to Bootstrap 8091
./bin/store -port 8085 -row 0 -col 2 -publisher http://localhost:8080 -bootstrap http://localhost:8091 -k 4 -myaddr http://localhost:8085 > store1_2.log 2>&1 &
STORE1_2_PID=$!

# --- Network Column 2 (Handles data cols 4, 5) ---
# Bootstrap Node 2 (Port 8092)
./bin/bootstrap -port 8092 -col 4 -store http://localhost:8086 -publisher http://localhost:8080 -k 4 > bootstrap2.log 2>&1 &
BOOTSTRAP2_PID=$!
# Store Node 2-1 (Port 8086) - registers to Bootstrap 8092
./bin/store -port 8086 -row 0 -col 4 -publisher http://localhost:8080 -bootstrap http://localhost:8092 -k 4 -myaddr http://localhost:8086 > store2_1.log 2>&1 &
STORE2_1_PID=$!
# Store Node 2-2 (Port 8087) - registers to Bootstrap 8092
./bin/store -port 8087 -row 0 -col 4 -publisher http://localhost:8080 -bootstrap http://localhost:8092 -k 4 -myaddr http://localhost:8087 > store2_2.log 2>&1 &
STORE2_2_PID=$!

# --- Network Column 3 (Handles data cols 6, 7) ---
# Bootstrap Node 3 (Port 8093)
./bin/bootstrap -port 8093 -col 6 -store http://localhost:8088 -publisher http://localhost:8080 -k 4 > bootstrap3.log 2>&1 &
BOOTSTRAP3_PID=$!
# Store Node 3-1 (Port 8088) - registers to Bootstrap 8093
./bin/store -port 8088 -row 0 -col 6 -publisher http://localhost:8080 -bootstrap http://localhost:8093 -k 4 -myaddr http://localhost:8088 > store3_1.log 2>&1 &
STORE3_1_PID=$!
# Store Node 3-2 (Port 8089) - registers to Bootstrap 8093
./bin/store -port 8089 -row 0 -col 6 -publisher http://localhost:8080 -bootstrap http://localhost:8093 -k 4 -myaddr http://localhost:8089 > store3_2.log 2>&1 &
STORE3_2_PID=$!

# --- Publisher Node (Port 8080) ---
./bin/publisher -config publisher_config.json > publisher.log 2>&1 &
PUBLISHER_PID=$!

# --- Light Nodes ---
# Light Node 1 (Port 8095) - Maps bootstraps instead of hardcoded stores!
./bin/light -port 8095 -publisher http://localhost:8080 -bootstraps "0:http://localhost:8090;1:http://localhost:8091;2:http://localhost:8092;3:http://localhost:8093" -k 4 > light1.log 2>&1 &
LIGHT1_PID=$!

# Light Node 2 (Port 8096)
./bin/light -port 8096 -publisher http://localhost:8080 -bootstraps "0:http://localhost:8090;1:http://localhost:8091;2:http://localhost:8092;3:http://localhost:8093" -k 4 > light2.log 2>&1 &
LIGHT2_PID=$!

# Ensure cleanup on exit
cleanup() {
    echo "=== Cleaning up background processes ==="
    kill $PUBLISHER_PID $BOOTSTRAP0_PID $BOOTSTRAP1_PID $BOOTSTRAP2_PID $BOOTSTRAP3_PID \
         $STORE0_1_PID $STORE0_2_PID $STORE1_1_PID $STORE1_2_PID $STORE2_1_PID $STORE2_2_PID $STORE3_1_PID $STORE3_2_PID \
         $LIGHT1_PID $LIGHT2_PID 2>/dev/null || true
    rm -rf bin/ publisher_config.json
    wait $PUBLISHER_PID $BOOTSTRAP0_PID $BOOTSTRAP1_PID $BOOTSTRAP2_PID $BOOTSTRAP3_PID \
         $STORE0_1_PID $STORE0_2_PID $STORE1_1_PID $STORE1_2_PID $STORE2_1_PID $STORE2_2_PID $STORE3_1_PID $STORE3_2_PID \
         $LIGHT1_PID $LIGHT2_PID 2>/dev/null || true
}
trap cleanup EXIT

# Allow servers to start
echo "Waiting for services to start..."
sleep 4

echo "=== 4. Verifying Dynamic Registration (Phase 1) ==="
# Retrieve bootstrap 0 active peers list
PEERS0=$(curl -s http://localhost:8090/bootstrap/peers | jq -c '.peers')
echo "Bootstrap Node 0 active peers list: $PEERS0"
# Verify Store Node 0-1 and 0-2 are both registered
if [[ "$PEERS0" != *"http://localhost:8082"* ]] || [[ "$PEERS0" != *"http://localhost:8083"* ]]; then
    echo "[-] ERROR: Dynamic registration failed!"
    exit 1
fi
echo "[+] Dynamic registration succeeded!"

echo "=== 5. Sending Publish Request to Publisher (ODS 4x4) ==="
PAYLOAD='{
  "block_id": "test-block-matrix",
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

echo "Publishing block to Publisher Node..."
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:8080/publish > /dev/null

echo "Waiting for Store Nodes to complete GossipSub and coding..."
block_id="test-block-matrix"
# Poll all 8 Store Nodes
ports=(8082 8083 8084 8085 8086 8087 8088 8089)

for i in {1..100}; do
    all_complete=true
    for port in "${ports[@]}"; do
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

echo "=== 6. Performing DAS Sampling on Light Node 1 (8095) for ALL EDS cells ==="
QUERY1_RESP=$(curl -s "http://localhost:8095/das/sample/test-block-matrix?all=true")
SUCCESS1=$(echo "$QUERY1_RESP" | jq -r '.success')

echo "Light Node 1 DAS Response Summary:"
echo "$QUERY1_RESP" | jq '{block_id: .block_id, success: .success, total_cells_sampled: (.results | length)}'

if [ "$SUCCESS1" != "true" ]; then
    echo "[-] ERROR: DAS Verification on Light Node 1 failed!"
    exit 1
fi

echo "=== 7. Testing Graceful Leave Mid-Test ==="
# Stop Store Node 0-2 (Port 8083) via SIGTERM
echo "Stopping Store Node 0-2 (Port 8083) gracefully..."
kill -15 $STORE0_2_PID
sleep 2

# Retrieve bootstrap 0 active peers list again
PEERS0_AFTER=$(curl -s http://localhost:8090/bootstrap/peers | jq -c '.peers')
echo "Bootstrap Node 0 active peers list after deregister: $PEERS0_AFTER"

# Verify Store Node 0-2 is removed and 0-1 remains
if [[ "$PEERS0_AFTER" == *"http://localhost:8083"* ]]; then
    echo "[-] ERROR: Graceful deregistration failed (peer still present)!"
    exit 1
fi
if [[ "$PEERS0_AFTER" != *"http://localhost:8082"* ]]; then
    echo "[-] ERROR: Remaining peer was incorrectly removed!"
    exit 1
fi
echo "[+] Graceful deregistration succeeded!"

echo "=== 8. Triggering DAS Sampling after Node Leave (Verify Dynamic Routing) ==="
# Query Light Node 2 (8096) for a cell in Column 0
# It should dynamically query bootstrap0, find only store0-1 (8082) active, retrieve from it, and verify successfully
QUERY2_RESP=$(curl -s "http://localhost:8096/das/sample/test-block-matrix?row=0&col=0")
SUCCESS2=$(echo "$QUERY2_RESP" | jq -r '.success')

echo "Light Node 2 DAS Response (after Node 0-2 left):"
echo "$QUERY2_RESP" | jq .

if [ "$SUCCESS2" != "true" ]; then
    echo "[-] ERROR: DAS Verification failed after peer leave!"
    exit 1
fi

echo "[+] SUCCESS: Dynamic Join, Leave, and Dynamic Routing verification passed successfully!"

#!/bin/bash
# publish.sh - Manually publish a block to the running CDA Network
# Usage: ./scripts/publish.sh [BLOCK_ID] [PUBLISHER_URL] [K] [CELL_SIZE]

BLOCK_ID=${1:-"manual-block-1"}
PUBLISHER_URL=${2:-"http://localhost:8080"}
K=${3:-8}
CELL_SIZE=${4:-64}

echo "Generating $K x $K ODS cells ($((K*K)) cells, ${CELL_SIZE}B each) for block '$BLOCK_ID'..."

PAYLOAD=$(python3 -c "
import json, sys, random
block_id = sys.argv[1]
k = int(sys.argv[2])
cell_size = int(sys.argv[3])
hex_len = cell_size * 2
data = ['%0*x' % (hex_len, random.randint(1, 1_000_000)) for _ in range(k * k)]
print(json.dumps({'block_id': block_id, 'data': data}))
" "$BLOCK_ID" "$K" "$CELL_SIZE")

echo "Publishing block '$BLOCK_ID' to Publisher at $PUBLISHER_URL/publish..."
RESP=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$PUBLISHER_URL/publish")
HTTP_BODY=$(echo "$RESP" | sed -e '$d')
HTTP_CODE=$(echo "$RESP" | tail -n1 | sed -e 's/HTTP_CODE://')

if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
    echo "[+] SUCCESS (HTTP $HTTP_CODE): Block '$BLOCK_ID' published successfully!"
    echo "$HTTP_BODY" | jq . 2>/dev/null || echo "$HTTP_BODY"
else
    echo "[-] FAILED (HTTP $HTTP_CODE): Could not publish block '$BLOCK_ID':"
    echo "$HTTP_BODY"
    exit 1
fi

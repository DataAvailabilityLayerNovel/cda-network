#!/bin/bash
# das.sh - Manually trigger DAS on a Light Node

BLOCK_ID=${1:-"manual-block-1"}
LIGHT_URL=${2:-"http://localhost:9401"}

echo "Running DAS for all cells in block '$BLOCK_ID' on Light Node at $LIGHT_URL..."
RAW_RESP=$(curl -s -w "\nHTTP_CODE:%{http_code}" "$LIGHT_URL/das/sample/$BLOCK_ID?all=true")

HTTP_BODY=$(echo "$RAW_RESP" | sed -e '$d')
HTTP_CODE=$(echo "$RAW_RESP" | tail -n1 | sed -e 's/HTTP_CODE://')

if [ "$HTTP_CODE" != "200" ]; then
    echo "[-] DAS request failed with HTTP $HTTP_CODE:"
    echo "$HTTP_BODY"
    exit 1
fi

# Format output and verify success
if echo "$HTTP_BODY" | jq -e '.success == true' > /dev/null 2>&1; then
    echo "$HTTP_BODY" | jq .
    exit 0
elif echo "$HTTP_BODY" | jq . > /dev/null 2>&1; then
    echo "$HTTP_BODY" | jq .
    # If success field is present and false, fail
    if echo "$HTTP_BODY" | jq -e '.success == false' > /dev/null 2>&1; then
        exit 1
    fi
    exit 0
else
    echo "$HTTP_BODY"
    exit 1
fi

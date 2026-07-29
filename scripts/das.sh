#!/bin/bash
# das.sh - Manually trigger DAS on a Light Node

BLOCK_ID=${1:-"manual-block-1"}
LIGHT_URL=${2:-"http://localhost:8095"}

echo "Running DAS for all cells in block '$BLOCK_ID' on Light Node at $LIGHT_URL..."
QUERY_RESP=$(curl -s "$LIGHT_URL/das/sample/$BLOCK_ID?all=true")
echo "$QUERY_RESP" | jq . || echo "$QUERY_RESP"

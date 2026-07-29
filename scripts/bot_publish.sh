#!/bin/bash
# bot_publish.sh - Liên tục đẩy dữ liệu lên mạng (Publisher Bot)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INTERVAL=${1:-5}
BLOCK_PREFIX="bot-block"
COUNTER=1

echo "[Publish Bot] Đang khởi động... Đẩy 1 block mới mỗi $INTERVAL giây."

while true; do
    BLOCK_ID="${BLOCK_PREFIX}-${COUNTER}"
    echo "========================================="
    echo "[Publish Bot] Đang đẩy block: $BLOCK_ID"
    "$SCRIPT_DIR/publish.sh" "$BLOCK_ID" "http://localhost:8080"
    
    COUNTER=$((COUNTER + 1))
    sleep $INTERVAL
done

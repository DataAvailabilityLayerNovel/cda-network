#!/bin/bash
# bot_das.sh - Liên tục thực hiện DAS (Light Node Bot)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INTERVAL=${1:-5}
WAIT_TIME=5
BLOCK_PREFIX="bot-block"
COUNTER=1

echo "[DAS Bot] Đang khởi động... Chờ $WAIT_TIME giây trước khi bắt đầu lấy mẫu block đầu tiên."
sleep $WAIT_TIME

while true; do
    BLOCK_ID="${BLOCK_PREFIX}-${COUNTER}"
    echo "========================================="
    echo "[DAS Bot] Thực hiện lấy mẫu DAS cho block: $BLOCK_ID"
    "$SCRIPT_DIR/das.sh" "$BLOCK_ID" "http://localhost:8095"
    
    COUNTER=$((COUNTER + 1))
    sleep $INTERVAL
done

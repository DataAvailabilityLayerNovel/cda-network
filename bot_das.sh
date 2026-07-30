#!/bin/bash
# bot_das.sh - Liên tục thực hiện DAS (Light Node Bot)

INTERVAL=${1:-5} # Tốc độ query bằng với tốc độ publish
WAIT_TIME=5      # Đợi 3 giây để block đầu tiên kịp lan truyền và verify xong
BLOCK_PREFIX="bot-block"
COUNTER=1

echo "[DAS Bot] Đang khởi động... Chờ $WAIT_TIME giây trước khi bắt đầu lấy mẫu block đầu tiên."
sleep $WAIT_TIME

while true; do
    BLOCK_ID="${BLOCK_PREFIX}-${COUNTER}"
    echo "========================================="
    echo "[DAS Bot] Thực hiện lấy mẫu DAS cho block: $BLOCK_ID"
    ./das.sh "$BLOCK_ID" "http://localhost:8095"
    
    COUNTER=$((COUNTER + 1))
    sleep $INTERVAL
done

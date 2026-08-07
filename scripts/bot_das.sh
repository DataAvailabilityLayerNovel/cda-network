#!/bin/bash
# bot_das.sh - Liên tục thực hiện DAS (Light Node Bot)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INTERVAL=${1:-5}
START_HEIGHT=${2:-1}

# Tự động phát hiện và bóc tách chiều cao nếu người dùng truyền vào dạng block-X
if [[ "$INTERVAL" =~ block-([0-9]+) ]]; then
    START_HEIGHT="${BASH_REMATCH[1]}"
    INTERVAL=5
fi
if [[ "$START_HEIGHT" =~ block-([0-9]+) ]]; then
    START_HEIGHT="${BASH_REMATCH[1]}"
fi

# Đảm bảo INTERVAL là số nguyên, nếu không gán mặc định là 5
if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]]; then
    INTERVAL=5
fi

WAIT_TIME=5
BLOCK_PREFIX="block"
COUNTER=$START_HEIGHT

echo "[DAS Bot] Đang khởi động... Bắt đầu lấy mẫu từ Block: ${BLOCK_PREFIX}-${COUNTER} (Interval: ${INTERVAL}s)"
sleep $WAIT_TIME

while true; do
    BLOCK_ID="${BLOCK_PREFIX}-${COUNTER}"
    echo "========================================="
    echo "[DAS Bot] Thực hiện lấy mẫu DAS cho block: $BLOCK_ID"
    "$SCRIPT_DIR/das.sh" "$BLOCK_ID" "http://localhost:9401"
    
    COUNTER=$((COUNTER + 1))
    sleep $INTERVAL
done

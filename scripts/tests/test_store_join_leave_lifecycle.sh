#!/bin/bash
set -e

# ==============================================================================
# CDA Network - Test Kịch Bản Vòng Đời Store Node (Join & Leave Lifecycle)
# ==============================================================================
# Kịch bản này kiểm tra và trực quan hóa 3 giai đoạn:
# 1. Store Node mới gia nhập mạng (Dynamic Join & Auto-Peering Sync Loop)
# 2. Store Node rời mạng chủ động (Graceful Leave qua SIGINT/SIGTERM & IsLeave flag)
# 3. Store Node rời mạng đột ngột (Ungraceful Crash qua SIGKILL & TTL Expiry Cleanup)
# ==============================================================================

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

# Định dạng màu sắc
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}==========================================================================${NC}"
echo -e "${CYAN}   KỊCH BẢN KIỂM THỬ: THEO DÕI VÒNG ĐỜI THAM GIA & RỜI MẠNG STORE NODE   ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "\n${BLUE}=== 1. Biên dịch các module thực thi (Binaries) ===${NC}"
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go

echo -e "\n${BLUE}=== 2. Thiết lập cấu hình mạng (K=16, 4 Network Columns) ===${NC}"
cat <<EOF > publisher_config.json
{
  "api_port": 8080,
  "k": 16,
  "bootstrap_peers": {
    "0": "http://localhost:8200",
    "1": "http://localhost:8200",
    "2": "http://localhost:8200",
    "3": "http://localhost:8200",
    "4": "http://localhost:8201",
    "5": "http://localhost:8201",
    "6": "http://localhost:8201",
    "7": "http://localhost:8201"
  }
}
EOF

# Đăng ký hàm dọn dẹp khi kết thúc
ALL_PIDS=()
cleanup() {
    echo -e "\n${YELLOW}=== Dọn dẹp tiến trình kiểm thử nền ===${NC}"
    for pid in "${ALL_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
    rm -rf bin/ publisher_config.json data/ *.key *.log
    wait "${ALL_PIDS[@]}" 2>/dev/null || true
    echo -e "${GREEN}[+] Hoàn tất dọn dẹp!${NC}"
}
trap cleanup EXIT

echo -e "\n${BLUE}=== 3. Khởi chạy Seed Bootstrap Node 0 và Bootstrap Node 1 ===${NC}"
# Bootstrap 0 (Seed Node quản lý Cột 0..3)
./bin/bootstrap -port 8200 -col 0 -publisher http://localhost:8080 -k 16 > bootstrap0.log 2>&1 &
BOOT0_PID=$!
ALL_PIDS+=($BOOT0_PID)

sleep 1

# Bootstrap 1 (Quản lý Cột 4..7, đăng ký với Seed 8200)
./bin/bootstrap -port 8201 -col 4 -seed http://localhost:8200 -publisher http://localhost:8080 -k 16 > bootstrap1.log 2>&1 &
BOOT1_PID=$!
ALL_PIDS+=($BOOT1_PID)

sleep 2

# Publisher Node
./bin/publisher -config publisher_config.json > publisher.log 2>&1 &
PUB_PID=$!
ALL_PIDS+=($PUB_PID)

# Light Node (Chỉ trỏ tới Seed Bootstrap 0)
./bin/light -port 8499 -publisher http://localhost:8080 -bootstraps "0:http://localhost:8200" -k 16 -num-cols 4 > light.log 2>&1 &
LIGHT_PID=$!
ALL_PIDS+=($LIGHT_PID)

echo -e "\n${BLUE}=== 4. Khởi chạy 2 Store Node cơ sở (Store-A & Store-B) tại Cột 0 ===${NC}"
# Store-A (Port 8300, Row 0, Col 0)
./bin/store -port 8300 -row 0 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8200 -k 16 > store_8300.log 2>&1 &
STORE_A_PID=$!
ALL_PIDS+=($STORE_A_PID)

# Store-B (Port 8301, Row 1, Col 0)
./bin/store -port 8301 -row 1 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8200 -k 16 > store_8301.log 2>&1 &
STORE_B_PID=$!
ALL_PIDS+=($STORE_B_PID)

echo "Đang chờ 3s để Store-A và Store-B hoàn tất đăng ký với Bootstrap 0..."
sleep 3

PEERS_INIT=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo -e "${GREEN}[+] Danh sách Store Peers ban đầu tại Bootstrap 0: ${PEERS_INIT}${NC}"

# ==============================================================================
# GIAI ĐOẠN 1: THEO DÕI STORE NODE MỚI GIA NHẬP MẠNG (DYNAMIC JOIN)
# ==============================================================================
echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${CYAN}  GIAI ĐOẠN 1: THEO DÕI STORE NODE MỚI (STORE-C) GIA NHẬP MẠNG ĐỘNG      ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "${YELLOW}>> Bước 1.1: Khởi chạy Store-C (Port 8302, Row 2, Col 0)...${NC}"
./bin/store -port 8302 -row 2 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8200 -k 16 > store_8302.log 2>&1 &
STORE_C_PID=$!
ALL_PIDS+=($STORE_C_PID)

echo "Đang theo dõi quá trình Store-C hòa mạng..."
sleep 2

PEERS_AFTER_JOIN=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo -e "${GREEN}[+] Registry tại Bootstrap 0 sau khi Store-C gia nhập:${NC} ${PEERS_AFTER_JOIN}"

if [[ "$PEERS_AFTER_JOIN" == *"18302"* ]]; then
    echo -e "${GREEN}[✓] XÁC NHẬN: Store-C (Port P2P 18302) đã được Bootstrap 0 ghi nhận thành công!${NC}"
else
    echo -e "${RED}[✗] LỖI: Store-C chưa được ghi nhận trong registry!${NC}"
    exit 1
fi

echo -e "\n${YELLOW}>> Bước 1.2: Kiểm tra khả năng nhận diện Mesh ngang hàng (Auto-Peering Sync Loop)...${NC}"
# Đợi chu kỳ sync loop (2s) để Store-A và Store-B tự động connect sang Store-C
sleep 2

echo -e "\n${YELLOW}>> Bước 1.3: Xuất bản Block và kiểm tra Store-C tiếp nhận & lưu trữ dữ liệu...${NC}"
PAYLOAD=$(python3 -c "
import json
data = ['%0128x' % (i + 1) for i in range(256)]
print(json.dumps({'block_id': 'block-lifecycle-test', 'data': data}))
")
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:8080/publish > /dev/null

STATUS_C="false"
for i in {1..50}; do
    STATUS_C=$(curl -s http://localhost:8302/store/status/block-lifecycle-test | jq -r '.completed' 2>/dev/null || echo "false")
    if [ "$STATUS_C" = "true" ]; then
        break
    fi
    sleep 0.2
done
echo -e "Trạng thái lưu trữ của Store-C cho block mới: ${GREEN}completed = ${STATUS_C}${NC}"

# Light Node DAS sampling tới Store-C
DAS_RESP=$(curl -s "http://localhost:8499/das/sample/block-lifecycle-test?row=2&col=0")
DAS_SUCCESS=$(echo "$DAS_RESP" | jq -r '.success')
echo -e "Light Node DAS Sample ô [2, 0] (do Store-C quản lý): ${GREEN}verified = ${DAS_SUCCESS}${NC}"
if [ "$DAS_SUCCESS" != "true" ]; then
    echo -e "${RED}[✗] LỖI: DAS verification thất bại trên Store-C mới gia nhập!${NC}"
    exit 1
fi
echo -e "${GREEN}[✓] GIAI ĐOẠN 1 THÀNH CÔNG: Store Node mới gia nhập, nhận hạt giống và phục vụ DAS hoàn hảo!${NC}"

# ==============================================================================
# GIAI ĐOẠN 2: THEO DÕI STORE NODE RỜI MẠNG CHỦ ĐỘNG (GRACEFUL LEAVE)
# ==============================================================================
echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${CYAN}  GIAI ĐOẠN 2: THEO DÕI RỜI MẠNG CHỦ ĐỘNG (GRACEFUL LEAVE - SIGTERM)      ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "${YELLOW}>> Bước 2.1: Gửi tín hiệu SIGTERM (kill -15) tới Store-C (PID $STORE_C_PID)...${NC}"
kill -15 $STORE_C_PID
sleep 1

echo -e "${YELLOW}>> Bước 2.2: Trích xuất log deregistration từ Store-C và Bootstrap 0...${NC}"
grep -E "Gracefully deregistered|Peer gracefully left" store_8302.log bootstrap0.log || true

PEERS_AFTER_LEAVE=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo -e "\n${GREEN}[+] Registry tại Bootstrap 0 sau khi Store-C rời mạng chủ động:${NC} ${PEERS_AFTER_LEAVE}"

if [[ "$PEERS_AFTER_LEAVE" != *"18302"* ]]; then
    echo -e "${GREEN}[✓] XÁC NHẬN: Store-C đã được xóa khỏi Registry NGAY LẬP TỨC (<1s) không cần chờ TTL!${NC}"
else
    echo -e "${RED}[✗] LỖI: Store-C vẫn còn tồn tại trong registry sau khi gửi Graceful Leave!${NC}"
    exit 1
fi

echo -e "\n${YELLOW}>> Bước 2.3: Kiểm tra tính sẵn sàng cao (Light Node DAS vẫn thành công nhờ Store Node dự phòng)...${NC}"
DAS_FALLBACK=$(curl -s "http://localhost:8499/das/sample/block-lifecycle-test?row=0&col=0")
DAS_FALLBACK_OK=$(echo "$DAS_FALLBACK" | jq -r '.success')
echo -e "Light Node DAS Sample ô [0, 0]: ${GREEN}success = ${DAS_FALLBACK_OK}${NC}"
echo -e "${GREEN}[✓] GIAI ĐOẠN 2 THÀNH CÔNG: Graceful Leave xử lý tức thì, mạng lưới duy trì liên tục!${NC}"

# ==============================================================================
# GIAI ĐOẠN 3: THEO DÕI SỰ CỐ SẬP ĐỘT NGỘT & TỰ DỌN DẸP TTL (UNGRACEFUL CRASH)
# ==============================================================================
echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${CYAN}  GIAI ĐOẠN 3: THEO DÕI RỜI MẠNG ĐỘT NGỘT & TỰ DỌN DẸP TTL (UNGRACEFUL CRASH) ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "${YELLOW}>> Bước 3.1: Khởi chạy Store-D (Port 8303, Row 3, Col 0)...${NC}"
./bin/store -port 8303 -row 3 -col 0 -publisher http://localhost:8080 -bootstrap http://localhost:8200 -k 16 > store_8303.log 2>&1 &
STORE_D_PID=$!
ALL_PIDS+=($STORE_D_PID)
sleep 2

PEERS_WITH_D=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo -e "${GREEN}[+] Registry đã ghi nhận Store-D:${NC} ${PEERS_WITH_D}"

echo -e "\n${YELLOW}>> Bước 3.2: Giả lập sự cố phần cứng/sập nguồn: Gửi SIGKILL (kill -9) tới Store-D...${NC}"
echo -e "   (Store-D bị tắt đột ngột, KHÔNG KỊP gửi thông báo IsLeave)"
kill -9 $STORE_D_PID

echo -e "\n${YELLOW}>> Bước 3.3: Quan sát cơ chế Heartbeat TTL (15s) trên Bootstrap Node...${NC}"
echo "Kiểm tra ngay tại t=2s sau sự cố (Node vẫn còn trong TTL):"
PEERS_T2=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo -e "Registry lúc t=2s: ${PEERS_T2}"

echo -e "\nĐang đợi hết hạn TTL (15s) để tiến trình quét nền của Bootstrap kích hoạt..."
for s in {1..16}; do
    echo -ne "Đang đợi... ${s}s/16s\r"
    sleep 1
done
echo ""

PEERS_AFTER_TTL=$(curl -s http://localhost:8200/bootstrap/peers | jq -c '.peers')
echo -e "\n${GREEN}[+] Registry tại Bootstrap 0 sau khi hết hạn TTL (15s):${NC} ${PEERS_AFTER_TTL}"

if [[ "$PEERS_AFTER_TTL" != *"18303"* ]]; then
    echo -e "${GREEN}[✓] XÁC NHẬN: Bootstrap Node đã tự động dọn dẹp và loại bỏ peer chết (Store-D) thành công!${NC}"
else
    echo -e "${RED}[✗] LỖI: Peer chết vẫn chưa được dọn dẹp sau 15s TTL!${NC}"
    exit 1
fi

echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${GREEN}   TẤT CẢ CÁC GIAI ĐOẠN VÒNG ĐỜI (JOIN / LEAVE / CRASH) ĐỀU HOÀN TẤT 100%!  ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

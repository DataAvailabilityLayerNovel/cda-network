#!/bin/bash
set -e

# ==============================================================================
# CDA Network - Test Kịch Bản Vòng Đời Store Node & Ma Trận Mạng Đa Node/Ô
# ==============================================================================
# Kịch bản này kiểm tra và trực quan hóa 4 giai đoạn:
# 1. Store Node mới gia nhập mạng (Dynamic Join & Auto-Peering Sync Loop + Catch-Up)
# 2. Store Node rời mạng chủ động (Graceful Leave qua SIGINT/SIGTERM & IsLeave flag)
# 3. Store Node rời mạng đột ngột (Ungraceful Crash qua SIGKILL & TTL Expiry Cleanup)
# 4. Nhiều Store Node trong cùng 1 ô mạng (Multi-Node Cell M>=2 & Kênh hàng mạng TopicCellRow)
# ==============================================================================

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../../.." && pwd )"
cd "$REPO_ROOT"

# Hàm trích xuất JSON bằng python3 (không phụ thuộc jq)
json_extract() {
    local expr="$1"
    python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    val = $expr
    if isinstance(val, (dict, list)):
        print(json.dumps(val, separators=(',', ':')))
    elif isinstance(val, bool):
        print('true' if val else 'false')
    elif val is None:
        print('null')
    else:
        print(val)
except Exception:
    print('')
"
}

# Định dạng màu sắc
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}==========================================================================${NC}"
echo -e "${CYAN}   KỊCH BẢN KIỂM THỬ: VÒNG ĐỜI JOIN/LEAVE & MA TRẬN ĐA STORE NODE/Ô      ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "\n${BLUE}=== 1. Biên dịch các module thực thi (Binaries) ===${NC}"
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
go build -o bin/store ./cda-store-node/cmd/store/main.go
go build -o bin/light ./cda-light-node/cmd/light/main.go

echo -e "\n${BLUE}=== 2. Thiết lập cấu hình mạng (K=4, KPiece=4, NetRows=4, NetCols=2, ActiveCols=1) ===${NC}"
cat <<EOF > publisher_config.json
{
  "api_port": 18080,
  "k": 4,
  "k_piece": 4,
  "active_cols": 1,
  "num_cols": 2,
  "net_cols": 2,
  "net_rows": 4,
  "bootstrap_peers": {
    "0": "http://localhost:18200"
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
    rm -rf bin/ publisher_config.json data/store_183* data/light_18499 data/publisher *.key bootstrap*.log publisher.log light.log store_183*.log
    wait "${ALL_PIDS[@]}" 2>/dev/null || true
    echo -e "${GREEN}[+] Hoàn tất dọn dẹp!${NC}"
}
trap cleanup EXIT

echo -e "\n${BLUE}=== 3. Khởi chạy Bootstrap Node 0 (PeerTTL=6s), Publisher và Light Node ===${NC}"
./bin/bootstrap -port 18200 -col 0 -publisher http://localhost:18080 -k 4 -k-piece 4 -net-rows 4 -net-cols 2 -peer-ttl 6s > bootstrap0.log 2>&1 &
BOOT0_PID=$!
ALL_PIDS+=($BOOT0_PID)

sleep 1

./bin/publisher -config publisher_config.json > publisher.log 2>&1 &
PUB_PID=$!
ALL_PIDS+=($PUB_PID)

./bin/light -port 18499 -publisher http://localhost:18080 -bootstraps "0:http://localhost:18200" -k 4 -k-piece 4 -net-cols 2 -net-rows 4 -auto-das=false > light.log 2>&1 &
LIGHT_PID=$!
ALL_PIDS+=($LIGHT_PID)

echo -e "\n${BLUE}=== 4. Khởi chạy 2 Store Node cơ sở (Store-A Row 0 & Store-B Row 1) tại Cột 0 ===${NC}"
./bin/store -port 18300 -row 0 -col 0 -publisher http://localhost:18080 -bootstrap http://localhost:18200 -k 4 -k-piece 4 -net-rows 4 -net-cols 2 -myaddr http://localhost:18300 > store_18300.log 2>&1 &
STORE_A_PID=$!
ALL_PIDS+=($STORE_A_PID)

./bin/store -port 18301 -row 1 -col 0 -publisher http://localhost:18080 -bootstrap http://localhost:18200 -k 4 -k-piece 4 -net-rows 4 -net-cols 2 -myaddr http://localhost:18301 > store_18301.log 2>&1 &
STORE_B_PID=$!
ALL_PIDS+=($STORE_B_PID)

echo "Đang chờ 3s để Store-A và Store-B hoàn tất đăng ký với Bootstrap 0..."
sleep 3

PEERS_INIT=$(curl -s http://localhost:18200/bootstrap/peers | json_extract "data.get('peers', [])")
echo -e "${GREEN}[+] Danh sách Store Peers ban đầu tại Bootstrap 0: ${PEERS_INIT}${NC}"

# ==============================================================================
# GIAI ĐOẠN 1: THEO DÕI STORE NODE MỚI GIA NHẬP MẠNG (DYNAMIC JOIN)
# ==============================================================================
echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${CYAN}  GIAI ĐOẠN 1: THEO DÕI STORE NODE MỚI (STORE-C) GIA NHẬP MẠNG ĐỘNG      ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "${YELLOW}>> Bước 1.1: Khởi chạy Store-C (Port 18302, Row 2, Col 0)...${NC}"
./bin/store -port 18302 -row 2 -col 0 -publisher http://localhost:18080 -bootstrap http://localhost:18200 -k 4 -k-piece 4 -net-rows 4 -net-cols 2 -myaddr http://localhost:18302 > store_18302.log 2>&1 &
STORE_C_PID=$!
ALL_PIDS+=($STORE_C_PID)

sleep 2

PEERS_AFTER_JOIN=$(curl -s http://localhost:18200/bootstrap/peers | json_extract "data.get('peers', [])")
echo -e "${GREEN}[+] Registry tại Bootstrap 0 sau khi Store-C gia nhập:${NC} ${PEERS_AFTER_JOIN}"

if [[ "$PEERS_AFTER_JOIN" == *"28302"* ]]; then
    echo -e "${GREEN}[✓] XÁC NHẬN: Store-C (Port P2P 28302) đã được Bootstrap 0 ghi nhận thành công!${NC}"
else
    echo -e "${RED}[✗] LỖI: Store-C chưa được ghi nhận trong registry!${NC}"
    exit 1
fi

sleep 2

echo -e "\n${YELLOW}>> Bước 1.2: Xuất bản Block và kiểm tra Store-C tiếp nhận & lưu trữ dữ liệu qua kênh hàng mạng...${NC}"
PAYLOAD=$(python3 -c "
import json
data = ['%0128x' % (i + 1) for i in range(16)]
print(json.dumps({'block_id': 'block-lifecycle-test-1', 'data': data}))
")
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" http://localhost:18080/publish > /dev/null

STATUS_C="false"
for i in {1..40}; do
    STATUS_C=$(curl -s http://localhost:18302/store/status/block-lifecycle-test-1 | json_extract "data.get('completed', False)")
    if [ "$STATUS_C" = "true" ]; then
        break
    fi
    sleep 0.25
done
echo -e "Trạng thái lưu trữ của Store-C cho block-lifecycle-test-1: ${GREEN}completed = ${STATUS_C}${NC}"

DAS_RESP=$(curl -s "http://localhost:18499/das/sample/block-lifecycle-test-1?row=2&col=0")
DAS_SUCCESS=$(echo "$DAS_RESP" | json_extract "data.get('success', False)")
echo -e "Light Node DAS Sample ô [2, 0]: ${GREEN}verified = ${DAS_SUCCESS}${NC}"
if [ "$DAS_SUCCESS" != "true" ]; then
    echo -e "${RED}[✗] LỖI: DAS verification thất bại trên Store-C mới gia nhập! Response: ${DAS_RESP}${NC}"
    exit 1
fi
echo -e "${GREEN}[✓] GIAI ĐOẠN 1 THÀNH CÔNG!${NC}"

# ==============================================================================
# GIAI ĐOẠN 2: THEO DÕI STORE NODE RỜI MẠNG CHỦ ĐỘNG (GRACEFUL LEAVE)
# ==============================================================================
echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${CYAN}  GIAI ĐOẠN 2: THEO DÕI RỜI MẠNG CHỦ ĐỘNG (GRACEFUL LEAVE - SIGTERM)      ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "${YELLOW}>> Bước 2.1: Gửi tín hiệu SIGTERM (kill -15) tới Store-C (PID $STORE_C_PID)...${NC}"
kill -15 $STORE_C_PID
sleep 1

grep -E "Gracefully deregistered|Peer gracefully left" store_18302.log bootstrap0.log || true

PEERS_AFTER_LEAVE=$(curl -s http://localhost:18200/bootstrap/peers | json_extract "data.get('peers', [])")
echo -e "\n${GREEN}[+] Registry tại Bootstrap 0 sau khi Store-C rời mạng chủ động:${NC} ${PEERS_AFTER_LEAVE}"

if [[ "$PEERS_AFTER_LEAVE" != *"28302"* ]]; then
    echo -e "${GREEN}[✓] XÁC NHẬN: Store-C đã được xóa khỏi Registry NGAY LẬP TỨC (<1s)!${NC}"
else
    echo -e "${RED}[✗] LỖI: Store-C vẫn còn tồn tại trong registry sau khi gửi Graceful Leave!${NC}"
    exit 1
fi

echo -e "\n${YELLOW}>> Bước 2.2: Kiểm tra DAS Sample ô [2, 0] vẫn thành công nhờ Custody Rebalancing...${NC}"
DAS_FALLBACK=$(curl -s "http://localhost:18499/das/sample/block-lifecycle-test-1?row=2&col=0")
DAS_FALLBACK_OK=$(echo "$DAS_FALLBACK" | json_extract "data.get('success', False)")
echo -e "Light Node DAS Sample ô [2, 0] sau khi Store-C rời mạng: ${GREEN}success = ${DAS_FALLBACK_OK}${NC}"
if [ "$DAS_FALLBACK_OK" != "true" ]; then
    echo -e "${RED}[✗] LỖI: DAS Sample thất bại sau khi Store-C rời mạng!${NC}"
    exit 1
fi
echo -e "${GREEN}[✓] GIAI ĐOẠN 2 THÀNH CÔNG!${NC}"

# ==============================================================================
# GIAI ĐOẠN 3: SỰ CỐ SẬP ĐỘT NGỘT & TỰ DỌN DẸP THEO PEER_TTL (UNGRACEFUL CRASH)
# ==============================================================================
echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${CYAN}  GIAI ĐOẠN 3: RỜI MẠNG ĐỘT NGỘT & DỌN DẸP THEO PEER_TTL=6s               ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "${YELLOW}>> Bước 3.1: Khởi chạy Store-D (Port 18303, Row 3, Col 0)...${NC}"
./bin/store -port 18303 -row 3 -col 0 -publisher http://localhost:18080 -bootstrap http://localhost:18200 -k 4 -k-piece 4 -net-rows 4 -net-cols 2 -myaddr http://localhost:18303 > store_18303.log 2>&1 &
STORE_D_PID=$!
ALL_PIDS+=($STORE_D_PID)
sleep 2

echo -e "${YELLOW}>> Bước 3.2: Gửi SIGKILL (kill -9) tới Store-D và chờ PeerTTL (6s)...${NC}"
kill -9 $STORE_D_PID
sleep 8

PEERS_AFTER_TTL=$(curl -s http://localhost:18200/bootstrap/peers | json_extract "data.get('peers', [])")
if [[ "$PEERS_AFTER_TTL" != *"28303"* ]]; then
    echo -e "${GREEN}[✓] XÁC NHẬN: Bootstrap Node đã tự động dọn dẹp Store-D sau PeerTTL=6s!${NC}"
else
    echo -e "${RED}[✗] LỖI: Peer chết vẫn chưa được dọn dẹp sau PeerTTL!${NC}"
    exit 1
fi
echo -e "${GREEN}[✓] GIAI ĐOẠN 3 THÀNH CÔNG!${NC}"

# ==============================================================================
# GIAI ĐOẠN 4: MA TRẬN ĐA NODE TRONG CÙNG 1 Ô (MULTI-NODE CELL M=3 & ROW CHANNEL)
# ==============================================================================
echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${CYAN}  GIAI ĐOẠN 4: ĐA STORE NODE TRONG CÙNG 1 Ô (M=3 REPLICAS TẠI Ô [0, 0])   ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

echo -e "${YELLOW}>> Bước 4.1: Khởi chạy thêm 2 Store Node (Store-A2 & Store-A3) vào CÙNG Ô [Row 0, Col 0]...${NC}"
./bin/store -port 18304 -row 0 -col 0 -publisher http://localhost:18080 -bootstrap http://localhost:18200 -k 4 -k-piece 4 -net-rows 4 -net-cols 2 -myaddr http://localhost:18304 > store_18304.log 2>&1 &
STORE_A2_PID=$!
ALL_PIDS+=($STORE_A2_PID)

./bin/store -port 18305 -row 0 -col 0 -publisher http://localhost:18080 -bootstrap http://localhost:18200 -k 4 -k-piece 4 -net-rows 4 -net-cols 2 -myaddr http://localhost:18305 > store_18305.log 2>&1 &
STORE_A3_PID=$!
ALL_PIDS+=($STORE_A3_PID)

sleep 3

ROW0_CUSTODY_COUNT=$(curl -s http://localhost:18200/bootstrap/peers | json_extract "len(data.get('row_custody_map', {}).get('0', []))")
echo -e "${GREEN}[+] Số lượng Custody Store Nodes cùng phụ trách Ô [Row 0, Col 0]: M = ${ROW0_CUSTODY_COUNT}${NC}"
if [ "$ROW0_CUSTODY_COUNT" != "3" ]; then
    echo -e "${RED}[✗] LỖI: Kỳ vọng M=3 Store Nodes tại Ô [0, 0], nhận được ${ROW0_CUSTODY_COUNT}!${NC}"
    exit 1
fi

echo -e "${YELLOW}>> Bước 4.2: Xuất bản Block mới và kiểm tra cả 3 Store Node trong Ô [0, 0] trao đổi mảnh qua /cda/1.0.0/col/0/row/0...${NC}"
PAYLOAD2=$(python3 -c "
import json
data = ['%0128x' % (i + 100) for i in range(16)]
print(json.dumps({'block_id': 'block-multinode-cell', 'data': data}))
")
curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD2" http://localhost:18080/publish > /dev/null

for port in 18300 18304 18305; do
    DONE="false"
    for i in {1..40}; do
        DONE=$(curl -s http://localhost:${port}/store/status/block-multinode-cell | json_extract "data.get('completed', False)")
        if [ "$DONE" = "true" ]; then
            break
        fi
        sleep 0.25
    done
    echo -e "  - Store Node (port ${port}, cell [0,0]): completed = ${GREEN}${DONE}${NC}"
    if [ "$DONE" != "true" ]; then
        echo -e "${RED}[✗] LỖI: Store Node port ${port} trong ô đa node chưa đạt full rank!${NC}"
        exit 1
    fi
done

DAS_MULTI=$(curl -s "http://localhost:18499/das/sample/block-multinode-cell?row=0&col=0")
DAS_MULTI_OK=$(echo "$DAS_MULTI" | json_extract "data.get('success', False)")
echo -e "Light Node DAS Sample ô [0, 0] (từ cụm 3 Store Nodes): ${GREEN}verified = ${DAS_MULTI_OK}${NC}"
if [ "$DAS_MULTI_OK" != "true" ]; then
    echo -e "${RED}[✗] LỖI: DAS Sample thất bại trên ô đa node!${NC}"
    exit 1
fi

echo -e "\n${CYAN}==========================================================================${NC}"
echo -e "${GREEN}   TẤT CẢ 4 GIAI ĐOẠN (JOIN / LEAVE / CRASH / MULTI-NODE CELL) THÀNH CÔNG! ${NC}"
echo -e "${CYAN}==========================================================================${NC}"

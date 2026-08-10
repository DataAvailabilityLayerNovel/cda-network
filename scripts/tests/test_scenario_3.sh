#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

echo "================================================================================"
echo "          CHẠY KỊCH BẢN 3: KIỂM THỬ PHỤC HỒI DỮ LIỆU KHỐI PHÂN TÁN"
echo "================================================================================"

# 1. Compile test program
echo "[1] Biên dịch chương trình kiểm thử Reconstructor..."
mkdir -p bin
go build -o bin/reconstruct_scenario3 ./cda-publisher-node/cmd/reconstruct_scenario3/main.go

# 2. Run Scenario 3 with 1 Network Column Loss (4 data columns lost)
echo ""
echo "[2] Chạy kiểm thử: Mất 1 cột mạng hoàn toàn (4 cột dữ liệu = 128 ô EDS bị mất)..."
./bin/reconstruct_scenario3 -k 16 -lost-cols 4 -publisher http://localhost:8080 -light http://localhost:9401

# 3. Run Scenario 3 with Extreme Stress: Mất 16 cột (50% ma trận bị mất - Giới hạn tối đa của Reed-Solomon)
echo ""
echo "[3] Chạy kiểm thử mở rộng: Mất 16 cột dữ liệu (50% ma trận = 512 ô EDS bị mất)..."
./bin/reconstruct_scenario3 -k 16 -lost-cols 16 -publisher http://localhost:8080 -light http://localhost:9401

echo ""
echo "[+] Hoàn thành toàn bộ Kịch bản 3 một cách xuất sắc!"

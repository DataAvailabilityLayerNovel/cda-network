# Hướng Dẫn Vận Hành Kiểm Thử CDA Network Bằng Docker & Docker Compose (Docker E2E Guide)

Tài liệu này hướng dẫn chi tiết cách chạy kịch bản kiểm thử End-to-End (E2E) tự động hóa của mạng lưới **CDA Network** trong môi trường container hóa với **Docker** và **Docker Compose**.

Kịch bản tích hợp đầy đủ luồng đồng thuận thực tế **CometBFT**, phân tán **P2P libp2p**, lưu trữ **Custody**, lấy mẫu **Auto-DAS** trên các container độc lập và giám sát trực quan thời gian thực qua **Prometheus & Grafana**.

---

## 1. Yêu Cầu Tiên Quyết (Prerequisites)

1. **Docker Engine**: Phiên bản 24.0+
2. **Docker Compose**: Plugin v2 (`docker compose version`)
3. **Go**: Phiên bản 1.23+ (dùng để chạy CometBFT test runner trên host)
4. **Python 3**: Dùng để chạy bộ sinh cấu hình `scripts/generate_compose.py`
5. **Cổng mạng khả dụng trên Host**:
   - `8080`, `18080`: Publisher API & P2P
   - `9090`: Prometheus Server
   - `3000`: Grafana Dashboard
   - `9200..9200+C`: Bootstrap Node APIs
   - `9300..9300+C*S`: Store Node APIs
   - `9401..9400+L`: Light Node APIs

---

## 2. Bảng Tham Số Dòng Lệnh CLI (`test_docker_e2e.sh`)

Script [`scripts/tests/test_docker_e2e.sh`](file:///home/ubuntu/cda-network/scripts/tests/test_docker_e2e.sh) hỗ trợ các cờ tham số linh hoạt:

| Cờ Tham Số | Tên Đầy Đủ | Giá Trị Mặc Định | Ý Nghĩa Kỹ Thuật |
| :--- | :--- | :---: | :--- |
| `-k` | `--k` | `8` | Kích thước ma trận ODS ($K \times K$ cells). EDS sẽ có kích thước $2K \times 2K$. |
| `-p` | `--k-piece` | `4` | Số mảnh RLNC trên mỗi cell ($K_{\text{piece}}$). |
| `-c` | `--cols`, `--active-cols` | `1` | Số nhóm cột mạng hoạt động thực tế trong Docker ($C$). |
| `-n` | `--num-cols` | $2K$ | Tổng số cột mạng trong kiến trúc EDS ($N$). |
| `-s` | `--stores-per-col` | `4` | Số lượng Store Node trên mỗi cột mạng ($S$). |
| `-l` | `--light-nodes`, `--lights` | `1` | Số lượng Light Node độc lập tham gia Auto-DAS ($L$). |
| `-b` | `--blocks` | `3` | Số lượng block liên tiếp được sinh và commit qua BFT consensus. |
| `-t` | `--txs`, `--txs-per-block` | `16` | Số lượng giao dịch thực tế trong mỗi block. |
| | `--keep-alive` | `false` | Giữ nguyên cụm Docker chạy sau khi test xong để xem Grafana. |
| `-h` | `--help` | - | Hiển thị trợ giúp dòng lệnh. |

---

## 3. Các Kịch Bản Thực Thi Tiêu Biểu

### 3.1 Kịch Bản Khởi Động Nhanh (Smoke Test)
Phù hợp để kiểm tra nhanh tính toàn vẹn của mã nguồn, CI/CD:
```bash
./scripts/tests/test_docker_e2e.sh -k 8 -p 4 -c 1 -s 4 -l 1 -b 2
```
*Thời gian chạy dự kiến: ~45 - 60 giây.*

### 3.2 Kịch Bản Chuẩn (Standard Verification)
Chạy kiểm thử 3 block liên tiếp với 4 store nodes và 2 light nodes:
```bash
./scripts/tests/test_docker_e2e.sh -k 8 -p 4 -c 1 -s 4 -l 2 -b 3 -t 16
```

### 3.3 Kịch Bản Cụm Lớn & Giữ Lại Grafana Để Soi Hiệu Năng (`--keep-alive`)
Triển khai ma trận $K=16$ ($32 \times 32$ EDS), 2 cột mạng, 16 Store Nodes, 2 Light Nodes, 5 blocks liên tiếp:
```bash
./scripts/tests/test_docker_e2e.sh -k 16 -p 4 -c 2 -s 8 -l 2 -b 5 -n 8 --keep-alive
```
Khi kịch bản hoàn tất, cụm container sẽ được giữ nguyên hoạt động để bạn mở trình duyệt xem biểu đồ.

---

## 4. Giám Sát Trực Quan Với Prometheus & Grafana

Khi cụm container đang hoạt động (hoặc khi dùng cờ `--keep-alive`):

### 4.1 Truy Cập Dashboard:
- **Địa chỉ:** `http://localhost:3000`
- **Tài khoản đăng nhập:** `admin`
- **Mật khẩu:** `admin`
- **Dashboard:** Vào mục **Dashboards** $\to$ Chọn **CDA Network Performance Dashboard**.

### 4.2 Các Chỉ Số Quan Trọng Trên Dashboard:
1. **Committed Blocks Throughput:** Tốc độ tạo và lưu trữ khối dữ liệu theo thời gian.
2. **Auto-DAS Sampling Latency:** Thời gian trung bình các Light Node hoàn thành lấy mẫu và kiểm chứng KZG qua mạng P2P.
3. **Node Encoding & Proof Durations:** 
   - *Publisher RS Encode Duration:* Thời gian mã hóa 2D Reed-Solomon.
   - *Bootstrap KZG Proof Duration:* Thời gian sinh Opening Proofs theo cột.
   - *Store Reconstruction Duration:* Thời gian giải mã RLNC khi khôi phục cell.
4. **System CPU & RAM Utilization:** Mức tiêu thụ tài nguyên thực tế của từng container node trong cụm.

---

## 5. Dọn Dẹp Tài Nguyên (Cleanup)

Nếu đã bật `--keep-alive` hoặc muốn dừng cụm thủ công bất kỳ lúc nào:
```bash
docker compose -f docker-compose.json down -v
```
Lệnh trên sẽ:
- Dừng và xóa toàn bộ các container trong mạng `cda-net`.
- Hủy bỏ các volume database tạm thời (`badgerdb` và cache).
- Giải phóng toàn bộ cổng mạng và RAM/CPU.

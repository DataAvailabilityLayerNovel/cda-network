# Hướng Dẫn Cấu Hình Trình Sinh Docker Compose (`scripts/generate_compose.py`)

Tài liệu này hướng dẫn chi tiết cách sử dụng và cấu hình công cụ **`scripts/generate_compose.py`** — trình tự động hóa thiết lập toàn bộ topology mạng lưới phân tán container hóa của **CDA Network**.

---

## 1. Vai Trò & Cơ Chế Hoạt Động

Trong môi trường phân tán, số lượng container và cổng dịch vụ có thể tăng nhanh theo kích thước ma trận $K$, số cột mạng $N$, số Store Node trên mỗi cột $S$ và số Light Node $L$. Ví dụ với cấu hình chuẩn $K=64$ (8 cột mạng, 8 store/cột, 2 light), hệ thống cần vận hành đồng thời **76 containers** cùng hàng trăm cổng P2P/REST.

Thay vì chỉnh sửa thủ công các file compose cồng kềnh, **`scripts/generate_compose.py`** tự động:
1. **Sinh file Docker Compose** (mặc định: `docker-compose.json`): Khởi tạo toàn bộ dịch vụ Publisher, Bootstrap Nodes, Store Nodes, Light Nodes, Prometheus Server và Grafana Dashboard trong cùng một mạng cầu nối cô lập (`cda-net`).
2. **Cấu hình định tuyến P2P Libp2p**: Thiết lập multiaddr dạng DNS chuẩn (`/dns4/bootstrap-c/tcp/<port>`), ánh xạ cổng P2P TCP/UDP (QUIC-v1) tự động cho từng node.
3. **Sinh cấu hình Publisher Node** (`publisher_config_docker.json`): Tự động trỏ Publisher tới danh sách Bootstrap Node tương ứng với các cột active.
4. **Sinh cấu hình giám sát Prometheus** (`data/prometheus.yml`): Tự động cấu hình danh sách cào metrics từ 100% các node trong mạng theo chu kỳ 1s.
5. **Sinh Dashboard Grafana tự động** (`data/grafana/provisioning/...`): Tự động nạp sẵn biểu đồ giám sát Throughput, độ trễ lấy mẫu Auto-DAS, lưu lượng GossipSub và mức sử dụng CPU.
6. **Xuất danh sách cổng Store Nodes** (`STORE_PORTS=...`): Hỗ trợ các script shell tự động bắt lấy danh sách cổng REST API để polling trạng thái `IsComplete`.

---

## 2. Bảng Tham Số Dòng Lệnh Đầy Đủ (CLI Reference)

### 2.1. Tham Số Ma Trận & Cấu Trúc Mạng

| Cờ Tham Số | Tên Đầy Đủ | Mặc Định | Ý Nghĩa Kỹ Thuật |
| :--- | :--- | :---: | :--- |
| `-k` | `--k` | `16` | Kích thước ma trận gốc ODS ($K \times K$). Ma trận mở rộng EDS sẽ có kích thước $2K \times 2K$. |
| `-p` | `--k-piece`, `--piece` | `4` | Số mảnh RLNC cần thiết trên mỗi ô dữ liệu ($K_{\text{piece}}$). |
| `-n` | `--num-cols`, `--cols` | `8` | Tổng số nhóm cột mạng logic trong EDS ($N$). |
| `-c` | `--active-cols` | `None` (bằng $N$) | Số lượng cột mạng hoạt động thực tế trong Docker Compose ($C \le N$). Giúp tiết kiệm tài nguyên khi kiểm thử. |
| `-s` | `--stores-per-col` | `8` | Số lượng Store Node phụ trách trên mỗi cột active ($S$). Tổng số Store = $C \times S$. |
| `-l` | `--lights`, `--light-nodes` | `2` | Số lượng Light Node độc lập tham gia lấy mẫu Auto-DAS ($L$). |
| | `--out` | `docker-compose.json` | Tên file Docker Compose đầu ra cần sinh. |

---

### 2.2. Tham Số Độ Tin Cậy & Dọn Dẹp Dữ Liệu (Resilience & Pruning)

| Cờ Tham Số | Kiểu Dữ Liệu | Mặc Định | Ý Nghĩa Kỹ Thuật |
| :--- | :---: | :---: | :--- |
| `--crash-on-fail` | `bool` | `False` | Buộc tiến trình node lập tức `log.Fatalf` (dừng khẩn cấp) nếu phát hiện bất kỳ sai lệch mật mã học nào (KZG inner-product, Merkle anchor hoặc RLNC rank). |
| `--prune-enable` | `bool` | `False` | Kích hoạt tính năng tự động dọn dẹp các mảnh thô (raw pieces) sau khi ô dữ liệu đã được recode hoặc hết hạn TTL. |
| `--prune-ttl` | `str` | `None` | Thời gian sống (TTL) của dữ liệu thô trước khi bị dọn dẹp (ví dụ: `15s`, `5m`, `1h`). |

---

### 2.3. Tham Số Tối Ưu Hóa Hiệu Năng & Concurrency (Tuning Parameters)

| Cờ Tham Số | Kiểu Dữ Liệu | Mặc Định | Node Áp Dụng | Ý Nghĩa Kỹ Thuật |
| :--- | :---: | :---: | :---: | :--- |
| `--store-gossip-batch-size` | `int` | `48` | Store | Kích thước batch gộp các thông điệp GossipSub trước khi đưa vào hàm xác thực KZG hàng loạt. |
| `--store-gossip-batch-workers` | `int` | `2` | Store | Số lượng worker goroutine chạy xác thực batch GossipSub song song. |
| `--store-gossip-batch-ticker-ms`| `int` | `10` | Store | Thời gian timeout tối đa (mili-giây) để xả batch GossipSub nếu chưa đầy batch size. |
| `--store-dissemination-sem` | `int` | `16` | Store | Giới hạn semaphore số lượng goroutine phát tán GossipSub đồng thời, tránh nghẽn CPU. |
| `--store-fallback-pull-sem` | `int` | `8` | Store | Giới hạn semaphore số lượng yêu cầu Active Pull đồng thời tới các peer cùng cột. |
| `--store-sharded-workers` | `int` | `16` | Store | Số worker xử lý các ô dữ liệu được sharding theo hash. |
| `--bootstrap-proof-gen-sem` | `int` | `8` | Bootstrap | Giới hạn concurrency khi tính toán KZG Opening Proofs song song cho các ô trong cột. |
| `--bootstrap-encode-workers` | `int` | `8` | Bootstrap | Số lượng luồng mã hóa RLNC seeds song song. |
| `--bootstrap-seeding-sem` | `int` | `16` | Bootstrap | Giới hạn số luồng P2P Seeding Stream đồng thời sang các Store Node. |
| `--bootstrap-batch-chunk-size`| `int` | `64` | Bootstrap | Số lượng mảnh trong một gói tin Seed gửi qua giao thức nhị phân/QUIC. |
| `--publisher-max-in-flight` | `int` | `2` | Publisher | Số lượng khối tối đa được phép pre-compute gối đầu trước trong buffer pipeline. |

---

### 2.4. Tham Số Quản Lý Tài Nguyên Phần Cứng (Hardware Quotas)

| Cờ Tham Số | Kiểu Dữ Liệu | Mặc Định | Ý Nghĩa Kỹ Thuật |
| :--- | :---: | :---: | :--- |
| `--gomaxprocs` | `int` | `None` (tất cả Cores) | Gán biến môi trường `GOMAXPROCS` cho Go runtime trong từng container để kiểm soát đa luồng. |
| `--cpus` | `str` | `None` (không giới hạn) | Thiết lập quota giới hạn CPU tối đa trên mỗi container Docker (ví dụ: `--cpus 4`, `--cpus 8`). |

---

## 3. Các Kịch Bản Cấu Hình Thực Tế Điển Hình

### 3.1. Kịch Bản 1: Khởi Động Nhanh (Smoke Test / Môi Trường Phát Triển)
Phù hợp cho máy cá nhân (RAM $\le$ 8GB, CPU $\le$ 4 Cores):
- $K=8$ ($8 \times 8 = 64$ ô ODS, $16 \times 16 = 256$ ô EDS).
- Chỉ chạy 1 cột active ($C=1$), 4 Store Nodes, 1 Light Node.

```bash
# 1. Sinh cấu hình
python3 scripts/generate_compose.py \
    --k 8 \
    --k-piece 4 \
    --cols 8 \
    --active-cols 1 \
    --stores-per-col 4 \
    --lights 1

# 2. Khởi động cụm Docker
docker compose -f docker-compose.json up -d --build
```

---

### 3.2. Kịch Bản 2: Kiểm Thử Toàn Diện Tích Hợp Có Bật Dọn Dẹp Pruning
Phù hợp cho kiểm thử E2E trung bình ($K=16$, $C=2$, 16 Store Nodes, 2 Light Nodes):

```bash
# Sinh cấu hình với Pruning TTL 15s và crash on fail
python3 scripts/generate_compose.py \
    --k 16 \
    --k-piece 4 \
    --cols 8 \
    --active-cols 2 \
    --stores-per-col 8 \
    --lights 2 \
    --crash-on-fail \
    --prune-enable \
    --prune-ttl 15s

docker compose -f docker-compose.json up -d --build
```

---

### 3.3. Kịch Bản 3: Đo Lường Hiệu Năng Tối Đa (Benchmark Khối 2MB, K=64)
Cấu hình tối ưu hóa cho bài kiểm tra tải lớn ($K=64$, khối 2.00 MB, 8 cột mạng, 64 Store Nodes):
- Tối ưu hóa GossipSub batch size lên 64, 4 batch workers.
- Tối ưu hóa Seeding semaphore và pre-compute buffer.

```bash
python3 scripts/generate_compose.py \
    --k 64 \
    --k-piece 8 \
    --cols 8 \
    --active-cols 8 \
    --stores-per-col 8 \
    --lights 2 \
    --store-gossip-batch-size 64 \
    --store-gossip-batch-workers 4 \
    --store-dissemination-sem 32 \
    --bootstrap-proof-gen-sem 16 \
    --bootstrap-seeding-sem 32 \
    --publisher-max-in-flight 2 \
    --out docker-compose-benchmark.yml

docker compose -f docker-compose-benchmark.yml up -d --build
```

---

### 3.4. Kịch Bản 4: Khảo Nghiệm Giới Hạn Tài Nguyên CPU
Dùng để đo lường độ nhạy hiệu năng khi giới hạn CPU quota cho các container:

```bash
# Giới hạn mỗi container chỉ được dùng tối đa 4 Cores và GOMAXPROCS=4
python3 scripts/generate_compose.py \
    --k 32 \
    --k-piece 8 \
    --cols 8 \
    --active-cols 2 \
    --stores-per-col 8 \
    --lights 1 \
    --cpus 4 \
    --gomaxprocs 4 \
    --out docker-compose-limited.yml

docker compose -f docker-compose-limited.yml up -d
```

---

## 4. Cấu Trúc Các File Sinh Ra & Ánh Xạ Cổng Mạng

### 4.1. Quy Ước Cổng Dịch Vụ Trên Host:
- **Publisher Node**:
  - HTTP REST API: `8080`
  - Libp2p P2P: `18080` (TCP & UDP/QUIC)
- **Bootstrap Nodes (theo cột $c$ từ $0 \dots C-1$)**:
  - HTTP REST API: `9200 + c` (ví dụ `9200`, `9201`, ...)
  - Libp2p P2P: `19200 + c` (ví dụ `19200`, `19201`, ...)
- **Store Nodes (theo index từ $0 \dots C \times S - 1$)**:
  - HTTP REST API: `9300 + idx` (ví dụ `9300`, `9301`, ..., `9363`)
  - Libp2p P2P: `19300 + idx` (TCP & UDP/QUIC)
  - BadgerDB & Completion Log: Ánh xạ ra thư mục `./data/store_<port>/` trên host.
- **Light Nodes (theo thứ tự $l$ từ $1 \dots L$)**:
  - HTTP REST API: `9400 + l` (ví dụ `9401`, `9402`, ...)
  - Libp2p P2P: `19400 + l`
  - Log lấy mẫu DAS: Ánh xạ ra thư mục `./data/light_<port>/` trên host.
- **Hạ tầng giám sát**:
  - Prometheus Server: `http://localhost:9090`
  - Grafana Dashboard: `http://localhost:3000` (User: `admin`, Pass: `admin`)

---

## 5. Quy Trình Vận Hành Mẫu

```bash
# Bước 1: Dọn dẹp tài nguyên cũ
bash scripts/cleanup.sh

# Bước 2: Sinh file compose với cấu hình mong muốn
python3 scripts/generate_compose.py --k 16 --cols 8 --active-cols 1 --stores-per-col 4 --lights 1

# Bước 3: Khởi động mạng lưới
docker compose -f docker-compose.json up -d

# Bước 4: Kiểm tra trạng thái sẵn sàng của Publisher
curl -s http://localhost:8080/health | jq .

# Bước 5: Kiểm tra danh sách peer đã đăng ký tại Bootstrap Node 0
curl -s http://localhost:9200/bootstrap/peers | jq .

# Bước 6: Sau khi kiểm thử xong, hạ cụm container
docker compose -f docker-compose.json down -v
```

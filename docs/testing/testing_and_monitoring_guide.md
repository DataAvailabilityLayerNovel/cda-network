# Hướng Dẫn Chạy Kiểm Thử & Giám Sát Mạng Lưới CDA Network

Tài liệu này cung cấp hướng dẫn toàn diện về cách vận hành, thực hiện kiểm thử tự động (E2E & Load Testing) và theo dõi trực quan (CLI & Grafana/Prometheus) đối với hệ thống **CDA Network** tích hợp lớp đồng thuận **CometBFT Consensus**.

---

## 1. Tổng Quan Kiến Trúc Môi Trường

Môi trường kiểm thử chuẩn bao gồm các thành phần:
- **Consensus Cluster**: 4 CometBFT Validator nodes (`cometbft-0` đến `cometbft-3`, RPC port `26657`).
- **Publisher Node**: Cầu nối Zero-Trust tiếp nhận block, mở rộng EDS 2D và tạo RLNC pieces (port `8080`).
- **Bootstrap Relays**: 1 hoặc nhiều Bootstrap relay nodes (port `9200`..).
- **Store Cluster**: 8 Store nodes phụ trách lưu trữ bảo chứng custody và RLNC recoding (ports `9300` đến `9307`).
- **Light Nodes**: 2 Light nodes tự động lấy mẫu DAS (`light-1`, `light-2`, ports `9401`, `9402`).
- **Monitoring Stack**: Prometheus (port `9090`) và Grafana (port `3000`).

---

## 2. Khởi Động Cụm Mạng (Cluster Setup)

Nếu chưa dựng cụm Docker hoặc cần tạo mới từ đầu, thực hiện tuần tự các bước sau:

```bash
# Bước 1: Khởi tạo cấu hình genesis và khóa validator cho CometBFT (4 nodes)
python3 scripts/init_cometbft_cluster.py --validators 4

# Bước 2: Sinh file docker-compose.json, prometheus.yml và dashboard Grafana
python3 scripts/generate_compose.py

# Bước 3: Build và khởi chạy toàn bộ cụm container
docker compose -f docker-compose.json up -d --build
```

Kiểm tra trạng thái các container:
```bash
docker compose -f docker-compose.json ps
```

---

## 3. Các Công Cụ Giám Sát Mạng Lưới (Monitoring)

Nên mở các công cụ giám sát **trước khi** chạy kiểm thử hoặc bơm transaction để quan sát trọn vẹn luồng dữ liệu từ đầu đến cuối.

### 3.1. CLI Monitor - Theo Dõi Dòng Chảy Thời Gian Thực

Script [`scripts/monitor/cda_network_monitor.py`](file:///home/ubuntu/cda-network/scripts/monitor/cda_network_monitor.py) cung cấp 2 chế độ quan sát:

#### Chế độ 1: Dòng Chảy Sự Kiện (Stream Mode - Khuyên Dùng Khi Test)
Hiển thị tuần tự các giai đoạn của từng block theo thời gian thực (3 stages):
```bash
python3 scripts/monitor/cda_network_monitor.py --mode stream
```
**Định dạng đầu ra mẫu:**
```text
[09:40:12] ⚡ [STAGE 1: CONSENSUS]   CometBFT committed block #12 | Mempool: 16 txs (4096 B) | Chain: chain-abc
[09:40:13] 📦 [STAGE 2: PUBLISHER]   Block #12 ODS processed: K=16 (16x16 = 256 cells) -> EDS (32x32 = 1024 cells) | Kate Root: 8f4b12...
[09:40:14] 🎉 [STAGE 3: CDA BLOCK_READY] Block #12 (block-12) reached CDA BlockReady | K=16 (ODS: 16x16=256 cells, EDS: 32x32=1024 cells) | Store Nodes: 100% | Latency: 1.85s -> DAS Sampling Active!
```

#### Chế độ 2: Bảng Điều Khiển Tổng Hợp (Dashboard Terminal Mode)
Hiển thị bảng trạng thái đầy đủ (Sức khỏe từng node, ma trận Store custody, 10 block gần nhất):
```bash
python3 scripts/monitor/cda_network_monitor.py --mode dashboard --interval 1.0
```

---

### 3.2. Grafana Dashboard - Giám Sát Trực Quan & Hiệu Năng

- **Địa chỉ truy cập**: [http://localhost:3000](http://localhost:3000)
- **Tài khoản**: `admin` / **Mật khẩu**: `admin`
- **Tên Dashboard**: **CDA Network Performance Dashboard**

#### Ý Nghĩa Các Panel Trọng Tâm (19 Panels):
1. **CometBFT Block Height**: Chiều cao block đồng thuận mới nhất của CometBFT.
2. **CDA BlockReady Height**: Chiều cao block đã hoàn thành 100% bảo chứng lưu trữ tại CDA layer.
3. **CDA Dimension K (ODS Matrix)**: Kích thước ma trận $K$ hiện tại ($8$ khi block trống, $16, 32, 64$ khi có payload).
4. **CDA Pipeline In-Flight**: Số block đang được xử lý đồng thời trong hàng đợi của Publisher (ngưỡng tối đa 32).
5. **Mempool Pending Txns**: Số lượng giao dịch chưa xử lý đang chờ trong mempool.
6. **Block Height Progression**: Biểu đồ đường so sánh giữa Consensus Height và BlockReady Height để phát hiện độ trễ (pipeline lag).
7. **CDA Pipeline Latency**: Biểu đồ độ trễ xử lý (từ khi Publisher nhận khối đến khi phát tín hiệu `BlockReady`).
8. **DAS Success Rate & Latency**: Tỷ lệ lấy mẫu thành công và thời gian phản hồi của các Light nodes.
9. **Total Stored Pieces per Store Node**: Số lượng mảnh dữ liệu (Custody + Recoded) phân bổ đều trên 8 Store nodes.

---

### 3.3. Prometheus Direct Queries

Truy cập Prometheus tại [http://localhost:9090](http://localhost:9090) để chạy các truy vấn nhanh:
- Kiểm tra node sống/chết: `up`
- Chiều cao CometBFT: `max(cometbft_consensus_height)`
- Chiều cao CDA hoàn tất: `cda_publisher_latest_completed_height`
- Kích thước $K$: `cda_publisher_latest_k`
- Số block in-flight: `cda_publisher_in_flight_blocks`
- Lưu lượng phát hành (bytes/s): `rate(cda_publisher_throughput_bytes_total[30s])`

---

## 4. Các Công Cụ Kiểm Thử (Testing Suites)

### 4.1. Kịch Bản Kiểm Thử E2E Tự Động (`test_cometbft_e2e_pipeline.py`)

Script [`scripts/tests/benchmark/test_cometbft_e2e_pipeline.py`](file:///home/ubuntu/cda-network/scripts/tests/benchmark/test_cometbft_e2e_pipeline.py) thực hiện quy trình kiểm tra 5 bước khép kín:
1. **Health Check**: Kiểm tra kết nối CometBFT RPC, Publisher, Bootstrap, Store nodes.
2. **Submit Transactions**: Đóng gói các transaction ngẫu nhiên và nộp qua RPC CometBFT `/broadcast_tx_commit`.
3. **Audit Block Height & CDA Dimensions**: Bắt chiều cao block đã commit, đối soát giá trị $K$, kích thước ODS/EDS, và Kate Commitments.
4. **BlockReady Signal Verification**: Chờ tín hiệu `BlockReady` từ cụm CDA và đo đạc độ trễ toàn trình.
5. **Custody Quorum Audit**: Truy vấn từng Store node để kiểm tra số lượng mảnh dữ liệu thực tế lưu trên BadgerDB có đạt 100% quota hay không.

#### Lệnh Chạy Cơ Bản:
```bash
# Kiểm tra nhanh với 16 transactions (payload 256 bytes)
python3 scripts/tests/benchmark/test_cometbft_e2e_pipeline.py --tx-count 16 --payload-size 256
```

#### Các Tham Số Tùy Chọn:
- `--tx-count`: Số giao dịch gửi trong một đợt (Mặc định: 16).
- `--payload-size`: Dung lượng mỗi giao dịch tính bằng byte (Mặc định: 256).
- `--cometbft`: Địa chỉ CometBFT RPC (Mặc định: `http://localhost:26657`).
- `--publisher`: Địa chỉ Publisher HTTP (Mặc định: `http://localhost:8080`).
- `--bootstrap`: Địa chỉ Bootstrap HTTP (Mặc định: `http://localhost:9200`).
- `--store-ports`: Danh sách cổng Store nodes (Mặc định: `9300-9307`).
- `--timeout`: Thời gian tối đa chờ hoàn tất BlockReady (Mặc định: 60s).

---

### 4.2. Bơm Tải Giao Dịch Linh Hoạt (`tx_submitter_bot.py`)

Script [`scripts/bots/tx_submitter_bot.py`](file:///home/ubuntu/cda-network/scripts/bots/tx_submitter_bot.py) được sử dụng để giả lập tải thực tế từ các ứng dụng Rollup L2 hoặc người dùng với khả năng chia theo nhiều đợt (batch) liên tục.

#### Ví Dụ 1: Bơm Đơn Khối Kích Thước Nhỏ (ODS $K=8$)
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 8 --payload-size 64 --batches 1
```

#### Ví Dụ 2: Bơm Khối Kích Thước Trung Bình (ODS $K=16$)
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 32 --payload-size 256 --batches 1
```

#### Ví Dụ 3: Bơm Khối Lớn (ODS $K=32$)
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 128 --payload-size 512 --batches 1
```

#### Ví Dụ 4: Bơm Tải Liên Tục Đa Đợt (Stress / Throughput Test)
Gửi 5 đợt liên tiếp, mỗi đợt 64 giao dịch, cách nhau 3 giây:
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 64 --payload-size 256 --batches 5 --delay 3.0
```

---

## 5. Hướng Dẫn Chạy Với Active Columns (active_cols = 1 hoặc 2)

Hệ thống CDA Network hỗ trợ chia nhỏ mạng lưới theo các cột (Column Sharding). Tham số `-c` (hoặc `--active-cols`) trong [`scripts/generate_compose.py`](file:///home/ubuntu/cda-network/scripts/generate_compose.py) cho phép cấu hình số lượng cột mạng thực tế được sinh ra và kích hoạt trong Docker.

Mỗi cột mạng hoạt động bao gồm:
- **1 Bootstrap Relay Node**: phụ trách tính toán bằng chứng KZG (FK20) và chuyển tiếp mảnh dữ liệu cho cột đó.
- **8 Store Nodes** (mặc định `-s 8`): phụ trách lưu trữ bảo chứng custody và tái mã hóa RLNC cho cột đó.

---

### 5.1. Cấu Hình 1 Active Column (`active_cols = 1`)

> **Khi nào nên dùng?**
> Thích hợp nhất cho máy phát triển cá nhân (dev machine / local laptop), máy có RAM hạn chế (8GB - 16GB) hoặc khi chỉ cần kiểm thử logic dòng chảy E2E.

- **Số lượng node**: 1 Bootstrap (`bootstrap-0`, port `9200`), 8 Store nodes (`store-0-1` đến `store-0-8`, ports `9300` - `9307`).

#### Các Bước Thực Hiện:

```bash
# 1. Dọn dẹp môi trường cũ (nếu có)
bash scripts/cleanup.sh

# 2. Khởi tạo 4 Validator CometBFT & Sinh Compose với 1 active column
python3 scripts/init_cometbft_cluster.py --validators 4
python3 scripts/generate_compose.py -c 1 --cometbft-validators 4

# 3. Khởi chạy toàn bộ cụm container
docker compose -f docker-compose.json up -d --build

# 4. Chạy CLI Monitor giám sát 8 Store nodes (ports 9300-9307)
python3 scripts/monitor/cda_network_monitor.py --mode stream --stores 9300-9307

# 5. Chạy kịch bản kiểm thử E2E tự động
python3 scripts/tests/benchmark/test_cometbft_e2e_pipeline.py --store-ports 9300 9301 9302 9303 9304 9305 9306 9307
```

---

### 5.2. Cấu Hình 2 Active Columns (`active_cols = 2`)

> **Khi nào nên dùng?**
> Thích hợp khi muốn kiểm thử tính năng **phân mảnh đa cột (Multi-Column P2P Sharding)**, kiểm tra khả năng Publisher phân tán dữ liệu song song qua nhiều Bootstrap relays và nhiều nhóm Store nodes độc lập.

- **Số lượng node**: 
  - 2 Bootstrap nodes: `bootstrap-0` (port `9200`), `bootstrap-1` (port `9201`).
  - 16 Store nodes: 
    - Cột 0: `store-0-1` đến `store-0-8` (ports `9300` - `9307`).
    - Cột 1: `store-1-1` đến `store-1-8` (ports `9308` - `9315`).

#### Các Bước Thực Hiện:

```bash
# 1. Dọn dẹp môi trường cũ (nếu có)
bash scripts/cleanup.sh

# 2. Khởi tạo 4 Validator CometBFT & Sinh Compose với 2 active columns
python3 scripts/init_cometbft_cluster.py --validators 4
python3 scripts/generate_compose.py -c 2 --cometbft-validators 4

# 3. Khởi chạy toàn bộ cụm container
docker compose -f docker-compose.json up -d --build

# 4. Chạy CLI Monitor giám sát toàn bộ 16 Store nodes (ports 9300-9315)
python3 scripts/monitor/cda_network_monitor.py --mode stream --stores 9300-9315

# 5. Chạy kịch bản kiểm thử E2E tự động trên cả 16 Store ports
python3 scripts/tests/benchmark/test_cometbft_e2e_pipeline.py --store-ports 9300 9301 9302 9303 9304 9305 9306 9307 9308 9309 9310 9311 9312 9313 9314 9315
```

---

### 5.3. Bảng So Sánh Cấu Hình Giữa C = 1 và C = 2

| Thông Số / Thành Phần | Chế Độ `active_cols = 1` | Chế Độ `active_cols = 2` |
| :--- | :--- | :--- |
| **Tham số lệnh generate** | `-c 1` | `-c 2` |
| **Bootstrap Nodes** | 1 node (`:9200`) | 2 nodes (`:9200`, `:9201`) |
| **Store Nodes** | 8 nodes (`:9300` - `:9307`) | 16 nodes (`:9300` - `:9315`) |
| **CometBFT Validators** | 4 nodes (`:26657`) | 4 nodes (`:26657`) |
| **Light Nodes (DAS)** | 2 nodes (`:9401`, `:9402`) | 2 nodes (`:9401`, `:9402`) |
| **Tổng số Containers** | ~18 containers | ~27 containers |
| **Mức RAM Khuyến Nghị** | $\ge 8\text{ GB}$ | $\ge 16\text{ GB}$ |
| **Cờ CLI Monitor `--stores`** | `--stores 9300-9307` | `--stores 9300-9315` |
| **Mục đích sử dụng chính** | Phát triển cục bộ, kiểm tra luồng E2E nhanh | Đo kiểm Sharding đa cột, cân bằng tải P2P |

---

## 6. Kịch Bản Kiểm Thử Toàn Trình Mẫu (Step-by-Step Walkthrough)

Dưới đây là kịch bản chuẩn được khuyến nghị khi thực hiện nghiệm thu mạng lưới:

### Bước 1: Mở 2 Cửa Sổ Terminal Theo Dõi
- **Terminal 1**: Chạy Stream Monitor:
  ```bash
  python3 scripts/monitor/cda_network_monitor.py --mode stream
  ```
- **Terminal 2 (hoặc trình duyệt)**: Mở Grafana tại `http://localhost:3000`.

### Bước 2: Quan Sát Trạng Thái Khối Trống (Empty Blocks)
Khi không có giao dịch nào nộp vào mạng:
- CometBFT vẫn sinh khối đều đặn (Block time ~1s).
- Kích thước ODS ghi nhận là $K=8$ ($8 \times 8 = 64$ cells).
- Mempool duy trì mức 0 txs.

### Bước 3: Thực Hiện Bơm Tải Giao Dịch
Trên **Terminal 3**, kích hoạt bơm 1 batch 32 transactions:
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 32 --payload-size 256 --batches 1
```

### Bước 4: Đánh Giá Kết Quả Trên Các Kênh
1. **Trên Terminal 1 (CLI Monitor)**:
   - Thấy sự kiện `STAGE 1: CONSENSUS` ghi nhận Mempool có 32 txs.
   - Thấy sự kiện `STAGE 2: PUBLISHER` xử lý ma trận chuyển sang $K=16$ ($16 \times 16 = 256$ cells).
   - Thấy sự kiện `STAGE 3: CDA BLOCK_READY` báo cáo 100% Store nodes custody và kích hoạt DAS.
2. **Trên Grafana**:
   - Panel **CometBFT Block Height** và **CDA BlockReady Height** đồng bộ tiến lên.
   - Panel **CDA Dimension K** nhảy lên giá trị 16.
   - Panel **CDA Pipeline In-Flight** tăng lên 1 rồi giảm về 0 sau khi hoàn tất.
   - Biểu đồ **CDA Pipeline Latency** ghi nhận điểm đo độ trễ thực tế.

---

## 7. Xử Lý Sự Cố & Dọn Dẹp (Troubleshooting & Cleanup)

### 7.1. Xử Lý Khi Container Bị Lệch Nhịp Hoặc Treo
Nếu muốn khởi động lại chỉ riêng Publisher và Grafana mà không làm gián đoạn cụm CometBFT:
```bash
docker compose -f docker-compose.json restart publisher grafana
```

Nếu cần biên dịch lại mã nguồn Go mới nhất vào container:
```bash
docker compose -f docker-compose.json up -d --build publisher
```

### 7.2. Dọn Dẹp Sạch Sẽ Môi Trường (Reset Data)
Khi muốn xóa toàn bộ dữ liệu lịch sử blockchain, cơ sở dữ liệu BadgerDB của Store nodes và khởi động lại từ khối #1:
```bash
# Dừng và xóa toàn bộ container, networks và volumes
bash scripts/cleanup.sh

# Khởi tạo lại cụm từ đầu
python3 scripts/init_cometbft_cluster.py --validators 4
python3 scripts/generate_compose.py
docker compose -f docker-compose.json up -d --build
```

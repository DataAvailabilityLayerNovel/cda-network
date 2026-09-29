# Hướng Dẫn Đo Lường Hiệu Năng & Độ Trễ Xử Lý Khối (CDA Benchmark & Latency Guide)

Tài liệu này cung cấp hướng dẫn toàn diện về bộ công cụ đo lường hiệu năng, độ trễ xử lý khối và thông lượng (throughput) trong mạng lưới **CDA Network**.

---

## 1. Tổng Quan Kiến Trúc Đo Lường (Benchmark Architecture)

Trong mạng lưới CDA, thời gian xử lý một khối dữ liệu ($T_{\text{block}}$) từ lúc sinh ra đến khi toàn bộ mạng hoàn thành lưu trữ và sẵn sàng phục vụ lấy mẫu được định nghĩa:

$$T_{\text{block}} = T_{\text{publish}} + T_{\text{kzg\_precompute}} + T_{\text{seed\_dispatch}} + T_{\text{dissemination\_lock}}$$

Trong đó:
1. **$T_{\text{publish}}$**: Thời gian Client đẩy khối dữ liệu $K \times K$ lên Publisher API (HTTP JSON hoặc BFT consensus payload).
2. **$T_{\text{kzg\_precompute}}$**: Thời gian Bootstrap Node nhận cột ODS, tính toán mở rộng ma trận Reed-Solomon sang EDS ($2K \times 2K$), sinh $2K$ cam kết KZG cột và sinh trước các KZG Opening Proofs cùng RLNC Seeds trong buffer.
3. **$T_{\text{seed\_dispatch}}$**: Thời gian Bootstrap phát tán các lô mảnh hạt giống (Seed Pieces) sang các Store Node phụ trách cột thông qua giao thức nhị phân và QUIC Stream.
4. **$T_{\text{dissemination\_lock}}$**: Thời gian Store Nodes trao đổi recoded pieces qua GossipSub để đạt trạng thái `IsComplete` (đầy đủ các ô Custody và ít nhất 1 mảnh xác thực trên mọi ô Non-Custody) và phát tín hiệu `StoreReady` về Publisher để phát `BlockReady`.

---

## 2. Các Công Cụ Đo Lường Chính

| Script | Ngôn Ngữ | Chế Độ Hoạt Động | Mục Đích Sử Dụng |
| :--- | :---: | :--- | :--- |
| [`scripts/tests/benchmark/test_pipeline_block_timing.py`](file:///home/ubuntu/cda-network/scripts/tests/benchmark/test_pipeline_block_timing.py) | Python 3 | Pipelined 1-Block-Ahead | Đo hiệu năng tối ưu khi tính toán KZG/RLNC của khối tiếp theo được thực hiện trước song song (pre-computation pipeline). |
| [`scripts/tests/benchmark/test_sequential_block_timing.py`](file:///home/ubuntu/cda-network/scripts/tests/benchmark/test_sequential_block_timing.py) | Python 3 | Strictly Sequential | Đo độ trễ cơ sở từng khối tuần tự độc lập (không gối đầu). |
| [`scripts/tests/benchmark/benchmark_matrix_runner.py`](file:///home/ubuntu/cda-network/scripts/tests/benchmark/benchmark_matrix_runner.py) | Python 3 | Automated Matrix Suite | Tự động quét và đo đạc qua nhiều cấu hình $K$, giới hạn CPU, kích thước batch và số workers. |
| [`scripts/tests/benchmark/analyze_benchmarks.py`](file:///home/ubuntu/cda-network/scripts/tests/benchmark/analyze_benchmarks.py) | Python 3 | Report Generator | Đọc kết quả JSON từ `data/benchmarks/` và xuất báo cáo Markdown phân tích độ nhạy. |
| [`scripts/bots/block_publisher_bot.py`](file:///home/ubuntu/cda-network/scripts/bots/block_publisher_bot.py) | Python 3 | Event-Driven / Streaming | Bot phát khối liên tục theo tín hiệu sự kiện SSE `/events/block-ready`. |
| [`scripts/bots/bot_das.sh`](file:///home/ubuntu/cda-network/scripts/bots/bot_das.sh) | Bash | Reactive Sampling | Bot liên tục kích hoạt DAS trên Light Node mỗi khi có khối mới. |

---

## 3. Hướng Dẫn Sử Dụng Từng Công Cụ

### 3.1. Đo Lường Pipeline Đẩy Khối Trước (`test_pipeline_block_timing.py`)

Kịch bản này là công cụ đo đạc chuẩn để đánh giá hiệu năng sau khi áp dụng tối ưu hóa Protobuf, QUIC, Binary Codec và Fast Locking.

```bash
# 1. Chạy 1 block kích thước chuẩn 2MB (K=64, cell_size=512)
python3 scripts/tests/benchmark/test_pipeline_block_timing.py --k 64 --count 1 --cell-size 512 --timeout 180

# 2. Chạy chuỗi 3 blocks liên tiếp để đánh giá tốc độ che giấu độ trễ (latency hiding)
python3 scripts/tests/benchmark/test_pipeline_block_timing.py --k 64 --count 3 --cell-size 512 --timeout 180

# 3. Chạy với khối nhỏ K=16 (64B/cell) để kiểm tra nhanh
python3 scripts/tests/benchmark/test_pipeline_block_timing.py --k 16 --count 5 --cell-size 64
```

**Bảng tham số CLI:**
- `--publisher`: Địa chỉ Publisher API (mặc định: `http://localhost:8080`).
- `--bootstrap`: Địa chỉ Bootstrap API (mặc định: `http://localhost:9200`).
- `--k`: Kích thước cạnh ma trận ODS ($K$). Số ô ODS là $K^2$.
- `--count`: Số lượng khối cần chạy benchmark liên tiếp.
- `--cell-size`: Kích thước mỗi ô dữ liệu (bytes), ví dụ `64` (mặc định) hoặc `512` (2MB block).
- `--timeout`: Thời gian tối đa (giây) chờ mỗi khối trước khi hủy.

---

### 3.2. Đo Lường Tuần Tự Cơ Sở (`test_sequential_block_timing.py`)

Dùng để xác định độ trễ thô độc lập của từng khối (Cold Latency):

```bash
python3 scripts/tests/benchmark/test_sequential_block_timing.py --k 64 --count 3 --cell-size 512 --timeout 180
```

---

### 3.3. Tự Động Hóa Ma Trận Benchmark Toàn Hệ Thống (`benchmark_matrix_runner.py`)

Script này tự động:
1. Sinh file cấu hình `docker-compose-benchmark.yml` tương ứng với từng kịch bản.
2. Dọn dẹp tài nguyên và khởi động lại cụm Docker.
3. Chạy đo thời gian xử lý khối với các mức kích thước ma trận và tham số song song hóa khác nhau.
4. Thu thập mức sử dụng CPU/RAM và lưu dữ liệu JSON vào `data/benchmarks/`.

```bash
# Chạy ma trận baseline trên K=8 và K=16 (3 blocks/kịch bản):
python3 scripts/tests/benchmark/benchmark_matrix_runner.py --type baseline --matrix-k 8,16 --blocks 3

# Chạy khảo nghiệm độ nhạy kích thước Batch GossipSub (Batch Size 16, 32, 48, 64):
python3 scripts/tests/benchmark/benchmark_matrix_runner.py --type batching --blocks 3

# Chạy khảo nghiệm giới hạn tài nguyên CPU (2, 4, 8 Cores):
python3 scripts/tests/benchmark/benchmark_matrix_runner.py --type cpu_cores --blocks 3
```

---

### 3.4. Xuất Báo Cáo & So Sánh Hiệu Năng (`analyze_benchmarks.py`)

Sau khi chạy xong ma trận benchmark, sử dụng script phân tích để sinh bảng tổng hợp:

```bash
# Phân tích file benchmark mới nhất
python3 scripts/tests/benchmark/analyze_benchmarks.py data/benchmarks/benchmark_latest.json

# Xuất ra file báo cáo Markdown
python3 scripts/tests/benchmark/analyze_benchmarks.py data/benchmarks/benchmark_latest.json --output data/benchmarks/report_latest.md
```

---

## 4. Quy Trình Chuẩn Bị & Chạy Benchmark Hoàn Chỉnh

Để đảm bảo kết quả đo lường khách quan và chính xác:

```bash
# Bước 1: Dọn dẹp sạch sẽ tài nguyên và cơ sở dữ liệu cũ
bash scripts/cleanup.sh

# Bước 2: Khởi động cụm Docker với cấu hình benchmark (ví dụ K=64, k_piece=8)
docker compose -f docker-compose-benchmark.yml up -d

# Bước 3: Đợi 5 giây để các node hoàn tất handshake P2P ban đầu
sleep 5

# Bước 4: Thực hiện đo đạc benchmark
python3 scripts/tests/benchmark/test_pipeline_block_timing.py --k 64 --count 3 --cell-size 512 --timeout 180

# Bước 5: Xem log chi tiết nếu cần phân tích sâu
docker compose -f docker-compose-benchmark.yml logs --tail=50 publisher bootstrap-0 store-0-1
```

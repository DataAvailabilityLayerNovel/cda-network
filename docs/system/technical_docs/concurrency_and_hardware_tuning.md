# ĐẶC TẢ THIẾT LẬP CONCURRENCY & TỐI ƯU HÓA PHẦN CỨNG (CDA NETWORK)

---

## I. TỔNG QUAN HỆ THỐNG & PHÂN BỔ PHẦN CỨNG HOST

Trong mạng Data Availability (CDA), hiệu năng thông lượng (Throughput) và độ trễ khối (Latency) chịu ảnh hưởng kết hợp từ:
1. **Phép tính mật mã đại số nặng**: Tính toán đa thức KZG Opening Proof, MSM (Multi-Scalar Multiplication) thuật toán Pippenger, phép ghép cặp đường cong Elliptic Pairings trên BLS12-381.
2. **Luồng truyền tải phân tán P2P**: LibP2P Stream, GossipSub PubSub Mesh, gộp mảng nhị phân Seed Pieces.
3. **Quản lý bộ nhớ đệm và khóa**: Tránh bão Goroutine (Goroutine Storm) và tranh chấp khóa đồng thời (Lock Contention) trên hàng nghìn Cell dữ liệu.

### 1. Thông số Phần cứng Môi trường Máy chủ (Host)
* **CPU Host**: **12 Cores / Threads** vật lý (`nproc = 12`).
* **RAM Host**: 64 GB+ ECC Memory.
* **Hệ điều hành**: Linux Ubuntu (Kernel tối ưu I/O đa luồng).

### 2. Thiết lập Môi trường Docker Container
Trong `scripts/generate_compose.py`:
* **Không giới hạn tài nguyên cứng (No CPU/Memory Throttling)**: Các container chạy trong cùng bridge network `cda-net` không đặt `cpus` hay `mem_limit`.
* **Cơ chế chia sẻ CPU**: Tất cả các node (**1 Publisher, 1 Bootstrap, 8 Store Nodes, 2 Light Nodes**) cùng chia sẻ tài nguyên 12 cores của Host.
* **Go Runtime**: Mặc định `GOMAXPROCS = 12` trong mỗi container, cho phép Go Scheduler và thư viện toán học `gnark-crypto` tận dụng tối đa đa nhân song song.

---

## II. BẢNG THIẾT LẬP CONCURRENCY CHI TIẾT THEO TỪNG NODE

### 1. Publisher Node (`cda-publisher-node`)

Publisher chịu trách nhiệm mã hóa mở rộng 2D Reed-Solomon (Leopard Codec), tạo cam kết KZG toàn cục và phân phối các cột dữ liệu sang Bootstrap Nodes.

| Thành phần / Cơ chế | Thiết lập Concurrency | Vị trí Mã nguồn | Mục đích & Đánh giá |
| :--- | :---: | :--- | :--- |
| **`CommitEDS` Worker Pool** | `runtime.NumCPU()` (Tối đa **16**) | `rlnc-rsmt2d/cda/kzg.go` | Song song hóa việc tính $N \times k$ cam kết mảnh KZG trên 12 lõi CPU. Giảm thời gian sinh cam kết từ ~2.5s xuống ~250ms ở $K=64$. |
| **Column Combine (`colWg`)** | **$N$ Goroutines** ($N=2K$) | `rlnc-rsmt2d/cda/publisher.go` | Tính tổ hợp cam kết cột $C^{\text{col}}_c$ bằng vector thử thách Fiat-Shamir toàn cục $x$. |
| **P2P Column Distribution** | **$N$ Goroutines** song song | `cda-publisher-node/internal/service/api.go` | Mở đồng thời $N$ stream P2P để truyền tải các cột dữ liệu sang Bootstrap tương ứng. |
| **Tách biệt Compute & Dispatch** | Phi khóa `ProcessODS` | `cda-publisher-node/internal/service/api.go` | Cho phép Block $i+1$ bắt đầu tính ODS/EDS ngay lập tức trong lúc Block $i$ đang truyền tải qua mạng P2P. |
| **In-Flight Queue Limits** | `MAX_IN_FLIGHT = 2` | `api.go` (Environment) | Giới hạn số block tối đa được tính toán gối đầu cùng thời điểm trong bộ nhớ đệm. |

---

### 2. Bootstrap Node (`cda-bootstrap-node`)

Bootstrap đóng vai trò định tuyến, tính toán KZG Opening Proofs và mã hóa RLNC thành các Seed Pieces để phân phối về các Store Nodes trong cột.

| Thành phần / Cơ chế | Thiết lập Concurrency | Vị trí Mã nguồn | Mục đích & Đánh giá |
| :--- | :---: | :--- | :--- |
| **KZG Opening Proof Gen** | **$N$ Goroutines** ($N=2K$) | `cda-bootstrap-node/internal/engine/proof_generator.go` | Tính toán $N \times k$ KZG opening proofs cho từng cell trong cột song song qua `ComputeOpenProofCell`. |
| **P2P Batch Seeding Semaphore** | **64 streams** (`make(chan struct{}, 64)`) | `cda-bootstrap-node/internal/p2p/receiver.go` | Semaphore chặn luồng mở stream, giới hạn tối đa 64 stream gửi seed cùng lúc tới Store Nodes. |
| **Batch Chunk Size** | **64 seeds / chunk** | `receiver.go` | Gom 64 mảnh hạt giống trong một payload batch duy nhất để giảm overhead bắt tay P2P stream. |
| **Pre-computation In-Flight Buffer**| **1-Block-Ahead Buffer** | `receiver.go` | Sinh sẵn toàn bộ seed cho Block $i+1$ và kích hoạt phát tức thì (Instant Dispatch) ngay khi Store Nodes hoàn thành Block $i$. |

---

### 3. Store Node (`cda-store-node`)

Store Node là thành phần chịu tải cao nhất (lưu trữ custody, xác thực mã hóa, tái mã hóa RLNC và phân tán GossipSub). Concurrency tại đây được kiểm soát nghiêm ngặt nhất:

| Thành phần / Cơ chế | Thiết lập Concurrency | Vị trí Mã nguồn | Mục đích & Đánh giá |
| :--- | :---: | :--- | :--- |
| **Sharded Cell Worker Pool** | `NumCPU() * 2` (Máy 12 cores = **24 Workers**) | `cda-store-node/internal/p2p/receiver.go` | Hàng đợi phân mảnh theo băm `cellKey`. Đảm bảo các tác vụ của cùng 1 cell chạy tuần tự tuyệt đối, triệt tiêu race condition. |
| **Worker Queue Capacity** | **2.000 tasks / channel** | `receiver.go` | Dung lượng đệm hàng đợi cho từng worker channel. |
| **GossipSub Batch Workers** | **2 Workers** | `receiver.go` | 2 luồng độc lập gom batch và gọi **KZG Batch Verify**. Giữ ở mức 2 để thuật toán Pippenger MSM của Gnark chiếm dụng trọn vẹn L3 cache mà không xung đột luồng. |
| **Gossip Batch Size & Ticker** | **48 pieces** / **10ms** | `receiver.go` | Gom tối đa 48 mảnh hoặc xả sau 10ms. Giảm chi phí ghép cặp từ $2N$ pairings xuống chỉ còn **2 pairings duy nhất**. |
| **Dissemination Semaphore** | **16 Goroutines** (`make(chan struct{}, 16)`) | `receiver.go` | Giới hạn tối đa 16 luồng recode và broadcast đồng thời khi các custody cell đạt full rank, ngăn ngừa quá tải hàng đợi GossipSub. |
| **Fallback Pull Semaphore** | **8 Streams** (`make(chan struct{}, 8)`) | `receiver.go` | Giới hạn tối đa 8 luồng kéo bù trực tiếp (Active Pull Direct Stream) khi bị rớt cell. |
| **Debounced Completion Worker** | Buffer **1.024**, ticker **40ms** | `receiver.go` | Gộp hàng nghìn tín hiệu kiểm tra `IsComplete`, chỉ quét chẩn đoán trạng thái block 40ms/lần, loại bỏ >99% lock contention. |
| **Fallback Grace Period** | **5 giây** (`time.Sleep(5s)`) | `receiver.go` | Thời gian chờ đợi GossipSub tự nhiên trước khi kích hoạt kéo bù dữ liệu. |

---

### 4. Light Node (`cda-light-node`)

Light Node thực hiện Data Availability Sampling (DAS) để kiểm định tính khả dụng của khối mà không cần tải toàn bộ dữ liệu.

| Thành phần / Cơ chế | Thiết lập Concurrency | Vị trí Mã nguồn | Mục đích & Đánh giá |
| :--- | :---: | :--- | :--- |
| **DAS Sampling Semaphore** | **16 Goroutines** (`make(chan struct{}, 16)`) | `cda-light-node/internal/service/api.go` | Giới hạn 16 luồng đồng thời gửi truy vấn P2P lấy 64 mẫu ngẫu nhiên từ các Store Node. |
| **Algebraic Verification** | Batch verification | `api.go` | Khôi phục cell từ các mảnh RLNC và đối chiếu cam kết KZG đại số. |

---

## III. PHÂN TÍCH TẢI TÀI NGUYÊN THEO QUY MÔ MA TRẬN ($K$)

Khi nâng quy mô $K$ (kích thước khối dữ liệu), khối lượng tính toán và mức độ cạnh tranh concurrency tăng theo cấp số nhân:

| Thông số | $K = 32$ | $K = 64$ | $K = 128$ | Ghi chú & Tác động |
| :--- | :---: | :---: | :---: | :--- |
| **Ma trận EDS ($2K \times 2K$)** | $64 \times 64 = 4.096$ cells | $128 \times 128 = 16.384$ cells | $256 \times 256 = \mathbf{65.536\text{ cells}}$ | Khối lượng dữ liệu gấp 4 lần qua mỗi bậc $K$. |
| **Số Cells / Store Node (16 cols, 1 active)** | 256 cells | 1.024 cells | **4.096 cells** | Lưu trữ và xử lý trực tiếp trên mỗi node. |
| **Số Mảnh (Pieces) / Store Node ($p=4$)** | 1.024 pieces | 4.096 pieces | **16.384 pieces** | Số lượng piece cần verify, lưu trữ và recode. |
| **Số KZG Proofs / Cột (Bootstrap)** | 256 base proofs | 512 base proofs | **1.024 base proofs** | Phép tính `Open` nặng tại Bootstrap Node. |
| **Thời gian xử lý trung bình / Block** | **~1.2s - 1.5s** | **~7.5s - 8.0s** | **~34s - 36s** | Kết quả thực nghiệm trên Host 12 Cores. |
| **Trạng thái Mạng P2P** | Rất nhẹ, 0% drop | Nhẹ, drop ngẫu nhiên 1-2 cell | Lưu lượng cao, cần pipeline gối đầu sâu | Tỷ lệ can thiệp của Fallback Pull. |

---

## IV. HƯỚNG DẪN ĐIỀU CHỈNH CONCURRENCY KHI THAY ĐỔI CẤU HÌNH PHẦN CỨNG

### 1. Khi chạy ở quy mô siêu lớn ($K \ge 128$)
1. **Kiểm soát Goroutine tại Bootstrap (`proof_generator.go`)**:
   - Hiện tại vòng lặp $N$ trong `GenerateColumnProofs` mở đồng thời 256 goroutines. Trên máy 12 cores, điều này gây áp lực context-switch lớn.
   - **Khuyến nghị**: Bọc vòng lặp bằng `sem := make(chan struct{}, 16)` để giới hạn số luồng tính `ComputeOpenProofCell` đồng thời tối đa là 16.
2. **Tăng Batch Size tại Store Node (`receiver.go`)**:
   - Tăng `batchSize` từ **48 lên 64 hoặc 96**. Với 16.384 pieces đổ về, batch lớn hơn sẽ tối ưu hóa hiệu suất phép tính toán Pippenger MSM của Gnark.

### 2. Khi chạy trên Server có nhiều CPU Cores (Ví dụ: 32 Cores - 64 Cores)
1. **Tăng số Worker Gom Batch (`receiver.go`)**:
   - Tăng `rcv.startGossipBatchWorkers(ctx, 2)` từ 2 lên **4 hoặc 6 workers**.
2. **Tăng Semaphore Dissemination (`receiver.go`)**:
   - Tăng `disseminationSem` từ 16 lên **32 hoặc 48** để tăng tốc độ phân tán GossipSub khi các cell đạt full rank.

---

## V. BẢNG THAM SỐ BIẾN MÔI TRƯỜNG & CÔNG CỤ ĐO LƯỜNG TỰ ĐỘNG

### 1. Bảng Tra Cứu Biến Môi Trường Điều Khiển Concurrency

Tất cả các tham số concurrency đều có thể ghi đè linh hoạt qua biến môi trường hoặc cờ lệnh của `scripts/generate_compose.py`:

| Node | Biến Môi Trường (Environment Variable) | Cờ Lệnh `generate_compose.py` | Giá Trị Mặc Định | Ý Nghĩa Kỹ Thuật |
| :--- | :--- | :--- | :---: | :--- |
| **Store** | `STORE_GOSSIP_BATCH_SIZE` | `--store-gossip-batch-size` | `48` | Kích thước batch gom mảnh GossipSub trước khi gọi KZG Batch Verify (Pippenger MSM). |
| **Store** | `STORE_GOSSIP_BATCH_WORKERS` | `--store-gossip-batch-workers` | `2` | Số worker độc lập gom và verify batch song song. |
| **Store** | `STORE_GOSSIP_BATCH_TICKER_MS` | `--store-gossip-batch-ticker-ms` | `10` | Chu kỳ flush batch tối đa (ms) nếu chưa gom đủ mảnh. |
| **Store** | `STORE_DISSEMINATION_SEM` | `--store-dissemination-sem` | `16` | Semaphore giới hạn số luồng broadcast recoded pieces qua GossipSub đồng thời. |
| **Store** | `STORE_FALLBACK_PULL_SEM` | `--store-fallback-pull-sem` | `8` | Semaphore giới hạn số luồng kéo bù trực tiếp (Direct Stream Pull). |
| **Store** | `STORE_SHARDED_WORKERS` | `--store-sharded-workers` | `NumCPU() * 2` | Kích thước hàng đợi phân mảnh theo băm cell ID. |
| **Bootstrap** | `BOOTSTRAP_PROOF_GEN_SEM` | `--bootstrap-proof-gen-sem` | `0` (Unbounded) | Semaphore giới hạn số luồng tính `ComputeOpenProofCell` đồng thời. Khuyến nghị `16` khi $K \ge 128$. |
| **Bootstrap** | `BOOTSTRAP_SEEDING_SEM` | `--bootstrap-seeding-sem` | `64` | Giới hạn số stream P2P gửi seed đồng thời tới Store Nodes. |
| **Bootstrap** | `BOOTSTRAP_BATCH_CHUNK_SIZE` | `--bootstrap-batch-chunk-size` | `64` | Kích thước gói seed trong một batch payload stream. |
| **Publisher** | `PUBLISHER_MAX_IN_FLIGHT` | `--publisher-max-in-flight` | `2` | Số block tối đa được tính toán gối đầu cùng thời điểm. |
| **Tất cả** | `GOMAXPROCS` | `--gomaxprocs` | `nproc` | Số luồng hệ điều hành tối đa Go Runtime được phép dùng. |
| **Container** | Quota CPU Limits | `--cpus` | Không giới hạn | Giới hạn CPU container trong Docker (`deploy.resources.limits.cpus`). |

---

### 2. Bộ Công Cụ Đo Lường Hiệu Năng Tự Động

1. **Chạy Ma Trận Đo Lường Đa Cấu Hình (`scripts/benchmark_matrix_runner.py`)**:
   - Tự động sinh compose, khởi động môi trường, chạy kiểm thử, thu thập metrics và xuất file JSON:
   ```bash
   # Quét nhanh chế độ Sequential vs Pipeline ở K=8, K=16:
   python3 scripts/benchmark_matrix_runner.py --type baseline --matrix-k 8,16 --blocks 3

   # Quét độ nhạy của Batch Size và Workers tại Store Node:
   python3 scripts/benchmark_matrix_runner.py --type batching --blocks 3

   # Quét Semaphore tính proof trên Bootstrap Node:
   python3 scripts/benchmark_matrix_runner.py --type bootstrap_sem --blocks 3

   # Quét theo hạn mức CPU Cores (4, 8, 12 cores):
   python3 scripts/benchmark_matrix_runner.py --type cpu_cores --blocks 3
   ```

2. **Phân Tích Kết Quả & Xuất Báo Cáo (`scripts/analyze_benchmarks.py`)**:
   - Đọc kết quả từ `data/benchmarks/benchmark_latest.json` và in bảng đối chiếu hiệu năng:
   ```bash
   python3 scripts/analyze_benchmarks.py data/benchmarks/benchmark_latest.json --out-md data/benchmarks/report.md
   ```


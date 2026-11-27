# CDA Network - Hướng Dẫn Vận Hành & Thao Tác Hệ Thống (Operations Guide)

Tài liệu này cung cấp hướng dẫn thực hành chi tiết, chuẩn hóa toàn diện quy trình vận hành cụm mạng **CDA Network** tích hợp lớp đồng thuận **CometBFT**, bao gồm: khởi tạo cụm, nộp giao dịch (batch & single large tx), giám sát trực quan với Grafana/Prometheus/CLI, và xử lý các sự cố vận hành thường gặp.

---

## 1. Tổng Quan Kiến Trúc & Tham Số Kỹ Thuật Chuẩn Hóa

### 1.1. Luồng Dữ Liệu Toàn Trình (End-to-End Pipeline)

Hệ thống hoạt động theo quy trình 4 giai đoạn khép kín:

```mermaid
flowchart LR
    A["Rollup L2 / User Txns"] -->|"Broadcast RPC :26657"| B["CometBFT Consensus (L1)"]
    B -->|"Committed Block Data"| C["CDA Engine / Publisher (:8080)"]
    C -->|"2D Reed-Solomon + RLNC Pieces"| D["Bootstrap Relay (:9200)"]
    D -->|"FK20 KZG Proofs + GossipSub"| E["8 Store Nodes (:9300-9307)"]
    E -->|"Custody Storage & Recoding"| F["CDA BlockReady Event"]
    F -->|"Light Clients DAS Sampling"| G["Light Nodes (:9401-9402)"]
```

1. **Giai đoạn 1 - CometBFT Consensus (L1)**: Nhận transaction từ client qua RPC (cổng `26657`), đưa vào Mempool, đồng thuận đề xuất block và commit block.
2. **Giai đoạn 2 - CDA Publisher (L2 Pipeline)**: Nhận block data từ CometBFT, chia thành các ô dữ liệu (cell), mở rộng ma trận ODS $K \times K \to$ EDS $2K \times 2K$ bằng mã Reed-Solomon 2D, đóng gói thành các mảnh RLNC pieces.
3. **Giai đoạn 3 - Bootstrap Relays & KZG Proving**: Tính toán đa thức KZG commitments và bằng chứng FK20 (tối ưu Jacobians/Mont), gắn proof vào từng piece và phát tán qua LibP2P GossipSub.
4. **Giai đoạn 4 - Store Nodes Custody & BlockReady**: 8 Store nodes nhận mảnh, xác thực mật mã KZG, lưu trữ custody vào BadgerDB, tiến hành RLNC recoding. Khi đạt đủ 100% quorum custody, Publisher phát tín hiệu `CDA BlockReady`. Light nodes bắt đầu lấy mẫu DAS.
5. **Giai đoạn 5 - Data & Transaction Retrieval (Khôi Phục Dữ Liệu & Truy Vấn)**: Rollup nodes hoặc client truy xuất toàn bộ dữ liệu khối (`GET /block/:height/data`) hoặc truy vấn giao dịch ngược qua Hot Path ($O(1)$) và Cold Path (CDA P2P batch fetch & giải mã RLNC).

---

### 1.2. Các Tham Số Chuẩn Hóa Toàn Hệ Thống

Toàn bộ các thành phần trong hệ thống đã được đồng bộ chuẩn hóa theo các tham số sau:

| Tham số | Giá trị chuẩn | Mô tả kỹ thuật |
| :--- | :--- | :--- |
| **Cell Size** | **512 bytes** | Kích thước chuẩn của một ô trong ma trận ODS/EDS (đồng bộ trong CometBFT CDA Engine, Publisher, Store Node và Submitter Bot). |
| **$k_{\text{piece}}$** | **4 fragments / cell** | Mỗi ô dữ liệu (cell 512B) được chia thành 4 fragments ($512\text{B} / 4 = 128\text{B}$). Một mảnh RLNC piece là một tổ hợp tuyến tính của 4 fragments này (payload = 128B). Cần tối thiểu 4 mảnh độc lập tuyến tính để giải mã phục hồi 1 cell hoàn chỉnh. |
| **Network Matrix ($R_{\text{net}} \times C_{\text{net}}$)** | **$4-8$ rows $\times 8$ cols** | Ma trận mạng P2P 2 chiều (`--net-rows`, `--net-cols`). Mỗi ô mạng $(r_{\text{net}}, c_{\text{net}})$ có thể chứa $M \ge 1$ Custody Store Nodes (`--nodes-per-cell`). |
| **Chia tải Custody (`Per-SubRow Striping`)** | **`1 Primary + 1 Backup` / subRow** | Khi 1 ô mạng có $M \ge 2$ node, mỗi `(blockID, subRow)` chỉ chọn tất định **1 Primary Custody (lưu 4 mảnh)** + **1 Backup Custody (recode rồi cắt về 1 mảnh)** xoay vòng theo `FNV1a(blockID, netRow) + subRow`, còn $M-2$ node còn lại làm Non-Custody (giữ 1 mảnh). |
| **Điểm Tín Nhiệm (`TrustScore`)** | **$0.0 - 100.0$ (Khởi tạo $60.0$)** | Phân hạng `TRUSTED` ($\ge 75$), `STANDARD` ($40-74.9$), `PROBATION` ($20-39.9$, sàn bảo vệ lỗi mềm $25.0$), `QUARANTINED` ($0.0$, chỉ khi gian lận mật mã KZG). Có cơ chế `Straggler Forgiveness` và `Co-Primary Guardian`. |
| **Kênh GossipSub Kép** | **`/seed` & `/recode`** | Bootstrap phát bể $2 \cdot k_{\text{piece}}$ hạt giống lên `/cda/1.0.0/col/<c>/row/<r>/seed`; Custody Nodes lấy mẫu ngẫu nhiên $S_{\text{node}} = \max(2, \lceil 2k_{\text{piece}}/M_{\text{active}} \rceil)$ mảnh và phát $Q = \max(1, \lceil k_{\text{piece}}/M_{\text{active}} \rceil) + 1$ mảnh recode lên `/cda/1.0.0/col/<c>/row/<r>/recode`. |
| **RPC Max Body Size** | **10 MB** (`10000000`) | Giới hạn payload HTTP tiếp nhận của CometBFT RPC để hỗ trợ các giao dịch dung lượng lớn. |
| **In-Flight Queue** | **Tối đa 32 blocks** | Hàng đợi pipeline xử lý song song của CDA Publisher trước khi áp dụng backpressure. |

> [!NOTE]
> **Quy tắc phân rã Cell, Chia tải Sub-Row & Bảo vệ Tín nhiệm**:
> - **Kích thước Fragment**: $\text{Fragment Size} = \frac{\text{Cell Size}}{k_{\text{piece}}} = \frac{512\text{ bytes}}{4} = 128\text{ bytes}$.
> - **Primary Custody (Full Rank)**: Giữ đủ $\text{Rank} = k_{\text{piece}} = 4$ mảnh RLNC độc lập tuyến tính dài hạn.
> - **Backup Custody (Immediate Pruning)**: Lấy mẫu 4 hạt giống từ `/seed`, phát đủ hạn ngạch $Q = 3$ mảnh recode lên `/recode`, sau đó **cắt tỉa (prune) ngay lập tức** về 1 mảnh recode và tự động hủy đăng ký (`Unsubscribe`) kênh `/seed` của hàng lân cận khi hàng đó đã có $\ge 2$ node bản địa.
> - **Co-Primary Guardian (Bảo vệ Kép khi `PROBATION`)**: Nếu một ô đơn node ($M=1$) có node duy nhất bị tụt điểm xuống hạng `PROBATION` ($T < 40.0$), hệ thống không abort node đó mà tự động nâng node Backup mượn từ hàng khác ($T \ge 40.0$) lên giữ song song $k_{\text{piece}} = 4$ mảnh gốc cho đến khi node bản địa tự hồi phục điểm (`+2.0/block`).

---

### 1.3. Bảng Ánh Xạ Kích Thước Dữ Liệu Khối sang Chiều $K$ của ODS

Chiều ma trận ODS $K$ được tính toán tự động dựa trên tổng số cells cần thiết:

$$K = 2^{\lceil \log_2(\sqrt{\lceil \text{TotalBytes} / 512 \rceil}) \rceil}$$

| Kích Thước Dữ Liệu Payload | Số Cells Tối Thiểu | Chiều Ma Trận ODS ($K$) | Kích Thước EDS ($2K \times 2K$) | Tổng Dung Lượng ODS Tối Đa |
| :--- | :--- | :--- | :--- | :--- |
| **Trống / Nhỏ (< 32 KB)** | $1 - 64$ cells | **$K = 8$** ($8 \times 8 = 64$ cells) | $16 \times 16 = 256$ cells | 32 KB |
| **Trung bình (32 KB - 128 KB)** | $65 - 256$ cells | **$K = 16$** ($16 \times 16 = 256$ cells) | $32 \times 32 = 1,024$ cells | 128 KB |
| **Lớn (128 KB - 512 KB)** | $257 - 1,024$ cells | **$K = 32$** ($32 \times 32 = 1,024$ cells) | $64 \times 64 = 4,096$ cells | 512 KB |
| **Khối Tối Đa (512 KB - 2 MB)** | $1,025 - 4,096$ cells | **$K = 64$** ($64 \times 64 = 4,096$ cells) | $128 \times 128 = 16,384$ cells | 2,048 KB (2 MB) |

---

## 2. Khởi Tạo & Quản Lý Cụm Mạng (Cluster Lifecycle)

### 2.1. Khởi Tạo Mới Toàn Bộ Cụm (Clean Initialization)

Hệ thống hỗ trợ **2 chế độ khởi tạo trên Máy Chủ Chính**:

#### A. Cấu hình Chuẩn 2 Máy Chủ (`active_cols = 1`, `net_rows = 4`, Máy Chính 6 Store Nodes + Máy Phụ 2 Store Nodes)
Khi vận hành trên **1 Máy Chính + 1 Máy Phụ** với `active_cols = 1` và `4 row mạng (Row 0..3)`:
- **Trên Máy Chính (`MAIN_HOST_IP`, ví dụ `192.168.1.10`)**: Chạy `CometBFT + Publisher + Bootstrap-0 + 6 Store Nodes (:9300..9305)`:
  - `Row 0`: 2 Store Nodes (`:9300`, `:9304`)
  - `Row 1`: 2 Store Nodes (`:9301`, `:9305`)
  - `Row 2`: 1 Store Node (`:9302` — tạm mượn Backup từ `Row 0/1` khi Máy Phụ chưa bật)
  - `Row 3`: 1 Store Node (`:9303` — tạm mượn Backup từ `Row 0/1` khi Máy Phụ chưa bật)
- **Trên Máy Phụ (`WORKER_PUBLIC_IP`, ví dụ `192.168.1.50`)**: Chạy **2 Store Nodes (`:9310`, `:9311`) + 1 Light Node (`:9410`)** qua [`docker-compose.worker.yml`](file:///home/ubuntu/cda-network/docker-compose.worker.yml). Khi khởi chạy với `STORE_ROW=-1`, 2 Store Nodes trên Máy Phụ tự động phát hiện `Row 2` và `Row 3` là 2 ô ít node nhất (`Least-Loaded`) và gia nhập vào `Row 2` & `Row 3`, tạo thành ma trận đối xứng **$4 \text{ hàng} \times 2 \text{ nodes/ô} = 8 \text{ Store Nodes}$**!

```bash
# 1. Trên Máy Chính (192.168.1.10):
bash scripts/cleanup.sh
python3 scripts/init_cometbft_cluster.py --validators 4
python3 scripts/generate_compose.py -c 1 -s 4 --total-stores 6 -l 1 -k 64
MAIN_HOST_IP=192.168.1.10 docker compose -f docker-compose.json up -d --build

# 2. Trên Máy Phụ (192.168.1.50):
MAIN_HOST_IP=192.168.1.10 \
WORKER_PUBLIC_IP=192.168.1.50 \
CDA_NET_ROWS=4 \
docker compose -f docker-compose.worker.yml up -d --build
```

#### B. Chế độ Tinh Gọn `Core-Only` (Chỉ chạy `CometBFT + Publisher + Bootstrap` trên Máy Chủ Chính)
Giúp giảm ~75% tải CPU/RAM trên máy chủ chính bằng cách không khởi chạy sẵn 8 Store Nodes và Light Nodes tại chỗ. Các Store Nodes và Light Nodes sẽ tham gia động từ các máy chủ phụ (qua [`docker-compose.worker.yml`](file:///home/ubuntu/cda-network/docker-compose.worker.yml)) hoặc bật linh hoạt theo nhu cầu qua [`scripts/cda_node_ctl.sh`](file:///home/ubuntu/cda-network/scripts/cda_node_ctl.sh):

```bash
# Bước 1: Dọn dẹp các container và volume dữ liệu cũ
bash scripts/cleanup.sh

# Bước 2: Khởi tạo cấu hình genesis, validator keys và nâng RPC max_body_bytes = 10MB
python3 scripts/init_cometbft_cluster.py --validators 4

# Bước 3 (Cách 1): Chạy trực tiếp file docker-compose.core.yml tối giản
docker compose -f docker-compose.core.yml up -d --build

# Bước 3 (Cách 2): Hoặc sinh docker-compose.json ở chế độ --core-only (0 Store Node, 0 Light Node, có kèm Prometheus/Grafana)
python3 scripts/generate_compose.py --core-only -c 1 -k 64
docker compose -f docker-compose.json up -d --build
```

> [!TIP]
> Trong chế độ `Core-Only`, bạn chỉ cần cho **1 hoặc 2 Store Nodes** gia nhập mạng (từ máy khác hoặc bằng lệnh `./scripts/cda_node_ctl.sh join-store auto`) là thuật toán `ComputeColumnCustodyMap` sẽ tự động phân công 1–2 node đó gánh hộ các hàng mạng còn trống và hoàn tất `CDA BlockReady` bình thường mà không cần chạy đủ 8 Store Nodes.

#### B. Chế độ Đầy Đủ Trên 1 Máy (`All-in-One Local Cluster`)
Khi muốn chạy sẵn toàn bộ 8 Store Nodes + Light Nodes trên cùng một máy chủ:

```bash
bash scripts/cleanup.sh
python3 scripts/init_cometbft_cluster.py --validators 4

# Sinh cấu hình mặc định (8 net-cols x 8 net-rows x 1 node/cell, chạy cột 0):
python3 scripts/generate_compose.py -c 1 -k 64

# Hoặc sinh cấu hình đa node trên mỗi ô mạng (ví dụ: 8 net-cols x 4 net-rows x 2 nodes/cell):
python3 scripts/generate_compose.py --net-cols 8 --net-rows 4 --nodes-per-cell 2 --active-cols 1

docker compose -f docker-compose.json up -d --build
```

---

### 2.2. Kiểm Tra Trạng Thái Sức Khỏe & Bảng Định Tuyến Ma Trận Mạng

```bash
# Xem danh sách và trạng thái toàn bộ containers
docker compose -f docker-compose.json ps

# Kiểm tra bảng phân bổ Custody (row_custody_map) trên Bootstrap Node
curl -s http://localhost:9200/bootstrap/peers | jq .

# Kiểm tra chiều cao khối hiện tại của CometBFT
curl -s http://localhost:26657/status | grep -o '"latest_block_height":"[^"]*"'

# Kiểm tra chiều cao khối đã hoàn thành CDA BlockReady
curl -s http://localhost:8080/api/v1/status

# Kiểm tra nhanh số lượng node online qua Prometheus
curl -s 'http://localhost:9090/api/v1/query?query=sum(up)'
```

---

### 2.3. Xem Nhật Ký Vận Hành (Container Logs)

```bash
# Xem log Publisher (tiếp nhận block từ CometBFT và phân phối RLNC pieces)
docker logs -f publisher

# Xem log Bootstrap (tính toán bằng chứng KZG FK20)
docker logs -f bootstrap-0

# Xem log Store Node (kiểm chứng custody và lưu trữ BadgerDB)
docker logs -f store-0-1

# Xem log CometBFT validator
docker logs -f cometbft-0
```

---

### 2.4. Tham Gia Mạng Từ Máy Chủ Khác (Multi-Host Node Onboarding) & Quản Lý Vòng Đời Node

#### A. Tham gia mạng từ Máy Phụ qua Docker (`docker-compose.worker.yml`)
Khi cụm chính đang chạy ở **Máy Chính (`MAIN_HOST_IP`, ví dụ `192.168.1.10`)**, bạn có thể đóng gói image hoặc đưa [`docker-compose.worker.yml`](file:///home/ubuntu/cda-network/docker-compose.worker.yml) sang **Máy Phụ (`WORKER_PUBLIC_IP`, ví dụ `192.168.1.50`)** để khởi chạy thêm Store Node và Light Node:

```bash
# 1. Tự động gia nhập Ô Ít Tải Nhất (Least-Loaded Cell Discovery: STORE_ROW=-1)
MAIN_HOST_IP=192.168.1.10 \
WORKER_PUBLIC_IP=192.168.1.50 \
STORE_ROW=-1 \
docker compose -f docker-compose.worker.yml up -d --build

# 2. Hoặc chỉ định gia nhập vào một ô cụ thể (ví dụ Row 0 để kích hoạt Multi-Node Cell M >= 2)
MAIN_HOST_IP=192.168.1.10 \
WORKER_PUBLIC_IP=192.168.1.50 \
STORE_ROW=0 \
docker compose -f docker-compose.worker.yml up -d
```

#### B. Quản lý Store Node, Light Node, DAS & Điểm Tín Nhiệm bằng CLI [`scripts/cda_node_ctl.sh`](file:///home/ubuntu/cda-network/scripts/cda_node_ctl.sh)
```bash
# 1. Xem trạng thái Ma trận mạng 2D, danh sách Store Nodes & Light Nodes
./scripts/cda_node_ctl.sh status

# 2. Khởi chạy thêm Store Node (Auto-Join vào ô ít tải nhất hoặc chỉ định hàng 0..7)
./scripts/cda_node_ctl.sh join-store auto
./scripts/cda_node_ctl.sh join-store 0

# 3. Cho Store Node rời mạng chủ động (Graceful Leave - SIGTERM)
./scripts/cda_node_ctl.sh leave-store 9310

# 4. Khởi chạy / Dừng Light Node và lấy mẫu DAS
./scripts/cda_node_ctl.sh join-light
./scripts/cda_node_ctl.sh das 5 4
./scripts/cda_node_ctl.sh das-cell 5 0 0

# 5. Xem bảng xếp hạng Điểm Tín nhiệm & Giả lập kịch bản Tín nhiệm nhanh
./scripts/cda_node_ctl.sh rep-status
./scripts/cda_node_ctl.sh rep-sim 9301 missed_block 5
./scripts/cda_node_ctl.sh rep-sim 9301 block_completed 26
./scripts/cda_node_ctl.sh rep-reset all

# 6. Chạy Suite Kiểm thử Tự động Hệ thống Điểm Tín nhiệm & Chống tranh chấp CPU
python3 scripts/tests/resilience/test_reputation_system.py
```

---

### 2.5. Hướng Dẫn Người Dùng Cuối Sử Dụng Web Dashboard (`:8889`)

Khởi chạy máy chủ giao diện Web:
```bash
python3 -u scripts/serve_dashboard.py 8889
```

Hệ thống tách biệt 2 giao diện:
1. **Giao diện Người dùng & Theo dõi Ma trận (`http://localhost:8889/`)**:
   - **Tạo/Quản lý Tài khoản Ed25519 (`Account & Keys`)**: Sinh khóa Ed25519 chuẩn RFC 8032 ngay trên trình duyệt với địa chỉ `cda1_<16_hex>`, hỗ trợ Export JSON và Import Private Seed Hex.
   - **Ký số & Đẩy Dữ liệu (`Upload Blob`)**: Mọi giao dịch gửi vào namespace `cda1_...` đều được tự động ký số `@<pubKeyHex>.<sigHex>` và được CometBFT `CheckTx` xác thực quyền sở hữu. Hỗ trợ gom Single Native Blob ($\le 256\text{ KB}$) vào **1 block duy nhất** và tự động cắt chunk song song (`Promise.all`) cho file lớn Multi-Part.
   - **Tra cứu & Tải File (`Namespace & Blobs`)**: Xem danh mục blob theo namespace và tải xuống file nhị phân gốc.
   - **Khám phá Khối (`Block Explorer`) & Giám sát Ma trận (`DA Network Matrix - Monitor`)**: Quan sát `Consensus Time`, `DA Time`, bảng mục lục khối (TOC), sơ đồ phân bổ ô ODS và trạng thái điểm tín nhiệm/chia tải của từng ô mạng (chế độ chỉ đọc).
2. **Giao diện Quản trị Mạng CDA (`http://localhost:8889/network-admin`)**:
   - Dành riêng cho người vận hành mạng để thêm/bớt Store Node, Light Node và chạy thử nghiệm DAS.

---

## 3. Hướng Dẫn Submit Giao Dịch (Transaction Submission)

Script submitter chính thức của hệ thống là [`scripts/bots/tx_submitter_bot.py`](file:///home/ubuntu/cda-network/scripts/bots/tx_submitter_bot.py).

> [!IMPORTANT]
> **Quy Tắc Định Dạng ABCI `kvstore` của CometBFT**:
> CometBFT sử dụng ứng dụng `kvstore` mặc định. Để giao dịch vượt qua kiểm tra `CheckTx`, mỗi giao dịch **bắt buộc phải có đúng một dấu `=`** (định dạng `key=value`). Nếu gửi chuỗi không có dấu `=`, `CheckTx` sẽ từ chối với mã lỗi `code 2`. Do đó, luôn sử dụng định dạng `--tx-format kv` (mặc định của bot).

---

### 3.1. Kịch Bản 1: Nộp Nhiều Giao Dịch Nhỏ Theo Đợt (Batch Mode)

Thích hợp để giả lập luồng giao dịch liên tục của ứng dụng Rollup L2:

#### A. Gửi 1 đợt 128 giao dịch (mỗi tx 512B, tổng payload ~65 KB, ODS $K=16$ hoặc $K=32$):
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 128 --payload-size 512 --batches 1
```

#### B. Bơm tải liên tục không giới hạn (Continuous Streaming, nghỉ 2s giữa các đợt):
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 64 --payload-size 512 --interval 2.0
```

#### C. Bơm tải đồng bộ có điều kiện (Chờ CDA BlockReady trước khi gửi batch tiếp theo):
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 128 --payload-size 512 --wait-cda --batches 5
```
*Tùy chọn `--wait-cda` bảo đảm không gây tràn hàng đợi mempool và đo lường chính xác độ trễ xử lý từng khối.*

---

### 3.2. Kịch Bản 2: Nộp 1 Giao Dịch Đơn Lẻ Dung Lượng Lớn (Large Single Tx)

Thích hợp để kiểm thử ma trận kích thước lớn ($K=32$, $K=64$) mà không phải gửi hàng trăm HTTP requests:

#### A. Gửi 1 tx dung lượng 128 KB (ODS $K=32$):
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 1 --payload-size 131072 --tx-format kv --batches 1
```

#### B. Gửi 1 tx dung lượng 512 KB (ODS $K=64$ - Khối tiêu chuẩn 2 MB EDS):
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 1 --payload-size 524288 --tx-format kv --batches 1
```

#### C. Gửi 1 tx dung lượng 1 MB (ODS $K=64$):
```bash
python3 scripts/bots/tx_submitter_bot.py --tx-count 1 --payload-size 1048576 --tx-format kv --batches 1
```

---

### 3.3. Kịch Bản 3: Nộp Giao Dịch Thủ Công Bằng `curl`

Nếu không sử dụng script bot, có thể nộp giao dịch trực tiếp qua JSON-RPC của CometBFT:

#### A. Nộp giao dịch dạng chuỗi text đơn giản:
```bash
curl -s -X POST 'http://localhost:26657/broadcast_tx_commit?tx="my_rollup_batch_001=payload_data_here"'
```

#### B. Nộp giao dịch dạng Hex Binary lớn:
```bash
# Tạo payload hex có chứa dấu '=' (0x3d)
KEY_HEX=$(echo -n "batch_key" | xxd -p)
EQUAL_HEX="3d"
VAL_HEX=$(head -c 1024 /dev/urandom | xxd -p -c 1024)
RAW_TX="0x${KEY_HEX}${EQUAL_HEX}${VAL_HEX}"

# Gửi broadcast_tx_commit
curl -s -X POST "http://localhost:26657/broadcast_tx_commit?tx=${RAW_TX}"
```

---

### 3.4. Bảng Tham Chiếu Toàn Bộ Cờ Lệnh của `tx_submitter_bot.py`

| Cờ Tham Số | Giá Trị Mặc Định | Ý Nghĩa & Mục Đích |
| :--- | :--- | :--- |
| `--stream` | `False` | **Chế độ bơm liên tục (Continuous Streaming)**: Bơm giao dịch liên tục vào Mempool CometBFT mà không đợi đóng block hay đợi CDA; luồng monitor nền tự động quan sát block và trạng thái CDA. |
| `--tx-count` | `64` | Số lượng giao dịch được broadcast đồng thời trong một batch. |
| `--payload-size` | `512` | Kích thước dữ liệu payload (bytes) của mỗi giao dịch. |
| `--cell-size` | `512` | Kích thước ô (bytes) dùng để tính toán số cells và dự đoán chiều $K$. |
| `--tx-format` | `kv` | Định dạng giao dịch: `kv` (key=value, an toàn cho ABCI kvstore) hoặc `raw_hex`. |
| `--interval` | `2.0` | Thời gian nghỉ (giây) giữa các đợt gửi giao dịch (đặt `0.1`–`0.5s` khi stream, hoặc `0.0` để bơm tối đa công suất). |
| `--batches` | `0` | Tổng số đợt nộp (`0` = chạy liên tục vô hạn đến khi bấm Ctrl+C). |
| `--workers` | `32` | Số lượng luồng worker chạy song song khi nộp HTTP broadcast. |
| `--pipeline` | `False` | Chế độ gối đầu: nộp block liên tiếp qua từng block CometBFT; kiểm tra CDA BlockReady chạy nền. |
| `--max-in-flight` | `2` | Giới hạn số block CDA đang xử lý song song trong chế độ pipeline (đặt `0` = không giới hạn). |
| `--wait-cda` / `--no-wait-cda` | `True` | Tự động đồng bộ: kiểm tra block đạt trạng thái `CDA BlockReady` (dùng `--no-wait-cda` để bỏ qua). |
| `--cometbft` | `http://localhost:26657` | Địa chỉ RPC endpoint của CometBFT node. |
| `--publisher` | `http://localhost:8080` | Địa chỉ REST API của CDA Publisher node. |

---

### 3.5. Bot Phân Rã, Lưu Trữ & Khôi Phục File Lớn (`file_pipeline_bot.py`)

Bot [`scripts/bots/file_pipeline_bot.py`](file:///home/ubuntu/cda-network/scripts/bots/file_pipeline_bot.py) được thiết kế chuyên biệt để giải quyết bài toán: **Lưu trữ một file bất kỳ (dung lượng từ vài MB đến hàng trăm MB) lên CDA Network bằng cách chia nhỏ thành các chunk $\sim 500\text{ KB}$, sinh Manifest JSON ghi nhớ metadata và hỗ trợ khôi phục (retrieve) nguyên vẹn file với kiểm tra SHA-256.**

#### A. Cú pháp nộp (Upload/Chunking):
```bash
# 1. Nộp một file có sẵn trên đĩa:
python3 scripts/bots/file_pipeline_bot.py \
  --file /path/to/my_data.bin \
  --chunk-size 500KB \
  --manifest my_data.manifest.json

# 2. Tự động sinh file ngẫu nhiên (ví dụ 5 MB) để benchmark:
python3 scripts/bots/file_pipeline_bot.py \
  --size 5MB \
  --chunk-size 500KB \
  --manifest benchmark_5MB.manifest.json
```

#### B. Cú pháp khôi phục (Retrieve & Reassemble):
```bash
python3 scripts/bots/file_pipeline_bot.py \
  --retrieve \
  --manifest benchmark_5MB.manifest.json \
  --output restored_data.bin
```

| Cờ Tham Số | Giá Trị Mặc Định | Ý Nghĩa & Mục Đích |
| :--- | :--- | :--- |
| `--file` | `""` | Đường dẫn file thực tế trên đĩa cần nộp. |
| `--size` | `2MB` | Kích thước dữ liệu mẫu cần sinh nếu không dùng `--file` (hỗ trợ `500KB`, `2MB`, `10M`...). |
| `--chunk-size` | `500KB` | Kích thước mỗi phần (chunk) tương ứng 1 block CometBFT ($K=32$ CDA). |
| `--slice-size` | `500` | Kích thước mỗi slice giao dịch trong chunk (bytes). |
| `--manifest` | `<filename>.manifest.json` | Đường dẫn file Manifest JSON lưu trữ metadata tra cứu. |
| `--retrieve` | `False` | Bật chế độ khôi phục file từ Manifest JSON. |
| `--output` | `""` | Đường dẫn file đích khi khôi phục (`--retrieve`). |
| `--workers` | `32` | Số worker broadcast hoặc tải về song song. |
| `--wait-cda` / `--no-wait-cda` | `True` | Theo dõi trạng thái CDA BlockReady chạy nền. |

---

## 4. Hướng Dẫn Retrieve Data & Truy Vấn Giao Dịch (Block Data & Transaction Retrieval)

Là một lớp Khả Dụng Dữ Liệu (Data Availability Layer), **CDA Network** cung cấp hai luồng truy xuất dữ liệu độc lập:
1. **Retrieve Data (Khôi phục toàn bộ khối dữ liệu ODS)**: Dành cho các Rollup Full Nodes, Sequencers, hoặc Observers cần đồng bộ (sync) lại toàn bộ blob dữ liệu của block từ các nút lưu ký phân tán CDA Store Nodes khi xảy ra tranh chấp (fraud proof) hoặc khởi động lại node.
2. **Retrieve Transaction (Truy vấn giao dịch đơn lẻ)**: Dành cho người dùng, ví hoặc Explorer cần tra cứu nhanh payload và bằng chứng Merkle Inclusion Proof của một giao dịch cụ thể theo `tx_hash` hoặc `key`.

---

### 4.1. Hướng Dẫn Retrieve Data Toàn Khối (Block Data & Blob Retrieval - `GET /block/:height/data`)

Endpoint này kích hoạt quy trình kéo mảnh phân tán và tái tạo toàn bộ ma trận dữ liệu gốc $K \times K$ cells của khối từ các nút lưu ký Store Nodes:

```mermaid
flowchart TD
    Client["Rollup Node / Observer / Client"] -->|"GET /block/:height/data"| Pub["CDA Publisher (:8080)"]
    Pub --> HeaderCheck["1. Tra cứu Block Header & Column Commitments"]
    Pub --> LRUCheck{"Check LRU Cache?"}
    LRUCheck -->|"Cache Hit"| ReturnJSON["Trả về mảng Hex Cells"]
    LRUCheck -->|"Cache Miss"| P2P["2. P2P ProtoStoreBatchFetch tới Store Nodes"]
    P2P --> RLNC["3. Giải mã RLNC Gaussian Elimination từng cell"]
    P2P --> RS2D["4. Leopard Reed-Solomon 2D nội suy các cột"]
    RS2D --> CacheSet["Lưu ma trận ODS vào LRU Cache (TTL 10m)"]
    CacheSet --> ReturnJSON
    ReturnJSON --> Reassemble["5. Client decode hex & gộp cells thành Blob dữ liệu gốc"]
```

#### A. Cú pháp gọi API:
Endpoint hỗ trợ cả số chiều cao khối (`:height`) hoặc mã định danh khối (`:block_id`):
```bash
# 1. Truy xuất dữ liệu khối theo số chiều cao:
curl -s "http://localhost:8080/block/46/data"

# 2. Truy xuất dữ liệu khối theo Block ID:
curl -s "http://localhost:8080/block/block-46/data"
```

#### B. Cấu trúc JSON Response chi tiết:
```json
{
  "status": "success",
  "block_id": "block-46",
  "height": 46,
  "k": 8,
  "total_cells": 64,
  "active_cols": 1,
  "num_cols": 8,
  "commits_root": "16714396089f210c230eb5831c4bdcbcf0eb16d7a8259e61d0bc1a1ee7812f52",
  "cells": [
    "74785f32306566653464665f313d37353035393030653164383139653133...",
    "356436306465666461383634653434653139303162303338323036316162...",
    "..."
  ]
}
```

| Trường Dữ Liệu | Kiểu | Ý Nghĩa Kỹ Thuật |
| :--- | :--- | :--- |
| `status` | string | Trạng thái truy xuất (`success` hoặc `error`). |
| `block_id` | string | Định danh khối (`block-<height>`). |
| `height` | int64 | Chiều cao khối đã truy xuất. |
| `k` | int | Kích thước cạnh ma trận ODS ($K \times K$). |
| `total_cells` | int | Tổng số ô trong ma trận ODS ($K^2$ cells, mỗi cell 512 bytes). |
| `active_cols` | int | Số cột mạng thực sự hoạt động trong topology hiện tại. |
| `num_cols` | int | Tổng số cột mạng logic (mặc định 8). |
| `commits_root` | hex string | Gốc Merkle cam kết đa thức KZG của khối để kiểm chứng tính xác thực. |
| `cells` | array[string] | Mảng chứa mã hex của toàn bộ các ô dữ liệu đã khôi phục. |

#### C. Script Python mẫu: Tải, Tái Tạo và Lưu Trữ Blob Dữ Liệu Gốc:
Client có thể sử dụng đoạn mã Python sau để tự động tải về, decode hex các cells và gộp lại thành dữ liệu nhị phân nguyên bản:

```python
#!/usr/bin/env python3
import json
import urllib.request

def retrieve_block_data(publisher_url, height, output_file=None):
    url = f"{publisher_url.rstrip('/')}/block/{height}/data"
    print(f"[*] Đang tải dữ liệu khối {height} từ {url}...")
    
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    
    if data.get("status") != "success":
        raise RuntimeError(f"Retrieve thất bại: {data.get('message')}")
    
    k = data["k"]
    cells_hex = data["cells"]
    commits_root = data.get("commits_root", "")
    print(f"✅ Đã tải thành công ma trận {k}x{k} ({len(cells_hex)} cells). CommitsRoot: {commits_root[:20]}...")
    
    # Decode các cell từ hex và ghép thành blob dữ liệu liên tục
    raw_blob = bytearray()
    for i, cell_hex in enumerate(cells_hex):
        cell_bytes = bytes.fromhex(cell_hex)
        # Bỏ qua các padding cell chỉ toàn số 0
        if any(b != 0 for b in cell_bytes):
            raw_blob.extend(cell_bytes)
    
    print(f"📦 Tổng dung lượng payload hữu ích thu được: {len(raw_blob)} bytes")
    
    if output_file:
        with open(output_file, "wb") as f:
            f.write(raw_blob)
        print(f"💾 Đã lưu dữ liệu ra file: {output_file}")
    
    return raw_blob

# Ví dụ chạy với khối 46:
if __name__ == "__main__":
    blob = retrieve_block_data("http://localhost:8080", 46, "block_46_data.bin")
```

#### D. Lệnh CLI một dòng trích xuất nhanh dữ liệu khối:
```bash
python3 -c "
import urllib.request, json
res = json.loads(urllib.request.urlopen('http://localhost:8080/block/46/data').read())
non_empty = [bytes.fromhex(c) for c in res['cells'] if any(x != '0' for x in c)]
print(f'Khối {res[\"height\"]}: {len(non_empty)} cells đã khôi phục, {sum(len(c) for c in non_empty)} bytes payload.')
"
```

---

### 4.2. Hướng Dẫn Truy Vấn Từng Giao Dịch & Trích Xuất Payload (Transaction Retrieval)

Hệ thống hỗ trợ tra cứu trực tiếp từng giao dịch theo mô hình lai hai tầng (**Two-Tier Hybrid Retrieval**):

```mermaid
flowchart TD
    Client["Client / User / Explorer"] -->|"GET /tx?hash=... hoặc ?key=..."| Pub["CDA Publisher (:8080)"]
    Pub -->|"Kiểm tra cờ force_cda"| Decision{"force_cda == true?"}
    
    Decision -->|"No (Hot Path)"| Comet["CometBFT RPC (:26657)"]
    Comet -->|"200 OK + Proof"| HotResp["Return source: 'cometbft' (Latency ~2ms)"]
    Comet -->|"404 Not Found (Pruned)"| ColdPath["Fallback to CDA Cold Path"]
    
    Decision -->|"Yes (Cold Path)"| ColdPath
    ColdPath --> VerifyCDA["1. Xác thực Header & CommitsRoot"]
    VerifyCDA --> LRUCheck{"Check LRU Cache"}
    LRUCheck -->|"Hit"| Extract["Extract Tx Payload from ODS"]
    LRUCheck -->|"Miss"| P2P["2. P2P ProtoStoreBatchFetch từ Store Nodes"]
    P2P --> RLNC["3. Giải mã RLNC Gaussian Elimination & RS-2D"]
    RLNC --> Cache["Cache ODS Matrix in LRU"]
    Cache --> Extract
    
    Extract --> ValidCheck{"Valid Reconstructed Payload?"}
    ValidCheck -->|"Yes (Active Col Reconstructed)"| CDAResp["Return source: 'cda' (Latency ~20ms)"]
    ValidCheck -->|"No (active-cols limitation)"| FallbackResp["Two-Phase Fallback with CDA Metadata (source: 'cda_fallback')"]
```

#### A. Truy Vấn Hot Path theo Tx Hash (`source: "cometbft"` - Độ trễ ~2ms):
```bash
curl -s "http://localhost:8080/tx?hash=0x<TX_HASH_HEX>"
```
**JSON Response mẫu (`source: "cometbft"`):**
```json
{
  "status": "success",
  "source": "cometbft",
  "height": 75,
  "tx_hash": "734AD9B103F25E5756FBC802BFAC22541E1D613D5829B3BA34FB8525425F72C7",
  "payload": "tx_key_123=data_value_here",
  "verified": true,
  "proof": { ... }
}
```

#### B. Truy Vấn theo Key:
```bash
curl -s "http://localhost:8080/tx?key=<TX_KEY_NAME>"
```

#### C. Truy Vấn Ép Buộc Qua Lớp CDA (Cold Path):
Thêm cờ `&force_cda=true` vào câu truy vấn:
```bash
curl -s "http://localhost:8080/tx?hash=0x<TX_HASH_HEX>&force_cda=true"
```
**JSON Response mẫu khi khôi phục trực tiếp từ CDA (`source: "cda"`):**
```json
{
  "status": "success",
  "source": "cda",
  "height": 75,
  "tx_hash": "734AD9B103F25E5756FBC802BFAC22541E1D613D5829B3BA34FB8525425F72C7",
  "payload": "tx_key_123=data_value_here",
  "verified": true,
  "cda_verified": true,
  "commits_root": "0a5e37581febf327a940aa66...",
  "active_cols": 1,
  "num_cols": 8
}
```

**JSON Response mẫu khi gặp giới hạn `active-cols` (`source: "cda_fallback"`):**
```json
{
  "status": "success",
  "source": "cda_fallback",
  "height": 75,
  "tx_hash": "734AD9B103F25E5756FBC802BFAC22541E1D613D5829B3BA34FB8525425F72C7",
  "payload": "tx_key_123=data_value_here",
  "verified": true,
  "cda_verified": true,
  "commits_root": "0a5e37581febf327a940aa66...",
  "fallback_reason": "active_cols_limitation",
  "active_cols": 1,
  "num_cols": 8
}
```

---

### 4.3. Khởi Chạy Publisher Node Phục Vụ Retrieve Data & Tx

Publisher Node đảm nhiệm vai trò Gateway phục vụ các API truy xuất khối và giao dịch.

#### A. Biên dịch binary Publisher:
```bash
go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
```

#### B. Các cờ tham số cấu hình:
| Cờ Tham Số | Mặc Định | Mô Tả |
| :--- | :--- | :--- |
| `-port` | `8080` | Cổng HTTP REST API của Publisher. |
| `-cometbft` | `http://localhost:26657` | Địa chỉ RPC của CometBFT node. |
| `-bootstrap` | `http://localhost:9200` | Địa chỉ HTTP của Bootstrap node (P2P Discovery). |
| `-k` | `16` | Kích thước ODS mặc định. |
| `-k-piece` | `4` | Tham số RLNC: số fragments cấu thành 1 cell (đồng thời là số mảnh độc lập tối thiểu cần thu thập để giải mã 1 cell). |
| `-num-cols` | `8` | Tổng số cột mạng logic. |
| `-active-cols`| `8` (hoặc số bootstrap) | Số cột thực sự hoạt động trong topology (hỗ trợ testnet ít node). |

#### C. Lệnh khởi chạy:
```bash
# Chạy với đầy đủ 8 cột mạng (Full Cluster):
./bin/publisher -port 8080 -bootstrap "http://localhost:9200" -k 16 -k-piece 4 -num-cols 8 -active-cols 8 -cometbft "http://localhost:26657"

# Chạy trong môi trường tài nguyên hạn chế (Hardware-constrained testnet: 1 active col):
./bin/publisher -port 8080 -bootstrap "http://localhost:9200" -k 16 -k-piece 4 -num-cols 8 -active-cols 1 -cometbft "http://localhost:26657"
```

---

### 4.4. Giám Sát, Log & Benchmark Đo Kiểm Tự Động

#### A. Theo dõi nhật ký phục hồi P2P:
Khi Publisher thực hiện khôi phục qua Cold Path hoặc Retrieve Block Data, nhật ký tiến trình hiển thị rõ quá trình kéo mảnh và giải mã:
```text
2026/10/15 06:47:31 [Reconstructor] Cache miss. Initiating distributed P2P block recovery for block-46 (Height: 46, K: 8, N: 16)...
2026/10/15 06:47:31 [P2P] Discovery: found bootstrap node for column 0 -> http://localhost:9200
2026/10/15 06:47:31 [Reconstructor] Successfully retrieved 2 / 16 columns from Store Nodes for block-46
2026/10/15 06:47:31 [Reconstructor] Successfully assembled ODS (9/64 cells populated) for block-46
2026/10/15 06:47:33 [Reconstructor] Cache hit for BlockID: block-46 (K=8)
```

#### B. Chạy kịch bản kiểm thử E2E tự động toàn trình:
Hệ thống cung cấp script [`scripts/tests/test_reverse_retrieval.py`](file:///home/ubuntu/cda-network/scripts/tests/test_reverse_retrieval.py) tự động thực thi 7 bước kiểm tra toàn diện:
```bash
python3 scripts/tests/test_reverse_retrieval.py --publisher http://localhost:8080 --cometbft http://localhost:26657 --bootstrap http://localhost:9200
```

#### C. Bảng so sánh độ trễ thực tế:
| Tuyến Truy Vấn | Cơ Chế Phục Hồi | Độ Trễ Thực Tế | Độ Chính Xác Dữ Liệu |
| :--- | :--- | :--- | :--- |
| **Hot Path (CometBFT)** | CometBFT RPC State Cache | **~1.5 – 2.5 ms** | 100% Bit-for-bit Exact Match |
| **Key-based Query** | Index Tra Cứu + Hot Path | **~1.2 – 2.0 ms** | 100% Bit-for-bit Exact Match |
| **Cold Path (CDA P2P)** | Distributed P2P Fetch + RLNC Decode | **~15.0 – 25.0 ms** | 100% Exact Reconstruction / Two-Phase Verified |
| **Block ODS Data** | Reconstruct & Render All Cells | **~2.0 – 5.0 ms** (Cache) | Toàn vẹn 100% cells trên active cols |

---

## 5. Giám Sát Hệ Thống Thời Gian Thực (Monitoring Stack)

Hệ thống cung cấp 3 kênh giám sát độc lập, phục vụ các mục đích theo dõi khác nhau:

---

### 5.1. Dashboard Grafana ([http://localhost:3000](http://localhost:3000))

- **Tài khoản**: `admin` / **Mật khẩu**: `admin`
- **Dashboard**: **CDA Network Performance Dashboard**

Dashboard đã được thiết kế lại tối ưu, tinh gọn và phân thành các khu vực rõ ràng:

#### 🌐 Khu Vực Tổng Quan Hệ Thống (Top Overview Row)
Khu vực trên cùng hiển thị 4 chỉ số cốt lõi tinh gọn nhất của toàn bộ hệ thống:

```
+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+
|      CometBFT Consensus Height    |        CDA BlockReady Height      |        ODS Matrix Dimension       |        Pipeline In-Flight         |
|                [ 218 ]            |                [ 218 ]            |       [ 64 x 64 (4,096 cells) ]   |                [ 0 ]              |
|        (Màu xanh lam nổi bật)     |        (Màu xanh lục hoàn tất)    |          (Màu cam rực rỡ)         |         (Số nguyên đơn thuần)     |
+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+
```

1. **CometBFT Consensus Height**: Chiều cao khối đồng thuận hiện tại của CometBFT.
2. **CDA BlockReady Height**: Chiều cao khối đã hoàn tất 100% bảo chứng lưu trữ trên 8 Store Nodes.
3. **ODS Matrix Dimension**: Hiển thị trực tiếp kích thước ma trận ODS ($K \times K$ cells, ví dụ: `64 × 64 (4,096 cells)`).
4. **Pipeline In-Flight**: Số lượng khối đang được xử lý song song trong hàng đợi Publisher (biểu thị số đơn thuần, xanh khi rảnh rỗi, vàng/đỏ khi tải cao).

#### ⚡ Tầng 1: CDA Pipeline Performance & Latency
- **Block Height Progression (Consensus vs BlockReady)**: Biểu đồ đường so sánh real-time tiến độ khối của CometBFT và CDA để nhận biết pipeline lag.
- **CDA Pipeline Latency (Publish $\to$ BlockReady Duration)**: Biểu đồ đo thời gian hoàn thành một block (Latency trung bình và P95).
- **Node Encoding & Proof Durations**: Đo đạc chi tiết thời gian RS encoding (Publisher), sinh bằng chứng KZG (Bootstrap) và giải mã RLNC (Store nodes).
- **Publisher Throughput Over Time**: Lưu lượng phát hành dữ liệu tính theo Moving Average 30s và 1m.

#### 💾 Tầng 2: Store Nodes, Custody Sharding & P2P Dissemination
- **Total Stored Pieces per Store Node**: Biểu đồ số lượng mảnh lưu trữ trên từng node trong 8 Store nodes.
- **Store Node Pieces Breakdown Table**: Bảng đối soát tức thời kiểm chứng tính cân bằng tải tuyệt đối giữa các Store nodes.
- **Store Node Database Size**: Dung lượng lưu trữ thực tế trên BadgerDB của từng node.
- **Store Node Network Rates**: Tần suất truy vấn P2P Request và tốc độ lan truyền mảnh qua GossipSub.

#### 🛡️ Tầng 3: Data Availability Sampling (DAS) & Security
- **DAS Success Rate**: Tỷ lệ lấy mẫu thành công của các Light Clients (ngưỡng an toàn 100%).
- **Byzantine Forged Pieces Blocked**: Cảnh báo số mảnh dữ liệu giả mạo bị phát hiện và ngăn chặn bởi chữ ký KZG.
- **DAS Sampling Latency**: Độ trễ truy vấn lấy mẫu DAS của Light nodes (P95 vs Average).

#### 🖥️ Tầng 4: Cluster Infrastructure & Mempool
- **System CPU Usage**: Số lượng CPU cores thực tế sử dụng của các tiến trình trong cụm.
- **Mempool Pending Txns**: Số lượng giao dịch đang chờ trong mempool CometBFT.
- **Healthy / Online Nodes**: Tổng số nodes và dịch vụ đang online trong toàn mạng.

---

### 5.2. CLI Network Monitor

Script [`scripts/monitor/cda_network_monitor.py`](file:///home/ubuntu/cda-network/scripts/monitor/cda_network_monitor.py) cho phép theo dõi dòng sự kiện trực tiếp từ terminal:

```bash
# Chế độ 1: Dòng chảy sự kiện 3 giai đoạn (Stream Mode - Khuyên Dùng)
python3 scripts/monitor/cda_network_monitor.py --mode stream

# Chế độ 2: Bảng điều khiển tổng hợp Terminal (Dashboard Mode)
python3 scripts/monitor/cda_network_monitor.py --mode dashboard --interval 1.0
```

---

### 5.3. Các Câu Lệnh Prometheus PromQL Hữu Dụng ([http://localhost:9090](http://localhost:9090))

| Mục Đích Theo Dõi | Câu Truy Vấn PromQL |
| :--- | :--- |
| **Chiều cao CometBFT** | `max(cometbft_consensus_height)` |
| **Chiều cao CDA hoàn tất** | `cda_publisher_latest_completed_height` |
| **Độ lệch khối (Pipeline Lag)** | `max(cometbft_consensus_height) - cda_publisher_latest_completed_height` |
| **Kích thước ODS K** | `cda_publisher_latest_k` |
| **Số block đang xử lý (In-Flight)** | `cda_publisher_in_flight_blocks` |
| **Thời gian xử lý block trung bình** | `rate(cda_publisher_block_duration_seconds_sum[1m]) / rate(cda_publisher_block_duration_seconds_count[1m])` |
| **Thời gian giải mã RLNC trên Store** | `rate(cda_store_rlnc_decode_duration_seconds_sum[1m]) / rate(cda_store_rlnc_decode_duration_seconds_count[1m])` |
| **Cân bằng tải Store Nodes** | `cda_store_linear_independent_pieces_count` |

---

## 6. Xử Lý Sự Cố Thường Gặp (Troubleshooting)

### Sự Cố 1: Gửi Giao Dịch Bị Lỗi `HTTP Error 400: Bad Request`
- **Hiện tượng**: Khi gửi transaction payload dung lượng lớn (> 300 KB), submitter bot nhận lỗi `HTTP Error 400: Bad Request`.
- **Nguyên nhân**: Cấu hình mặc định `rpc.max_body_bytes` của CometBFT là 1 MB (`1048576` bytes). Khi gửi payload hex Base64, kích thước body vượt quá 1 MB nên CometBFT RPC reject ngay lập tức.
- **Cách khắc phục**:
  1. Kiểm tra file [`scripts/init_cometbft_cluster.py`](file:///home/ubuntu/cda-network/scripts/init_cometbft_cluster.py#L115), đảm bảo tham số `rpc.max_body_bytes = 10000000` (10 MB).
  2. Nếu cụm đã chạy từ cấu hình cũ, chạy lại lệnh:
     ```bash
     python3 scripts/init_cometbft_cluster.py --validators 4
     docker compose -f docker-compose.json restart cometbft-0 cometbft-1 cometbft-2 cometbft-3
     ```

---

### Sự Cố 2: Giao Dịch Bị Từ Chối Với Lỗi `CheckTx rejected tx with code 2`
- **Hiện tượng**: Bot báo lỗi `Broadcast failed: CheckTx rejected tx with code 2`.
- **Nguyên nhân**: Định dạng giao dịch gửi vào không chứa dấu `=` (vi phạm yêu cầu của ứng dụng `kvstore` của CometBFT).
- **Cách khắc phục**:
  Chỉ định rõ cờ `--tx-format kv` khi chạy bot submitter:
  ```bash
  python3 scripts/bots/tx_submitter_bot.py --tx-format kv ...
  ```

---

### Sự Cố 3: Mempool Bị Đầy (`mempool is full` / `Tx already exists in cache`)
- **Hiện tượng**: Bơm giao dịch tốc độ quá cao khiến mempool không kịp giải phóng.
- **Cách khắc phục**:
  - Tăng khoảng thời gian nghỉ `--interval` (ví dụ từ `1.0` lên `2.5`).
  - Thêm cờ `--wait-cda` để buộc bot đợi block trước hoàn tất BlockReady mới bơm block tiếp theo.

---

### Sự Cố 4: BlockReady Bị Trễ Hoặc Treo (Latency Vượt Quá 30 Giây)
- **Hiện tượng**: CometBFT vẫn tăng height nhưng `cda_publisher_latest_completed_height` dừng lại.
- **Nguyên nhân & Cách kiểm tra**:
  1. Kiểm tra log Publisher xem block nào đang nghẽn:
     ```bash
     docker logs --tail 50 publisher
     ```
  2. Kiểm tra log Bootstrap xem FK20 KZG proofs có bị crash không:
     ```bash
     docker logs --tail 50 bootstrap-0
     ```
  3. Kiểm tra xem Store Nodes có đủ 8 nodes online không:
     ```bash
     curl -s 'http://localhost:9090/api/v1/query?query=sum(up{job="store"})'
     ```

---

## 7. Tiêu Chuẩn Hiệu Năng & Kết Quả Đạt Được (Performance Baseline)

Dưới đây là các chỉ số chuẩn đo đạc thực tế trên hệ thống sau khi đã áp dụng các tối ưu hóa:

| Chỉ Số Đánh Giá | Kết Quả Đạt Được | Ghi Chú Kỹ Thuật |
| :--- | :--- | :--- |
| **Pipeline Latency ($K=64$, 512 KB payload)** | **~6.5s – 7.2s** | Đã giảm mạnh từ 43.5s nhờ tối ưu Jacobian/Mont và loại bỏ seed dư thừa. |
| **Pipeline Latency ($K=16$, 64 KB payload)** | **~1.8s – 2.2s** | Khối vừa và nhỏ đạt BlockReady gần như tức thì. |
| **Hot Path Tx Retrieval Latency** | **~1.5 – 2.5 ms** | Phản hồi $O(1)$ qua CometBFT RPC kèm Merkle Proof. |
| **Cold Path CDA Tx Retrieval Latency** | **~15.0 – 25.0 ms** | Kéo mảnh phân tán P2P `ProtoStoreBatchFetch` & giải mã RLNC. |
| **Tốc Độ Giải Mã RLNC (Store Node)** | **~9.2 $\mu\text{s}$ / cell** | Giải mã ma trận Cauchy cực nhanh trong bộ nhớ RAM. |
| **Phân Bổ Tải Custody (8 Store Nodes)** | **Cân bằng tải 100% tuyệt đối** | Mỗi store node nắm giữ số lượng mảnh custody và recoded bằng nhau chằn chặn. |
| **Độ Tin Cậy BlockReady (SLA)** | **100% blocks < 10 giây** | 119/119 blocks đạt BlockReady dưới 10 giây trong suốt quá trình stress test. |
| **DAS Sampling Success Rate** | **100.0%** | Light nodes lấy mẫu ngẫu nhiên thành công 100% không có block nào bị thiếu mảnh. |


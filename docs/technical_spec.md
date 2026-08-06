# CDA Network — Mô Tả Kỹ Thuật Chi Tiết

> **Phiên bản:** 1.0  
> **Ngày:** 2026-07-30  
> **Trạng thái:** Prototype / Research Implementation

---

## Tổng Quan Kiến Trúc

CDA (Column Data Availability) Network là hệ thống Data Availability Layer theo mô hình **2D Reed-Solomon + KZG polynomial commitment**. Dữ liệu gốc được mở rộng thành Extended Data Square (EDS), phân phối theo cột tới các Store Node, và Light Node có thể xác minh tính sẵn có của dữ liệu thông qua DAS (Data Availability Sampling) mà không cần tải toàn bộ dữ liệu.

### Sơ Đồ Tổng Quan

```
           Publisher Node
               │
    ┌──────────┼──────────┐
    │          │          │
Bootstrap-0  Bootstrap-1  Bootstrap-N   ← Mỗi bootstrap quản lý 1 "network column"
    │              │
Store-0-1  Store-1-1   ← Store nodes đăng ký với bootstrap của cột mình
Store-0-2  Store-1-2
    │              │
         Light Nodes        ← Light nodes query bootstrap → store để DAS sampling
```

### Tham Số Thiết Kế (Tách biệt K và k-piece)

| Tham số | Giá trị mặc định | Ý nghĩa |
|---------|-----------------|---------|
| `K` | 16 (hoặc 4 ở test nhỏ) | Số chunks ODS mỗi chiều ($K \times K$ ô dữ liệu gốc) |
| `k-piece` | 4 hoặc 8 | Số lượng mảnh độc lập mã hóa RLNC được tạo ra từ mỗi cột ban đầu |
| `n = 2K` | 32 (hoặc 8 ở test nhỏ) | Kích thước EDS sau khi mở rộng ($2K \times 2K$ ô) |
| Network columns | 8 (hoặc 4 ở test nhỏ) | Số lượng bootstrap node mạng = n/4 |
| Data cols per bootstrap | 4 (hoặc 2 ở test nhỏ) | Mỗi bootstrap node quản lý `n / network_columns` cột dữ liệu EDS |
| Store nodes per column | ≥4 (hoặc 2 ở test nhỏ) | Redundancy: các Store Node lưu trữ các hàng theo modulo |

> **Lưu ý tách biệt thông số:** Trước đây, kích thước khối gốc `K` và số lượng mảnh RLNC `k-piece` được gộp làm một. Hiện tại hệ thống đã tách biệt hoàn chỉnh. Khi tăng kích thước ma trận `K` (ví dụ từ 4 lên 16), số lượng mảnh RLNC phân mảnh từ cột (`k-piece`) vẫn có thể duy trì ở mức nhỏ (ví dụ 4 hoặc 8) để giảm thiểu tối đa overhead truyền tải P2P trong mạng lưới.

---

## 1. Publisher Node

### Vị Trí Code
- `cda-publisher-node/cmd/publisher/main.go`
- `cda-publisher-node/internal/service/api.go`
- `cda-publisher-node/internal/engine/pipeline.go`
- `cda-publisher-node/internal/p2p/sender.go`

### Nhiệm Vụ
Publisher Node là **điểm đầu vào** của hệ thống. Nó nhận dữ liệu raw từ bên ngoài (vd: rollup sequencer), xử lý toàn bộ pipeline mã hóa và phân phối dữ liệu tới mạng.

### Pipeline Xử Lý

```
Input (ODS hex data)
    ↓
1. Decode ODS: 4×4 ô dữ liệu gốc, mỗi ô là field element BLS12-381
    ↓
2. 2D Reed-Solomon Extension:
   - IFFT theo hàng → pad zero → FFT → mở rộng hàng
   - IFFT theo cột → pad zero → FFT → mở rộng cột
   → EDS $2K \times 2K$
    ↓
3. KZG Commitment (mỗi cột EDS):
   - Tính piece commitments $C_0..C_{k_{\text{piece}}-1}$ cho $k_{\text{piece}}$ pieces mỗi cột
   - Tính Merkle tree của piece commitments → commits_root
   - Tính combined column commitment (inner product commitment của cột)
    ↓
4. Block Header: {BlockID, commits_root, column_comms[2K], coeffs}
    ↓
5. Publish Header lên GossipSub topic /cda/1.0.0/header
    ↓
6. Distribute Columns: gửi mỗi cột EDS tới Bootstrap Node tương ứng qua P2P
```

### Giao Thức P2P
- **Outbound P2P stream:** `/cda/publisher/push-chunk/1.0.0` → gửi cột cho bootstrap
- **GossipSub Publish:** `/cda/1.0.0/header` → broadcast block header
- **HTTP Serve:** `GET /header/{blockID}` → fallback cho light node

### Keypair
- Seed: `"cda-publisher"`
- Hàm: `GenerateDeterministicKeypair("cda-publisher")`
- PeerID cố định, không phụ thuộc vào vị trí trong matrix

### Đã Làm Được
- [x] Pipeline mã hóa 2D RS + KZG hoàn chỉnh
- [x] Gửi cột tới đúng bootstrap node theo column index
- [x] Publish block header qua GossipSub với cơ chế chờ mesh peer
- [x] HTTP fallback endpoint cho light node
- [x] Kết nối P2P tới bootstrap nodes để tham gia GossipSub mesh

### Hạn Chế / Cần Cải Thiện
- **Chưa có authentication:** Bất kỳ client nào cũng có thể POST `/publish` — cần signature verification hoặc access control
- **Single publisher bottleneck:** Chỉ có 1 publisher node; trong hệ thống thực cần consensus về ai được phép publish
- **Không lưu trữ persistent:** Nếu publisher crash, dữ liệu block header bị mất; cần DB (LevelDB/Postgres)
- **GossipSub không có re-broadcast:** Nếu light node online sau khi header được publish, nó chỉ có thể fallback về HTTP

---

## 2. Bootstrap Node

### Vị Trí Code
- `cda-bootstrap-node/cmd/bootstrap/main.go`
- `cda-bootstrap-node/internal/p2p/receiver.go`
- `cda-bootstrap-node/internal/p2p/broadcaster.go`
- `cda-bootstrap-node/internal/engine/rlnc_encoder.go`

### Nhiệm Vụ
Bootstrap Node đóng **hai vai trò đồng thời:**

1. **Verifier & Seeder:** Nhận cột EDS từ publisher, verify 3-layer cryptographic proofs, sinh RLNC coded pieces và seed tới store nodes
2. **Registry & Router:** Duy trì danh sách active store nodes (peer registry) và cung cấp routing service cho light nodes khi DAS sampling

### Quá Trình Xử Lý Khi Nhận Cột

```
Publisher → [/cda/publisher/push-chunk/1.0.0] → Bootstrap
    ↓
Phase 1 (Synchronous, ~5ms):
  Layer 1 Verify: Merkle proof của từng piece commitment
  Layer 2 Verify: Combined column commitment vs header
  Layer 3 Sign:   GossipSub broadcast anchor tới store nodes
    ↓
Phase 2 (Async goroutine):
  Sinh $k_{\text{piece}}$ opening proofs (KZG) cho từng piece mỗi hàng
  RLNC encode: sinh $2 \cdot k_{\text{piece}}$ coded pieces ($k_{\text{piece}}$ original + $k_{\text{piece}}$ parity)
  Seed pieces tới Store Nodes: gửi $k_{\text{piece}}$ pieces tới Primary Store Node và $k_{\text{piece}}$ pieces tới Backup Store Node của hàng đó qua [/cda/bootstrap/seed-cell/1.0.0]
```

### Peer Registry
- Store nodes đăng ký qua P2P stream `/cda/bootstrap/routing/1.0.0`
- Registry: `activePeers map[peer.ID]PeerInfo` với TTL 15 giây (nếu không refresh)
- **Graceful leave:** Store node gửi `IsLeave: true` trước khi tắt → bootstrap xoá ngay khỏi registry

### Keypair
- Seed: `"cda-bootstrap-{colID}"` (vd: `"cda-bootstrap-0"`, `"cda-bootstrap-2"`, `"cda-bootstrap-4"`, `"cda-bootstrap-6"`)
- PeerID deterministic → publisher và light node có thể tính toán PeerID của bootstrap mà không cần discovery

### HTTP Endpoints
- `GET /bootstrap/peers` → trả danh sách active store peers (địa chỉ multiaddr)
- `GET /health` → health check

### GossipSub Topics
- **Subscribe:** `/cda/1.0.0/header` → relay block header tới light nodes trong mesh
- **Publish:** `/cda/1.0.0/col/{colIdx}` → broadcast anchor (commitments) tới store nodes

### Đã Làm Được
- [x] 3-layer cryptographic verification (Merkle + KZG column + combined column)
- [x] RLNC encoding và seeding tới store nodes
- [x] Dynamic peer registry với TTL và graceful leave
- [x] P2P routing service cho light nodes
- [x] GossipSub relay cho block header
- [x] Deterministic PeerID cho phép discovery không cần external service

### Hạn Chế / Cần Cải Thiện
- **Không có Byzantine fault tolerance:** Nếu bootstrap node bị compromise, nó có thể trả về danh sách store node giả → cần Merkle-based peer attestation
- **Single bootstrap per column:** Không có backup bootstrap; nếu bootstrap crash, cột đó mất toàn bộ routing
- **Seeding không có retry:** Nếu store node chưa kịp đăng ký khi seeding diễn ra, pieces bị bỏ qua hoàn toàn → cần retry queue hoặc lazy seeding
- **RLNC coding chưa có recoding:** Store nodes không thể recode từ pieces nhận được để phân phối tiếp (chỉ lưu, không lan truyền chủ động) 
- **Chưa có slashing:** Không có cơ chế phạt store node khai báo sai hoặc offline
- **Không có load balancing:** Tất cả store nodes trong cột nhận cùng số pieces từ bootstrap

---

## 3. Store Node

### Vị Trí Code
- `cda-store-node/cmd/store/main.go`
- `cda-store-node/internal/p2p/receiver.go`
- `cda-store-node/internal/p2p/broadcaster.go`
- `cda-store-node/internal/storage/custody.go`

### Nhiệm Vụ
Store Node là **nút lưu trữ** trong hệ thống. Mỗi store node chịu trách nhiệm cho một subset của EDS matrix (custody assignment) dựa theo vị trí `(row, col)` trong matrix.

### Custody Assignment
```
Ví dụ với K=4, n=8: EDS 8×8
- Store 0-1 (row=0, col=0): lưu hàng 0, 2, 4, 6 của cột subnet 0 (cột 0,1)
- Store 0-2 (row=1, col=0): lưu hàng 1, 3, 5, 7 của cột subnet 0 (cột 0,1)
→ Rule: store với rowIdx lưu các hàng r sao cho r % (n/K) == rowIdx % (n/K)
```

### Quá Trình Xử Lý Khi Nhận Pieces

```
Bootstrap → [/cda/bootstrap/seed-cell/1.0.0] → Store Node
    ↓
Layer 3 Verify:
  - Decode piece data, coefficients, combined proof
  - Verify piece vs combined column commitment (KZG opening)
    ↓
Store: custody.StorePiece(blockID, row, col, piece)
    ↓
Khi đủ $k_{\text{piece}}$ pieces cho cell [row, col]:
  - RLNC decode: reconstruct original data
  - Broadcast recoded pieces tới column peers (active pull optimization)
```

### Active Pull (Peer Recovery)
Khi light node query cell mà store không có đủ pieces:
```
Light → [/cda/store/get-cell-pieces] → Store-0-1
    ↓ (không đủ pieces)
Store-0-1 → [/cda/store/fetch-pieces] → Store-0-2 (cùng column subnet)
    ↓
Store-0-2 trả pieces → Store-0-1 combine → trả về light node
```

### GossipSub Topics (Subscribe)
- `/cda/1.0.0/col/{colIdx}` (cho tất cả `colIdx` trong tầm custody từ `startCol` đến `endCol - 1`) → nhận anchor (piece commitments) và các mảnh recode từ các store node khác trong nhóm cột dữ liệu.
- `/cda/1.0.0/row/{rowIdx}` → (reserved) cross-column coordination

### Keypair
- Hàm: `GenerateKeypairForCell(row, col, k1=k, k2=n, salt)`
- Brute-force tìm keypair sao cho `CalculateCell(PeerID) == (row, col)`
- Đảm bảo PeerID encode vị trí trong matrix → verifiable custody assignment

### Graceful Shutdown
Khi nhận SIGTERM/SIGINT:
1. Gửi `BootstrapRoutingRequest{IsLeave: true}` tới bootstrap → xoá khỏi registry ngay
2. Close HTTP server và P2P host

### Đã Làm Được
- [x] Layer 3 KZG verification của từng piece nhận từ bootstrap
- [x] In-memory custody store với thread-safe access và phục hồi tự động khi khởi động
- [x] **Lưu trữ persistent theo Block trên ổ đĩa:** Gom nhóm các pieces, anchors, và recoded pieces của mỗi block vào một file JSON duy nhất (`data/store_{port}/blocks/{blockID}.json`) giúp tối ưu hiệu năng I/O và quản lý file.
- [x] **Key Caching:** Cache keypair PeerID xuống file đĩa (`store_{port}.key`) giúp loại bỏ brute-force startup cost
- [x] **Token-bucket rate limiting:** Giới hạn tốc độ yêu cầu P2P trên store node theo PeerID để chống spam active pull. Tự động nâng hạn mức mặc định lên 1000 requests/giây (với burst 1000) để đáp ứng các bài test DAS mật độ cao dưới tải nặng của môi trường docker compose.
- [x] RLNC decoding: recover original cell data từ $k_{\text{piece}}$ coded pieces
- [x] Active pull: query peers trong cùng column subnet khi thiếu pieces
- [x] Graceful deregistration khi shutdown
- [x] HTTP status endpoint để test script kiểm tra completion
- [x] GossipSub subscription cho column anchor

### Hạn Chế / Cần Cải Thiện
- **Không có replication factor đảm bảo:** Nếu chỉ có 1 store node mỗi cột, single point of failure (tuy nhiên RS 2D vẫn khôi phục được ma trận nếu mất nguyên một cột)
- **Cần seed node / genesis bootstrap list:** Node mới gia nhập mạng cần biết ít nhất 1 địa chỉ IP thực của bootstrap; production nên dùng DNS seed (`bootstrap.cda-network.example.com`) hoặc hardcode genesis list

---

## 4. Light Node

### Vị Trí Code
- `cda-light-node/cmd/light/main.go`
- `cda-light-node/internal/service/api.go`
- `cda-light-node/internal/verifier/das_verifier.go`

### Nhiệm Vụ
Light Node thực hiện **Data Availability Sampling (DAS)**: xác minh rằng dữ liệu của block là sẵn có trong mạng mà không cần tải toàn bộ block. Light node là đối tượng phục vụ cuối cùng (client node) trong hệ thống.

### DAS Sampling Flow

```
Client → GET /das/sample/{blockID}?row=0&col=3 → Light Node
    ↓
1. Get Block Header:
   - Check local P2P cache (nhận qua GossipSub /cda/1.0.0/header)
   - Fallback: HTTP GET publisher/header/{blockID}
    ↓
2. Xác định Bootstrap cho cell (col):
   netColIdx = col / (n/4)
   bootAddr = bootstrapsMap[netColIdx]
    ↓
3. Query Bootstrap qua P2P [/cda/bootstrap/routing/1.0.0]:
   → Nhận danh sách active store nodes trong column subnet
    ↓
4. Thử kết nối dự phòng tuần tự (Fallback Loop):
   - Chọn ngẫu nhiên danh sách các store node ứng cử viên trong nhóm cột
   - Lần lượt kết nối (với timeout 8s) và truy vấn đến tối đa 3 Store Node cho đến khi thành công
    ↓
5. Query Store Node qua P2P [/cda/store/get-cell-pieces]:
   - Nhận coded pieces cho cell [row, col]
   - Nếu Store Node trả về lỗi logic (vd: rate limit exceeded), tự động chuyển sang Store Node dự phòng kế tiếp
    ↓
6. Verify locally:
   - RLNC decode: reconstruct cell data từ $k_{\text{piece}}$ pieces
   - KZG verify: piece vs column commitment (từ block header)
   - Output: verified=true/false + cell_data
```

### Sampling Modes
| Mode | Query Parameter | Mô Tả |
|------|----------------|--------|
| Specific cell | `?row=R&col=C` | Sample đúng 1 ô |
| Random DAS | `?samples=N` | Random N ô (default 4) |
| Full matrix | `?all=true` | Sample toàn bộ $2K \times 2K$ ô |

### Keypair
- Seed: `"cda-light-{port}"` (vd: `"cda-light-8095"`)
- PeerID không cần map vào cell, chỉ cần unique identifier

### GossipSub
- **Subscribe:** `/cda/1.0.0/header` → cache block headers ngay khi nhận
- **Connect:** Dial tới tất cả bootstrap nodes khi khởi động để tham gia GossipSub mesh

### Đã Làm Được
- [x] Random DAS sampling với KZG verification cục bộ
- [x] Full matrix sampling (N×N ô)
- [x] Dynamic routing: query bootstrap để tìm store nodes (không hardcode)
- [x] P2P kết nối trực tiếp tới store nodes để lấy pieces
- [x] Block header caching qua GossipSub
- [x] HTTP fallback khi header chưa có trong cache
- [x] GossipSub mesh connection khi khởi động

### Hạn Chế / Cần Cải Thiện
- **Sampling không stateless:** Light node cần biết địa chỉ bootstrap nodes trước (từ config); trong mạng thực cần DNS-based discovery hoặc genesis config
- **Không có erasure coding verification đầy đủ:** Chỉ verify tính đúng đắn của piece (KZG), không verify rằng các cells tuân theo cấu trúc 2D RS (cần row proof + col proof)
- **Random sampling chưa đủ mẫu:** Mặc định 4 samples, nhưng theo lý thuyết cần ~30 samples để đạt 99.9% confidence rằng block không bị withhold
- **Không có caching lâu dài:** Headers và verification results không persist qua restart

---

## 5. Shared P2P Layer (`cda-p2p`)

### Giao Thức P2P Stream (libp2p)

| Protocol ID | Hướng | Mô Tả |
|------------|-------|--------|
| `/cda/publisher/push-chunk/1.0.0` | Publisher → Bootstrap | Gửi cột EDS để verify và seed |
| `/cda/bootstrap/routing/1.0.0` | Any → Bootstrap | Store đăng ký / Light node query routing |
| `/cda/bootstrap/seed-cell/1.0.0` | Bootstrap → Store | Gửi coded RLNC piece để lưu trữ |
| `/cda/store/fetch-pieces/1.0.0` | Store → Store | Active pull giữa các store nodes cùng subnet |
| `/cda/store/get-cell-pieces/1.0.0` | Light → Store | Light node lấy pieces để verify |

### GossipSub Topics

| Topic | Publisher | Subscriber | Nội Dung |
|-------|-----------|-----------|---------|
| `/cda/1.0.0/header` | Publisher | Bootstrap, Light, Store | Block header (blockID, commitments) |
| `/cda/1.0.0/col/{N}` | Bootstrap | Store nodes của cột N | Anchor payload (piece commitments + Merkle proofs) |

### Keypair Scheme

```
Node Type      Seed                          Hàm
─────────────────────────────────────────────────────────────
Publisher      "cda-publisher"               GenerateDeterministicKeypair
Bootstrap-N    "cda-bootstrap-{colID}"       GenerateDeterministicKeypair
Store [r,c]    brute-force {r,c,counter}     GenerateKeypairForCell
Light-P        "cda-light-{port}"            GenerateDeterministicKeypair
```

### CalculateCell — Verifiable Custody

```go
func CalculateCell(pid peer.ID, k1, k2 int, salt string) (row, col int)
```
Dùng **BLAKE3** hash của PeerID để tính vị trí `(row, col)` trong matrix. Đảm bảo:
- Deterministic: cùng PeerID luôn cho cùng cell
- Non-predictable: không thể chọn trước PeerID để map vào cell mong muốn

---

## 6. Dynamic Membership (Store & Light Nodes Join/Leave)

Mạng lưới CDA được thiết kế để hỗ trợ tính năng tự phục hồi (self-healing) và tự động nhận diện thành viên (dynamic membership) khi các node tự do gia nhập (Join) hoặc rời mạng (Leave).

### A. Store Node (Thành viên lưu trữ custody)

Store Node có trạng thái đăng ký động với Bootstrap Node của cột mà nó quản lý.

1. **Gia nhập mạng (Join & Register):**
   - Khi khởi động, Store Node kết nối với Bootstrap Node được cấu hình và gửi yêu cầu `BootstrapRoutingRequest` chứa `PeerInfo` (ID, Multiaddrs, tọa độ Row, Col).
   - Bootstrap Node lưu thông tin này vào bảng đăng ký động `activePeers` với thời gian sống **TTL = 15 giây**.
   - Bootstrap Node phản hồi danh sách các Store Node khác hiện có trong cùng nhóm hàng (`RowPeers`) và cùng cột (`ColPeers`).
   - Store Node mới lập tức thực hiện kết nối P2P trực tiếp tới các peer này để thiết lập mạng lưới liên kết ngang hàng **Persistent Clique (Mesh)** của subnet cột.

2. **Duy trì hoạt động (Heartbeat & Keepalive):**
   - Để tránh bị xóa khỏi danh sách do hết hạn TTL, Store Node chạy một luồng nền (sync loop) định kỳ **mỗi 2 giây** gọi hàm `registerAndSyncPeers()`.
   - Mỗi chu kỳ, Store Node gửi lại request đăng ký lên Bootstrap Node để làm mới (refresh) TTL và đồng thời nhận về danh sách row/col peers cập nhật mới nhất.
   - Nhờ chu kỳ 2 giây này, các Store Node cũ trong mạng sẽ tự động phát hiện và kết nối với Store Node mới gia nhập chỉ trong vòng tối đa 2 giây.

3. **Rời mạng (Leave):**
   - **Rời mạng chủ động (Graceful Leave):** Khi nhận tín hiệu tắt máy (`SIGINT/SIGTERM`), Store Node gửi gói tin `BootstrapRoutingRequest` với cờ `IsLeave: true` tới Bootstrap Node. Bootstrap Node sẽ xóa ngay lập tức node đó khỏi registry.
   - **Rời mạng đột ngột (Ungraceful Leave / Crash):** Nếu Store Node bị sập đột ngột (mất điện, lỗi phần cứng), Bootstrap Node sẽ tự động xóa node khỏi registry sau khi hết hạn **15 giây TTL** mà không nhận được heartbeat.

---

### B. Light Node (Client truy vấn DAS)

Light Node hoạt động hoàn toàn không lưu trạng thái đăng ký (Stateless Client), giúp tối giản tải quản lý thành viên.

1. **Gia nhập mạng (Join):**
   - Light Node khi khởi động sẽ thực hiện kết nối P2P trực tiếp tới danh sách các Bootstrap Node.
   - Nó đăng ký (subscribe) vào GossipSub topic chung `/cda/1.0.0/header` để nhận block header mới từ Publisher.
   - Light Node **không đăng ký** vào bất kỳ bảng thành viên custody nào của Bootstrap Node vì nó không lưu giữ mảnh dữ liệu (nhiệm vụ custody).

2. **Truy vấn Động (On-Demand Discovery):**
   - Khi cần kiểm thử DAS cho một ô dữ liệu `[row, col]`, Light Node tính toán Bootstrap Node chịu trách nhiệm cho cột `col` và gửi truy vấn `/cda/bootstrap/routing/1.0.0` để lấy danh sách Store Node đang hoạt động thực tế.
   - Vì danh sách này được truy vấn động trực tiếp từ Bootstrap Node ngay tại thời điểm sample, Light Node luôn nhận được danh sách Store Node mới nhất và chính xác nhất (bao gồm cả các Store Node mới gia nhập).

3. **Xử lý Store Node Offline (Fallback Loop):**
   - Nếu Store Node mục tiêu đột ngột rời mạng (hoặc bị sập), yêu cầu kết nối của Light Node sẽ gặp lỗi.
   - Nhờ cơ chế **Fallback Loop (kết nối dự phòng tuần tự)**, Light Node sẽ tự động thử kết nối sang Store Node dự phòng khác trong danh sách (tối đa 3 node) với timeout 8s để đảm bảo việc lấy mảnh dữ liệu vẫn thành công.

4. **Rời mạng (Leave):**
   - Light Node có thể tắt bất cứ lúc nào mà không cần gửi thông báo rời mạng, vì Bootstrap Node không lưu trữ bất kỳ trạng thái nào của Light Node trong registry.

---

## 7. Hạn Chế Tổng Thể Của Prototype

### Cryptographic / Security

| Vấn đề | Mô Tả | Giải Pháp Đề Xuất |
|--------|--------|-------------------|
| Trusted setup | SRS được tạo ngẫu nhiên (`big.NewInt(-1)`) | Cần ceremony (Powers of Tau) trong production |
| DAS sample count thấp | Prototype test với 2 light nodes; statistical security cần nhiều node độc lập sampling cùng lúc | Tăng số lượng light node, mỗi node sample ≥30 cells ngẫu nhiên để đạt 99.9% confidence |


### Infrastructure / Scalability

| Vấn đề | Mô Tả | Giải Pháp Đề Xuất |
|--------|--------|-------------------|
| All in-memory | Tất cả state (headers trên Publisher, registry trên Bootstrap) lưu RAM | Chuyển đổi sang persistent storage cho Publisher/Bootstrap (Store Node đã persistent) |
| Không có NAT traversal | `NewP2PHost` chỉ bind `/ip4/0.0.0.0`, không có hole punching | Enable libp2p AutoNAT + Circuit Relay |
| Single point of failure | Mỗi thành phần chỉ có 1 instance | Đã hỗ trợ HA Failover cho Bootstrap (Light/Store nodes hỗ trợ khai báo array bootstrap và failover). Cần tích hợp thêm leader election cho multi-bootstrap |
| Bootstrap node discovery | Node mới cần địa chỉ bootstrap từ config; không có tự động khám phá | DNS seed record hoặc genesis bootstrap list (không cần DHT vì routing đã structured theo ma trận) |


### Network Protocol

| Vấn đề | Mô Tả | Giải Pháp Đề Xuất |
|--------|--------|------------------|
| Không có block ordering | BlockID là string tự do, không có height/slot | Thêm slot number và fork choice rule (phục vụ mô phỏng) |
| GossipSub không có retention | Header message mất nếu node offline khi publish | Implement store-and-forward hoặc message retention |
| HTTP fallback centralized | Light node phải biết địa chỉ publisher | Loại bỏ HTTP fallback sau khi GossipSub ổn định |
| Không có rate limiting | Bất kỳ peer nào cũng có thể spam requests | Thêm request rate limiting per PeerID |

---

## 8. Tóm Tắt Đánh Giá

### Những Gì Đã Hoạt Động Tốt

```
✅ Toàn bộ pipeline mã hóa: ODS → EDS → KZG commitments → RLNC pieces
✅ 3-layer cryptographic verification trên bootstrap node
✅ Dynamic peer discovery và routing không hardcode
✅ DAS sampling với algebraic verification cục bộ trên light node
✅ Graceful join/leave với bootstrap registry
✅ E2E test hoàn chỉnh bao gồm node failure simulation
✅ Docker-based deployment với docker-compose
✅ GossipSub P2P propagation cho block headers (sau fix timing race)
✅ Lưu trữ persistent cục bộ cho store node (file-based JSON)
✅ Cache keypair PeerID để tránh brute-force khi restart
✅ Rate limiting cho các stream active pull
✅ Bootstrap HA failover và dự phòng kết nối
```

### Ưu Tiên Cải Thiện Để Deploy Thực Tế

1. **Persistent storage cho Publisher/Bootstrap** — Bổ sung cơ chế lưu trữ đĩa cho block headers trên Publisher và registry trên Bootstrap node
2. **Trusted KZG setup** — Cần ceremony thực sự (Powers of Tau), không thể dùng random SRS
3. **Scale light node fleet** — Statistical security của DAS phụ thuộc vào số lượng light node độc lập sampling; cần tối thiểu hàng chục node   
4. **NAT traversal** — libp2p AutoNAT + Circuit Relay để nodes hoạt động sau firewall/NAT
5. **DNS seed / genesis bootstrap list** — Cho phép tự động hóa quá trình cấu hình bootstrap khi node mới gia nhập mạng


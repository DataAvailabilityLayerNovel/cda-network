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

### Tham Số Thiết Kế (tham số K=4)

| Tham số | Giá trị | Ý nghĩa |
|---------|---------|---------|
| `k` | 4 | Số chunks ODS mỗi chiều (4×4 = 16 ô dữ liệu gốc) |
| `n = 2k` | 8 | Kích thước EDS sau khi mở rộng (8×8 = 64 ô) |
| Network columns | 4 | Số bootstrap node = n/2 |
| Data cols per bootstrap | 2 | Mỗi bootstrap quản lý 2 cột dữ liệu EDS |
| Store nodes per column | ≥2 | Redundancy: mỗi store lưu các hàng theo modulo |

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
   - IFFT theo hàng → pad zero → FFT → mở rộng thành 8×4 (mở rộng hàng)
   - IFFT theo cột → pad zero → FFT → mở rộng thành 8×8 (mở rộng cột)
   → EDS 8×8
    ↓
3. KZG Commitment (mỗi cột EDS):
   - Tính piece commitments C_0..C_{k-1} cho k=4 pieces mỗi cột
   - Tính Merkle tree của piece commitments → commits_root
   - Tính combined column commitment (inner product commitment của cột)
    ↓
4. Block Header: {BlockID, commits_root, column_comms[8], coeffs}
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
  Sinh k=4 opening proofs (KZG) cho từng piece mỗi hàng
  RLNC encode: sinh n=8 coded pieces (k=4 original + 4 parity)
  Seed pieces tới registered store nodes qua [/cda/bootstrap/seed-cell/1.0.0]
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
- **Registry in-memory:** Toàn bộ peer registry lưu trong RAM, không persistent; restart mất toàn bộ state
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
k=4, n=8: EDS 8×8
- Store 0-1 (row=0, col=0): lưu hàng 0, 2, 4, 6 của cột subnet 0 (cột 0,1)
- Store 0-2 (row=1, col=0): lưu hàng 1, 3, 5, 7 của cột subnet 0 (cột 0,1)
→ Rule: store với rowIdx lưu các hàng r sao cho r % 2 == rowIdx % 2
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
Khi đủ k pieces cho cell [row, col]:
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
- `/cda/1.0.0/col/{colIdx}` → nhận anchor (piece commitments) từ bootstrap
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
- [x] In-memory custody store với thread-safe access
- [x] RLNC decoding: recover original cell data từ k coded pieces
- [x] Active pull: query peers trong cùng column subnet khi thiếu pieces
- [x] Graceful deregistration khi shutdown
- [x] HTTP status endpoint để test script kiểm tra completion
- [x] GossipSub subscription cho column anchor

### Hạn Chế / Cần Cải Thiện
- **Lưu trữ in-memory:** Restart store node → mất toàn bộ pieces; cần persistent storage (LevelDB, RocksDB)
- **Custody assignment chưa verifiable on-chain:** Không có smart contract để enforce store node phải lưu đúng custody
- **Không có proof-of-custody:** Store node không cần chứng minh đang lưu dữ liệu; cần periodic challenge-response
- **Không có replication factor đảm bảo:** Nếu chỉ có 1 store node mỗi cột, single point of failure
- **Active pull không có rate limiting:** Light node có thể gây DoS bằng cách query liên tục
- **PeerID brute-force startup cost:** `GenerateKeypairForCell` có thể mất vài giây nếu điều kiện khắt khe
- **Chưa có DHT:** Peer discovery dựa vào bootstrap registry, không có global DHT fallback

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
4. Random chọn 1 store node từ danh sách
    ↓
5. Query Store Node qua P2P [/cda/store/get-cell-pieces]:
   → Nhận coded pieces cho cell [row, col]
    ↓
6. Verify locally:
   - RLNC decode: reconstruct cell data từ k pieces
   - KZG verify: piece vs column commitment (từ block header)
   - Output: verified=true/false + cell_data
```

### Sampling Modes
| Mode | Query Parameter | Mô Tả |
|------|----------------|--------|
| Specific cell | `?row=R&col=C` | Sample đúng 1 ô |
| Random DAS | `?samples=N` | Random N ô (default 4) |
| Full matrix | `?all=true` | Sample toàn bộ n×n ô (64 ô với k=4) |

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
- **Header trust từ 1 nguồn:** Chỉ nhận header từ publisher (hoặc GossipSub relay của publisher), không verify header đến từ consensus majority
- **Không có fraud proof:** Nếu store node trả về data giả, light node sẽ phát hiện sai KZG verify nhưng không thể tạo fraud proof để submit on-chain
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

## 6. Hạn Chế Tổng Thể Của Prototype

### Cryptographic / Security

| Vấn đề | Mô Tả | Giải Pháp Đề Xuất |
|--------|--------|-------------------|
| Trusted setup | SRS (Structured Reference String) được tạo ngẫu nhiên (`big.NewInt(-1)`) | Cần ceremony (Powers of Tau) trong production |
| Không có row proofs | Chỉ verify column commitment, không verify row structure | Thêm row commitment scheme |
| Không có proof-of-custody | Store nodes không cần chứng minh đang giữ data | Implement periodic challenge (PoC protocol) |
| No fraud proofs | Light node phát hiện lỗi nhưng không thể prove on-chain | Implement zkSNARK-based fraud proofs |

### Infrastructure / Scalability

| Vấn đề | Mô Tả | Giải Pháp Đề Xuất |
|--------|--------|-------------------|
| All in-memory | Tất cả state (headers, peers, pieces) lưu RAM | Thêm persistent layer (LevelDB/RocksDB) |
| Không có DHT | Peer discovery dựa hoàn toàn vào bootstrap registry | Integrate libp2p Kademlia DHT |
| Không có NAT traversal | `NewP2PHost` chỉ bind `/ip4/0.0.0.0`, không có hole punching | Enable libp2p AutoNAT + Circuit Relay |
| Single point of failure | Mỗi thành phần chỉ có 1 instance | Implement HA với consensus (RAFT/Tendermint) |
| Không có config management | Config qua CLI flags | Chuyển sang config file + environment variables + Kubernetes ConfigMap |

### Network Protocol

| Vấn đề | Mô Tả | Giải Pháp Đề Xuất |
|--------|--------|-------------------|
| Không có block ordering | BlockID là string tự do, không có height/slot | Thêm slot number và fork choice rule |
| GossipSub không có retention | Header message mất nếu node offline khi publish | Implement store-and-forward hoặc message retention |
| HTTP fallback centralized | Light node phải biết địa chỉ publisher | Loại bỏ HTTP fallback sau khi GossipSub ổn định |
| Không có rate limiting | Bất kỳ peer nào cũng có thể spam requests | Thêm request rate limiting per PeerID |

---

## 7. Tóm Tắt Đánh Giá

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
```

### Ưu Tiên Cải Thiện Để Deploy Thực Tế

1. **Persistent storage** — Không thể có production system với all-in-memory
2. **Trusted KZG setup** — Cần ceremony thực sự, không thể dùng random SRS
3. **Proof-of-custody protocol** — Core mechanism của DA layer
4. **Fraud proof generation** — Để light node có thể report on-chain
5. **DHT-based peer discovery** — Không phụ thuộc vào bootstrap node
6. **Consensus/ordering layer** — Ai được phép publish block và theo thứ tự nào
7. **NAT traversal** — Để nodes hoạt động sau firewall/NAT trong môi trường thực

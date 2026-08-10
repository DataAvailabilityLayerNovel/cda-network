# BẢN THIẾT KẾ KIẾN TRÚC HỆ THỐNG MẠNG CDA (2D MESH & CDA LAYER)

---

## I. TỔNG QUAN TỔ CHỨC MẠNG LƯỚI 2D ($k_1 \times k_2$)

Mạng lưới CDA được tổ chức thành một ma trận gồm $k_1$ Hàng và $k_2$ Cột. Toàn bộ dữ liệu khối (EDS) mở rộng qua Reed-Solomon 2D có kích thước ma trận tương ứng.

```text
       Subnet Cột 0          Subnet Cột 1                    Subnet Cột k2-1
            │                     │                                │
 Subnet ──► ┌─────────────────────┬─────────────────── ... ────────┐
 Hàng 0     │ Cell (0,0)          │ Cell (0,1)        ...          │ Cell (0, k2-1)
            ├─────────────────────┼─────────────────── ... ────────┤
 Subnet ──► │ Cell (1,0)          │ Cell (1,1)        ...          │ Cell (1, k2-1)
 Hàng 1     ├─────────────────────┼─────────────────── ... ────────┤
            │ ...                 │ ...               ...          │ ...
            ├─────────────────────┼─────────────────── ... ────────┤
 Subnet ──► │ Cell (k1-1, 0)      │ Cell (k1-1, 1)    ...          │ Cell (k1-1, k2-1)
 Hàng k1-1  └─────────────────────┴─────────────────── ... ────────┘
```

### 1. Thuật toán Gán Ô Mạng $Cell(P)$ cho Node mới

Khi một Node $P$ khởi tạo (dù là Normal Node hay Bootstrap Node):

* **Node ID:** Khởi tạo từ Ed25519 Keypair $\rightarrow$ `PeerID`.
* **Vị trí Cố định:** Tọa độ $(r, c) = Cell(P)$ được tính thông qua hàm băm ngẫu nhiên đồng nhất:

$$\text{Hash}(P) = \text{Blake3}(\text{PeerID} \parallel \text{NetworkSalt})$$
$$r = \text{Hash}(P) \pmod{k_1} \quad (\text{Hàng } r), \qquad c = \text{Hash}(P) \pmod{k_2} \quad (\text{Cột } c)$$

### 2. Quy tắc Giữ Kết nối Peer (Topology & Connection Rules)

* **Store / Normal Node ($role = 0$) tại ô $(r, c)$:**
  * Chỉ duy trì kết nối dài hạn với các Node nằm trong **Subnet Hàng $r$** ($Row(P') = r$) và các Node trong **Subnet Cột $c$** ($Col(P') = c$).
  * Lưu trữ custody phần dữ liệu và các mảnh RLNC thuộc **Cột $c$**.

* **Bootstrap Node ($role = 1$) tại ô $(r, c)$:**
  * Tham gia và duy trì kết nối với **TẤT CẢ các Subnet Hàng** ($r \in [0, k_1-1]$) để làm hạ tầng Discovery cho mạng.
  * Duy trì kết nối với **Subnet Cột $c$** mà nó thuộc về để khởi tạo và phát tán hạt giống RLNC.

---

## II. LUỒNG DỮ LIỆU TỔNG THỂ & QUY TRÌNH VẬN HÀNH 4 LOẠI NODE

```text
========================================================================================================================
                                          LUỒNG DỮ LIỆU TỔNG THỂ TRÊN LƯỚI 2D
========================================================================================================================

 [ PUBLISHER NODE ]
        │
        ├─── (1) Broadcast Header ────────► Topic PubSub: `/cda/1.0.0/header` (Toàn mạng)
        │                                   (Chứa: commits_root, C_col, Vector x toàn cục)
        │
        └─── (2) Direct Stream ───────────► [ BOOTSTRAP NODE (Cột c) ]
                 Chunk_c + Merkle Proof            │
                 + Tập (C_0 .. C_{k-1})            ├── PHASE 1: FAST COMMITMENT BROADCAST LAYER (Độ trễ ~0)
                                                   │    ├── 1. Verify Merkle Proof (C_j với commits_root)
                                                   │    ├── 2. Verify Fiat-Shamir: ∑ x_j * C_j == C_col
                                                   │    └── 3. BROADCAST TOÀN CỘT: Topic `/cda/1.0.0/col/c`
                                                   │                                        │
                                                   │                                        ▼
                                                   │                         [ STORE NODES IN COLUMN c ]
                                                   │                         (Nhận & Anchor tập C_j chuẩn trước)
                                                   │
                                                   └── PHASE 2: COMPUTE & RLNC SEEDING LAYER (Async Heavy Task)
                                                        ├── 1. Compute FK20: Gen N x k Proofs Π_{j,r}
                                                        ├── 2. Generate m_min = 3 RLNC seeds per Row r
                                                        └── 3. UNICAST SEEDS: Send (d_i, g_i, P_i) [i=1..3]
                                                                                            │
                                                                                            ▼
                                                                             [ STORE NODES (Custody Cell [r, c]) ]
                                                                             ├── 1. Verify C_j với commits_root & Header
                                                                             ├── 2. Verify Piece RLNC: Verify(∑g_j*C_j, r, d, P)
                                                                             ├── 3. Rank(g) Check (Ensure Rank >= 2)
                                                                             ├── 4. Recode RLNC -> (d_new, g_new, P_new)
                                                                             └── 5. GossipSub Recoded Pieces trong Cột c
                                                                                                 ▲
                                                                                                 │ (Query Random Cells - DAS)
                                                                             [ LIGHT NODE / VERIFIER ]
                                                                             ├── 1. Tải Header nhẹ (~13 KB)
                                                                             ├── 2. Collect k independent RLNC pieces (d', g', P')
                                                                             ├── 3. Gaussian Solve: S = A^-1 * d', Π = A^-1 * P'
                                                                             └── 4. Verify O(1) trực tiếp với C_col & Vector x
========================================================================================================================
```

---

### 1. Publisher Node (Node Phát Hành Khối)

1. Broadcast Header công khai lên Topic `/cda/1.0.0/header`.
2. **Direct Stream Unicast:** Mở stream `/cda/publisher/push-chunk/1.0.0` gửi trực tiếp cho Bootstrap Node Cột $c$: Chunk $c$, tập $(C_{c, 0} \dots C_{c, k-1})$, và Merkle Proof của $C_{c, j}$ dẫn về `commits_root`.

---

### 2. Bootstrap Node (Nút Khởi Tạo Cột)

Chạy trên **Pipeline 2-Phase** để tối ưu tốc độ:

* **Phase 1: Fast Commitment Broadcast (Độ trễ micro-seconds)**
  **Column Broadcast:** Bắn ngay tập $(C_{c,0} \dots C_{c,k-1})$ + Merkle Proof lên Topic PubSub Subnet Cột `/cda/1.0.0/col/c` để các Store Node anchor sẵn trạng thái.

* **Phase 2: Compute & Seed RLNC Pieces (Async Task)**
  1. **Unicast Seeding:** Mở P2P stream `/cda/bootstrap/seed-cell/1.0.0` gửi $3$ bộ mảnh hạt giống $(d_{i,r}, g_i, P_{i,r})$ tới các Store Nodes giữ Custody Cell $[r, c]$.
  2. **Routing Support:** Phục vụ RPC `/cda/bootstrap/routing/1.0.0` để các Node mới truy vấn danh sách Node thuộc các Row Subnets.

---

### 3. Store Node (Node Lưu Trữ Custody)

Nằm tại ô $(r, c)$, chỉ giữ kết nối với Subnet Hàng $r$ và Subnet Cột $c$:

1. **Anchor Cam kết (Từ Phase 1 của Bootstrap Node):**
  * Nhận tập $C_j$ từ Topic `/cda/1.0.0/col/c`.
  * Verify Merkle với `commits_root` và verify Fiat-Shamir với $C^{\text{col}}_c$ trên Header. Lưu cố định $C_j$ vào RAM.

2. **Tiếp nhận & Verify Mảnh RLNC (Từ Phase 2 hoặc Peer Gossip):**
  * Nhận $m_{\text{min}} = 3$ hạt giống từ Bootstrap Node hoặc mảnh mã hóa từ các Store Node khác.
  * **KZG Pairing Check:** Tính $C_{\text{combined}} = \sum g_j \cdot C_{c, j}$, gọi hàm Verify Pairing: $\text{Verify}(C_{\text{combined}}, r, d, P) == \text{true}$.

3. **Lọc Rank & P2P Recoding:**
  * Khử Gauss ma trận $g$. Nếu Rank không tăng $\rightarrow$ Drop.
  * Tích lũy đủ $m \ge 2$ mảnh độc lập tuyến tính, sinh $\beta_i \in \mathbb{F}_r$ để Recode mảnh mới: $d_{\text{new}} = \sum \beta_i d_i$, $g_{\text{new}} = \sum \beta_i g_i$, $P_{\text{new}} = \sum \beta_i P_i$.
  * GossipSub mảnh mới lên Topic Subnet Cột `/cda/1.0.0/col/c`.

4. **Active Peer Pull Retrieval (Khi cần khôi phục Cell):**
  * Nếu thiếu mảnh ($< k$), mở P2P Stream `/cda/store/fetch-pieces/1.0.0` kéo mảnh từ các Store Node lân cận cùng cột.
  * Đính kèm cờ `is_remote_hop = true` trong Protobuf để ngắt vòng lặp truy vấn (Anti-Query Loops).

---

### 4. Light Node / Verifier Node (Node Lấy Mẫu DAS)

1. **Header Sync:** Tải Block Header ($\approx 13 \text{ KB}$) từ Topic `/cda/1.0.0/header`.
2. **Random Sampling:** Sinh ngẫu nhiên vị trí ô $[r_1, c_1]$. Dùng Kademlia DHT tìm `PeerID` của các Store Node giữ ô $[r_1, c_1]$.
3. **Direct Query:** Mở P2P Stream `/cda/store/get-cell-pieces/1.0.0` lấy $k$ mảnh mã hóa RLNC.

---

## IV. QUY TRÌNH GIA NHẬP MẠNG & DỌN DẸP KẾT NỐI (2-ROUND PRUNING)

Khi một Node mới $P_{\text{new}}$ khởi chạy:

```text
========================================================================================================================
                                 QUY TRÌNH KẾT NỐI VÀ DỌN DẸP PEER CỦA NODE MỚI
========================================================================================================================

 [ NODE MỚI: P_new ]
        │
        ├── 1. Tự tính Cell(P_new) = (r_new, c_new) = Blake3(PeerID) % (k1, k2)
        │
        ├── 2. [KẾT NỐI TẠM THỜI] Connect Seed Bootstrap Nodes -> Gọi RPC `/cda/bootstrap/routing/1.0.0`
        │      └── Nhận danh sách Peers thuộc Row Subnet r_new và Column Subnet c_new
        │
        ├── 3. [KẾT NỐI CHÍNH THỨC] Thiết lập kết nối dài hạn (Persistent Cliques):
        │      ├── Connect tới tất cả Peers thuộc Subnet Hàng r_new (/cda/1.0.0/row/r_new)
        │      └── Connect tới tất cả Peers thuộc Subnet Cột c_new (/cda/1.0.0/col/c_new)
        │
        └── 4. [CƠ CHẾ DỌN DẸP] Chờ T_grace = 2 rounds (vòng đồng bộ):
               └── Disconnect toàn bộ Bootstrap Nodes KHÔNG NẰM TRÊN Hàng r_new hoặc Cột c_new.
========================================================================================================================
```

---

## V. TỔNG HỢP GIAO THỨC PROTOBUF & MẠNG P2P

| Giao thức / Topic | Dạng giao tiếp | Bên gửi $\rightarrow$ Bên nhận | Dữ liệu luân chuyển |
| --- | --- | --- | --- |
| `/cda/1.0.0/header` | **PubSub** | Publisher $\rightarrow$ Toàn mạng | `BlockHeader` Protobuf ($\approx 13\text{ KB}$) |
| `/cda/1.0.0/col/{c}` | **PubSub** | Bootstrap / Store $\rightarrow$ Store Nodes Cột $c$ | Tập $C_j$ + Merkle Proof (Phase 1), Mảnh RLNC Recoded (Phase 2) |
| `/cda/1.0.0/row/{r}` | **PubSub** | Nodes Hàng $r \leftrightarrow$ Nodes Hàng $r$ | Discovery, Heartbeat, Đồng bộ trạng thái Hàng |
| `/cda/publisher/push-chunk/1.0.0` | **P2P Stream** | Publisher $\rightarrow$ Bootstrap Node | Chunk dữ liệu thô Cột $c$, $C_j$, Merkle Proof |
| `/cda/bootstrap/seed-cell/1.0.0` | **P2P Stream** | Bootstrap Node $\rightarrow$ Store Node | $m_{\text{min}} = 3$ Mảnh RLNC hạt giống $(d_i, g_i, P_i)$ |
| `/cda/store/fetch-pieces/1.0.0` | **P2P Stream** | Store Node $\leftrightarrow$ Store Node | Request/Response kéo mảnh độc lập (chứa cờ `is_remote_hop`) |
| `/cda/store/get-cell-pieces/1.0.0` | **P2P Stream** | Light Node $\rightarrow$ Store Node | Truy vấn DAS rút mẫu ô $[r_1, c_1]$ |
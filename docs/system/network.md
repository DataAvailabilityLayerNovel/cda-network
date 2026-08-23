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

### 2. Quy tắc Giữ Kết nối Peer & Phân định Custody (Topology & Custody Rules)

* **Store / Normal Node ($role = 0$) tại ô $(r, c)$ (`rowIdx = r`):**
  * Duy trì kết nối dài hạn với các Node nằm trong **Subnet Hàng $r$** ($Row(P') = r$) và các Node trong **Subnet Cột $c$** ($Col(P') = c$).
  * **Primary Custody Cells ($r \pmod{S} == \text{rowIdx}$):** Lưu trữ cố định và vĩnh viễn $\ge k_{piece}$ mảnh RLNC cùng cam kết anchor cho tất cả các ô Primary Custody thuộc nhiệm vụ.
  * **Backup Custody Cells ($(r+1) \pmod{S} == \text{rowIdx}$):** Lưu trữ tạm thời mảnh hạt giống nhận từ Bootstrap để hỗ trợ gossip trong giai đoạn phân tán cột (với độ trễ 25ms). Ngay khi hoàn thành phát tán lên kênh per-node topic `TopicNode(selfPeerID)`, các mảnh raw của ô Backup sẽ được dọn dẹp (`PruneRawPieces`) để tối ưu hóa bộ nhớ.

* **Bootstrap Node ($role = 1$) tại ô $(r, c)$:**
  * Tham gia và duy trì kết nối với **TẤT CẢ các Subnet Hàng** ($r \in [0, k_1-1]$) để làm hạ tầng Discovery cho mạng.
  * Duy trì kết nối với **Subnet Cột $c$** mà nó thuộc về để khởi tạo và phát tán hạt giống RLNC qua dedicated per-node stream.

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
                                                        ├── 2. Generate 2 x k_piece RLNC seeds per Row r
                                                        └── 3. UNICAST PER-NODE SEEDS: `/cda/store/<peerID>/seed/1.0.0`
                                                             (k_piece seeds cho Primary Node, k_piece seeds cho Backup Node)
                                                                                             │
                                                                                             ▼
                                                                              [ STORE NODES (Custody Cell [r, c]) ]
                                                                              ├── 1. Verify C_j với commits_root & Header
                                                                              ├── 2. Verify Piece RLNC: Verify(∑g_j*C_j, r, d, P)
                                                                              ├── 3. Gaussian Rank Filter (Ensure Rank >= 2)
                                                                              ├── 4. Recode RLNC -> (d_new, g_new, P_new)
                                                                              ├── 5. Broadcast Recoded Piece lên Dedicated Topic:
                                                                              │      `/cda/1.0.0/node/<selfPeerID>`
                                                                              └── 6. Active Pull từ Backup Node khi rank < k_piece
                                                                                                  ▲
                                                                                                  │ (Query Random Cells - DAS)
                                                                              [ LIGHT NODE / VERIFIER ]
                                                                              ├── 1. Tải Header nhẹ (~13 KB)
                                                                              ├── 2. Query k independent RLNC pieces (d', g', P')
                                                                              ├── 3. Gaussian Invert: S = A^-1 * d', Π = A^-1 * P'
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
  1. **Per-Node Unicast Seeding:** Mở dedicated P2P stream `/cda/store/<peerID>/seed/1.0.0` gửi $2 \times k_{piece}$ mảnh hạt giống $(d_{i,r}, g_i, P_{i,r})$ tới từng Store Node (gửi $k_{piece}$ mảnh cho Primary Node và $k_{piece}$ mảnh cho Backup Node).
  2. **Routing Support:** Phục vụ RPC `/cda/bootstrap/routing/1.0.0` để các Node mới truy vấn danh sách Node thuộc các Row Subnets.

---

### 3. Store Node (Node Lưu Trữ Custody)

Nằm tại ô $(r, c)$, giữ kết nối với Subnet Hàng $r$ và Subnet Cột $c$:

1. **Anchor Cam kết (Từ Phase 1 của Bootstrap Node):**
   * Nhận tập $C_j$ từ Topic `/cda/1.0.0/col/c`.
   * Verify Merkle với `commits_root` và verify Fiat-Shamir với $C^{\text{col}}_c$ trên Header. Lưu cố định $C_j$ vào RAM.

2. **Tiếp nhận & Verify Mảnh RLNC (Từ Phase 2 hoặc Dedicated Node Topic):**
   * Nhận $k_{piece}$ hạt giống từ Bootstrap Node qua stream dedicated hoặc mảnh recoded từ kênh dedicated `/cda/1.0.0/node/<peerID>` của các custody nodes khác.
   * **KZG Pairing Check:** Tính $C_{\text{combined}} = \sum g_j \cdot C_{c, j}$, gọi hàm Verify Pairing: $\text{Verify}(C_{\text{combined}}, r, d, P) == \text{true}$.

3. **Lọc Rank & P2P Recoding:**
   * Khử Gauss ma trận $g$. Nếu Rank không tăng $\rightarrow$ Drop.
   * Tích lũy đủ $m \ge 2$ mảnh độc lập tuyến tính, sinh $\beta_i \in \mathbb{F}_r$ để Recode mảnh mới bằng số học trường $\mathbb{F}_r$ (`vectorMulAddFr`): $d_{\text{new}} = \sum \beta_i d_i$, $g_{\text{new}} = \sum \beta_i g_i$, $P_{\text{new}} = \sum \beta_i P_i$.
   * **Dedicated Node Dissemination:** Phát tán mảnh mã hóa mới lên dedicated per-node topic `/cda/1.0.0/node/<selfPeerID>` để các non-custody nodes đăng ký theo dõi.

4. **Active Pull Retrieval ưu tiên Backup Node:**
   * Nếu thiếu mảnh cho ô custody ($< k_{piece}$), mở P2P Stream `/cda/store/fetch-pieces/1.0.0` với 3 lượt thử (backoff 150ms).
   * **Ưu tiên hàng đầu**: Tự động đưa Backup Node (`Row == (row + 1) % storesPerCol`) lên đầu danh sách truy vấn P2P để kéo bổ sung các mảnh hạt giống chưa nhận đủ từ Bootstrap.

---

### 4. Light Node / Verifier Node (Node Lấy Mẫu DAS)

1. **Header Sync:** Tải Block Header ($\approx 13 \text{ KB}$) từ Topic `/cda/1.0.0/header`.
2. **Random Sampling:** Sinh ngẫu nhiên vị trí ô $[r_1, c_1]$. Dùng Kademlia DHT tìm `PeerID` của các Store Node giữ ô $[r_1, c_1]$.
3. **Direct Query:** Mở P2P Stream `/cda/store/get-cell-pieces/1.0.0` lấy $k_{piece}$ mảnh mã hóa RLNC.
4. **Algebraic Combined Verification:** Nghịch đảo ma trận hệ số $A_{\text{recode}}$ trong $\mathbb{F}_r$, tái tạo mảnh thô và bằng chứng KZG tổ hợp, sau đó gọi `v.kzg.Verify` để xác thực ô dữ liệu với chi phí $O(1)$.

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
| `/cda/1.0.0/store-ready` | **PubSub** | Store Node $\rightarrow$ Bootstrap Relay $\rightarrow$ Publisher | `GossipStoreReadyPayload` (Hoàn thành Custody Node $Row$) |
| `/cda/1.0.0/block-ready` | **PubSub** | Publisher $\rightarrow$ Bootstrap Relay $\rightarrow$ Light Nodes | `GossipBlockReadyPayload` (Xác nhận 100% Block sẵn sàng cho DAS) |
| `/cda/1.0.0/col/{c}` | **PubSub** | Bootstrap / Store $\rightarrow$ Store Nodes Cột $c$ | Tập $C_j$ + Merkle Proof (Phase 1 Commitments) |
| `/cda/1.0.0/node/{peerID}` | **PubSub** | Store Node $\rightarrow$ Non-Custody Nodes | Recoded RLNC pieces phát tán theo kênh per-node dedicated |
| `/cda/1.0.0/row/{r}` | **PubSub** | Nodes Hàng $r \leftrightarrow$ Nodes Hàng $r$ | Discovery, Heartbeat, Đồng bộ trạng thái Hàng |
| `/cda/publisher/push-chunk/1.0.0` | **P2P Stream** | Publisher $\rightarrow$ Bootstrap Node | Chunk dữ liệu thô Cột $c$, $C_j$, Merkle Proof |
| `/cda/store/{peerID}/seed/1.0.0` | **P2P Stream** | Bootstrap Node $\rightarrow$ Store Node | $2 \times k_{piece}$ Mảnh RLNC hạt giống $(d_i, g_i, P_i)$ theo kênh per-node |
| `/cda/store/fetch-pieces/1.0.0` | **P2P Stream** | Store Node $\leftrightarrow$ Store Node | Request/Response kéo mảnh độc lập (ưu tiên Backup Node, chứa cờ `is_remote_hop`) |
| `/cda/store/get-cell-pieces/1.0.0` | **P2P Stream** | Light Node $\rightarrow$ Store Node | Truy vấn DAS rút mẫu ô $[r_1, c_1]$ |

---

## VI. KIẾN TRÚC TỔNG HỢP TÍN HIỆU HOÀN THÀNH & CỔNG KIỂM SOÁT TUẦN TỰ (PUBLISHER GATE)

```text
========================================================================================================================
                      LUỒNG TÍN HIỆU XÁC NHẬN HOÀN THÀNH VÀ ĐỒNG BỘ TUẦN TỰ KHỐI (BLOCK SEQUENTIALITY)
========================================================================================================================

 [ STORE NODES (Custody Rows r) ]
        │
        ├── (1) Custody Complete (IsComplete) ──► Broadcast `/cda/1.0.0/store-ready`
        │                                         Payload: {BlockID, NetColIdx, RowIdx, StoresPerCol}
        │
 [ BOOTSTRAP RELAY LAYER ] 
        │
        ├── (2) Relay GossipSub Mesh ───────────► [ PUBLISHER NODE (Aggregator & Gate) ]
        │                                         ├── 1. Filter: Bỏ qua cột non-active (netColNumber >= activeCols)
        │                                         ├── 2. Multi-Store Aggregator:
        │                                         │      Cột NetColIdx hoàn thành 100% ⇔ len(rows) == StoresPerCol
        │                                         ├── 3. Block Complete Check:
        │                                         │      Block Complete ⇔ len(completedNetCols) == activeCols
        │                                         └── 4. Broadcast `/cda/1.0.0/block-ready`
        │                                                                │
        ├── (3) Relay BlockReady Broadcast ──────────────────────────────┼──────────────────────────┐
        │                                                                ▼                          ▼
 [ LIGHT NODES (Verifier) ] ◄────────────────────────────────────────────┘                [ PUBLISHER QUEUE ]
 └── Nhận BlockReady ──► Kích hoạt Auto-DAS Sampling                                      └── Giải phóng Block H+1!
========================================================================================================================
```

### 1. Bộ lọc Cột Active (Active Column Filter) tại Publisher
* Publisher tính chỉ số cột mạng: $\text{netColNumber} = \text{netColIdx} / \text{colsPerNetCol}$.
* Tín hiệu từ các cột non-active ($\text{netColNumber} \ge \text{activeCols}$) bị hủy bỏ hoàn toàn, đảm bảo Publisher chỉ chờ đúng $A = \text{activeCols}$ cột active cần thiết.

### 2. Điều kiện Phát StoreReady & Tổng hợp Đa Node Custody (StoreReady & Multi-Store Aggregation)
* **Điều kiện phát StoreReady tại Store Node (`IsComplete`)**:
  - **Lưu trữ Custody Complete**: Đạt $\ge k_{piece}$ mảnh đối với 100% các ô **Custody** thuộc quyền quản lý của node ($r \pmod{S} == \text{RowIdx}$). Điều này đảm bảo node có 100% năng lực giải mã, recode và phục hồi chính xác các ô custody nhiệm vụ của nó.
  - **Phát tán Kênh Dedicated Complete**: Hoàn thành việc recode và phát tán thành công mảnh lên kênh dedicated per-node `TopicNode(selfPeerID) = /cda/1.0.0/node/<peerID>` cho 100% các ô custody của node (`cellBroadcastCount[cellKey] >= 1`). Tín hiệu `StoreReady` chỉ được kích hoạt khi cả 2 điều kiện trên đồng thời thỏa mãn.
* **Tổng hợp tại Publisher**: Cột mạng $\text{NetColIdx}$ hoàn thành khi và chỉ khi **tất cả $S = \text{storesPerCol}$ Store Nodes** (đủ mọi `RowIdx` từ $0 \dots S-1$) báo `StoreReady`:
$$\text{len}(\text{storeReadyMap}[\text{BlockID}][\text{NetColIdx}]) == \text{storesPerCol}$$
* Khối `BlockID` hoàn thành khi tất cả $A$ cột mạng active đạt trạng thái hoàn thành:
$$\text{len}(\text{completedNetCols}[\text{BlockID}]) == \text{activeCols}$$

### 3. Cổng Kiểm soát Tuần tự Khối (Sequential Completion Gate)
* Trực tiếp kiểm soát tại API `/publish` của Publisher Node:
  * Khi yêu cầu gửi Block $H$ tới: Nếu Block $H-1$ chưa hoàn thành (`latestCompletedHeight < H - 1`), hàm `handlePublish` cho Block $H$ đi vào trạng thái **CHỜ (BLOCK)** trên `publishCond.Wait()`.
  * Ngay khi Block $H-1$ đạt 100% active columns ready và phát `BlockReady`, Publisher cập nhật `latestCompletedHeight = H - 1` và giải phóng `publishCond.Broadcast()`.
  * Block $H$ được tháo khóa và mới bắt đầu được mã hóa và đẩy vào mạng.
* **Đảm bảo tuyệt đối**: Không xảy ra hiện tượng chồng chéo xử lý khối (block overlap) ở bất kỳ quy mô ma trận nào ($K=16, 32, 64$).
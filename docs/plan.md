# BẢN THIẾT KẾ HỆ THỐNG MẠNG DATA AVAILABILITY (CDA) - BẢN CHUẨN HÓA HOÀN CHỈNH

---

## I. CẤU TRÚC DỮ LIỆU CỐT LÕI & NGUYÊN LÝ MẬT MÃ

### 1. Header Công Khai Toàn Cục (Global Block Header)

Dung lượng Header được tối ưu hóa ở mức siêu nhẹ ($\approx 13 \text{ KB}$ cho ma trận $256 \times 256$), bao gồm các trường dữ liệu cốt lõi:

* $\text{BlockID} / \text{Height} / \text{Timestamp}$: Thông tin định danh và quản lý khối.


* $\text{commits\_root}$ (**32 bytes**): Merkle Root băm trực tiếp từ tập $N \times k$ cam kết mảnh $C_{c, j}$, giúp cố định cấu trúc cam kết và xác thực vẹn toàn cho P2P ở cấp độ micro-seconds.
* $C^{\text{col}}_0, C^{\text{col}}_1, \dots, C^{\text{col}}_{N-1}$: Danh sách $N$ cam kết cột gốc (mỗi cam kết là 1 điểm trên $G_1$, nén 48 bytes).


* $x \in \mathbb{F}_r^k$: Vector thử thách Fiat-Shamir toàn cục gồm $k$ phần tử $\mathbb{F}_r$ ($k \times 32 \text{ bytes} = 512 \text{ bytes}$).


* $\text{Signature}$: Chữ ký mật mã của Publisher đảm bảo tính chống chối bỏ.

---

### 2. Ràng buộc Mật mã Fiat-Shamir & Merkle Integrity

Để đảm bảo vừa nén dung lượng Header, vừa chống giả mạo cam kết mảnh $C_{c, j}$:

1. **Ràng buộc Fiat-Shamir Toàn cục:** Publisher tính $N \times k$ cam kết mảnh $C_{c, j}$ ($c \in [0, N-1], j \in [0, k-1]$). Vector $x$ được sinh ra từ Random Oracle băm nối toàn bộ ma trận cam kết:



$$x = \text{HashToField}(C_{0,0} \parallel C_{0,1} \parallel \dots \parallel C_{N-1,k-1}) \in \mathbb{F}_r^k$$



2. **Merkle Commitments Root:** Toàn bộ $N \times k$ cam kết mảnh $C_{c, j}$ được băm thành Cây Merkle để thu về $\text{commits\_root}$ (32 bytes). $\text{commits\_root}$ chính là đại diện cấu trúc của tập cam kết gốc, đồng thời là tiền ảnh (pre-image) tạo ra Vector $x$.
3. **Tổ hợp Cam kết Cột:** Cam kết cột gốc $C^{\text{col}}_c$ được tính bằng tổ hợp tuyến tính:

$$C^{\text{col}}_c = \sum_{j=0}^{k-1} x_j \cdot C_{c, j}$$




---

## II. THIẾT KẾ CHI TIẾT LUỒNG VẬN HÀNH CÁC NODE

```text
========================================================================================================================
                                                LUỒNG DỮ LIỆU TỔNG THỂ (CDA)
========================================================================================================================

 [ PUBLISHER NODE ]
        │
        ├─── (1) Broadcast Header ────────► [ GLOBAL HEADER CHANNEL / P2P ]
        │                                   (Chứa: commits_root, C_col, Vector x toàn cục)
        │
        └─── (2) Send Chunk_c ────────────► [ BOOTSTRAP NODE (Column c) ]
                 + Merkle Proof                    │
                 + (C_0 .. C_{k-1})                 ├── PHASE 1: FAST COMMITMENT BROADCAST LAYER (Độ trễ ~0)
                                                   │    ├── 1. Verify Merkle Proof (C_j với commits_root)
                                                   │    ├── 2. Verify Fiat-Shamir: ∑ x_j * C_j == C_col
                                                   │    └── 3. BROADCAST TOÀN CỘT: Phát tán (C_0..C_{k-1}) + Merkle Proof
                                                   │                                        │
                                                   │                                        ▼
                                                   │                         [ STORE NODES IN COLUMN c ]
                                                   │                         (Nhận & Anchor tập C_j chuẩn trước)
                                                   │
                                                   └── PHASE 2: COMPUTE & RLNC SEEDING LAYER (Async Heavy Task)
                                                        ├── 1. Compute Proof: Gen N x k Proofs Π_{j,r}
                                                        ├── 2. Generate 2 * k_piece RLNC seeds per Row r
                                                        └── 3. UNICAST SEEDS: Send k_piece seeds to Primary & k_piece seeds to Backup
                                                                                            │
                                                                                            ▼
                                                                             [ STORE NODES (Custody Cell [r, c]) ]
                                                                             ├── 1. Verify C_j với commits_root & Header
                                                                             ├── 2. Verify Piece RLNC: Verify(∑g_j*C_j, r, d, P)
                                                                             ├── 3. Rank(g) Check (Ensure Rank >= 2)
                                                                             ├── 4. Recode RLNC -> (d_new, g_new, P_new)
                                                                             └── 5. GossipSub Recoded Pieces trong Cột c (Cho toàn bộ custody columns)
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

### 1. Publisher Node (Nút Phát Hành Khối)

#### Quy trình xử lý:

1. **Mở rộng 2D Reed-Solomon:** Nhận ODS, mở rộng thành ma trận EDS $N \times N$ bằng Leopard Codec.


2. **Phân mảnh & Tính Cam kết KZG:** Chia Cell $2\text{ KB}$ thành $k$ mảnh nhỏ (32B), nhóm thành $k$ cột mảnh $D_0, \dots, D_{k-1}$. Tính $C_{c, j} = \text{Commit}(D_{c, j})$ cho toàn bộ ma trận $N \times k$.


3. **Dựng Merkle Tree cho Commitments:** Băm $N \times k$ cam kết mảnh $C_{c, j}$ thành Cây Merkle để thu về **`commits_root` (32 bytes)**.
4. **Ràng buộc Fiat-Shamir Toàn cục:**
* Tính $x = \text{HashToField}(C_{0,0} \parallel \dots \parallel C_{N-1,k-1})$.


* Tính $N$ cam kết cột gốc $C^{\text{col}}_c = \sum_{j=0}^{k-1} x_j \cdot C_{c, j}$.


* Đóng gói và Broadcast Header công khai ($\text{commits\_root}, C^{\text{col}}_0 \dots C^{\text{col}}_{N-1}, x$).


5. **Phân tán dữ liệu P2P:** Với Cột $c$, gửi Chunk dữ liệu $c$ ($D_0 \dots D_{k-1}$), tập $k$ cam kết mảnh $(C_{c, 0} \dots C_{c, k-1})$, và **Merkle Proof cho tập $C_{c, j}$ đối với `commits_root**` cho Bootstrap Node của Cột $c$.



---

### 2. Bootstrap Node (Nút Khởi Tạo Cột)

Mỗi Bootstrap Node phụ trách một Cột $c$ và xử lý dữ liệu theo quy trình **2 Phase độc lập** nhằm tối ưu hóa đường truyền P2P và giảm độ trễ:

#### ⚡ Phase 1: Fast Commitment Broadcast (Fast Path)
*Mục tiêu:* Phát tán tập cam kết $C_{c, j}$ ra toàn bộ Cột $c$ với độ trễ micro-seconds.
1. **Verify 2 Lớp:** 
   * *Lớp 1 (Merkle):* Kiểm tra Merkle Proof của tập $C_{c, j}$ với `commits_root` trên Header.
   * *Lớp 2 (Fiat-Shamir):* Kiểm tra $\sum_{j=0}^{k-1} x_j \cdot C_{c, j} \stackrel{?}{=} C^{\text{col}}_c$ sử dụng Vector $x$ từ Header.
2. **Column Broadcast:** Ngay khi xác minh thành công, lập tức GossipSub tập $(C_{c,0} \dots C_{c,k-1})$ kèm Merkle Proof ra **toàn bộ Store Nodes trong Cột $c$** để anchor trạng thái.

---

#### ⚙️ Phase 2: Compute & Seed RLNC Pieces (Async Heavy Task)
*Mục tiêu:* Sinh các mảnh mã hóa RLNC hạt giống ban đầu cho từng ô lưu ký.
1. **FK20 Proof Generation:** Chạy FK20 tính $N \times k$ proofs cơ sở $\Pi_{j,r}$ cho toàn Cột $c$.
2. **RLNC Seed Generation:** Tại mỗi Hàng $r$ (Cell $[r, c]$), sinh $2 \times k_{\text{piece}}$ mảnh mã hóa hạt giống (trong đó $k_{\text{piece}}$ mảnh cho Primary Node và $k_{\text{piece}}$ mảnh cho Backup Node).
3. **Unicast Seeding & Backup Routing:** Gửi $k_{\text{piece}}$ mảnh hạt giống trực tiếp tới **Primary Store Node** (đáp ứng đúng chỉ số hàng $r \pmod{\text{len}(\text{colPeers})}$) và $k_{\text{piece}}$ mảnh tới **Backup Store Node** (node kế tiếp trong danh sách hoạt động của cột mạng). Việc này đảm bảo tính dự phòng cao và đẩy nhanh tốc độ lan truyền dữ liệu mà không bị hardcode.



---

### 3. Store Node (Nút Lưu Trữ P2P)

Các Store Node thuộc Custody Cell $[r, c]$ tiếp nhận dữ liệu theo quy trình **2 tầng xác thực & mã hóa lại (Recoding)** tương ứng với luồng truyền từ Bootstrap Node:

#### ⚡ 1. Tiếp nhận & Anchor Tập Cam kết (Tương thích Phase 1 Bootstrap Node)
Khi nhận được tập $k$ cam kết mảnh $(C_{c, 0} \dots C_{c, k-1})$ kèm Merkle Proof phát tán từ GossipSub toàn Cột $c$:
1. **Kiểm tra Lớp 1 (Merkle Path):** Verify Merkle Proof của tập $C_{c, j}$ với `commits_root` trên Header (đảm bảo $C_j$ chính chủ $100\%$).
2. **Kiểm tra Lớp 2 (Fiat-Shamir Consistency):** Sử dụng Vector $x$ toàn cục từ Header công khai, xác nhận tính nhất quán đại số:
   $$\sum_{j=0}^{k-1} x_j \cdot C_{c, j} \stackrel{?}{=} C^{\text{col}}_c$$
* *Kết quả:* Sau khi verify thành công, Store Node lưu cố định (anchor) tập $C_{c, j}$ vào bộ nhớ để phục vụ xác thực các mảnh mã hóa RLNC về sau.

---

#### ⚙️ 2. Tiếp nhận, Xác thực & Recode Mảnh RLNC (Tương thích Phase 2 Bootstrap Node)
Khi nhận các mảnh mã hóa RLNC $(d, g, P)$ (từ Unicast Seeding của Bootstrap Node hoặc từ GossipSub của các Store Node khác):

1. **Xác thực Mảnh Mã hóa RLNC (KZG Pairing Check):**
   * Tính cam kết kết hợp theo vector hệ số mã hóa $g$: 
     $$C_{\text{combined}} = \sum_{j=0}^{k-1} g_j \cdot C_{c, j}$$
   * Gọi hàm kiểm tra KZG Pairing tại hàng $r$: 
     $$\text{Verify}(C_{\text{combined}}, r, d, P) \stackrel{?}{=} \text{true}$$

2. **Lọc Rank & Đảm bảo Độc lập Tuyến tính:**
   * Đưa vector hệ số $g$ vào ma trận hệ số hiện có của Cell $[r, c]$ và chạy thuật toán khử Gauss.
   * *Nếu Rank không tăng* (mảnh bị trùng lặp tuyến tính) $\rightarrow$ Drop để tránh lãng phí bộ nhớ và băng thông.

3. **Mã hóa lại (P2P Recoding) & Phát tán GossipSub:**
   * Ngay khi tích lũy đủ $m \ge 2$ mảnh hợp lệ độc lập tuyến tính (được bảo đảm khi nhận đủ mảnh từ Bootstrap Node hoặc qua GossipSub/Active Pull), Store Node sinh ngẫu nhiên các hệ số $\beta_1, \dots, \beta_m \in \mathbb{F}_r$ để tạo mảnh mã hóa mới:
     $$d_{\text{new}} = \sum_{i=1}^{m} \beta_i \cdot d_i, \quad g_{\text{new}} = \sum_{i=1}^{m} \beta_i \cdot g_i, \quad P_{\text{new}} = \sum_{i=1}^{m} \beta_i \cdot P_i$$
   * **Lưu trữ & Chuyển tiếp:** Lưu bộ mảnh mới $(d_{\text{new}}, g_{\text{new}}, P_{\text{new}})$ vào DB cục bộ (BadgerDB/LevelDB) và GossipSub tới tất cả các cột dữ liệu trong tầm custody (tất cả các topic GossipSub cột tương ứng từ `startCol` đến `endCol - 1` mà store node đăng ký lắng nghe).





---

### 4. Light Node / Verifier Node (Nút Lấy Mẫu DAS)

#### Quy trình xử lý:

1. **Tải Block Header nhẹ ($\approx 13 \text{ KB}$):** Đã sở hữu sẵn toàn bộ bảng $N$ cam kết cột $C^{\text{col}}_c$ và Vector $x$ toàn cục.
2. **Gửi truy vấn Sampling (DAS GET Request):** Bốc ngẫu nhiên tọa độ Cell $[r_1, c_1]$, gửi request hỏi Store Nodes thuộc ô $[r_1, c_1]$.


3. **Thu thập & Giải mã Gaussian Cục bộ:**
* Gom đủ $k$ mảnh RLNC $(d'_i, g'_i, P'_i)$ độc lập tuyến tính.


* Lập ma trận $A = [g'_1; \dots; g'_k]$, giải hệ Gauss trên $\mathbb{F}_r$: $S = A^{-1} \cdot d'$ và $\Pi = A^{-1} \cdot P'$.




4. **Xác thực Trực tiếp O(1) với Header:**
* **Verify từng mảnh:** $\text{Verify}(C_{c_1, j}, r_1, S_j, \Pi_j) == \text{true} \quad \forall j \in [0, k-1]$.


* **Verify tổ hợp với Header:** Dùng trực tiếp $C^{\text{col}}_{c_1}$ và Vector $x$ có sẵn trên Header:

$$\text{Verify}\left(C^{\text{col}}_{c_1}, r_1, \sum_{j=0}^{k-1} x_j \cdot S_j, \sum_{j=0}^{k-1} x_j \cdot \Pi_j\right) == \text{true}$$



* *Không cần tải thêm Merkle Proof hay tập $C_j$ khi lấy mẫu, tối ưu tốc độ tối đa.*


5. **Fallback:** Nếu Cell $[r_1, c_1]$ bị timeout, truy vấn trực tiếp Bootstrap Nodes của Cột $c_1$.



---

## III. MA TRẬN BẢO MẬT & HIỆU NĂNG CỦA THIẾT KẾ HOÀN CHỈNH

| Tiêu chí | Trạng thái thiết kế hoàn chỉnh | Ý nghĩa kỹ thuật |
| --- | --- | --- |
| **Dung lượng Header** | **Siêu nhẹ ($\approx 13 \text{ KB}$)** | Chứa `commits_root` (32B), $N$ điểm $C^{\text{col}}$ ($12.28\text{KB}$), và Vector $x$ ($512\text{B}$). Giảm $>93\%$ so với truyền $N \times k$ Commitments gốc.
| **Cấu trúc Commitments** | **Đồng nhất Mật mã 100%** | `commits_root` vừa cố định cấu trúc $N \times k$ cam kết $C_{c, j}$, vừa chính là tiền ảnh sinh ra Vector $x$. |
| **Lọc DoS P2P Tốc độ cao** | **Bảo vệ Lớp 1 (Merkle)** | Bootstrap/Store Node verify `commits_root` bằng Hash trong vài micro-giây trước khi chạy KZG Pairing đắt đỏ. |
| **Bảo vệ Cam kết Mảnh** | **Fiat-Shamir Toàn cục** | Bootstrap/Store Node không thể tráo đổi $C_j$ vì $x = \text{Hash}(C_{\text{all}})$. |
| **Hiệu năng Light Node (DAS)** | **Tối ưu O(1)** | Light Node chỉ cần Header $\approx 13\text{KB}$ là tự Verify mảnh rút mẫu trực tiếp, không phình dung lượng do Merkle Proof.
| **Khả năng Mở rộng CPU** | **FK20 Acceleration** | Publisher chỉ tạo Commitment. Đẩy việc tính $N \times k$ Proofs xuống Bootstrap Node.

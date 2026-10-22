# Thuật Ngữ Kỹ Thuật (CDA Network Glossary)

Tài liệu này là nguồn chuẩn hóa duy nhất (Single Source of Truth) định nghĩa các khái niệm, ký hiệu toán học và tham số cấu trúc trong toàn bộ kiến trúc CDA Network.

---

## 1. Đơn Vị Dữ Liệu & Cấu Trúc Ma Trận

### Cell (Ô Dữ Liệu)
- **Định nghĩa**: Đơn vị dữ liệu nguyên tử cơ bản trong ma trận dữ liệu của CDA Network.
- **Kích thước chuẩn**: $512\text{ bytes}$ (`cellSize = 512`).
- **Vị trí**: Mỗi phần tử tại tọa độ hàng $r$ và cột $c$ trong ma trận ODS/EDS được gọi là một Cell ($[r, c]$).

### Fragment (Mảnh Con Của Cell)
- **Định nghĩa**: Một phần con nhỏ được chia đều từ một Cell để làm đầu vào cho phép mã hóa mạng tuyến tính ngẫu nhiên (RLNC).
- **Kích thước**: 
  $$\text{Fragment Size} = \frac{\text{Cell Size}}{k_{\text{piece}}}$$
  *(Với $\text{Cell Size} = 512\text{ B}$ và $k_{\text{piece}} = 4$, $\text{Fragment Size} = 128\text{ bytes}$).*
- **Số lượng**: Đúng $k_{\text{piece}}$ fragments $S_0, S_1, \dots, S_{k_{\text{piece}}-1}$ tạo nên một Cell hoàn chỉnh.

### RLNC Piece (Mảnh Mã Hóa Tuyến Tính)
- **Định nghĩa**: Gói dữ liệu mã hóa được sinh ra từ tổ hợp tuyến tính ngẫu nhiên của các fragments trong cùng một Cell:
  $$d_i = \sum_{j=0}^{k_{\text{piece}}-1} g_{i,j} \cdot S_j \pmod{\text{Field}}$$
  trong đó $g_{i,j}$ là các hệ số ngẫu nhiên thuộc trường hữu hạn $\mathbb{F}_r$, $S_j$ là fragment thứ $j$.
- **Kích thước payload**: Bằng kích thước của 1 Fragment ($128\text{ bytes}$ khi $k_{\text{piece}} = 4$).
- **Đặc tính**: Các Store Nodes có thể thực hiện tái mã hóa (Recoding) các pieces mà không cần giải mã ngược về Cell gốc.

### ODS (Original Data Square)
- **Định nghĩa**: Ma trận vuông dữ liệu gốc chứa toàn bộ các giao dịch/blob dữ liệu của một khối.
- **Kích thước**: $K \times K$ cells ($K$ hàng $\times K$ cột).
- **Dung lượng ODS**: $K^2 \times \text{Cell Size}$ (ví dụ: $K=32 \implies 1,024 \times 512\text{B} = 512\text{ KB}$).

### EDS (Extended Data Square)
- **Định nghĩa**: Ma trận vuông dữ liệu mở rộng sau khi áp dụng mã hóa xóa 2D Reed-Solomon (RSMT-2D) lên ma trận ODS.
- **Kích thước**: $2K \times 2K$ cells (gồm $N = 2K$ hàng và $N = 2K$ cột).
- **Tỷ lệ mở rộng**: $4\times$ số lượng cells so với ODS (tỷ lệ mã hóa 1/2 theo chiều ngang và 1/2 theo chiều dọc).

---

## 2. Các Tham Số Cấu Hình Cốt Lõi

| Tham Số | Ký Hiệu | Ý Nghĩa Kỹ Thuật | Giá Trị Chuẩn |
| :--- | :--- | :--- | :--- |
| **ODS Dimension** | $K$ | Số hàng và số cột của ma trận dữ liệu gốc ODS. Tự động co giãn theo dung lượng khối (8, 16, 32, 64). | `8` – `64` |
| **EDS Dimension** | $N$ | Số hàng và số cột của ma trận mở rộng EDS ($N = 2K$). | $2K$ |
| **RLNC Degree** | $k_{\text{piece}}$ | **Số fragments cấu thành 1 Cell**, đồng thời là **hạng tuyến tính (Rank) tối thiểu** của tập hợp các mảnh RLNC cần thiết để giải mã khôi phục lại Cell gốc. | `4` |
| **Cell Size** | - | Dung lượng dữ liệu của 1 cell ma trận. | `512 bytes` |
| **Network Columns** | $C_{\text{net}}$ | Số lượng cột mạng logic phân tán độc lập trên mạng P2P (`--net-cols`). | `8` |
| **Network Rows** | $R_{\text{net}}$ | Số lượng hàng mạng logic (ô mạng / Network Cells) trong mỗi cột mạng (`--net-rows`). | `4` – `8` |
| **Nodes per Cell** | $M$ | Số lượng Custody Store Nodes cùng tham gia bảo chứng trong một ô mạng $(r_{\text{net}}, c_{\text{net}})$ (`--nodes-per-cell`, $M \ge 1$). | `1` – `3+` |

---

## 3. Quy Trình Lưu Trữ, Kênh GossipSub Kép & Đồng Thuận DA

### Network Matrix Cell (Ô Mạng Lưu Trữ)
- Không gian mạng P2P được tổ chức thành ma trận 2 chiều $R_{\text{net}} \times C_{\text{net}}$.
- Mỗi ô dữ liệu $[r, c]$ trong ma trận EDS ($2K \times 2K$) được ánh xạ về ô mạng $(r_{\text{net}}, c_{\text{net}})$ với:
  $$r_{\text{net}} = r \bmod R_{\text{net}}, \qquad c_{\text{net}} = \left\lfloor \frac{c}{2K / C_{\text{net}}} \right\rfloor$$
- Một ô mạng $(r_{\text{net}}, c_{\text{net}})$ có thể chứa $M \ge 1$ Store Nodes cùng đóng vai trò **Custody Nodes** (mỗi node có `PeerID` và khóa Ed25519 riêng biệt).
- Nếu một ô mạng bị trống ($M = 0$) hoặc chỉ còn $1$ node duy nhất ($M = 1$) do node rời mạng/sự cố, hệ thống tự động mượn **1 Borrowed Backup Custody Node** từ ô mạng kế cận $((r_{\text{net}} + 1) \bmod R_{\text{net}})$ trong cùng cột thông qua bảng định tuyến động `row_custody_map`.

### Dual-Channel Row GossipSub (`TopicCellRowSeed` & `TopicCellRowRecode`)
Mỗi hàng mạng $r_{\text{net}}$ trong cột $c_{\text{net}}$ sử dụng **2 kênh GossipSub tách biệt**:
1. **Kênh Seed (`TopicCellRowSeed`) — `/cda/1.0.0/col/<netCol>/row/<netRow>/seed`**:
   - Dành riêng cho **Bootstrap Node** phát tán bể hạt giống (Seed Pool) gồm $S_{\text{pool}} = 2 \cdot k_{\text{piece}}$ mảnh RLNC cho mỗi ô dữ liệu $[r, c]$ thuộc hàng mạng $r_{\text{net}}$.
   - **Chỉ các Custody Store Nodes** được phân công bảo chứng cho hàng $r_{\text{net}}$ (bao gồm cả Native Custody và Borrowed Backup) mới subscribe vào kênh này.
   - **Lấy mẫu ngẫu nhiên (Randomized Seed Sub-Sampling)**: Từ bể $2 \cdot k_{\text{piece}}$ mảnh trên kênh Seed, mỗi Custody Node tự lấy ngẫu nhiên (shuffle theo `PeerID`, `blockID`, `row`, `col`) một tập con gồm:
     $$S_{\text{node}} = \max\left(2, \left\lceil \frac{2 \cdot k_{\text{piece}}}{M} \right\rceil\right)$$
     mảnh để tránh trùng lặp bộ seed giữa các node trong cùng ô mạng.
2. **Kênh Recode (`TopicCellRowRecode`) — `/cda/1.0.0/col/<netCol>/row/<netRow>/recode`**:
   - Dành cho các **Custody Store Nodes** phát tán các mảnh đã tái mã hóa (Recoded RLNC Pieces).
   - **Toàn bộ các Store Nodes trong cột $c_{\text{net}}$** (cả Custody Nodes cùng ô mạng lẫn Non-Custody Nodes ở các hàng mạng khác) đều subscribe vào kênh này.
   - Mỗi Custody Node phát tán một hạn ngạch (Quota) gồm:
     $$Q = \max\left(1, \left\lceil \frac{k_{\text{piece}}}{M} \right\rceil\right) + 1$$
     mảnh recode lên kênh `TopicCellRowRecode`.

### Custody Storage, Borrowed Backup Pruning & Non-Custody Re-Mixing
- **Native Custody Nodes (Full Rank Quorum)**:
  Thu thập mảnh từ `TopicCellRowSeed` và trao đổi mảnh recode với các node cùng ô trên `TopicCellRowRecode` cho đến khi tích lũy đủ **Full Rank**:
  $$\text{Rank}(\text{Stored Pieces for } [r, c]) \ge k_{\text{piece}}$$
  và giữ nguyên $k_{\text{piece}}$ mảnh độc lập tuyến tính để lưu trữ dài hạn.
- **Borrowed Backup Custody Nodes (Immediate Pruning)**:
  Tham gia nhận seed và phát đủ hạn ngạch $Q$ mảnh recode lên `TopicCellRowRecode`, sau đó **cắt tỉa (prune) ngay lập tức** các mảnh thô về đúng **1 mảnh recode** (trừ trường hợp cột mạng quá thưa với `activePeersInCol <= kPiece` thì giữ nguyên $k_{\text{piece}}$ mảnh để đảm bảo khả năng khôi phục cột).
- **Non-Custody Store Nodes (Hierarchical RLNC Re-Mixing)**:
  Chỉ lắng nghe trên `TopicCellRowRecode`. Khi thu thập được $\ge 2$ mảnh độc lập tuyến tính từ $\ge 2$ nguồn (`minSources >= 2`), node thực hiện **Recode lần 2** (`RecodePiecesWithVerify`) để sinh ra **1 mảnh RLNC mới** có độ độc lập tuyến tính cao hơn rồi khóa ô (`nonCustodyLocked`) và xóa mảnh tạm. Nếu hết thời gian chờ ngắn mà chỉ nhận được 1 mảnh, node lưu trực tiếp 1 mảnh đó và khóa ô để không làm chậm tiến độ hoàn thành khối.

### CDA BlockReady
- Tín hiệu sự kiện phát ra từ CDA Publisher / Bootstrap Relay khi toàn bộ các hàng mạng `0 .. NetRows-1` trong các cột mạng hoạt động đã có ít nhất một Custody Store Node hoàn thành lưu ký (`StoreReady`) cho khối.
- Đánh dấu khối đã sẵn sàng khả dụng dữ liệu để các Light Clients bắt đầu lấy mẫu kiểm chứng.

### DAS (Data Availability Sampling)
- Quy trình kiểm tra tính sẵn sàng dữ liệu của Light Clients bằng cách truy vấn ngẫu nhiên một số lượng nhỏ các cells trong ma trận EDS qua mạng P2P.
- Đảm bảo xác suất phát hiện gian lận khối $\ge 99.99\%$ chỉ với một số lượng mẫu $O(1)$ truy vấn.

### KZG Commitment & FK20 Proofs
- **KZG Commitment**: Đa thức cam kết mật mã học cho mỗi cột của ma trận EDS trên đường cong elip BLS12-381.
- **FK20 Multi-Proof**: Thuật toán Feist-Khovratovich tính toán đồng thời tất cả các opening proofs cho toàn bộ các phần tử trong cột với độ phức tạp $O(N \log N)$ thay vì $O(N^2)$.

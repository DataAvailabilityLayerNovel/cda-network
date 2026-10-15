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
| **Network Columns** | $C_{\text{net}}$ | Số lượng cột mạng logic phân tán độc lập trên mạng P2P. | `8` |
| **Stores per Column** | $S$ | Số lượng Store Nodes lưu trữ bảo chứng (custody) trong mỗi cột. | `8` |

---

## 3. Quy Trình Lưu Trữ & Đồng Thuận DA

### Custody Storage (Lưu Trữ Bảo Chứng)
- Một Store Node trong cột $c$ được phân công bảo chứng lưu trữ một tập hợp các ô dữ liệu $[r, c]$.
- **Ngưỡng hoàn thành lưu ký (Full Rank Quorum)**:
  Store Node chỉ được xem là hoàn thành lưu trữ (Custody Complete) cho ô $[r, c]$ khi và chỉ khi:
  $$\text{Rank}(\text{Stored Pieces for } [r, c]) \ge k_{\text{piece}}$$
  Nghĩa là node đã lưu trữ ít nhất $k_{\text{piece}}$ mảnh RLNC độc lập tuyến tính với nhau.

### CDA BlockReady
- Tín hiệu sự kiện phát ra từ CDA Publisher / Bootstrap Relay khi toàn bộ các Store Nodes trong mạng đã đạt 100% quorum lưu ký custody cho khối.
- Đánh dấu khối đã sẵn sàng khả dụng dữ liệu để các Light Clients bắt đầu lấy mẫu kiểm chứng.

### DAS (Data Availability Sampling)
- Quy trình kiểm tra tính sẵn sàng dữ liệu của Light Clients bằng cách truy vấn ngẫu nhiên một số lượng nhỏ các cells trong ma trận EDS qua mạng P2P.
- Đảm bảo xác suất phát hiện gian lận khối $\ge 99.99\%$ chỉ với một số lượng mẫu $O(1)$ truy vấn.

### KZG Commitment & FK20 Proofs
- **KZG Commitment**: Đa thức cam kết mật mã học cho mỗi cột của ma trận EDS trên đường cong elip BLS12-381.
- **FK20 Multi-Proof**: Thuật toán Feist-Khovratovich tính toán đồng thời tất cả các opening proofs cho toàn bộ các phần tử trong cột với độ phức tạp $O(N \log N)$ thay vì $O(N^2)$.

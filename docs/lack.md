# Hạn Chế Hệ Thống & Các Định Hướng Chỉnh Sửa, Nâng Cấp (CDA Network)

Tài liệu này ghi lại các hạn chế kỹ thuật hiện tại của hệ thống nguyên mẫu (prototype) CDA Network và đề xuất các chỉnh sửa, tối ưu hóa cần thiết trước khi đưa vào môi trường chạy thực tế (production).

---

## 1. Các Hạn Chế Kỹ Thuật Hiện Tại

### A. Mạng Giao Tiếp P2P Chỉ Là Giả Lập (Simulated HTTP P2P)
*   **Hạn chế:** Hệ thống hiện tại đang sử dụng các giao thức REST HTTP (POST cho GossipSub và GET cho Peer Pull) để giả lập mạng ngang hàng P2P.
*   **Hệ quả:** 
    *   Không phản ánh được các vấn đề thực tế như: độ trễ mạng (network latency), mất gói tin, tường lửa (NAT traversal), và tình trạng mất kết nối đột ngột của node (peer churn).
    *   Không có cơ chế quản lý vòng đời kết nối an toàn.

### B. Danh Sách Peer Tĩnh & Thiếu Cơ Chế Tự Phát Hiện (Static Peer Discovery)
*   **Hạn chế:** Các Store Node phải khai báo thủ công danh sách các peer lân cận thông qua tham số dòng lệnh `-peers`.
*   **Hệ quả:** Hệ thống thiếu tính linh hoạt. Khi một Store Node mới tham gia mạng hoặc một node cũ bị sập, các node còn lại không thể tự động cập nhật danh sách kết nối dẫn tới hiệu suất kéo mảnh (pull retrieval) bị suy giảm.

### C. Giả Định Kích Thước Cell Cố Định (Fixed Cell Size Assumption)
*   **Hạn chế:** Trong file [receiver.go](file:///home/ubuntu/cda-network/cda-store-node/internal/p2p/receiver.go), kích thước mảnh gốc (`pieceSize`) được tính toán dựa trên giả định kích thước cell mặc định là 64 bytes (`pieceSize := 64 / rcv.k`).
*   **Hệ quả:** Nếu kích thước cell trong ma trận ODS thay đổi (ví dụ: khối dữ liệu lớn hơn với cell 128-byte hoặc 256-byte), cơ chế khôi phục dữ liệu sẽ trả về kết quả sai hoặc bị lỗi cắt mảnh không chính xác.

### D. Cơ Chế Bảo Vệ Tránh Vòng Lặp Truy Vấn Đơn Giản (Simple Query Loop Prevention)
*   **Hạn chế:** Cơ chế ngăn ngừa vòng lặp khi kéo mảnh hiện tại chỉ dựa vào cờ `?remote=true` ở query parameter của HTTP request.
*   **Hệ quả:** Trong một mạng lưới liên kết phức tạp với nhiều node chéo nhau, cơ chế này chỉ ngăn được vòng lặp trực tiếp cấp 1 (direct loop) nhưng không ngăn được các vòng lặp đa nhánh sâu hơn nếu các node chuyển tiếp yêu cầu đi vòng qua các peer khác.

### E. Quản Lý Trạng Thái Lưu Trữ Trong Bộ Nhớ Tạm (In-Memory Storage Only)
*   **Hạn chế:** Custody store của Store Node hiện đang lưu trữ hoàn toàn trên RAM (`in-memory maps` trong [custody.go](file:///home/ubuntu/cda-network/cda-store-node/internal/storage/custody.go)).
*   **Hệ quả:** Khi Store Node bị khởi động lại, toàn bộ dữ liệu custody, các mảnh hạt giống và cam kết đã neo giữ sẽ bị mất hoàn toàn, yêu cầu node phải đồng bộ lại từ đầu từ Bootstrap Node.

---

## 2. Các Đề Xuất Chỉnh Sửa & Nâng Cấp

| Thành phần | Hành vi hiện tại | Giải pháp nâng cấp / Chỉnh sửa |
| :--- | :--- | :--- |
| **Giao thức P2P** | HTTP API client/server giả lập | Tích hợp thư viện **`libp2p`** chính thức của Go, sử dụng giao thức Gossipsub để truyền tin và Kademlia DHT để tự động phát hiện node (Peer Discovery). |
| **Bảo mật mảnh dữ liệu** | Không kiểm tra chéo sau giải mã | Sau khi dùng phép khử Gauss để giải mã phục hồi ô dữ liệu, node cần thực hiện cam kết lại dữ liệu đó và so khớp chéo với KZG commitment tương ứng để đảm bảo dữ liệu phục hồi hoàn toàn chính xác. |
| **Cấu trúc dữ liệu mảnh** | Đóng gói byte trần | Thêm metadata vào cấu trúc gói tin gửi đi (bao gồm kích thước cell gốc, tọa độ khối dữ liệu, và danh sách các node trung gian đã đi qua để ngăn chặn vòng lặp định tuyến). |
| **Cơ sở dữ liệu** | Lưu trữ trên RAM (in-memory) | Tích hợp cơ sở dữ liệu khóa-giá trị gọn nhẹ như **`BadgerDB`** hoặc **`LevelDB`** để lưu trữ lâu dài (persistence) dữ liệu custody và cam kết. |
| **Mã hóa hệ số** | Chọn ngẫu nhiên trần $[1, 10]$ | Xây dựng thuật toán tạo số ngẫu nhiên giả mã nguồn (CSPRNG) để tạo hệ số RLNC phân tán tối ưu trong trường $\mathbb{F}_r$, loại bỏ hoàn toàn khả năng trùng lặp hệ số giữa các node khác nhau trên cùng một cột. |
| **Phép toán trường Fr** | Chạy đơn luồng tuần tự | Sử dụng đa luồng (multi-threading) và tập lệnh SIMD (AVX2/AVX-512) để tối ưu hóa hiệu năng của phép giải mã Gauss và nhân ma trận trên trường $\mathbb{F}_r$. |

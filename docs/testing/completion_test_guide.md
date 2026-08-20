# Hướng Dẫn Kịch Bản Kiểm Thử Completion Logging

Tài liệu này hướng dẫn cách chạy kịch bản kiểm thử để kiểm chứng tính năng tự động ghi nhận trạng thái **IsComplete** (khi một store node thu thập đầy đủ các mảnh custody cần thiết cho một block) ra file log vật lý.

---

## 1. Cơ Chế Hoạt Động
Mỗi khi Store Node nhận được mảnh dữ liệu thô (từ Bootstrap Node) hoặc tự động tái cấu trúc mảnh thành công (từ quá trình Active Pull / Gossip), nó sẽ chạy hàm kiểm tra trạng thái hoàn thành custody cho block tương ứng:
* Nếu **đầy đủ** số lượng mảnh cần thiết cho tất cả các ô dữ liệu thuộc quyền custody của mình trong cột, node chuyển trạng thái thành **IsComplete**.
* Node sẽ tự động ghi một dòng log chứa mốc thời gian, chiều cao block (`height`) và Block ID vào file log cục bộ tại đường dẫn trên host: `data/store_<port>/completion.log`.

---

## 2. Kịch Bản Kiểm Thử Từng Bước

### Bước 1: Khởi động mạng lưới CDA Node
Tạo cấu hình Docker Compose giả lập với 8 cột mạng, 8 store node mỗi cột, và kích hoạt tính năng dọn dẹp (pruning):

```bash
# Dọn dẹp 
bash scripts/cleanup.sh

# Cách 1: Khởi động ĐẦY ĐỦ 8 Cột Mạng (Khuyến nghị để DAS toàn bộ ma trận ?all=true)
# python3 scripts/generate_compose.py --cols 8 --stores-per-col 8 --lights 1 --k 16 --k-piece 4 --prune-enable --prune-ttl 15s

# Cách 2: Khởi động Rút Gọn 1 Cột Mạng (Tiết kiệm tài nguyên máy, chỉ test riêng Cột 0)
python3 scripts/generate_compose.py --cols 8 --active-cols 1 --stores-per-col 8 --lights 1 --k 16 --k-piece 4 --prune-enable --prune-ttl 15s

# Build và chạy mạng lưới trong nền
docker compose -f docker-compose.json up -d --build
```

### Bước 2: Chạy Bot đẩy dữ liệu Event-Driven
Chạy bot đẩy dữ liệu event-driven để xuất bản 3 block (bot tự động chờ tín hiệu `BlockReady` từ Publisher/Bootstrap trước khi đẩy block tiếp theo):

```bash
python3 scripts/block_publisher_bot.py --interval 0 --k 16 --count 3 --start-height 1
```

### Bước 2.5: Cơ Chế Reactive Auto-DAS dựa trên Tín hiệu BlockReady
Light Node hiện được tích hợp cơ chế **Auto-DAS theo sự kiện `BlockReady`**:
- Khi tất cả $S = \text{storesPerCol}$ Store Node trong toàn bộ các cột active hoàn thành 100% custody, Publisher phát tín hiệu **`BlockReady`** trên kênh GossipSub `/cda/1.0.0/block-ready` (Bootstrap Node làm relay).
- Light Node nhận tín hiệu `BlockReady` và **tự động kích hoạt luồng lấy mẫu DAS ngay lập tức** cho block đó.
- Toàn bộ kết quả xác thực đại số được tự động ghi nhận trực tiếp theo thời gian thực vào:
  ```bash
  cat data/light_9401/das_success.log
  ```
- Hoặc bạn có thể xem trực tiếp log sự kiện của Light Node qua Docker:
  ```bash
  docker compose -f docker-compose.json logs -f light-1 | grep "Auto-DAS"
  ```
  *Log mẫu thời gian thực khi có block mới:*
  ```text
  [Auto-DAS] [Height: 1] [LightNode] BlockReady signal received for block-1 — enqueuing for DAS
  [Auto-DAS] [Height: 1] 🚀 BlockReady triggered DAS for block-1 — sampling 256/1024 active cells (25%)...
  ```



### Bước 3: Theo dõi qua log console (Stdout)
Bạn có thể xem log Stdout của các Store Node để kiểm tra xem họ đã thông báo đạt trạng thái hoàn thành chưa:
```bash
docker compose -f docker-compose.json logs store-0-1 | grep "reached IsComplete status"
```
*Kết quả mẫu đầu ra:*
```
store-0-1-1  | 2026/08/07 02:35:50 [Height: 1] [StoreNode] Column registry reached IsComplete status successfully. Logged to data/store_9300/completion.log.
```

### Bước 4: Kiểm tra File Log trên ổ đĩa Host
Sau khi Bot gửi thành công các block, bạn có thể kiểm tra trực tiếp nội dung file log completion được lưu trữ trên máy chủ host:

```bash
# Kiểm tra file log của Store Node chạy tại port 9300
cat data/store_9300/completion.log
```

*Nội dung file log mẫu sẽ trông như sau:*
```
[2026-08-07 02:35:50] [Height: 1] Block block-1: Column registry reached IsComplete status successfully.
[2026-08-07 02:36:00] [Height: 2] Block block-2: Column registry reached IsComplete status successfully.
[2026-08-07 02:36:10] [Height: 3] Block block-3: Column registry reached IsComplete status successfully.
```

### Bước 5: Kiểm tra File Log DAS thành công của Light Node
Mỗi khi Light Node lấy mẫu và xác thực đại số thành công cho một ô dữ liệu, thông tin chi tiết sẽ được ghi nhận vào `das_success.log` trên host:

```bash
# Kiểm tra file log của Light Node chạy tại port 9401
cat data/light_9401/das_success.log
```

*Nội dung log mẫu ghi nhận thành công:*
```
[2026-08-07 02:56:18] [Height: 1] Block block-1, Cell [9, 31]: DAS sampling verification succeeded. Reconstructed cell: 30303030...
```

---

## 3. Dọn Dẹp Tài Nguyên Sau Khi Test
Sau khi hoàn thành kiểm tra, chạy lệnh sau để dọn dẹp các container và các file database thô tránh ghi đè xung đột ở các lần test sau:

```bash
bash scripts/cleanup.sh
```

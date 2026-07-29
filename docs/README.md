# Hướng dẫn chạy và sử dụng CDA Network với Docker

Tài liệu này hướng dẫn cách sử dụng kịch bản triển khai mạng động (dynamic container orchestration) cho CDA Network bằng Docker.

## Yêu cầu hệ thống
1. Đã cài đặt **Docker** và **Docker Compose**.
2. Đã cài đặt **Python 3**.

---

## 1. Triển khai mạng bằng cấu hình mặc định (Ma trận 4x4)

Theo mặc định, kịch bản kiểm thử sẽ triển khai 1 mạng giả lập với các thành phần sau:
- **1 Publisher Node** (Node xuất bản block)
- **4 Bootstrap Nodes** (Mỗi node đại diện cho 1 cột mạng, quản lý việc khám phá Store Nodes)
- **8 Store Nodes** (2 Store Nodes trên mỗi cột, lưu trữ các mảnh dữ liệu coded pieces)
- **2 Light Nodes** (Lấy mẫu ngẫu nhiên DAS)

Để chạy kịch bản mạng với các thông số mặc định này, chỉ cần thực thi:
```bash
./run_docker_test.sh
```

*(Trong kịch bản này, script sẽ tự động tạo cấu hình compose, xoá các state cũ, build image, chạy các node, và thực hiện E2E Testing bao gồm kiểm tra DAS và việc một node bị tắt ngẫu nhiên (Graceful leave)).*

---

## 2. Triển khai mạng với cấu hình tùy chỉnh (Khả năng co giãn động)

Bạn có thể thay đổi số lượng cột, số lượng Store Node trên mỗi cột và số lượng Light Node tùy ý. 
Kịch bản chạy chấp nhận 3 tham số (theo đúng thứ tự):
1. `COLS`: Số lượng cột mạng (số Bootstrap nodes).
2. `STORES_PER_COL`: Số lượng Store Node nằm trong một cột.
3. `LIGHTS`: Số lượng Light Node tham gia mạng.

**Cú pháp:**
```bash
./run_docker_test.sh <COLS> <STORES_PER_COL> <LIGHTS>
```

**Ví dụ:** Triển khai ma trận gồm 6 cột mạng, 3 store nodes trên mỗi cột, và 4 light nodes:
```bash
./run_docker_test.sh 6 3 4
```

---

## 3. Tạo cấu hình Docker Compose độc lập (Không chạy script test)

Nếu bạn chỉ muốn tạo file `docker-compose.json` để tự quản lý vòng đời container (thay vì chạy thông qua script test), bạn có thể gọi trực tiếp file Python:

```bash
python3 generate_compose.py --cols 4 --stores-per-col 2 --lights 2 --k 4
```

Lệnh này sẽ sinh ra file `docker-compose.json` ở thư mục hiện tại. Sau đó bạn có thể quản lý các node bằng Docker Compose truyền thống:
```bash
docker compose -f docker-compose.json up -d
docker compose -f docker-compose.json logs -f
docker compose -f docker-compose.json down -v
```

---

## 4. Dừng hệ thống và xóa tài nguyên
Nếu bạn đang chạy ngầm các node bằng Docker Compose thủ công, hãy dọn dẹp bằng lệnh sau để đảm bảo các trạng thái mạng cũ không làm gián đoạn bài test sau:
```bash
docker compose -f docker-compose.json down -v
```

---

## 5. Tương tác thủ công với mạng (Publish và DAS)

Khi mạng đã được dựng lên qua script hoặc docker compose (`docker compose -f docker-compose.json up -d`), bạn có thể chủ động đẩy block và chạy DAS mà không cần phải chạy script test tự động.

- **Đẩy dữ liệu lên mạng**: 
  ```bash
  ./publish.sh <block_id> <publisher_url>
  ```
  *(Ví dụ: `./publish.sh block-test-1 http://localhost:8080`)*

- **Lấy mẫu dữ liệu (DAS)**:
  ```bash
  ./das.sh <block_id> <light_node_url>
  ```
  *(Ví dụ: `./das.sh block-test-1 http://localhost:8095`)*

---

## 6. Chạy Bot Tự Động (Auto Publish & Auto DAS)

Để tự động hóa quá trình đẩy dữ liệu và kiểm tra lấy mẫu trên một mạng đang chạy, bạn có thể sử dụng 2 kịch bản Bot được cung cấp sẵn (chạy bằng 2 terminal khác nhau để dễ quan sát):

- **Bot Đẩy Dữ Liệu (`bot_publish.sh`)**:
  ```bash
  ./bot_publish.sh 5
  ```
  *(Số `5` là khoảng thời gian tính bằng giây giữa các lần đẩy block. Bot sẽ liên tục đẩy các block có tên `bot-block-1`, `bot-block-2`, v.v. lên mạng)*

- **Bot Lấy Mẫu DAS (`bot_das.sh`)**:
  ```bash
  ./bot_das.sh 5
  ```
  *(Bot sẽ đợi 3 giây trước khi bắt đầu để đảm bảo block đầu tiên đã được xử lý xong bởi mạng. Sau đó cứ mỗi `5` giây nó sẽ tự động chạy DAS đối chiếu với tên block tương ứng do Publish Bot tạo ra).*

---

## 7. Chế độ Crash-on-fail (Dành riêng cho Test)

Để phục vụ kiểm thử hệ thống chặt chẽ, các Node (Store, Bootstrap, Light) hỗ trợ một cờ (flag) `--crash-on-fail`.
Nếu cờ này được kích hoạt, bất cứ khi nào một Node phát hiện dữ liệu lỗi trong quá trình Verify (ví dụ: lỗi Merkle Proof, dữ liệu mâu thuẫn từ Publisher, hay quá trình tái tạo mảnh DAS thất bại), Node đó sẽ **chủ động gọi hàm tự hủy (`os.Exit(1)`) và tắt container**. 

Chế độ này giúp bạn dễ dàng theo dõi và bắt lỗi bằng cách kiểm tra các container thoát đột ngột (thay vì chỉ ghi log lỗi và tiếp tục hoạt động).

Để bật chế độ này khi tạo cấu hình Compose thủ công, hãy truyền thêm tham số vào script Python:
```bash
python3 generate_compose.py --cols 4 --stores-per-col 2 --lights 2 --k 4 --crash-on-fail
```
*(Script tự động `run_docker_test.sh` mặc định đã tự động bật cờ này để kiểm thử một cách nghiêm ngặt nhất).*

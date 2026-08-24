# Hướng Dẫn Sử Dụng CLI & Lệnh Curl REST API (CDA Network)

Tài liệu này cung cấp hướng dẫn đầy đủ về các giao tiếp HTTP REST API, Server-Sent Events (SSE) và các công cụ dòng lệnh (CLI/Shell/Python) được tích hợp sẵn trong **CDA Network**.

---

## 1. Tổng Quan Kiến Trúc API & CLI

CDA Network cung cấp giao tiếp HTTP REST API và CLI ở 4 loại Node chính trong hệ thống:
- **Publisher Node**: Tiếp nhận khối dữ liệu thô (ODS), mã hóa ma trận, tính toán cam kết KZG/Merkle và phát tán khối.
- **Light Node**: Tiếp nhận truy vấn kiểm định DAS (Data Availability Sampling), tự động lấy mẫu các ô dữ liệu qua P2P.
- **Bootstrap Node**: Quản lý định tuyến topology, cung cấp danh bạ peer, theo dõi chỉ số Prometheus và phát sự kiện `BlockReady` qua Server-Sent Events (SSE).
- **Store Node**: Lưu trữ mảnh dữ liệu custody phân tán, trả lời truy vấn DAS qua P2P stream và cung cấp HTTP API nội bộ để kiểm tra trạng thái lưu trữ custody (`/store/status`).

Ngoài các endpoint REST direct, dự án đi kèm bộ script tự động hóa trong thư mục `scripts/` hỗ trợ kiểm thử thủ công và tự động.

---

## 2. Publisher Node REST API

Mặc định Publisher Node lắng nghe tại cổng `http://localhost:8080`.

### 2.1. Đưa Khối Dữ Liệu Vào Hệ Thống (`POST /publish`)

Tiếp nhận mảng các ô dữ liệu ODS (hex strings $K \times K$), thực hiện mã hóa RS (RLNC-RSMT2D), sinh Merkle/KZG Proof, lưu trữ Header vào BadgerDB và phân phối các mảnh cột đến Bootstrap Nodes.

- **URL**: `http://localhost:8080/publish`
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`
- **Request Body Payload**:
  ```json
  {
    "block_id": "manual-block-1",
    "data": [
      "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "00000000000000000000000000000000000000000000AB0000000000000000000000000000000000000000000000000000000000000000000000000000000002"
    ],
    "signature": "optional_ed25519_hex_signature"
  }
  ```

#### Lệnh `curl` Mẫu:
```bash
curl -s -X POST -H "Content-Type: application/json" \
  -d '{
    "block_id": "manual-block-1",
    "data": [
      "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "00000000000000000000000000000000000000000000AB0000000000000000000000000000000000000000000000000000000000000000000000000000000002",
      "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003",
      "00000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000004"
    ]
  }' http://localhost:8080/publish
```

#### Response Trả Về (`200 OK`):
```json
{
  "block_id": "manual-block-1",
  "commits_root": "a1b2c3...",
  "column_comm": ["...", "..."],
  "coeffs": "..."
}
```

---

### 2.2. Truy Vấn Block Header (`GET /header/{block_id}`)

Lấy thông tin `BlockHeader` của một khối đã xuất bản.

- **URL**: `http://localhost:8080/header/{block_id}`
- **Method**: `GET`

#### Lệnh `curl` Mẫu:
```bash
curl -s http://localhost:8080/header/manual-block-1
```

---

## 3. Light Node REST API

Mặc định Light Node lắng nghe tại cổng `http://localhost:9401` (hoặc `http://localhost:8499`).

### 3.1. Lấy Mẫu Khả Dụng Dữ Liệu (`GET /das/sample/{block_id}`)

Light Node sẽ truy vấn thông tin định tuyến từ Bootstrap Node, sau đó kết nối trực tiếp đến các Store Node qua P2P stream để tải và xác thực bằng chứng toán học (KZG/RLNC proof).

#### Các Tham Số Query Parameter:
1. **Lấy Mẫu Toàn Bộ Ô Cột Hoạt Động (`all=true`)**:
   ```bash
   curl -s "http://localhost:9401/das/sample/manual-block-1?all=true"
   ```
2. **Lấy Mẫu 1 Ô Cụ Thể (`row={row}&col={col}`)**:
   ```bash
   curl -s "http://localhost:9401/das/sample/manual-block-1?row=0&col=2"
   ```
3. **Lấy Mẫu Ngẫu Nhiên $N$ Ô (`samples={n}`)**:
   ```bash
   curl -s "http://localhost:9401/das/sample/manual-block-1?samples=8"
   ```

#### Response Trả Về (`200 OK`):
```json
{
  "block_id": "manual-block-1",
  "success": true,
  "results": [
    {
      "row": 0,
      "col": 2,
      "verified": true,
      "cell_data": "00000000..."
    }
  ]
}
```

---

## 4. Bootstrap Node REST API & SSE Events

Mặc định Bootstrap Node lắng nghe tại cổng `http://localhost:9200` (hoặc `http://localhost:8200`).

### 4.1. Truy Vấn Danh Sách Peer Active (`GET /bootstrap/peers`)
Cung cấp thông tin các Store Node đang hoạt động trong cột mạng.

```bash
curl -s http://localhost:9200/bootstrap/peers
```

### 4.2. Kiểm Tra Sức Khỏe Node (`GET /health`)
```bash
curl -s http://localhost:9200/health
# Trả về: healthy (HTTP 200 OK)
```

### 4.3. Xuất Chỉ Số Prometheus (`GET /metrics`)
Expose các counter/histogram chỉ số đo lường hiệu năng:
```bash
curl -s http://localhost:9200/metrics | grep cda_
```

### 4.4. Lấy Tín Hiệu BlockReady Mới Nhất (`GET /block-ready/latest`)
Trả về JSON chứa thông tin khối đã sẵn sàng được lưu trữ hoàn toàn (100% custody) bởi cụm Store Node:
```bash
curl -s http://localhost:9200/block-ready/latest
```

### 4.5. Đăng Ký Luồng Sự Kiện Real-time SSE (`GET /events/block-ready`)
Đăng ký lắng nghe tín hiệu `BlockReady` theo thời gian thực (Server-Sent Events). Sử dụng cờ `-N` (`--no-buffer`) trong `curl`:

```bash
curl -N -s http://localhost:9200/events/block-ready
```

#### Ví dụ Output Luồng SSE:
```text
data: {"block_id":"block-1","height":1}

data: {"block_id":"block-2","height":2}
```

---

## 5. Store Node Internal HTTP Status API

Store Node chủ yếu hoạt động qua mạng giao thức **LibP2P stream** để nhận mảnh dữ liệu và phục vụ DAS. Tuy nhiên, Store Node cũng lắng nghe trên cổng HTTP nội bộ (mặc định cấu hình qua `cfg.Port` như `8081`, `8082`,...) cung cấp các endpoint kiểm tra trạng thái:

### 5.1. Truy Vấn Trạng Thái Lưu Trữ Custody Block (`GET /store/status/{block_id}`)
Trạng thái xem Store Node đã hoàn tất tiếp nhận đủ mảnh dữ liệu (`completed: true/false`) của một khối cụ thể hay chưa:
```bash
curl -s http://localhost:8081/store/status/manual-block-1
```
**Response mẫu (`200 OK`)**:
```json
{
  "block_id": "manual-block-1",
  "completed": true
}
```

### 5.2. Healthcheck & Metrics Endpoint
```bash
# Kiểm tra sức khỏe Store Node
curl -s http://localhost:8081/health

# Đo lường Prometheus Metrics của Store Node
curl -s http://localhost:8081/metrics
```

---

## 6. Bảng Tổng Hợp Công Cụ Dòng Lệnh (CLI Scripts)

Dự án cung cấp bộ tiện ích nằm trong thư mục [scripts/](file:///home/ubuntu/cda-network/scripts):

| Script / Tool | Cú Pháp Sử Dụng | Mô Tả Chức Năng |
| :--- | :--- | :--- |
| **`scripts/publish.sh`** | `bash scripts/publish.sh [BLOCK_ID] [PUBLISHER_URL]` | Xuất bản thủ công 1 block mẫu lên Publisher Node. |
| **`scripts/das.sh`** | `bash scripts/das.sh [BLOCK_ID] [LIGHT_URL]` | Thực hiện DAS kiểm thử toàn bộ ô (`all=true`) trên Light Node. |
| **`scripts/bot_das.sh`** | `bash scripts/bot_das.sh [INTERVAL] [START_HEIGHT]` | Bot tự động gọi DAS liên tục tăng dần chiều cao block. |
| **`scripts/block_publisher_bot.py`** | `python3 scripts/block_publisher_bot.py --k 32 --publisher http://localhost:8080 --bootstrap http://localhost:9200` | Bot đẩy khối tự động Event-Driven (lắng nghe SSE `/events/block-ready` từ Bootstrap trước khi đẩy khối tiếp theo). |

---

## 7. Kịch Bản Kiểm Thử Nhanh E2E Bằng Lệnh Curl

Dưới đây là chuỗi lệnh mẫu để khởi chạy vòng đời phát hành & kiểm định 1 block:

```bash
# Bước 1: Kiểm tra Bootstrap Node đang hoạt động
curl -s http://localhost:9200/health

# Bước 2: Xuất bản block-1 lên Publisher Node
./scripts/publish.sh block-1 http://localhost:8080

# Bước 3: Kiểm tra thông tin Header của block-1
curl -s http://localhost:8080/header/block-1 | jq .

# Bước 4: Thực hiện lấy mẫu DAS từ Light Node
./scripts/das.sh block-1 http://localhost:9401

# Bước 5: Truy vấn chỉ số Prometheus từ Bootstrap Node
curl -s http://localhost:9200/metrics | grep cda_
```

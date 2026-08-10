# Kế Hoạch Triển Khai, Kiểm Thử & Đo Lường Hệ Thống CDA Network

Tài liệu này vạch ra lộ trình đưa hệ thống CDA từ môi trường giả lập cục bộ lên mạng lưới phi tập trung thực tế, thiết lập các kịch bản kiểm thử tích hợp (E2E) nâng cao và xây dựng bộ công cụ đo lường hiệu năng tự động.

---

## 1. Kế Hoạch Triển Khai Hệ Thống (Deployment Plan)

Để vận hành hệ thống CDA trên môi trường Production phi tập trung, chúng ta cần chuyển dịch kiến trúc mạng lưới từ Docker Compose đơn máy chủ sang mô hình điều phối phân tán Multi-Node.

### A. Phân Bổ Kiến Trúc Vật Lý & Phần Cứng (Topology & Hardware Spec)

```mermaid
graph TD
    subgraph Layer L2 / Sequencer
        Publisher[Publisher Node]
    end
    
    subgraph Layer Mạng Định Tuyến (Routing Layer)
        B0[Bootstrap Node 0]
        B1[Bootstrap Node 1]
        B_N[Bootstrap Node N]
    end

    subgraph Layer Lưu Trữ Custody (Custody Layer)
        S0_1[Store Node 0-1]
        S0_2[Store Node 0-2]
        S1_1[Store Node 1-1]
        S1_2[Store Node 1-2]
    end

    subgraph Clients
        L1[Light Node 1]
        L2[Light Node 2]
    end

    Publisher -->|Cột EDS + Merkle Proof| B0
    Publisher -->|Cột EDS + Merkle Proof| B1
    Publisher -->|Cột EDS + Merkle Proof| B_N

    B0 -->|Seed RLNC Pieces| S0_1
    B0 -->|Seed RLNC Pieces| S0_2
    B1 -->|Seed RLNC Pieces| S1_1
    B1 -->|Seed RLNC Pieces| S1_2

    L1 -->|DAS Query| S0_1
    L2 -->|DAS Query| S1_2
```

Mỗi loại node được cấu hình với các thông số phần cứng tối ưu:

| Loại Node | Yêu Cầu CPU | RAM | Lưu Trữ | Yêu Cầu Đặc Biệt |
|---|---|---|---|---|
| **Publisher** | 8 Cores | 16 GB | 500 GB NVMe | Băng thông mạng cao (tối thiểu 1 Gbps) để phân phối cột. |
| **Bootstrap** | 16 Cores | 32 GB | 100 GB NVMe | Cần hỗ trợ tập lệnh AVX-512 hoặc GPU (NVIDIA CUDA) để tăng tốc độ sinh KZG Opening Proofs không đồng bộ. |
| **Store Node** | 4 Cores | 8 GB | 2 TB SSD | Tốc độ I/O đĩa cao để ghi và phục hồi mảnh (reconstruction) nhanh chóng. |
| **Light Node** | 2 Cores | 4 GB | 50 GB | Cấu hình gọn nhẹ, có thể chạy trên thiết bị người dùng cuối. |

### B. Chiến Lược Mạng Lưới & Khám Phá Node (P2P Networking & Discovery)

1. **DNS Bootstrap Seeds:**
   - Thay thế việc cấu hình cứng IP bằng cơ chế DNS Seed (ví dụ: `seed.cda-network.org`). Mạng lưới sẽ phân giải DNS này thành danh sách các Bootstrap Node đang hoạt động để các node mới kết nối ban đầu.
2. **Column-Based GossipSub Subnets:**
   - Sử dụng thư viện `libp2p/go-libp2p-pubsub` để tạo các kênh GossipSub có cấu trúc.
   - Mỗi cột EDS $c$ ($0 \le c < 2K$) sẽ tương ứng với một P2P topic `/cda/1.0.0/col/{c}`.
   - Các Store Node và Bootstrap Node chịu trách nhiệm lưu trữ cột đó sẽ chỉ subscribe vào topic tương ứng, giúp cô lập lưu lượng truyền tải và giảm thiểu băng thông tổng thể của mạng.
3. **Mã Hóa & Nhận Diện Verifiable Custody:**
   - Store Node chạy thuật toán `GenerateKeypairForCell` để brute-force sinh Private Key sao cho mã băm của PeerID trỏ đúng vào tọa độ hàng và cột được giao (Verifiable Address Mapping).
   - Cơ chế này giúp ngăn chặn việc Store Node giả mạo tọa độ để tránh nghĩa vụ lưu trữ dữ liệu thực tế.

---

## 2. Kịch Bản Kiểm Thử Hệ Thống (Test Scenarios & Resiliency)

Chúng tôi thiết kế 3 kịch bản kiểm thử tích hợp chính để đảm bảo hệ thống đạt độ tin cậy và hiệu năng theo đặc tả kỹ thuật:

### Kịch Bản 1: Kiểm Thử Khả Năng Chịu Lỗi Của Store Node (Store Node Resiliency & Failover)

*   **Mục tiêu:** Đảm bảo Light Node vẫn thực hiện thành công DAS sampling khi một số lượng Store Node nhất định trong cột gặp sự cố hoặc offline đột ngột.
*   **Các bước thực hiện:**
    1.  Triển khai mô hình mặc định: $K=16$, $k_{\text{piece}}=4$, 8 cột mạng, mỗi cột có 8 Store Node.
    2.  Publisher tiến hành đóng gói khối dữ liệu ODS $16 \times 16$ và phân phối thành công.
    3.  Tắt đột ngột 3 Store Node bất kỳ trong cùng một cột mạng (giả lập sự cố mất điện / lỗi đĩa hàng loạt).
    4.  Kích hoạt Light Node thực hiện DAS Sampling toàn bộ ma trận (Full Matrix DAS).
*   **Tiêu chuẩn đánh giá:**
    - Light Node khi quét DAS qua các cột bị mất node sẽ tự động kích hoạt **vòng lặp thử lại dự phòng (Fallback Loop)**.
    - Thời gian truy vấn có thể tăng nhẹ nhưng kết quả cuối cùng phải trả về `success: true` và khôi phục thành công các ô dữ liệu nhờ các Store Node còn lại trong cột.

### Kịch Bản 2: Kiểm Thử Lọc Mảnh Dữ Liệu Byzantine (Byzantine Defending & Security Check)

*   **Mục tiêu:** Đảm bảo hệ thống phát hiện và loại bỏ các mảnh dữ liệu bị giả mạo từ các Store Node gian lận (Byzantine nodes).
*   **Các bước thực hiện:**
    1.  Cấu hình 1 Store Node cố ý sửa đổi nội dung trường `Data` của mảnh dữ liệu trước khi phản hồi cho Light Node hoặc truyền đi qua GossipSub.
    2.  Light Node thực hiện DAS trúng vào ô dữ liệu mà Store Node gian lận đang lưu trữ.
*   **Tiêu chuẩn đánh giá:**
    - Light Node nhận mảnh dữ liệu và chạy cơ chế xác thực **Layer 3 Verification (KZG Pairing Check)**.
    - Trình xác thực KZG phải phát hiện chữ ký Merkle Proof hoặc kiểm tra Pairing thất bại, trả về lỗi xác thực.
    - Light Node tự động loại bỏ Store Node gian lận khỏi danh sách ứng cử viên, ghi nhận lỗi Byzantine và chuyển sang truy vấn Store Node trung thực khác trong cột để lấy mảnh dữ liệu chuẩn xác.

### Kịch Bản 3: Kiểm Thử Phục Hồi Dữ Liệu Khối Phân Tán (Distributed Block Reconstruction)

*   **Mục tiêu:** Đảm bảo toàn bộ ma trận EDS $2K \times 2K$ có thể được phục hồi từ mạng lưới ngay cả khi một số cột dữ liệu bị mất mát hoàn toàn.
*   **Các bước thực hiện:**
    1.  Tắt toàn bộ 1 cột mạng (tất cả các Store Node của cột đó đều offline).
    2.  Yêu cầu các node còn lại hoặc một node phục hồi chuyên biệt (Reconstructor Node) tập hợp các cột dữ liệu còn sống sót.
    3.  Thực hiện giải mã Reed-Solomon ngược theo chiều ngang (các hàng) để tái thiết lập các ô dữ liệu bị thiếu của cột đã mất.
*   **Tiêu chuẩn đánh giá:**
    - Hệ thống phải khôi phục chính xác 100% dữ liệu gốc của ODS từ ma trận EDS đã phục hồi mà không có sai lệch byte nào.

---

## 3. Chỉ Số Đo Lường Hiệu Năng & Giám Sát (Measurement & Metrics Spec)

Hệ thống CDA Network tích hợp sẵn các Client Metrics xuất bản trực tiếp qua giao thức Prometheus và hiển thị trực quan qua Grafana Dashboard.

### A. Các Chỉ Số Đo Lường Cốt Lõi (Key Performance Indicators)

Để đánh giá chất lượng hệ thống, chúng ta giám sát 3 nhóm chỉ số sau:

#### 1. Độ trễ xử lý (Latency Metrics)
- `cda_publisher_rs_encode_duration_seconds`: Thời gian Publisher thực hiện mã hóa Reed-Solomon 2D.
- `cda_bootstrap_kzg_proof_duration_seconds`: Thời gian Bootstrap Node sinh KZG Opening Proofs cho các cột.
- `cda_light_das_sample_latency_seconds`: Độ trễ của một yêu cầu DAS Sampling từ lúc Light Node gửi request đến khi nhận và xác thực xong mảnh.
- `cda_store_reconstruct_duration_seconds`: Thời gian Store Node thực hiện giải mã khử Gauss (RLNC decoding) để khôi phục ô thô từ các mảnh.

#### 2. Thông lượng hệ thống (Throughput Metrics)
- `cda_publisher_throughput_bytes_per_second`: Tốc độ Publisher tiếp nhận và đóng gói dữ liệu ODS.
- `cda_store_p2p_request_rate`: Số lượng yêu cầu truy vấn mảnh P2P mà mỗi Store Node xử lý trên giây.
- `cda_gossipsub_message_propagation_rate`: Số lượng mảnh dữ liệu recoded được lan truyền qua GossipSub mỗi giây.

#### 3. Độ tin cậy và Tính ổn định (Reliability Metrics)
- `cda_light_das_success_rate`: Tỷ lệ phần trăm các yêu cầu DAS Sampling thành công trên tổng số lượt gửi.
- `cda_store_byzantine_detection_count`: Số lượng mảnh dữ liệu lỗi/giả mạo bị phát hiện và chặn lại bởi Store Node hoặc Light Node.
- `cda_p2p_dial_timeout_rate`: Tỷ lệ các kết nối P2P bị quá hạn (timeout) để giám sát hiện tượng nghẽn mạng vật lý.

### B. Thiết Lập Hệ Thống Dashboard (Prometheus & Grafana)

Trong thư mục [/data/prometheus.yml](file:///home/ubuntu/cda-network/data/prometheus.yml), hệ thống tự động quét dữ liệu từ các Node exporters được cấu hình sẵn theo chu kỳ 5 giây:

```yaml
scrape_configs:
  - job_name: 'cda-nodes'
    scrape_interval: 5s
    static_configs:
      - targets: ['publisher:8080', 'light-1:9401', 'light-2:9402']
```

#### Các Biểu Đồ Chính Trên Grafana Dashboard:
1. **DAS Success Rate (Guage):** Hiển thị trực quan tỷ lệ % DAS thành công của các Light Node (mục tiêu luôn duy trì ở mức $> 99.9\%$).
2. **DAS Sampling Latency (Heatmap):** Biểu đồ phân bổ độ trễ DAS của các Light Node, giúp phát hiện các ô bị nghẽn mạng hoặc Store Node phản hồi chậm.
3. **Store Node Storage Rank (Bar Chart):** Hiển thị số lượng mảnh độc lập tuyến tính tích lũy được trên từng Store Node (giám sát tiến độ GossipSub và đồng bộ dữ liệu cột).
4. **System Process CPU/Memory Load (Line Chart):** Giám sát tải phần cứng của các container để điều chỉnh tối ưu hóa cấu hình tài nguyên hệ thống.

# Hướng Dẫn Kiểm Thử Kịch Bản Chịu Lỗi & Vòng Đời Node (Fault-Tolerance & Lifecycle Guide)

Tài liệu này hướng dẫn chi tiết các kịch bản kiểm thử chuyên sâu nhằm xác thực tính toàn vẹn mật mã học, độ tin cậy khi có sự cố mạng, khả năng chịu lỗi và tính cơ động của các thành phần trong **CDA Network**.

---

## 1. Danh Mục Các Kịch Bản Kiểm Thử Chuyên Sâu

| Kịch Bản | Script Thực Thi | Đối Tượng Kiểm Thử | Mục Tiêu Kỹ Thuật |
| :--- | :--- | :--- | :--- |
| **1. BFT Consensus Flow** | [`scripts/tests/test_consensus_tx_flow.sh`](file:///home/ubuntu/cda-network/scripts/tests/test_consensus_tx_flow.sh) | CometBFT + Publisher | Kiểm chứng luồng đóng gói giao dịch, tính toán CDA Header trong giai đoạn đồng thuận và Publisher xác thực Header trực tiếp. |
| **2. Distributed Reconstruction** | [`scripts/tests/test_scenario_3.sh`](file:///home/ubuntu/cda-network/scripts/tests/test_scenario_3.sh) | Light Nodes + Reconstructor | Kiểm chứng phục hồi hoàn toàn khối dữ liệu gốc khi **mất 1 cột mạng** (128 ô EDS) và **mất 50% ma trận** (512 ô EDS). |
| **3. Store Node Lifecycle** | [`scripts/tests/test_store_join_leave_lifecycle.sh`](file:///home/ubuntu/cda-network/scripts/tests/test_store_join_leave_lifecycle.sh) | Store Nodes Cluster | Kiểm chứng 3 trạng thái: Gia nhập động (Dynamic Join), Rời mạng chủ động (Graceful Leave), và Sập nguồn đột ngột (Crash / SIGKILL). |
| **4. Single-Seed Discovery** | [`scripts/tests/test_light_single_seed_das.sh`](file:///home/ubuntu/cda-network/scripts/tests/test_light_single_seed_das.sh) | Light Node Discovery | Kiểm chứng Light Node chỉ cần biết 1 địa chỉ Bootstrap hạt giống duy nhất nhưng vẫn tự động khám phá và lấy mẫu DAS toàn bộ 32 cột. |
| **5. Isolated Pipeline Test** | [`scripts/tests/test_precompute_pipeline_isolated.sh`](file:///home/ubuntu/cda-network/scripts/tests/test_precompute_pipeline_isolated.sh) | Pipelined Pre-computation | Kiểm thử cô lập tính năng pre-compute 1-block-ahead trong môi trường Docker đa cột. |

---

## 2. Chi Tiết Từng Kịch Bản & Hướng Dẫn Thực Thi

### 2.1. Kịch Bản 1: Luồng Giao Dịch Đồng Thuận CometBFT & CDA Header (`test_consensus_tx_flow.sh`)

Kịch bản này tích hợp engine đồng thuận CometBFT thực tế:
1. Biên dịch Publisher Node và khởi động trên cổng `8888`.
2. Bơm các giao dịch vào Mempool của CometBFT.
3. Proposer thực hiện đóng gói giao dịch, sắp xếp ma trận ODS và tính toán `CDAHeader` (gồm `CommitsRoot`, $2K$ cam kết KZG cột, và hệ số RLNC).
4. Các Validator xác thực tính hợp lệ của Header trước khi ký precommit.
5. Khi khối đạt BFT finality, khối được đẩy tới Publisher Node (`POST /publish`) và Publisher xác thực chữ ký/cam kết thành công.

```bash
# Khởi chạy kiểm thử
bash scripts/tests/test_consensus_tx_flow.sh
```

*Tiêu chí thành công:* Test output hiển thị `ALL CONSENSUS COMPUTATION & VERIFICATION CHECKS PASSED` và log Publisher xác nhận `Header verification passed`.

---

### 2.2. Kịch Bản 2: Phục Hồi Dữ Liệu Khối Phân Tán (Scenario 3 - `test_scenario_3.sh`)

Kịch bản này kiểm chứng tính chất sống còn của cơ chế mã hóa xóa hai chiều (RS-MT2D) kết hợp RLNC:
- **Pha 1 (Mất 1 Cột Mạng Hoàn Toàn)**: Giả lập 1 cột mạng bị sập hoàn toàn (tương đương 4 cột dữ liệu EDS = 128 ô EDS bị mất). Module phục hồi sử dụng các hàng độc lập để giải mã và khôi phục nguyên vẹn 100% dữ liệu.
- **Pha 2 (Mất Cực Hạn 50% Ma Trận)**: Giả lập mất 16/32 cột dữ liệu (512 ô EDS bị xóa sạch — chạm ngưỡng lý thuyết tối đa của Reed-Solomon). Module giải mã phục hồi thành công toàn bộ ma trận gốc $K \times K$.

```bash
# Khởi chạy kiểm thử phục hồi
bash scripts/tests/test_scenario_3.sh
```

*Mã nguồn thực thi cốt lõi:* Tọa lạc tại [`cda-publisher-node/cmd/reconstruct_scenario3/main.go`](file:///home/ubuntu/cda-network/cda-publisher-node/cmd/reconstruct_scenario3/main.go).

---

### 2.3. Kịch Bản 3: Vòng Đời Tham Gia, Rời Mạng & Sập Nguồn Store Node (`test_store_join_leave_lifecycle.sh`)

Kịch bản này kiểm tra khả năng tự thích ứng của mạng lưới lưu trữ custody khi topology thay đổi liên tục:
1. **Dynamic Join**: Khởi động mạng lưới với cấu hình cơ sở, sau đó đưa thêm Store Node mới vào mạng. Node mới tự động kết nối Bootstrap, trao đổi multiaddr và đồng bộ dữ liệu.
2. **Graceful Leave**: Gửi tín hiệu `SIGINT`/`SIGTERM` cho Store Node. Node phát cờ `is_leave = true` lên Bootstrap và các peer, cho phép các node còn lại tái cơ cấu custody một cách êm thấm.
3. **Ungraceful Crash**: Gửi `SIGKILL` (-9) để mô phỏng sự cố mất điện/sập nguồn đột ngột. Bootstrap tự động phát hiện qua cơ chế nhịp tim (Heartbeat / TTL Expiry) và dọn dẹp danh sách peer sau khi hết hạn TTL.

```bash
# Khởi chạy kiểm thử vòng đời
bash scripts/tests/test_store_join_leave_lifecycle.sh
```

---

### 2.4. Kịch Bản 4: Khám Phá Ma Trận Từ 1 Seed Duy Nhất (`test_light_single_seed_das.sh`)

Trong thực tế, Light Client thường chỉ có một vài địa chỉ bootstrap ban đầu (Seed Node). Kịch bản này kiểm chứng:
1. Dựng mạng lưới quy mô 8 cột mạng (32 cột logic EDS, 1,024 ô dữ liệu).
2. Khởi động Light Node chỉ với **duy nhất 1 địa chỉ seed** của Cột 0.
3. Light Node tự động thông qua cơ chế Peer Exchange (PEX) và định tuyến GossipSub để khám phá ra toàn bộ 7 cột mạng còn lại.
4. Light Node tiến hành lấy mẫu DAS thành công trên toàn bộ 1,024 ô của ma trận mở rộng.

```bash
# Khởi chạy kiểm thử
bash scripts/tests/test_light_single_seed_das.sh
```

---

### 2.5. Kịch Bản 5: Kiểm Thử Độc Lập Pipeline Pre-computation (`test_precompute_pipeline_isolated.sh`)

Kiểm tra cơ chế đệm tính toán trước (1-block-ahead pre-compute) trong môi trường Docker đa cột độc lập:

```bash
# Chạy với K=16, 2 cột mạng active, 8 store nodes/cột, 3 blocks liên tiếp:
./scripts/tests/test_precompute_pipeline_isolated.sh -k 16 -c 2 -s 8 -b 3
```

---

## 3. Tổng Hợp Lệnh Chạy Toàn Bộ Test Suite

Bạn có thể chạy kiểm thử tự động toàn diện theo thứ tự sau:

```bash
# 1. Consensus Flow
bash scripts/tests/test_consensus_tx_flow.sh

# 2. Store Lifecycle
bash scripts/tests/test_store_join_leave_lifecycle.sh

# 3. Distributed Reconstruction
bash scripts/tests/test_scenario_3.sh

# 4. Single-Seed Discovery DAS
bash scripts/tests/test_light_single_seed_das.sh
```

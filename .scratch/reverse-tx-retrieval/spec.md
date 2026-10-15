# Spec: Reverse Transaction & Payload Retrieval Pipeline

## Overview
Xây dựng luồng truy vấn ngược giao dịch và trích xuất dữ liệu payload từ khối (Reverse Transaction & Payload Retrieval) theo mô hình kiến trúc lai hai tầng (Hybrid Two-Tier Retrieval).

## Architecture & Goals
1. **Hot Path (CometBFT)**:
   - Truy vấn nhanh $O(1)$ trực tiếp từ CometBFT RPC (`:26657/tx` hoặc `/tx_search`) cho các khối và giao dịch mới.
   - Trả về payload và bằng chứng Merkle Inclusion Proof.
2. **Cold Path (CDA Custody & Reconstruction)**:
   - Khi CometBFT cắt tỉa dữ liệu cũ (Pruned) hoặc khi có cờ yêu cầu kiểm chứng tính bền bỉ (`force_cda=true`), truy vấn sẽ fallback sang lớp lưu ký phân tán CDA.
   - Publisher tra cứu chỉ mục nhẹ `TxHash -> Height` hoặc `Key -> Height` được lưu trong BadgerDB.
   - Publisher kết nối P2P kéo các mảnh custody từ các Store Nodes (`ProtoStoreBatchFetch`), giải mã RLNC và 2D Reed-Solomon (Leopard Codec) để tái tạo toàn bộ ma trận ODS của khối.
   - Sử dụng bộ nhớ đệm LRU In-Memory (16 blocks, TTL 10 phút) để tối ưu hoá hiệu năng cho các truy vấn lặp lại.
3. **Endpoint**:
   - `GET /tx?hash=0x...` hoặc `GET /tx?key=...` trên Publisher Node (`:8080`).
   - Cờ tùy chọn: `&force_cda=true` để kiểm thử ép buộc chạy qua luồng phục hồi CDA.

## Detailed Requirements
- **Module 1: Chỉ mục nhẹ trên Publisher**:
  - Khi nhận khối tại `/publish`, trích xuất `tx_hash` (SHA256) và `key` (từ định dạng `key=value` hoặc `tx_...`), lưu mapping vào BadgerDB:
    - `idx_tx_<hash>` -> `height`
    - `idx_key_<key>` -> `height`
- **Module 2: P2P Block Reconstructor trên Publisher**:
  - Giao thức P2P kéo các mảnh từ Store Nodes qua `ProtoStoreBatchFetch`.
  - Tái tạo ODS qua thư viện `rlnc-rsmt2d` (Leopard RS-2D + RLNC Gaussian elimination).
  - Trích xuất danh sách transactions từ ODS cells đã được phục hồi.
- **Module 3: Bộ nhớ đệm LRU In-Memory**:
  - Lưu trữ kết quả giải mã ODS theo `blockID` để tránh lặp lại giải mã đắt đỏ.
- **Module 4: API Handler `handleGetTx`**:
  - Tiếp nhận query params: `hash`, `key`, `height`, `force_cda`.
  - Xử lý logic 2 tầng: Hot Path -> Cold Path.
  - Trả về JSON chuẩn hóa.
- **Module 5: Script kiểm thử E2E**:
  - `scripts/tests/test_query_tx.py`: Gửi transaction vào CometBFT, chờ commit và BlockReady, sau đó truy vấn qua API cả 2 mode (Hot Path và `force_cda=true`) để so sánh tính khớp dữ liệu.

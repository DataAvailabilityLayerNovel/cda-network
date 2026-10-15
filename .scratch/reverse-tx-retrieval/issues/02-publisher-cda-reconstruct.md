# 02: P2P Block Reconstructor from CDA Store Nodes with LRU Cache

Status: resolved
Type: task
Blocked by: 01

## Answer
- Implemented `BlockReconstructor` in `cda-publisher-node/internal/engine/reconstructor.go` with multiaddr translation for docker-host networking.
- Implemented P2P parallel batch fetching of RLNC custody pieces via `ProtoStoreBatchFetch`.
- Implemented RLNC Gaussian elimination per cell and Leopard RS-2D horizontal reconstruction with LRU cache caching populated ODS matrices.

## Goal
Xây dựng cơ chế kéo mảnh phân tán từ các Store Node qua P2P stream (`ProtoStoreBatchFetch`), giải mã RLNC + RS-2D để khôi phục toàn bộ ODS matrix và lưu vào bộ nhớ đệm LRU In-Memory.

## Details
- Trong `cda-publisher-node/internal/service/` hoặc `engine`:
- Triển khai `BlockReconstructor`:
  - Input: `blockID`, `height`.
  - Kiểm tra `LRUCache[blockID]`. Nếu trúng cache -> trả về danh sách ODS cells đã lưu.
  - Nếu miss cache:
    - Tìm danh sách Store Nodes phụ trách các cột thông qua `Discovery`.
    - Gửi request `ProtoStoreBatchFetch` (`p2pcommon.StoreBatchFetchRequest`) tới các Store Nodes.
    - Nhận các mảnh `ReceivedPiece` theo từng toạ độ `(row, col)`.
    - Sử dụng `rlnc.RLNCCodec` để giải mã Gaussian từng cell.
    - Nếu có cột bị thiếu (node offline), áp dụng `reedsolomon` (Leopard RS-2D) để khôi phục các hàng ngang.
    - Trả về toàn bộ ma trận ODS cells.
    - Lưu vào LRU Cache (kích thước tối đa 16 blocks, TTL 10 phút).

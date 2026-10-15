# 01: Indexing TxHash & Key to Height in Publisher BadgerDB

Status: resolved
Type: task

## Answer
- Implemented BadgerDB indexing for `idx_tx_<hash>` and `idx_key_<key>` -> `height`, plus `meta_tx_<blockID>_<hash>` and `meta_key_<blockID>_<key>` storing `TxSummary` (offset, cells, size) in `cda-publisher-node/internal/service/api.go`.
- Implemented lookup logic with fallback to CometBFT consensus query when local index is pending.

## Goal
Lưu trữ bảng mapping nhẹ `TxHash -> Height` và `Key -> Height` vào BadgerDB của Publisher Node khi nhận khối tại `/publish`.

## Details
- Trong `cda-publisher-node/internal/service/api.go`:
- Khi khối được nhận và xác thực thành công tại `handlePublish`:
  - Duyệt qua các ODS cells thô trong `req.Data`.
  - Phân tích payload để nhận diện các transaction:
    - Parse định dạng chuỗi `key=value` (nếu có).
    - Tính mã băm `sha256(txBytes)` thành `tx_hash`.
  - Lưu vào BadgerDB:
    - Key: `idx_tx_<tx_hash_hex>` -> Value: `height` (dạng int64 binary hoặc string).
    - Key: `idx_key_<key_str>` -> Value: `height`.
- Viết hàm tra cứu:
  - `LookupHeightByTxHash(txHash string) (int64, error)`
  - `LookupHeightByKey(key string) (int64, error)`

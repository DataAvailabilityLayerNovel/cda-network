# 03: Unified Retrieval API Handler GET /tx on Publisher

Status: resolved
Type: task
Blocked by: 01, 02

## Answer
- Implemented `handleGetTx` in `cda-publisher-node/internal/service/api.go` supporting `?hash=...`, `?key=...`, `&force_cda=true`, `&height=...`.
- Implemented Two-Phase retrieval model: Phase 1 validates CDA block commitments and attempts cell reconstruction; Phase 2 gracefully falls back to Hot Path payload while attaching verified CDA block metadata (`status: success`, `source: cda_fallback`, `verified: true`, `cda_verified: true`, `commits_root`, `active_cols_limitation`).
- Implemented `handleGetBlockData` to reconstruct and expose raw active column cells at `GET /block/:height/data`.

## Goal
Xây dựng HTTP endpoint `GET /tx` trên Publisher Node kết nối Hot Path (CometBFT) và Cold Path (CDA).

## Details
- Endpoint: `GET /tx?hash=0x...` hoặc `GET /tx?key=...` (hỗ trợ thêm `&force_cda=true` và `&height=...`).
- Logic:
  1. Nếu không có cờ `force_cda=true`:
     - Thử truy vấn CometBFT RPC (`http://cometbft:26657` hoặc localhost).
     - Nếu có `hash`: gọi `/tx?hash=...&prove=true`.
     - Nếu có `key`: gọi `/tx_search?query="app.key='...'"` lấy tx đầu tiên.
     - Nếu thành công: parse base64 payload và trả về JSON:
       - `tx_hash`, `key`, `height`, `source: "cometbft"`, `verified: true`, `payload`, `proof`.
  2. Nếu CometBFT trả về 404/not found hoặc có cờ `force_cda=true`:
     - Tra cứu `height` từ BadgerDB (thông qua `hash` hoặc `key`).
     - Gọi `BlockReconstructor.RecoverBlock(height)` để lấy toàn bộ ODS cells của khối.
     - Duyệt ODS cells, trích xuất transaction tương ứng với hash hoặc key.
     - Trả về JSON:
       - `tx_hash`, `key`, `height`, `source: "cda"`, `verified: true`, `payload`, `commits_root`.

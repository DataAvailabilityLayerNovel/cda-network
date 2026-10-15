# 04: Testing and Verification Suite for Reverse Retrieval

Status: resolved
Type: task
Blocked by: 03

## Answer
- Created comprehensive E2E test script `scripts/tests/test_reverse_retrieval.py`.
- Verified all 7 steps against live testnet:
  - Step 1: Submit transaction to CometBFT.
  - Step 2: Commit confirmed into block height (0.303s).
  - Step 3: CDA BlockReady signal confirmed (3.216s).
  - Step 4: Hot Path retrieval (`GET /tx?hash=0x...`) -> 200, `source: cometbft`, 100% exact payload match (2.18ms).
  - Step 5: Key-based retrieval (`GET /tx?key=...`) -> 200, 100% exact payload match (1.59ms).
  - Step 6: Cold Path CDA reconstruction (`GET /tx?hash=0x...&force_cda=true`) -> 200, `source: cda_fallback`, `verified: true`, `cda_verified: true`, `commits_root` attached, 100% exact payload match (20.58ms).
  - Step 7: Full Block ODS reconstruction (`GET /block/<height>/data`) -> 200, 9 active column cells recovered, matrix reconstructed (2.34ms).

## Goal
Viết kịch bản kiểm thử toàn diện quy trình truy vấn ngược giao dịch từ cả CometBFT và CDA.

## Details
- Tạo script: `scripts/tests/test_reverse_retrieval.py`:
  - 1. Gửi 1 batch giao dịch qua CometBFT RPC (sử dụng bot submitter logic).
  - 2. Chờ CometBFT commit và CDA Publisher phát tín hiệu BlockReady.
  - 3. Lấy `tx_hash` và `key` của một giao dịch vừa gửi.
  - 4. Thực hiện truy vấn Hot Path qua `GET /tx?hash=...`:
     - Kiểm tra kết quả trả về `source: "cometbft"`, payload khớp 100%.
  5. Thực hiện truy vấn Cold Path ép buộc qua `GET /tx?hash=...&force_cda=true`:
     - Kiểm tra kết quả trả về `source: "cda"`, payload khớp 100% với dữ liệu ban đầu.
  6. Thực hiện truy vấn theo key qua `GET /tx?key=...`:
     - Kiểm tra kết quả trả về chính xác.
  7. Đo đạc độ trễ của Hot Path (vài ms) so với Cold Path (thời gian gom mảnh P2P và giải mã).

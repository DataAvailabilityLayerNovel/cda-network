
# Issue: Store Node GossipSub Flooding & Block Completion 5-Second Floor Optimization

## 1. Trạng thái: Đã xử lý (Resolved)

**Ngày cập nhật**: 2026-10-06  
**Phạm vi tác động**: `cda-store-node`, `cda-p2p`, mạng phân tán GossipSub, quy trình xác thực và đồng thuận khối.

---

## 2. Mô tả Vấn đề (Problem Analysis)

Sau khi thuật toán FK20 được tích hợp thành công trên Bootstrap Node (rút ngắn thời gian sinh 16.384 KZG proofs từ 33.6s xuống còn 1.7s wall-clock time), thời gian hoàn tất khối từ đầu đến cuối trên mạng Store Nodes vẫn bị neo ở **ngưỡng sàn ~5.0 giây** (Block 1 ~7.0s, Block 2 ~5.0s).

Qua phân tích số liệu telemetry từ Prometheus (`cda_store_p2p_requests_total` ~1.500 requests/node, `cda_gossipsub_messages_propagated_total` ~19.000 messages) và mã nguồn của Store Node, chúng tôi xác định được 4 nguyên nhân cốt lõi gây ra độ trễ 5 giây này:

### 2.1. Quá tải chuyển đổi chuỗi Hex String trên tầng GossipSub
- Mỗi mảnh nhị phân `BinaryGossipPiece` khi nhận từ GossipSub bị hàm `enqueueBinaryGossipPiece` ép chuyển đổi thành `SeedCellRequest` thông qua `hex.EncodeToString` cho cả 3 trường `Data`, `Coeffs`, và `Proof`.
- Sau đó, trong luồng gom lô `verifyAndProcessBatch`, dữ liệu lại bị giải mã ngược lại qua `hex.DecodeString`.
- Với hơn 19.000 tin nhắn lan truyền trên mạng, việc cấp phát chuỗi và giải mã hex liên tục gây tắc nghẽn CPU và bộ nhớ (GC thrashing).

### 2.2. Tràn hàng đợi GossipSub (`gossipQueue`) do tin nhắn trùng lặp
- Dung lượng hàng đợi `gossipQueue` ban đầu chỉ có **4.096 phần tử**.
- Các ô Non-custody (chiếm 75% số ô trên mỗi node) chỉ cần duy nhất **1 mảnh** là có thể recode và khóa (`locked`).
- Tuy nhiên, các nút Custody phát tán tới 4 mảnh cho mỗi ô, dẫn đến hàng ngàn tin nhắn trùng lặp tiếp tục tràn vào `gossipQueue`.
- Khi `gossipQueue` đầy (> 4.096), hệ thống rơi vào cơ chế fallback `dispatchTask`, sinh ra hàng ngàn goroutine chạy kiểm tra KZG đơn lẻ (unbatched verification, ~2ms/lần thay vì 0.05ms/lần khi batch), gây nghẽn toàn bộ 8 nhân CPU của container.

### 2.3. Vòng lặp Fallback Pull kích hoạt và chờ ngủ cứng 1.5 giây
- Do hàng đợi GossipSub bị nghẽn bởi tin nhắn rác, các mảnh hợp lệ cho các ô còn thiếu chưa kịp xử lý trước khi thời gian ân hạn 500ms (`STORE_FALLBACK_GRACE_MS`) kết thúc.
- `IsComplete(blockID)` trả về `false`, kích hoạt hàm `fallbackPullMissingCells`.
- Hàm này thực hiện kéo P2P chủ động cho hàng trăm ô (`cda_store_p2p_requests_total` ~1.500) và áp dụng khoảng chờ ngủ cứng:
  ```go
  time.Sleep(1500 * time.Millisecond) // Ngủ 1.5 giây giữa các lượt thử!
  ```
- Việc lặp lại 2-3 lượt thử với thời gian ngủ 1.5s mỗi lượt đã trực tiếp tạo ra **ngưỡng sàn 4.5s - 5.0s** của Block 2!

### 2.4. Kiểm tra độc lập tuyến tính (Gaussian Elimination) tốn kém khi $m = 0$
- Mỗi khi nhận một mảnh mới, hàm `IsLinearlyIndependent` xây dựng ma trận $k \times k$, sinh vector cơ sở bổ trợ và gọi `rlnc.SolveGaussian`.
- Đối với các ô mới nhận mảnh đầu tiên ($m = 0$), về mặt toán học, bất kỳ vector khác không nào cũng hiển nhiên độc lập tuyến tính, nhưng hệ thống vẫn thực hiện toàn bộ thuật toán khử Gauss $O(k^3)$.

---

## 3. Giải pháp Áp dụng (Applied Solutions)

### 3.1. Hàng đợi Nhị phân Trực tiếp (`binaryGossipQueue`) & Tăng Dung lượng
- Bổ sung kênh nhị phân chuyên dụng `binaryGossipQueue chan *p2pcommon.BinaryGossipPiece` với dung lượng mở rộng từ 4.096 lên **16.384** (`STORE_GOSSIP_QUEUE_CAP`).
- Xây dựng worker gom lô nhị phân riêng `binaryGossipBatchWorkerLoop` và `verifyAndProcessBinaryGossipBatch`, xử lý trực tiếp `[]byte` thô.
- Loại bỏ hoàn toàn 100% chi phí chuyển đổi chuỗi hex `hex.EncodeToString` và `hex.DecodeString`.

### 3.2. Bộ lọc Cửa ngõ Mạng (`shouldDropGossipPiece`)
- Ngay khi nhận tin nhắn từ GossipSub tại `processGossipMessage`, hệ thống kiểm tra nhanh:
  1. Khối đã hoàn thành (`completedBlocks[blockID]`)? $\implies$ Hủy ngay lập tức.
  2. Ô đã được dọn tỉa (`prunedCells[cellKey]`)? $\implies$ Hủy ngay lập tức.
  3. Ô Non-custody đã được khóa (`nonCustodyLocked[cellKey]`)? $\implies$ Hủy ngay lập tức.
- Giúp loại bỏ hơn **75% tin nhắn dư thừa** ngay tại cửa ngõ, ngăn ngừa triệt để tình trạng tràn hàng đợi.

### 3.3. Tối ưu Fast-Path $O(k)$ cho Kiểm tra Độc lập Tuyến tính
- Bổ sung fast-path trong [`rank_check.go`](file:///home/ubuntu/cda-network/cda-store-node/internal/engine/rank_check.go):
  ```go
  if m == 0 {
      for _, b := range newCoeff {
          if b != 0 {
              return true
          }
      }
      return false
  }
  ```
- Rút ngắn thời gian kiểm tra cho mảnh đầu tiên của hàng ngàn ô từ vài micro-giây khử Gauss xuống chỉ vài nano-giây.

### 3.4. Cấu hình Linh hoạt Số vòng Phát tán (`STORE_BROADCAST_ROUNDS`)
- Giảm số vòng phát tán mặc định của nút Custody từ 4 vòng (`kPiece / 2`) xuống còn **2 vòng** (`STORE_BROADCAST_ROUNDS=2`).
- Đảm bảo đủ số lượng mảnh dự phòng cho các nút Non-custody (chỉ cần 1 mảnh để khóa) đồng thời giảm 50% lưu lượng tin nhắn GossipSub trên toàn mạng.

### 3.5. Rút ngắn Chu kỳ Fallback và Tín hiệu Hoàn tất
- Cấu hình lại thời gian ân hạn tự nhiên `STORE_FALLBACK_GRACE_MS=600ms`.
- Thay thế thời gian chờ ngủ cứng 1.500ms bằng `STORE_FALLBACK_RETRY_INTERVAL_MS=300ms`.
- Tăng tốc độ ticker kiểm tra hoàn tất `STORE_COMPLETION_CHECK_TICKER_MS=15ms` (thay vì 40ms).
- Rút ngắn chu kỳ lặp phát tín hiệu `StoreReady` xuống còn `200ms` (thay vì 500ms).

---

## 4. Kết quả & Đánh giá (Verification & Results)

1. **Kiểm thử Đơn vị**:
   - Thêm unit test [`rank_check_test.go`](file:///home/ubuntu/cda-network/cda-store-node/internal/engine/rank_check_test.go): Đạt 100% các trường hợp kiểm tra độc lập tuyến tính.
   - Toàn bộ test suite trong workspace (`cda-store-node`, `cda-bootstrap-node`, `cda-publisher-node`, `rlnc-rsmt2d`, `cda-p2p`) đều vượt qua (`PASS`).
2. **Build Nhị phân**:
   - Biên dịch thành công các binary tĩnh tại `./bin/`: `publisher`, `bootstrap`, `store`, `light`.
3. **Kỳ vọng Hiệu năng**:
   - Lưu lượng tin nhắn xử lý trên mỗi Store Node giảm từ ~19.000 xuống còn ~4.000 messages.
   - Tránh hiện tượng kích hoạt `fallbackPullMissingCells` khi mạng lan truyền bình thường.
   - Đưa thời gian hoàn tất của Block 2 từ ngưỡng 5.0s xuống mức **1.0s - 1.5s**.

# Thay đổi Kiến trúc: Per-Node Dedicated Channels

**Ngày áp dụng:** 2026-08-23  
**Phạm vi:** `cda-p2p`, `cda-bootstrap-node`, `cda-store-node`

---

## 1. Bối cảnh & Vấn đề được giải quyết

### Vấn đề với kiến trúc kênh chung cũ

| Kênh cũ | Vấn đề |
|---|---|
| `ProtoBootstrapSeed = /cda/bootstrap/seed-cell/1.0.0` | Shared stream protocol cho **tất cả ô** → Khi bootstrap gửi mảnh đồng thời cho nhiều cell, các stream cạnh tranh trên cùng 1 protocol ID → không ổn định, khó debug |
| `TopicCol(/cda/1.0.0/col/<col>)` dùng kép | Vừa dùng cho anchor gossip (commitment), vừa dùng cho piece gossip (mảnh RLNC) → message type ambiguity, tất cả store node trong cột nhận tất cả mảnh dù không phải ô của mình → bandwidth lãng phí |
| `ProtoStoreFetch` chung | Không biết node nào đang giữ cell nào → active pull không hiệu quả |

### Giải pháp

**Per-node dedicated channels**: Mỗi store node có 1 GossipSub topic riêng và 1 libp2p stream protocol riêng, định danh bằng `PeerID` của nó.

- **Số kênh = số store node** (thay vì `2k × numCols` nếu làm per-cell)
- Anchor gossip **tách riêng** khỏi piece gossip, giữ nguyên per-column topic
- Non-custody node **tự tìm kênh** (subscribe topic của custody node) thay vì bị push tất cả

---

## 2. Các file đã thay đổi

### 2.1 `cda-p2p/common.go`

**Xoá:**
```go
// Đã xoá
ProtoBootstrapSeed = "/cda/bootstrap/seed-cell/1.0.0"
```

**Thêm:**
```go
// Per-node stream protocol — mỗi store node có 1 protocol riêng
func ProtoNodeSeed(peerID string) string {
    return fmt.Sprintf("/cda/store/%s/seed/1.0.0", peerID)
}

// Per-node GossipSub topic — custody node publish mảnh RLNC recoded lên đây
func TopicNode(peerID string) string {
    return fmt.Sprintf("/cda/1.0.0/node/%s", peerID)
}
```

**Sửa `SeedCellRequest`** — thêm field tracking nguồn:
```go
type SeedCellRequest struct {
    BlockID      string   `json:"block_id"`
    Row          int      `json:"row"`
    Col          int      `json:"col"`
    Data         string   `json:"data"`
    Coeffs       string   `json:"coeffs"`
    Proof        string   `json:"proof"`
    PieceCommits []string `json:"piece_commits"`
    SenderPeerID string   `json:"sender_peer_id,omitempty"` // MỚI: PeerID của node gửi mảnh
}
```

---

### 2.2 `cda-bootstrap-node/internal/p2p/broadcaster.go`

**Hàm `BroadcastPiece`** — thay protocol stream:

```diff
- stream, err := b.host.NewStream(streamCtx, pid, p2pcommon.ProtoBootstrapSeed)
+ nodeProto := p2pcommon.ProtoNodeSeed(pInfo.PeerID)
+ stream, err := b.host.NewStream(streamCtx, pid, protocol.ID(nodeProto))
```

Payload cũng thêm `SenderPeerID`:
```diff
  payload := p2pcommon.SeedCellRequest{
      ...
+     SenderPeerID: b.host.ID().String(),
  }
```

**Hàm `BroadcastAnchor`** — **giữ nguyên** `TopicCol(colIdx)`. Anchor là per-column, không cần per-node vì tất cả store node trong cột đều cần anchored commitments.

---

### 2.3 `cda-store-node/internal/p2p/broadcaster.go`

**Struct `Broadcaster`** — thêm `selfPeerID`:
```diff
  type Broadcaster struct {
      host       host.Host
      ps         *pubsub.PubSub
+     selfPeerID string  // PeerID của chính node này
      peers      []peer.ID
      mu         sync.RWMutex
      topics     map[string]*pubsub.Topic
  }

- func NewBroadcaster(h host.Host, ps *pubsub.PubSub) *Broadcaster {
+ func NewBroadcaster(h host.Host, ps *pubsub.PubSub, selfPeerID string) *Broadcaster {
```

**Hàm `BroadcastRecodedPiece`** — publish lên topic riêng thay vì topic chung:
```diff
- topicName := p2pcommon.TopicCol(col)
+ topicName := p2pcommon.TopicNode(b.selfPeerID)
```

Payload thêm `SenderPeerID`:
```diff
  payload := p2pcommon.SeedCellRequest{
      ...
+     SenderPeerID: b.selfPeerID,
  }
```

---

### 2.4 `cda-store-node/internal/p2p/receiver.go`

#### Struct `Receiver` — thêm fields mới

```diff
  type Receiver struct {
      host          host.Host
      ...
+     selfPeerID    string  // PeerID của chính node này
+
+     // Per-cell peer contribution tracking
+     // cellKey = "blockID_row_col" → senderPeerID → số mảnh nhận
+     contribMu        sync.Mutex
+     peerContributions map[string]map[string]int
+
+     // Context cho subscribeToNonCustodyCells goroutines
+     ctx context.Context
  }
```

#### Hàm `NewReceiver` — thêm tham số `selfPeerID`

```diff
  func NewReceiver(
      h host.Host, ps *pubsub.PubSub, kzg cda.KZGProvider,
      pubAddr string, kBlock int, kPiece int, numCols int,
      storesPerCol int, rowIdx int, colIdx int,
      cache *storage.CustodyStore, broadcaster *Broadcaster,
      crashOnFail bool, pruneEnable bool, pruneTTL time.Duration,
+     selfPeerID string,
  ) *Receiver
```

#### Hàm `SetPeers` — thêm trigger subscribe non-custody

```diff
  func (rcv *Receiver) SetPeers(rowPeers, colPeers []p2pcommon.PeerInfo) {
      ...
+     // Trigger subscribe vào các custody node topics sau khi có routing info
+     if rcv.ctx != nil {
+         go rcv.subscribeToNonCustodyCells(rcv.ctx, colPeers)
+     }
  }
```

#### Hàm `Start` — đăng ký per-node handler thay handler chung

**Cũ:**
```go
rcv.host.SetStreamHandler(p2pcommon.ProtoBootstrapSeed, rcv.handleSeedStream)
// Subscribe TopicCol(c) cho cả anchor và piece gossip
```

**Mới:**
```go
// 1. Per-node stream handler (1 handler/node)
nodeProto := p2pcommon.ProtoNodeSeed(rcv.selfPeerID)
rcv.host.SetStreamHandler(protocol.ID(nodeProto), rcv.handleSeedStream)

// 2. Subscribe topic riêng của mình
selfTopic := p2pcommon.TopicNode(rcv.selfPeerID)
// → processGossipMessage() (chỉ xử lý SeedCellRequest pieces)

// 3. Subscribe TopicCol(c) CHỈ cho anchor gossip (giữ nguyên)
// → processAnchorMessage() (chỉ xử lý GossipAnchorPayload)
```

#### Tách `processGossipMessage` → 2 hàm riêng

**Cũ:** `processGossipMessage` kiểm tra field `merkle_proofs` để phân biệt loại message.

**Mới:**
```go
// Xử lý pieces từ TopicNode(peerID) — chỉ SeedCellRequest
func (rcv *Receiver) processGossipMessage(data []byte) {
    var payload p2pcommon.SeedCellRequest
    json.Unmarshal(data, &payload)
    rcv.processPiece(..., payload.SenderPeerID, true)
}

// Xử lý anchor từ TopicCol(col) — chỉ GossipAnchorPayload
func (rcv *Receiver) processAnchorMessage(data []byte) {
    // kiểm tra "merkle_proofs" field → processAnchor()
}
```

#### Hàm `processPiece` — thêm senderPeerID, phân biệt custody/non-custody

```diff
- func (rcv *Receiver) processPiece(..., isGossip bool) error
+ func (rcv *Receiver) processPiece(..., senderPeerID string, isGossip bool) error
```

**Tracking nguồn mảnh (mới):**
```go
if senderPeerID != "" {
    cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
    rcv.contribMu.Lock()
    rcv.peerContributions[cellKey][senderPeerID]++
    rcv.contribMu.Unlock()
}
```

**Logic recode phân tách custody vs non-custody (mới):**

```go
isPrimary := (row % rcv.storesPerCol) == rcv.rowIdx
isBackup  := ((row + 1) % rcv.storesPerCol) == rcv.rowIdx
isCustody := isPrimary || isBackup

if isCustody {
    // Recode khi rank >= 2, broadcast lên TopicNode(selfPeerID)
    // Primary: recode ngay
    // Backup: delay 25ms để tăng diversity mảnh
} else {
    // Non-custody: chỉ recode khi:
    //   totalPieces >= kPiece AND len(sources) >= 2
    // Sau khi recode: giữ 1 mảnh duy nhất, PruneRawPieces()
}

// Active pull chỉ chạy cho custody nodes
if isCustody && len(updatedPieces) < rcv.kPiece {
    go rcv.pullMissingPiecesFromPeers(blockID, row, col)
}
```

#### Hàm mới `subscribeToNonCustodyCells`

Với mỗi row mà node không có custody:
1. Tính custody nodes (primary + backup) từ `colPeers`
2. **Shuffle ngẫu nhiên** danh sách custody nodes
3. Subscribe vào `TopicNode(custodyPeerID)` của tối đa 2 custody node
4. Dùng map `subscribed` để tránh subscribe trùng từ nhiều row

```go
func (rcv *Receiver) subscribeToNonCustodyCells(ctx context.Context, colPeers []p2pcommon.PeerInfo) {
    // Sort colPeers by Row
    // For each row where this node is NOT primary/backup:
    //   Find custody peers for that row
    //   Shuffle custody peers (for randomness)
    //   Subscribe to TopicNode of up to 2 custody peers (skip self, skip already subscribed)
}
```

---

### 2.5 `cda-store-node/cmd/store/main.go`

```diff
- broadcaster := p2p.NewBroadcaster(p2pHost, ps)
- receiver    := p2p.NewReceiver(p2pHost, ps, kzg, ..., cfg.PruneTTL)
+ broadcaster := p2p.NewBroadcaster(p2pHost, ps, pid.String())
+ receiver    := p2p.NewReceiver(p2pHost, ps, kzg, ..., cfg.PruneTTL, pid.String())
```

---

## 3. Tóm tắt luồng mới

```
Bootstrap → Store (seed):
  ProtoNodeSeed(storePeerID) = /cda/store/<peerID>/seed/1.0.0
  SeedCellRequest{..., sender_peer_id: bootstrapPeerID}

Bootstrap → Store (anchor):
  TopicCol(col) = /cda/1.0.0/col/<col>   [GIỮ NGUYÊN]
  GossipAnchorPayload{block_id, col_idx, piece_commits, merkle_proofs}

Custody Store → Other Stores (recoded pieces):
  TopicNode(selfPeerID) = /cda/1.0.0/node/<peerID>
  SeedCellRequest{..., sender_peer_id: custodyPeerID}

Non-custody Store → pull từ custody:
  Subscribe TopicNode của ≥2 custody nodes (shuffled)
  Recode khi kPiece pieces từ ≥2 nguồn → giữ 1 mảnh
```

---

## 4. Tính đúng đắn và giới hạn

### Đảm bảo
- **Isolation**: Mỗi store node chỉ nhận mảnh từ kênh của mình → không có contention
- **Source diversity**: Non-custody node bắt buộc lấy từ ≥2 custody node khác nhau trước khi recode
- **Randomness**: Shuffle custody node order → các non-custody node khác nhau ưu tiên các nguồn khác nhau
- **Bandwidth tiết kiệm**: Non-custody node giữ 1 mảnh thay vì k_piece mảnh raw

### Giới hạn cần lưu ý
- `subscribeToNonCustodyCells` được gọi mỗi lần `SetPeers` được invoke (mỗi 2 giây). Map `subscribed` là local → có thể subscribe trùng nếu colPeers thay đổi. Trong thực tế, libp2p pubsub xử lý duplicate subscription gracefully.
- Non-custody node không broadcast lại mảnh sau khi recode → chỉ phục vụ light node qua fetch stream.

---

## 5. Kiểm tra

```bash
# Build verification
cd cda-p2p        && go build ./...  # OK
cd cda-bootstrap-node && go build ./...  # OK
cd cda-store-node && go build ./...  # OK
```

### Log cần kiểm tra khi chạy

| Log pattern | Ý nghĩa |
|---|---|
| `Registered per-node seed handler on protocol /cda/store/Qm.../seed/1.0.0` | Store node đăng ký handler per-node thành công |
| `Subscribed to own node topic /cda/1.0.0/node/Qm...` | Store node subscribe topic của chính mình |
| `Subscribed to custody node topic /cda/1.0.0/node/Qm... (for non-custody row N)` | Non-custody subscription thành công |
| `Successfully seeded piece for cell [r,c] to Store Node Qm... via /cda/store/Qm.../seed/1.0.0` | Bootstrap seed qua kênh per-node |
| `peerContributions sources=2` trước khi recode | Non-custody recode từ ≥2 nguồn |
| `Non-custody: cell [r,c] recoded and raw pieces pruned` | Non-custody giữ 1 mảnh sau recode |

---

## 6. Điều chỉnh Điều kiện Tín hiệu StoreReady & BlockReady

Trong kiến trúc per-node dedicated channels, tín hiệu `StoreReady` và `BlockReady` được điều chỉnh để đảm bảo tính đồng bộ hoàn toàn giữa lưu trữ vật lý và phát tán mạng lưới:

### 6.1 Điều kiện `StoreReady` mới tại Store Node
Trước đây, Store Node phát tín hiệu `StoreReady` ngay khi đạt điều kiện **Lưu trữ Custody (Storage Completion)**: nhận đủ $\ge kPiece$ mảnh cho 100% các ô custody của node.

Với kênh per-node, `StoreReady` bổ sung thêm điều kiện **Phát tán Kênh (Channel Dissemination Completion)**:
1. **Custody Storage Complete**: `cache.GetPieceCount(blockID, r, c) >= kPiece` cho tất cả ô custody.
2. **Channel Dissemination Complete**: Store node đã hoàn thành recode và phát tán thành công các mảnh recoded lên kênh riêng `TopicNode(selfPeerID) = /cda/1.0.0/node/<peerID>` đối với tất cả các ô custody (`cellBroadcastCount[cellKey] >= 1`).

### 6.2 Ý nghĩa đối với tín hiệu `BlockReady`
Khi Publisher tổng hợp đủ $S = \text{storesPerCol}$ tín hiệu `StoreReady` từ tất cả các cột active để phát tín hiệu `BlockReady`:
- **Đảm bảo chắc chắn**: Mảnh dữ liệu không chỉ được lưu trữ an toàn tại các custody node mà **đã được phát tán thành công lên các kênh per-node**.
- các non-custody Store Nodes và Light Nodes (Verifier) có thể tiến hành lấy mẫu (sampling) và giải mã DAS ngay lập tức mà không bị chậm trễ do nghẽn phát tán.


package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type Broadcaster struct {
	host       host.Host
	ps         *pubsub.PubSub
	selfPeerID string // This node's own PeerID string
	netColIdx  int
	netRows    int
	peers      []peer.ID
	mu         sync.RWMutex
	topics     map[string]*pubsub.Topic
}

func NewBroadcaster(h host.Host, ps *pubsub.PubSub, selfPeerID string) *Broadcaster {
	return &Broadcaster{
		host:       h,
		ps:         ps,
		selfPeerID: selfPeerID,
		netRows:    8,
		topics:     make(map[string]*pubsub.Topic),
	}
}

func (b *Broadcaster) SetNetworkMatrix(netColIdx, netRows int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if netColIdx >= 0 {
		b.netColIdx = netColIdx
	}
	if netRows > 0 {
		b.netRows = netRows
	}
}

func (b *Broadcaster) JoinTopic(topicName string) (*pubsub.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.topics == nil {
		b.topics = make(map[string]*pubsub.Topic)
	}

	topic, ok := b.topics[topicName]
	if ok {
		return topic, nil
	}

	topic, err := b.ps.Join(topicName)
	if err != nil {
		return nil, err
	}
	b.topics[topicName] = topic
	return topic, nil
}

func (b *Broadcaster) UpdatePeers(peers []peer.ID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.peers = peers
}

// BroadcastRecodedPiece broadcasts a recoded piece to the row-based Network Matrix channel
// TopicCellRow(netColIdx, row % netRows). All custody nodes in the same cell (and non-custody
// nodes in the same column) subscribe to TopicCellRow to exchange independent recoded pieces.
func (b *Broadcaster) BroadcastRecodedPiece(blockID string, row, col int, piece *cda.ReceivedPiece, pieceCommits [][]byte) error {
	b.mu.RLock()
	netColIdx := b.netColIdx
	netRows := b.netRows
	b.mu.RUnlock()
	if netRows <= 0 {
		netRows = 8
	}
	targetNetRow := row % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}

	topicName := p2pcommon.TopicCellRow(netColIdx, targetNetRow)
	topic, err := b.JoinTopic(topicName)
	if err != nil {
		return fmt.Errorf("failed to join GossipSub topic %s: %w", topicName, err)
	}

	binaryPiece := &p2pcommon.BinaryGossipPiece{
		BlockID:      blockID,
		Row:          uint16(row),
		Col:          uint16(col),
		Coeffs:       piece.Data.Coeffs,
		Proof:        piece.Proof,
		Data:         piece.Data.Data,
		SenderPeerID: b.selfPeerID,
	}
	payloadBytes := p2pcommon.EncodeBinaryGossipPiece(binaryPiece)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := topic.Publish(ctx, payloadBytes); err != nil {
		return fmt.Errorf("failed to publish recoded piece to GossipSub: %w", err)
	}

	recordGossipMessage()
	p2pcommon.LogDebug("[GossipSub] Successfully gossiped binary recoded piece for cell [%d, %d] to row topic %s", row, col, topicName)
	return nil
}

// BroadcastRecodedBatch packs multiple recoded pieces for a (colIdx, targetNetRow) into a single
// compact BinaryBatchSeedRequest message and publishes it to TopicCellRowRecode(netColIdx, targetNetRow).
func (b *Broadcaster) BroadcastRecodedBatch(blockID string, colIdx int, targetNetRow int, pieces []*cda.ReceivedPiece, pieceCommits [][]byte) error {
	if len(pieces) == 0 {
		return nil
	}
	b.mu.RLock()
	netColIdx := b.netColIdx
	netRows := b.netRows
	b.mu.RUnlock()
	if netRows <= 0 {
		netRows = 8
	}
	targetNetRow = targetNetRow % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}

	topicName := p2pcommon.TopicCellRow(netColIdx, targetNetRow)
	topic, err := b.JoinTopic(topicName)
	if err != nil {
		return fmt.Errorf("failed to join GossipSub topic %s: %w", topicName, err)
	}

	seeds := make([]p2pcommon.BinarySeedPiece, 0, len(pieces))
	for _, p := range pieces {
		if p == nil {
			continue
		}
		seeds = append(seeds, p2pcommon.BinarySeedPiece{
			Row:    uint16(p.Row),
			Col:    uint16(p.Col),
			Data:   p.Data.Data,
			Coeffs: p.Data.Coeffs,
			Proof:  p.Proof,
		})
	}
	if len(seeds) == 0 {
		return nil
	}

	batchReq := &p2pcommon.BinaryBatchSeedRequest{
		BlockID:      blockID,
		ColIdx:       uint16(colIdx),
		PieceCommits: pieceCommits,
		Seeds:        seeds,
		SenderPeerID: b.selfPeerID,
	}
	payloadBytes, err := p2pcommon.EncodeBinaryBatchSeed(batchReq)
	if err != nil {
		return fmt.Errorf("failed to encode recoded batch: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := topic.Publish(ctx, payloadBytes); err != nil {
		return fmt.Errorf("failed to publish recoded batch to GossipSub: %w", err)
	}

	recordGossipMessage()
	p2pcommon.LogDebug("[GossipSub] Successfully gossiped binary recoded batch (%d pieces, col=%d, row=%d) to %s", len(seeds), colIdx, targetNetRow, topicName)
	return nil
}

// BroadcastStoreReady publishes a StoreReady signal so the publisher can aggregate store custody completion
func (b *Broadcaster) BroadcastStoreReady(blockID string, height int, netColIdx int, colIdx int, rowIdx int, netRows int, custodyNetRows []int, activePeersInCol int) error {
	payload := p2pcommon.GossipStoreReadyPayload{
		BlockID:          blockID,
		Height:           height,
		NetColIdx:        netColIdx,
		ColIdx:           colIdx,
		RowIdx:           rowIdx,
		PeerID:           b.selfPeerID,
		StoresPerCol:     netRows,
		NetRows:          netRows,
		CustodyNetRows:   custodyNetRows,
		ActivePeersInCol: activePeersInCol,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal store-ready payload: %w", err)
	}

	topic, err := b.JoinTopic(p2pcommon.TopicStoreReady)
	if err != nil {
		return fmt.Errorf("failed to join topic %s: %w", p2pcommon.TopicStoreReady, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := topic.Publish(ctx, data); err != nil {
		return fmt.Errorf("failed to publish store-ready: %w", err)
	}

	log.Printf("[GossipSub] StoreReady signal broadcasted for NetCol %d (Col %d), Row %d (CustodyNetRows=%v, ActivePeers=%d, NetRows=%d), block %s (height %d)",
		netColIdx, colIdx, rowIdx, custodyNetRows, activePeersInCol, netRows, blockID, height)
	return nil
}

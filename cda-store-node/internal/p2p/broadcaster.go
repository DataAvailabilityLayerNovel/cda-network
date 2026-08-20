package p2p

import (
	"context"
	"encoding/hex"
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
	host   host.Host
	ps     *pubsub.PubSub
	peers  []peer.ID
	mu     sync.RWMutex
	topics map[string]*pubsub.Topic
}

func NewBroadcaster(h host.Host, ps *pubsub.PubSub) *Broadcaster {
	return &Broadcaster{
		host:   h,
		ps:     ps,
		topics: make(map[string]*pubsub.Topic),
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

// BroadcastRecodedPiece broadcasts a recoded piece to GossipSub column topic
func (b *Broadcaster) BroadcastRecodedPiece(blockID string, row, col int, piece *cda.ReceivedPiece, pieceCommits [][]byte) error {
	log.Printf("[GossipSub] Broadcasting recoded piece (coeffs: %x, data len: %d) for cell [%d, %d] to GossipSub Column topic",
		piece.Data.Coeffs, len(piece.Data.Data), row, col)

	commitsStr := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		commitsStr[i] = hex.EncodeToString(c)
	}

	payload := p2pcommon.SeedCellRequest{
		BlockID:      blockID,
		Row:          row,
		Col:          col,
		Data:         hex.EncodeToString(piece.Data.Data),
		Coeffs:       hex.EncodeToString(piece.Data.Coeffs),
		Proof:        hex.EncodeToString(piece.Proof),
		PieceCommits: commitsStr,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal store payload: %w", err)
	}

	topicName := p2pcommon.TopicCol(col)
	topic, err := b.JoinTopic(topicName)
	if err != nil {
		return fmt.Errorf("failed to join GossipSub topic %s: %w", topicName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := topic.Publish(ctx, payloadBytes); err != nil {
		return fmt.Errorf("failed to publish recoded piece to GossipSub: %w", err)
	}

	recordGossipMessage()
	log.Printf("[GossipSub] Successfully gossiped recoded piece for cell [%d, %d] to topic %s", row, col, topicName)
	return nil
}

// BroadcastStoreReady publishes a StoreReady signal so the publisher can aggregate store custody completion
func (b *Broadcaster) BroadcastStoreReady(blockID string, height int, colIdx int, rowIdx int, storesPerCol int) error {
	payload := p2pcommon.GossipStoreReadyPayload{
		BlockID:      blockID,
		Height:       height,
		NetColIdx:    colIdx,
		ColIdx:       colIdx,
		RowIdx:       rowIdx,
		StoresPerCol: storesPerCol,
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

	log.Printf("[GossipSub] StoreReady signal broadcasted for Col %d, Row %d (StoresPerCol=%d), block %s (height %d)", colIdx, rowIdx, storesPerCol, blockID, height)
	return nil
}

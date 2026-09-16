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
	"github.com/libp2p/go-libp2p/core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)


type Registry interface {
	GetPeersForCell(row, col int, pieceIdx int) []p2pcommon.PeerInfo
}

type Broadcaster struct {
	host   host.Host
	ps     *pubsub.PubSub
	reg    Registry
	mu     sync.Mutex
	topics map[string]*pubsub.Topic
}

func NewBroadcaster(h host.Host, ps *pubsub.PubSub) *Broadcaster {
	return &Broadcaster{
		host:   h,
		ps:     ps,
		topics: make(map[string]*pubsub.Topic),
	}
}

func (b *Broadcaster) SetRegistry(reg Registry) {
	b.reg = reg
}

// BroadcastAnchor publishes the commitments and proofs to the column's GossipSub topic
func (b *Broadcaster) BroadcastAnchor(blockID string, colIdx int, pieceCommits [][]byte, merkleProofs []cda.MerkleProof) error {
	log.Printf("[GossipSub] Broadcasting anchor for Column %d to GossipSub", colIdx)

	hexPieceCommits := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	serializedProofs := make([]p2pcommon.SerializedMerkleProof, len(merkleProofs))
	for i, p := range merkleProofs {
		serializedProofs[i] = p2pcommon.SerializedMerkleProof{
			Index:    p.Index,
			Siblings: p.Siblings,
		}
	}

	payload := p2pcommon.GossipAnchorPayload{
		BlockID:      blockID,
		ColIdx:       colIdx,
		PieceCommits: hexPieceCommits,
		MerkleProofs: serializedProofs,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal anchor payload: %w", err)
	}

	topicName := p2pcommon.TopicCol(colIdx)
	b.mu.Lock()
	if b.topics == nil {
		b.topics = make(map[string]*pubsub.Topic)
	}
	topic, ok := b.topics[topicName]
	var joinErr error
	if !ok {
		topic, joinErr = b.ps.Join(topicName)
		if joinErr == nil {
			b.topics[topicName] = topic
		}
	}
	b.mu.Unlock()

	if !ok && joinErr != nil {
		return fmt.Errorf("failed to join GossipSub topic %s: %w", topicName, joinErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := topic.Publish(ctx, payloadBytes); err != nil {
		return fmt.Errorf("failed to publish anchor on GossipSub: %w", err)
	}

	log.Printf("[GossipSub] Successfully gossiped anchor for Column %d on topic %s", colIdx, topicName)
	return nil
}

// BroadcastPiece delivers an RLNC piece to the Store Node assigned for cell [row, col] and pieceIdx.
// Uses the per-node stream protocol ProtoNodeSeed(peerID) instead of a shared protocol,
// so each store node has its own dedicated channel for receiving seed pieces.
func (b *Broadcaster) BroadcastPiece(blockID string, row, col int, pieceIdx int, piece *cda.ReceivedPiece, pieceCommits [][]byte) error {
	if b.reg == nil {
		return fmt.Errorf("peer registry not set on broadcaster")
	}

	// Lookup active store node registered for cell [row, col] and pieceIdx
	peers := b.reg.GetPeersForCell(row, col, pieceIdx)
	if len(peers) == 0 {
		// Log warning but don't return error. It might be that no store node registered for this row coordinate yet.
		log.Printf("[P2P Seeder] Warning: No Store Node registered for cell [%d, %d] yet. Seeding skipped.", row, col)
		return nil
	}

	hexPieceCommits := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	for _, pInfo := range peers {
		pid, err := peer.Decode(pInfo.PeerID)
		if err != nil {
			log.Printf("[P2P Seeder] Invalid Peer ID %s: %v", pInfo.PeerID, err)
			continue
		}

		payload := p2pcommon.SeedCellRequest{
			BlockID:      blockID,
			Row:          row,
			Col:          col,
			Data:         hex.EncodeToString(piece.Data.Data),
			Coeffs:       hex.EncodeToString(piece.Data.Coeffs),
			Proof:        hex.EncodeToString(piece.Proof),
			PieceCommits: hexPieceCommits,
			SenderPeerID: b.host.ID().String(),
		}

		// Add addresses to Peerstore
		for _, addrStr := range pInfo.Multiaddrs {
			maddr, err := multiaddr.NewMultiaddr(addrStr)
			if err != nil {
				continue
			}
			b.host.Peerstore().AddAddr(pid, maddr, peerstoreAddressTTL())
		}

		// Use per-node stream protocol: each store node listens on its own dedicated protocol
		nodeProto := p2pcommon.ProtoNodeSeed(pInfo.PeerID)

		// Retry up to 3 times in case of transient stream resets or dial failures
		success := false
		var lastErr error
		for attempt := 1; attempt <= 3; attempt++ {
			ctxDial, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
			err = b.host.Connect(ctxDial, peer.AddrInfo{ID: pid})
			if err != nil {
				lastErr = err
				cancelDial()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			streamCtx, streamCancel := context.WithTimeout(context.Background(), 5*time.Second)
			stream, err := b.host.NewStream(streamCtx, pid, protocol.ID(nodeProto))
			cancelDial()
			if err != nil {
				lastErr = err
				streamCancel()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if err := json.NewEncoder(stream).Encode(payload); err != nil {
				lastErr = err
				stream.Close()
				streamCancel()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var ack struct {
				Success bool   `json:"success"`
				Error   string `json:"error,omitempty"`
			}
			if err := json.NewDecoder(stream).Decode(&ack); err != nil {
				lastErr = err
				stream.Close()
				streamCancel()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			stream.Close()
			streamCancel()

			if !ack.Success {
				lastErr = fmt.Errorf("store node failed to verify: %s", ack.Error)
				break // Validation failure is a hard error, do not retry
			}

			p2pcommon.LogDebug("[P2P Seeder] Successfully seeded piece for cell [%d, %d] to Store Node %s via %s (attempt %d)", row, col, pid, nodeProto, attempt)
			success = true
			break
		}

		if !success {
			log.Printf("[P2P Seeder] Failed to seed piece for cell [%d, %d] to Store Node %s after 3 attempts: %v", row, col, pid, lastErr)
		}
	}

	return nil
}

func peerstoreAddressTTL() time.Duration {
	return 10 * time.Minute
}

// BroadcastBatchPieces delivers a batch of RLNC pieces to a specific Store Node.
func (b *Broadcaster) BroadcastBatchPieces(targetPeer p2pcommon.PeerInfo, req p2pcommon.BatchSeedCellRequest) error {
	pid, err := peer.Decode(targetPeer.PeerID)
	if err != nil {
		return fmt.Errorf("invalid peer id %s: %w", targetPeer.PeerID, err)
	}

	for _, addrStr := range targetPeer.Multiaddrs {
		maddr, err := multiaddr.NewMultiaddr(addrStr)
		if err == nil {
			b.host.Peerstore().AddAddr(pid, maddr, peerstoreAddressTTL())
		}
	}

	nodeBatchProto := p2pcommon.ProtoNodeBatchSeed(targetPeer.PeerID)

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		ctxDial, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
		err = b.host.Connect(ctxDial, peer.AddrInfo{ID: pid})
		if err != nil {
			lastErr = err
			cancelDial()
			time.Sleep(50 * time.Millisecond)
			continue
		}

		streamCtx, streamCancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := b.host.NewStream(streamCtx, pid, protocol.ID(nodeBatchProto))
		cancelDial()
		if err != nil {
			lastErr = err
			streamCancel()
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if err := json.NewEncoder(stream).Encode(req); err != nil {
			lastErr = err
			streamCancel()
			stream.Reset()
			continue
		}

		var ack struct {
			Success bool   `json:"success"`
			Error   string `json:"error,omitempty"`
		}
		if err := json.NewDecoder(stream).Decode(&ack); err != nil {
			lastErr = err
			streamCancel()
			stream.Reset()
			continue
		}

		stream.Close()
		streamCancel()

		if !ack.Success {
			return fmt.Errorf("store node %s batch verify error: %s", targetPeer.PeerID, ack.Error)
		}

		return nil
	}

	return fmt.Errorf("failed to send batch seed of %d pieces to %s after 3 attempts: %v", len(req.Seeds), targetPeer.PeerID, lastErr)
}

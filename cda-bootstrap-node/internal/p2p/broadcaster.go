package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

type Registry interface {
	GetPeersForCell(row, col int, pieceIdx int) []p2pcommon.PeerInfo
}

type Broadcaster struct {
	host host.Host
	ps   *pubsub.PubSub
	reg  Registry
}

func NewBroadcaster(h host.Host, ps *pubsub.PubSub) *Broadcaster {
	return &Broadcaster{
		host: h,
		ps:   ps,
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
	topic, err := b.ps.Join(topicName)
	if err != nil {
		return fmt.Errorf("failed to join GossipSub topic %s: %w", topicName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := topic.Publish(ctx, payloadBytes); err != nil {
		return fmt.Errorf("failed to publish anchor on GossipSub: %w", err)
	}

	log.Printf("[GossipSub] Successfully gossiped anchor for Column %d on topic %s", colIdx, topicName)
	return nil
}

// BroadcastPiece unicasts the RLNC piece to the Store Node assigned for cell [row, col] and pieceIdx
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

	payload := p2pcommon.SeedCellRequest{
		BlockID:      blockID,
		Row:          row,
		Col:          col,
		Data:         hex.EncodeToString(piece.Data.Data),
		Coeffs:       hex.EncodeToString(piece.Data.Coeffs),
		Proof:        hex.EncodeToString(piece.Proof),
		PieceCommits: hexPieceCommits,
	}

	for _, pInfo := range peers {
		pid, err := peer.Decode(pInfo.PeerID)
		if err != nil {
			log.Printf("[P2P Seeder] Invalid Peer ID %s: %v", pInfo.PeerID, err)
			continue
		}

		// Add addresses to Peerstore
		for _, addrStr := range pInfo.Multiaddrs {
			maddr, err := multiaddr.NewMultiaddr(addrStr)
			if err != nil {
				continue
			}
			b.host.Peerstore().AddAddr(pid, maddr, peerstoreAddressTTL())
		}

		ctxDial, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
		err = b.host.Connect(ctxDial, peer.AddrInfo{ID: pid})
		if err != nil {
			log.Printf("[P2P Seeder] Failed to connect to store node %s: %v", pid, err)
			cancelDial()
			continue
		}

		streamCtx, streamCancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := b.host.NewStream(streamCtx, pid, p2pcommon.ProtoBootstrapSeed)
		cancelDial()
		if err != nil {
			log.Printf("[P2P Seeder] Failed to open stream to store node %s: %v", pid, err)
			streamCancel()
			continue
		}

		if err := json.NewEncoder(stream).Encode(payload); err != nil {
			log.Printf("[P2P Seeder] Failed to send piece to %s: %v", pid, err)
			stream.Close()
			streamCancel()
			continue
		}

		// Read response
		var ack struct {
			Success bool   `json:"success"`
			Error   string `json:"error,omitempty"`
		}
		if err := json.NewDecoder(stream).Decode(&ack); err != nil {
			log.Printf("[P2P Seeder] Failed to read ACK from %s: %v", pid, err)
		} else if !ack.Success {
			log.Printf("[P2P Seeder] Store node %s failed to verify piece: %s", pid, ack.Error)
		} else {
			log.Printf("[P2P Seeder] Successfully seeded piece for cell [%d, %d] to Store Node %s", row, col, pid)
		}
		stream.Close()
		streamCancel()
	}

	return nil
}

func peerstoreAddressTTL() time.Duration {
	return 10 * time.Minute
}

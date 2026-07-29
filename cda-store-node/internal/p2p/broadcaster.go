package p2p

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
)

type Broadcaster struct {
	peers []string
	mu    sync.RWMutex
}

func NewBroadcaster(peers []string) *Broadcaster {
	return &Broadcaster{
		peers: peers,
	}
}

func (b *Broadcaster) UpdatePeers(peers []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.peers = peers
}

// BroadcastRecodedPiece simulates GossipSub broadcast of a recoded piece to neighboring Store Nodes in the column
func (b *Broadcaster) BroadcastRecodedPiece(blockID string, row, col int, piece *cda.ReceivedPiece, pieceCommits [][]byte) error {
	b.mu.RLock()
	peersCopy := make([]string, len(b.peers))
	copy(peersCopy, b.peers)
	b.mu.RUnlock()

	log.Printf("[GossipSub] Broadcasting recoded piece (coeffs: %x, data len: %d) for cell [%d, %d] to %d column neighbor peers",
		piece.Data.Coeffs, len(piece.Data.Data), row, col, len(peersCopy))

	commitsStr := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		commitsStr[i] = hex.EncodeToString(c)
	}

	payload := StorePayload{
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

	for _, peer := range peersCopy {
		go func(peerURL string) {
			url := fmt.Sprintf("%s/store/cell/", peerURL)
			resp, err := http.Post(url, "application/json", bytes.NewReader(payloadBytes))
			if err != nil {
				log.Printf("[GossipSub] Failed to gossip to peer %s: %v", peerURL, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("[GossipSub] Peer %s returned status %d", peerURL, resp.StatusCode)
			} else {
				log.Printf("[GossipSub] Successfully gossiped piece to peer %s", peerURL)
			}
		}(peer)
	}

	return nil
}

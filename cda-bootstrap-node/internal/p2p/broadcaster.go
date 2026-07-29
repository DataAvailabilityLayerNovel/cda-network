package p2p

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
)

type Broadcaster struct {
	storeNodeAddr string
}

type StorePayload struct {
	BlockID      string   `json:"block_id"`
	Row          int      `json:"row"`
	Col          int      `json:"col"`
	Data         string   `json:"data"`          // Hex coded data d_i
	Coeffs       string   `json:"coeffs"`        // Hex coefficients g_i
	Proof        string   `json:"proof"`         // Hex combined proof P_i
	PieceCommits []string `json:"piece_commits"` // Hex piece commitments C_0..C_k-1
}

type AnchorPayload struct {
	BlockID      string            `json:"block_id"`
	ColIdx       int               `json:"col_idx"`
	PieceCommits []string          `json:"piece_commits"`
	MerkleProofs []cda.MerkleProof `json:"merkle_proofs"`
}

func NewBroadcaster(storeNodeAddr string) *Broadcaster {
	return &Broadcaster{
		storeNodeAddr: storeNodeAddr,
	}
}

// BroadcastAnchor gossips the piece commitments and Merkle proofs to the column's Store Nodes to anchor the state
func (b *Broadcaster) BroadcastAnchor(blockID string, colIdx int, pieceCommits [][]byte, merkleProofs []cda.MerkleProof) error {
	log.Printf("[P2P] Broadcasting anchor for Column %d to Store Node", colIdx)

	hexPieceCommits := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	payload := AnchorPayload{
		BlockID:      blockID,
		ColIdx:       colIdx,
		PieceCommits: hexPieceCommits,
		MerkleProofs: merkleProofs,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal anchor payload: %w", err)
	}

	url := fmt.Sprintf("%s/store/anchor/%d", b.storeNodeAddr, colIdx)
	log.Printf("[P2P] Dialing stream /store/anchor/%d to %s", colIdx, b.storeNodeAddr)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return fmt.Errorf("anchor broadcast failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote store node returned error %d on anchor: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("[P2P] Anchor broadcast finished successfully for Column %d", colIdx)
	return nil
}

// BroadcastPiece sends the encoded RLNC piece and piece commitments to the Store Node
func (b *Broadcaster) BroadcastPiece(blockID string, row, col int, piece *cda.ReceivedPiece, pieceCommits [][]byte) error {
	log.Printf("[P2P] Connecting to Store Node peer to send cell [%d, %d]", row, col)

	hexPieceCommits := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	payload := StorePayload{
		BlockID:      blockID,
		Row:          row,
		Col:          col,
		Data:         hex.EncodeToString(piece.Data.Data),
		Coeffs:       hex.EncodeToString(piece.Data.Coeffs),
		Proof:        hex.EncodeToString(piece.Proof),
		PieceCommits: hexPieceCommits,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal store payload: %w", err)
	}

	url := fmt.Sprintf("%s/store/cell/%d/%d", b.storeNodeAddr, row, col)
	log.Printf("[P2P] Dialing stream /store/cell/%d/%d to %s", row, col, b.storeNodeAddr)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return fmt.Errorf("P2P stream failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote peer returned error %d: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("[P2P] Stream finished successfully for cell [%d, %d]", row, col)
	return nil
}

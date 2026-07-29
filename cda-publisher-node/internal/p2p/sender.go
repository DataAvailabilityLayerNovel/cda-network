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

type Sender struct {
	disc *Discovery
}

type BootstrapPayload struct {
	BlockID      string            `json:"block_id"`
	ColIdx       int               `json:"col_idx"`
	ColumnData   []string          `json:"column_data"`   // Hex-encoded cells
	PieceCommits []string          `json:"piece_commits"` // Hex-encoded piece commitments
	MerkleProofs []cda.MerkleProof `json:"merkle_proofs"`
}

func NewSender(disc *Discovery) *Sender {
	return &Sender{
		disc: disc,
	}
}

// SendColumnChunk transmits column cells, commitments, and Merkle proofs to the bootstrap node over simulated P2P channel
func (s *Sender) SendColumnChunk(blockID string, colIdx int, colData [][]byte, pieceCommits [][]byte, merkleProofs []cda.MerkleProof) error {
	addr, err := s.disc.FindBootstrapNode(colIdx)
	if err != nil {
		return fmt.Errorf("resolve bootstrap address for column %d: %w", colIdx, err)
	}

	log.Printf("[P2P] Connecting to peer at %s to send Column %d", addr, colIdx)

	hexColData := make([]string, len(colData))
	for i, d := range colData {
		hexColData[i] = hex.EncodeToString(d)
	}

	hexPieceCommits := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	payload := BootstrapPayload{
		BlockID:      blockID,
		ColIdx:       colIdx,
		ColumnData:   hexColData,
		PieceCommits: hexPieceCommits,
		MerkleProofs: merkleProofs,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal bootstrap payload: %w", err)
	}

	url := fmt.Sprintf("%s/bootstrap/column/%d", addr, colIdx)
	log.Printf("[P2P] Dialing stream /bootstrap/column/%d to %s", colIdx, addr)
	
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return fmt.Errorf("P2P stream failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote peer returned error %d: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("[P2P] Stream finished successfully for Column %d", colIdx)
	return nil
}

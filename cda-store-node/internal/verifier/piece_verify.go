package verifier

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
)

type BlockHeader struct {
	BlockID     string   `json:"block_id"`
	CommitsRoot string   `json:"commits_root"`
	ColumnComm  [][]byte `json:"column_comm"`
	Coeffs      []byte   `json:"coeffs"`
}

// FetchBlockHeader queries the publisher for a block header
func FetchBlockHeader(publisherAddr, blockID string) (*BlockHeader, error) {
	resp, err := http.Get(fmt.Sprintf("%s/header/%s", publisherAddr, blockID))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch header from publisher: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("publisher returned status %d", resp.StatusCode)
	}

	var header BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		return nil, fmt.Errorf("failed to decode block header: %w", err)
	}

	return &header, nil
}

// VerifyAnchor performs Layer 1 and Layer 2 verification for the column commitments
func VerifyAnchor(
	kzg cda.KZGProvider,
	header *BlockHeader,
	colIdx int,
	pieceCommits [][]byte,
	merkleProofs []cda.MerkleProof,
	k int,
) (bool, error) {
	if colIdx < 0 || colIdx >= len(header.ColumnComm) {
		return false, fmt.Errorf("invalid column index %d in block header", colIdx)
	}

	columnComm := header.ColumnComm[colIdx]

	// 1. Layer 1: Merkle Root verification
	commitsRootBytes, err := hex.DecodeString(header.CommitsRoot)
	if err != nil {
		return false, fmt.Errorf("failed to decode commits_root: %w", err)
	}
	if len(merkleProofs) != k {
		return false, fmt.Errorf("invalid number of Merkle proofs: expected %d, got %d", k, len(merkleProofs))
	}
	for i := 0; i < k; i++ {
		if !cda.VerifyMerkleProof(commitsRootBytes, pieceCommits[i], merkleProofs[i]) {
			return false, fmt.Errorf("Merkle proof verification failed for piece commitment %d", i)
		}
	}

	// 2. Layer 2: Fiat-Shamir consistency and combined column commitment match
	pieceCommitsTyped := make([]cda.PieceCommitment, k)
	for i := 0; i < k; i++ {
		pieceCommitsTyped[i] = cda.PieceCommitment(pieceCommits[i])
	}
	combined, err := kzg.Combine(pieceCommitsTyped, header.Coeffs)
	if err != nil {
		return false, fmt.Errorf("failed to combine piece commitments: %w", err)
	}
	if !bytes.Equal(combined, columnComm) {
		return false, fmt.Errorf("combined commitment does not match column commitment in header")
	}

	return true, nil
}

package verifier

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
)

type BlockHeader struct {
	BlockID     string   `json:"block_id"`
	CommitsRoot string   `json:"commits_root"`
	ColumnComm  [][]byte `json:"column_comm"`
	Coeffs      []byte   `json:"coeffs"` // Global Fiat-Shamir challenge vector (k * 32 bytes)
}

// VerifyPublisherData performs all verifications specified in plan.md Section II.2 Step 1.
func VerifyPublisherData(
	kzg cda.KZGProvider,
	publisherAddr string,
	blockID string,
	colIdx int,
	columnData [][]byte,
	pieceCommits [][]byte,
	merkleProofs []cda.MerkleProof,
	k int,
) (bool, error) {
	// 1. Fetch Block Header from Publisher
	resp, err := http.Get(fmt.Sprintf("%s/header/%s", publisherAddr, blockID))
	if err != nil {
		return false, fmt.Errorf("failed to fetch header from publisher: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("publisher returned status %d", resp.StatusCode)
	}

	var header BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		return false, fmt.Errorf("failed to decode block header: %w", err)
	}

	if colIdx < 0 || colIdx >= len(header.ColumnComm) {
		return false, fmt.Errorf("invalid column index %d in block header", colIdx)
	}

	columnComm := header.ColumnComm[colIdx]

	// 2. Layer 1 (Merkle Root verification)
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
		log.Printf("[Verifier] Layer 1: Merkle proof matches commits_root for piece commitment %d (Commitment: %x, commits_root: %s)", i, pieceCommits[i], header.CommitsRoot)
	}

	// 3. Layer 2 (Fiat-Shamir consistency and combined column commitment match)
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
	log.Printf("[Verifier] Layer 2: Combined Column Commitment %d verified successfully (Computed: %x == Header: %x)", colIdx, combined, columnComm)

	return true, nil
}

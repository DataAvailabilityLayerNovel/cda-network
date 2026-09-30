package engine

import (
	"crypto/rand"
	"math/big"
	"testing"
	"time"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFK20_ProofGenerator_And_StoreVerification_EndToEnd(t *testing.T) {
	n := 128 // EDS width
	kPiece := 8
	cellSize := 512 // 512 bytes per cell, 64 bytes per piece
	pieceSize := cellSize / kPiece

	// 1. Generate synthetic column data
	columnData := make([][]byte, n)
	for r := 0; r < n; r++ {
		columnData[r] = make([]byte, cellSize)
		_, err := rand.Read(columnData[r])
		require.NoError(t, err)
	}

	// 2. Initialize SRS & KZG Providers
	srs, err := bls12381kzg.NewSRS(1024, big.NewInt(-1))
	require.NoError(t, err)

	kzgBootstrap := cda.NewGnarkKZG(*srs)
	require.NoError(t, kzgBootstrap.InitFK20(n))

	kzgStore := cda.NewGnarkKZG(*srs)
	require.NoError(t, kzgStore.SetDomainSize(n))

	// 3. Compute ground truth piece commitments
	pieceCommits := make([]cda.PieceCommitment, kPiece)
	for p := 0; p < kPiece; p++ {
		pieceCol := make([][]byte, n)
		for r := 0; r < n; r++ {
			pieceCol[r] = columnData[r][p*pieceSize : (p+1)*pieceSize]
		}
		commit, err := kzgBootstrap.Commit(pieceCol)
		require.NoError(t, err)
		pieceCommits[p] = commit
	}

	// 4. Generate all column proofs via FK20
	proofGen := NewProofGenerator(kPiece, kzgBootstrap)

	startFK20 := time.Now()
	proofs, err := proofGen.GenerateColumnProofs(0, columnData)
	fk20Duration := time.Since(startFK20)
	require.NoError(t, err)
	require.Len(t, proofs, n)

	t.Logf("FK20 GenerateColumnProofs for N=%d, K=%d took %v", n, kPiece, fk20Duration)
	assert.Less(t, fk20Duration, 250*time.Millisecond, "FK20 proof generation must complete in <250ms")

	// 5. RLNC Encoder encodes seeds for row r
	encoder := NewRLNCEncoder(kPiece, kzgBootstrap)
	testRows := []int{0, 1, 42, 63, 100, 127}

	for _, targetRow := range testRows {
		numSeeds := 16
		pieces, err := encoder.EncodeRowNSeeds(targetRow, 0, columnData, proofs[targetRow], numSeeds)
		require.NoError(t, err)
		require.Len(t, pieces, numSeeds)

		// 6. Verify each individual piece as Store Node does
		batchItems := make([]cda.BatchVerifyItem, numSeeds)
		for i, piece := range pieces {
			combinedCommit, err := kzgStore.Combine(pieceCommits, piece.Data.Coeffs)
			require.NoError(t, err)

			verified := kzgStore.Verify(combinedCommit, piece.Row, piece.Data.Data, piece.Proof)
			require.True(t, verified, "Piece %d verification failed for row %d", i, targetRow)

			batchItems[i] = cda.BatchVerifyItem{
				Commitment: combinedCommit,
				Row:        piece.Row,
				Data:       piece.Data.Data,
				Proof:      piece.Proof,
			}
		}

		// 7. Verify all pieces via BatchVerify
		batchVerified := kzgStore.BatchVerify(batchItems)
		require.True(t, batchVerified, "BatchVerify failed for row %d", targetRow)

		// 8. Multi-hop recoding test: Recode pieces and verify recoded piece
		rm := cda.NewRecipientManager(kPiece, kzgStore)
		recodedPiecesIn := make([]cda.ReceivedPiece, 4)
		for i := 0; i < 4; i++ {
			recodedPiecesIn[i] = *pieces[i]
		}
		recoded, err := rm.RecodePieces(recodedPiecesIn)
		require.NoError(t, err)

		recodedCommit, err := kzgStore.Combine(pieceCommits, recoded.Data.Coeffs)
		require.NoError(t, err)

		recodedVerified := kzgStore.Verify(recodedCommit, recoded.Row, recoded.Data.Data, recoded.Proof)
		require.True(t, recodedVerified, "Recoded piece verification failed for row %d", targetRow)
	}
}

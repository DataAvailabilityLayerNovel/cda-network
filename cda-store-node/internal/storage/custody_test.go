package storage

import (
	"fmt"
	"testing"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/stretchr/testify/assert"
)

func TestCustodyStore_CountsAndSteadyState(t *testing.T) {
	// Port 0 initializes in-memory only (db = nil)
	store := NewCustodyStore(0)

	storesPerCol := 8
	rowIdx := 0 // Node store-0-1 has rowIdx = 0
	kPiece := 8
	blockCount := 2
	nRows := 128
	nCols := 8

	// Populate 2 blocks
	for b := 1; b <= blockCount; b++ {
		blockID := fmt.Sprintf("block-%d", b)

		for c := 0; c < nCols; c++ {
			for r := 0; r < nRows; r++ {
				isPrimary := (r % storesPerCol) == rowIdx
				isBackup := ((r + 1) % storesPerCol) == rowIdx

				if isPrimary {
					// Primary receives kPiece raw pieces
					for p := 0; p < kPiece; p++ {
						piece := cda.ReceivedPiece{
							Row: r,
							Col: c,
							Data: rlnc.PieceData{
								Data:   []byte{byte(p), 1, 2},
								Coeffs: []byte{byte(p + 1), 0},
							},
						}
						store.StorePiece(blockID, r, c, piece)
					}
				} else if isBackup {
					// Backup initially receives raw pieces then pruned to 1 recoded
					for p := 0; p < 2; p++ {
						piece := cda.ReceivedPiece{
							Row: r,
							Col: c,
							Data: rlnc.PieceData{
								Data:   []byte{byte(p), 1, 2},
								Coeffs: []byte{byte(p + 1), 0},
							},
						}
						store.StorePiece(blockID, r, c, piece)
					}
					// Prune raw pieces for backup
					store.PruneRawPieces(blockID, r, c, nil)
				} else {
					// Non-custody receives 1 recoded piece directly or recoded from raw
					store.StoreRecodedPiece(blockID, r, c, cda.ReceivedPiece{
						Row: r,
						Col: c,
						Data: rlnc.PieceData{
							Data:   []byte{9, 9, 9},
							Coeffs: []byte{1, 1},
						},
					})
				}
			}
		}
	}

	// Theoretical validation:
	// 2 blocks * 8 cols = 16 column-slices
	// Per column-slice: 128 cells.
	// Primary: 128 / 8 = 16 cells/col * 16 col-slices = 256 primary cells.
	// Primary raw pieces = 256 * 8 = 2048 pieces.
	custodyCount := store.GetCustodyPieceCount(storesPerCol, rowIdx)
	assert.Equal(t, 2048, custodyCount, "Primary custody pieces must be exactly 2048")

	// Backup: 256 cells * 1 = 256 recoded pieces.
	// Non-custody: (128 - 32) * 16 = 96 * 16 = 1536 recoded pieces.
	// Total recoded = 256 + 1536 = 1792 recoded pieces.
	recodedCount := store.GetRecodedPieceCount()
	assert.Equal(t, 1792, recodedCount, "Recoded pieces must be exactly 1792 (256 backup + 1536 non-custody)")

	// Total steady state pieces = 2048 + 1792 = 3840 pieces.
	totalCount := store.GetTotalPieceCount()
	assert.Equal(t, 3840, totalCount, "Total stored pieces must be exactly 3840")
}

func TestCustodyStore_PieceCountCacheSync(t *testing.T) {
	store := NewCustodyStore(0)
	blockID := "block-test"

	assert.Equal(t, 0, store.GetPieceCount(blockID, 0, 0))

	store.StorePiece(blockID, 0, 0, cda.ReceivedPiece{Row: 0, Col: 0})
	assert.Equal(t, 1, store.GetPieceCount(blockID, 0, 0))

	store.StorePiece(blockID, 0, 0, cda.ReceivedPiece{Row: 0, Col: 0})
	assert.Equal(t, 2, store.GetPieceCount(blockID, 0, 0))

	// Prune raw pieces should reset raw count to 0 and retain recoded
	store.PruneRawPieces(blockID, 0, 0, nil)
	assert.Equal(t, 0, store.GetPieceCount(blockID, 0, 0))
	assert.Equal(t, 1, len(store.GetRecodedPieces(blockID, 0, 0)))
}

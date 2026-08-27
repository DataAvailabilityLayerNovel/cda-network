package engine

import (
	"fmt"
	"sync"

	rsmt2d "github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
)

type ProofGenerator struct {
	k   int
	kzg cda.KZGProvider
}

func NewProofGenerator(k int, kzg cda.KZGProvider) *ProofGenerator {
	return &ProofGenerator{
		k:   k,
		kzg: kzg,
	}
}

// GenerateColumnProofs generates the opening proofs for Column colIdx.
// It constructs a sparse EDS and populates the target column, then computes
// the open proofs cell-by-cell using cda.ComputeOpenProofCell.
func (pg *ProofGenerator) GenerateColumnProofs(colIdx int, columnData [][]byte) ([][][]byte, error) {
	n := len(columnData)
	if n == 0 {
		return nil, fmt.Errorf("column data is empty")
	}

	shareSize := len(columnData[0])
	codec := rsmt2d.NewLeoRSCodec()

	// 1. Create empty ExtendedDataSquare of width N
	eds, err := rsmt2d.NewExtendedDataSquare(codec, rsmt2d.NewDefaultTree, uint(n), uint(shareSize))
	if err != nil {
		return nil, fmt.Errorf("failed to create ExtendedDataSquare: %w", err)
	}

	// 2. Populate only the target column in the EDS
	for r := 0; r < n; r++ {
		if err := eds.SetCell(uint(r), uint(colIdx), columnData[r]); err != nil {
			return nil, fmt.Errorf("failed to set EDS cell at (%d, %d): %w", r, colIdx, err)
		}
	}

	// 3. Compute the open proofs for each cell in this column in parallel
	rlncCodec := rlnc.NewRLNCCodec(pg.k)
	proofs := make([][][]byte, n)
	var errOnce sync.Once
	var computeErr error
	var wg sync.WaitGroup

	for r := 0; r < n; r++ {
		wg.Add(1)
		go func(rowIdx int) {
			defer wg.Done()
			cellProofs, err := cda.ComputeOpenProofCell(rlncCodec, eds, pg.kzg, rowIdx, colIdx)
			if err != nil {
				errOnce.Do(func() {
					computeErr = fmt.Errorf("failed to compute open proof cell at row %d: %w", rowIdx, err)
				})
				return
			}
			proofs[rowIdx] = cellProofs
		}(r)
	}
	wg.Wait()

	if computeErr != nil {
		return nil, computeErr
	}

	return proofs, nil
}

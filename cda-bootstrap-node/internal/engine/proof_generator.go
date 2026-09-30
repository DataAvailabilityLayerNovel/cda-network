package engine

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	rsmt2d "github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
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
// When using GnarkKZG, it leverages FK20 (Feist-Khovratovich) to amortize all N proofs
// across all k pieces in O(k * N log N) time (<50ms for N=128), eliminating the legacy O(k * N^2) bottleneck.
func (pg *ProofGenerator) GenerateColumnProofs(colIdx int, columnData [][]byte) ([][][]byte, error) {
	n := len(columnData)
	if n == 0 {
		return nil, fmt.Errorf("column data is empty")
	}

	// Fast-path: FK20 via GnarkKZG
	if gnarkKZG, ok := pg.kzg.(*cda.GnarkKZG); ok {
		// Ensure FK20 engine is initialized for domain size n
		if gnarkKZG.FK20() == nil || gnarkKZG.FK20().Domain().Cardinality != uint64(n) {
			if err := gnarkKZG.InitFK20(n); err != nil {
				return nil, fmt.Errorf("failed to initialize FK20 engine for size %d: %w", n, err)
			}
		}

		engine := gnarkKZG.FK20()
		if engine != nil {
			cellSize := len(columnData[0])
			pieceSize := cellSize / pg.k
			proofs := make([][][]byte, n)
			for r := 0; r < n; r++ {
				proofs[r] = make([][]byte, pg.k)
			}

			var wg sync.WaitGroup
			var errOnce sync.Once
			var fkErr error

			for piece := 0; piece < pg.k; piece++ {
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					scalars := make([]fr.Element, n)
					start := p * pieceSize
					end := start + pieceSize
					for r := 0; r < n; r++ {
						scalars[r] = rlnc.FoldMultiScalar(columnData[r][start:end])
					}

					pProofs, err := engine.ComputeAllProofs(scalars)
					if err != nil {
						errOnce.Do(func() {
							fkErr = fmt.Errorf("FK20 compute all proofs failed for piece %d: %w", p, err)
						})
						return
					}

					for r := 0; r < n; r++ {
						proofs[r][p] = []byte(pProofs[r])
					}
				}(piece)
			}
			wg.Wait()

			if fkErr != nil {
				return nil, fkErr
			}
			return proofs, nil
		}
	}

	// Fallback path: Cell-by-cell legacy computation
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

	var sem chan struct{}
	if envLimit := strings.TrimSpace(os.Getenv("BOOTSTRAP_PROOF_GEN_SEM")); envLimit != "" {
		if limit, err := strconv.Atoi(envLimit); err == nil && limit > 0 {
			sem = make(chan struct{}, limit)
		}
	}

	for r := 0; r < n; r++ {
		if sem != nil {
			sem <- struct{}{}
		}
		wg.Add(1)
		go func(rowIdx int) {
			defer wg.Done()
			if sem != nil {
				defer func() { <-sem }()
			}
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

package engine

import (
	"fmt"
	"os"
	"sort"
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

// GenerateMultiColumnProofs generates opening proofs for multiple columns simultaneously.
// When using GnarkKZG, it packs all polynomials across all columns and pieces into a single
// multi-polynomial batch, computing all proofs with exactly 1 global Montgomery inversion
// and zero-alloc memory arena serialization.
func (pg *ProofGenerator) GenerateMultiColumnProofs(columnsData map[int][][]byte) (map[int][][][]byte, error) {
	if len(columnsData) == 0 {
		return make(map[int][][][]byte), nil
	}

	var n int
	cols := make([]int, 0, len(columnsData))
	for colIdx, data := range columnsData {
		cols = append(cols, colIdx)
		if n == 0 {
			n = len(data)
		} else if len(data) != n {
			return nil, fmt.Errorf("inconsistent column heights in batch: expected %d, got %d for col %d", n, len(data), colIdx)
		}
	}
	sort.Ints(cols)

	if n == 0 {
		return nil, fmt.Errorf("column data is empty")
	}

	// Fast-path: FK20 Multi-Poly Batching via GnarkKZG
	if gnarkKZG, ok := pg.kzg.(*cda.GnarkKZG); ok {
		engine, err := gnarkKZG.GetOrInitFK20(n)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize FK20 engine for size %d: %w", n, err)
		}
		if engine != nil {
			numCols := len(cols)
			totalPolys := numCols * pg.k
			polys := make([][]fr.Element, totalPolys)

			type polyIndex struct {
				colIdx int
				piece  int
			}
			polyMap := make([]polyIndex, totalPolys)

			idx := 0
			var foldWg sync.WaitGroup
			for _, colIdx := range cols {
				colData := columnsData[colIdx]
				cellSize := len(colData[0])
				pieceSize := cellSize / pg.k

				for p := 0; p < pg.k; p++ {
					pIdx := idx
					polyMap[pIdx] = polyIndex{colIdx: colIdx, piece: p}
					idx++

					foldWg.Add(1)
					go func(outIdx, piece int, data [][]byte) {
						defer foldWg.Done()
						scalars := make([]fr.Element, n)
						start := piece * pieceSize
						end := start + pieceSize
						for r := 0; r < n; r++ {
							scalars[r] = rlnc.FoldMultiScalar(data[r][start:end])
						}
						polys[outIdx] = scalars
					}(pIdx, p, colData)
				}
			}
			foldWg.Wait()

			// Batch compute all polynomials with 1 Montgomery inversion & zero-alloc arena
			allProofs, err := engine.ComputeMultiPolyProofs(polys)
			if err != nil {
				return nil, fmt.Errorf("FK20 ComputeMultiPolyProofs failed: %w", err)
			}

			// Unpack results into map[colIdx][row][piece][]byte
			result := make(map[int][][][]byte, numCols)
			for _, colIdx := range cols {
				result[colIdx] = make([][][]byte, n)
				for r := 0; r < n; r++ {
					result[colIdx][r] = make([][]byte, pg.k)
				}
			}

			for pIdx, ref := range polyMap {
				pProofs := allProofs[pIdx]
				for r := 0; r < n; r++ {
					result[ref.colIdx][r][ref.piece] = []byte(pProofs[r])
				}
			}

			return result, nil
		}
	}

	// Fallback path: Process each column via legacy cell computation
	result := make(map[int][][][]byte, len(cols))
	for _, colIdx := range cols {
		proofs, err := pg.generateColumnProofsLegacy(colIdx, columnsData[colIdx])
		if err != nil {
			return nil, err
		}
		result[colIdx] = proofs
	}
	return result, nil
}

// GenerateColumnProofs generates the opening proofs for Column colIdx.
// When using GnarkKZG, it leverages FK20 to amortize proofs across all k pieces.
func (pg *ProofGenerator) GenerateColumnProofs(colIdx int, columnData [][]byte) ([][][]byte, error) {
	if _, ok := pg.kzg.(*cda.GnarkKZG); ok {
		res, err := pg.GenerateMultiColumnProofs(map[int][][]byte{colIdx: columnData})
		if err != nil {
			return nil, err
		}
		return res[colIdx], nil
	}
	return pg.generateColumnProofsLegacy(colIdx, columnData)
}

func (pg *ProofGenerator) generateColumnProofsLegacy(colIdx int, columnData [][]byte) ([][][]byte, error) {
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

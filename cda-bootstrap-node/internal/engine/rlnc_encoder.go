package engine

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
)

type RLNCEncoder struct {
	k   int
	kzg cda.KZGProvider
}

func NewRLNCEncoder(k int, kzg cda.KZGProvider) *RLNCEncoder {
	return &RLNCEncoder{
		k:   k,
		kzg: kzg,
	}
}

// EvaluatePieceColumn evaluates the polynomial representing a piece-column at a given row index.
func EvaluatePieceColumn(columnData [][]byte, pieceIdx, rowIdx, k, frSize int) []byte {
	n := len(columnData)

	var eval fr.Element
	var z fr.Element
	z.SetInterface(int64(rowIdx))

	for r := 0; r < n; r++ {
		var val fr.Element
		val.SetBytes(columnData[r][pieceIdx*frSize : (pieceIdx+1)*frSize])

		var zPower fr.Element
		zPower.Exp(z, big.NewInt(int64(r)))

		var term fr.Element
		term.Mul(&val, &zPower)
		eval.Add(&eval, &term)
	}

	bytesVal := eval.Bytes()
	return append([]byte(nil), bytesVal[:]...)
}

// EncodeRowNSeeds performs RLNC encoding on the cell of a specific row and column
// to generate `count` distinct random seed pieces (d_i, g_i, P_i) using random coefficients g_i.
func (e *RLNCEncoder) EncodeRowNSeeds(row int, col int, columnData [][]byte, pieceProofs [][]byte, count int) ([]*cda.ReceivedPiece, error) {
	if len(pieceProofs) != e.k {
		return nil, fmt.Errorf("invalid piece proofs count: got %d, expected %d", len(pieceProofs), e.k)
	}
	if count <= 0 {
		count = e.k
	}

	// 1. Evaluate piece columns at row to find fragments in evaluation form
	frSize := len(columnData[0]) / e.k
	fragments := make([][]byte, e.k)
	for j := 0; j < e.k; j++ {
		fragments[j] = EvaluatePieceColumn(columnData, j, row, e.k, frSize)
	}

	openingProofs := make([]cda.OpeningProof, e.k)
	for i := 0; i < e.k; i++ {
		openingProofs[i] = cda.OpeningProof(pieceProofs[i])
	}

	pieces := make([]*cda.ReceivedPiece, count)
	var generatedCoeffs [][]byte

	for seedIdx := 0; seedIdx < count; seedIdx++ {
		// 2. Generate random coefficients in Fr and check linear independence
		var coeffs []byte
		for {
			coeffs = make([]byte, e.k)
			for i := 0; i < e.k; i++ {
				b := make([]byte, 1)
				if _, err := rand.Read(b); err != nil {
					coeffs[i] = byte(i + 1)
				} else {
					coeffs[i] = (b[0] % 30) + 1 // [1, 30]
				}
			}
			if isLinearlyIndependent(generatedCoeffs, coeffs, e.k) {
				break
			}
		}
		generatedCoeffs = append(generatedCoeffs, coeffs)
		if len(generatedCoeffs) == e.k {
			generatedCoeffs = nil
		}

		// 3. Compute RLNC coded data: d_i = sum(g_{i,j} * S_j)
		shareSize := len(fragments[0])
		codedData := make([]byte, shareSize)
		for j := 0; j < e.k; j++ {
			vectorMulAddFrLocal(codedData, fragments[j], coeffs[j])
		}

		// 4. Compute combined proof: P_i = sum(g_{i,j} * Pi_j)
		combinedProof, err := e.kzg.CombineProofs(openingProofs, coeffs)
		if err != nil {
			return nil, fmt.Errorf("failed to combine proofs for seed %d: %w", seedIdx, err)
		}

		pieces[seedIdx] = &cda.ReceivedPiece{
			Row: row,
			Col: col,
			Data: rlnc.PieceData{
				Data:   codedData,
				Coeffs: coeffs,
			},
			Proof: combinedProof,
		}
	}

	return pieces, nil
}

func (e *RLNCEncoder) EncodeRow3Seeds(row int, col int, columnData [][]byte, pieceProofs [][]byte) ([]*cda.ReceivedPiece, error) {
	return e.EncodeRowNSeeds(row, col, columnData, pieceProofs, e.k)
}

func isLinearlyIndependent(existingCoeffs [][]byte, newCoeff []byte, k int) bool {
	m := len(existingCoeffs)
	if m >= k {
		return false
	}

	A := make([][]byte, k)
	for i := 0; i < m; i++ {
		A[i] = append([]byte(nil), existingCoeffs[i]...)
	}
	A[m] = append([]byte(nil), newCoeff...)

	for i := m + 1; i < k; i++ {
		row := make([]byte, k)
		row[i] = 1
		A[i] = row
	}

	B := make([][]byte, k)
	for i := 0; i < k; i++ {
		B[i] = make([]byte, 32)
	}

	_, err := rlnc.SolveGaussian(A, B)
	return err == nil
}

func vectorMulAddFrLocal(dst, src []byte, coeff byte) {
	if coeff == 0 {
		return
	}

	var dstEl fr.Element
	var srcEl fr.Element
	var coeffEl fr.Element
	var term fr.Element

	dstEl.SetBytes(dst)
	srcEl.SetBytes(src)
	coeffEl.SetUint64(uint64(coeff))

	term.Mul(&srcEl, &coeffEl)
	dstEl.Add(&dstEl, &term)

	out := dstEl.Bytes()
	copy(dst, out[32-len(dst):])
}

package engine

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

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

// ComputeZPowers computes [z^0, z^1, ..., z^{n-1}] where z = rowIdx in Fr.
func ComputeZPowers(rowIdx, n int) []fr.Element {
	var z fr.Element
	z.SetInterface(int64(rowIdx))

	zPowers := make([]fr.Element, n)
	if n > 0 {
		zPowers[0].SetOne()
		for r := 1; r < n; r++ {
			zPowers[r].Mul(&zPowers[r-1], &z)
		}
	}
	return zPowers
}

// EvaluatePieceColumn evaluates the polynomial representing a piece-column at a given row index.
func EvaluatePieceColumn(columnData [][]byte, pieceIdx, rowIdx, k, frSize int) []byte {
	n := len(columnData)
	zPowers := ComputeZPowers(rowIdx, n)
	return EvaluatePieceColumnWithPowers(columnData, pieceIdx, rowIdx, k, frSize, zPowers)
}

// EvaluatePieceColumnWithPowers evaluates the polynomial representing a piece-column using precomputed powers.
func EvaluatePieceColumnWithPowers(columnData [][]byte, pieceIdx, rowIdx, k, frSize int, zPowers []fr.Element) []byte {
	n := len(columnData)
	numChunks := (frSize + 31) / 32
	if numChunks <= 0 {
		numChunks = 1
	}

	result := make([]byte, numChunks*32)

	for m := 0; m < numChunks; m++ {
		var eval fr.Element
		for r := 0; r < n; r++ {
			chunkStart := pieceIdx*frSize + m*32
			chunkEnd := chunkStart + 32
			if chunkEnd > (pieceIdx+1)*frSize {
				chunkEnd = (pieceIdx + 1) * frSize
			}

			var val fr.Element
			if chunkStart < len(columnData[r]) {
				end := chunkEnd
				if end > len(columnData[r]) {
					end = len(columnData[r])
				}
				val.SetBytes(columnData[r][chunkStart:end])
			}

			var term fr.Element
			term.Mul(&val, &zPowers[r])
			eval.Add(&eval, &term)
		}

		bytesVal := eval.Bytes()
		copy(result[m*32:(m+1)*32], bytesVal[:])
	}

	return result
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

	// 1. Precompute zPowers once for the entire row and evaluate all k piece columns
	n := len(columnData)
	zPowers := ComputeZPowers(row, n)
	frSize := len(columnData[0]) / e.k
	fragments := make([][]byte, e.k)
	for j := 0; j < e.k; j++ {
		fragments[j] = EvaluatePieceColumnWithPowers(columnData, j, row, e.k, frSize, zPowers)
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
			coeffs = make([]byte, 2*e.k)
			for i := 0; i < e.k; i++ {
				b := make([]byte, 2)
				if _, err := rand.Read(b); err != nil {
					binary.BigEndian.PutUint16(coeffs[i*2:], uint16(i+1))
				} else {
					val := (binary.BigEndian.Uint16(b) % 1000) + 1
					binary.BigEndian.PutUint16(coeffs[i*2:], val)
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
			cVal := binary.BigEndian.Uint16(coeffs[j*2 : (j+1)*2])
			vectorMulAddFrLocal(codedData, fragments[j], cVal)
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
		row := make([]byte, 2*k)
		row[2*i+1] = 1
		A[i] = row
	}

	B := make([][]byte, k)
	for i := 0; i < k; i++ {
		B[i] = make([]byte, 32)
	}

	_, err := rlnc.SolveGaussianFr(A, B)
	return err == nil
}

func vectorMulAddFrLocal(dst, src []byte, coeff uint16) {
	rlnc.VectorMulAddFr(dst, src, coeff)
}

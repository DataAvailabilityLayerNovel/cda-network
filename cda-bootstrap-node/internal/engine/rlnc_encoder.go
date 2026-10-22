package engine

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
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

// ComputeZPowersFromPoint computes [z^0, z^1, ..., z^{n-1}] for a given evaluation point z in Fr.
func ComputeZPowersFromPoint(z fr.Element, n int) []fr.Element {
	zPowers := make([]fr.Element, n)
	if n > 0 {
		zPowers[0].SetOne()
		for r := 1; r < n; r++ {
			zPowers[r].Mul(&zPowers[r-1], &z)
		}
	}
	return zPowers
}

// ComputeZPowers computes [z^0, z^1, ..., z^{n-1}] where z = rowIdx in Fr.
func ComputeZPowers(rowIdx, n int) []fr.Element {
	var z fr.Element
	z.SetInterface(int64(rowIdx))
	return ComputeZPowersFromPoint(z, n)
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
	var z fr.Element
	gnarkKZG, isGnark := e.kzg.(*cda.GnarkKZG)
	if isGnark {
		z = gnarkKZG.GnarkGetEvaluationPointForDomain(row, n)
	} else {
		z = e.kzg.GetEvaluationPoint(row)
	}
	zPowers := ComputeZPowersFromPoint(z, n)
	frSize := len(columnData[0]) / e.k
	fragments := make([][]byte, e.k)
	for j := 0; j < e.k; j++ {
		fragments[j] = EvaluatePieceColumnWithPowers(columnData, j, row, e.k, frSize, zPowers)
	}

	openingProofs := make([]cda.OpeningProof, e.k)
	for i := 0; i < e.k; i++ {
		openingProofs[i] = cda.OpeningProof(pieceProofs[i])
	}

	// Pre-decompress G1Affine proof points once per row when using GnarkKZG
	var unpackedPts []bls12381.G1Affine
	var unpackedVals []fr.Element
	if isGnark {
		pts, vals, err := gnarkKZG.GnarkUnpackProofs(openingProofs)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack row %d opening proofs: %w", row, err)
		}
		unpackedPts = pts
		unpackedVals = vals
	}

	pieces := make([]*cda.ReceivedPiece, count)
	basis := NewEchelonBasis(e.k)

	for seedIdx := 0; seedIdx < count; seedIdx++ {
		// When transitioning to a new group of e.k seeds (e.g. from Primary to Backup),
		// reset the basis so that the backup seeds are mutually independent among themselves.
		if seedIdx > 0 && seedIdx%e.k == 0 {
			basis.Reset()
		}

		// 2. Generate coefficients in Fr guaranteed to be linearly independent via Row Echelon reduction
		var coeffs []byte
		attempts := 0
		for {
			attempts++
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

			// Deterministic fallback if random sampling exceeds 50 attempts
			if attempts > 50 {
				targetCol := basis.Rank()
				if targetCol < e.k {
					binary.BigEndian.PutUint16(coeffs[targetCol*2:], uint16(1001+attempts))
				}
			}

			if basis.TryAdd(coeffs) {
				break
			}
		}

		// 3. Compute RLNC coded data: d_i = sum(g_{i,j} * S_j)
		shareSize := len(fragments[0])
		codedData := make([]byte, shareSize)
		for j := 0; j < e.k; j++ {
			cVal := binary.BigEndian.Uint16(coeffs[j*2 : (j+1)*2])
			vectorMulAddFrLocal(codedData, fragments[j], cVal)
		}

		// 4. Compute combined proof: P_i = sum(g_{i,j} * Pi_j)
		var combinedProof cda.OpeningProof
		var err error
		if isGnark && unpackedVals != nil {
			combinedProof, err = gnarkKZG.GnarkCombineUnpackedProofs(unpackedPts, unpackedVals, coeffs)
		} else {
			combinedProof, err = e.kzg.CombineProofs(openingProofs, coeffs)
		}
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

// EchelonBasis tracks a basis of vectors in Fr^k in reduced row echelon form.
type EchelonBasis struct {
	k     int
	basis [][]fr.Element
}

func NewEchelonBasis(k int) *EchelonBasis {
	return &EchelonBasis{
		k:     k,
		basis: make([][]fr.Element, 0, k),
	}
}

// Reset clears the basis so a new independent set of vectors can be formed.
func (eb *EchelonBasis) Reset() {
	eb.basis = eb.basis[:0]
}

// Rank returns the current dimension of the vector space spanned by the basis.
func (eb *EchelonBasis) Rank() int {
	return len(eb.basis)
}

// TryAdd tests if the given coefficient vector is linearly independent of the current basis.
// If it is independent, it is added to the basis in reduced row echelon form and returns true.
// If it is linearly dependent (remainder is 0), returns false.
func (eb *EchelonBasis) TryAdd(rawCoeffs []byte) bool {
	if len(eb.basis) >= eb.k {
		return false
	}

	// 1. Convert raw bytes to []fr.Element
	row := make([]fr.Element, eb.k)
	if len(rawCoeffs) == 2*eb.k {
		for j := 0; j < eb.k; j++ {
			val := binary.BigEndian.Uint16(rawCoeffs[j*2 : (j+1)*2])
			row[j].SetUint64(uint64(val))
		}
	} else if len(rawCoeffs) == eb.k {
		for j := 0; j < eb.k; j++ {
			row[j].SetUint64(uint64(rawCoeffs[j]))
		}
	} else {
		return false
	}

	// 2. Eliminate row against current basis
	for i := 0; i < len(eb.basis); i++ {
		pivotCol := -1
		for c := 0; c < eb.k; c++ {
			if !eb.basis[i][c].IsZero() {
				pivotCol = c
				break
			}
		}
		if pivotCol == -1 {
			continue
		}

		if !row[pivotCol].IsZero() {
			factor := row[pivotCol]
			for c := pivotCol; c < eb.k; c++ {
				var term fr.Element
				term.Mul(&eb.basis[i][c], &factor)
				row[c].Sub(&row[c], &term)
			}
		}
	}

	// 3. Find leading non-zero element in remainder
	pivotCol := -1
	for c := 0; c < eb.k; c++ {
		if !row[c].IsZero() {
			pivotCol = c
			break
		}
	}

	if pivotCol == -1 {
		// All zeros: linearly dependent!
		return false
	}

	// 4. Normalize the row so that basis[pivotCol] == 1
	var inv fr.Element
	inv.Inverse(&row[pivotCol])
	for c := pivotCol; c < eb.k; c++ {
		row[c].Mul(&row[c], &inv)
	}

	eb.basis = append(eb.basis, row)
	return true
}

func isLinearlyIndependent(existingCoeffs [][]byte, newCoeff []byte, k int) bool {
	if len(existingCoeffs) >= k {
		return false
	}
	basis := NewEchelonBasis(k)
	for _, c := range existingCoeffs {
		_ = basis.TryAdd(c)
	}
	return basis.TryAdd(newCoeff)
}

func vectorMulAddFrLocal(dst, src []byte, coeff uint16) {
	rlnc.VectorMulAddFr(dst, src, coeff)
}

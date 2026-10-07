package engine

import (
	"encoding/binary"

	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
)

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

// IsLinearlyIndependent checks if the new coefficient vector is linearly independent
// of the existing ones using Row Echelon Gaussian elimination over BLS12-381 Fr.
func IsLinearlyIndependent(existingCoeffs [][]byte, newCoeff []byte, k int) bool {
	// Fast-path: The first vector (m == 0) is always linearly independent if it is non-zero
	if len(existingCoeffs) == 0 {
		for _, b := range newCoeff {
			if b != 0 {
				return true
			}
		}
		return false
	}

	basis := NewEchelonBasis(k)
	for _, c := range existingCoeffs {
		_ = basis.TryAdd(c)
	}
	if basis.Rank() >= k {
		return false
	}
	return basis.TryAdd(newCoeff)
}

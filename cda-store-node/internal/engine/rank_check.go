package engine

import (
	rlnc "github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
)

// IsLinearlyIndependent checks if the new coefficient vector is linearly independent
// of the existing ones. It does so by constructing a k x k matrix (padding with standard
// basis vectors) and attempting to solve it using rlnc.SolveGaussian.
func IsLinearlyIndependent(existingCoeffs [][]byte, newCoeff []byte, k int) bool {
	m := len(existingCoeffs)
	if m >= k {
		return false // A vector space of dimension k cannot have more than k independent vectors
	}

	// 1. Build a k x k matrix A
	A := make([][]byte, k)
	for i := 0; i < m; i++ {
		A[i] = append([]byte(nil), existingCoeffs[i]...)
	}
	A[m] = append([]byte(nil), newCoeff...)

	// Pad the remaining rows with standard basis vectors: [0..0, 1, 0..0]
	// where 1 is at index i.
	for i := m + 1; i < k; i++ {
		row := make([]byte, k)
		row[i] = 1 // 1 in Fr is represented as 1 at index i (SetUint64(1) works)
		A[i] = row
	}

	// 2. Create a dummy B vector of size k x 32 bytes (must match frSymbolSize)
	B := make([][]byte, k)
	for i := 0; i < k; i++ {
		B[i] = make([]byte, 32)
	}

	// 3. Try to solve the system A * X = B.
	// If it fails with "singular matrix", the matrix is not invertible, meaning
	// the new vector is linearly dependent on the existing ones.
	_, err := rlnc.SolveGaussian(A, B)
	return err == nil
}

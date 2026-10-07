package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsLinearlyIndependent(t *testing.T) {
	k := 8

	// 1. First non-zero vector is always independent
	v1 := make([]byte, k)
	v1[0] = 1
	assert.True(t, IsLinearlyIndependent(nil, v1, k))
	assert.True(t, IsLinearlyIndependent([][]byte{}, v1, k))

	// All-zero vector is not independent
	vZero := make([]byte, k)
	assert.False(t, IsLinearlyIndependent(nil, vZero, k))

	// 2. Second independent vector
	v2 := make([]byte, k)
	v2[1] = 1
	existing := [][]byte{v1}
	assert.True(t, IsLinearlyIndependent(existing, v2, k))

	// 3. Dependent vector (same as v1)
	assert.False(t, IsLinearlyIndependent(existing, v1, k))

	// 4. Exceeding dimension k
	full := make([][]byte, k)
	for i := 0; i < k; i++ {
		row := make([]byte, k)
		row[i] = 1
		full[i] = row
	}
	extra := make([]byte, k)
	extra[2] = 1
	assert.False(t, IsLinearlyIndependent(full, extra, k))
}

func TestIsLinearlyIndependent_Uint16Coeffs(t *testing.T) {
	k := 4

	// Vector 1: [1, 2, 0, 0] in uint16 big endian
	v1 := []byte{0, 1, 0, 2, 0, 0, 0, 0}
	assert.True(t, IsLinearlyIndependent(nil, v1, k))

	// Vector 2: [0, 0, 3, 4] in uint16 big endian
	v2 := []byte{0, 0, 0, 0, 0, 3, 0, 4}
	existing := [][]byte{v1}
	assert.True(t, IsLinearlyIndependent(existing, v2, k))

	// Vector 3: Linear combination: 2*v1 + v2 = [2, 4, 3, 4] -> must be DEPENDENT!
	v3 := []byte{0, 2, 0, 4, 0, 3, 0, 4}
	existing = [][]byte{v1, v2}
	assert.False(t, IsLinearlyIndependent(existing, v3, k))

	// Vector 4: Independent: [0, 1, 0, 0]
	v4 := []byte{0, 0, 0, 1, 0, 0, 0, 0}
	assert.True(t, IsLinearlyIndependent(existing, v4, k))
}


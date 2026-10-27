package service

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLightNode_BlacklistPeer(t *testing.T) {
	svc := &APIService{
		blacklistedPeers: make(map[string]time.Time),
	}

	peerA := "12D3KooWTestPeerA"
	peerB := "12D3KooWTestPeerB"

	// Initially not blacklisted
	assert.False(t, svc.IsPeerBlacklisted(peerA))
	assert.False(t, svc.IsPeerBlacklisted(peerB))

	// Blacklist peerA for 1 hour
	svc.BlacklistPeer(peerA, 1*time.Hour)
	assert.True(t, svc.IsPeerBlacklisted(peerA))
	assert.False(t, svc.IsPeerBlacklisted(peerB))

	// Blacklist peerB with a negative duration (already expired)
	svc.BlacklistPeer(peerB, -1*time.Second)
	assert.False(t, svc.IsPeerBlacklisted(peerB))
}

func TestLightNode_Tier2RSRepair(t *testing.T) {
	k := 8
	n := 2 * k
	cellSize := 64

	// Generate deterministic row shares using Leopard RS
	enc, err := reedsolomon.New(k, k, reedsolomon.WithLeopardGF(true))
	require.NoError(t, err)

	rowShares := make([][]byte, n)
	for i := 0; i < k; i++ {
		rowShares[i] = make([]byte, cellSize)
		_, _ = rand.Read(rowShares[i])
	}
	for i := k; i < n; i++ {
		rowShares[i] = make([]byte, cellSize)
	}

	// RS encode parity shares
	err = enc.Encode(rowShares)
	require.NoError(t, err)

	targetCol := 3
	expectedCell := make([]byte, cellSize)
	copy(expectedCell, rowShares[targetCol])

	// Corrupt matrix: erase targetCol and k-1 other parity shares
	corruptedShares := make([][]byte, n)
	for i := 0; i < n; i++ {
		if i == targetCol || i >= n-(k-1) {
			corruptedShares[i] = nil // missing share MUST be nil
		} else {
			corruptedShares[i] = make([]byte, cellSize)
			copy(corruptedShares[i], rowShares[i])
		}
	}

	// Reconstruct using Leopard RS
	err = enc.Reconstruct(corruptedShares)
	require.NoError(t, err)

	assert.Equal(t, expectedCell, corruptedShares[targetCol], "Reconstructed cell must match original cell byte-for-byte")
}

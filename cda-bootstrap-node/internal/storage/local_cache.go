package storage

import (
	"sync"
	"time"
)

type CachedColumn struct {
	BlockID      string
	ColIdx       int
	ColumnData   [][]byte
	PieceCommits [][]byte
	Proofs       [][][]byte // row -> pieceIdx -> proof bytes
	CreatedAt    time.Time
}

type LocalCache struct {
	mu     sync.RWMutex
	cached map[string]*CachedColumn
}

func NewLocalCache() *LocalCache {
	return &LocalCache{
		cached: make(map[string]*CachedColumn),
	}
}

func (c *LocalCache) Store(blockID string, colIdx int, colData [][]byte, pieceCommits [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cached[blockID] = &CachedColumn{
		BlockID:      blockID,
		ColIdx:       colIdx,
		ColumnData:   colData,
		PieceCommits: pieceCommits,
		CreatedAt:    time.Now(),
	}
}

func (c *LocalCache) SetProofs(blockID string, proofs [][][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.cached[blockID]; exists {
		entry.Proofs = proofs
	}
}

func (c *LocalCache) Get(blockID string) (*CachedColumn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.cached[blockID]
	return entry, exists
}

func (c *LocalCache) Prune(olderThan time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for blockID, entry := range c.cached {
		if now.Sub(entry.CreatedAt) > olderThan {
			delete(c.cached, blockID)
		}
	}
}

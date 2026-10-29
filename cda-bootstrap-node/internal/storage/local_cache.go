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
	Proofs       [][][]byte     // row -> pieceIdx -> proof bytes
	SeedBatches  map[int][]byte // netRow -> encoded BinaryBatchSeedRequest
	CreatedAt    time.Time
}

type LocalCache struct {
	mu     sync.RWMutex
	cached map[string]map[int]*CachedColumn
}

func NewLocalCache() *LocalCache {
	return &LocalCache{
		cached: make(map[string]map[int]*CachedColumn),
	}
}

func (c *LocalCache) Store(blockID string, colIdx int, colData [][]byte, pieceCommits [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cached[blockID] == nil {
		c.cached[blockID] = make(map[int]*CachedColumn)
	}
	c.cached[blockID][colIdx] = &CachedColumn{
		BlockID:      blockID,
		ColIdx:       colIdx,
		ColumnData:   colData,
		PieceCommits: pieceCommits,
		SeedBatches:  make(map[int][]byte),
		CreatedAt:    time.Now(),
	}
}

func (c *LocalCache) SetSeedBatchForColRow(blockID string, colIdx int, netRow int, encodedBatch []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cols, exists := c.cached[blockID]; exists {
		if entry, ok := cols[colIdx]; ok {
			if entry.SeedBatches == nil {
				entry.SeedBatches = make(map[int][]byte)
			}
			entry.SeedBatches[netRow] = encodedBatch
		}
	}
}

func (c *LocalCache) GetSeedBatchForColRow(blockID string, colIdx int, netRow int) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if cols, exists := c.cached[blockID]; exists {
		if entry, ok := cols[colIdx]; ok && entry.SeedBatches != nil {
			b, found := entry.SeedBatches[netRow]
			return b, found && len(b) > 0
		}
	}
	return nil, false
}

func (c *LocalCache) SetProofs(blockID string, proofs [][][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cols, exists := c.cached[blockID]; exists {
		for _, entry := range cols {
			entry.Proofs = proofs
		}
	}
}

func (c *LocalCache) SetProofsForCol(blockID string, colIdx int, proofs [][][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cols, exists := c.cached[blockID]; exists {
		if entry, ok := cols[colIdx]; ok {
			entry.Proofs = proofs
		}
	}
}

func (c *LocalCache) Get(blockID string) (*CachedColumn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cols, exists := c.cached[blockID]
	if !exists || len(cols) == 0 {
		return nil, false
	}
	var best *CachedColumn
	for _, entry := range cols {
		if best == nil || entry.ColIdx < best.ColIdx {
			best = entry
		}
	}
	return best, best != nil
}

func (c *LocalCache) GetCol(blockID string, colIdx int) (*CachedColumn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cols, exists := c.cached[blockID]
	if !exists {
		return nil, false
	}
	entry, ok := cols[colIdx]
	return entry, ok
}

func (c *LocalCache) GetAll(blockID string) []*CachedColumn {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cols, exists := c.cached[blockID]
	if !exists {
		return nil
	}
	res := make([]*CachedColumn, 0, len(cols))
	for _, entry := range cols {
		res = append(res, entry)
	}
	return res
}

func (c *LocalCache) RecentBlockIDs(limit int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	type blkTime struct {
		id string
		t  time.Time
	}
	var list []blkTime
	for bid, cols := range c.cached {
		var latest time.Time
		for _, entry := range cols {
			if entry.CreatedAt.After(latest) {
				latest = entry.CreatedAt
			}
		}
		list = append(list, blkTime{id: bid, t: latest})
	}
	// Sort newest first
	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ {
			if list[j].t.After(list[i].t) {
				list[i], list[j] = list[j], list[i]
			}
		}
	}
	if limit > 0 && len(list) > limit {
		list = list[:limit]
	}
	out := make([]string, len(list))
	for i, item := range list {
		out[i] = item.id
	}
	return out
}

func (c *LocalCache) Prune(olderThan time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for blockID, cols := range c.cached {
		for colIdx, entry := range cols {
			if now.Sub(entry.CreatedAt) > olderThan {
				delete(cols, colIdx)
			}
		}
		if len(cols) == 0 {
			delete(c.cached, blockID)
		}
	}
}


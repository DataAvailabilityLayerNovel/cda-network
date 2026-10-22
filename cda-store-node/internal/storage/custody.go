package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/dgraph-io/badger/v4"
)

type AnchoredData struct {
	ColIdx       int      `json:"col_idx"`
	PieceCommits [][]byte `json:"piece_commits"`
}

type CustodyStore struct {
	db              *badger.DB
	port            int
	countMu         sync.RWMutex
	pieceCountCache map[string]int
	memMu           sync.RWMutex
	piecesCache     map[string][]cda.ReceivedPiece
	recodedCache    map[string][]cda.ReceivedPiece
	anchorCache     map[string][][]byte
}

func NewCustodyStore(port int) *CustodyStore {
	store := &CustodyStore{
		port:            port,
		pieceCountCache: make(map[string]int),
		piecesCache:     make(map[string][]cda.ReceivedPiece),
		recodedCache:    make(map[string][]cda.ReceivedPiece),
		anchorCache:     make(map[string][][]byte),
	}

	if port > 0 {
		baseDir := fmt.Sprintf("data/store_%d", port)
		dbPath := filepath.Join(baseDir, "badger")
		_ = os.MkdirAll(dbPath, 0755)

		opts := badger.DefaultOptions(dbPath).
			WithLogger(nil).
			WithValueLogFileSize(12 * 1024 * 1024). // 12 MB value log file size
			WithMemTableSize(8 * 1024 * 1024).     // 8 MB memtable size
			WithValueThreshold(256)                 // values larger than 256 bytes go to vlog
		db, err := badger.Open(opts)
		if err != nil {
			panic(fmt.Sprintf("failed to open badger db: %v", err))
		}
		store.db = db
	}

	return store
}

func (s *CustodyStore) Port() int {
	return s.port
}

func (s *CustodyStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *CustodyStore) AnchorCommitments(blockID string, colIdx int, pieceCommits [][]byte) {
	keyStr := fmt.Sprintf("anchor_%s_%d", blockID, colIdx)

	s.memMu.Lock()
	s.anchorCache[keyStr] = pieceCommits
	s.memMu.Unlock()

	if s.db == nil {
		return
	}
	valData, err := json.Marshal(AnchoredData{
		ColIdx:       colIdx,
		PieceCommits: pieceCommits,
	})
	if err != nil {
		return
	}

	go func(key []byte, val []byte) {
		_ = s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, val)
		})
	}([]byte(keyStr), valData)
}

func (s *CustodyStore) GetAnchoredCommitments(blockID string, colIdx int) ([][]byte, bool) {
	keyStr := fmt.Sprintf("anchor_%s_%d", blockID, colIdx)

	s.memMu.RLock()
	if commits, ok := s.anchorCache[keyStr]; ok {
		s.memMu.RUnlock()
		return commits, true
	}
	s.memMu.RUnlock()

	if s.db == nil {
		return nil, false
	}
	key := []byte(keyStr)
	var pieceCommits [][]byte
	var ok bool

	_ = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var data AnchoredData
			if err := json.Unmarshal(val, &data); err != nil {
				return err
			}
			pieceCommits = data.PieceCommits
			ok = true
			return nil
		})
	})

	if ok {
		s.memMu.Lock()
		s.anchorCache[keyStr] = pieceCommits
		s.memMu.Unlock()
	}

	return pieceCommits, ok
}

func (s *CustodyStore) StorePiece(blockID string, row, col int, piece cda.ReceivedPiece) {
	keyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)

	s.memMu.Lock()
	pieces := s.piecesCache[keyStr]
	pieces = append(pieces, piece)
	s.piecesCache[keyStr] = pieces
	newCount := len(pieces)

	// Make defensive copy to prevent data race with background persistence
	pList := make([]cda.ReceivedPiece, newCount)
	copy(pList, pieces)

	s.countMu.Lock()
	s.pieceCountCache[keyStr] = newCount
	s.countMu.Unlock()
	s.memMu.Unlock()

	if s.db != nil {
		go func(k string, toSave []cda.ReceivedPiece) {
			valData, err := json.Marshal(toSave)
			if err != nil {
				return
			}
			_ = s.db.Update(func(txn *badger.Txn) error {
				// Anti-regression: do not overwrite DB with fewer pieces if an out-of-order write occurs
				item, err := txn.Get([]byte(k))
				if err == nil {
					var existing []cda.ReceivedPiece
					if err := item.Value(func(val []byte) error {
						return json.Unmarshal(val, &existing)
					}); err == nil && len(existing) >= len(toSave) {
						return nil // Existing state has equal or more pieces; do not overwrite!
					}
				}
				return txn.Set([]byte(k), valData)
			})
		}(keyStr, pList)
	}
}

func (s *CustodyStore) GetPieces(blockID string, row, col int) []cda.ReceivedPiece {
	keyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)

	s.memMu.RLock()
	if pieces, ok := s.piecesCache[keyStr]; ok {
		res := make([]cda.ReceivedPiece, len(pieces))
		copy(res, pieces)
		s.memMu.RUnlock()
		return res
	}
	s.memMu.RUnlock()

	if s.db == nil {
		return nil
	}
	key := []byte(keyStr)
	var pieces []cda.ReceivedPiece

	_ = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &pieces)
		})
	})

	if len(pieces) > 0 {
		s.memMu.Lock()
		s.piecesCache[keyStr] = pieces
		s.memMu.Unlock()
	}

	return pieces
}

func (s *CustodyStore) StoreRecodedPiece(blockID string, row, col int, piece cda.ReceivedPiece) {
	keyStr := fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col)
	pieces := []cda.ReceivedPiece{piece}

	s.memMu.Lock()
	s.recodedCache[keyStr] = pieces
	s.memMu.Unlock()

	if s.db != nil {
		go func() {
			valData, err := json.Marshal(pieces)
			if err != nil {
				return
			}
			_ = s.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(keyStr), valData)
			})
		}()
	}
}

func (s *CustodyStore) DeleteRecodedPieces(blockID string, row, col int) {
	keyStr := fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col)

	s.memMu.Lock()
	delete(s.recodedCache, keyStr)
	s.memMu.Unlock()

	if s.db != nil {
		go func(k []byte) {
			_ = s.db.Update(func(txn *badger.Txn) error {
				return txn.Delete(k)
			})
		}([]byte(keyStr))
	}
}

func (s *CustodyStore) GetRecodedPieces(blockID string, row, col int) []cda.ReceivedPiece {
	keyStr := fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col)

	s.memMu.RLock()
	if pieces, ok := s.recodedCache[keyStr]; ok {
		res := make([]cda.ReceivedPiece, len(pieces))
		copy(res, pieces)
		s.memMu.RUnlock()
		return res
	}
	s.memMu.RUnlock()

	if s.db == nil {
		return nil
	}
	key := []byte(keyStr)
	var pieces []cda.ReceivedPiece

	_ = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &pieces)
		})
	})

	if len(pieces) > 0 {
		s.memMu.Lock()
		s.recodedCache[keyStr] = pieces
		s.memMu.Unlock()
	}

	return pieces
}

func (s *CustodyStore) GetPieceCount(blockID string, row, col int) int {
	keyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)

	s.countMu.RLock()
	c, found := s.pieceCountCache[keyStr]
	s.countMu.RUnlock()
	if found {
		return c
	}

	s.memMu.RLock()
	if pieces, ok := s.piecesCache[keyStr]; ok {
		cnt := len(pieces)
		s.memMu.RUnlock()
		s.countMu.Lock()
		s.pieceCountCache[keyStr] = cnt
		s.countMu.Unlock()
		return cnt
	}
	s.memMu.RUnlock()

	if s.db == nil {
		return 0
	}

	key := []byte(keyStr)
	count := 0

	_ = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var pieces []cda.ReceivedPiece
			if err := json.Unmarshal(val, &pieces); err == nil {
				count = len(pieces)
			}
			return nil
		})
	})

	s.countMu.Lock()
	s.pieceCountCache[keyStr] = count
	s.countMu.Unlock()

	return count
}

// GetTotalPiecesForCell returns the total count of pieces (raw + recoded) stored for cell [row, col].
func (s *CustodyStore) GetTotalPiecesForCell(blockID string, row, col int) int {
	rawCount := s.GetPieceCount(blockID, row, col)
	recodedPieces := s.GetRecodedPieces(blockID, row, col)
	return rawCount + len(recodedPieces)
}

func (s *CustodyStore) GetTotalPieceCount() int {
	count := 0
	s.memMu.RLock()
	if len(s.piecesCache) > 0 || len(s.recodedCache) > 0 {
		for _, pieces := range s.piecesCache {
			count += len(pieces)
		}
		for _, pieces := range s.recodedCache {
			count += len(pieces)
		}
		s.memMu.RUnlock()
		return count
	}
	s.memMu.RUnlock()

	if s.db == nil {
		return 0
	}
	_ = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if strings.HasPrefix(key, "received_") || strings.HasPrefix(key, "recoded_") {
				_ = item.Value(func(val []byte) error {
					var pieces []cda.ReceivedPiece
					if err := json.Unmarshal(val, &pieces); err == nil {
						count += len(pieces)
					}
					return nil
				})
			}
		}
		return nil
	})
	return count
}

func (s *CustodyStore) GetRecodedPieceCount() int {
	count := 0
	s.memMu.RLock()
	if len(s.recodedCache) > 0 {
		for _, pieces := range s.recodedCache {
			count += len(pieces)
		}
		s.memMu.RUnlock()
		return count
	}
	s.memMu.RUnlock()

	if s.db == nil {
		return 0
	}
	_ = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("recoded_")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			_ = item.Value(func(val []byte) error {
				var pieces []cda.ReceivedPiece
				if err := json.Unmarshal(val, &pieces); err == nil {
					count += len(pieces)
				}
				return nil
			})
		}
		return nil
	})
	return count
}

func (s *CustodyStore) GetCustodyPieceCount(storesPerCol, rowIdx int) int {
	if storesPerCol <= 0 {
		return 0
	}
	count := 0
	s.memMu.RLock()
	if len(s.piecesCache) > 0 {
		for key, pieces := range s.piecesCache {
			parts := strings.Split(key, "_")
			if len(parts) >= 3 {
				rowStr := parts[len(parts)-2]
				var row int
				if _, err := fmt.Sscanf(rowStr, "%d", &row); err == nil {
					if (row % storesPerCol) == rowIdx {
						count += len(pieces)
					}
				}
			}
		}
		s.memMu.RUnlock()
		return count
	}
	s.memMu.RUnlock()

	if s.db == nil {
		return 0
	}
	_ = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("received_")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			// key format: "received_{blockID}_{row}_{col}"
			parts := strings.Split(key, "_")
			if len(parts) >= 3 {
				rowStr := parts[len(parts)-2]
				var row int
				if _, err := fmt.Sscanf(rowStr, "%d", &row); err == nil {
					if (row % storesPerCol) == rowIdx {
						_ = item.Value(func(val []byte) error {
							var pieces []cda.ReceivedPiece
							if err := json.Unmarshal(val, &pieces); err == nil {
								count += len(pieces)
							}
							return nil
						})
					}
				}
			}
		}
		return nil
	})
	return count
}

func (s *CustodyStore) IsComplete(blockID string, colIdx, k int) bool {
	if s.db == nil {
		return false
	}
	n := 2 * k
	// With K=16, N=32. Each bootstrap node manages 4 columns.
	colsPerNetCol := n / 8
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}

	complete := true
	_ = s.db.View(func(txn *badger.Txn) error {
		for c := colIdx; c < colIdx+colsPerNetCol; c++ {
			for r := 0; r < n; r++ {
				key := []byte(fmt.Sprintf("received_%s_%d_%d", blockID, r, c))
				item, err := txn.Get(key)
				if err != nil {
					complete = false
					return nil
				}
				err = item.Value(func(val []byte) error {
					var pieces []cda.ReceivedPiece
					if err := json.Unmarshal(val, &pieces); err != nil || len(pieces) < k {
						complete = false
					}
					return nil
				})
				if err != nil || !complete {
					return nil
				}
			}
		}
		return nil
	})

	return complete
}

func (s *CustodyStore) PruneRawPieces(blockID string, row, col int, rm *cda.RecipientManager) int {
	rawKeyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)
	recodedKeyStr := fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col)

	s.memMu.Lock()
	rawPieces := s.piecesCache[rawKeyStr]
	alreadyHasRecoded := len(s.recodedCache[recodedKeyStr]) > 0
	delete(s.piecesCache, rawKeyStr)
	s.countMu.Lock()
	s.pieceCountCache[rawKeyStr] = 0
	s.countMu.Unlock()
	s.memMu.Unlock()

	deletedCount := len(rawPieces)
	if deletedCount == 0 && alreadyHasRecoded {
		return 0
	}

	var compressedPiece cda.ReceivedPiece
	if !alreadyHasRecoded && len(rawPieces) > 0 {
		if len(rawPieces) >= 2 && rm != nil {
			recoded, err := rm.RecodePieces(rawPieces)
			if err == nil && recoded != nil {
				compressedPiece = *recoded
			} else {
				pieceCommits, _ := s.GetAnchoredCommitments(blockID, col)
				if recVer, vErr := rm.RecodePiecesWithVerify(rawPieces, pieceCommits, 3); vErr == nil && recVer != nil {
					compressedPiece = *recVer
				} else {
					compressedPiece = rawPieces[0]
				}
			}
		} else {
			compressedPiece = rawPieces[0]
		}

		s.memMu.Lock()
		if len(s.recodedCache[recodedKeyStr]) == 0 {
			s.recodedCache[recodedKeyStr] = []cda.ReceivedPiece{compressedPiece}
		} else {
			alreadyHasRecoded = true
		}
		s.memMu.Unlock()
	}

	if s.db != nil {
		go func(rawKey, recKey []byte, pList []cda.ReceivedPiece, compPiece cda.ReceivedPiece, hadRecoded bool) {
			_ = s.db.Update(func(txn *badger.Txn) error {
				_ = txn.Delete(rawKey)
				if !hadRecoded && len(pList) > 0 {
					_, err := txn.Get(recKey)
					if err != nil {
						valData, err := json.Marshal([]cda.ReceivedPiece{compPiece})
						if err == nil {
							_ = txn.Set(recKey, valData)
						}
					}
				}
				return nil
			})
		}([]byte(rawKeyStr), []byte(recodedKeyStr), rawPieces, compressedPiece, alreadyHasRecoded)
	}

	return deletedCount
}

func (s *CustodyStore) GetDBSize() int64 {
	if s.db == nil {
		return 0
	}
	var totalBytes int64
	_ = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			totalBytes += item.KeySize() + item.ValueSize()
		}
		return nil
	})
	return totalBytes
}


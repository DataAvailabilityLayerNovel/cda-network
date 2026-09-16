package storage

import (
	"encoding/json"
	"fmt"
	"log"
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
}

func NewCustodyStore(port int) *CustodyStore {
	store := &CustodyStore{
		port:            port,
		pieceCountCache: make(map[string]int),
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
	if s.db == nil {
		return
	}
	key := []byte(fmt.Sprintf("anchor_%s_%d", blockID, colIdx))
	valData, err := json.Marshal(AnchoredData{
		ColIdx:       colIdx,
		PieceCommits: pieceCommits,
	})
	if err != nil {
		return
	}

	_ = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, valData)
	})
}

func (s *CustodyStore) GetAnchoredCommitments(blockID string, colIdx int) ([][]byte, bool) {
	if s.db == nil {
		return nil, false
	}
	key := []byte(fmt.Sprintf("anchor_%s_%d", blockID, colIdx))
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

	return pieceCommits, ok
}

func (s *CustodyStore) StorePiece(blockID string, row, col int, piece cda.ReceivedPiece) {
	if s.db == nil {
		return
	}
	keyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)
	key := []byte(keyStr)

	var newCount int
	_ = s.db.Update(func(txn *badger.Txn) error {
		var pieces []cda.ReceivedPiece
		item, err := txn.Get(key)
		if err == nil {
			_ = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &pieces)
			})
		}
		pieces = append(pieces, piece)
		newCount = len(pieces)
		valData, err := json.Marshal(pieces)
		if err != nil {
			return err
		}
		return txn.Set(key, valData)
	})

	s.countMu.Lock()
	s.pieceCountCache[keyStr] = newCount
	s.countMu.Unlock()
}

func (s *CustodyStore) GetPieces(blockID string, row, col int) []cda.ReceivedPiece {
	if s.db == nil {
		return nil
	}
	key := []byte(fmt.Sprintf("received_%s_%d_%d", blockID, row, col))
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

	return pieces
}

func (s *CustodyStore) StoreRecodedPiece(blockID string, row, col int, piece cda.ReceivedPiece) {
	if s.db == nil {
		return
	}
	key := []byte(fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col))

	_ = s.db.Update(func(txn *badger.Txn) error {
		pieces := []cda.ReceivedPiece{piece}
		valData, err := json.Marshal(pieces)
		if err != nil {
			return err
		}
		return txn.Set(key, valData)
	})
}

func (s *CustodyStore) GetRecodedPieces(blockID string, row, col int) []cda.ReceivedPiece {
	if s.db == nil {
		return nil
	}
	key := []byte(fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col))
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

	return pieces
}

func (s *CustodyStore) GetPieceCount(blockID string, row, col int) int {
	if s.db == nil {
		return 0
	}
	keyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)

	s.countMu.RLock()
	c, found := s.pieceCountCache[keyStr]
	s.countMu.RUnlock()
	if found {
		return c
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
	if s.db == nil {
		return 0
	}
	rawCount := s.GetPieceCount(blockID, row, col)
	recodedPieces := s.GetRecodedPieces(blockID, row, col)
	return rawCount + len(recodedPieces)
}

func (s *CustodyStore) GetTotalPieceCount() int {
	if s.db == nil {
		return 0
	}
	count := 0
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
	if s.db == nil {
		return 0
	}
	count := 0
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
	if s.db == nil || storesPerCol <= 0 {
		return 0
	}
	count := 0
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
	if s.db == nil {
		return 0
	}
	deletedCount := 0
	rawKey := []byte(fmt.Sprintf("received_%s_%d_%d", blockID, row, col))
	recodedKey := []byte(fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col))

	pieceCommits, _ := s.GetAnchoredCommitments(blockID, col)

	_ = s.db.Update(func(txn *badger.Txn) error {
		var rawPieces []cda.ReceivedPiece
		item, err := txn.Get(rawKey)
		if err == nil {
			_ = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &rawPieces)
			})
			deletedCount = len(rawPieces)
		}

		if len(rawPieces) > 0 {
			var compressedPiece cda.ReceivedPiece
			if len(rawPieces) >= 2 && rm != nil {
				recoded, err := rm.RecodePiecesWithVerify(rawPieces, pieceCommits, 5)
				if err == nil && recoded != nil {
					compressedPiece = *recoded
				} else {
					compressedPiece = rawPieces[0]
					log.Printf("[CustodyStore] RecodeWithVerify fallback for cell [%d, %d]: %v. Retaining raw piece 0.", row, col, err)
				}
			} else {
				compressedPiece = rawPieces[0]
			}

			retained := []cda.ReceivedPiece{compressedPiece}
			if valData, err := json.Marshal(retained); err == nil {
				_ = txn.Set(recodedKey, valData)
			}
		}

		return txn.Delete(rawKey)
	})

	rawKeyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)
	s.countMu.Lock()
	s.pieceCountCache[rawKeyStr] = 0
	s.countMu.Unlock()

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


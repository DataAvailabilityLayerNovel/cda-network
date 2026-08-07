package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/dgraph-io/badger/v4"
)

type AnchoredData struct {
	ColIdx       int      `json:"col_idx"`
	PieceCommits [][]byte `json:"piece_commits"`
}

type CustodyStore struct {
	db   *badger.DB
	port int
}

func NewCustodyStore(port int) *CustodyStore {
	store := &CustodyStore{port: port}

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
	key := []byte(fmt.Sprintf("received_%s_%d_%d", blockID, row, col))

	_ = s.db.Update(func(txn *badger.Txn) error {
		var pieces []cda.ReceivedPiece
		item, err := txn.Get(key)
		if err == nil {
			_ = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &pieces)
			})
		}
		pieces = append(pieces, piece)
		valData, err := json.Marshal(pieces)
		if err != nil {
			return err
		}
		return txn.Set(key, valData)
	})
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
		var pieces []cda.ReceivedPiece
		item, err := txn.Get(key)
		if err == nil {
			_ = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &pieces)
			})
		}
		pieces = append(pieces, piece)
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
	key := []byte(fmt.Sprintf("received_%s_%d_%d", blockID, row, col))
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

	return count
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
		prefix := []byte("received_")
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

func (s *CustodyStore) PruneRawPieces(blockID string, row, col int) int {
	if s.db == nil {
		return 0
	}
	deletedCount := 0
	key := []byte(fmt.Sprintf("received_%s_%d_%d", blockID, row, col))
	_ = s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == nil {
			_ = item.Value(func(val []byte) error {
				var pieces []cda.ReceivedPiece
				if err := json.Unmarshal(val, &pieces); err == nil {
					deletedCount = len(pieces)
				}
				return nil
			})
		}
		return txn.Delete(key)
	})

	if deletedCount > 0 {
		go func() {
			for {
				err := s.db.RunValueLogGC(0.5)
				if err != nil {
					break
				}
			}
		}()
	}

	return deletedCount
}

func (s *CustodyStore) GetDBSize() int64 {
	if s.port <= 0 {
		return 0
	}
	baseDir := fmt.Sprintf("data/store_%d", s.port)
	dbPath := filepath.Join(baseDir, "badger")

	var size int64
	_ = filepath.Walk(dbPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}


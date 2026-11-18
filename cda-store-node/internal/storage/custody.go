package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/dgraph-io/badger/v4"
)

const binaryCustodyPiecesMagic uint32 = 0x43535401 // "CST\x01"

func marshalPiecesBinary(pieces []cda.ReceivedPiece) ([]byte, error) {
	totalLen := 4 + 2
	for i := range pieces {
		p := &pieces[i]
		totalLen += 2 + 2 + 2 + len(p.Data.Data) + 1 + len(p.Data.Coeffs) + 2 + len(p.Proof)
	}
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], binaryCustodyPiecesMagic)
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(pieces)))
	idx := 6
	for i := range pieces {
		p := &pieces[i]
		binary.BigEndian.PutUint16(buf[idx:], uint16(p.Row))
		idx += 2
		binary.BigEndian.PutUint16(buf[idx:], uint16(p.Col))
		idx += 2

		dLen := len(p.Data.Data)
		binary.BigEndian.PutUint16(buf[idx:], uint16(dLen))
		idx += 2
		copy(buf[idx:], p.Data.Data)
		idx += dLen

		cLen := len(p.Data.Coeffs)
		buf[idx] = uint8(cLen)
		idx++
		copy(buf[idx:], p.Data.Coeffs)
		idx += cLen

		pLen := len(p.Proof)
		binary.BigEndian.PutUint16(buf[idx:], uint16(pLen))
		idx += 2
		copy(buf[idx:], p.Proof)
		idx += pLen
	}
	return buf[:idx], nil
}

func unmarshalPieces(data []byte, out *[]cda.ReceivedPiece) error {
	if len(data) >= 6 && binary.BigEndian.Uint32(data[0:4]) == binaryCustodyPiecesMagic {
		count := int(binary.BigEndian.Uint16(data[4:6]))
		res := make([]cda.ReceivedPiece, count)
		idx := 6
		for i := 0; i < count; i++ {
			if idx+6 > len(data) {
				return fmt.Errorf("truncated binary custody piece header")
			}
			row := int(binary.BigEndian.Uint16(data[idx:]))
			idx += 2
			col := int(binary.BigEndian.Uint16(data[idx:]))
			idx += 2
			dLen := int(binary.BigEndian.Uint16(data[idx:]))
			idx += 2
			if idx+dLen+1 > len(data) {
				return fmt.Errorf("truncated binary custody piece data")
			}
			dBytes := make([]byte, dLen)
			copy(dBytes, data[idx:idx+dLen])
			idx += dLen

			cLen := int(data[idx])
			idx++
			if idx+cLen+2 > len(data) {
				return fmt.Errorf("truncated binary custody piece coeffs")
			}
			cBytes := make([]byte, cLen)
			copy(cBytes, data[idx:idx+cLen])
			idx += cLen

			pLen := int(binary.BigEndian.Uint16(data[idx:]))
			idx += 2
			if idx+pLen > len(data) {
				return fmt.Errorf("truncated binary custody piece proof")
			}
			pBytes := make([]byte, pLen)
			copy(pBytes, data[idx:idx+pLen])
			idx += pLen

			res[i] = cda.ReceivedPiece{
				Row: row,
				Col: col,
				Data: rlnc.PieceData{
					Data:   dBytes,
					Coeffs: cBytes,
				},
				Proof: cda.OpeningProof(pBytes),
			}
		}
		*out = res
		return nil
	}
	return json.Unmarshal(data, out)
}

func unmarshalPiecesCount(data []byte) (int, error) {
	if len(data) >= 6 && binary.BigEndian.Uint32(data[0:4]) == binaryCustodyPiecesMagic {
		return int(binary.BigEndian.Uint16(data[4:6])), nil
	}
	var pieces []cda.ReceivedPiece
	if err := json.Unmarshal(data, &pieces); err != nil {
		return 0, err
	}
	return len(pieces), nil
}

type AnchoredData struct {
	ColIdx       int      `json:"col_idx"`
	PieceCommits [][]byte `json:"piece_commits"`
}

type CustodyStore struct {
	db                   *badger.DB
	port                 int
	countMu              sync.RWMutex
	pieceCountCache      map[string]int
	memMu                sync.RWMutex
	piecesCache          map[string][]cda.ReceivedPiece
	recodedCache         map[string][]cda.ReceivedPiece
	anchorCache          map[string][][]byte
	activeRawPieces      int
	activeRecodedPieces  int
	activeRawByRow       map[int]int
	evictedRawPieces     int
	evictedRecodedPieces int
	evictedRawByRow      map[int]int
}

func NewCustodyStore(port int) *CustodyStore {
	store := &CustodyStore{
		port:            port,
		pieceCountCache: make(map[string]int),
		piecesCache:     make(map[string][]cda.ReceivedPiece),
		recodedCache:    make(map[string][]cda.ReceivedPiece),
		anchorCache:     make(map[string][][]byte),
		activeRawByRow:  make(map[int]int),
		evictedRawByRow: make(map[int]int),
	}

	if port > 0 {
		baseDir := fmt.Sprintf("data/store_%d", port)
		dbPath := filepath.Join(baseDir, "badger")
		_ = os.MkdirAll(dbPath, 0755)

		opts := badger.DefaultOptions(dbPath).
			WithLogger(nil).
			WithSyncWrites(false).                  // Asynchronous buffered writes to eliminate host disk fsync contention
			WithValueLogFileSize(16 * 1024 * 1024). // 16 MB value log file size
			WithMemTableSize(16 * 1024 * 1024).     // 16 MB memtable size
			WithValueThreshold(1024)                // Keep small pieces in memtable/LSM to avoid vlog file thrashing
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
	s.StorePiecesForCell(blockID, row, col, []cda.ReceivedPiece{piece})
}

// StorePiecesForCell appends multiple verified pieces for cell (row, col) in a single cache update
// and triggers at most 1 asynchronous binary persistence transaction to BadgerDB.
func (s *CustodyStore) StorePiecesForCell(blockID string, row, col int, newPieces []cda.ReceivedPiece) {
	if len(newPieces) == 0 {
		return
	}
	keyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)

	s.memMu.Lock()
	pieces := s.piecesCache[keyStr]
	added := len(newPieces)
	pieces = append(pieces, newPieces...)
	s.piecesCache[keyStr] = pieces
	s.activeRawPieces += added
	s.activeRawByRow[row] += added
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
			valData, err := marshalPiecesBinary(toSave)
			if err != nil {
				return
			}
			_ = s.db.Update(func(txn *badger.Txn) error {
				// Anti-regression: O(1) header check to ensure we do not overwrite DB with fewer pieces
				item, err := txn.Get([]byte(k))
				if err == nil {
					var existingCount int
					if err := item.Value(func(val []byte) error {
						var cErr error
						existingCount, cErr = unmarshalPiecesCount(val)
						return cErr
					}); err == nil && existingCount >= len(toSave) {
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
			return unmarshalPieces(val, &pieces)
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
	prevLen := len(s.recodedCache[keyStr])
	s.recodedCache[keyStr] = pieces
	s.activeRecodedPieces += 1 - prevLen
	s.memMu.Unlock()

	if s.db != nil {
		go func() {
			valData, err := marshalPiecesBinary(pieces)
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
	if prev, exists := s.recodedCache[keyStr]; exists {
		s.activeRecodedPieces -= len(prev)
		delete(s.recodedCache, keyStr)
	}
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
			return unmarshalPieces(val, &pieces)
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
			if c, err := unmarshalPiecesCount(val); err == nil {
				count = c
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
	s.memMu.RLock()
	total := s.activeRawPieces + s.evictedRawPieces + s.activeRecodedPieces + s.evictedRecodedPieces
	hasMem := total > 0 || len(s.piecesCache) > 0 || len(s.recodedCache) > 0
	s.memMu.RUnlock()
	if hasMem {
		return total
	}

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
					if c, err := unmarshalPiecesCount(val); err == nil {
						count += c
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
	s.memMu.RLock()
	total := s.activeRecodedPieces + s.evictedRecodedPieces
	hasMem := total > 0 || len(s.recodedCache) > 0
	s.memMu.RUnlock()
	if hasMem {
		return total
	}

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
				if c, err := unmarshalPiecesCount(val); err == nil {
					count += c
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
	s.memMu.RLock()
	if len(s.activeRawByRow) > 0 || len(s.evictedRawByRow) > 0 {
		count := 0
		for row, c := range s.activeRawByRow {
			if (row % storesPerCol) == rowIdx {
				count += c
			}
		}
		for row, c := range s.evictedRawByRow {
			if (row % storesPerCol) == rowIdx {
				count += c
			}
		}
		s.memMu.RUnlock()
		return count
	}
	s.memMu.RUnlock()

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
			key := string(item.Key())
			// key format: "received_{blockID}_{row}_{col}"
			parts := strings.Split(key, "_")
			if len(parts) >= 3 {
				rowStr := parts[len(parts)-2]
				var row int
				if _, err := fmt.Sscanf(rowStr, "%d", &row); err == nil {
					if (row % storesPerCol) == rowIdx {
						_ = item.Value(func(val []byte) error {
							if c, err := unmarshalPiecesCount(val); err == nil {
								count += c
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

// EvictBlockFromRAM removes in-memory slice entries for an old completed block (already persisted in BadgerDB)
// while preserving O(1) cumulative piece counters so memory remains bounded over thousands of blocks.
func (s *CustodyStore) EvictBlockFromRAM(blockID string, n, startCol, endCol int) {
	if s.db == nil || blockID == "" || n <= 0 {
		return
	}
	s.memMu.Lock()
	s.countMu.Lock()
	for c := startCol; c < endCol; c++ {
		delete(s.anchorCache, fmt.Sprintf("anchor_%s_%d", blockID, c))
		for r := 0; r < n; r++ {
			rawKey := fmt.Sprintf("received_%s_%d_%d", blockID, r, c)
			if pieces, ok := s.piecesCache[rawKey]; ok {
				cnt := len(pieces)
				s.activeRawPieces -= cnt
				s.activeRawByRow[r] -= cnt
				s.evictedRawPieces += cnt
				s.evictedRawByRow[r] += cnt
				delete(s.piecesCache, rawKey)
			}
			delete(s.pieceCountCache, rawKey)

			recKey := fmt.Sprintf("recoded_%s_%d_%d", blockID, r, c)
			if recs, ok := s.recodedCache[recKey]; ok {
				cnt := len(recs)
				s.activeRecodedPieces -= cnt
				s.evictedRecodedPieces += cnt
				delete(s.recodedCache, recKey)
			}
		}
	}
	s.countMu.Unlock()
	s.memMu.Unlock()
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
				key := fmt.Appendf(nil, "received_%s_%d_%d", blockID, r, c)
				item, err := txn.Get(key)
				if err != nil {
					complete = false
					return nil
				}
				err = item.Value(func(val []byte) error {
					if cnt, err := unmarshalPiecesCount(val); err != nil || cnt < k {
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

func (s *CustodyStore) PruneRawPieces(blockID string, row, col int, rm *cda.RecipientManager, domainSizes ...int) int {
	rawKeyStr := fmt.Sprintf("received_%s_%d_%d", blockID, row, col)
	recodedKeyStr := fmt.Sprintf("recoded_%s_%d_%d", blockID, row, col)

	s.memMu.Lock()
	rawPieces := s.piecesCache[rawKeyStr]
	alreadyHasRecoded := len(s.recodedCache[recodedKeyStr]) > 0
	if len(rawPieces) > 0 {
		s.activeRawPieces -= len(rawPieces)
		s.activeRawByRow[row] -= len(rawPieces)
		delete(s.piecesCache, rawKeyStr)
	}
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
			if recFast, fErr := rm.RecodePiecesFastCheck(rawPieces, 5); fErr == nil && recFast != nil {
				compressedPiece = *recFast
			} else {
				compressedPiece = rawPieces[0]
			}
		} else {
			compressedPiece = rawPieces[0]
		}

		s.memMu.Lock()
		if len(s.recodedCache[recodedKeyStr]) == 0 {
			s.recodedCache[recodedKeyStr] = []cda.ReceivedPiece{compressedPiece}
			s.activeRecodedPieces++
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
						valData, err := marshalPiecesBinary([]cda.ReceivedPiece{compPiece})
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
	lsm, vlog := s.db.Size()
	if lsm+vlog > 0 {
		return lsm + vlog
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


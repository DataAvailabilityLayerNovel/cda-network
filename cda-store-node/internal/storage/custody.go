package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
)

type CustodyStore struct {
	mu            sync.RWMutex
	baseDir       string
	anchored      map[string][][]byte            // blockID_col -> piece commitments
	received      map[string][]cda.ReceivedPiece // blockID_row_col -> received pieces
	recodedPieces map[string][]cda.ReceivedPiece // blockID_row_col -> recoded pieces
}

func NewCustodyStore(port int) *CustodyStore {
	baseDir := ""
	store := &CustodyStore{
		anchored:      make(map[string][][]byte),
		received:      make(map[string][]cda.ReceivedPiece),
		recodedPieces: make(map[string][]cda.ReceivedPiece),
	}

	if port > 0 {
		baseDir = fmt.Sprintf("data/store_%d", port)
		store.baseDir = baseDir
		_ = os.MkdirAll(filepath.Join(baseDir, "pieces"), 0755)
		_ = os.MkdirAll(filepath.Join(baseDir, "anchors"), 0755)
		_ = os.MkdirAll(filepath.Join(baseDir, "recoded"), 0755)

		store.loadFromDisk()
	}

	return store
}

func (s *CustodyStore) loadFromDisk() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Load pieces
	piecesDir := filepath.Join(s.baseDir, "pieces")
	files, err := os.ReadDir(piecesDir)
	if err == nil {
		for _, f := range files {
			if filepath.Ext(f.Name()) == ".json" {
				name := strings.TrimSuffix(f.Name(), ".json")
				filePath := filepath.Join(piecesDir, f.Name())
				data, err := os.ReadFile(filePath)
				if err == nil {
					var pieces []cda.ReceivedPiece
					if err := json.Unmarshal(data, &pieces); err == nil {
						s.received[name] = pieces
					}
				}
			}
		}
	}

	// Load anchors
	anchorsDir := filepath.Join(s.baseDir, "anchors")
	files, err = os.ReadDir(anchorsDir)
	if err == nil {
		for _, f := range files {
			if filepath.Ext(f.Name()) == ".json" {
				name := strings.TrimSuffix(f.Name(), ".json")
				filePath := filepath.Join(anchorsDir, f.Name())
				data, err := os.ReadFile(filePath)
				if err == nil {
					var commits [][]byte
					if err := json.Unmarshal(data, &commits); err == nil {
						s.anchored[name] = commits
					}
				}
			}
		}
	}

	// Load recoded pieces
	recodedDir := filepath.Join(s.baseDir, "recoded")
	files, err = os.ReadDir(recodedDir)
	if err == nil {
		for _, f := range files {
			if filepath.Ext(f.Name()) == ".json" {
				name := strings.TrimSuffix(f.Name(), ".json")
				filePath := filepath.Join(recodedDir, f.Name())
				data, err := os.ReadFile(filePath)
				if err == nil {
					var pieces []cda.ReceivedPiece
					if err := json.Unmarshal(data, &pieces); err == nil {
						s.recodedPieces[name] = pieces
					}
				}
			}
		}
	}
}

func (s *CustodyStore) AnchorCommitments(blockID string, colIdx int, pieceCommits [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s_%d", blockID, colIdx)
	s.anchored[key] = pieceCommits

	if s.baseDir != "" {
		filePath := filepath.Join(s.baseDir, "anchors", fmt.Sprintf("%s.json", key))
		data, err := json.Marshal(pieceCommits)
		if err == nil {
			_ = os.WriteFile(filePath, data, 0644)
		}
	}
}

func (s *CustodyStore) GetAnchoredCommitments(blockID string, colIdx int) ([][]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("%s_%d", blockID, colIdx)
	commits, ok := s.anchored[key]
	return commits, ok
}

func (s *CustodyStore) StorePiece(blockID string, row, col int, piece cda.ReceivedPiece) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s_%d_%d", blockID, row, col)
	s.received[key] = append(s.received[key], piece)

	if s.baseDir != "" {
		filePath := filepath.Join(s.baseDir, "pieces", fmt.Sprintf("%s.json", key))
		data, err := json.Marshal(s.received[key])
		if err == nil {
			_ = os.WriteFile(filePath, data, 0644)
		}
	}
}

func (s *CustodyStore) GetPieces(blockID string, row, col int) []cda.ReceivedPiece {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("%s_%d_%d", blockID, row, col)
	pieces := s.received[key]
	if pieces == nil {
		return nil
	}
	res := make([]cda.ReceivedPiece, len(pieces))
	copy(res, pieces)
	return res
}

func (s *CustodyStore) StoreRecodedPiece(blockID string, row, col int, piece cda.ReceivedPiece) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s_%d_%d", blockID, row, col)
	s.recodedPieces[key] = append(s.recodedPieces[key], piece)

	if s.baseDir != "" {
		filePath := filepath.Join(s.baseDir, "recoded", fmt.Sprintf("%s.json", key))
		data, err := json.Marshal(s.recodedPieces[key])
		if err == nil {
			_ = os.WriteFile(filePath, data, 0644)
		}
	}
}

func (s *CustodyStore) GetRecodedPieces(blockID string, row, col int) []cda.ReceivedPiece {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("%s_%d_%d", blockID, row, col)
	pieces := s.recodedPieces[key]
	if pieces == nil {
		return nil
	}
	res := make([]cda.ReceivedPiece, len(pieces))
	copy(res, pieces)
	return res
}

func (s *CustodyStore) GetPieceCount(blockID string, row, col int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("%s_%d_%d", blockID, row, col)
	return len(s.received[key])
}

func (s *CustodyStore) IsComplete(blockID string, colIdx, k int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 2 * k
	colsPerNetCol := n / 4
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}

	for c := colIdx; c < colIdx+colsPerNetCol; c++ {
		for r := 0; r < n; r++ {
			key := fmt.Sprintf("%s_%d_%d", blockID, r, c)
			if len(s.received[key]) < k {
				return false
			}
		}
	}
	return true
}

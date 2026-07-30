package storage

import (
	"fmt"
	"sync"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
)

type CustodyStore struct {
	mu            sync.RWMutex
	anchored      map[string][][]byte            // blockID_col -> piece commitments
	received      map[string][]cda.ReceivedPiece // blockID_row_col -> received pieces
	recodedPieces map[string][]cda.ReceivedPiece // blockID_row_col -> recoded pieces
}

func NewCustodyStore() *CustodyStore {
	return &CustodyStore{
		anchored:      make(map[string][][]byte),
		received:      make(map[string][]cda.ReceivedPiece),
		recodedPieces: make(map[string][]cda.ReceivedPiece),
	}
}

func (s *CustodyStore) AnchorCommitments(blockID string, colIdx int, pieceCommits [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s_%d", blockID, colIdx)
	s.anchored[key] = pieceCommits
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

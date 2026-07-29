package p2p

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"cda-bootstrap-node/internal/storage"
)

type SyncResponse struct {
	BlockID      string     `json:"block_id"`
	ColIdx       int        `json:"col_idx"`
	ColumnData   []string   `json:"column_data"`
	PieceCommits []string   `json:"piece_commits"`
	Proofs       [][]string `json:"proofs"` // row -> piece -> hex proof
}

type SyncService struct {
	cache *storage.LocalCache
}

func NewSyncService(cache *storage.LocalCache) *SyncService {
	return &SyncService{
		cache: cache,
	}
}

func (s *SyncService) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/bootstrap/sync/", s.handleSync)
}

func (s *SyncService) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Format: /bootstrap/sync/:blockID
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Missing block ID", http.StatusBadRequest)
		return
	}
	blockID := parts[3]

	log.Printf("[P2P] Handling Sync Request for Block %s", blockID)

	col, exists := s.cache.Get(blockID)
	if !exists {
		http.Error(w, "Block not found in cache", http.StatusNotFound)
		return
	}

	// Hex-encode column data
	hexColData := make([]string, len(col.ColumnData))
	for i, cell := range col.ColumnData {
		hexColData[i] = hex.EncodeToString(cell)
	}

	// Hex-encode piece commitments
	hexPieceCommits := make([]string, len(col.PieceCommits))
	for i, c := range col.PieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	// Hex-encode proofs
	hexProofs := make([][]string, len(col.Proofs))
	for rowIdx, rowProofs := range col.Proofs {
		hexProofs[rowIdx] = make([]string, len(rowProofs))
		for pIdx, pBytes := range rowProofs {
			hexProofs[rowIdx][pIdx] = hex.EncodeToString(pBytes)
		}
	}

	resp := SyncResponse{
		BlockID:      col.BlockID,
		ColIdx:       col.ColIdx,
		ColumnData:   hexColData,
		PieceCommits: hexPieceCommits,
		Proofs:       hexProofs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

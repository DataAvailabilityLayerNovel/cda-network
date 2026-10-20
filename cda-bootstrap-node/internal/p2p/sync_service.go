package p2p

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"cda-bootstrap-node/internal/storage"
)

type SyncResponse struct {
	BlockID      string         `json:"block_id"`
	ColIdx       int            `json:"col_idx"`
	ColumnData   []string       `json:"column_data"`
	PieceCommits []string       `json:"piece_commits"`
	Proofs       [][]string     `json:"proofs"`            // row -> piece -> hex proof
	Columns      []SyncResponse `json:"columns,omitempty"` // populated when multiple columns are returned
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

func encodeCachedCol(col *storage.CachedColumn) SyncResponse {
	hexColData := make([]string, len(col.ColumnData))
	for i, cell := range col.ColumnData {
		hexColData[i] = hex.EncodeToString(cell)
	}

	hexPieceCommits := make([]string, len(col.PieceCommits))
	for i, c := range col.PieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	hexProofs := make([][]string, len(col.Proofs))
	for rowIdx, rowProofs := range col.Proofs {
		hexProofs[rowIdx] = make([]string, len(rowProofs))
		for pIdx, pBytes := range rowProofs {
			hexProofs[rowIdx][pIdx] = hex.EncodeToString(pBytes)
		}
	}

	return SyncResponse{
		BlockID:      col.BlockID,
		ColIdx:       col.ColIdx,
		ColumnData:   hexColData,
		PieceCommits: hexPieceCommits,
		Proofs:       hexProofs,
	}
}

func (s *SyncService) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Format: /bootstrap/sync/:blockID
	parts := strings.Split(strings.TrimSuffix(r.URL.Path, "/"), "/")
	if len(parts) < 4 || parts[3] == "" {
		http.Error(w, "Missing block ID", http.StatusBadRequest)
		return
	}
	blockID := parts[3]

	if blockID == "recent" {
		limit := 5
		if lStr := r.URL.Query().Get("limit"); lStr != "" {
			if lVal, err := strconv.Atoi(lStr); err == nil && lVal > 0 {
				limit = lVal
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"block_ids": s.cache.RecentBlockIDs(limit),
		})
		return
	}

	log.Printf("[P2P] Handling Sync Request for Block %s", blockID)

	if colStr := r.URL.Query().Get("col"); colStr != "" {
		if colIdx, err := strconv.Atoi(colStr); err == nil {
			col, exists := s.cache.GetCol(blockID, colIdx)
			if !exists {
				http.Error(w, "Column not found in cache", http.StatusNotFound)
				return
			}
			resp := encodeCachedCol(col)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
	}

	allCols := s.cache.GetAll(blockID)
	if len(allCols) == 0 {
		http.Error(w, "Block not found in cache", http.StatusNotFound)
		return
	}

	primaryCol, _ := s.cache.Get(blockID)
	resp := encodeCachedCol(primaryCol)
	if len(allCols) > 1 {
		for _, c := range allCols {
			resp.Columns = append(resp.Columns, encodeCachedCol(c))
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}


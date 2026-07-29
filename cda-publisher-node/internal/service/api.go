package service

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"cda-publisher-node/internal/engine"
	"cda-publisher-node/internal/p2p"
)

type APIService struct {
	mu       sync.RWMutex
	headers  map[string]*engine.BlockHeader
	pipeline *engine.Pipeline
	sender   *p2p.Sender
}

type PublishRequest struct {
	BlockID string   `json:"block_id"`
	Data    []string `json:"data"` // List of cells as hex strings
}

func NewAPIService(pipeline *engine.Pipeline, sender *p2p.Sender) *APIService {
	return &APIService{
		headers:  make(map[string]*engine.BlockHeader),
		pipeline: pipeline,
		sender:   sender,
	}
}

func (s *APIService) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/publish", s.handlePublish)
	mux.HandleFunc("/header/", s.handleGetHeader)
}

func (s *APIService) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Data) == 0 {
		http.Error(w, "Data cannot be empty", http.StatusBadRequest)
		return
	}

	// 1. Decode hex data cells (ODS)
	decodedData := make([][]byte, len(req.Data))
	for i, h := range req.Data {
		b, err := hex.DecodeString(h)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid hex string at index %d: %v", i, err), http.StatusBadRequest)
			return
		}
		decodedData[i] = b
	}

	// 2. Run engine pipeline: ODS -> EDS -> Commitments -> Header
	header, pubData, proofs, eds, err := s.pipeline.ProcessODS(decodedData, req.BlockID)
	if err != nil {
		http.Error(w, "Engine pipeline failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. Save Header
	s.mu.Lock()
	s.headers[header.BlockID] = header
	s.mu.Unlock()

	log.Printf("Successfully generated Block Header for BlockID: %s", header.BlockID)

	// 4. Distribute each column via P2P sender
	n := int(eds.Width())
	k := len(pubData.PieceComm) / n
	for c := 0; c < n; c++ {
		colData := eds.Col(uint(c))
		pieceCommits := pubData.PieceComm[c*k : c*k+k]

		pieceCommitsBytes := make([][]byte, k)
		for i := 0; i < k; i++ {
			pieceCommitsBytes[i] = append([]byte(nil), pieceCommits[i]...)
		}

		colProofs := proofs[c*k : c*k+k]

		if err := s.sender.SendColumnChunk(header.BlockID, c, colData, pieceCommitsBytes, colProofs); err != nil {
			log.Printf("Failed to distribute column %d to bootstrap: %v", c, err)
			http.Error(w, fmt.Sprintf("P2P distribution failed for column %d: %v", c, err), http.StatusInternalServerError)
			return
		}
	}

	// Respond with the BlockHeader
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(header)
}

func (s *APIService) handleGetHeader(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	blockID := r.URL.Path[len("/header/"):]
	if blockID == "" {
		http.Error(w, "Missing block ID", http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	header, exists := s.headers[blockID]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, "Header not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(header)
}

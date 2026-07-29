package service

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"

	"cda-light-node/internal/verifier"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
)

type APIService struct {
	publisherAddr string
	bootstrapsMap map[int]string
	verifier      *verifier.DASVerifier
	crashOnFail   bool
}

type BlockHeader struct {
	BlockID     string   `json:"block_id"`
	CommitsRoot string   `json:"commits_root"`
	ColumnComm  [][]byte `json:"column_comm"`
	Coeffs      []byte   `json:"coeffs"`
}

type StorePayload struct {
	BlockID string `json:"block_id"`
	Row     int    `json:"row"`
	Col     int    `json:"col"`
	Data    string `json:"data"`
	Coeffs  string `json:"coeffs"`
	Proof   string `json:"proof"`
}

type CellRetrieveResponse struct {
	BlockID     string         `json:"block_id"`
	Row         int            `json:"row"`
	Col         int            `json:"col"`
	Recovered   bool           `json:"recovered"`
	Data        string         `json:"data,omitempty"`
	PiecesCount int            `json:"pieces_count"`
	Pieces      []StorePayload `json:"pieces"`
}

type SampleResult struct {
	Row      int    `json:"row"`
	Col      int    `json:"col"`
	Verified bool   `json:"verified"`
	CellData string `json:"cell_data,omitempty"`
	Error    string `json:"error,omitempty"`
}

type DASResponse struct {
	BlockID string         `json:"block_id"`
	Success bool           `json:"success"`
	Results []SampleResult `json:"results"`
}

func NewAPIService(publisherAddr string, bootstrapsMap map[int]string, v *verifier.DASVerifier, crashOnFail bool) *APIService {
	return &APIService{
		publisherAddr: publisherAddr,
		bootstrapsMap: bootstrapsMap,
		verifier:      v,
		crashOnFail:   crashOnFail,
	}
}

func (s *APIService) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/das/sample/", s.handleDASSample)
}

func (s *APIService) handleDASSample(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	blockID := r.URL.Path[len("/das/sample/"):]
	if blockID == "" {
		http.Error(w, "Missing block ID", http.StatusBadRequest)
		return
	}

	// 1. Fetch Block Header from Publisher
	headerUrl := fmt.Sprintf("%s/header/%s", s.publisherAddr, blockID)
	resp, err := http.Get(headerUrl)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch block header: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		http.Error(w, "Block Header not found on Publisher", http.StatusNotFound)
		return
	}

	var header BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse block header: %v", err), http.StatusInternalServerError)
		return
	}

	// Determine matrix size N
	n := len(header.ColumnComm)
	if n == 0 {
		http.Error(w, "Invalid block header: no column commitments", http.StatusInternalServerError)
		return
	}

	// 2. Parse query parameters
	q := r.URL.Query()
	rowStr := q.Get("row")
	colStr := q.Get("col")
	samplesStr := q.Get("samples")
	allStr := q.Get("all")

	var results []SampleResult

	if allStr == "true" {
		// Sample EVERY single cell in the EDS (N x N)
		for rIdx := 0; rIdx < n; rIdx++ {
			for cIdx := 0; cIdx < n; cIdx++ {
				res := s.sampleCell(blockID, rIdx, cIdx, &header)
				results = append(results, res)
			}
		}
	} else if rowStr != "" && colStr != "" {
		// Specific cell sampling
		row, err1 := strconv.Atoi(rowStr)
		col, err2 := strconv.Atoi(colStr)
		if err1 != nil || err2 != nil {
			http.Error(w, "Invalid row/col parameter", http.StatusBadRequest)
			return
		}
		res := s.sampleCell(blockID, row, col, &header)
		results = append(results, res)
	} else {
		// Random DAS sampling
		numSamples := 4
		if samplesStr != "" {
			if val, err := strconv.Atoi(samplesStr); err == nil && val > 0 {
				numSamples = val
			}
		}

		for i := 0; i < numSamples; i++ {
			row := rand.Intn(n)
			col := rand.Intn(n)
			res := s.sampleCell(blockID, row, col, &header)
			results = append(results, res)
		}
	}

	// 3. Respond with results
	allSuccess := true
	for _, res := range results {
		if !res.Verified {
			allSuccess = false
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(DASResponse{
		BlockID: blockID,
		Success: allSuccess,
		Results: results,
	})
}

func (s *APIService) sampleCell(blockID string, row, col int, header *BlockHeader) SampleResult {
	n := len(header.ColumnComm)
	colsPerNetCol := n / 4
	netColIdx := 0
	if colsPerNetCol > 0 {
		netColIdx = col / colsPerNetCol
	}

	bootAddr, ok := s.bootstrapsMap[netColIdx]
	if !ok {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("no bootstrap node configured for network column %d", netColIdx)}
	}

	// Dynamic Resolution: query bootstrap node for active stores
	peersUrl := fmt.Sprintf("%s/bootstrap/peers", bootAddr)
	peersResp, err := http.Get(peersUrl)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to discover peers from bootstrap %s: %v", bootAddr, err)}
	}
	defer peersResp.Body.Close()

	if peersResp.StatusCode != http.StatusOK {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("bootstrap returned status %d on peers query", peersResp.StatusCode)}
	}

	var peersData struct {
		Peers []string `json:"peers"`
	}
	if err := json.NewDecoder(peersResp.Body).Decode(&peersData); err != nil || len(peersData.Peers) == 0 {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("no active store nodes registered on bootstrap %s", bootAddr)}
	}

	// Pick a random store node in the dynamic list to retrieve the cell pieces
	storeUrl := peersData.Peers[rand.Intn(len(peersData.Peers))]
	url := fmt.Sprintf("%s/store/cell/retrieve/%s/%d/%d", storeUrl, blockID, row, col)

	resp, err := http.Get(url)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to query store %s: %v", storeUrl, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("store returned status %d", resp.StatusCode)}
	}

	var retrieveResp CellRetrieveResponse
	if err := json.NewDecoder(resp.Body).Decode(&retrieveResp); err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to parse store response: %v", err)}
	}

	// Convert retrieved pieces into cda.ReceivedPiece
	var recvPieces []cda.ReceivedPiece
	for _, p := range retrieveResp.Pieces {
		decData, err1 := hex.DecodeString(p.Data)
		decCoeffs, err2 := hex.DecodeString(p.Coeffs)
		decProof, err3 := hex.DecodeString(p.Proof)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}

		recvPieces = append(recvPieces, cda.ReceivedPiece{
			Row: p.Row,
			Col: p.Col,
			Data: rlnc.PieceData{
				Data:   decData,
				Coeffs: decCoeffs,
			},
			Proof: cda.OpeningProof(decProof),
		})
	}

	// Perform local algebraic verification using block header
	columnCommitment := header.ColumnComm[col]
	recoveredCell, verified, err := s.verifier.VerifyReconstructedCell(recvPieces, columnCommitment, header.Coeffs, row)
	if err != nil {
		if s.crashOnFail {
			log.Fatalf("DAS verification failed (CRASH) for cell [%d, %d]: %v", row, col, err)
		}
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("verification error: %v", err)}
	}

	if !verified {
		if s.crashOnFail {
			log.Fatalf("DAS verification failed (CRASH) for cell [%d, %d]: algebraic direct verification failed", row, col)
		}
		return SampleResult{Row: row, Col: col, Verified: false, Error: "algebraic direct verification failed"}
	}

	return SampleResult{
		Row:      row,
		Col:      col,
		Verified: true,
		CellData: recoveredCell,
	}
}

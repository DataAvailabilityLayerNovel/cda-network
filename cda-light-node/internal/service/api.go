package service

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cda-light-node/internal/verifier"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

type APIService struct {
	publisherAddr string
	bootstrapsMap map[int]string
	verifier      *verifier.DASVerifier
	crashOnFail   bool
	host          host.Host
	ps            *pubsub.PubSub

	headersMu sync.RWMutex
	headers   map[string]*BlockHeader
}

type BlockHeader struct {
	BlockID     string   `json:"block_id"`
	CommitsRoot string   `json:"commits_root"`
	ColumnComm  [][]byte `json:"column_comm"`
	Coeffs      []byte   `json:"coeffs"`
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

func NewAPIService(
	publisherAddr string,
	bootstrapsMap map[int]string,
	v *verifier.DASVerifier,
	crashOnFail bool,
	h host.Host,
	ps *pubsub.PubSub,
) *APIService {
	return &APIService{
		publisherAddr: publisherAddr,
		bootstrapsMap: bootstrapsMap,
		verifier:      v,
		crashOnFail:   crashOnFail,
		host:          h,
		ps:            ps,
		headers:       make(map[string]*BlockHeader),
	}
}

func (s *APIService) CacheHeader(header *BlockHeader) {
	s.headersMu.Lock()
	defer s.headersMu.Unlock()
	s.headers[header.BlockID] = header
	log.Printf("[P2P Sync] Cached block header for block %s received via GossipSub", header.BlockID)
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

	// 1. Get header from local P2P cache or fallback to HTTP from Publisher
	s.headersMu.RLock()
	header, exists := s.headers[blockID]
	s.headersMu.RUnlock()

	if !exists {
		log.Printf("[LightNode] Header not found in local P2P cache for block %s. Fetching via HTTP fallback...", blockID)
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

		header = &BlockHeader{}
		if err := json.NewDecoder(resp.Body).Decode(header); err != nil {
			http.Error(w, fmt.Sprintf("Failed to parse block header: %v", err), http.StatusInternalServerError)
			return
		}
		s.CacheHeader(header)
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
				res := s.sampleCell(blockID, rIdx, cIdx, header)
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
		res := s.sampleCell(blockID, row, col, header)
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
			res := s.sampleCell(blockID, row, col, header)
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

	if strings.HasPrefix(bootAddr, "http://") || strings.HasPrefix(bootAddr, "https://") {
		u, err := url.Parse(bootAddr)
		if err == nil {
			hostStr := u.Hostname()
			portStr := u.Port()
			if portVal, err := strconv.Atoi(portStr); err == nil {
				p2pPort := portVal + 10000
				if hostStr == "localhost" || hostStr == "127.0.0.1" {
					bootAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p2pPort)
				} else {
					bootAddr = fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, p2pPort)
				}
			}
		}
	}

	colIdx := netColIdx * (n / 4)
	_, bootPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", colIdx))
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to calculate bootstrap PeerID: %v", err)}
	}

	bootstrapAddrFull := fmt.Sprintf("%s/p2p/%s", bootAddr, bootPID.String())
	maddr, err := multiaddr.NewMultiaddr(bootstrapAddrFull)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("invalid bootstrap multiaddr: %v", err)}
	}

	bootInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to parse peer info: %v", err)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.host.Connect(ctx, *bootInfo); err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to connect to bootstrap %s: %v", bootInfo.ID, err)}
	}

	// Query bootstrap node for active store node list via P2P routing RPC
	stream, err := s.host.NewStream(ctx, bootInfo.ID, p2pcommon.ProtoBootstrapRouting)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to open routing stream: %v", err)}
	}
	defer stream.Close()

	req := p2pcommon.BootstrapRoutingRequest{
		Peer: p2pcommon.PeerInfo{
			PeerID:     s.host.ID().String(),
			Multiaddrs: []string{}, // Light node does not need dynamic indexing
			Row:        -1,
			Col:        -1,
		},
		TargetRow: row,
		TargetCol: col,
	}

	if err := json.NewEncoder(stream).Encode(req); err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to request routing info: %v", err)}
	}

	var routingResp p2pcommon.BootstrapRoutingResponse
	if err := json.NewDecoder(stream).Decode(&routingResp); err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to read routing info: %v", err)}
	}

	// Filter store nodes matching target column subnet
	var activeStores []p2pcommon.PeerInfo
	for _, p := range routingResp.ColPeers {
		storeNetCol := 0
		if colsPerNetCol > 0 {
			storeNetCol = p.Col / colsPerNetCol
		}
		if storeNetCol == netColIdx {
			activeStores = append(activeStores, p)
		}
	}

	// Fallback to all column peers if none registered specifically for target cell (Bootstrap may return column peers)
	if len(activeStores) == 0 {
		activeStores = routingResp.ColPeers
	}

	if len(activeStores) == 0 {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("no active store nodes registered on bootstrap %s for column %d", bootInfo.ID, col)}
	}

	// Pick a random store node in the dynamic list to query
	selectedStore := activeStores[rand.Intn(len(activeStores))]
	storePID, err := peer.Decode(selectedStore.PeerID)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("invalid store Peer ID: %v", err)}
	}

	// Add store multiaddrs to peerstore
	for _, mStr := range selectedStore.Multiaddrs {
		m, err := multiaddr.NewMultiaddr(mStr)
		if err == nil {
			s.host.Peerstore().AddAddr(storePID, m, 10*time.Minute)
		}
	}

	if err := s.host.Connect(ctx, peer.AddrInfo{ID: storePID}); err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to connect to store node %s: %v", storePID, err)}
	}

	// Dial store pieces query P2P stream
	storeStream, err := s.host.NewStream(ctx, storePID, p2pcommon.ProtoStoreGetPieces)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to open P2P stream on store node: %v", err)}
	}
	defer storeStream.Close()

	fetchReq := p2pcommon.StoreFetchRequest{
		BlockID:     blockID,
		Row:         row,
		Col:         col,
		IsRemoteHop: false,
	}

	if err := json.NewEncoder(storeStream).Encode(fetchReq); err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to send P2P request: %v", err)}
	}

	var fetchResp p2pcommon.StoreFetchResponse
	if err := json.NewDecoder(storeStream).Decode(&fetchResp); err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("failed to read P2P response: %v", err)}
	}

	// Convert retrieved pieces into cda.ReceivedPiece
	var recvPieces []cda.ReceivedPiece
	for _, p := range fetchResp.Pieces {
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

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
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

type cachedRoutingInfo struct {
	stores    []p2pcommon.PeerInfo
	timestamp time.Time
}

type APIService struct {
	publisherAddr string
	bootstrapsMap map[int][]string
	numCols       int // number of network columns (= len(bootstrapsMap))
	verifier      *verifier.DASVerifier
	crashOnFail   bool
	host          host.Host
	ps            *pubsub.PubSub

	headersMu sync.RWMutex
	headers   map[string]*BlockHeader

	routingCacheMu sync.RWMutex
	routingCache   map[int]cachedRoutingInfo
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
	bootstrapsMap map[int][]string,
	v *verifier.DASVerifier,
	crashOnFail bool,
	h host.Host,
	ps *pubsub.PubSub,
) *APIService {
	numCols := len(bootstrapsMap)
	if numCols == 0 {
		numCols = 1
	}
	return &APIService{
		publisherAddr: publisherAddr,
		bootstrapsMap: bootstrapsMap,
		numCols:       numCols,
		verifier:      v,
		crashOnFail:   crashOnFail,
		host:          h,
		ps:            ps,
		headers:       make(map[string]*BlockHeader),
		routingCache:  make(map[int]cachedRoutingInfo),
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
		// Sample EVERY single cell in the EDS (N x N) concurrently
		results = make([]SampleResult, n*n)
		var wg sync.WaitGroup
		sem := make(chan struct{}, 16) // limit concurrency to 16 goroutines
		for rIdx := 0; rIdx < n; rIdx++ {
			for cIdx := 0; cIdx < n; cIdx++ {
				wg.Add(1)
				go func(r, c int) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					res := s.sampleCell(blockID, r, c, header)
					results[r*n+c] = res
				}(rIdx, cIdx)
			}
		}
		wg.Wait()
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
	recordDASAttempt(allSuccess)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(DASResponse{
		BlockID: blockID,
		Success: allSuccess,
		Results: results,
	})
}

func (s *APIService) getRoutingInfo(netColIdx int, colsPerNetCol int, row int, col int) ([]p2pcommon.PeerInfo, error) {
	s.routingCacheMu.RLock()
	if info, ok := s.routingCache[netColIdx]; ok && time.Since(info.timestamp) < 3*time.Second {
		s.routingCacheMu.RUnlock()
		return info.stores, nil
	}
	s.routingCacheMu.RUnlock()

	bootAddrs, ok := s.bootstrapsMap[netColIdx]
	if !ok || len(bootAddrs) == 0 {
		return nil, fmt.Errorf("no bootstrap node configured for network column %d (colsPerNetCol=%d, numCols=%d)", netColIdx, colsPerNetCol, s.numCols)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var activeStores []p2pcommon.PeerInfo
	var lastErr error

	colIdx := netColIdx * colsPerNetCol
	_, bootPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", colIdx))
	if err != nil {
		return nil, fmt.Errorf("failed to calculate bootstrap PeerID: %v", err)
	}

	for _, bootAddr := range bootAddrs {
		resolvedAddr := bootAddr
		if strings.HasPrefix(resolvedAddr, "http://") || strings.HasPrefix(resolvedAddr, "https://") {
			u, err := url.Parse(resolvedAddr)
			if err == nil {
				hostStr := u.Hostname()
				portStr := u.Port()
				if portVal, err := strconv.Atoi(portStr); err == nil {
					p2pPort := portVal + 10000
					if hostStr == "localhost" || hostStr == "127.0.0.1" {
						resolvedAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p2pPort)
					} else {
						resolvedAddr = fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, p2pPort)
					}
				}
			}
		}

		bootstrapAddrFull := fmt.Sprintf("%s/p2p/%s", resolvedAddr, bootPID.String())
		maddr, err := multiaddr.NewMultiaddr(bootstrapAddrFull)
		if err != nil {
			lastErr = fmt.Errorf("invalid bootstrap multiaddr %s: %v", bootstrapAddrFull, err)
			continue
		}

		bootInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			lastErr = fmt.Errorf("failed to parse peer info: %v", err)
			continue
		}

		if s.host.Network().Connectedness(bootInfo.ID) != network.Connected {
			connCtx, connCancel := context.WithTimeout(ctx, 4*time.Second)
			errDial := s.host.Connect(connCtx, *bootInfo)
			connCancel()
			if errDial != nil {
				recordDialAttempt(false)
				lastErr = fmt.Errorf("failed to connect to bootstrap %s: %v", bootInfo.ID, errDial)
				continue
			}
		}
		recordDialAttempt(true)

		streamCtx, streamCancel := context.WithTimeout(ctx, 5*time.Second)
		stream, err := s.host.NewStream(streamCtx, bootInfo.ID, p2pcommon.ProtoBootstrapRouting)
		if err != nil {
			streamCancel()
			lastErr = fmt.Errorf("failed to open routing stream: %v", err)
			continue
		}

		req := p2pcommon.BootstrapRoutingRequest{
			Peer: p2pcommon.PeerInfo{
				PeerID:     s.host.ID().String(),
				Multiaddrs: []string{},
				Row:        -1,
				Col:        -1,
			},
			TargetRow: row,
			TargetCol: col,
		}

		if err := json.NewEncoder(stream).Encode(req); err != nil {
			stream.Close()
			streamCancel()
			lastErr = fmt.Errorf("failed to request routing info: %v", err)
			continue
		}

		var routingResp p2pcommon.BootstrapRoutingResponse
		if err := json.NewDecoder(stream).Decode(&routingResp); err != nil {
			stream.Close()
			streamCancel()
			lastErr = fmt.Errorf("failed to read routing info: %v", err)
			continue
		}
		stream.Close()
		streamCancel()

		for _, p := range routingResp.ColPeers {
			storeNetCol := 0
			if colsPerNetCol > 0 {
				storeNetCol = p.Col / colsPerNetCol
			}
			if storeNetCol == netColIdx {
				activeStores = append(activeStores, p)
			}
		}

		if len(activeStores) == 0 {
			activeStores = routingResp.ColPeers
		}

		if len(activeStores) > 0 {
			lastErr = nil
			break
		}
		lastErr = fmt.Errorf("no active store nodes returned by bootstrap %s", bootInfo.ID)
	}

	if lastErr != nil {
		return nil, lastErr
	}

	s.routingCacheMu.Lock()
	s.routingCache[netColIdx] = cachedRoutingInfo{
		stores:    activeStores,
		timestamp: time.Now(),
	}
	s.routingCacheMu.Unlock()

	return activeStores, nil
}

func (s *APIService) sampleCell(blockID string, row, col int, header *BlockHeader) SampleResult {
	start := time.Now()
	defer func() {
		DASSampleLatency.Observe(time.Since(start).Seconds())
	}()
	n := len(header.ColumnComm)
	colsPerNetCol := n / s.numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}
	netColIdx := col / colsPerNetCol

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activeStores, err := s.getRoutingInfo(netColIdx, colsPerNetCol, row, col)
	if err != nil {
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("routing query failed on all bootstraps: %v", err)}
	}

	var recvPieces []cda.ReceivedPiece
	var queryErrors []string
	var querySuccess bool

	// Shuffle active stores to pick random candidates in order
	shuffledStores := append([]p2pcommon.PeerInfo(nil), activeStores...)
	for i := range shuffledStores {
		j := rand.Intn(i + 1)
		shuffledStores[i], shuffledStores[j] = shuffledStores[j], shuffledStores[i]
	}

	maxTries := len(shuffledStores)

	type CombinedResponse struct {
		Success     *bool                  `json:"success,omitempty"`
		Error       string                 `json:"error,omitempty"`
		BlockID     string                 `json:"block_id"`
		Row         int                    `json:"row"`
		Col         int                    `json:"col"`
		Recovered   bool                   `json:"recovered"`
		Data        string                 `json:"data,omitempty"`
		PiecesCount int                    `json:"pieces_count"`
		Pieces      []p2pcommon.CodedPiece `json:"pieces"`
	}

	for idx := 0; idx < maxTries; idx++ {
		selectedStore := shuffledStores[idx]
		storePID, err := peer.Decode(selectedStore.PeerID)
		if err != nil {
			queryErrors = append(queryErrors, fmt.Sprintf("invalid store Peer ID %s: %v", selectedStore.PeerID, err))
			continue
		}

		// Add store multiaddrs to peerstore
		for _, mStr := range selectedStore.Multiaddrs {
			m, err := multiaddr.NewMultiaddr(mStr)
			if err == nil {
				s.host.Peerstore().AddAddr(storePID, m, 10*time.Minute)
			}
		}

		if s.host.Network().Connectedness(storePID) != network.Connected {
			storeConnCtx, storeConnCancel := context.WithTimeout(ctx, 4*time.Second)
			err = s.host.Connect(storeConnCtx, peer.AddrInfo{ID: storePID})
			storeConnCancel()

			if err != nil {
				recordDialAttempt(false)
				queryErrors = append(queryErrors, fmt.Sprintf("failed to connect to store node %s: %v", storePID, err))
				continue
			}
		}
		recordDialAttempt(true)

		// Open stream with a fresh context for actual data transfer
		storeStreamCtx, storeStreamCancel := context.WithTimeout(ctx, 5*time.Second)
		storeStream, err := s.host.NewStream(storeStreamCtx, storePID, p2pcommon.ProtoStoreGetPieces)
		if err != nil {
			queryErrors = append(queryErrors, fmt.Sprintf("failed to open P2P stream on store node %s: %v", storePID, err))
			storeStreamCancel()
			continue
		}

		fetchReq := p2pcommon.StoreFetchRequest{
			BlockID:     blockID,
			Row:         row,
			Col:         col,
			IsRemoteHop: false,
		}

		if err := json.NewEncoder(storeStream).Encode(fetchReq); err != nil {
			queryErrors = append(queryErrors, fmt.Sprintf("failed to send P2P request to %s: %v", storePID, err))
			storeStream.Close()
			storeStreamCancel()
			continue
		}

		var combinedResp CombinedResponse
		if err := json.NewDecoder(storeStream).Decode(&combinedResp); err != nil {
			queryErrors = append(queryErrors, fmt.Sprintf("failed to read P2P response from %s: %v", storePID, err))
			storeStream.Close()
			storeStreamCancel()
			continue
		}
		storeStream.Close()
		storeStreamCancel()

		if combinedResp.Error != "" {
			queryErrors = append(queryErrors, fmt.Sprintf("store node %s returned error: %s", storePID, combinedResp.Error))
			continue
		}

		// Convert retrieved pieces into cda.ReceivedPiece and accumulate independent pieces
		targetK := s.verifier.K()
		for _, p := range combinedResp.Pieces {
			decData, err1 := hex.DecodeString(p.Data)
			decCoeffs, err2 := hex.DecodeString(p.Coeffs)
			decProof, err3 := hex.DecodeString(p.Proof)
			if err1 != nil || err2 != nil || err3 != nil {
				continue
			}

			// Check linear independence against already collected pieces in recvPieces
			existingCoeffs := make([][]byte, len(recvPieces))
			for i, existingP := range recvPieces {
				existingCoeffs[i] = existingP.Data.Coeffs
			}

			if verifier.IsLinearlyIndependent(existingCoeffs, decCoeffs, targetK) {
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
		}

		if len(recvPieces) >= targetK {
			querySuccess = true
			break
		}
	}

	if !querySuccess || len(recvPieces) < s.verifier.K() {
		errMsg := fmt.Sprintf("insufficient independent pieces collected across stores: got %d, expected %d", len(recvPieces), s.verifier.K())
		if len(queryErrors) > 0 {
			errMsg += fmt.Sprintf("; store query details: %s", strings.Join(queryErrors, "; "))
		}
		return SampleResult{
			Row:      row,
			Col:      col,
			Verified: false,
			Error:    errMsg,
		}
	}


	// Perform local algebraic verification using block header
	columnCommitment := header.ColumnComm[col]
	recoveredCell, verified, err := s.verifier.VerifyReconstructedCell(recvPieces, columnCommitment, header.Coeffs, row)
	if err != nil {
		ByzantineDetectionsTotal.Inc()
		if s.crashOnFail {
			log.Fatalf("DAS verification failed (CRASH) for cell [%d, %d]: %v", row, col, err)
		}
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("verification error: %v", err)}
	}

	if !verified {
		ByzantineDetectionsTotal.Inc()
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

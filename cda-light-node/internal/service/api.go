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
	"os"
	"path/filepath"
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

	port int

	waitingHeadersMu sync.Mutex
	waitingHeaders   map[string]chan struct{}

	// Sequential DAS queue: block IDs are enqueued when BlockReady signal is received
	dasQueue       chan string
	dasTriggeredMu sync.Mutex
	dasTriggered   map[string]bool
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
	port int,
	numCols int,
) *APIService {
	if numCols <= 0 {
		numCols = 8
	}
	svc := &APIService{
		publisherAddr:  publisherAddr,
		bootstrapsMap:  bootstrapsMap,
		numCols:        numCols,
		verifier:       v,
		crashOnFail:    crashOnFail,
		host:           h,
		ps:             ps,
		headers:        make(map[string]*BlockHeader),
		routingCache:   make(map[int]cachedRoutingInfo),
		port:           port,
		waitingHeaders: make(map[string]chan struct{}),
		dasQueue:       make(chan string, 1024),
		dasTriggered:   make(map[string]bool),
	}
	return svc
}

func (s *APIService) CacheHeader(header *BlockHeader) {
	s.headersMu.Lock()
	s.headers[header.BlockID] = header
	s.headersMu.Unlock()

	height := p2pcommon.ParseHeightFromBlockID(header.BlockID)
	log.Printf("[Height: %d] [LightNode] Cached block header for block %s", height, header.BlockID)

	// Signal any waiting HTTP handler goroutines
	s.waitingHeadersMu.Lock()
	if ch, exists := s.waitingHeaders[header.BlockID]; exists {
		close(ch)
		delete(s.waitingHeaders, header.BlockID)
	}
	s.waitingHeadersMu.Unlock()
}

// StartDASWorker starts the sequential DAS queue worker. Call once after construction.
func (s *APIService) StartDASWorker(ctx context.Context, numRandomSamples int) {
	go s.dasWorkerLoop(ctx, numRandomSamples)
}

// dasWorkerLoop processes DAS tasks sequentially, one block at a time.
func (s *APIService) dasWorkerLoop(ctx context.Context, numRandomSamples int) {
	for {
		select {
		case blockID, ok := <-s.dasQueue:
			if !ok {
				return
			}
			// Wait for header availability with a bounded timeout
			header := s.waitForHeader(blockID, 30*time.Second)
			if header == nil {
				log.Printf("[Auto-DAS] [LightNode] Header not available for block %s after timeout, skipping DAS", blockID)
				continue
			}
			s.TriggerAutoDAS(header, numRandomSamples)
		case <-ctx.Done():
			return
		}
	}
}

// waitForHeader waits until the header for blockID is cached or the deadline is reached.
func (s *APIService) waitForHeader(blockID string, timeout time.Duration) *BlockHeader {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.headersMu.RLock()
		h := s.headers[blockID]
		s.headersMu.RUnlock()
		if h != nil {
			return h
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// EnqueueBlockReady is called when a BlockReady GossipSub signal is received.
// It enqueues the block ID for sequential DAS, deduplicating by block ID.
func (s *APIService) EnqueueBlockReady(blockID string) {
	s.dasTriggeredMu.Lock()
	defer s.dasTriggeredMu.Unlock()
	if s.dasTriggered[blockID] {
		return
	}
	s.dasTriggered[blockID] = true
	height := p2pcommon.ParseHeightFromBlockID(blockID)
	log.Printf("[Auto-DAS] [Height: %d] [LightNode] BlockReady signal received for %s — enqueuing for DAS", height, blockID)
	select {
	case s.dasQueue <- blockID:
	default:
		log.Printf("[Auto-DAS] [LightNode] DAS queue full, dropping block %s", blockID)
	}
}

// TriggerAutoDAS executes Data Availability Sampling automatically when a BlockHeader is received via GossipSub.
func (s *APIService) TriggerAutoDAS(header *BlockHeader, numRandomSamples int) {
	if header == nil || header.BlockID == "" {
		return
	}

	blockID := header.BlockID
	height := p2pcommon.ParseHeightFromBlockID(blockID)
	n := len(header.ColumnComm)
	if n == 0 {
		return
	}

	colsPerNetCol := n / s.numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}

	// Build the complete pool of cells belonging to active columns
	var allActiveCells [][2]int
	for rIdx := 0; rIdx < n; rIdx++ {
		for cIdx := 0; cIdx < n; cIdx++ {
			netColIdx := cIdx / colsPerNetCol
			if _, active := s.bootstrapsMap[netColIdx]; active {
				allActiveCells = append(allActiveCells, [2]int{rIdx, cIdx})
			}
		}
	}
	if len(allActiveCells) == 0 {
		return
	}

	totalCells := len(allActiveCells)

	// Determine how many cells to sample
	sampleCount := numRandomSamples
	if sampleCount <= 0 {
		// Default: sample 25% of all active cells
		sampleCount = totalCells / 4
		if sampleCount < 1 {
			sampleCount = 1
		}
	}
	if sampleCount > totalCells {
		sampleCount = totalCells
	}

	// Fisher-Yates partial shuffle to pick sampleCount unique cells
	pool := make([][2]int, totalCells)
	copy(pool, allActiveCells)
	for i := 0; i < sampleCount; i++ {
		j := i + rand.Intn(totalCells-i)
		pool[i], pool[j] = pool[j], pool[i]
	}
	targetCells := pool[:sampleCount]

	log.Printf("[Auto-DAS] [Height: %d] 🚀 BlockReady triggered DAS for %s — sampling %d/%d active cells (%.0f%%)...",
		height, blockID, sampleCount, totalCells, float64(sampleCount)/float64(totalCells)*100)
	startTime := time.Now()

	results := make([]SampleResult, len(targetCells))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16) // limit concurrency to 16 goroutines

	for idx, cell := range targetCells {
		wg.Add(1)
		go func(idx int, r, c int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[idx] = s.sampleCell(blockID, r, c, header)
		}(idx, cell[0], cell[1])
	}
	wg.Wait()

	verifiedCount := 0
	for _, res := range results {
		if res.Verified {
			verifiedCount++
		}
	}

	duration := time.Since(startTime)
	if verifiedCount == len(results) {
		log.Printf("[Auto-DAS] [Height: %d] ✅ DAS VERIFIED for %s (%d/%d sampled cells verified in %v)",
			height, blockID, verifiedCount, len(results), duration)
	} else {
		failedCells := []string{}
		for _, res := range results {
			if !res.Verified && len(failedCells) < 10 {
				failedCells = append(failedCells, fmt.Sprintf("[%d,%d]:%s", res.Row, res.Col, res.Error))
			}
		}
		log.Printf("[Auto-DAS] [Height: %d] ⚠️ DAS Partial for %s (%d/%d verified in %v). Failed cells: %v",
			height, blockID, verifiedCount, len(results), duration, failedCells)
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

	// 1. Get header from local P2P cache or fallback to HTTP from Publisher
	s.headersMu.RLock()
	header, exists := s.headers[blockID]
	s.headersMu.RUnlock()

	if !exists {
		height := p2pcommon.ParseHeightFromBlockID(blockID)
		log.Printf("[Height: %d] [LightNode] Header for %s not in local cache. Fetching from Publisher via HTTP...", height, blockID)

		headerUrl := fmt.Sprintf("%s/header/%s", s.publisherAddr, blockID)
		resp, err := http.Get(headerUrl)
		if err == nil && resp.StatusCode == http.StatusOK {
			header = &BlockHeader{}
			if err := json.NewDecoder(resp.Body).Decode(header); err == nil {
				s.CacheHeader(header)
				exists = true
			}
			resp.Body.Close()
		} else if resp != nil {
			resp.Body.Close()
		}
	}

	if !exists {
		height := p2pcommon.ParseHeightFromBlockID(blockID)
		log.Printf("[Height: %d] [LightNode] Block %s not published yet. Waiting for GossipSub broadcast or Publisher upload...", height, blockID)

		s.waitingHeadersMu.Lock()
		ch, existsCh := s.waitingHeaders[blockID]
		if !existsCh {
			ch = make(chan struct{})
			s.waitingHeaders[blockID] = ch
		}
		s.waitingHeadersMu.Unlock()

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		timeout := time.After(60 * time.Second)
		headerUrl := fmt.Sprintf("%s/header/%s", s.publisherAddr, blockID)

		for !exists {
			select {
			case <-ch:
				s.headersMu.RLock()
				header, exists = s.headers[blockID]
				s.headersMu.RUnlock()
			case <-ticker.C:
				resp, err := http.Get(headerUrl)
				if err == nil {
					if resp.StatusCode == http.StatusOK {
						header = &BlockHeader{}
						if err := json.NewDecoder(resp.Body).Decode(header); err == nil {
							s.CacheHeader(header)
							exists = true
						}
					}
					resp.Body.Close()
				}
			case <-r.Context().Done():
				http.Error(w, "Request context canceled while waiting for header", http.StatusRequestTimeout)
				return
			case <-timeout:
				http.Error(w, "Timeout waiting for block header via GossipSub and HTTP", http.StatusNotFound)
				return
			}
		}
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
		colsPerNetCol := n / s.numCols
		if colsPerNetCol == 0 {
			colsPerNetCol = 1
		}

		var activeCells [][2]int
		for rIdx := 0; rIdx < n; rIdx++ {
			for cIdx := 0; cIdx < n; cIdx++ {
				netColIdx := cIdx / colsPerNetCol
				if _, active := s.bootstrapsMap[netColIdx]; active {
					activeCells = append(activeCells, [2]int{rIdx, cIdx})
				}
			}
		}

		results = make([]SampleResult, len(activeCells))
		var wg sync.WaitGroup
		sem := make(chan struct{}, 16) // limit concurrency to 16 goroutines
		for idx, cell := range activeCells {
			wg.Add(1)
			go func(idx int, r, c int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				res := s.sampleCell(blockID, r, c, header)
				results[idx] = res
			}(idx, cell[0], cell[1])
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

		colsPerNetCol := n / s.numCols
		if colsPerNetCol == 0 {
			colsPerNetCol = 1
		}

		var activeCols []int
		for cIdx := 0; cIdx < n; cIdx++ {
			netColIdx := cIdx / colsPerNetCol
			if _, active := s.bootstrapsMap[netColIdx]; active {
				activeCols = append(activeCols, cIdx)
			}
		}

		if len(activeCols) == 0 {
			http.Error(w, "No active columns configured for sampling", http.StatusInternalServerError)
			return
		}

		for i := 0; i < numSamples; i++ {
			row := rand.Intn(n)
			col := activeCols[rand.Intn(len(activeCols))]
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

func (s *APIService) getRoutingInfo(netColIdx int, colsPerNetCol int, row int, col int) ([]p2pcommon.PeerInfo, error) {
	s.routingCacheMu.RLock()
	if info, ok := s.routingCache[netColIdx]; ok && time.Since(info.timestamp) < 3*time.Second {
		s.routingCacheMu.RUnlock()
		return info.stores, nil
	}
	s.routingCacheMu.RUnlock()

	bootAddrs, ok := s.bootstrapsMap[netColIdx]
	
	// Dynamic Matrix Discovery: If bootstrap for netColIdx is not known, ask known seed bootstrap(s)
	if !ok || len(bootAddrs) == 0 {
		var seedAddrs []string
		var seedColIdx int
		for k, v := range s.bootstrapsMap {
			if len(v) > 0 {
				seedColIdx = k
				seedAddrs = v
				break
			}
		}
		if len(seedAddrs) > 0 {
			targetColIdx := netColIdx * colsPerNetCol
			_, seedPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", seedColIdx*colsPerNetCol))
			if err == nil {
				for _, sAddr := range seedAddrs {
					resolvedSeed := sAddr
					if strings.HasPrefix(resolvedSeed, "http://") || strings.HasPrefix(resolvedSeed, "https://") {
						u, err := url.Parse(resolvedSeed)
						if err == nil {
							hostStr := u.Hostname()
							portStr := u.Port()
							if portVal, err := strconv.Atoi(portStr); err == nil {
								p2pPort := portVal + 10000
								if hostStr == "localhost" || hostStr == "127.0.0.1" {
									resolvedSeed = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p2pPort)
								} else {
									resolvedSeed = fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, p2pPort)
								}
							}
						}
					}
					seedFull := fmt.Sprintf("%s/p2p/%s", resolvedSeed, seedPID.String())
					maddr, err := multiaddr.NewMultiaddr(seedFull)
					if err == nil {
						if seedInfo, err := peer.AddrInfoFromP2pAddr(maddr); err == nil {
							ctxSeed, cancelSeed := context.WithTimeout(context.Background(), 5*time.Second)
							_ = s.host.Connect(ctxSeed, *seedInfo)
							stream, errStream := s.host.NewStream(ctxSeed, seedInfo.ID, p2pcommon.ProtoBootstrapRouting)
							if errStream == nil {
								req := p2pcommon.BootstrapRoutingRequest{
									Peer: p2pcommon.PeerInfo{
										PeerID:     s.host.ID().String(),
										Multiaddrs: []string{},
										Row:        -1,
										Col:        -1,
									},
									TargetRow: row,
									TargetCol: targetColIdx,
								}
								if err := json.NewEncoder(stream).Encode(req); err == nil {
									var resp p2pcommon.BootstrapRoutingResponse
									if err := json.NewDecoder(stream).Decode(&resp); err == nil {
										var discoveredStores []p2pcommon.PeerInfo
										for _, p := range resp.ColPeers {
											if p.Row == -2 {
												// Found Bootstrap Node for a column
												pNetCol := p.Col / colsPerNetCol
												if len(p.Multiaddrs) > 0 {
													s.bootstrapsMap[pNetCol] = p.Multiaddrs
												}
											} else {
												pNetCol := p.Col / colsPerNetCol
												if pNetCol == netColIdx || p.Col == col {
													discoveredStores = append(discoveredStores, p)
												}
											}
										}
										for _, p := range resp.RowPeers {
											if p.Row >= 0 {
												pNetCol := p.Col / colsPerNetCol
												if pNetCol == netColIdx || p.Col == col {
													discoveredStores = append(discoveredStores, p)
												}
											}
										}
										if len(discoveredStores) > 0 {
											s.routingCacheMu.Lock()
											s.routingCache[netColIdx] = cachedRoutingInfo{
												stores:    discoveredStores,
												timestamp: time.Now(),
											}
											s.routingCacheMu.Unlock()
											stream.Close()
											cancelSeed()
											return discoveredStores, nil
										}
									}
								}
								stream.Close()
							}
							cancelSeed()
						}
					}
				}
			}
		}
		// Refresh bootAddrs if discovered
		bootAddrs = s.bootstrapsMap[netColIdx]
	}

	if len(bootAddrs) == 0 {
		return nil, fmt.Errorf("no bootstrap node configured or dynamically discovered for network column %d (colsPerNetCol=%d, numCols=%d)", netColIdx, colsPerNetCol, s.numCols)
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
		recordDASAttempt(false)
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("routing query failed on all bootstraps: %v", err)}
	}

	var recvPieces []cda.ReceivedPiece
	var queryErrors []string
	var querySuccess bool

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

	maxAttempts := 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Re-shuffle active stores on each attempt
		shuffledStores := append([]p2pcommon.PeerInfo(nil), activeStores...)
		for i := range shuffledStores {
			j := rand.Intn(i + 1)
			shuffledStores[i], shuffledStores[j] = shuffledStores[j], shuffledStores[i]
		}

		maxTries := len(shuffledStores)
		for idx := 0; idx < maxTries; idx++ {
			selectedStore := shuffledStores[idx]
			storePID, err := peer.Decode(selectedStore.PeerID)
			if err != nil {
				queryErrors = append(queryErrors, fmt.Sprintf("invalid store Peer ID %s: %v", selectedStore.PeerID, err))
				continue
			}

			height := p2pcommon.ParseHeightFromBlockID(blockID)
			log.Printf("[DAS] [Height: %d] Querying cell [%d, %d] of block %s (attempt %d/%d). Dialing store node %s...",
				height, row, col, blockID, attempt+1, maxAttempts, storePID)

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

			log.Printf("[DAS] [Height: %d] Received response from store node %s with %d pieces. Independent pieces accumulated: %d/%d",
				height, storePID, len(combinedResp.Pieces), len(recvPieces), targetK)

			if len(recvPieces) >= targetK {
				querySuccess = true
				break
			}
		}

		if querySuccess {
			break
		}

		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(600 * time.Millisecond):
			}
		}
	}

	if !querySuccess || len(recvPieces) < s.verifier.K() {
		recordDASAttempt(false)
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
		recordDASAttempt(false)
		ByzantineDetectionsTotal.Inc()
		if s.crashOnFail {
			log.Fatalf("DAS verification failed (CRASH) for cell [%d, %d]: %v", row, col, err)
		}
		return SampleResult{Row: row, Col: col, Verified: false, Error: fmt.Sprintf("verification error: %v", err)}
	}

	if !verified {
		recordDASAttempt(false)
		ByzantineDetectionsTotal.Inc()
		if s.crashOnFail {
			log.Fatalf("DAS verification failed (CRASH) for cell [%d, %d]: algebraic direct verification failed", row, col)
		}
		return SampleResult{Row: row, Col: col, Verified: false, Error: "algebraic direct verification failed"}
	}

	recordDASAttempt(true)

	height := p2pcommon.ParseHeightFromBlockID(blockID)
	log.Printf("[DAS] [Height: %d] Cell [%d, %d] algebraic verification succeeded! Reconstructed cell data (hex): %x", height, row, col, recoveredCell)

	if s.port > 0 {
		logDir := fmt.Sprintf("data/light_%d", s.port)
		_ = os.MkdirAll(logDir, 0755)
		logPath := filepath.Join(logDir, "das_success.log")
		f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			defer f.Close()
			timestamp := time.Now().Format("2006-01-02 15:04:05")
			logLine := fmt.Sprintf("[%s] [Height: %d] Block %s, Cell [%d, %d]: DAS sampling verification succeeded. Reconstructed cell: %x\n",
				timestamp, height, blockID, row, col, recoveredCell)
			_, _ = f.WriteString(logLine)
		}
	}

	return SampleResult{
		Row:      row,
		Col:      col,
		Verified: true,
		CellData: recoveredCell,
	}
}

package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/klauspost/reedsolomon"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type CachedBlock struct {
	BlockID   string
	Height    int64
	ODS       [][]byte
	CachedAt  time.Time
}

type BlockReconstructor struct {
	host         host.Host
	disc         DiscoveryProvider
	pipeline     *Pipeline
	cacheMu      sync.RWMutex
	lruCache     map[string]*CachedBlock
	cacheOrder   []string
	maxCacheSize int
	cacheTTL     time.Duration

	routingMu    sync.RWMutex
	routingCache map[int][]p2pcommon.PeerInfo
}

type DiscoveryProvider interface {
	FindBootstrapNode(colIdx int, nOpt ...int) (string, error)
	GetBootstrapColID(colIdx int, nOpt ...int) int
}

func NewBlockReconstructor(h host.Host, disc DiscoveryProvider, p *Pipeline) *BlockReconstructor {
	return &BlockReconstructor{
		host:         h,
		disc:         disc,
		pipeline:     p,
		lruCache:     make(map[string]*CachedBlock),
		cacheOrder:   make([]string, 0, 16),
		maxCacheSize: 16,
		cacheTTL:     10 * time.Minute,
		routingCache: make(map[int][]p2pcommon.PeerInfo),
	}
}

func (r *BlockReconstructor) GetCachedODS(blockID string) ([][]byte, bool) {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	item, exists := r.lruCache[blockID]
	if !exists {
		return nil, false
	}
	if time.Since(item.CachedAt) > r.cacheTTL {
		return nil, false
	}
	return item.ODS, true
}

func (r *BlockReconstructor) PutCachedODS(blockID string, height int64, ods [][]byte) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	if _, exists := r.lruCache[blockID]; !exists {
		if len(r.cacheOrder) >= r.maxCacheSize {
			oldest := r.cacheOrder[0]
			r.cacheOrder = r.cacheOrder[1:]
			delete(r.lruCache, oldest)
		}
		r.cacheOrder = append(r.cacheOrder, blockID)
	}

	r.lruCache[blockID] = &CachedBlock{
		BlockID:  blockID,
		Height:   height,
		ODS:      ods,
		CachedAt: time.Now(),
	}
}

// RecoverODS pulls pieces from Store Nodes across columns, decodes RLNC cells, and recovers the K x K ODS matrix.
func (r *BlockReconstructor) RecoverODS(ctx context.Context, blockID string, height int64, k int) ([][]byte, error) {
	if k <= 0 {
		k = r.pipeline.k
	}
	if k <= 0 {
		k = 16
	}
	n := 2 * k

	if cached, ok := r.GetCachedODS(blockID); ok {
		log.Printf("[Reconstructor] Cache hit for BlockID: %s (K=%d)", blockID, k)
		return cached, nil
	}

	log.Printf("[Reconstructor] Cache miss. Initiating distributed P2P block recovery for %s (Height: %d, K: %d, N: %d)...",
		blockID, height, k, n)

	// Step 1: Recover EDS matrix cells by querying Store Nodes per column.
	// We need at least K columns to reconstruct the full matrix using Reed-Solomon.
	// Prefer retrieving ODS columns (0..K-1) directly.
	edsCells := make([][][]byte, n) // [row][col]
	for row := 0; row < n; row++ {
		edsCells[row] = make([][]byte, n)
	}

	var edsMu sync.Mutex
	recoveredCols := make(map[int]bool)
	var wg sync.WaitGroup

	// Try recovering columns 0..N-1 concurrently
	for col := 0; col < n; col++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			colPiecesMap, err := r.fetchColCells(ctx, blockID, c, n)
			if err != nil {
				return
			}

			codec := rlnc.NewRLNCCodec(r.pipeline.k)
			recoveredRowCount := 0

			for row := 0; row < n; row++ {
				key := fmt.Sprintf("%d_%d", row, c)
				pieces := colPiecesMap[key]
				if len(pieces) < r.pipeline.k {
					continue
				}

				rlncPieces := make([]rlnc.PieceData, len(pieces))
				for i := 0; i < len(pieces); i++ {
					rlncPieces[i] = pieces[i].Data
				}

				frags, err := codec.Decode(rlncPieces)
				if err != nil {
					continue
				}

				var fullCell []byte
				for _, frag := range frags {
					fullCell = append(fullCell, frag...)
				}

				edsMu.Lock()
				edsCells[row][c] = fullCell
				edsMu.Unlock()
				recoveredRowCount++
			}

			if recoveredRowCount > 0 {
				edsMu.Lock()
				recoveredCols[c] = true
				edsMu.Unlock()
			}
		}(col)
	}

	wg.Wait()

	edsMu.Lock()
	numRecoveredCols := len(recoveredCols)
	edsMu.Unlock()

	log.Printf("[Reconstructor] Successfully retrieved %d / %d columns from Store Nodes for %s", numRecoveredCols, n, blockID)

	if numRecoveredCols == 0 {
		return nil, fmt.Errorf("no columns recovered from Store Nodes for %s", blockID)
	}

	// Step 2: Determine cell size from recovered cells
	cellSize := 512
	for rIdx := 0; rIdx < n; rIdx++ {
		for cIdx := 0; cIdx < n; cIdx++ {
			if len(edsCells[rIdx][cIdx]) > 0 {
				cellSize = len(edsCells[rIdx][cIdx])
				break
			}
		}
	}

	// Step 3: Horizontal 2D Reed-Solomon Reconstruction if we have at least K columns
	if numRecoveredCols >= k {
		rsEncoder, err := reedsolomon.New(k, k, reedsolomon.WithLeopardGF(true))
		if err == nil {
			for row := 0; row < k; row++ {
				rowShares := make([][]byte, n)
				needsReconstruct := false

				for col := 0; col < n; col++ {
					if len(edsCells[row][col]) > 0 {
						rowShares[col] = append([]byte(nil), edsCells[row][col]...)
					} else {
						rowShares[col] = nil
						if col < k {
							needsReconstruct = true
						}
					}
				}

				if needsReconstruct {
					if err := rsEncoder.Reconstruct(rowShares); err == nil {
						for col := 0; col < k; col++ {
							edsCells[row][col] = rowShares[col]
						}
					}
				}
			}
		}
	} else {
		log.Printf("[Reconstructor] %d columns recovered (< K=%d). Populating direct column cells without RS extrapolation.",
			numRecoveredCols, k)
	}

	// Step 4: Extract K x K ODS matrix
	ods := make([][]byte, k*k)
	populatedCells := 0
	for rIdx := 0; rIdx < k; rIdx++ {
		for cIdx := 0; cIdx < k; cIdx++ {
			cell := edsCells[rIdx][cIdx]
			if len(cell) > 0 {
				ods[rIdx*k+cIdx] = cell
				populatedCells++
			} else {
				ods[rIdx*k+cIdx] = make([]byte, cellSize)
			}
		}
	}

	if populatedCells > 0 {
		r.PutCachedODS(blockID, height, ods)
	}
	log.Printf("[Reconstructor] Successfully assembled ODS (%d/%d cells populated) for %s", populatedCells, k*k, blockID)

	return ods, nil
}

func (r *BlockReconstructor) fetchColCells(ctx context.Context, blockID string, colIdx, n int) (map[string][]cda.ReceivedPiece, error) {
	stores, err := r.findStoreNodes(ctx, colIdx, n)
	if err != nil || len(stores) == 0 {
		return nil, fmt.Errorf("no store nodes found for col %d: %v", colIdx, err)
	}

	aggregatedPieces := make(map[string][]cda.ReceivedPiece)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, store := range stores {
		wg.Add(1)
		go func(s p2pcommon.PeerInfo) {
			defer wg.Done()
			var targetCoords []p2pcommon.CellCoord
			if s.Row >= 0 {
				targetCoords = []p2pcommon.CellCoord{{Row: s.Row, Col: colIdx}}
			} else {
				targetCoords = make([]p2pcommon.CellCoord, n)
				for r := 0; r < n; r++ {
					targetCoords[r] = p2pcommon.CellCoord{Row: r, Col: colIdx}
				}
			}

			piecesMap, err := r.batchFetchFromStore(ctx, s, blockID, targetCoords)
			if err != nil {
				return
			}

			mu.Lock()
			for key, pieces := range piecesMap {
				aggregatedPieces[key] = append(aggregatedPieces[key], pieces...)
			}
			mu.Unlock()
		}(store)
	}

	wg.Wait()
	return aggregatedPieces, nil
}

func (r *BlockReconstructor) findStoreNodes(ctx context.Context, colIdx, n int) ([]p2pcommon.PeerInfo, error) {
	r.routingMu.RLock()
	if cached, ok := r.routingCache[colIdx]; ok && len(cached) > 0 {
		r.routingMu.RUnlock()
		return cached, nil
	}
	r.routingMu.RUnlock()

	bootAddrStr, err := r.disc.FindBootstrapNode(colIdx, n)
	if err != nil {
		return nil, err
	}

	baseColID := r.disc.GetBootstrapColID(colIdx, n)
	_, bootPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", baseColID))
	if err != nil {
		return nil, err
	}

	bootAddr := bootAddrStr
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

	bootMultiaddrStr := fmt.Sprintf("%s/p2p/%s", bootAddr, bootPID.String())
	maddr, err := multiaddr.NewMultiaddr(bootMultiaddrStr)
	if err != nil {
		return nil, err
	}

	bootInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, err
	}

	r.host.Peerstore().AddAddrs(bootInfo.ID, bootInfo.Addrs, 10*time.Minute)

	connCtx, connCancel := context.WithTimeout(ctx, 4*time.Second)
	_ = r.host.Connect(connCtx, *bootInfo)
	connCancel()

	streamCtx, streamCancel := context.WithTimeout(ctx, 5*time.Second)
	defer streamCancel()

	stream, err := r.host.NewStream(streamCtx, bootInfo.ID, p2pcommon.ProtoBootstrapRouting)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	req := p2pcommon.BootstrapRoutingRequest{
		Peer: p2pcommon.PeerInfo{
			PeerID:     r.host.ID().String(),
			Multiaddrs: []string{},
			Row:        -1,
			Col:        -1,
		},
		TargetRow: 0,
		TargetCol: colIdx,
	}

	if err := json.NewEncoder(stream).Encode(req); err != nil {
		return nil, err
	}

	var resp p2pcommon.BootstrapRoutingResponse
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		return nil, err
	}

	r.routingMu.Lock()
	r.routingCache[colIdx] = resp.ColPeers
	r.routingMu.Unlock()

	return resp.ColPeers, nil
}

func (r *BlockReconstructor) batchFetchFromStore(ctx context.Context, store p2pcommon.PeerInfo, blockID string, coords []p2pcommon.CellCoord) (map[string][]cda.ReceivedPiece, error) {
	pid, err := peer.Decode(store.PeerID)
	if err != nil {
		return nil, err
	}

	for _, mStr := range store.Multiaddrs {
		if m, err := multiaddr.NewMultiaddr(mStr); err == nil {
			r.host.Peerstore().AddAddr(pid, m, 10*time.Minute)
		}
		// If multiaddr contains store-0-<N>, map to localhost port (19300 + N - 1) for native host execution
		for n := 1; n <= 8; n++ {
			if strings.Contains(mStr, fmt.Sprintf("store-0-%d", n)) {
				localPort := 19300 + (n - 1)
				if mLocal, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", localPort)); err == nil {
					r.host.Peerstore().AddAddr(pid, mLocal, 10*time.Minute)
				}
				break
			}
		}
	}

	connCtx, connCancel := context.WithTimeout(ctx, 3*time.Second)
	_ = r.host.Connect(connCtx, peer.AddrInfo{ID: pid, Addrs: r.host.Peerstore().Addrs(pid)})
	connCancel()

	streamCtx, streamCancel := context.WithTimeout(ctx, 8*time.Second)
	defer streamCancel()

	stream, err := r.host.NewStream(streamCtx, pid, p2pcommon.ProtoStoreBatchFetch)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	req := p2pcommon.StoreBatchFetchRequest{
		BlockID: blockID,
		Cells:   coords,
	}

	if err := json.NewEncoder(stream).Encode(req); err != nil {
		return nil, err
	}

	var resp p2pcommon.StoreBatchFetchResponse
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		return nil, err
	}

	result := make(map[string][]cda.ReceivedPiece)
	for key, pieces := range resp.Cells {
		for _, p := range pieces {
			decData, err1 := hex.DecodeString(p.Data)
			decCoeffs, err2 := hex.DecodeString(p.Coeffs)
			if err1 != nil || err2 != nil {
				continue
			}

			var decProof []byte
			if p.Proof != "" {
				decProof, _ = hex.DecodeString(p.Proof)
			}

			result[key] = append(result[key], cda.ReceivedPiece{
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

	return result, nil
}

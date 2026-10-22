package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"cda-bootstrap-node/internal/engine"
	"cda-bootstrap-node/internal/storage"
	"cda-bootstrap-node/internal/verifier"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/dgraph-io/badger/v4"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

func getEnvInt(key string, defaultVal int) int {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return defaultVal
}

type Receiver struct {
	host          host.Host
	kzg           cda.KZGProvider
	publisherAddr string
	k             int
	cache         *storage.LocalCache
	proofGen      *engine.ProofGenerator
	encoder       *engine.RLNCEncoder
	broadcaster   *Broadcaster
	crashOnFail   bool
	db            *badger.DB

	// Network Matrix & Dynamic Peer Registry
	peersMu      sync.RWMutex
	activePeers  map[peer.ID]p2pcommon.PeerInfo
	lastSeenPeer map[peer.ID]time.Time
	colID        int
	kBlock       int
	netRows      int
	netCols      int
	peerTTL      time.Duration

	pruneEnable bool
	pruneTTL    time.Duration

	// Multi-block Pipelining & Pre-computation State
	pipelineMu              sync.Mutex
	pipelineCond            *sync.Cond
	latestCompletedHeight   int
	currentDispatchedHeight int
	prevDispatchedHeight    int
	dispatchedDeadline      time.Time
	phase2Sem               chan struct{}
}

func NewReceiver(
	h host.Host,
	kzg cda.KZGProvider,
	publisherAddr string,
	k int,
	cache *storage.LocalCache,
	proofGen *engine.ProofGenerator,
	encoder *engine.RLNCEncoder,
	broadcaster *Broadcaster,
	crashOnFail bool,
	colID int,
	pruneEnable bool,
	pruneTTL time.Duration,
) *Receiver {
	rcv := &Receiver{
		host:          h,
		kzg:           kzg,
		publisherAddr: publisherAddr,
		k:             k,
		colID:         colID,
		kBlock:        16,
		cache:         cache,
		proofGen:      proofGen,
		encoder:       encoder,
		broadcaster:   broadcaster,
		crashOnFail:   crashOnFail,
		activePeers:   make(map[peer.ID]p2pcommon.PeerInfo),
		lastSeenPeer:  make(map[peer.ID]time.Time),
		netRows:       getEnvInt("CDA_NET_ROWS", 8),
		netCols:       getEnvInt("CDA_NET_COLS", 8),
		peerTTL:       15 * time.Second,
		pruneEnable:   pruneEnable,
		pruneTTL:      pruneTTL,
		phase2Sem:     make(chan struct{}, getEnvInt("BOOTSTRAP_PHASE2_CONCURRENCY", 4)),
	}
	rcv.pipelineCond = sync.NewCond(&rcv.pipelineMu)
	// Connect broadcaster back to registry
	broadcaster.SetRegistry(rcv)

	if colID >= 0 {
		baseDir := fmt.Sprintf("data/bootstrap_%d", colID)
		dbPath := filepath.Join(baseDir, "badger")
		_ = os.MkdirAll(dbPath, 0755)

		opts := badger.DefaultOptions(dbPath).WithLogger(nil)
		db, err := badger.Open(opts)
		if err != nil {
			panic(fmt.Sprintf("failed to open badger db on bootstrap: %v", err))
		}
		rcv.db = db

		// Load active peers from BadgerDB
		_ = db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			prefix := []byte("peer_")
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				k := item.Key()
				pidStr := string(k[len(prefix):])
				pid, err := peer.Decode(pidStr)
				if err != nil {
					continue
				}
				_ = item.Value(func(val []byte) error {
					var info p2pcommon.PeerInfo
					if err := json.Unmarshal(val, &info); err == nil {
						rcv.activePeers[pid] = info
						rcv.lastSeenPeer[pid] = time.Now() // Give peerTTL to re-register
					}
					return nil
				})
			}
			return nil
		})
	}

	return rcv
}

// SetNetworkMatrixConfig configures the network row/col dimensions and peer heartbeat expiry TTL.
func (rcv *Receiver) SetNetworkMatrixConfig(netRows int, peerTTL time.Duration, netColsAndK ...int) {
	rcv.peersMu.Lock()
	defer rcv.peersMu.Unlock()
	if netRows > 0 {
		rcv.netRows = netRows
	}
	if peerTTL > 0 {
		rcv.peerTTL = peerTTL
	}
	if len(netColsAndK) > 0 && netColsAndK[0] > 0 {
		rcv.netCols = netColsAndK[0]
	}
	if len(netColsAndK) > 1 && netColsAndK[1] > 0 {
		rcv.kBlock = netColsAndK[1]
	}
}

func (rcv *Receiver) getNetColIdx(colIdx int, n int) int {
	rcv.peersMu.RLock()
	netCols := rcv.netCols
	kBlock := rcv.kBlock
	rcv.peersMu.RUnlock()
	if netCols <= 0 {
		netCols = 8
	}
	if n <= 0 {
		if kBlock <= 0 {
			kBlock = 16
		}
		n = 2 * kBlock
	}
	colsPerNetCol := n / netCols
	if colsPerNetCol <= 0 {
		colsPerNetCol = 1
	}
	if colIdx >= netCols || (colsPerNetCol > 1 && colIdx%colsPerNetCol == 0 && colIdx >= colsPerNetCol) {
		return colIdx / colsPerNetCol
	}
	return colIdx / colsPerNetCol
}

func (rcv *Receiver) Close() error {
	if rcv.db != nil {
		return rcv.db.Close()
	}
	return nil
}

// Start listens to libp2p streams
func (rcv *Receiver) Start(ctx context.Context) {
	rcv.host.SetStreamHandler(p2pcommon.ProtoPublisherPush, rcv.handleReceiveColumn)
	rcv.host.SetStreamHandler(p2pcommon.ProtoBootstrapRouting, rcv.handleRouting)

	// Pre-join GossipSub TopicCellRowSeed channels for this Bootstrap's network column
	rcv.peersMu.RLock()
	netRows := rcv.netRows
	kBlock := rcv.kBlock
	colID := rcv.colID
	rcv.peersMu.RUnlock()
	if netRows <= 0 {
		netRows = 8
	}
	if colID < 0 {
		colID = 0
	}
	netColIdx := rcv.getNetColIdx(colID, 2*kBlock)
	rcv.broadcaster.JoinSeedTopics(netColIdx, netRows)

	// Periodically clean up stale peers (older than peerTTL, default 15s)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var expiredPeers []peer.ID
				rcv.peersMu.Lock()
				ttl := rcv.peerTTL
				if ttl <= 0 {
					ttl = 15 * time.Second
				}
				now := time.Now()
				for pid, lastSeen := range rcv.lastSeenPeer {
					if now.Sub(lastSeen) > ttl {
						log.Printf("[P2P Registry] Removing stale peer %s (inactive for %v > %v)", pid, now.Sub(lastSeen).Round(time.Second), ttl)
						if rcv.db != nil {
							_ = rcv.db.Update(func(txn *badger.Txn) error {
								return txn.Delete([]byte("peer_" + pid.String()))
							})
						}
						delete(rcv.activePeers, pid)
						delete(rcv.lastSeenPeer, pid)
						expiredPeers = append(expiredPeers, pid)
					}
				}
				rcv.peersMu.Unlock()
				for _, pid := range expiredPeers {
					rcv.broadcaster.ClosePersistentStream(pid)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Periodically prune local cache (if enabled)
	if rcv.pruneEnable {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					rcv.cache.Prune(rcv.pruneTTL)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (rcv *Receiver) getCustodySnapshot() (map[int][]p2pcommon.PeerInfo, int, int) {
	rcv.peersMu.RLock()
	defer rcv.peersMu.RUnlock()

	netRows := rcv.netRows
	if netRows <= 0 {
		netRows = 8
	}
	var allPeers []p2pcommon.PeerInfo
	for _, info := range rcv.activePeers {
		allPeers = append(allPeers, info)
	}
	activeStores := p2pcommon.GetActiveColumnStores(allPeers)
	custodyMap := p2pcommon.ComputeColumnCustodyMap(activeStores, netRows)
	return custodyMap, netRows, len(activeStores)
}

// GetCustodySummary returns the current RowCustodyMap, NetRows, and count of active store peers in this column.
func (rcv *Receiver) GetCustodySummary() (map[int][]p2pcommon.PeerInfo, int, int) {
	return rcv.getCustodySnapshot()
}

func (rcv *Receiver) handleReceiveColumn(stream network.Stream) {
	defer stream.Close()

	var payload p2pcommon.PublisherPushRequest
	if err := json.NewDecoder(stream).Decode(&payload); err != nil {
		rcv.respondWithError(stream, fmt.Sprintf("failed to decode payload: %v", err))
		return
	}

	colIdx := payload.ColIdx
	height := p2pcommon.ParseHeightFromBlockID(payload.BlockID)
	log.Printf("[Height: %d] [P2P] Received Push stream for Column %d, Block %s", height, colIdx, payload.BlockID)

	// 1. Decode hex data cells and commitments
	columnData := make([][]byte, len(payload.ColumnData))
	for i, h := range payload.ColumnData {
		b, err := hex.DecodeString(h)
		if err != nil {
			rcv.respondWithError(stream, fmt.Sprintf("invalid hex in column data: %v", err))
			return
		}
		columnData[i] = b
	}

	pieceCommits := make([][]byte, len(payload.PieceCommits))
	for i, h := range payload.PieceCommits {
		b, err := hex.DecodeString(h)
		if err != nil {
			rcv.respondWithError(stream, fmt.Sprintf("invalid hex in piece commitments: %v", err))
			return
		}
		pieceCommits[i] = b
	}

	merkleProofs := make([]cda.MerkleProof, len(payload.MerkleProofs))
	for i, p := range payload.MerkleProofs {
		merkleProofs[i] = cda.MerkleProof{
			Index:    p.Index,
			Siblings: p.Siblings,
		}
	}

	// 2. Verifier: Verify Fiat-Shamir and consistency
	ok, err := verifier.VerifyPublisherData(rcv.kzg, rcv.publisherAddr, payload.BlockID, colIdx, columnData, pieceCommits, merkleProofs, rcv.k)
	if err != nil {
		if rcv.crashOnFail {
			log.Fatalf("[Height: %d] Verifier error for Column %d (CRASH): %v", height, colIdx, err)
		}
		log.Printf("[Height: %d] Verifier error for Column %d: %v", height, colIdx, err)
		rcv.respondWithError(stream, "Verification error: "+err.Error())
		return
	}
	if !ok {
		if rcv.crashOnFail {
			log.Fatalf("[Height: %d] Verifier failed (CRASH): data from publisher for Column %d is not consistent", height, colIdx)
		}
		log.Printf("[Height: %d] Verifier failed: data from publisher for Column %d is not consistent", height, colIdx)
		rcv.respondWithError(stream, "Data consistency check failed")
		return
	}

	log.Printf("[Height: %d] [P2P] Verification succeeded for Column %d", height, colIdx)

	// 3. Store in Local Cache
	rcv.cache.Store(payload.BlockID, colIdx, columnData, pieceCommits)

	// Respond with success to Publisher immediately
	json.NewEncoder(stream).Encode(struct {
		Success bool `json:"success"`
	}{Success: true})

	// 4. Phase 2 (Async Heavy Task & Pipelined Pre-computation)
	go func() {
		log.Printf("[Height: %d] [P2P] Starting Phase 2 Async: computing proofs and seeding RLNC pieces for Column %d", height, colIdx)

		if rcv.phase2Sem != nil {
			rcv.phase2Sem <- struct{}{}
		}
		releasePhase2Sem := sync.OnceFunc(func() {
			if rcv.phase2Sem != nil {
				<-rcv.phase2Sem
			}
		})
		defer releasePhase2Sem()

		// Generate KZG Opening Proofs for Column
		start := time.Now()
		proofs, err := rcv.proofGen.GenerateColumnProofs(colIdx, columnData)
		duration := time.Since(start).Seconds()

		if err != nil {
			log.Printf("[Height: %d] Async Proof Generator error for Column %d: %v", height, colIdx, err)
			return
		}
		KZGProofDuration.Observe(duration)
		rcv.cache.SetProofsForCol(payload.BlockID, colIdx, proofs)
		log.Printf("[Height: %d] Successfully generated opening proofs async for Column %d", height, colIdx)

		// Snapshot the column custody map so seed generation matches active cell size M
		custodyMap, netRows, _ := rcv.getCustodySnapshot()

		// Encode RLNC pieces and pre-compute all seeds in memory buffer for active Store Nodes
		n := len(columnData)
		overrideSeeds := getEnvInt("BOOTSTRAP_TOTAL_SEEDS_PER_ROW", 0)

		type precomputedPiece struct {
			row      int
			pieceIdx int
			piece    *cda.ReceivedPiece
		}

		piecesByRow := make([][]*cda.ReceivedPiece, n)
		var encodeErr error
		var encodeErrOnce sync.Once
		var encodeWg sync.WaitGroup

		workerLimit := getEnvInt("BOOTSTRAP_ENCODE_WORKERS", runtime.NumCPU())
		if workerLimit > n {
			workerLimit = n
		}
		if workerLimit <= 0 {
			workerLimit = 1
		}

		for w := 0; w < workerLimit; w++ {
			encodeWg.Add(1)
			go func(workerID int) {
				defer encodeWg.Done()
				for rowIdx := workerID; rowIdx < n; rowIdx += workerLimit {
					targetNetRow := rowIdx % netRows
					custodyPeers := custodyMap[targetNetRow]
					totalSeeds, _, _ := p2pcommon.ComputeSeedingPlan(len(custodyPeers), rcv.k)
					if overrideSeeds > totalSeeds {
						totalSeeds = overrideSeeds
					}

					codedPieces, err := rcv.encoder.EncodeRowNSeeds(rowIdx, colIdx, columnData, proofs[rowIdx], totalSeeds)
					if err != nil {
						encodeErrOnce.Do(func() {
							encodeErr = fmt.Errorf("async RLNC encoding error for row %d (col %d): %w", rowIdx, colIdx, err)
						})
						return
					}
					piecesByRow[rowIdx] = codedPieces
				}
			}(w)
		}
		encodeWg.Wait()
		releasePhase2Sem()

		if encodeErr != nil {
			log.Printf("[Height: %d] %v", height, encodeErr)
			return
		}

		precomputedPieces := make([]precomputedPiece, 0, n*2*rcv.k)
		for row := 0; row < n; row++ {
			for pieceIdx, piece := range piecesByRow[row] {
				precomputedPieces = append(precomputedPieces, precomputedPiece{
					row:      row,
					pieceIdx: pieceIdx,
					piece:    piece,
				})
			}
		}

		log.Printf("[Height: %d] [Bootstrap Pre-computation] Successfully pre-computed %d opening proofs and %d RLNC seeds in buffer for Column %d, Block %s",
			height, n, len(precomputedPieces), colIdx, payload.BlockID)

		// Sequential Completion Gate: Wait until store nodes complete the PREVIOUS in-flight block before dispatching.
		rcv.pipelineMu.Lock()
		if height > rcv.currentDispatchedHeight {
			rcv.prevDispatchedHeight = rcv.currentDispatchedHeight
			rcv.currentDispatchedHeight = height
			rcv.dispatchedDeadline = time.Now().Add(8 * time.Second)
			if rcv.prevDispatchedHeight > 0 && rcv.prevDispatchedHeight > rcv.latestCompletedHeight {
				log.Printf("[Height: %d] [Bootstrap Pipeline] Pre-computed buffer ready! Waiting for in-flight block-%d to complete before dispatching...",
					height, rcv.prevDispatchedHeight)
			}
		}
		prevHeight := rcv.prevDispatchedHeight
		deadline := rcv.dispatchedDeadline
		if height == rcv.currentDispatchedHeight && prevHeight > 0 && prevHeight > rcv.latestCompletedHeight {
			for rcv.latestCompletedHeight < prevHeight && time.Now().Before(deadline) {
				rcv.pipelineMu.Unlock()
				time.Sleep(20 * time.Millisecond)
				rcv.pipelineMu.Lock()
			}
		}
		rcv.pipelineMu.Unlock()

		// Dispatch Anchor to GossipSub now that previous block is complete and this block is active
		if err := rcv.broadcaster.BroadcastAnchor(payload.BlockID, colIdx, pieceCommits, merkleProofs); err != nil {
			log.Printf("[Height: %d] Failed to broadcast anchor for Column %d: %v", height, colIdx, err)
		}

		// Group precomputed 2*kPiece seed pools by network row (netRow = item.row % netRows)
		// and publish each pool onto TopicCellRowSeed(netColIdx, netRow).
		rowSeedPools := make(map[int][]p2pcommon.BinarySeedPiece, netRows)
		for _, item := range precomputedPieces {
			targetNetRow := item.row % netRows
			seedReq := p2pcommon.BinarySeedPiece{
				Row:    uint16(item.row),
				Col:    uint16(colIdx),
				Data:   item.piece.Data.Data,
				Coeffs: item.piece.Data.Coeffs,
				Proof:  item.piece.Proof,
			}
			rowSeedPools[targetNetRow] = append(rowSeedPools[targetNetRow], seedReq)
		}

		netColIdx := rcv.getNetColIdx(colIdx, n)
		latestCustodyMap, _, _ := rcv.getCustodySnapshot()

		var wg sync.WaitGroup
		seedingLimit := getEnvInt("BOOTSTRAP_SEEDING_SEM", 128)
		sem := make(chan struct{}, seedingLimit)
		for targetNetRow, sList := range rowSeedPools {
			if len(sList) == 0 {
				continue
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(nRow int, seeds []p2pcommon.BinarySeedPiece) {
				defer wg.Done()
				defer func() { <-sem }()

				batchReq := p2pcommon.BinaryBatchSeedRequest{
					BlockID:      payload.BlockID,
					ColIdx:       uint16(colIdx),
					PieceCommits: pieceCommits,
					Seeds:        seeds,
					SenderPeerID: rcv.host.ID().String(),
				}

				custodyPeers := latestCustodyMap[nRow]
				subCount, err := rcv.broadcaster.BroadcastRowSeedPool(netColIdx, nRow, batchReq)
				if err == nil && subCount >= len(custodyPeers) && subCount > 0 {
					log.Printf("[Height: %d] [GossipSub SeedPool] Published %d seeds for Col %d NetRow %d onto %s (%d subscribers)",
						height, len(seeds), colIdx, nRow, p2pcommon.TopicCellRowSeed(netColIdx, nRow), subCount)
					return
				}

				// Fallback if GossipSub GRAFT has not settled for all assigned custody peers on this row topic
				for _, tp := range custodyPeers {
					if bErr := rcv.broadcaster.BroadcastBinaryBatchPieces(tp, batchReq); bErr != nil {
						log.Printf("[Height: %d] Cold-start fallback seed stream to %s (NetRow %d) failed: %v", height, tp.PeerID, nRow, bErr)
					}
				}
			}(targetNetRow, sList)
		}
		wg.Wait()
		log.Printf("[Height: %d] Async Phase 2 finished successfully: published %d seed pool pieces across %d network rows for Column %d",
			height, len(precomputedPieces), len(rowSeedPools), colIdx)
	}()
}

// OnBlockCompleted records completion of a block height to trigger immediate dispatch of pre-computed seeds for the next block.
func (rcv *Receiver) OnBlockCompleted(height int) {
	rcv.pipelineMu.Lock()
	if height > rcv.latestCompletedHeight {
		rcv.latestCompletedHeight = height
		log.Printf("[Bootstrap Pipeline] Recorded completed height %d (ready to dispatch seeds for height %d)", height, height+1)
		if rcv.pipelineCond != nil {
			rcv.pipelineCond.Broadcast()
		}
	}
	rcv.pipelineMu.Unlock()
}

func (rcv *Receiver) handleRouting(stream network.Stream) {
	defer stream.Close()

	var req p2pcommon.BootstrapRoutingRequest
	if err := json.NewDecoder(stream).Decode(&req); err != nil {
		log.Printf("[P2P Registry] Failed to decode routing request: %v", err)
		return
	}

	pid, err := peer.Decode(req.Peer.PeerID)
	if err != nil {
		log.Printf("[P2P Registry] Invalid PeerID: %s: %v", req.Peer.PeerID, err)
		return
	}

	isLightNode := req.Peer.Row == -1 && req.Peer.Col == -1

	// Update registry
	rcv.peersMu.Lock()
	if !isLightNode {
		if req.IsLeave {
			if rcv.db != nil {
				_ = rcv.db.Update(func(txn *badger.Txn) error {
					return txn.Delete([]byte("peer_" + req.Peer.PeerID))
				})
			}
			delete(rcv.activePeers, pid)
			delete(rcv.lastSeenPeer, pid)
			rcv.peersMu.Unlock()
			rcv.broadcaster.ClosePersistentStream(pid)
			log.Printf("[P2P Registry] Peer gracefully left: %s", pid)
			return
		}

		if rcv.db != nil {
			valData, _ := json.Marshal(req.Peer)
			_ = rcv.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte("peer_" + req.Peer.PeerID), valData)
			})
		}
		rcv.activePeers[pid] = req.Peer
		rcv.lastSeenPeer[pid] = time.Now()
	}

	// Find peers in the requester's row or column
	var rowPeers []p2pcommon.PeerInfo
	var colPeers []p2pcommon.PeerInfo
	var allStorePeers []p2pcommon.PeerInfo

	for p, info := range rcv.activePeers {
		if info.Row >= 0 {
			allStorePeers = append(allStorePeers, info)
		}
		if p == pid {
			continue // skip self in rowPeers/colPeers list
		}
		if isLightNode {
			if req.TargetCol >= 0 {
				if info.Col == req.TargetCol || (info.Row == -2 && info.Col == req.TargetCol) {
					colPeers = append(colPeers, info)
				}
			} else {
				colPeers = append(colPeers, info)
			}
			if req.TargetRow >= 0 {
				if info.Row == req.TargetRow {
					rowPeers = append(rowPeers, info)
				}
			} else {
				rowPeers = append(rowPeers, info)
			}
		} else {
			if info.Row == req.Peer.Row {
				rowPeers = append(rowPeers, info)
			}
			if info.Col == req.Peer.Col {
				colPeers = append(colPeers, info)
			}
		}
	}

	// If light node queried specific column/row but no exact match, return all active peers as fallback
	if isLightNode && (len(colPeers) == 0 || len(rowPeers) == 0) {
		for p, info := range rcv.activePeers {
			if p == pid {
				continue
			}
			if len(colPeers) == 0 {
				colPeers = append(colPeers, info)
			}
			if len(rowPeers) == 0 {
				rowPeers = append(rowPeers, info)
			}
		}
	}

	netRows := rcv.netRows
	if netRows <= 0 {
		netRows = 8
	}
	rcv.peersMu.Unlock()

	activeStores := p2pcommon.GetActiveColumnStores(allStorePeers)
	custodyMap := p2pcommon.ComputeColumnCustodyMap(activeStores, netRows)

	resp := p2pcommon.BootstrapRoutingResponse{
		RowPeers:         rowPeers,
		ColPeers:         colPeers,
		RowCustodyMap:    custodyMap,
		NetRows:          netRows,
		ActivePeersInCol: len(activeStores),
	}

	if err := json.NewEncoder(stream).Encode(resp); err != nil {
		log.Printf("[P2P Registry] Failed to write routing response to %s: %v", pid, err)
	}
}

func (rcv *Receiver) GetPeersForCell(row, col int, pieceIdx int) []p2pcommon.PeerInfo {
	custodyMap, netRows, _ := rcv.getCustodySnapshot()
	if len(custodyMap) == 0 {
		return nil
	}

	targetNetRow := row % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}
	custodyPeers := custodyMap[targetNetRow]
	if len(custodyPeers) == 0 {
		return nil
	}

	_, seedsPerPeer, _ := p2pcommon.ComputeSeedingPlan(len(custodyPeers), rcv.k)
	if seedsPerPeer <= 0 {
		seedsPerPeer = rcv.k
		if seedsPerPeer <= 0 {
			seedsPerPeer = 4
		}
	}

	peerSlot := (pieceIdx / seedsPerPeer) % len(custodyPeers)
	return []p2pcommon.PeerInfo{custodyPeers[peerSlot]}
}

func (rcv *Receiver) GetActivePeers() []string {
	rcv.peersMu.RLock()
	defer rcv.peersMu.RUnlock()
	var list []string
	for _, info := range rcv.activePeers {
		list = append(list, info.Multiaddrs...)
	}
	return list
}

func (rcv *Receiver) respondWithError(stream network.Stream, errMsg string) {
	json.NewEncoder(stream).Encode(struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}{
		Success: false,
		Error:   errMsg,
	})
}


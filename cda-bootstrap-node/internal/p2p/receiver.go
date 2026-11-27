package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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
	pipelineMu            sync.Mutex
	pipelineCond          *sync.Cond
	latestCompletedHeight int
	lastDispatchedHeight  int
	pendingDispatchQueue  []int
	phase2Sem             chan struct{}

	// Block-Level Aggregator Barrier
	barrierMu      sync.Mutex
	pendingBatches map[string]*pendingBlockBatch

	// Byzantine Blacklist & Peer Isolation
	blacklistMu      sync.RWMutex
	blacklistedPeers map[peer.ID]time.Time

	// Contention-Safe Node Trust & Reputation Engine
	repEngine *p2pcommon.ReputationEngine
}

type pendingBlockBatch struct {
	mu           sync.Mutex
	blockID      string
	height       int
	n            int
	expectedCols int
	columns      map[int][][]byte
	pieceCommits map[int][][]byte
	merkleProofs map[int][]cda.MerkleProof
	timer        *time.Timer
	dispatched   bool
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
		pruneEnable:      pruneEnable,
		pruneTTL:         pruneTTL,
		phase2Sem:            make(chan struct{}, getEnvInt("BOOTSTRAP_PHASE2_CONCURRENCY", 4)),
		pendingDispatchQueue: make([]int, 0),
		blacklistedPeers:     make(map[peer.ID]time.Time),
		pendingBatches:   make(map[string]*pendingBlockBatch),
	}
	rcv.pipelineCond = sync.NewCond(&rcv.pipelineMu)
	// Connect broadcaster back to registry
	broadcaster.SetRegistry(rcv)

	rcv.repEngine = p2pcommon.NewReputationEngine(func(rec p2pcommon.PeerReputationRecord) {
		if rcv.db != nil && rec.PeerID != "" {
			if valData, err := json.Marshal(rec); err == nil {
				_ = rcv.db.Update(func(txn *badger.Txn) error {
					return txn.Set([]byte("rep_"+rec.PeerID), valData)
				})
			}
		}
	})

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

		// Load persisted reputation records from BadgerDB first
		_ = db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			prefix := []byte("rep_")
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				_ = it.Item().Value(func(val []byte) error {
					var rec p2pcommon.PeerReputationRecord
					if err := json.Unmarshal(val, &rec); err == nil && rec.PeerID != "" {
						rcv.repEngine.LoadRecord(rec)
					}
					return nil
				})
			}
			return nil
		})

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
						info = rcv.repEngine.EnsurePeer(info)
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

	// Subscribe to TopicByzantineReport for automatic peer isolation & griefing defense
	if rcv.broadcaster != nil && rcv.broadcaster.PubSub() != nil {
		ps := rcv.broadcaster.PubSub()
		byzTopic, err := ps.Join(p2pcommon.TopicByzantineReport)
		if err == nil {
			byzSub, err := byzTopic.Subscribe()
			if err == nil {
				go func() {
					for {
						msg, err := byzSub.Next(ctx)
						if err != nil {
							return
						}
						// Skip reports sent by ourselves
						if msg.ReceivedFrom == rcv.host.ID() {
							continue
						}
						var report p2pcommon.ByzantineNodeReport
						if err := json.Unmarshal(msg.Data, &report); err == nil {
							log.Printf("[Byzantine Monitor] Received report from %s against %s for block %s cell [%d, %d]",
								report.ReporterPeerID, report.ReportedPeerID, report.BlockID, report.Row, report.Col)
							_, _ = rcv.VerifyAndHandleByzantineReport(report)
						}
					}
				}()
			}
		}
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
		if rcv.repEngine != nil {
			info = rcv.repEngine.EnsurePeer(info)
		}
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

	payload, err := p2pcommon.ReadBinaryPublisherPush(stream)
	if err != nil {
		rcv.respondWithBinaryError(stream, fmt.Sprintf("failed to decode binary payload: %v", err))
		return
	}

	colIdx := int(payload.ColIdx)
	height := p2pcommon.ParseHeightFromBlockID(payload.BlockID)
	log.Printf("[Height: %d] [P2P] Received Binary Push stream for Column %d, Block %s", height, colIdx, payload.BlockID)

	columnData := payload.ColumnData
	pieceCommits := payload.PieceCommits

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
		rcv.respondWithBinaryError(stream, "Verification error: "+err.Error())
		return
	}
	if !ok {
		if rcv.crashOnFail {
			log.Fatalf("[Height: %d] Verifier failed (CRASH): data from publisher for Column %d is not consistent", height, colIdx)
		}
		log.Printf("[Height: %d] Verifier failed: data from publisher for Column %d is not consistent", height, colIdx)
		rcv.respondWithBinaryError(stream, "Data consistency check failed")
		return
	}

	log.Printf("[Height: %d] [P2P] Verification succeeded for Column %d", height, colIdx)

	// 3. Store in Local Cache
	rcv.cache.Store(payload.BlockID, colIdx, columnData, pieceCommits)

	// Respond with binary success to Publisher immediately
	_ = p2pcommon.WriteBinaryBatchResponse(stream, &p2pcommon.BinaryBatchSeedResponse{
		Success: true,
	})

	// 4. Block-Level Aggregator Barrier & Async Phase 2
	rcv.enqueueColumnForPhase2(payload.BlockID, height, colIdx, columnData, pieceCommits, merkleProofs)
}

func (rcv *Receiver) enqueueColumnForPhase2(
	blockID string,
	height int,
	colIdx int,
	columnData [][]byte,
	pieceCommits [][]byte,
	merkleProofs []cda.MerkleProof,
) {
	n := len(columnData)
	rcv.peersMu.RLock()
	netCols := rcv.netCols
	rcv.peersMu.RUnlock()
	if netCols <= 0 {
		netCols = 8
	}
	expectedCols := n / netCols
	if expectedCols <= 0 {
		expectedCols = 1
	}

	rcv.barrierMu.Lock()
	batch, exists := rcv.pendingBatches[blockID]
	fallbackTimeout := time.Duration(getEnvInt("BOOTSTRAP_BARRIER_TIMEOUT_MS", 2000)) * time.Millisecond
	if !exists {
		batch = &pendingBlockBatch{
			blockID:      blockID,
			height:       height,
			n:            n,
			expectedCols: expectedCols,
			columns:      make(map[int][][]byte),
			pieceCommits: make(map[int][][]byte),
			merkleProofs: make(map[int][]cda.MerkleProof),
		}
		rcv.pendingBatches[blockID] = batch
		batch.timer = time.AfterFunc(fallbackTimeout, func() {
			rcv.flushBlockBatch(blockID)
		})
	} else {
		// Debounce: reset the fallback timer on every arriving column so we don't prematurely flush
		// while the publisher is actively transmitting the remaining columns of this block!
		batch.mu.Lock()
		if !batch.dispatched && batch.timer != nil {
			batch.timer.Reset(fallbackTimeout)
		}
		batch.mu.Unlock()
	}
	rcv.barrierMu.Unlock()

	batch.mu.Lock()
	if batch.dispatched {
		batch.mu.Unlock()
		log.Printf("[Height: %d] Column %d arrived after batch dispatch, processing individually", height, colIdx)
		go rcv.processPhase2Batch(blockID, height, map[int][][]byte{colIdx: columnData}, map[int][][]byte{colIdx: pieceCommits}, map[int][]cda.MerkleProof{colIdx: merkleProofs})
		return
	}

	batch.columns[colIdx] = columnData
	batch.pieceCommits[colIdx] = pieceCommits
	batch.merkleProofs[colIdx] = merkleProofs

	shouldFlush := len(batch.columns) >= batch.expectedCols
	if shouldFlush && batch.timer != nil {
		batch.timer.Stop()
	}
	batch.mu.Unlock()

	if shouldFlush {
		go rcv.flushBlockBatch(blockID)
	}
}

func (rcv *Receiver) flushBlockBatch(blockID string) {
	rcv.barrierMu.Lock()
	batch, exists := rcv.pendingBatches[blockID]
	if !exists {
		rcv.barrierMu.Unlock()
		return
	}
	rcv.barrierMu.Unlock()

	batch.mu.Lock()
	if batch.dispatched || len(batch.columns) == 0 {
		batch.mu.Unlock()
		return
	}
	batch.dispatched = true
	if batch.timer != nil {
		batch.timer.Stop()
	}
	columns := batch.columns
	pieceCommits := batch.pieceCommits
	merkleProofs := batch.merkleProofs
	height := batch.height
	batch.mu.Unlock()

	// Retain the batch in pendingBatches for 30s so late-arriving columns
	// are cleanly routed to individual processing rather than recreating orphaned batches.
	time.AfterFunc(30*time.Second, func() {
		rcv.barrierMu.Lock()
		delete(rcv.pendingBatches, blockID)
		rcv.barrierMu.Unlock()
	})

	rcv.processPhase2Batch(blockID, height, columns, pieceCommits, merkleProofs)
}

func (rcv *Receiver) processPhase2Batch(
	blockID string,
	height int,
	columnsData map[int][][]byte,
	pieceCommitsMap map[int][][]byte,
	merkleProofsMap map[int][]cda.MerkleProof,
) {
	numCols := len(columnsData)
	log.Printf("[Height: %d] [P2P] Starting Phase 2 Async Batch: computing multi-column proofs and seeding RLNC pieces for %d columns in Block %s",
		height, numCols, blockID)

	if rcv.phase2Sem != nil {
		rcv.phase2Sem <- struct{}{}
	}
	releasePhase2Sem := sync.OnceFunc(func() {
		if rcv.phase2Sem != nil {
			<-rcv.phase2Sem
		}
	})
	defer releasePhase2Sem()

	// 1. Generate KZG Opening Proofs for all columns in this batch with 1 Montgomery inversion!
	start := time.Now()
	allProofs, err := rcv.proofGen.GenerateMultiColumnProofs(columnsData)
	duration := time.Since(start).Seconds()

	if err != nil {
		log.Printf("[Height: %d] Async Proof Generator error for block %s batch: %v", height, blockID, err)
		return
	}
	KZGProofDuration.Observe(duration)

	for colIdx, colProofs := range allProofs {
		rcv.cache.SetProofsForCol(blockID, colIdx, colProofs)
		log.Printf("[Height: %d] Successfully generated opening proofs for Column %d (part of %d-col batch)", height, colIdx, numCols)
	}

	// 2. Snapshot the column custody map
	custodyMap, netRows, _ := rcv.getCustodySnapshot()

	// 3. Encode RLNC pieces and precompute seeds for each column
	type precomputedPiece struct {
		colIdx   int
		row      int
		pieceIdx int
		piece    *cda.ReceivedPiece
	}

	var allPrecomputedPieces []precomputedPiece
	var piecesMu sync.Mutex

	var encodeWg sync.WaitGroup
	var encodeErr error
	var encodeErrOnce sync.Once

	for colIdx, colData := range columnsData {
		colProofs := allProofs[colIdx]
		n := len(colData)
		overrideSeeds := getEnvInt("BOOTSTRAP_TOTAL_SEEDS_PER_ROW", 0)

		encodeWg.Add(1)
		go func(cIdx int, cData [][]byte, cProofs [][][]byte) {
			defer encodeWg.Done()
			workerLimit := getEnvInt("BOOTSTRAP_ENCODE_WORKERS", runtime.NumCPU())
			if workerLimit > n {
				workerLimit = n
			}
			if workerLimit <= 0 {
				workerLimit = 1
			}

			piecesByRow := make([][]*cda.ReceivedPiece, n)
			precomputedFragments := rcv.encoder.PrecomputeColumnFragments(cData)
			var rowWg sync.WaitGroup
			for w := 0; w < workerLimit; w++ {
				rowWg.Add(1)
				go func(workerID int) {
					defer rowWg.Done()
					for rowIdx := workerID; rowIdx < n; rowIdx += workerLimit {
						targetNetRow := rowIdx % netRows
						custodyPeers := custodyMap[targetNetRow]
						totalSeeds, _, _ := p2pcommon.ComputeSeedingPlan(len(custodyPeers), rcv.k)
						if overrideSeeds > totalSeeds {
							totalSeeds = overrideSeeds
						}

						var codedPieces []*cda.ReceivedPiece
						var err error
						if rowIdx < len(precomputedFragments) && len(precomputedFragments[rowIdx]) == rcv.k {
							codedPieces, err = rcv.encoder.EncodeRowNSeedsWithFragments(rowIdx, cIdx, precomputedFragments[rowIdx], cProofs[rowIdx], totalSeeds)
						} else {
							codedPieces, err = rcv.encoder.EncodeRowNSeeds(rowIdx, cIdx, cData, cProofs[rowIdx], totalSeeds)
						}
						if err != nil {
							encodeErrOnce.Do(func() {
								encodeErr = fmt.Errorf("async RLNC encoding error for row %d (col %d): %w", rowIdx, cIdx, err)
							})
							return
						}
						piecesByRow[rowIdx] = codedPieces
					}
				}(w)
			}
			rowWg.Wait()

			if encodeErr == nil {
				colPieces := make([]precomputedPiece, 0, n*2*rcv.k)
				for row := 0; row < n; row++ {
					for pieceIdx, piece := range piecesByRow[row] {
						colPieces = append(colPieces, precomputedPiece{
							colIdx:   cIdx,
							row:      row,
							pieceIdx: pieceIdx,
							piece:    piece,
						})
					}
				}
				piecesMu.Lock()
				allPrecomputedPieces = append(allPrecomputedPieces, colPieces...)
				piecesMu.Unlock()
			}
		}(colIdx, colData, colProofs)
	}
	encodeWg.Wait()
	releasePhase2Sem()

	if encodeErr != nil {
		log.Printf("[Height: %d] Encoding error in batch: %v", height, encodeErr)
		return
	}

	log.Printf("[Height: %d] [Bootstrap Pre-computation] Successfully pre-computed opening proofs and %d RLNC seeds for %d columns in Block %s",
		height, len(allPrecomputedPieces), numCols, blockID)

	// 4. Sequential Completion Gate: Wait until store nodes complete all previous in-flight blocks before dispatching.
	rcv.waitForDispatchTurn(height, blockID)

	// 5. Broadcast Anchors for all columns in this batch
	for colIdx := range columnsData {
		pCommits := pieceCommitsMap[colIdx]
		mProofs := merkleProofsMap[colIdx]
		if err := rcv.broadcaster.BroadcastAnchor(blockID, colIdx, pCommits, mProofs); err != nil {
			log.Printf("[Height: %d] Failed to broadcast anchor for Column %d: %v", height, colIdx, err)
		}
	}

	// 6. Group precomputed seeds by (colIdx, targetNetRow) and broadcast to GossipSub / persistent streams
	type colRowKey struct {
		colIdx int
		netRow int
	}
	seedsByColRow := make(map[colRowKey][]p2pcommon.BinarySeedPiece)
	for _, item := range allPrecomputedPieces {
		targetNetRow := item.row % netRows
		k := colRowKey{colIdx: item.colIdx, netRow: targetNetRow}
		seedReq := p2pcommon.BinarySeedPiece{
			Row:    uint16(item.row),
			Col:    uint16(item.colIdx),
			Data:   item.piece.Data.Data,
			Coeffs: item.piece.Data.Coeffs,
			Proof:  item.piece.Proof,
		}
		seedsByColRow[k] = append(seedsByColRow[k], seedReq)
	}

	latestCustodyMap, _, _ := rcv.getCustodySnapshot()
	seedingLimit := getEnvInt("BOOTSTRAP_SEEDING_SEM", 128)
	sem := make(chan struct{}, seedingLimit)
	var seedWg sync.WaitGroup

	for crKey, sList := range seedsByColRow {
		if len(sList) == 0 {
			continue
		}
		seedWg.Add(1)
		sem <- struct{}{}
		go func(key colRowKey, seeds []p2pcommon.BinarySeedPiece) {
			defer seedWg.Done()
			defer func() { <-sem }()

			cIdx := key.colIdx
			nRow := key.netRow
			pCommits := pieceCommitsMap[cIdx]
			n := len(columnsData[cIdx])
			netColIdx := rcv.getNetColIdx(cIdx, n)

			batchReq := p2pcommon.BinaryBatchSeedRequest{
				BlockID:      blockID,
				ColIdx:       uint16(cIdx),
				PieceCommits: pCommits,
				Seeds:        seeds,
				SenderPeerID: rcv.host.ID().String(),
			}

			if encodedBatch, encErr := p2pcommon.EncodeBinaryBatchSeed(&batchReq); encErr == nil {
				rcv.cache.SetSeedBatchForColRow(blockID, cIdx, nRow, encodedBatch)
			}

			custodyPeers := latestCustodyMap[nRow]
			subCount, err := rcv.broadcaster.BroadcastRowSeedPool(netColIdx, nRow, batchReq)
			if err == nil && subCount > 0 {
				log.Printf("[Height: %d] [GossipSub SeedPool] Published %d seeds for Col %d NetRow %d onto %s (%d subscribers)",
					height, len(seeds), cIdx, nRow, p2pcommon.TopicCellRowSeed(netColIdx, nRow), subCount)
			}

			// Only fall back to persistent binary TCP stream when GossipSub did not reach all assigned custody peers
			if err != nil || subCount < len(custodyPeers) {
				for _, tp := range custodyPeers {
					if bErr := rcv.broadcaster.BroadcastBinaryBatchPieces(tp, batchReq); bErr != nil {
						log.Printf("[Height: %d] Persistent seed stream to %s (Col %d NetRow %d) failed: %v", height, tp.PeerID, cIdx, nRow, bErr)
					}
				}
			}
		}(crKey, sList)
	}
	seedWg.Wait()

	log.Printf("[Height: %d] Async Phase 2 finished successfully: published %d seed pool pieces across %d columns for Block %s",
		height, len(allPrecomputedPieces), numCols, blockID)
}

// waitForDispatchTurn enforces sequential dissemination to Store Nodes:
// Block H holds all precomputed seeds and anchors in Bootstrap RAM until Store Nodes have completed
// custody of all previous blocks (or until safety timeout expires).
func (rcv *Receiver) waitForDispatchTurn(height int, blockID string) {
	if height <= 0 {
		return
	}

	rcv.pipelineMu.Lock()
	defer rcv.pipelineMu.Unlock()

	// Enqueue this height into pendingDispatchQueue in sorted order if not already present
	found := false
	for _, h := range rcv.pendingDispatchQueue {
		if h == height {
			found = true
			break
		}
	}
	if !found {
		rcv.pendingDispatchQueue = append(rcv.pendingDispatchQueue, height)
		sort.Ints(rcv.pendingDispatchQueue)
	}

	dispatchTimeout := time.Duration(getEnvInt("BOOTSTRAP_DISPATCH_TIMEOUT_SEC", 60)) * time.Second
	deadline := time.Now().Add(dispatchTimeout)
	lastPollTime := time.Time{}

	for {
		// Condition 1: Must be the lowest height in the queue
		isHeadOfQueue := len(rcv.pendingDispatchQueue) > 0 && rcv.pendingDispatchQueue[0] == height

		// Condition 2: Store nodes must have completed the last dispatched block
		// (or if 0 Store Nodes have joined the column so far, pre-cache immediately without blocking so future joiners can catch up).
		_, _, activeStoresCount := rcv.getCustodySnapshot()
		prevCompleted := activeStoresCount == 0 || rcv.lastDispatchedHeight == 0 || rcv.latestCompletedHeight >= rcv.lastDispatchedHeight

		if isHeadOfQueue && prevCompleted {
			// Gate cleared! Pop from queue and advance lastDispatchedHeight
			rcv.pendingDispatchQueue = rcv.pendingDispatchQueue[1:]
			rcv.lastDispatchedHeight = height
			log.Printf("[Height: %d] [Bootstrap Pipeline] Sequential gate cleared (latestCompleted=%d, lastDispatched=%d). Dispatching anchors and seeds for %s to Store Nodes!",
				height, rcv.latestCompletedHeight, rcv.lastDispatchedHeight, blockID)
			return
		}

		if time.Now().After(deadline) {
			log.Printf("[Height: %d] [Bootstrap Pipeline] WARNING: Dispatch gate timed out after %v (head=%v, latestCompleted=%d, lastDispatched=%d). Forcing dispatch of %s as safety fallback.",
				height, dispatchTimeout, isHeadOfQueue, rcv.latestCompletedHeight, rcv.lastDispatchedHeight, blockID)
			for i, h := range rcv.pendingDispatchQueue {
				if h == height {
					rcv.pendingDispatchQueue = append(rcv.pendingDispatchQueue[:i], rcv.pendingDispatchQueue[i+1:]...)
					break
				}
			}
			rcv.lastDispatchedHeight = height
			return
		}

		// Fallback active poll to Publisher status every 1.5s in case TopicBlockReady GossipSub was missed
		if time.Since(lastPollTime) >= 1500*time.Millisecond {
			lastPollTime = time.Now()
			go rcv.checkPublisherStatusAsync()
		}

		rcv.pipelineMu.Unlock()
		time.Sleep(30 * time.Millisecond)
		rcv.pipelineMu.Lock()
	}
}

func (rcv *Receiver) checkPublisherStatusAsync() {
	if rcv.publisherAddr == "" {
		return
	}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(rcv.publisherAddr + "/status")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var st struct {
		LatestCompletedHeight int `json:"latest_completed_height"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err == nil && st.LatestCompletedHeight > 0 {
		rcv.OnBlockCompleted(st.LatestCompletedHeight)
	}
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

	if rcv.IsPeerBlacklisted(pid) {
		log.Printf("[P2P Registry] Rejected blacklisted peer %s from joining network matrix", pid)
		rcv.broadcaster.ClosePersistentStream(pid)
		_ = rcv.host.Network().ClosePeer(pid)
		_ = json.NewEncoder(stream).Encode(p2pcommon.BootstrapRoutingResponse{
			RowPeers: nil,
			ColPeers: nil,
		})
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

		if rcv.repEngine != nil {
			req.Peer = rcv.repEngine.EnsurePeer(req.Peer)
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
		if rcv.repEngine != nil {
			info = rcv.repEngine.EnsurePeer(info)
			rcv.activePeers[p] = info
		}
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

func (rcv *Receiver) respondWithBinaryError(stream network.Stream, errMsg string) {
	_ = p2pcommon.WriteBinaryBatchResponse(stream, &p2pcommon.BinaryBatchSeedResponse{
		Success: false,
		Error:   errMsg,
	})
}

// BlacklistPeer adds a peer ID to the blacklist for a specified duration (or permanent if <= 0)
// and closes all active streams/connections to that peer.
func (rcv *Receiver) BlacklistPeer(pid peer.ID, duration time.Duration, reason string) {
	rcv.blacklistMu.Lock()
	if rcv.blacklistedPeers == nil {
		rcv.blacklistedPeers = make(map[peer.ID]time.Time)
	}
	expiry := time.Now().Add(duration)
	if duration <= 0 {
		expiry = time.Now().Add(24 * 365 * time.Hour) // ~1 year (effectively permanent)
	}
	rcv.blacklistedPeers[pid] = expiry
	rcv.blacklistMu.Unlock()

	// Evict from active peers registry
	rcv.peersMu.Lock()
	delete(rcv.activePeers, pid)
	delete(rcv.lastSeenPeer, pid)
	if rcv.db != nil {
		_ = rcv.db.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte("peer_" + pid.String()))
		})
	}
	rcv.peersMu.Unlock()

	// Close persistent streams and disconnect network connections
	rcv.broadcaster.ClosePersistentStream(pid)
	_ = rcv.host.Network().ClosePeer(pid)

	log.Printf("[Byzantine Blacklist] Peer %s blacklisted until %v. Reason: %s", pid, expiry.Format(time.RFC3339), reason)
}

// IsPeerBlacklisted checks whether a peer is currently in the active blacklist.
func (rcv *Receiver) IsPeerBlacklisted(pid peer.ID) bool {
	rcv.blacklistMu.RLock()
	defer rcv.blacklistMu.RUnlock()
	if rcv.blacklistedPeers == nil {
		return false
	}
	expiry, exists := rcv.blacklistedPeers[pid]
	if !exists {
		return false
	}
	if time.Now().After(expiry) {
		return false
	}
	return true
}

// GetBlacklist returns a snapshot of currently blacklisted peers and their expiry.
func (rcv *Receiver) GetBlacklist() map[string]string {
	rcv.blacklistMu.RLock()
	defer rcv.blacklistMu.RUnlock()
	res := make(map[string]string, len(rcv.blacklistedPeers))
	now := time.Now()
	for pid, exp := range rcv.blacklistedPeers {
		if exp.After(now) {
			res[pid.String()] = exp.Format(time.RFC3339)
		}
	}
	return res
}

// VerifyAndHandleByzantineReport verifies a ByzantineNodeReport in O(1) time and takes disciplinary action.
func (rcv *Receiver) VerifyAndHandleByzantineReport(report p2pcommon.ByzantineNodeReport) (bool, error) {
	reportedPID, err := peer.Decode(report.ReportedPeerID)
	if err != nil {
		return false, fmt.Errorf("invalid reported peer ID %q: %w", report.ReportedPeerID, err)
	}
	reporterPID, err := peer.Decode(report.ReporterPeerID)
	if err != nil {
		return false, fmt.Errorf("invalid reporter peer ID %q: %w", report.ReporterPeerID, err)
	}

	dataBytes, err := hex.DecodeString(report.Data)
	if err != nil {
		return false, fmt.Errorf("invalid hex in report data: %w", err)
	}
	coeffsBytes, err := hex.DecodeString(report.Coeffs)
	if err != nil {
		return false, fmt.Errorf("invalid hex in report coeffs: %w", err)
	}
	proofBytes, err := hex.DecodeString(report.Proof)
	if err != nil {
		return false, fmt.Errorf("invalid hex in report proof: %w", err)
	}

	pieceCommitsBytes := make([]cda.PieceCommitment, len(report.PieceCommits))
	for i, cHex := range report.PieceCommits {
		b, err := hex.DecodeString(cHex)
		if err != nil {
			return false, fmt.Errorf("invalid hex in piece commit %d: %w", i, err)
		}
		pieceCommitsBytes[i] = cda.PieceCommitment(b)
	}

	if len(pieceCommitsBytes) == 0 {
		return false, fmt.Errorf("empty piece commitments in byzantine report")
	}

	// 1. Combine commitments: C_piece = sum(g_j * C_j)
	combinedCommit, err := rcv.kzg.Combine(pieceCommitsBytes, coeffsBytes)
	if err != nil {
		// If combining fails (e.g. invalid coeffs length), the piece served by reportedPID is genuinely invalid!
		log.Printf("[Byzantine] Combine piece commitments failed for reported peer %s: %v. Confirming byzantine fault.", reportedPID, err)
		if rcv.repEngine != nil {
			rcv.repEngine.RecordEvent(reportedPID.String(), p2pcommon.RepEventByzantineFault, report.BlockID, 1)
		}
		rcv.BlacklistPeer(reportedPID, 24*time.Hour, fmt.Sprintf("invalid coefficients/commitments format: %v", err))
		return true, nil
	}

	// 2. O(1) Pairing verification: does the piece verify against the combined commitment?
	n := 2 * rcv.kBlock
	if rcv.cache != nil {
		if cachedCol, ok := rcv.cache.GetCol(report.BlockID, report.Col); ok && len(cachedCol.ColumnData) > 0 {
			n = len(cachedCol.ColumnData)
		} else if cachedPrimary, ok := rcv.cache.Get(report.BlockID); ok && len(cachedPrimary.ColumnData) > 0 {
			n = len(cachedPrimary.ColumnData)
		}
	}
	if n <= 0 {
		n = 32
	}
	isMathematicallyValid := rcv.kzg.VerifyForDomain(combinedCommit, report.Row, dataBytes, cda.OpeningProof(proofBytes), n)

	if !isMathematicallyValid {
		// Piece is genuinely INVALID! Accusation is TRUE!
		log.Printf("[Byzantine Report VERIFIED] Peer %s distributed corrupted piece for block %s cell [%d, %d]! Isolating peer.",
			reportedPID, report.BlockID, report.Row, report.Col)
		if rcv.repEngine != nil {
			rcv.repEngine.RecordEvent(reportedPID.String(), p2pcommon.RepEventByzantineFault, report.BlockID, 1)
		}
		rcv.BlacklistPeer(reportedPID, 2*time.Minute, fmt.Sprintf("distributed invalid piece for block %s cell [%d, %d] (reason: %s)", report.BlockID, report.Row, report.Col, report.Reason))
		return true, nil
	}

	// Piece is actually VALID! Reporter attempted a FALSE ACCUSATION (Griefing Attack)!
	log.Printf("[Byzantine Report REJECTED - GRIEFING DETECTED] Reporter %s falsely accused peer %s for valid piece on block %s cell [%d, %d]! Isolating reporter.",
		reporterPID, reportedPID, report.BlockID, report.Row, report.Col)
	if rcv.repEngine != nil {
		rcv.repEngine.RecordEvent(reporterPID.String(), p2pcommon.RepEventGriefingFault, report.BlockID, 1)
	}
	rcv.BlacklistPeer(reporterPID, 2*time.Minute, fmt.Sprintf("false accusation/griefing against peer %s on block %s cell [%d, %d]", reportedPID, report.BlockID, report.Row, report.Col))
	return false, nil
}

// OnStoreReady records a Store Node's custody completion signal in the ReputationEngine.
// Even if StoreReady arrives after the 2/3 BlockReady threshold due to single-machine CPU contention,
// the peer is credited (+2.0) and any straggler miss for that block is automatically forgiven.
func (rcv *Receiver) OnStoreReady(payload p2pcommon.GossipStoreReadyPayload) {
	if rcv.repEngine == nil {
		return
	}
	peerID := payload.PeerID
	if peerID == "" {
		rcv.peersMu.RLock()
		for _, info := range rcv.activePeers {
			if info.Row == payload.RowIdx {
				peerID = info.PeerID
				break
			}
		}
		rcv.peersMu.RUnlock()
	}
	if peerID != "" {
		rcv.repEngine.RecordBlockCompleted(peerID, payload.BlockID, payload.RowIdx, payload.ColIdx)
	}
}

// GetReputationRecords returns all tracked Store Node reputation records enriched with current active status.
func (rcv *Receiver) GetReputationRecords() []p2pcommon.PeerReputationRecord {
	if rcv.repEngine == nil {
		return nil
	}
	rcv.peersMu.RLock()
	for _, info := range rcv.activePeers {
		rcv.repEngine.EnsurePeer(info)
	}
	rcv.peersMu.RUnlock()
	return rcv.repEngine.GetAllRecords()
}

// ApplyReputationEvent applies a reputation event (or simulated batch of events) to a target peer (matched by PeerID prefix or row).
func (rcv *Receiver) ApplyReputationEvent(peerQuery string, targetRow int, eventType string, blockID string, count int) (p2pcommon.PeerReputationRecord, error) {
	if rcv.repEngine == nil {
		return p2pcommon.PeerReputationRecord{}, fmt.Errorf("reputation engine not initialized")
	}
	resolvedPeerID := ""
	rcv.peersMu.RLock()
	for _, info := range rcv.activePeers {
		if peerQuery != "" && (info.PeerID == peerQuery || strings.Contains(info.PeerID, peerQuery)) {
			resolvedPeerID = info.PeerID
			break
		}
		if peerQuery == "" && targetRow >= 0 && info.Row == targetRow {
			resolvedPeerID = info.PeerID
			break
		}
	}
	rcv.peersMu.RUnlock()

	if resolvedPeerID == "" {
		if peerQuery != "" {
			resolvedPeerID = peerQuery
		} else {
			return p2pcommon.PeerReputationRecord{}, fmt.Errorf("no matching peer found for query=%q row=%d", peerQuery, targetRow)
		}
	}

	rec := rcv.repEngine.RecordEvent(resolvedPeerID, eventType, blockID, count)
	if rec.TrustTier == p2pcommon.TrustTierQuarantined {
		if pid, err := peer.Decode(resolvedPeerID); err == nil {
			rcv.BlacklistPeer(pid, 2*time.Minute, fmt.Sprintf("reputation quarantined via event %s", eventType))
		}
	} else if eventType == p2pcommon.RepEventManualReset {
		if pid, err := peer.Decode(resolvedPeerID); err == nil {
			rcv.blacklistMu.Lock()
			delete(rcv.blacklistedPeers, pid)
			rcv.blacklistMu.Unlock()
		}
	}
	return rec, nil
}


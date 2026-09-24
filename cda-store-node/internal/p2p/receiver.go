package p2p

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cda-store-node/internal/engine"
	"cda-store-node/internal/storage"
	"cda-store-node/internal/verifier"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	rlnc "github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

// mathRandIntn wraps rand.Intn for use in shuffle (avoids import collision)
func mathRandIntn(n int) int { return rand.Intn(n) }

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
	ps            *pubsub.PubSub
	kzg           cda.KZGProvider
	rm            *cda.RecipientManager
	publisherAddr string
	selfPeerID    string // This node's own PeerID string
	kBlock        int
	kPiece        int
	numCols       int
	storesPerCol  int
	rowIdx        int
	colIdx        int
	cache         *storage.CustodyStore
	broadcaster   *Broadcaster
	crashOnFail   bool

	// Peer lists for Row and Column subnets
	peersMu   sync.RWMutex
	rowPeers  []p2pcommon.PeerInfo
	colPeers  []p2pcommon.PeerInfo

	// Per-cell peer contribution tracking: cellKey -> senderPeerID -> count
	// Used to enforce "pull from ≥2 different custody nodes" before recoding
	contribMu         sync.Mutex
	peerContributions map[string]map[string]int

	// Rate limiting
	rateLimitersMu sync.Mutex
	rateLimiters   map[peer.ID]*tokenBucket

	// In-memory stored piece count to avoid heavy DB iteration
	totalStoredPieces   int64
	totalStoredPiecesMu sync.Mutex
	cellMu              sync.Mutex

	// Pruning configurations and state
	pruneEnable      bool
	pruneTTL         time.Duration
	pruneMu          sync.Mutex
	cellLastActivity map[string]time.Time
	prunedCells      map[string]bool

	// Block completion file logging
	completedMu     sync.Mutex
	completedBlocks map[string]bool

	// Broadcast dissemination tracking for StoreReady condition
	broadcastMu        sync.Mutex
	cellBroadcastCount map[string]int

	// Non-custody cell lock: once recoded with >=2 pieces from >=2 sources, ignore further pieces for this cell
	nonCustodyLockedMu sync.Mutex
	nonCustodyLocked   map[string]bool

	// Track globally subscribed non-custody topics to prevent duplicate subscriptions across SetPeers calls
	// Track globally subscribed non-custody topics to prevent duplicate subscriptions across SetPeers calls
	subscribedTopicsMu sync.Mutex
	subscribedTopics   map[string]bool

	// Sharded worker queues by cell hash to ensure cell-level ordering and eliminate race conditions
	workerChans []chan func()
	numWorkers  int

	// Gossip batch processing
	gossipQueue     chan p2pcommon.SeedCellRequest
	gossipBatchStop chan struct{}

	// Cooldown / Grace period for active pulls
	pullMu       sync.Mutex
	pendingPulls map[string]bool

	// Active fallback monitor tracking per block
	fallbackMu      sync.Mutex
	activeFallbacks map[string]bool

	// Debounced block completion checker
	completionCheckChan chan string

	// Bounded concurrency semaphore for custody cell dissemination
	disseminationSem chan struct{}

	// Context for subscribeToNonCustodyCells goroutines lifetime
	ctx context.Context
}

func hashCellKey(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func (rcv *Receiver) dispatchTask(cellKey string, task func()) {
	if rcv.numWorkers <= 0 || len(rcv.workerChans) == 0 {
		task()
		return
	}
	idx := int(hashCellKey(cellKey) % uint32(rcv.numWorkers))
	select {
	case rcv.workerChans[idx] <- task:
	default:
		log.Printf("[StoreNode] workerChan %d full, dropping task for %s", idx, cellKey)
	}
}

func NewReceiver(
	h host.Host,
	ps *pubsub.PubSub,
	kzg cda.KZGProvider,
	pubAddr string,
	kBlock int,
	kPiece int,
	numCols int,
	storesPerCol int,
	rowIdx int,
	colIdx int,
	cache *storage.CustodyStore,
	broadcaster *Broadcaster,
	crashOnFail bool,
	pruneEnable bool,
	pruneTTL time.Duration,
	selfPeerID string,
) *Receiver {
	nWorkers := runtime.NumCPU() * 2
	if nWorkers < 4 {
		nWorkers = 4
	}
	if envWorkers := getEnvInt("STORE_SHARDED_WORKERS", 0); envWorkers > 0 {
		nWorkers = envWorkers
	}
	chans := make([]chan func(), nWorkers)
	for i := 0; i < nWorkers; i++ {
		chans[i] = make(chan func(), 2000)
	}

	return &Receiver{
		host:              h,
		ps:                ps,
		kzg:               kzg,
		rm:                cda.NewRecipientManager(kPiece, kzg),
		publisherAddr:     pubAddr,
		selfPeerID:        selfPeerID,
		kBlock:            kBlock,
		kPiece:            kPiece,
		numCols:           numCols,
		storesPerCol:      storesPerCol,
		rowIdx:            rowIdx,
		colIdx:            colIdx,
		cache:             cache,
		broadcaster:       broadcaster,
		crashOnFail:       crashOnFail,
		rateLimiters:      make(map[peer.ID]*tokenBucket),
		totalStoredPieces: int64(cache.GetTotalPieceCount()),
		pruneEnable:       pruneEnable,
		pruneTTL:          pruneTTL,
		cellLastActivity:  make(map[string]time.Time),
		prunedCells:       make(map[string]bool),
		completedBlocks:    make(map[string]bool),
		cellBroadcastCount: make(map[string]int),
		nonCustodyLocked:   make(map[string]bool),
		subscribedTopics:   make(map[string]bool),
		peerContributions: make(map[string]map[string]int),
		workerChans:       chans,
		numWorkers:        nWorkers,
		pendingPulls:        make(map[string]bool),
		activeFallbacks:     make(map[string]bool),
		gossipQueue:         make(chan p2pcommon.SeedCellRequest, 4096),
		gossipBatchStop:     make(chan struct{}),
		completionCheckChan: make(chan string, 1024),
		disseminationSem:    make(chan struct{}, getEnvInt("STORE_DISSEMINATION_SEM", 16)),
	}
}

func (rcv *Receiver) SetPeers(rowPeers, colPeers []p2pcommon.PeerInfo) {
	rcv.peersMu.Lock()
	rcv.rowPeers = rowPeers
	rcv.colPeers = colPeers
	rcv.peersMu.Unlock()

	// Update broadcaster with column peers
	var pIDs []peer.ID
	for _, p := range colPeers {
		if pid, err := peer.Decode(p.PeerID); err == nil {
			pIDs = append(pIDs, pid)
		}
	}
	rcv.broadcaster.UpdatePeers(pIDs)

	// Subscribe to non-custody cells via custody node per-node topics (if context is ready)
	if rcv.ctx != nil {
		go rcv.subscribeToNonCustodyCells(rcv.ctx, colPeers)
	}
}

func (rcv *Receiver) Start(ctx context.Context) {
	// Store context for use in SetPeers (which may be called after Start)
	rcv.ctx = ctx

	// Start sharded worker pool (1 goroutine per workerChan)
	for i := 0; i < rcv.numWorkers; i++ {
		go rcv.workerLoop(ctx, rcv.workerChans[i])
	}
	// Start debounced completion check worker loop
	go rcv.completionCheckWorkerLoop(ctx)

	// Start batch workers for GossipSub messages (configurable via STORE_GOSSIP_BATCH_WORKERS, default 2)
	rcv.startGossipBatchWorkers(ctx, getEnvInt("STORE_GOSSIP_BATCH_WORKERS", 2))

	StartMetricsTicker(ctx)
	LinearIndependentPiecesCount.Set(float64(rcv.totalStoredPieces))

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				actualPieces := rcv.cache.GetTotalPieceCount()
				custodyPieces := rcv.cache.GetCustodyPieceCount(rcv.storesPerCol, rcv.rowIdx)
				recodedPieces := rcv.cache.GetRecodedPieceCount()
				rcv.totalStoredPiecesMu.Lock()
				rcv.totalStoredPieces = int64(actualPieces)
				rcv.totalStoredPiecesMu.Unlock()
				LinearIndependentPiecesCount.Set(float64(actualPieces))
				CustodyPiecesCount.Set(float64(custodyPieces))
				RecodedPiecesCount.Set(float64(recodedPieces))

				dbSize := rcv.cache.GetDBSize()
				DatabaseSizeBytes.Set(float64(dbSize))
			case <-ctx.Done():
				return
			}
		}
	}()

	if rcv.pruneEnable {
		go rcv.startPruner(ctx)
	}

	// 1. Register per-node stream handler (replaces shared ProtoBootstrapSeed).
	//    Each store node listens on its own dedicated protocol so bootstrap can
	//    dial the right node without contention on a shared protocol name.
	nodeProto := p2pcommon.ProtoNodeSeed(rcv.selfPeerID)
	rcv.host.SetStreamHandler(protocol.ID(nodeProto), rcv.handleSeedStream)
	log.Printf("[StoreNode] Registered per-node seed handler on protocol %s", nodeProto)

	nodeBatchProto := p2pcommon.ProtoNodeBatchSeed(rcv.selfPeerID)
	rcv.host.SetStreamHandler(protocol.ID(nodeBatchProto), rcv.handleBatchSeedStream)
	log.Printf("[StoreNode] Registered per-node batch seed handler on protocol %s", nodeBatchProto)

	// Keep generic fetch handlers (used by light nodes and active-pull)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreFetch, rcv.handleFetchStream)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreGetPieces, rcv.handleFetchStream)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreBatchFetch, rcv.handleBatchFetchStream)

	// 2. Subscribe to this node's own dedicated GossipSub topic.
	//    Bootstrap / other custody nodes will publish to this topic when they want to
	//    send anchor payloads or recoded pieces intended for our custody cells.
	self := p2pcommon.TopicNode(rcv.selfPeerID)
	selfTopic, err := rcv.broadcaster.JoinTopic(self)
	if err != nil {
		log.Fatalf("[StoreNode] Failed to join own node topic %s: %v", self, err)
	}
	selfSub, err := selfTopic.Subscribe()
	if err != nil {
		log.Fatalf("[StoreNode] Failed to subscribe to own node topic %s: %v", self, err)
	}
	log.Printf("[StoreNode] Subscribed to own node topic %s", self)
	go func(sub *pubsub.Subscription) {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom == rcv.host.ID() {
				continue // skip self
			}
			data := msg.Data
			var payload p2pcommon.SeedCellRequest
			if err := json.Unmarshal(data, &payload); err == nil && payload.BlockID != "" {
				rcv.enqueueGossipPiece(payload)
			}
		}
	}(selfSub)

	// 3. Subscribe to column anchor topics (kept as-is — per-column, not per-node)
	n := 2 * rcv.kBlock
	numCols := rcv.numCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}
	netColIdx := rcv.colIdx / colsPerNetCol
	startCol := netColIdx * colsPerNetCol
	endCol := startCol + colsPerNetCol

	for c := startCol; c < endCol; c++ {
		colTopicName := p2pcommon.TopicCol(c)
		topic, err := rcv.broadcaster.JoinTopic(colTopicName)
		if err != nil {
			log.Fatalf("Failed to join column anchor GossipSub topic %s: %v", colTopicName, err)
		}
		sub, err := topic.Subscribe()
		if err != nil {
			log.Fatalf("Failed to subscribe to column anchor GossipSub topic %s: %v", colTopicName, err)
		}
		go func(sub *pubsub.Subscription) {
			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					return
				}
				if msg.ReceivedFrom == rcv.host.ID() {
					continue
				}
				data := msg.Data
				var raw map[string]interface{}
				if err := json.Unmarshal(data, &raw); err == nil {
					bID, _ := raw["block_id"].(string)
					cIdx := c
					colKey := fmt.Sprintf("%s_col_%d", bID, cIdx)
					rcv.dispatchTask(colKey, func() { rcv.processAnchorMessage(data) })
				}
			}
		}(sub)
	}
}

// workerLoop drains workerChan sequentially — one task at a time for sharded cells.
func (rcv *Receiver) workerLoop(ctx context.Context, ch chan func()) {
	for {
		select {
		case task, ok := <-ch:
			if !ok {
				return
			}
			task()
		case <-ctx.Done():
			return
		}
	}
}

// enqueueGossipPiece queues an incoming GossipSub seed cell for batch verification.
func (rcv *Receiver) enqueueGossipPiece(payload p2pcommon.SeedCellRequest) {
	recordGossipMessage()
	select {
	case rcv.gossipQueue <- payload:
	default:
		// Fallback: process directly via worker if queue is full
		cellKey := fmt.Sprintf("%s_%d_%d", payload.BlockID, payload.Row, payload.Col)
		rcv.dispatchTask(cellKey, func() {
			rcv.processPiece(payload.BlockID, payload.Row, payload.Col, payload.Data, payload.Coeffs, payload.Proof, payload.PieceCommits, payload.SenderPeerID, true)
		})
	}
}

// processGossipMessage handles incoming messages on this node's own TopicNode topic.
// Messages here are recoded RLNC pieces published by other custody nodes.
func (rcv *Receiver) processGossipMessage(data []byte) {
	var payload p2pcommon.SeedCellRequest
	if err := json.Unmarshal(data, &payload); err != nil {
		return
	}
	rcv.enqueueGossipPiece(payload)
}

func (rcv *Receiver) startGossipBatchWorkers(ctx context.Context, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go rcv.gossipBatchWorkerLoop(ctx)
	}
}

func (rcv *Receiver) gossipBatchWorkerLoop(ctx context.Context) {
	batchSize := getEnvInt("STORE_GOSSIP_BATCH_SIZE", 48)
	batch := make([]p2pcommon.SeedCellRequest, 0, batchSize)
	tickerMs := getEnvInt("STORE_GOSSIP_BATCH_TICKER_MS", 10)
	ticker := time.NewTicker(time.Duration(tickerMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rcv.gossipBatchStop:
			return
		case item, ok := <-rcv.gossipQueue:
			if !ok {
				return
			}
			batch = append(batch, item)
			if len(batch) >= batchSize {
				rcv.verifyAndProcessBatch(batch)
				batch = make([]p2pcommon.SeedCellRequest, 0, batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				rcv.verifyAndProcessBatch(batch)
				batch = make([]p2pcommon.SeedCellRequest, 0, batchSize)
			}
		}
	}
}

// processAnchorMessage handles incoming messages on the column's TopicCol topic.
// Messages here are GossipAnchorPayload from bootstrap node.
func (rcv *Receiver) processAnchorMessage(data []byte) {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return
	}
	if _, isAnchor := raw["merkle_proofs"]; isAnchor {
		var payload p2pcommon.GossipAnchorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return
		}
		rcv.processAnchor(payload.BlockID, payload.ColIdx, payload.PieceCommits, payload.MerkleProofs)
	}
}

func (rcv *Receiver) processAnchor(blockID string, colIdx int, commitsStr []string, proofsStr []p2pcommon.SerializedMerkleProof) {
	height := p2pcommon.ParseHeightFromBlockID(blockID)
	log.Printf("[Height: %d] [StoreNode] Received Anchor Gossip for Column %d, Block %s", height, colIdx, blockID)

	// 1. Fetch Block Header from Publisher
	header, err := verifier.FetchBlockHeader(rcv.publisherAddr, blockID)
	if err != nil {
		log.Printf("[StoreNode] Failed to fetch block header from publisher: %v", err)
		return
	}

	// 2. Decode piece commitments
	pieceCommits := make([][]byte, len(commitsStr))
	for i, h := range commitsStr {
		b, err := hex.DecodeString(h)
		if err != nil {
			return
		}
		pieceCommits[i] = b
	}

	merkleProofs := make([]cda.MerkleProof, len(proofsStr))
	for i, p := range proofsStr {
		merkleProofs[i] = cda.MerkleProof{
			Index:    p.Index,
			Siblings: p.Siblings,
		}
	}

	// 3. Verify Anchor
	ok, err := verifier.VerifyAnchor(rcv.kzg, header, colIdx, pieceCommits, merkleProofs, rcv.kPiece)
	if err != nil {
		if rcv.crashOnFail {
			log.Fatalf("Anchor verification failed (CRASH): %v", err)
		}
		log.Printf("Anchor verification failed: %v", err)
		return
	}
	if !ok {
		if rcv.crashOnFail {
			log.Fatalf("Anchor verification failed (CRASH): invalid Merkle path or Fiat-Shamir combination")
		}
		log.Printf("Anchor verification failed: invalid Merkle path or Fiat-Shamir combination")
		return
	}

	// 4. Save anchored commitments
	rcv.cache.AnchorCommitments(blockID, colIdx, pieceCommits)
	log.Printf("[Height: %d] [StoreNode] Successfully anchored commitments for Block %s, Column %d", height, blockID, colIdx)

	// 5. Schedule periodic fallback pull in case GossipSub drops pieces (deduplicated per block)
	rcv.fallbackMu.Lock()
	if !rcv.activeFallbacks[blockID] {
		rcv.activeFallbacks[blockID] = true
		rcv.fallbackMu.Unlock()
		go func(bID string) {
			defer func() {
				rcv.fallbackMu.Lock()
				delete(rcv.activeFallbacks, bID)
				rcv.fallbackMu.Unlock()
			}()

			// Grace period to allow GossipSub dissemination and Batch Verify to complete naturally
			graceSec := getEnvInt("STORE_FALLBACK_GRACE_SEC", 3)
			time.Sleep(time.Duration(graceSec) * time.Second)
			if rcv.IsComplete(bID) {
				return
			}
			for attempt := 1; attempt <= 4; attempt++ {
				if rcv.IsComplete(bID) {
					return
				}
				rcv.fallbackPullMissingCells(bID, attempt)
				time.Sleep(1500 * time.Millisecond)
			}
		}(blockID)
	} else {
		rcv.fallbackMu.Unlock()
	}
}

func (rcv *Receiver) handleSeedStream(stream network.Stream) {
	defer stream.Close()

	var payload p2pcommon.SeedCellRequest
	if err := json.NewDecoder(stream).Decode(&payload); err != nil {
		rcv.respondWithError(stream, fmt.Sprintf("failed to decode seed: %v", err))
		return
	}

	err := rcv.processPiece(payload.BlockID, payload.Row, payload.Col, payload.Data, payload.Coeffs, payload.Proof, payload.PieceCommits, payload.SenderPeerID, false)
	if err != nil {
		rcv.respondWithError(stream, err.Error())
		return
	}

	json.NewEncoder(stream).Encode(struct {
		Success bool `json:"success"`
	}{Success: true})
}

func (rcv *Receiver) handleBatchSeedStream(stream network.Stream) {
	defer stream.Close()

	var payload p2pcommon.BatchSeedCellRequest
	if err := json.NewDecoder(stream).Decode(&payload); err != nil {
		rcv.respondWithError(stream, fmt.Sprintf("failed to decode batch seed: %v", err))
		return
	}

	if len(payload.Seeds) > 0 {
		rcv.verifyAndProcessBatch(payload.Seeds)
	}

	json.NewEncoder(stream).Encode(struct {
		Success bool `json:"success"`
	}{Success: true})
}

func (rcv *Receiver) verifyAndProcessBatch(seeds []p2pcommon.SeedCellRequest) {
	if len(seeds) == 0 {
		return
	}

	type candidateItem struct {
		blockID       string
		row, col      int
		piece         cda.ReceivedPiece
		decodedCoeffs []byte
		senderPeerID  string
		combinedComm  []byte
	}

	var candidates []candidateItem
	var batchItems []cda.BatchVerifyItem

	for _, s := range seeds {
		cellKey := fmt.Sprintf("%s_%d_%d", s.BlockID, s.Row, s.Col)
		rcv.completedMu.Lock()
		if rcv.completedBlocks != nil && rcv.completedBlocks[s.BlockID] {
			rcv.completedMu.Unlock()
			continue
		}
		rcv.completedMu.Unlock()

		isPrimaryEarly := (s.Row % rcv.storesPerCol) == rcv.rowIdx
		isBackupEarly := ((s.Row + 1) % rcv.storesPerCol) == rcv.rowIdx
		if !isPrimaryEarly && !isBackupEarly {
			rcv.nonCustodyLockedMu.Lock()
			locked := rcv.nonCustodyLocked[cellKey]
			rcv.nonCustodyLockedMu.Unlock()
			if locked {
				continue
			}
		}

		rcv.pruneMu.Lock()
		pruned := rcv.prunedCells[cellKey]
		rcv.pruneMu.Unlock()
		if pruned {
			continue
		}

		decodedData, err1 := hex.DecodeString(s.Data)
		decodedCoeffs, err2 := hex.DecodeString(s.Coeffs)
		decodedProof, err3 := hex.DecodeString(s.Proof)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}

		piece := cda.ReceivedPiece{
			Row: s.Row,
			Col: s.Col,
			Data: rlnc.PieceData{
				Data:   decodedData,
				Coeffs: decodedCoeffs,
			},
			Proof: cda.OpeningProof(decodedProof),
		}

		pieceCommits, exists := rcv.cache.GetAnchoredCommitments(s.BlockID, s.Col)
		if !exists {
			if len(s.PieceCommits) > 0 {
				pieceCommits = make([][]byte, len(s.PieceCommits))
				for i, h := range s.PieceCommits {
					b, _ := hex.DecodeString(h)
					pieceCommits[i] = b
				}
				rcv.cache.AnchorCommitments(s.BlockID, s.Col, pieceCommits)
			}
		}

		if len(pieceCommits) < rcv.kPiece {
			continue
		}

		pieceCommitsTyped := make([]cda.PieceCommitment, rcv.kPiece)
		for i := 0; i < rcv.kPiece; i++ {
			pieceCommitsTyped[i] = cda.PieceCommitment(pieceCommits[i])
		}
		combinedCommit, err := rcv.kzg.Combine(pieceCommitsTyped, decodedCoeffs)
		if err != nil {
			continue
		}

		batchItems = append(batchItems, cda.BatchVerifyItem{
			Commitment: combinedCommit,
			Row:        s.Row,
			Data:       decodedData,
			Proof:      cda.OpeningProof(decodedProof),
		})

		candidates = append(candidates, candidateItem{
			blockID:       s.BlockID,
			row:           s.Row,
			col:           s.Col,
			piece:         piece,
			decodedCoeffs: decodedCoeffs,
			senderPeerID:  s.SenderPeerID,
			combinedComm:  combinedCommit,
		})
	}

	// Batch KZG Verification using RLC (only 2 pairings for the whole batch!)
	if len(batchItems) > 0 {
		if rcv.kzg.BatchVerify(batchItems) {
			for _, item := range candidates {
				_ = rcv.processVerifiedPiece(item.blockID, item.row, item.col, item.piece, item.decodedCoeffs, item.senderPeerID)
			}
		} else {
			// Fallback: verify individually to isolate any corrupted piece
			for _, item := range candidates {
				if rcv.kzg.Verify(item.combinedComm, item.row, item.piece.Data.Data, item.piece.Proof) {
					_ = rcv.processVerifiedPiece(item.blockID, item.row, item.col, item.piece, item.decodedCoeffs, item.senderPeerID)
				}
			}
		}
	}
}

func (rcv *Receiver) processPiece(blockID string, row, col int, dataStr, coeffsStr, proofStr string, pieceCommitsStr []string, senderPeerID string, isGossip bool) error {
	cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)

	// Early exit 1: drop incoming pieces if this block is already completed and locked
	rcv.completedMu.Lock()
	if rcv.completedBlocks != nil && rcv.completedBlocks[blockID] {
		rcv.completedMu.Unlock()
		return nil
	}
	rcv.completedMu.Unlock()

	// Early exit 2: non-custody cells that are already locked (recoded and done)
	isPrimaryEarly := (row % rcv.storesPerCol) == rcv.rowIdx
	isBackupEarly := ((row + 1) % rcv.storesPerCol) == rcv.rowIdx
	if !isPrimaryEarly && !isBackupEarly {
		rcv.nonCustodyLockedMu.Lock()
		locked := rcv.nonCustodyLocked[cellKey]
		rcv.nonCustodyLockedMu.Unlock()
		if locked {
			log.Printf("[StoreNode] Non-custody cell [%d, %d] already locked. Dropping piece.", row, col)
			return nil
		}
	}

	// Early exit 3: cells that are already pruned
	rcv.pruneMu.Lock()
	pruned := rcv.prunedCells[cellKey]
	rcv.pruneMu.Unlock()
	if pruned {
		return nil
	}

	// 1. Decode payload fields from hex
	decodedData, err := hex.DecodeString(dataStr)
	if err != nil {
		return fmt.Errorf("invalid hex in data")
	}

	decodedCoeffs, err := hex.DecodeString(coeffsStr)
	if err != nil {
		return fmt.Errorf("invalid hex in coeffs")
	}

	decodedProof, err := hex.DecodeString(proofStr)
	if err != nil {
		return fmt.Errorf("invalid hex in proof")
	}

	piece := cda.ReceivedPiece{
		Row: row,
		Col: col,
		Data: rlnc.PieceData{
			Data:   decodedData,
			Coeffs: decodedCoeffs,
		},
		Proof: cda.OpeningProof(decodedProof),
	}

	// 2. Retrieve anchored commitments
	pieceCommits, exists := rcv.cache.GetAnchoredCommitments(blockID, col)
	if !exists {
		// Fallback: fetch header and anchor commitments
		height := p2pcommon.ParseHeightFromBlockID(blockID)
		log.Printf("[Height: %d] [StoreNode] Anchored commitments not found in cache for block %s. Fetching from publisher...", height, blockID)
		_, err := verifier.FetchBlockHeader(rcv.publisherAddr, blockID)
		if err != nil {
			return fmt.Errorf("failed to fetch header: %w", err)
		}

		if len(pieceCommitsStr) == 0 {
			return fmt.Errorf("commitments not found in request payload for verification")
		}

		pieceCommits = make([][]byte, len(pieceCommitsStr))
		for i, h := range pieceCommitsStr {
			b, _ := hex.DecodeString(h)
			pieceCommits[i] = b
		}

		rcv.cache.AnchorCommitments(blockID, col, pieceCommits)
	}

	// 3. Layer 3 (KZG Pairing Check): Verify the piece matches the combined column commitment
	rowIdx := row
	combinedProof := cda.OpeningProof(decodedProof)

	pieceCommitsTyped := make([]cda.PieceCommitment, rcv.kPiece)
	for i := 0; i < rcv.kPiece; i++ {
		pieceCommitsTyped[i] = cda.PieceCommitment(pieceCommits[i])
	}
	combinedCommit, err := rcv.kzg.Combine(pieceCommitsTyped, decodedCoeffs)
	if err != nil {
		return fmt.Errorf("failed to compute combined commitment: %w", err)
	}

	if !rcv.kzg.Verify(combinedCommit, rowIdx, piece.Data.Data, combinedProof) {
		ByzantineDetectionsTotal.Inc()
		if rcv.crashOnFail {
			log.Fatalf("Layer 3 verification failed (CRASH): Piece [%d, %d] does not match combined column commitment", rowIdx, col)
		}
		return fmt.Errorf("layer 3 verification failed")
	}

	return rcv.processVerifiedPiece(blockID, row, col, piece, decodedCoeffs, senderPeerID)
}

func (rcv *Receiver) processVerifiedPiece(blockID string, row, col int, piece cda.ReceivedPiece, decodedCoeffs []byte, senderPeerID string) error {

	rcv.cellMu.Lock()

	isPrimary := (row % rcv.storesPerCol) == rcv.rowIdx
	isBackup := ((row + 1) % rcv.storesPerCol) == rcv.rowIdx
	isCustody := isPrimary || isBackup

	// Non-custody nodes: gather pieces from multiple sources, recode into 1 piece, and lock.
	if !isCustody {
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
		rcv.nonCustodyLockedMu.Lock()
		alreadyLocked := rcv.nonCustodyLocked[cellKey]
		rcv.nonCustodyLockedMu.Unlock()

		if alreadyLocked {
			rcv.cellMu.Unlock()
			return nil
		}

		existingPieces := rcv.cache.GetPieces(blockID, row, col)
		existingCoeffs := make([][]byte, len(existingPieces))
		for i, p := range existingPieces {
			existingCoeffs[i] = p.Data.Coeffs
		}

		if !engine.IsLinearlyIndependent(existingCoeffs, decodedCoeffs, rcv.kPiece) {
			DependentPiecesDroppedTotal.Inc()
			rcv.cellMu.Unlock()
			return nil
		}

		rcv.cache.StorePiece(blockID, row, col, piece)

		if senderPeerID != "" {
			rcv.contribMu.Lock()
			if rcv.peerContributions[cellKey] == nil {
				rcv.peerContributions[cellKey] = make(map[string]int)
			}
			rcv.peerContributions[cellKey][senderPeerID]++
			rcv.contribMu.Unlock()
		}

		minPieces := 2
		if rcv.kPiece < minPieces {
			minPieces = rcv.kPiece
		}
		minSources := 2
		if rcv.kPiece < minSources {
			minSources = rcv.kPiece
		}

		rcv.contribMu.Lock()
		numSources := len(rcv.peerContributions[cellKey])
		rcv.contribMu.Unlock()

		updatedPieces := rcv.cache.GetPieces(blockID, row, col)
		if len(updatedPieces) >= minPieces && numSources >= minSources {
			pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
			var finalPiece *cda.ReceivedPiece
			recoded, err := rcv.rm.RecodePiecesWithVerify(updatedPieces[:minPieces], pieceCommits, 5)
			if err == nil && recoded != nil {
				finalPiece = recoded
			} else {
				finalPiece = &updatedPieces[0]
			}

			rcv.cache.StoreRecodedPiece(blockID, row, col, *finalPiece)
			rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
			rcv.cellMu.Unlock()

			rcv.nonCustodyLockedMu.Lock()
			rcv.nonCustodyLocked[cellKey] = true
			rcv.nonCustodyLockedMu.Unlock()

			rcv.pruneMu.Lock()
			rcv.prunedCells[cellKey] = true
			rcv.pruneMu.Unlock()

			log.Printf("[StoreNode] Non-custody: cell [%d, %d] successfully recoded from %d pieces across %d sources; raw pieces pruned, cell locked.", row, col, len(updatedPieces), numSources)
			rcv.CheckAndLogCompletion(blockID)
			return nil
		}

		rcv.cellMu.Unlock()
		return nil
	}

	// 4. Rank Filtering (Gaussian Elimination) for Custody cells
	existingPieces := rcv.cache.GetPieces(blockID, row, col)
	existingCoeffs := make([][]byte, len(existingPieces))
	for i, p := range existingPieces {
		existingCoeffs[i] = p.Data.Coeffs
	}

	if !engine.IsLinearlyIndependent(existingCoeffs, decodedCoeffs, rcv.kPiece) {
		DependentPiecesDroppedTotal.Inc()
		p2pcommon.LogDebug("[P2P] Received piece for cell [%d, %d]. Linear independence check: dependent (redundant). Dropping piece.", row, col)
		rcv.cellMu.Unlock()
		return nil
	}

	// 5. Store valid piece in custody store
	rcv.cache.StorePiece(blockID, row, col, piece)
	rcv.totalStoredPiecesMu.Lock()
	rcv.totalStoredPieces++
	currCount := rcv.totalStoredPieces
	rcv.totalStoredPiecesMu.Unlock()
	LinearIndependentPiecesCount.Set(float64(currCount))
	log.Printf("[P2P] Received piece for cell [%d, %d]. Linear independence check: independent. Stored piece (local rank increased to %d/%d).", row, col, len(existingPieces)+1, rcv.kPiece)

	if rcv.pruneEnable {
		rcv.pruneMu.Lock()
		key := fmt.Sprintf("%s_%d_%d", blockID, row, col)
		if !rcv.prunedCells[key] {
			rcv.cellLastActivity[key] = time.Now()
		}
		rcv.pruneMu.Unlock()
	}

	// 5b. Track peer contributions for this cell (used by non-custody recode decision)
	if senderPeerID != "" {
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
		rcv.contribMu.Lock()
		if rcv.peerContributions[cellKey] == nil {
			rcv.peerContributions[cellKey] = make(map[string]int)
		}
		rcv.peerContributions[cellKey][senderPeerID]++
		rcv.contribMu.Unlock()
	}

	rcv.CheckAndLogCompletion(blockID)

	// 6. P2P Recoding & GossipSub forwarding
	updatedPieces := rcv.cache.GetPieces(blockID, row, col)
	rcv.cellMu.Unlock()

	if isCustody {
		// Custody nodes (primary + backup): recode only when rank >= kPiece (full rank).
		// Broadcast kPiece/2 independently recoded pieces to give subscribers enough diversity.
		if len(updatedPieces) >= rcv.kPiece {
			go func(bID string, r, c int, pieces []cda.ReceivedPiece, primary bool) {
				rcv.disseminationSem <- struct{}{}
				defer func() { <-rcv.disseminationSem }()

				numBroadcast := rcv.kPiece / 2
				if numBroadcast < 1 {
					numBroadcast = 1
				}
				log.Printf("[GossipSub] Cell [%d, %d] reached full rank %d/%d (Primary=%v). Broadcasting %d recoded pieces...", r, c, len(pieces), rcv.kPiece, primary, numBroadcast)

				targetPieces := pieces
				if len(targetPieces) > rcv.kPiece {
					targetPieces = targetPieces[:rcv.kPiece]
				}
				pieceCommits, _ := rcv.cache.GetAnchoredCommitments(bID, c)
				broadcastSucceeded := 0
				for i := 0; i < numBroadcast; i++ {
					recodedPiece, err := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5)
					if err != nil {
						log.Printf("[GossipSub] Failed to recode pieces for cell [%d, %d] (round %d/%d): %v", r, c, i+1, numBroadcast, err)
						continue
					}
					if !primary {
						rcv.cellMu.Lock()
						rcv.cache.StoreRecodedPiece(bID, r, c, *recodedPiece)
						rcv.cellMu.Unlock()
					}

					if rcv.pruneEnable {
						rcv.pruneMu.Lock()
						key := fmt.Sprintf("%s_%d_%d", bID, r, c)
						if !rcv.prunedCells[key] {
							rcv.cellLastActivity[key] = time.Now()
						}
						rcv.pruneMu.Unlock()
					}
					if err := rcv.broadcaster.BroadcastRecodedPiece(bID, r, c, recodedPiece, pieceCommits); err != nil {
						log.Printf("[GossipSub] Failed to publish recoded piece for cell [%d, %d] (round %d/%d): %v", r, c, i+1, numBroadcast, err)
					} else {
						broadcastSucceeded++
						log.Printf("[GossipSub] Successfully published recoded piece for cell [%d, %d] (Primary=%v, round %d/%d).", r, c, primary, i+1, numBroadcast)
						cellKey := fmt.Sprintf("%s_%d_%d", bID, r, c)
						rcv.broadcastMu.Lock()
						rcv.cellBroadcastCount[cellKey]++
						rcv.broadcastMu.Unlock()
						rcv.CheckAndLogCompletion(bID)
					}
				}

				if !primary {
					// Backup custody node: prune raw pieces after dissemination attempt
					rcv.cellMu.Lock()
					rcv.cache.PruneRawPieces(bID, r, c, rcv.rm)
					rcv.cellMu.Unlock()
					cellKey := fmt.Sprintf("%s_%d_%d", bID, r, c)
					rcv.pruneMu.Lock()
					rcv.prunedCells[cellKey] = true
					rcv.pruneMu.Unlock()
					log.Printf("[StoreNode] Backup: cell [%d, %d] dissemination finished (%d/%d rounds); raw pieces pruned.", r, c, broadcastSucceeded, numBroadcast)
					rcv.CheckAndLogCompletion(bID)
				}
			}(blockID, row, col, updatedPieces, isPrimary)
		}
	}

	// 7. Active Pull: if custody node still lacks pieces, pull from column peers after a grace period
	if isCustody && len(updatedPieces) < rcv.kPiece {
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
		rcv.pullMu.Lock()
		if !rcv.pendingPulls[cellKey] {
			rcv.pendingPulls[cellKey] = true
			rcv.pullMu.Unlock()
			go func(bID string, r, c int, key string) {
				pullDelay := time.Duration(getEnvInt("STORE_ACTIVE_PULL_DELAY_SEC", 1)) * time.Second
				time.Sleep(pullDelay)
				rcv.pullMu.Lock()
				delete(rcv.pendingPulls, key)
				rcv.pullMu.Unlock()

				rcv.cellMu.Lock()
				pCount := len(rcv.cache.GetPieces(bID, r, c))
				rcv.cellMu.Unlock()
				if pCount < rcv.kPiece {
					rcv.pullMissingPiecesFromPeers(bID, r, c)
				}
			}(blockID, row, col, cellKey)
		} else {
			rcv.pullMu.Unlock()
		}
	}

	return nil
}

// subscribeToNonCustodyCells subscribes to the per-node GossipSub topics of custody nodes
// that manage cells this node does NOT have primary/backup custody of.
// It picks >=2 custody nodes per needed cell (shuffled for randomness) and subscribes to
// each custody node's TopicNode. ceil(kPiece/2) pieces from each source yields k_piece total.
//
// NOTE: This is called lazily from SetPeers after bootstrap routing delivers colPeers.
func (rcv *Receiver) subscribeToNonCustodyCells(ctx context.Context, colPeers []p2pcommon.PeerInfo) {
	if len(colPeers) == 0 {
		return
	}

	// Sort colPeers by Row for deterministic primary/backup calculation
	sortedPeers := make([]p2pcommon.PeerInfo, len(colPeers))
	copy(sortedPeers, colPeers)
	sort.Slice(sortedPeers, func(i, j int) bool {
		return sortedPeers[i].Row < sortedPeers[j].Row
	})

	n := 2 * rcv.kBlock
	// Track which topics we've already subscribed to (avoid duplicate subscriptions)
	subscribed := make(map[string]bool)

	for row := 0; row < n; row++ {
		// Skip cells for which this node is primary or backup custody
		isPrimary := (row % rcv.storesPerCol) == rcv.rowIdx
		isBackup := ((row + 1) % rcv.storesPerCol) == rcv.rowIdx
		if isPrimary || isBackup {
			continue
		}

		// Find primary and backup custody peers for this row
		var custodyPeers []p2pcommon.PeerInfo
		for _, p := range sortedPeers {
			if p.Row == row%rcv.storesPerCol || p.Row == (row+1)%rcv.storesPerCol {
				custodyPeers = append(custodyPeers, p)
			}
		}

		// Shuffle custody peers for randomness (different non-custody nodes favour different sources)
		for i := len(custodyPeers) - 1; i > 0; i-- {
			j := mathRandIntn(i + 1)
			custodyPeers[i], custodyPeers[j] = custodyPeers[j], custodyPeers[i]
		}

		// Subscribe to up to 2 custody node topics (skip self)
		subscribed2 := 0
		for _, custodyPeer := range custodyPeers {
			if subscribed2 >= 2 {
				break
			}
			if custodyPeer.PeerID == rcv.selfPeerID {
				continue
			}
			topicName := p2pcommon.TopicNode(custodyPeer.PeerID)
			if subscribed[topicName] {
				subscribed2++
				continue // already subscribed from a previous row's custody overlap
			}

			rcv.subscribedTopicsMu.Lock()
			if rcv.subscribedTopics == nil {
				rcv.subscribedTopics = make(map[string]bool)
			}
			if rcv.subscribedTopics[topicName] {
				rcv.subscribedTopicsMu.Unlock()
				subscribed[topicName] = true
				subscribed2++
				continue // already subscribed globally from a previous SetPeers invocation
			}
			rcv.subscribedTopics[topicName] = true
			rcv.subscribedTopicsMu.Unlock()

			topic, err := rcv.broadcaster.JoinTopic(topicName)
			if err != nil {
				log.Printf("[StoreNode] Failed to join non-custody topic %s: %v", topicName, err)
				continue
			}
			sub, err := topic.Subscribe()
			if err != nil {
				log.Printf("[StoreNode] Failed to subscribe to non-custody topic %s: %v", topicName, err)
				continue
			}
			subscribed[topicName] = true
			subscribed2++
			log.Printf("[StoreNode] Subscribed to custody node topic %s (for non-custody row %d)", topicName, row)

			go func(sub *pubsub.Subscription) {
				for {
					msg, err := sub.Next(ctx)
					if err != nil {
						return
					}
					if msg.ReceivedFrom == rcv.host.ID() {
						continue
					}
					data := msg.Data
					var payload p2pcommon.SeedCellRequest
					if err := json.Unmarshal(data, &payload); err == nil && payload.BlockID != "" {
						rcv.enqueueGossipPiece(payload)
					}
				}
			}(sub)
		}
	}
}

func (rcv *Receiver) fallbackPullMissingCells(blockID string, attempt int) {
	if rcv.IsComplete(blockID) {
		return
	}

	n := 2 * rcv.kBlock
	numCols := rcv.numCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}

	netColIdx := rcv.colIdx / colsPerNetCol
	startCol := netColIdx * colsPerNetCol
	endCol := startCol + colsPerNetCol
	storesPerCol := rcv.storesPerCol
	if storesPerCol <= 0 {
		storesPerCol = 8
	}

	rcv.peersMu.RLock()
	peersCopy := make([]p2pcommon.PeerInfo, len(rcv.colPeers))
	copy(peersCopy, rcv.colPeers)
	rcv.peersMu.RUnlock()

	if len(peersCopy) == 0 {
		return
	}

	peerByRow := make(map[int]p2pcommon.PeerInfo)
	for _, p := range peersCopy {
		peerByRow[p.Row] = p
	}

	peerRequests := make(map[string][]p2pcommon.CellCoord)
	peerInfoMap := make(map[string]p2pcommon.PeerInfo)

	for c := startCol; c < endCol; c++ {
		for r := 0; r < n; r++ {
			isPrimary := (r % storesPerCol) == rcv.rowIdx
			isBackup := ((r + 1) % storesPerCol) == rcv.rowIdx
			cellKey := fmt.Sprintf("%s_%d_%d", blockID, r, c)

			primaryRow := r % storesPerCol
			backupRow := (r + 1) % storesPerCol

			if isPrimary {
				rcv.broadcastMu.Lock()
				bCount := rcv.cellBroadcastCount[cellKey]
				rcv.broadcastMu.Unlock()

				pCount := rcv.cache.GetPieceCount(blockID, r, c)
				if pCount < rcv.kPiece || bCount < 1 {
					if bp, ok := peerByRow[backupRow]; ok && bp.PeerID != rcv.selfPeerID {
						peerRequests[bp.PeerID] = append(peerRequests[bp.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
						peerInfoMap[bp.PeerID] = bp
					}
					if attempt >= 2 {
						for _, p := range peersCopy {
							if p.PeerID != rcv.selfPeerID && p.Row != backupRow {
								peerRequests[p.PeerID] = append(peerRequests[p.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
								peerInfoMap[p.PeerID] = p
							}
						}
					}
				}
			} else if isBackup {
				rcv.broadcastMu.Lock()
				bCount := rcv.cellBroadcastCount[cellKey]
				rcv.broadcastMu.Unlock()

				rcv.pruneMu.Lock()
				pruned := rcv.prunedCells[cellKey]
				rcv.pruneMu.Unlock()

				pCount := rcv.cache.GetPieceCount(blockID, r, c)
				if bCount < 1 && !pruned && pCount < rcv.kPiece {
					if pp, ok := peerByRow[primaryRow]; ok && pp.PeerID != rcv.selfPeerID {
						peerRequests[pp.PeerID] = append(peerRequests[pp.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
						peerInfoMap[pp.PeerID] = pp
					}
					if attempt >= 2 {
						for _, p := range peersCopy {
							if p.PeerID != rcv.selfPeerID && p.Row != primaryRow {
								peerRequests[p.PeerID] = append(peerRequests[p.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
								peerInfoMap[p.PeerID] = p
							}
						}
					}
				}
			} else { // Non-custody
				rcv.nonCustodyLockedMu.Lock()
				locked := rcv.nonCustodyLocked[cellKey]
				rcv.nonCustodyLockedMu.Unlock()

				if !locked {
					if pp, ok := peerByRow[primaryRow]; ok && pp.PeerID != rcv.selfPeerID {
						peerRequests[pp.PeerID] = append(peerRequests[pp.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
						peerInfoMap[pp.PeerID] = pp
					}
					if bp, ok := peerByRow[backupRow]; ok && bp.PeerID != rcv.selfPeerID {
						peerRequests[bp.PeerID] = append(peerRequests[bp.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
						peerInfoMap[bp.PeerID] = bp
					}
					if attempt >= 2 {
						for _, p := range peersCopy {
							if p.PeerID != rcv.selfPeerID && p.Row != primaryRow && p.Row != backupRow {
								peerRequests[p.PeerID] = append(peerRequests[p.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
								peerInfoMap[p.PeerID] = p
							}
						}
					}
				}
			}
		}
	}

	if len(peerRequests) == 0 {
		return
	}

	var wg sync.WaitGroup
	for pid, coords := range peerRequests {
		pInfo := peerInfoMap[pid]
		wg.Add(1)
		go func(peer p2pcommon.PeerInfo, reqCoords []p2pcommon.CellCoord) {
			defer wg.Done()
			cellsMap, err := rcv.batchFetchFromPeer(blockID, peer, reqCoords)
			if err != nil {
				log.Printf("[StoreNode] Batch fetch from peer %s (row %d, %d cells) failed: %v", peer.PeerID, peer.Row, len(reqCoords), err)
				return
			}
			rcv.processBatchFetchPieces(blockID, peer.Row, peer.PeerID, cellsMap)
		}(pInfo, coords)
	}
	wg.Wait()

	time.Sleep(100 * time.Millisecond)
	rcv.CheckAndLogCompletion(blockID)
}

func (rcv *Receiver) batchFetchFromPeer(blockID string, pInfo p2pcommon.PeerInfo, coords []p2pcommon.CellCoord) (map[string][]p2pcommon.CodedPiece, error) {
	if len(coords) == 0 {
		return nil, nil
	}

	pid, err := peer.Decode(pInfo.PeerID)
	if err != nil || pid == rcv.host.ID() {
		return nil, fmt.Errorf("invalid peer id or self: %v", err)
	}

	for _, addrStr := range pInfo.Multiaddrs {
		maddr, err := multiaddr.NewMultiaddr(addrStr)
		if err == nil {
			rcv.host.Peerstore().AddAddr(pid, maddr, 10*time.Minute)
		}
	}

	ctxDial, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()

	err = rcv.host.Connect(ctxDial, peer.AddrInfo{ID: pid})
	if err != nil {
		return nil, fmt.Errorf("connect failed: %w", err)
	}

	stream, err := rcv.host.NewStream(ctxDial, pid, p2pcommon.ProtoStoreBatchFetch)
	if err != nil {
		return nil, fmt.Errorf("open stream failed: %w", err)
	}
	defer stream.Close()

	req := p2pcommon.StoreBatchFetchRequest{
		BlockID: blockID,
		Cells:   coords,
	}

	if err := json.NewEncoder(stream).Encode(req); err != nil {
		return nil, fmt.Errorf("encode req failed: %w", err)
	}

	var resp p2pcommon.StoreBatchFetchResponse
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode resp failed: %w", err)
	}

	return resp.Cells, nil
}

func (rcv *Receiver) processBatchFetchPieces(blockID string, senderRow int, senderPeerID string, cellsMap map[string][]p2pcommon.CodedPiece) {
	if len(cellsMap) == 0 {
		return
	}

	storesPerCol := rcv.storesPerCol
	if storesPerCol <= 0 {
		storesPerCol = 8
	}

	for key, pList := range cellsMap {
		if len(pList) == 0 {
			continue
		}
		var row, col int
		if _, err := fmt.Sscanf(key, "%d_%d", &row, &col); err != nil {
			row = pList[0].Row
			col = pList[0].Col
		}

		isPrimary := (row % storesPerCol) == rcv.rowIdx
		isBackup := ((row + 1) % storesPerCol) == rcv.rowIdx
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)

		if isPrimary {
			rcv.cellMu.Lock()
			latestPieces := rcv.cache.GetPieces(blockID, row, col)
			for _, pPayload := range pList {
				decData, err1 := hex.DecodeString(pPayload.Data)
				decCoeffs, err2 := hex.DecodeString(pPayload.Coeffs)
				decProof, err3 := hex.DecodeString(pPayload.Proof)
				if err1 != nil || err2 != nil || err3 != nil {
					continue
				}

				existingCoeffs := make([][]byte, len(latestPieces))
				for idx, val := range latestPieces {
					existingCoeffs[idx] = val.Data.Coeffs
				}

				if engine.IsLinearlyIndependent(existingCoeffs, decCoeffs, rcv.kPiece) {
					p := cda.ReceivedPiece{
						Row: row,
						Col: col,
						Data: rlnc.PieceData{
							Data:   decData,
							Coeffs: decCoeffs,
						},
						Proof: cda.OpeningProof(decProof),
					}
					rcv.cache.StorePiece(blockID, row, col, p)
					latestPieces = append(latestPieces, p)
				}
			}
			fullRank := len(latestPieces) >= rcv.kPiece
			rcv.cellMu.Unlock()

			if fullRank {
				rcv.broadcastMu.Lock()
				bCount := rcv.cellBroadcastCount[cellKey]
				rcv.broadcastMu.Unlock()
				if bCount < 1 {
					pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
					rcv.cellMu.Lock()
					allPieces := rcv.cache.GetPieces(blockID, row, col)
					rcv.cellMu.Unlock()
					if len(allPieces) >= rcv.kPiece {
						recodedPiece, err := rcv.rm.RecodePiecesWithVerify(allPieces[:rcv.kPiece], pieceCommits, 5)
						if err == nil {
							if err := rcv.broadcaster.BroadcastRecodedPiece(blockID, row, col, recodedPiece, pieceCommits); err == nil {
								rcv.broadcastMu.Lock()
								rcv.cellBroadcastCount[cellKey]++
								rcv.broadcastMu.Unlock()
							}
						}
					}
				}
			}
		} else if isBackup {
			rcv.broadcastMu.Lock()
			bCount := rcv.cellBroadcastCount[cellKey]
			rcv.broadcastMu.Unlock()

			rcv.pruneMu.Lock()
			pruned := rcv.prunedCells[cellKey]
			rcv.pruneMu.Unlock()

			if bCount >= 1 || pruned {
				continue
			}

			rcv.cellMu.Lock()
			latestPieces := rcv.cache.GetPieces(blockID, row, col)
			for _, pPayload := range pList {
				decData, err1 := hex.DecodeString(pPayload.Data)
				decCoeffs, err2 := hex.DecodeString(pPayload.Coeffs)
				decProof, err3 := hex.DecodeString(pPayload.Proof)
				if err1 != nil || err2 != nil || err3 != nil {
					continue
				}

				existingCoeffs := make([][]byte, len(latestPieces))
				for idx, val := range latestPieces {
					existingCoeffs[idx] = val.Data.Coeffs
				}

				if engine.IsLinearlyIndependent(existingCoeffs, decCoeffs, rcv.kPiece) {
					p := cda.ReceivedPiece{
						Row: row,
						Col: col,
						Data: rlnc.PieceData{
							Data:   decData,
							Coeffs: decCoeffs,
						},
						Proof: cda.OpeningProof(decProof),
					}
					rcv.cache.StorePiece(blockID, row, col, p)
					latestPieces = append(latestPieces, p)
				}
			}
			fullRank := len(latestPieces) >= rcv.kPiece
			rcv.cellMu.Unlock()

			if fullRank {
				numBroadcast := rcv.kPiece / 2
				if numBroadcast < 1 {
					numBroadcast = 1
				}
				pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
				rcv.cellMu.Lock()
				allPieces := rcv.cache.GetPieces(blockID, row, col)
				rcv.cellMu.Unlock()
				if len(allPieces) >= rcv.kPiece {
					targetPieces := allPieces[:rcv.kPiece]
					for i := 0; i < numBroadcast; i++ {
						recodedPiece, err := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5)
						if err != nil {
							continue
						}
						if err := rcv.broadcaster.BroadcastRecodedPiece(blockID, row, col, recodedPiece, pieceCommits); err == nil {
							rcv.broadcastMu.Lock()
							rcv.cellBroadcastCount[cellKey]++
							rcv.broadcastMu.Unlock()
						}
					}
					rcv.cellMu.Lock()
					rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
					rcv.cellMu.Unlock()

					rcv.pruneMu.Lock()
					rcv.prunedCells[cellKey] = true
					rcv.pruneMu.Unlock()
				}
			}
		} else { // Non-custody
			rcv.nonCustodyLockedMu.Lock()
			if rcv.nonCustodyLocked[cellKey] {
				rcv.nonCustodyLockedMu.Unlock()
				continue
			}
			rcv.nonCustodyLockedMu.Unlock()

			rcv.cellMu.Lock()
			latestPieces := rcv.cache.GetPieces(blockID, row, col)
			for _, pPayload := range pList {
				decData, err1 := hex.DecodeString(pPayload.Data)
				decCoeffs, err2 := hex.DecodeString(pPayload.Coeffs)
				decProof, err3 := hex.DecodeString(pPayload.Proof)
				if err1 != nil || err2 != nil || err3 != nil {
					continue
				}

				existingCoeffs := make([][]byte, len(latestPieces))
				for idx, val := range latestPieces {
					existingCoeffs[idx] = val.Data.Coeffs
				}

				if engine.IsLinearlyIndependent(existingCoeffs, decCoeffs, rcv.kPiece) {
					p := cda.ReceivedPiece{
						Row: row,
						Col: col,
						Data: rlnc.PieceData{
							Data:   decData,
							Coeffs: decCoeffs,
						},
						Proof: cda.OpeningProof(decProof),
					}
					rcv.cache.StorePiece(blockID, row, col, p)
					latestPieces = append(latestPieces, p)

					rcv.contribMu.Lock()
					if rcv.peerContributions[cellKey] == nil {
						rcv.peerContributions[cellKey] = make(map[string]int)
					}
					rcv.peerContributions[cellKey][senderPeerID]++
					rcv.contribMu.Unlock()
				}
			}

			rcv.contribMu.Lock()
			numSources := len(rcv.peerContributions[cellKey])
			rcv.contribMu.Unlock()

			minPieces := 2
			if rcv.kPiece < minPieces {
				minPieces = rcv.kPiece
			}
			minSources := 2
			if rcv.kPiece < minSources {
				minSources = rcv.kPiece
			}

			var finalPiece *cda.ReceivedPiece
			pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)

			if len(latestPieces) >= minPieces && numSources >= minSources {
				recoded, err := rcv.rm.RecodePiecesWithVerify(latestPieces[:minPieces], pieceCommits, 5)
				if err == nil && recoded != nil {
					finalPiece = recoded
				} else {
					finalPiece = &latestPieces[0]
				}
			} else if len(latestPieces) >= 1 {
				if len(latestPieces) >= 2 {
					recoded, err := rcv.rm.RecodePiecesWithVerify(latestPieces[:2], pieceCommits, 5)
					if err == nil && recoded != nil {
						finalPiece = recoded
					} else {
						finalPiece = &latestPieces[0]
					}
				} else {
					finalPiece = &latestPieces[0]
				}
			}

			if finalPiece != nil {
				rcv.cache.StoreRecodedPiece(blockID, row, col, *finalPiece)
				rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
				rcv.cellMu.Unlock()

				rcv.nonCustodyLockedMu.Lock()
				rcv.nonCustodyLocked[cellKey] = true
				rcv.nonCustodyLockedMu.Unlock()

				rcv.pruneMu.Lock()
				rcv.prunedCells[cellKey] = true
				rcv.pruneMu.Unlock()
			} else {
				rcv.cellMu.Unlock()
			}
		}
	}
}

func (rcv *Receiver) handleBatchFetchStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	if !rcv.allowRequest(remotePeer) {
		log.Printf("[RateLimiter] Rejected batch fetch request from peer %s (rate limit exceeded)", remotePeer)
		rcv.respondWithError(stream, "rate limit exceeded")
		return
	}

	recordP2PRequest()

	var req p2pcommon.StoreBatchFetchRequest
	if err := json.NewDecoder(stream).Decode(&req); err != nil {
		return
	}

	resp := p2pcommon.StoreBatchFetchResponse{
		BlockID: req.BlockID,
		Cells:   make(map[string][]p2pcommon.CodedPiece, len(req.Cells)),
	}

	colCommitsCache := make(map[int][]string)

	for _, coord := range req.Cells {
		rcv.cellMu.Lock()
		localPieces := rcv.cache.GetPieces(req.BlockID, coord.Row, coord.Col)
		localRecoded := rcv.cache.GetRecodedPieces(req.BlockID, coord.Row, coord.Col)
		rcv.cellMu.Unlock()

		allPieces := append([]cda.ReceivedPiece(nil), localPieces...)
		for _, p := range localRecoded {
			existingCoeffs := make([][]byte, len(allPieces))
			for idx, val := range allPieces {
				existingCoeffs[idx] = val.Data.Coeffs
			}
			if engine.IsLinearlyIndependent(existingCoeffs, p.Data.Coeffs, rcv.kPiece) {
				allPieces = append(allPieces, p)
			}
		}

		if len(allPieces) == 0 {
			continue
		}

		commitsStr, ok := colCommitsCache[coord.Col]
		if !ok {
			pieceCommits, _ := rcv.cache.GetAnchoredCommitments(req.BlockID, coord.Col)
			commitsStr = make([]string, len(pieceCommits))
			for i, c := range pieceCommits {
				commitsStr[i] = hex.EncodeToString(c)
			}
			colCommitsCache[coord.Col] = commitsStr
		}

		cellPieces := make([]p2pcommon.CodedPiece, len(allPieces))
		for i, p := range allPieces {
			cellPieces[i] = p2pcommon.CodedPiece{
				Row:          p.Row,
				Col:          p.Col,
				Data:         hex.EncodeToString(p.Data.Data),
				Coeffs:       hex.EncodeToString(p.Data.Coeffs),
				Proof:        hex.EncodeToString(p.Proof),
				PieceCommits: commitsStr,
			}
		}
		cellKey := fmt.Sprintf("%d_%d", coord.Row, coord.Col)
		resp.Cells[cellKey] = cellPieces
	}

	_ = json.NewEncoder(stream).Encode(resp)
}

func (rcv *Receiver) pullMissingPiecesFromPeers(blockID string, row, col int) {
	rcv.cellMu.Lock()
	localPieces := rcv.cache.GetPieces(blockID, row, col)
	rcv.cellMu.Unlock()
	if len(localPieces) >= rcv.kPiece {
		return
	}

	rcv.peersMu.RLock()
	peersCopy := make([]p2pcommon.PeerInfo, len(rcv.colPeers))
	copy(peersCopy, rcv.colPeers)
	rcv.peersMu.RUnlock()

	if len(peersCopy) == 0 {
		return
	}

	// Prioritize Primary Node, then Backup Node for this row
	primaryRow := row % rcv.storesPerCol
	backupRow := (row + 1) % rcv.storesPerCol
	sort.SliceStable(peersCopy, func(i, j int) bool {
		if peersCopy[i].Row == primaryRow {
			return true
		}
		if peersCopy[j].Row == primaryRow {
			return false
		}
		if peersCopy[i].Row == backupRow {
			return true
		}
		if peersCopy[j].Row == backupRow {
			return false
		}
		return peersCopy[i].Row < peersCopy[j].Row
	})

	var allPieces []cda.ReceivedPiece

	for attempt := 1; attempt <= 3; attempt++ {
		rcv.cellMu.Lock()
		localPieces = rcv.cache.GetPieces(blockID, row, col)
		rcv.cellMu.Unlock()
		if len(localPieces) >= rcv.kPiece {
			return
		}

		allPieces = append([]cda.ReceivedPiece(nil), localPieces...)

		for _, pInfo := range peersCopy {
			pid, err := peer.Decode(pInfo.PeerID)
			if err != nil || pid == rcv.host.ID() {
				continue
			}

			for _, addrStr := range pInfo.Multiaddrs {
				maddr, err := multiaddr.NewMultiaddr(addrStr)
				if err == nil {
					rcv.host.Peerstore().AddAddr(pid, maddr, 10*time.Minute)
				}
			}

			ctxDial, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
			err = rcv.host.Connect(ctxDial, peer.AddrInfo{ID: pid})
			if err != nil {
				cancelDial()
				continue
			}

			pStream, err := rcv.host.NewStream(ctxDial, pid, p2pcommon.ProtoStoreFetch)
			if err != nil {
				cancelDial()
				continue
			}

			fetchReq := p2pcommon.StoreFetchRequest{
				BlockID:     blockID,
				Row:         row,
				Col:         col,
				IsRemoteHop: true,
			}

			if err := json.NewEncoder(pStream).Encode(fetchReq); err != nil {
				pStream.Close()
				cancelDial()
				continue
			}

			var peerResp p2pcommon.StoreFetchResponse
			if err := json.NewDecoder(pStream).Decode(&peerResp); err != nil {
				pStream.Close()
				cancelDial()
				continue
			}
			pStream.Close()
			cancelDial()

			for _, pPayload := range peerResp.Pieces {
				decData, err1 := hex.DecodeString(pPayload.Data)
				decCoeffs, err2 := hex.DecodeString(pPayload.Coeffs)
				decProof, err3 := hex.DecodeString(pPayload.Proof)
				if err1 != nil || err2 != nil || err3 != nil {
					continue
				}

				p := cda.ReceivedPiece{
					Row: pPayload.Row,
					Col: pPayload.Col,
					Data: rlnc.PieceData{
						Data:   decData,
						Coeffs: decCoeffs,
					},
					Proof: cda.OpeningProof(decProof),
				}

				rcv.cellMu.Lock()
				latestPieces := rcv.cache.GetPieces(blockID, row, col)
				existingCoeffs := make([][]byte, len(latestPieces))
				for idx, val := range latestPieces {
					existingCoeffs[idx] = val.Data.Coeffs
				}

				if engine.IsLinearlyIndependent(existingCoeffs, decCoeffs, rcv.kPiece) {
					rcv.cache.StorePiece(blockID, row, col, p)
					latestPieces = append(latestPieces, p)
					log.Printf("[StoreNode] Active pull (attempt %d): stored independent piece for cell [%d, %d] from peer %s (Row %d) (count: %d/%d)", attempt, row, col, pid, pInfo.Row, len(latestPieces), rcv.kPiece)
					allPieces = latestPieces
					rcv.CheckAndLogCompletion(blockID)
				}
				rcv.cellMu.Unlock()

				if len(allPieces) >= rcv.kPiece {
					break
				}
			}

			if len(allPieces) >= rcv.kPiece {
				break
			}
		}

		if len(allPieces) >= rcv.kPiece {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	if len(allPieces) >= rcv.kPiece {
		pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
		// Active pull: broadcast kPiece/2 independently recoded pieces for primary or backup custody nodes
		isPullPrimary := (row % rcv.storesPerCol) == rcv.rowIdx
		isPullBackup := ((row + 1) % rcv.storesPerCol) == rcv.rowIdx

		targetPieces := allPieces
		if len(targetPieces) > rcv.kPiece {
			targetPieces = targetPieces[:rcv.kPiece]
		}

		// Always recode+store once first (for local availability)
		firstRecoded, err := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5)
		if err == nil {
			rcv.cellMu.Lock()
			rcv.cache.StoreRecodedPiece(blockID, row, col, *firstRecoded)
			rcv.cellMu.Unlock()
			log.Printf("[StoreNode] Successfully recoded cell [%d, %d] in active pull.", row, col)
			rcv.CheckAndLogCompletion(blockID)

			if isPullPrimary || isPullBackup {
				numBroadcast := rcv.kPiece / 2
				if numBroadcast < 1 {
					numBroadcast = 1
				}
				broadcastSucceeded := 0
				for i := 0; i < numBroadcast; i++ {
					recodedPiece, rerr := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5)
					if rerr != nil {
						log.Printf("[GossipSub] Active pull: recode round %d/%d failed for cell [%d, %d]: %v", i+1, numBroadcast, row, col, rerr)
						continue
					}
					if berr := rcv.broadcaster.BroadcastRecodedPiece(blockID, row, col, recodedPiece, pieceCommits); berr != nil {
						log.Printf("[GossipSub] Active pull: broadcast round %d/%d failed for cell [%d, %d] (Primary=%v): %v", i+1, numBroadcast, row, col, isPullPrimary, berr)
					} else {
						broadcastSucceeded++
						log.Printf("[GossipSub] Active pull: broadcast round %d/%d succeeded for cell [%d, %d] (Primary=%v).", i+1, numBroadcast, row, col, isPullPrimary)
						cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
						rcv.broadcastMu.Lock()
						rcv.cellBroadcastCount[cellKey]++
						rcv.broadcastMu.Unlock()
						rcv.CheckAndLogCompletion(blockID)
					}
				}

				if !isPullPrimary {
					// Backup: prune raw pieces after active-pull dissemination attempt
					rcv.cellMu.Lock()
					rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
					rcv.cellMu.Unlock()
					cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
					rcv.pruneMu.Lock()
					rcv.prunedCells[cellKey] = true
					rcv.pruneMu.Unlock()
					log.Printf("[StoreNode] Backup: active pull cell [%d, %d] dissemination finished (%d/%d rounds); raw pieces pruned.", row, col, broadcastSucceeded, numBroadcast)
				}
			} else {
				// Non-custody node: prune raw pieces, lock cell permanently, and satisfy IsComplete
				rcv.cellMu.Lock()
				rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
				rcv.cellMu.Unlock()
				cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
				rcv.nonCustodyLockedMu.Lock()
				rcv.nonCustodyLocked[cellKey] = true
				rcv.nonCustodyLockedMu.Unlock()
				rcv.pruneMu.Lock()
				rcv.prunedCells[cellKey] = true
				rcv.pruneMu.Unlock()
				log.Printf("[StoreNode] Non-custody: active pull cell [%d, %d] recoded, raw pieces pruned, cell locked permanently.", row, col)
				rcv.CheckAndLogCompletion(blockID)
			}
		} else {
			log.Printf("[StoreNode] Failed to recode cell [%d, %d] in active pull: %v", row, col, err)
		}
	}
}

func (rcv *Receiver) pullNonCustodyPiece(blockID string, row, col int) {
	cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
	rcv.nonCustodyLockedMu.Lock()
	if rcv.nonCustodyLocked[cellKey] {
		rcv.nonCustodyLockedMu.Unlock()
		return
	}
	rcv.nonCustodyLockedMu.Unlock()

	rcv.peersMu.RLock()
	peersCopy := make([]p2pcommon.PeerInfo, len(rcv.colPeers))
	copy(peersCopy, rcv.colPeers)
	rcv.peersMu.RUnlock()

	if len(peersCopy) == 0 {
		return
	}

	primaryRow := row % rcv.storesPerCol
	backupRow := (row + 1) % rcv.storesPerCol
	sort.SliceStable(peersCopy, func(i, j int) bool {
		if peersCopy[i].Row == primaryRow {
			return true
		}
		if peersCopy[j].Row == primaryRow {
			return false
		}
		if peersCopy[i].Row == backupRow {
			return true
		}
		return false
	})

	var pulledPieces []cda.ReceivedPiece
	for _, pInfo := range peersCopy {
		if pInfo.Row != primaryRow && pInfo.Row != backupRow {
			continue
		}
		pid, err := peer.Decode(pInfo.PeerID)
		if err != nil || pid == rcv.host.ID() {
			continue
		}

		ctxDial, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
		err = rcv.host.Connect(ctxDial, peer.AddrInfo{ID: pid})
		if err != nil {
			cancelDial()
			continue
		}

		pStream, err := rcv.host.NewStream(ctxDial, pid, p2pcommon.ProtoStoreFetch)
		if err != nil {
			cancelDial()
			continue
		}

		fetchReq := p2pcommon.StoreFetchRequest{
			BlockID:     blockID,
			Row:         row,
			Col:         col,
			IsRemoteHop: true,
		}
		if err := json.NewEncoder(pStream).Encode(fetchReq); err != nil {
			pStream.Close()
			cancelDial()
			continue
		}

		var peerResp p2pcommon.StoreFetchResponse
		if err := json.NewDecoder(pStream).Decode(&peerResp); err != nil {
			pStream.Close()
			cancelDial()
			continue
		}
		pStream.Close()
		cancelDial()

		if len(peerResp.Pieces) > 0 {
			pPayload := peerResp.Pieces[0]
			decData, err1 := hex.DecodeString(pPayload.Data)
			decCoeffs, err2 := hex.DecodeString(pPayload.Coeffs)
			decProof, err3 := hex.DecodeString(pPayload.Proof)
			if err1 == nil && err2 == nil && err3 == nil {
				p := cda.ReceivedPiece{
					Row: pPayload.Row,
					Col: pPayload.Col,
					Data: rlnc.PieceData{
						Data:   decData,
						Coeffs: decCoeffs,
					},
					Proof: cda.OpeningProof(decProof),
				}
				pulledPieces = append(pulledPieces, p)
				if len(pulledPieces) >= 2 {
					break
				}
			}
		}
	}

	if len(pulledPieces) == 0 {
		return
	}

	pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
	var finalPiece *cda.ReceivedPiece
	if len(pulledPieces) >= 2 {
		recoded, err := rcv.rm.RecodePiecesWithVerify(pulledPieces[:2], pieceCommits, 5)
		if err == nil && recoded != nil {
			finalPiece = recoded
		} else {
			finalPiece = &pulledPieces[0]
		}
	} else {
		finalPiece = &pulledPieces[0]
	}

	rcv.cellMu.Lock()
	rcv.cache.StoreRecodedPiece(blockID, row, col, *finalPiece)
	rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
	rcv.cellMu.Unlock()

	rcv.nonCustodyLockedMu.Lock()
	rcv.nonCustodyLocked[cellKey] = true
	rcv.nonCustodyLockedMu.Unlock()

	rcv.pruneMu.Lock()
	rcv.prunedCells[cellKey] = true
	rcv.pruneMu.Unlock()

	log.Printf("[StoreNode] Non-custody active pull: successfully stored piece for cell [%d, %d] from %d custody sources", row, col, len(pulledPieces))
	rcv.CheckAndLogCompletion(blockID)
}

func (rcv *Receiver) handleFetchStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	if !rcv.allowRequest(remotePeer) {
		log.Printf("[RateLimiter] Rejected fetch request from peer %s (rate limit exceeded)", remotePeer)
		rcv.respondWithError(stream, "rate limit exceeded")
		return
	}

	recordP2PRequest()

	var req p2pcommon.StoreFetchRequest
	if err := json.NewDecoder(stream).Decode(&req); err != nil {
		return
	}

	// 1. Gather all local pieces (raw and recoded filtered by rank)
	rcv.cellMu.Lock()
	localPieces := rcv.cache.GetPieces(req.BlockID, req.Row, req.Col)
	allPieces := append([]cda.ReceivedPiece(nil), localPieces...)

	localRecoded := rcv.cache.GetRecodedPieces(req.BlockID, req.Row, req.Col)
	for _, p := range localRecoded {
		existingCoeffs := make([][]byte, len(allPieces))
		for idx, val := range allPieces {
			existingCoeffs[idx] = val.Data.Coeffs
		}
		if engine.IsLinearlyIndependent(existingCoeffs, p.Data.Coeffs, rcv.kPiece) {
			allPieces = append(allPieces, p)
		}
	}
	rcv.cellMu.Unlock()

	height := p2pcommon.ParseHeightFromBlockID(req.BlockID)
	if len(allPieces) > 0 || p2pcommon.IsDebug() {
		log.Printf("[Height: %d] [StoreNode] Retrieving cell [%d, %d] for block %s. Local pieces: %d", height, req.Row, req.Col, req.BlockID, len(allPieces))
	}

	// Get commitments
	pieceCommits, _ := rcv.cache.GetAnchoredCommitments(req.BlockID, req.Col)
	pieceCommitsStr := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		pieceCommitsStr[i] = hex.EncodeToString(c)
	}

	// 2. Query peers in column network if needed and allowed
	if len(allPieces) < rcv.kPiece && !req.IsRemoteHop {
		p2pcommon.LogDebug("[StoreNode] Not enough local pieces (%d/%d). Querying peers in column network...", len(allPieces), rcv.kPiece)
		rcv.peersMu.RLock()
		peersCopy := make([]p2pcommon.PeerInfo, len(rcv.colPeers))
		copy(peersCopy, rcv.colPeers)
		rcv.peersMu.RUnlock()

		for _, pInfo := range peersCopy {
			pid, err := peer.Decode(pInfo.PeerID)
			if err != nil {
				continue
			}

			// Add addresses
			for _, addrStr := range pInfo.Multiaddrs {
				maddr, err := multiaddr.NewMultiaddr(addrStr)
				if err != nil {
					continue
				}
				rcv.host.Peerstore().AddAddr(pid, maddr, 10*time.Minute)
			}

			ctxDial, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
			err = rcv.host.Connect(ctxDial, peer.AddrInfo{ID: pid})
			if err != nil {
				cancelDial()
				continue
			}

			pStream, err := rcv.host.NewStream(ctxDial, pid, p2pcommon.ProtoStoreFetch)
			if err != nil {
				cancelDial()
				continue
			}
			p2pcommon.LogDebug("[StoreNode] Fetch query: requesting pieces for cell [%d, %d] from peer %s...", req.Row, req.Col, pid)

			fetchReq := p2pcommon.StoreFetchRequest{
				BlockID:     req.BlockID,
				Row:         req.Row,
				Col:         req.Col,
				IsRemoteHop: true, // Prevent cycle
			}

			if err := json.NewEncoder(pStream).Encode(fetchReq); err != nil {
				pStream.Close()
				cancelDial()
				continue
			}

			var peerResp p2pcommon.StoreFetchResponse
			if err := json.NewDecoder(pStream).Decode(&peerResp); err != nil {
				pStream.Close()
				cancelDial()
				continue
			}
			pStream.Close()
			cancelDial()
			p2pcommon.LogDebug("[StoreNode] Fetch query: received response from peer %s with %d pieces", pid, len(peerResp.Pieces))

			for _, pPayload := range peerResp.Pieces {
				decData, err1 := hex.DecodeString(pPayload.Data)
				decCoeffs, err2 := hex.DecodeString(pPayload.Coeffs)
				decProof, err3 := hex.DecodeString(pPayload.Proof)
				if err1 != nil || err2 != nil || err3 != nil {
					continue
				}

				p := cda.ReceivedPiece{
					Row: pPayload.Row,
					Col: pPayload.Col,
					Data: rlnc.PieceData{
						Data:   decData,
						Coeffs: decCoeffs,
					},
					Proof: cda.OpeningProof(decProof),
				}

				existingCoeffs := make([][]byte, len(allPieces))
				for idx, val := range allPieces {
					existingCoeffs[idx] = val.Data.Coeffs
				}

				if engine.IsLinearlyIndependent(existingCoeffs, decCoeffs, rcv.kPiece) {
					allPieces = append(allPieces, p)
					p2pcommon.LogDebug("[StoreNode] Added independent piece from peer %s. Current count: %d", pid, len(allPieces))
					if len(allPieces) >= rcv.kPiece {
						break
					}
				}
			}

			if len(allPieces) >= rcv.kPiece {
				break
			}
		}
	}

	// 3. Try to recover the original cell data if we have k independent pieces
	recovered := false
	var cellDataStr string
	if len(allPieces) >= rcv.kPiece {
		start := time.Now()
		recoveredFrags, err := rcv.rm.RecoverCell(allPieces[:rcv.kPiece])
		ReconstructDuration.Observe(time.Since(start).Seconds())
		if err == nil {
			pieceSize := 64 / rcv.kPiece
			var buf bytes.Buffer
			for _, frag := range recoveredFrags {
				if len(frag) >= pieceSize {
					buf.Write(frag[len(frag)-pieceSize:])
				} else {
					buf.Write(frag)
				}
			}
			cellDataStr = hex.EncodeToString(buf.Bytes())
			recovered = true
			log.Printf("[StoreNode] Successfully recovered cell [%d, %d] data: %s...", req.Row, req.Col, cellDataStr[:16])
		} else {
			log.Printf("[StoreNode] Failed to recover cell: %v", err)
		}
	}
	if len(allPieces) > 0 || p2pcommon.IsDebug() {
		log.Printf("[StoreNode] Fetch query completed for cell [%d, %d]. Total independent pieces gathered: %d/%d (Recovery Success: %v)", req.Row, req.Col, len(allPieces), rcv.kPiece, recovered)
	}

	// Respond back
	respPieces := make([]p2pcommon.CodedPiece, len(allPieces))
	for i, p := range allPieces {
		respPieces[i] = p2pcommon.CodedPiece{
			Row:          p.Row,
			Col:          p.Col,
			Data:         hex.EncodeToString(p.Data.Data),
			Coeffs:       hex.EncodeToString(p.Data.Coeffs),
			Proof:        hex.EncodeToString(p.Proof),
			PieceCommits: pieceCommitsStr,
		}
	}

	resp := p2pcommon.StoreFetchResponse{
		BlockID:     req.BlockID,
		Row:         req.Row,
		Col:         req.Col,
		Recovered:   recovered,
		Data:        cellDataStr,
		PiecesCount: len(allPieces),
		Pieces:      respPieces,
	}

	json.NewEncoder(stream).Encode(resp)
}

func (rcv *Receiver) DiagnoseCompletion(blockID string) (bool, map[string]interface{}) {
	n := 2 * rcv.kBlock
	numCols := rcv.numCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}

	netColIdx := rcv.colIdx / colsPerNetCol
	startCol := netColIdx * colsPerNetCol
	endCol := startCol + colsPerNetCol

	storesPerCol := rcv.storesPerCol
	if storesPerCol <= 0 {
		storesPerCol = 8
	}

	minBroadcast := 1

	rcv.broadcastMu.Lock()
	defer rcv.broadcastMu.Unlock()

	var unmetReasons []string

	for c := startCol; c < endCol; c++ {
		for r := 0; r < n; r++ {
			isPrimary := (r % storesPerCol) == rcv.rowIdx
			isBackup := ((r + 1) % storesPerCol) == rcv.rowIdx
			cellKey := fmt.Sprintf("%s_%d_%d", blockID, r, c)
			if isPrimary {
				// Primary: must retain >= kPiece raw pieces for long-term storage
				// AND must have disseminated >= 1 recoded piece to the network
				pCount := rcv.cache.GetPieceCount(blockID, r, c)
				bCount := rcv.cellBroadcastCount[cellKey]
				if pCount < rcv.kPiece {
					unmetReasons = append(unmetReasons, fmt.Sprintf("primary [%d,%d]: pieces=%d < %d", r, c, pCount, rcv.kPiece))
				}
				if bCount < minBroadcast {
					unmetReasons = append(unmetReasons, fmt.Sprintf("primary [%d,%d]: broadcast=%d < %d", r, c, bCount, minBroadcast))
				}
			} else if isBackup {
				// Backup: complete if dissemination finished (>= minBroadcast) OR if raw pieces pruned (<= 1 piece)
				if rcv.cellBroadcastCount[cellKey] < minBroadcast {
					rcv.pruneMu.Lock()
					pruned := rcv.prunedCells[cellKey]
					rcv.pruneMu.Unlock()
					total := rcv.cache.GetTotalPiecesForCell(blockID, r, c)
					if !pruned && total > 1 {
						unmetReasons = append(unmetReasons, fmt.Sprintf("backup [%d,%d]: not pruned and pieces=%d > 1", r, c, total))
					}
				}
			} else {
				// Non-custody: must store exactly 1 piece and be locked!
				cellKey := fmt.Sprintf("%s_%d_%d", blockID, r, c)
				rcv.nonCustodyLockedMu.Lock()
				locked := rcv.nonCustodyLocked[cellKey]
				rcv.nonCustodyLockedMu.Unlock()

				total := rcv.cache.GetTotalPiecesForCell(blockID, r, c)
				if locked && total > 1 {
					rcv.cellMu.Lock()
					rcv.cache.PruneRawPieces(blockID, r, c, rcv.rm)
					rcv.cellMu.Unlock()
					total = rcv.cache.GetTotalPiecesForCell(blockID, r, c)
				}
				if !locked || total != 1 {
					unmetReasons = append(unmetReasons, fmt.Sprintf("non-custody [%d,%d]: locked=%v, pieces=%d != 1", r, c, locked, total))
				}
			}
		}
	}

	details := map[string]interface{}{
		"unmet_count":   len(unmetReasons),
		"unmet_reasons": unmetReasons,
	}
	return len(unmetReasons) == 0, details
}

// IsComplete returns true if all custody requirements for blockID are satisfied.
func (rcv *Receiver) IsComplete(blockID string) bool {
	complete, _ := rcv.DiagnoseCompletion(blockID)
	return complete
}

func (rcv *Receiver) completionCheckWorkerLoop(ctx context.Context) {
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()

	pending := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return
		case blockID, ok := <-rcv.completionCheckChan:
			if !ok {
				return
			}
			pending[blockID] = true
		case <-ticker.C:
			if len(pending) == 0 {
				continue
			}
			for bID := range pending {
				rcv.doCheckAndLogCompletion(bID)
				delete(pending, bID)
			}
		}
	}
}

func (rcv *Receiver) CheckAndLogCompletion(blockID string) {
	rcv.completedMu.Lock()
	if rcv.completedBlocks != nil && rcv.completedBlocks[blockID] {
		rcv.completedMu.Unlock()
		return
	}
	rcv.completedMu.Unlock()

	select {
	case rcv.completionCheckChan <- blockID:
	default:
	}
}

func (rcv *Receiver) doCheckAndLogCompletion(blockID string) {
	rcv.completedMu.Lock()
	if rcv.completedBlocks == nil {
		rcv.completedBlocks = make(map[string]bool)
	}
	if rcv.completedBlocks[blockID] {
		rcv.completedMu.Unlock()
		return
	}
	rcv.completedMu.Unlock()

	if rcv.IsComplete(blockID) {
		rcv.completedMu.Lock()
		if rcv.completedBlocks[blockID] {
			rcv.completedMu.Unlock()
			return
		}
		rcv.completedBlocks[blockID] = true
		rcv.completedMu.Unlock()

		height := p2pcommon.ParseHeightFromBlockID(blockID)
		port := rcv.cache.Port()
		if port > 0 {
			logDir := fmt.Sprintf("data/store_%d", port)
			_ = os.MkdirAll(logDir, 0755)
			logPath := filepath.Join(logDir, "completion.log")

			f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err == nil {
				defer f.Close()
				timestamp := time.Now().Format("2006-01-02 15:04:05")
				logLine := fmt.Sprintf("[%s] [Height: %d] Block %s: Column registry reached IsComplete status successfully.\n", timestamp, height, blockID)
				_, _ = f.WriteString(logLine)
				log.Printf("[Height: %d] [StoreNode] Column registry reached IsComplete status successfully. Logged to %s.", height, logPath)
			} else {
				log.Printf("[Height: %d] [StoreNode] Warning: failed to write completion log: %v", height, err)
			}
		}

		// Broadcast StoreReady so the Publisher node can aggregate custody completion across all store nodes.
		if rcv.broadcaster != nil {
			go func(bID string, h int, col int, row int, stores int) {
				for attempt := 1; attempt <= 5; attempt++ {
					_ = rcv.broadcaster.BroadcastStoreReady(bID, h, col, row, stores)
					time.Sleep(2 * time.Second)
				}
			}(blockID, height, rcv.colIdx, rcv.rowIdx, rcv.storesPerCol)
		}
	}
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

func (rcv *Receiver) allowRequest(pid peer.ID) bool {
	rcv.rateLimitersMu.Lock()
	defer rcv.rateLimitersMu.Unlock()

	tb, exists := rcv.rateLimiters[pid]
	if !exists {
		tb = newTokenBucket(1000.0, 1000.0) // 1000 burst, 1000 tokens refill per second
		rcv.rateLimiters[pid] = tb
	}

	return tb.allow()
}

type tokenBucket struct {
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func newTokenBucket(maxTokens, refillRate float64) *tokenBucket {
	return &tokenBucket{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (tb *tokenBucket) allow() bool {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}
	return false
}

func (rcv *Receiver) startPruner(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rcv.pruneMu.Lock()
			now := time.Now()
			var toPrune []string
			for key, lastSeen := range rcv.cellLastActivity {
				if now.Sub(lastSeen) > rcv.pruneTTL {
					toPrune = append(toPrune, key)
				}
			}
			for _, key := range toPrune {
				delete(rcv.cellLastActivity, key)
			}
			rcv.pruneMu.Unlock()

			// Perform pruning for non-custody cells
			for _, key := range toPrune {
				parts := strings.Split(key, "_")
				if len(parts) >= 3 {
					blockID := strings.Join(parts[:len(parts)-2], "_")
					rowStr := parts[len(parts)-2]
					colStr := parts[len(parts)-1]

					var row, col int
					_, err1 := fmt.Sscanf(rowStr, "%d", &row)
					_, err2 := fmt.Sscanf(colStr, "%d", &col)
					if err1 == nil && err2 == nil {
						isCustody := (row % rcv.storesPerCol) == rcv.rowIdx
						if !isCustody {
							rcv.pruneMu.Lock()
							if rcv.prunedCells[key] {
								rcv.pruneMu.Unlock()
								continue
							}
							rcv.prunedCells[key] = true
							rcv.pruneMu.Unlock()

							log.Printf("[Pruning] Cell [%d, %d] of block %s is non-custody. Compressing raw pieces into 1 recoded piece and pruning raw pieces...", row, col, blockID)
							deleted := rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
							if deleted > 1 {
								rcv.totalStoredPiecesMu.Lock()
								subVal := int64(deleted - 1)
								if rcv.totalStoredPieces >= subVal {
									rcv.totalStoredPieces -= subVal
								} else {
									rcv.totalStoredPieces = 0
								}
								currCount := rcv.totalStoredPieces
								rcv.totalStoredPiecesMu.Unlock()
								LinearIndependentPiecesCount.Set(float64(currCount))
							}
							rcv.CheckAndLogCompletion(blockID)
						}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

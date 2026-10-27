package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/http"
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

func getEnvBool(key string, defaultVal bool) bool {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
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

	// Peer lists and Network Matrix Custody Map for Row and Column subnets
	peersMu           sync.RWMutex
	rowPeers          []p2pcommon.PeerInfo
	colPeers          []p2pcommon.PeerInfo
	rowCustodyMap     map[int][]p2pcommon.PeerInfo
	activePeersInCol  int
	bootstrapHTTPAddr string
	initialSyncOnce   sync.Once

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

	blacklistMu      sync.RWMutex
	blacklistedPeers map[string]time.Time

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
	cellDisseminating  map[string]bool

	// Non-custody cell lock: once recoded with >=2 pieces from >=2 sources, ignore further pieces for this cell
	nonCustodyLockedMu sync.Mutex
	nonCustodyLocked   map[string]bool
	nonCustodyPending  map[string]nonCustodyPendingCell

	// Track globally subscribed topics to prevent duplicate subscriptions
	subscribedTopicsMu sync.Mutex
	subscribedTopics   map[string]bool

	// Sharded worker queues by cell hash to ensure cell-level ordering and eliminate race conditions
	workerChans []chan func()
	numWorkers  int

	// Gossip batch processing
	gossipQueue       chan p2pcommon.SeedCellRequest
	binaryGossipQueue chan *p2pcommon.BinaryGossipPiece
	gossipBatchStop   chan struct{}

	// Cooldown / Grace period for active pulls
	pullMu       sync.Mutex
	pendingPulls map[string]bool

	// Active fallback monitor tracking per block
	fallbackMu      sync.Mutex
	activeFallbacks map[string]bool

	// Dynamic block dimension tracking: blockID -> N (2*K)
	blockNMu      sync.RWMutex
	blockNFetchMu sync.Mutex
	blockN        map[string]int

	// Debounced block completion checker
	completionCheckChan chan string

	// Bounded concurrency semaphore for custody cell dissemination
	disseminationSem chan struct{}

	// Context for GossipSub subscription goroutines lifetime
	ctx context.Context
}

type nonCustodyPendingCell struct {
	blockID   string
	row       int
	col       int
	firstSeen time.Time
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

	rcv := &Receiver{
		host:                h,
		ps:                  ps,
		kzg:                 kzg,
		rm:                  cda.NewRecipientManager(kPiece, kzg),
		publisherAddr:       pubAddr,
		selfPeerID:          selfPeerID,
		kBlock:              kBlock,
		kPiece:              kPiece,
		numCols:             numCols,
		storesPerCol:        storesPerCol,
		rowIdx:              rowIdx,
		colIdx:              colIdx,
		cache:               cache,
		broadcaster:         broadcaster,
		crashOnFail:         crashOnFail,
		rateLimiters:        make(map[peer.ID]*tokenBucket),
		totalStoredPieces:   int64(cache.GetTotalPieceCount()),
		pruneEnable:         pruneEnable,
		pruneTTL:            pruneTTL,
		cellLastActivity:    make(map[string]time.Time),
		prunedCells:         make(map[string]bool),
		completedBlocks:     make(map[string]bool),
		cellBroadcastCount:  make(map[string]int),
		cellDisseminating:   make(map[string]bool),
		nonCustodyLocked:    make(map[string]bool),
		nonCustodyPending:   make(map[string]nonCustodyPendingCell),
		subscribedTopics:    make(map[string]bool),
		peerContributions:   make(map[string]map[string]int),
		workerChans:         chans,
		numWorkers:          nWorkers,
		pendingPulls:        make(map[string]bool),
		activeFallbacks:     make(map[string]bool),
		gossipQueue:         make(chan p2pcommon.SeedCellRequest, getEnvInt("STORE_GOSSIP_QUEUE_CAP", 16384)),
		binaryGossipQueue:   make(chan *p2pcommon.BinaryGossipPiece, getEnvInt("STORE_GOSSIP_QUEUE_CAP", 16384)),
		gossipBatchStop:     make(chan struct{}),
		completionCheckChan: make(chan string, 1024),
		disseminationSem:    make(chan struct{}, getEnvInt("STORE_DISSEMINATION_SEM", 16)),
		blockN:              make(map[string]int),
		blacklistedPeers:    make(map[string]time.Time),
	}
	if broadcaster != nil {
		broadcaster.SetNetworkMatrix(rcv.getNetColIdx(), storesPerCol)
	}
	return rcv
}

// BlacklistPeer records a peer as isolated for the given duration
func (rcv *Receiver) BlacklistPeer(peerID string, duration time.Duration) {
	if peerID == "" {
		return
	}
	rcv.blacklistMu.Lock()
	defer rcv.blacklistMu.Unlock()
	if rcv.blacklistedPeers == nil {
		rcv.blacklistedPeers = make(map[string]time.Time)
	}
	expiry := time.Now().Add(duration)
	rcv.blacklistedPeers[peerID] = expiry
	log.Printf("[StoreNode] 🚫 Blacklisted byzantine peer %s until %s", peerID, expiry.Format(time.RFC3339))
}

// IsPeerBlacklisted returns true if the peer is currently blacklisted
func (rcv *Receiver) IsPeerBlacklisted(peerID string) bool {
	if peerID == "" {
		return false
	}
	rcv.blacklistMu.RLock()
	defer rcv.blacklistMu.RUnlock()
	expiry, exists := rcv.blacklistedPeers[peerID]
	if !exists {
		return false
	}
	if time.Now().After(expiry) {
		return false
	}
	return true
}

func (rcv *Receiver) SetBootstrapHTTPAddr(addr string) {
	rcv.peersMu.Lock()
	rcv.bootstrapHTTPAddr = addr
	rcv.peersMu.Unlock()
}

func (rcv *Receiver) SetPeers(rowPeers, colPeers []p2pcommon.PeerInfo) {
	rcv.SetRoutingState(rowPeers, colPeers, nil, rcv.storesPerCol, 0)
}

func (rcv *Receiver) SetRoutingState(rowPeers, colPeers []p2pcommon.PeerInfo, custodyMap map[int][]p2pcommon.PeerInfo, netRows int, activePeersInCol int) {
	rcv.peersMu.Lock()
	rcv.rowPeers = rowPeers
	rcv.colPeers = colPeers
	if netRows > 0 {
		rcv.storesPerCol = netRows
	}
	effectiveNetRows := rcv.storesPerCol
	if effectiveNetRows <= 0 {
		effectiveNetRows = 8
	}

	if len(custodyMap) > 0 {
		rcv.rowCustodyMap = custodyMap
	} else {
		// Deterministic fallback: compute custody map from colPeers + self
		allColStores := append([]p2pcommon.PeerInfo(nil), colPeers...)
		allColStores = append(allColStores, p2pcommon.PeerInfo{
			PeerID: rcv.selfPeerID,
			Row:    rcv.rowIdx,
			Col:    rcv.colIdx,
		})
		rcv.rowCustodyMap = p2pcommon.ComputeColumnCustodyMap(allColStores, effectiveNetRows)
	}
	if activePeersInCol > 0 {
		rcv.activePeersInCol = activePeersInCol
	} else {
		allColStores := append([]p2pcommon.PeerInfo(nil), colPeers...)
		allColStores = append(allColStores, p2pcommon.PeerInfo{
			PeerID: rcv.selfPeerID,
			Row:    rcv.rowIdx,
			Col:    rcv.colIdx,
		})
		rcv.activePeersInCol = len(p2pcommon.GetActiveColumnStores(allColStores))
	}
	rcv.peersMu.Unlock()

	if rcv.broadcaster != nil {
		rcv.broadcaster.SetNetworkMatrix(rcv.getNetColIdx(), effectiveNetRows)
	}

	// Update broadcaster with column peers
	var pIDs []peer.ID
	for _, p := range colPeers {
		if pid, err := peer.Decode(p.PeerID); err == nil {
			pIDs = append(pIDs, pid)
		}
	}
	if rcv.broadcaster != nil {
		rcv.broadcaster.UpdatePeers(pIDs)
	}

	// Ensure subscriptions to all row channels in this column and legacy peer topics
	if rcv.ctx != nil {
		go rcv.subscribeToRowChannels(rcv.ctx)
		go rcv.subscribeToNonCustodyCells(rcv.ctx, colPeers)
		rcv.initialSyncOnce.Do(func() {
			go rcv.catchUpRecentBlocksOnJoin(rcv.ctx)
		})
	}
}

// evalCustody evaluates whether this Store Node is an assigned Custody Node for data row,
// whether it retains full rank kPiece pieces (native retention) vs pruning down to 1 (cross-row backup),
// and the total number M of custody nodes assigned to data row's cell.
func (rcv *Receiver) evalCustody(row int) (isCustody bool, isNativeRetention bool, numCustodyPeers int) {
	rcv.peersMu.RLock()
	cmap := rcv.rowCustodyMap
	netRows := rcv.storesPerCol
	activeInCol := rcv.activePeersInCol
	if activeInCol <= 0 {
		activeInCol = len(rcv.colPeers)
	}
	rcv.peersMu.RUnlock()
	if netRows <= 0 {
		netRows = 8
	}
	isCustody, isNativeRetention, numCustodyPeers = p2pcommon.EvaluateNodeCustodyForRow(rcv.selfPeerID, rcv.rowIdx, row, netRows, cmap)
	if isCustody && activeInCol > 0 && activeInCol <= rcv.kPiece {
		isNativeRetention = true
	}
	return isCustody, isNativeRetention, numCustodyPeers
}

func (rcv *Receiver) getCustodyNetRows() []int {
	rcv.peersMu.RLock()
	cmap := rcv.rowCustodyMap
	netRows := rcv.storesPerCol
	rcv.peersMu.RUnlock()
	if netRows <= 0 {
		netRows = 8
	}
	return p2pcommon.GetNodeCustodyNetRows(rcv.selfPeerID, rcv.rowIdx, netRows, cmap)
}

func (rcv *Receiver) getActivePeersInCol() int {
	rcv.peersMu.RLock()
	count := rcv.activePeersInCol
	netRows := rcv.storesPerCol
	rcv.peersMu.RUnlock()
	if count > 0 {
		return count
	}
	if netRows > 0 {
		return netRows
	}
	return 8
}

func (rcv *Receiver) getCustodyPeersForRow(row int) []p2pcommon.PeerInfo {
	rcv.peersMu.RLock()
	cmap := rcv.rowCustodyMap
	netRows := rcv.storesPerCol
	colPeers := append([]p2pcommon.PeerInfo(nil), rcv.colPeers...)
	rcv.peersMu.RUnlock()
	if netRows <= 0 {
		netRows = 8
	}
	targetNetRow := row % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}
	if len(cmap) > 0 {
		if peers := cmap[targetNetRow]; len(peers) > 0 {
			var res []p2pcommon.PeerInfo
			for _, p := range peers {
				if p.PeerID != rcv.selfPeerID {
					res = append(res, p)
				}
			}
			return res
		}
	}
	var fallback []p2pcommon.PeerInfo
	backupRow := (targetNetRow + 1) % netRows
	for _, p := range colPeers {
		if p.PeerID == rcv.selfPeerID || p.Row < 0 {
			continue
		}
		pRow := p.Row % netRows
		if pRow == targetNetRow || pRow == backupRow {
			fallback = append(fallback, p)
		}
	}
	return fallback
}

func (rcv *Receiver) getNetColIdx() int {
	numCols := rcv.numCols
	if numCols <= 0 {
		numCols = 8
	}
	baseK := rcv.kBlock
	if baseK <= 0 {
		baseK = 16
	}
	baseColsPerNetCol := (2 * baseK) / numCols
	if baseColsPerNetCol <= 0 {
		baseColsPerNetCol = 1
	}
	netCol := rcv.colIdx
	if rcv.colIdx >= numCols || (baseColsPerNetCol > 1 && rcv.colIdx%baseColsPerNetCol == 0 && rcv.colIdx >= baseColsPerNetCol) {
		netCol = rcv.colIdx / baseColsPerNetCol
	}
	return netCol
}

func (rcv *Receiver) subscribeToTopicOnce(ctx context.Context, topicName string, logLabel string) {
	rcv.subscribedTopicsMu.Lock()
	if rcv.subscribedTopics == nil {
		rcv.subscribedTopics = make(map[string]bool)
	}
	if rcv.subscribedTopics[topicName] {
		rcv.subscribedTopicsMu.Unlock()
		return
	}
	rcv.subscribedTopics[topicName] = true
	rcv.subscribedTopicsMu.Unlock()

	topic, err := rcv.broadcaster.JoinTopic(topicName)
	if err != nil {
		log.Printf("[StoreNode] Failed to join topic %s: %v", topicName, err)
		return
	}
	sub, err := topic.Subscribe(pubsub.WithBufferSize(4096))
	if err != nil {
		log.Printf("[StoreNode] Failed to subscribe to topic %s: %v", topicName, err)
		return
	}
	log.Printf("[StoreNode] Subscribed to %s topic %s", logLabel, topicName)

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
			if len(data) >= 4 && binary.BigEndian.Uint32(data[:4]) == p2pcommon.BinarySeedMagic {
				batchReq, err := p2pcommon.DecodeBinaryBatchSeed(data)
				if err == nil && batchReq != nil && len(batchReq.Seeds) > 0 {
					go rcv.verifyAndProcessBinaryBatch(batchReq)
				}
				continue
			}
			if len(data) > 0 && data[0] == p2pcommon.BinaryGossipMagic {
				bgp, err := p2pcommon.DecodeBinaryGossipPiece(data)
				if err == nil && bgp.BlockID != "" {
					if !rcv.shouldDropGossipPiece(bgp.BlockID, int(bgp.Row), int(bgp.Col)) {
						rcv.enqueueBinaryGossipPiece(bgp)
					}
				}
				continue
			}
			var payload p2pcommon.SeedCellRequest
			if err := json.Unmarshal(data, &payload); err == nil && payload.BlockID != "" {
				if !rcv.shouldDropGossipPiece(payload.BlockID, payload.Row, payload.Col) {
					rcv.enqueueGossipPiece(payload)
				}
			}
		}
	}(sub)
}

// subscribeToRowChannels subscribes to:
// 1. All NetRows recode channels in this column: /cda/1.0.0/col/<netColIdx>/row/<0..netRows-1>/recode
//    (both Custody Nodes and Non-Custody Nodes receive recoded pieces here).
// 2. Assigned custody seed channels: /cda/1.0.0/col/<netColIdx>/row/<custodyRow>/seed
//    (only Custody Nodes assigned to custodyRow receive Bootstrap's 2*kPiece seed pool here).
func (rcv *Receiver) subscribeToRowChannels(ctx context.Context) {
	rcv.peersMu.RLock()
	netRows := rcv.storesPerCol
	rcv.peersMu.RUnlock()
	if netRows <= 0 {
		netRows = 8
	}
	netColIdx := rcv.getNetColIdx()
	for r := 0; r < netRows; r++ {
		recodeTopic := p2pcommon.TopicCellRowRecode(netColIdx, r)
		rcv.subscribeToTopicOnce(ctx, recodeTopic, fmt.Sprintf("network matrix recode channel (col=%d, row=%d)", netColIdx, r))
	}
	for _, custodyRow := range rcv.getCustodyNetRows() {
		seedTopic := p2pcommon.TopicCellRowSeed(netColIdx, custodyRow)
		rcv.subscribeToTopicOnce(ctx, seedTopic, fmt.Sprintf("network matrix seed channel (col=%d, row=%d)", netColIdx, custodyRow))
	}
}

func (rcv *Receiver) Start(ctx context.Context) {
	// Store context for use in SetPeers / SetRoutingState
	rcv.ctx = ctx

	// Start sharded worker pool (1 goroutine per workerChan)
	for i := 0; i < rcv.numWorkers; i++ {
		go rcv.workerLoop(ctx, rcv.workerChans[i])
	}
	// Start debounced completion check worker loop
	go rcv.completionCheckWorkerLoop(ctx)
	// Start single batched ticker loop for non-custody timeout fallback locks
	go rcv.nonCustodyFallbackLoop(ctx)

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

	// 1. Register per-node stream handlers
	nodeProto := p2pcommon.ProtoNodeSeed(rcv.selfPeerID)
	rcv.host.SetStreamHandler(protocol.ID(nodeProto), rcv.handleSeedStream)
	log.Printf("[StoreNode] Registered per-node seed handler on protocol %s", nodeProto)

	nodeBatchProto := p2pcommon.ProtoNodeBatchSeed(rcv.selfPeerID)
	rcv.host.SetStreamHandler(protocol.ID(nodeBatchProto), rcv.handleBatchSeedStream)
	log.Printf("[StoreNode] Registered per-node batch seed handler on protocol %s", nodeBatchProto)

	nodePersistentProto := p2pcommon.ProtoNodePersistentSeed(rcv.selfPeerID)
	rcv.host.SetStreamHandler(protocol.ID(nodePersistentProto), rcv.handlePersistentSeedStream)
	log.Printf("[StoreNode] Registered per-node persistent seed handler on protocol %s", nodePersistentProto)

	nodeBinaryPersistentProto := p2pcommon.ProtoNodeBinaryPersistentSeed(rcv.selfPeerID)
	rcv.host.SetStreamHandler(protocol.ID(nodeBinaryPersistentProto), rcv.handleBinaryPersistentSeedStream)
	log.Printf("[StoreNode] Registered per-node binary persistent seed handler on protocol %s", nodeBinaryPersistentProto)

	// Keep generic fetch handlers (used by light nodes and active-pull)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreFetch, rcv.handleFetchStream)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreGetPieces, rcv.handleFetchStream)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreBatchFetch, rcv.handleBatchFetchStream)

	// 2. Subscribe to all row-based Network Matrix channels in this column AND own node topic
	rcv.subscribeToRowChannels(ctx)
	rcv.subscribeToTopicOnce(ctx, p2pcommon.TopicNode(rcv.selfPeerID), "own node")

	// 3. Subscribe to column anchor topics
	n := 2 * rcv.kBlock
	numCols := rcv.numCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}
	netColIdx := rcv.getNetColIdx()
	startCol := netColIdx * colsPerNetCol
	endCol := startCol + colsPerNetCol

	for c := startCol; c < endCol; c++ {
		colTopicName := p2pcommon.TopicCol(c)
		topic, err := rcv.broadcaster.JoinTopic(colTopicName)
		if err != nil {
			log.Fatalf("Failed to join column anchor GossipSub topic %s: %v", colTopicName, err)
		}
		sub, err := topic.Subscribe(pubsub.WithBufferSize(1024))
		if err != nil {
			log.Fatalf("Failed to subscribe to column anchor GossipSub topic %s: %v", colTopicName, err)
		}
		go func(sub *pubsub.Subscription, colIndex int) {
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
					colKey := fmt.Sprintf("%s_col_%d", bID, colIndex)
					rcv.dispatchTask(colKey, func() { rcv.processAnchorMessage(data) })
				}
			}
		}(sub, c)
	}

	// 4. Subscribe to TopicByzantineReport to auto-isolate Byzantine nodes
	if rcv.broadcaster != nil {
		byzTopic, err := rcv.broadcaster.JoinTopic(p2pcommon.TopicByzantineReport)
		if err == nil {
			byzSub, errSub := byzTopic.Subscribe(pubsub.WithBufferSize(256))
			if errSub == nil {
				go func() {
					for {
						msg, err := byzSub.Next(ctx)
						if err != nil {
							return
						}
						var report p2pcommon.ByzantineNodeReport
						if err := json.Unmarshal(msg.Data, &report); err == nil && report.ReportedPeerID != "" {
							rcv.BlacklistPeer(report.ReportedPeerID, 30*time.Minute)
						}
					}
				}()
			}
		}
	}

	rcv.initialSyncOnce.Do(func() {
		go rcv.catchUpRecentBlocksOnJoin(ctx)
	})
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

// shouldDropGossipPiece checks if an incoming piece is for a block that is already complete,
// or for a cell that is already pruned or non-custody locked.
func (rcv *Receiver) shouldDropGossipPiece(blockID string, row, col int) bool {
	isCustody, isNativeRetention, _ := rcv.evalCustody(row)
	rcv.completedMu.Lock()
	if rcv.completedBlocks != nil && rcv.completedBlocks[blockID] {
		rcv.completedMu.Unlock()
		if !isNativeRetention || rcv.cache.GetPieceCount(blockID, row, col) >= rcv.kPiece {
			return true
		}
	} else {
		rcv.completedMu.Unlock()
	}

	cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)

	rcv.pruneMu.Lock()
	pruned := rcv.prunedCells[cellKey]
	rcv.pruneMu.Unlock()
	if pruned {
		return true
	}

	if !isCustody {
		rcv.nonCustodyLockedMu.Lock()
		locked := rcv.nonCustodyLocked[cellKey]
		rcv.nonCustodyLockedMu.Unlock()
		if locked {
			return true
		}
	} else if isNativeRetention && rcv.cache.GetPieceCount(blockID, row, col) >= rcv.kPiece {
		return true
	}
	return false
}

// enqueueBinaryGossipPiece enqueues a compact binary gossip piece directly to binaryGossipQueue
func (rcv *Receiver) enqueueBinaryGossipPiece(bgp *p2pcommon.BinaryGossipPiece) {
	recordGossipMessage()
	select {
	case rcv.binaryGossipQueue <- bgp:
	default:
		// Queue full fallback
		req := p2pcommon.SeedCellRequest{
			BlockID:      bgp.BlockID,
			Row:          int(bgp.Row),
			Col:          int(bgp.Col),
			Data:         hex.EncodeToString(bgp.Data),
			Coeffs:       hex.EncodeToString(bgp.Coeffs),
			Proof:        hex.EncodeToString(bgp.Proof),
			SenderPeerID: bgp.SenderPeerID,
		}
		cellKey := fmt.Sprintf("%s_%d_%d", req.BlockID, req.Row, req.Col)
		rcv.dispatchTask(cellKey, func() {
			rcv.processPiece(req.BlockID, req.Row, req.Col, req.Data, req.Coeffs, req.Proof, req.PieceCommits, req.SenderPeerID, true)
		})
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
	if len(data) > 0 && data[0] == p2pcommon.BinaryGossipMagic {
		bgp, err := p2pcommon.DecodeBinaryGossipPiece(data)
		if err == nil && bgp.BlockID != "" {
			if rcv.shouldDropGossipPiece(bgp.BlockID, int(bgp.Row), int(bgp.Col)) {
				return
			}
			rcv.enqueueBinaryGossipPiece(bgp)
		}
		return
	}
	var payload p2pcommon.SeedCellRequest
	if err := json.Unmarshal(data, &payload); err != nil {
		return
	}
	if rcv.shouldDropGossipPiece(payload.BlockID, payload.Row, payload.Col) {
		return
	}
	rcv.enqueueGossipPiece(payload)
}

func (rcv *Receiver) startGossipBatchWorkers(ctx context.Context, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go rcv.gossipBatchWorkerLoop(ctx)
		go rcv.binaryGossipBatchWorkerLoop(ctx)
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

func (rcv *Receiver) binaryGossipBatchWorkerLoop(ctx context.Context) {
	batchSize := getEnvInt("STORE_GOSSIP_BATCH_SIZE", 48)
	batch := make([]*p2pcommon.BinaryGossipPiece, 0, batchSize)
	tickerMs := getEnvInt("STORE_GOSSIP_BATCH_TICKER_MS", 10)
	ticker := time.NewTicker(time.Duration(tickerMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rcv.gossipBatchStop:
			return
		case item, ok := <-rcv.binaryGossipQueue:
			if !ok {
				return
			}
			batch = append(batch, item)
			if len(batch) >= batchSize {
				rcv.verifyAndProcessBinaryGossipBatch(batch)
				batch = make([]*p2pcommon.BinaryGossipPiece, 0, batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				rcv.verifyAndProcessBinaryGossipBatch(batch)
				batch = make([]*p2pcommon.BinaryGossipPiece, 0, batchSize)
			}
		}
	}
}

func (rcv *Receiver) verifyAndProcessBinaryGossipBatch(items []*p2pcommon.BinaryGossipPiece) {
	if len(items) == 0 {
		return
	}

	type candidateItem struct {
		blockID      string
		row, col     int
		piece        cda.ReceivedPiece
		coeffs       []byte
		senderPeerID string
		combinedComm []byte
	}

	var candidates []candidateItem
	var batchItems []cda.BatchVerifyItem
	var unanchored []*p2pcommon.BinaryGossipPiece

	for _, s := range items {
		row := int(s.Row)
		col := int(s.Col)
		cellKey := fmt.Sprintf("%s_%d_%d", s.BlockID, row, col)

		if rcv.shouldDropGossipPiece(s.BlockID, row, col) {
			continue
		}

		rcv.pruneMu.Lock()
		pruned := rcv.prunedCells[cellKey]
		rcv.pruneMu.Unlock()
		if pruned {
			continue
		}

		pieceCommits, exists := rcv.cache.GetAnchoredCommitments(s.BlockID, col)
		if !exists || len(pieceCommits) < rcv.kPiece {
			unanchored = append(unanchored, s)
			continue
		}

		pieceCommitsTyped := make([]cda.PieceCommitment, rcv.kPiece)
		for i := 0; i < rcv.kPiece; i++ {
			pieceCommitsTyped[i] = cda.PieceCommitment(pieceCommits[i])
		}

		combinedCommit, err := rcv.kzg.Combine(pieceCommitsTyped, s.Coeffs)
		if err != nil {
			continue
		}

		piece := cda.ReceivedPiece{
			Row: row,
			Col: col,
			Data: rlnc.PieceData{
				Data:   s.Data,
				Coeffs: s.Coeffs,
			},
			Proof: cda.OpeningProof(s.Proof),
		}

		batchItems = append(batchItems, cda.BatchVerifyItem{
			Commitment: combinedCommit,
			Row:        row,
			Data:       s.Data,
			Proof:      cda.OpeningProof(s.Proof),
		})

		candidates = append(candidates, candidateItem{
			blockID:      s.BlockID,
			row:          row,
			col:          col,
			piece:        piece,
			coeffs:       s.Coeffs,
			senderPeerID: s.SenderPeerID,
			combinedComm: combinedCommit,
		})
	}

	if len(batchItems) > 0 {
		blockCandidates := make(map[string][]candidateItem)
		blockBatchItems := make(map[string][]cda.BatchVerifyItem)
		for i, item := range candidates {
			blockCandidates[item.blockID] = append(blockCandidates[item.blockID], item)
			blockBatchItems[item.blockID] = append(blockBatchItems[item.blockID], batchItems[i])
		}

		for bID, bItems := range blockBatchItems {
			bCands := blockCandidates[bID]
			n := rcv.getBlockN(bID)
			if rcv.kzg.BatchVerifyForDomain(bItems, n) {
				for _, item := range bCands {
					_ = rcv.processVerifiedPiece(item.blockID, item.row, item.col, item.piece, item.coeffs, item.senderPeerID)
				}
			} else {
				for _, item := range bCands {
					if rcv.kzg.VerifyForDomain(item.combinedComm, item.row, item.piece.Data.Data, item.piece.Proof, n) {
						_ = rcv.processVerifiedPiece(item.blockID, item.row, item.col, item.piece, item.coeffs, item.senderPeerID)
					}
				}
			}
		}
	}

	if len(unanchored) > 0 {
		go func(retryItems []*p2pcommon.BinaryGossipPiece) {
			time.Sleep(15 * time.Millisecond)
			for _, it := range retryItems {
				select {
				case rcv.binaryGossipQueue <- it:
				default:
				}
			}
		}(unanchored)
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
	if header != nil && len(header.ColumnComm) > 0 {
		rcv.setBlockN(blockID, len(header.ColumnComm))
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
			graceMs := getEnvInt("STORE_FALLBACK_GRACE_MS", 300)
			time.Sleep(time.Duration(graceMs) * time.Millisecond)
			if rcv.IsComplete(bID) {
				return
			}
			retryIntervalMs := getEnvInt("STORE_FALLBACK_RETRY_INTERVAL_MS", 300)
			for attempt := 1; attempt <= 4; attempt++ {
				if rcv.IsComplete(bID) {
					return
				}
				rcv.fallbackPullMissingCells(bID, attempt)
				time.Sleep(time.Duration(retryIntervalMs) * time.Millisecond)
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

func (rcv *Receiver) handlePersistentSeedStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("[StoreNode] Persistent seed stream connected from %s", remotePeer)

	dec := json.NewDecoder(stream)
	enc := json.NewEncoder(stream)

	for {
		var payload p2pcommon.BatchSeedCellRequest
		if err := dec.Decode(&payload); err != nil {
			log.Printf("[StoreNode] Persistent seed stream from %s closed: %v", remotePeer, err)
			return
		}

		if len(payload.Seeds) > 0 {
			rcv.verifyAndProcessBatch(payload.Seeds)
		}

		if err := enc.Encode(struct {
			Success bool `json:"success"`
		}{Success: true}); err != nil {
			log.Printf("[StoreNode] Failed to write ACK on persistent seed stream to %s: %v", remotePeer, err)
			return
		}
	}
}

func (rcv *Receiver) handleBinaryPersistentSeedStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	log.Printf("[StoreNode] Binary persistent seed stream connected from %s", remotePeer)

	for {
		payload, err := p2pcommon.ReadBinaryBatchSeed(stream)
		if err != nil {
			log.Printf("[StoreNode] Binary persistent seed stream from %s closed: %v", remotePeer, err)
			return
		}

		if len(payload.Seeds) > 0 {
			rcv.verifyAndProcessBinaryBatch(payload)
		}

		ack := &p2pcommon.BinaryBatchSeedResponse{
			Success: true,
		}
		if err := p2pcommon.WriteBinaryBatchResponse(stream, ack); err != nil {
			log.Printf("[StoreNode] Failed to write ACK on binary persistent seed stream to %s: %v", remotePeer, err)
			return
		}
	}
}

func (rcv *Receiver) verifyAndProcessBinaryBatch(payload *p2pcommon.BinaryBatchSeedRequest) {
	if payload == nil || len(payload.Seeds) == 0 {
		return
	}

	blockID := payload.BlockID
	colIdx := int(payload.ColIdx)

	pieceCommits, exists := rcv.cache.GetAnchoredCommitments(blockID, colIdx)
	if !exists && len(payload.PieceCommits) > 0 {
		pieceCommits = payload.PieceCommits
		rcv.cache.AnchorCommitments(blockID, colIdx, pieceCommits)
	}

	if len(pieceCommits) < rcv.kPiece {
		return
	}

	pieceCommitsTyped := make([]cda.PieceCommitment, rcv.kPiece)
	for i := 0; i < rcv.kPiece; i++ {
		pieceCommitsTyped[i] = cda.PieceCommitment(pieceCommits[i])
	}

	type cellCoord struct {
		row, col int
	}
	seedsByCell := make(map[cellCoord][]p2pcommon.BinarySeedPiece)
	var cellOrder []cellCoord
	maxCoord := int(payload.ColIdx)
	for _, s := range payload.Seeds {
		if int(s.Row) > maxCoord {
			maxCoord = int(s.Row)
		}
		if int(s.Col) > maxCoord {
			maxCoord = int(s.Col)
		}
		cc := cellCoord{row: int(s.Row), col: int(s.Col)}
		if _, seen := seedsByCell[cc]; !seen {
			cellOrder = append(cellOrder, cc)
		}
		seedsByCell[cc] = append(seedsByCell[cc], s)
	}
	if maxCoord >= 8 {
		inferredN := 16
		for inferredN <= maxCoord {
			inferredN *= 2
		}
		rcv.setBlockN(blockID, inferredN)
	}

	var sampledSeeds []p2pcommon.BinarySeedPiece
	for _, cc := range cellOrder {
		cellSeeds := seedsByCell[cc]
		isCustodyCell, _, numCustodyPeers := rcv.evalCustody(cc.row)
		if !isCustodyCell {
			// TopicCellRowSeed is exclusively for Custody Nodes assigned to this NetRow
			continue
		}
		_, seedsPerPeer, _ := p2pcommon.ComputeSeedingPlan(numCustodyPeers, rcv.kPiece)
		if len(cellSeeds) > seedsPerPeer {
			// Per-node randomized sub-sampling so each Custody Node in the cell
			// picks an independent random subset of size seedsPerPeer
			shuffled := make([]p2pcommon.BinarySeedPiece, len(cellSeeds))
			copy(shuffled, cellSeeds)
			h := fnv.New64a()
			_, _ = h.Write([]byte(rcv.selfPeerID))
			_, _ = h.Write([]byte(blockID))
			var rcBuf [16]byte
			binary.LittleEndian.PutUint32(rcBuf[0:4], uint32(cc.row))
			binary.LittleEndian.PutUint32(rcBuf[4:8], uint32(cc.col))
			binary.LittleEndian.PutUint64(rcBuf[8:16], uint64(time.Now().UnixNano()))
			_, _ = h.Write(rcBuf[:])
			rng := rand.New(rand.NewSource(int64(h.Sum64())))
			rng.Shuffle(len(shuffled), func(i, j int) {
				shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			})
			cellSeeds = shuffled[:seedsPerPeer]
		}
		sampledSeeds = append(sampledSeeds, cellSeeds...)
	}

	type candidateItem struct {
		row, col int
		piece    cda.ReceivedPiece
		coeffs   []byte
	}

	var candidates []candidateItem
	var batchItems []cda.BatchVerifyItem

	for _, s := range sampledSeeds {
		row := int(s.Row)
		col := int(s.Col)
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)

		isCustodyEarly, isNativeEarly, _ := rcv.evalCustody(row)

		rcv.completedMu.Lock()
		isBlockCompleted := rcv.completedBlocks != nil && rcv.completedBlocks[blockID]
		rcv.completedMu.Unlock()
		if isBlockCompleted {
			if !isNativeEarly || rcv.cache.GetPieceCount(blockID, row, col) >= rcv.kPiece {
				continue
			}
		}
		if !isCustodyEarly {
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

		combinedCommit, err := rcv.kzg.Combine(pieceCommitsTyped, s.Coeffs)
		if err != nil {
			continue
		}

		piece := cda.ReceivedPiece{
			Row: row,
			Col: col,
			Data: rlnc.PieceData{
				Data:   s.Data,
				Coeffs: s.Coeffs,
			},
			Proof: cda.OpeningProof(s.Proof),
		}

		batchItems = append(batchItems, cda.BatchVerifyItem{
			Commitment: combinedCommit,
			Row:        row,
			Data:       s.Data,
			Proof:      cda.OpeningProof(s.Proof),
		})

		candidates = append(candidates, candidateItem{
			row:    row,
			col:    col,
			piece:  piece,
			coeffs: s.Coeffs,
		})
	}

	if len(batchItems) > 0 {
		n := rcv.getBlockN(blockID)
		if rcv.kzg.BatchVerifyForDomain(batchItems, n) {
			for _, item := range candidates {
				_ = rcv.processVerifiedPiece(blockID, item.row, item.col, item.piece, item.coeffs, payload.SenderPeerID)
			}
		} else {
			for _, item := range candidates {
				combinedCommit, _ := rcv.kzg.Combine(pieceCommitsTyped, item.coeffs)
				if rcv.kzg.VerifyForDomain(combinedCommit, item.row, item.piece.Data.Data, item.piece.Proof, n) {
					_ = rcv.processVerifiedPiece(blockID, item.row, item.col, item.piece, item.coeffs, payload.SenderPeerID)
				}
			}
		}
	}
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
		isCustodyEarly, isNativeEarly, _ := rcv.evalCustody(s.Row)

		rcv.completedMu.Lock()
		isBlockCompleted := rcv.completedBlocks != nil && rcv.completedBlocks[s.BlockID]
		rcv.completedMu.Unlock()
		if isBlockCompleted {
			if !isNativeEarly || rcv.cache.GetPieceCount(s.BlockID, s.Row, s.Col) >= rcv.kPiece {
				continue
			}
		}
		if !isCustodyEarly {
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
		blockCandidates := make(map[string][]candidateItem)
		blockBatchItems := make(map[string][]cda.BatchVerifyItem)
		for i, item := range candidates {
			blockCandidates[item.blockID] = append(blockCandidates[item.blockID], item)
			blockBatchItems[item.blockID] = append(blockBatchItems[item.blockID], batchItems[i])
		}

		for bID, bItems := range blockBatchItems {
			bCands := blockCandidates[bID]
			n := rcv.getBlockN(bID)
			if rcv.kzg.BatchVerifyForDomain(bItems, n) {
				for _, item := range bCands {
					_ = rcv.processVerifiedPiece(item.blockID, item.row, item.col, item.piece, item.decodedCoeffs, item.senderPeerID)
				}
			} else {
				// Fallback: verify individually to isolate any corrupted piece
				for _, item := range bCands {
					if rcv.kzg.VerifyForDomain(item.combinedComm, item.row, item.piece.Data.Data, item.piece.Proof, n) {
						_ = rcv.processVerifiedPiece(item.blockID, item.row, item.col, item.piece, item.decodedCoeffs, item.senderPeerID)
					}
				}
			}
		}
	}
}

func (rcv *Receiver) processPiece(blockID string, row, col int, dataStr, coeffsStr, proofStr string, pieceCommitsStr []string, senderPeerID string, isGossip bool) error {
	cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)

	isCustodyEarly, isNativeEarly, _ := rcv.evalCustody(row)

	// Early exit 1: drop incoming pieces if this block is already completed and locked
	rcv.completedMu.Lock()
	isBlockCompleted := rcv.completedBlocks != nil && rcv.completedBlocks[blockID]
	rcv.completedMu.Unlock()
	if isBlockCompleted {
		if !isNativeEarly || rcv.cache.GetPieceCount(blockID, row, col) >= rcv.kPiece {
			return nil
		}
	}
	if !isCustodyEarly {
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
		h, err := verifier.FetchBlockHeader(rcv.publisherAddr, blockID)
		if err != nil {
			return fmt.Errorf("failed to fetch header: %w", err)
		}
		if h != nil && len(h.ColumnComm) > 0 {
			rcv.setBlockN(blockID, len(h.ColumnComm))
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

	n := rcv.getBlockN(blockID)
	if !rcv.kzg.VerifyForDomain(combinedCommit, rowIdx, piece.Data.Data, combinedProof, n) {
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

	isCustody, isNativeRetention, numCustodyPeers := rcv.evalCustody(row)

	// Non-custody nodes: gather >=2 pieces from >=2 sources on TopicCellRowRecode, recode into 1 piece, and lock
	// (fallback to storing 1 piece directly if <2 sources/pieces arrive within wait window).
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
		if numCustodyPeers > 0 && numCustodyPeers < minSources {
			minSources = numCustodyPeers
		}

		rcv.contribMu.Lock()
		numSources := len(rcv.peerContributions[cellKey])
		rcv.contribMu.Unlock()

		fastLock := getEnvBool("STORE_FAST_LOCK_NON_CUSTODY", false)
		updatedPieces := rcv.cache.GetPieces(blockID, row, col)
		canLock := false
		if fastLock && len(updatedPieces) >= 1 {
			canLock = true
		} else if len(updatedPieces) >= minPieces && numSources >= minSources {
			canLock = true
		}

		if canLock {
			rcv.nonCustodyLockedMu.Lock()
			if rcv.nonCustodyLocked[cellKey] {
				rcv.nonCustodyLockedMu.Unlock()
				rcv.cellMu.Unlock()
				return nil
			}
			rcv.nonCustodyLocked[cellKey] = true
			delete(rcv.nonCustodyPending, cellKey)
			rcv.nonCustodyLockedMu.Unlock()

			piecesSnapshot := make([]cda.ReceivedPiece, len(updatedPieces))
			copy(piecesSnapshot, updatedPieces)
			rcv.cellMu.Unlock()

			var finalPiece *cda.ReceivedPiece
			if len(piecesSnapshot) >= minPieces && numSources >= minSources {
				pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
				if len(pieceCommits) > 0 {
					n := rcv.getBlockN(blockID)
					if recVer, vErr := rcv.rm.RecodePiecesWithVerify(piecesSnapshot[:minPieces], pieceCommits, 5, n); vErr == nil && recVer != nil {
						finalPiece = recVer
					}
				}
				if finalPiece == nil {
					recoded, err := rcv.rm.RecodePieces(piecesSnapshot[:minPieces])
					if err == nil && recoded != nil {
						finalPiece = recoded
					} else {
						finalPiece = &piecesSnapshot[0]
					}
				}
			} else {
				finalPiece = &piecesSnapshot[0]
			}

			rcv.cache.StoreRecodedPiece(blockID, row, col, *finalPiece)
			rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)

			rcv.pruneMu.Lock()
			rcv.prunedCells[cellKey] = true
			rcv.pruneMu.Unlock()

			p2pcommon.LogDebug("[StoreNode] Non-custody: cell [%d, %d] recoded/locked from %d piece(s) across %d sources; raw pieces pruned.", row, col, len(piecesSnapshot), numSources)
			rcv.CheckAndLogCompletion(blockID)
			return nil
		}

		if len(updatedPieces) == 1 {
			rcv.nonCustodyLockedMu.Lock()
			if !rcv.nonCustodyLocked[cellKey] {
				if _, exists := rcv.nonCustodyPending[cellKey]; !exists {
					rcv.nonCustodyPending[cellKey] = nonCustodyPendingCell{
						blockID:   blockID,
						row:       row,
						col:       col,
						firstSeen: time.Now(),
					}
				}
			}
			rcv.nonCustodyLockedMu.Unlock()
		}

		rcv.cellMu.Unlock()
		return nil
	}

	// 4. Rank Filtering (Gaussian Elimination) for Custody cells
	existingPieces := rcv.cache.GetPieces(blockID, row, col)
	if isNativeRetention && len(existingPieces) >= rcv.kPiece {
		// Already at full rank for native custody cell
		rcv.cellMu.Unlock()
		return nil
	}
	if !isNativeRetention {
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
		rcv.pruneMu.Lock()
		alreadyPruned := rcv.prunedCells[cellKey]
		rcv.pruneMu.Unlock()
		if alreadyPruned {
			rcv.cellMu.Unlock()
			return nil
		}
	}
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

	// 5b. Track peer contributions for this cell
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

	// 6. P2P Recoding & Row-channel GossipSub forwarding
	updatedPieces := rcv.cache.GetPieces(blockID, row, col)
	rcv.cellMu.Unlock()

	if isCustody {
		_, seedsPerPeer, broadcastQuota := p2pcommon.ComputeSeedingPlan(numCustodyPeers, rcv.kPiece)
		minRecodePieces := seedsPerPeer
		if minRecodePieces > rcv.kPiece {
			minRecodePieces = rcv.kPiece
		}
		if minRecodePieces < 2 && rcv.kPiece >= 2 {
			minRecodePieces = 2
		}

		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
		rcv.broadcastMu.Lock()
		alreadyBroadcast := rcv.cellBroadcastCount[cellKey]
		disseminating := rcv.cellDisseminating[cellKey]
		shouldDisseminate := !disseminating && alreadyBroadcast == 0 &&
			(len(updatedPieces) >= minRecodePieces || len(updatedPieces) >= rcv.kPiece)
		if shouldDisseminate {
			rcv.cellDisseminating[cellKey] = true
		}
		rcv.broadcastMu.Unlock()

		if shouldDisseminate {
			go func(bID string, r, c int, pieces []cda.ReceivedPiece, nativeRetention bool, quota int) {
				rcv.disseminationSem <- struct{}{}
				defer func() { <-rcv.disseminationSem }()

				numBroadcast := getEnvInt("STORE_BROADCAST_ROUNDS", quota)
				if numBroadcast < 1 {
					numBroadcast = 1
				}
				log.Printf("[GossipSub] Cell [%d, %d] rank %d/%d (NativeRetention=%v, M=%d). Broadcasting %d recoded pieces on recode channel...",
					r, c, len(pieces), rcv.kPiece, nativeRetention, numCustodyPeers, numBroadcast)

				// Re-fetch latest pieces in case remaining seeds from the batch arrived microseconds after trigger
				rcv.cellMu.Lock()
				latestCellPieces := rcv.cache.GetPieces(bID, r, c)
				rcv.cellMu.Unlock()
				targetPieces := pieces
				if len(latestCellPieces) > len(targetPieces) {
					targetPieces = latestCellPieces
				}
				if len(targetPieces) > rcv.kPiece {
					targetPieces = targetPieces[:rcv.kPiece]
				}
				n := rcv.getBlockN(bID)
				pieceCommits, _ := rcv.cache.GetAnchoredCommitments(bID, c)
				broadcastSucceeded := 0
				for i := 0; i < numBroadcast; i++ {
					var recodedPiece *cda.ReceivedPiece
					var err error
					if len(pieceCommits) > 0 {
						recodedPiece, err = rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5, n)
					}
					if recodedPiece == nil {
						recodedPiece, err = rcv.rm.RecodePieces(targetPieces)
					}
					if err != nil || recodedPiece == nil {
						log.Printf("[GossipSub] Failed to recode pieces for cell [%d, %d] (round %d/%d): %v", r, c, i+1, numBroadcast, err)
						continue
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
						p2pcommon.LogDebug("[GossipSub] Successfully published recoded piece for cell [%d, %d] (NativeRetention=%v, round %d/%d).", r, c, nativeRetention, i+1, numBroadcast)
						cKey := fmt.Sprintf("%s_%d_%d", bID, r, c)
						rcv.broadcastMu.Lock()
						rcv.cellBroadcastCount[cKey]++
						rcv.broadcastMu.Unlock()
						rcv.CheckAndLogCompletion(bID)
					}
				}

				if broadcastSucceeded == 0 {
					cKey := fmt.Sprintf("%s_%d_%d", bID, r, c)
					rcv.broadcastMu.Lock()
					delete(rcv.cellDisseminating, cKey)
					rcv.broadcastMu.Unlock()
				}

				if !nativeRetention && broadcastSucceeded > 0 {
					// Generate 1 fresh independent recoded piece for local retention before pruning raw pieces
					var retainedPiece *cda.ReceivedPiece
					if len(pieceCommits) > 0 {
						retainedPiece, _ = rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5, n)
					}
					if retainedPiece == nil {
						retainedPiece, _ = rcv.rm.RecodePieces(targetPieces)
					}
					if retainedPiece != nil {
						rcv.cache.StoreRecodedPiece(bID, r, c, *retainedPiece)
					}
					// Cross-row borrowed backup custody node: immediately prune raw pieces down to 1 recoded piece
					// once it has completed broadcasting its recoded quota onto TopicCellRowRecode.
					cKey := fmt.Sprintf("%s_%d_%d", bID, r, c)
					rcv.pruneMu.Lock()
					rcv.prunedCells[cKey] = true
					rcv.pruneMu.Unlock()
					rcv.cache.PruneRawPieces(bID, r, c, rcv.rm)
					p2pcommon.LogDebug("[StoreNode] Borrowed Backup: cell [%d, %d] dissemination finished (%d/%d rounds); raw pieces pruned immediately.", r, c, broadcastSucceeded, numBroadcast)
					rcv.CheckAndLogCompletion(bID)
				}
			}(blockID, row, col, updatedPieces, isNativeRetention, broadcastQuota)
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
				pullDelayMs := getEnvInt("STORE_ACTIVE_PULL_DELAY_MS", 200)
				if sec := getEnvInt("STORE_ACTIVE_PULL_DELAY_SEC", 0); sec > 0 {
					pullDelayMs = sec * 1000
				}
				pullDelay := time.Duration(pullDelayMs) * time.Millisecond
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

// subscribeToNonCustodyCells ensures subscriptions to row channels and any legacy per-peer topics.
func (rcv *Receiver) subscribeToNonCustodyCells(ctx context.Context, colPeers []p2pcommon.PeerInfo) {
	rcv.subscribeToRowChannels(ctx)
}

func (rcv *Receiver) fallbackPullMissingCells(blockID string, attempt int) {
	if rcv.IsComplete(blockID) {
		return
	}

	n := rcv.getBlockN(blockID)
	numCols := rcv.numCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}

	netColIdx := rcv.getNetColIdx()
	startCol := netColIdx * colsPerNetCol
	endCol := startCol + colsPerNetCol
	if endCol > n {
		endCol = n
	}

	rcv.peersMu.RLock()
	peersCopy := make([]p2pcommon.PeerInfo, len(rcv.colPeers))
	copy(peersCopy, rcv.colPeers)
	rcv.peersMu.RUnlock()

	if len(peersCopy) == 0 {
		return
	}

	peerRequests := make(map[string][]p2pcommon.CellCoord)
	peerInfoMap := make(map[string]p2pcommon.PeerInfo)

	for c := startCol; c < endCol; c++ {
		for r := 0; r < n; r++ {
			isCustody, isNativeRetention, _ := rcv.evalCustody(r)
			cellKey := fmt.Sprintf("%s_%d_%d", blockID, r, c)
			rowCustodyPeers := rcv.getCustodyPeersForRow(r)

			if isCustody && isNativeRetention {
				rcv.broadcastMu.Lock()
				bCount := rcv.cellBroadcastCount[cellKey]
				rcv.broadcastMu.Unlock()

				pCount := rcv.cache.GetPieceCount(blockID, r, c)
				if pCount < rcv.kPiece || bCount < 1 {
					for _, cp := range rowCustodyPeers {
						peerRequests[cp.PeerID] = append(peerRequests[cp.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
						peerInfoMap[cp.PeerID] = cp
					}
					if attempt >= 2 {
						for _, p := range peersCopy {
							if p.PeerID != rcv.selfPeerID && p.Row >= 0 {
								peerRequests[p.PeerID] = append(peerRequests[p.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
								peerInfoMap[p.PeerID] = p
							}
						}
					}
				}
			} else if isCustody && !isNativeRetention {
				rcv.broadcastMu.Lock()
				bCount := rcv.cellBroadcastCount[cellKey]
				rcv.broadcastMu.Unlock()

				rcv.pruneMu.Lock()
				pruned := rcv.prunedCells[cellKey]
				rcv.pruneMu.Unlock()

				pCount := rcv.cache.GetPieceCount(blockID, r, c)
				if bCount < 1 && !pruned && pCount < rcv.kPiece {
					for _, cp := range rowCustodyPeers {
						peerRequests[cp.PeerID] = append(peerRequests[cp.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
						peerInfoMap[cp.PeerID] = cp
					}
					if attempt >= 2 {
						for _, p := range peersCopy {
							if p.PeerID != rcv.selfPeerID && p.Row >= 0 {
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
					for _, cp := range rowCustodyPeers {
						peerRequests[cp.PeerID] = append(peerRequests[cp.PeerID], p2pcommon.CellCoord{Row: r, Col: c})
						peerInfoMap[cp.PeerID] = cp
					}
					if attempt >= 2 {
						for _, p := range peersCopy {
							if p.PeerID != rcv.selfPeerID && p.Row >= 0 {
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

	for key, pList := range cellsMap {
		if len(pList) == 0 {
			continue
		}
		var row, col int
		if _, err := fmt.Sscanf(key, "%d_%d", &row, &col); err != nil {
			row = pList[0].Row
			col = pList[0].Col
		}

		isCustody, isNativeRetention, numCustodyPeers := rcv.evalCustody(row)
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)

		if isCustody && isNativeRetention {
			rcv.cellMu.Lock()
			latestPieces := rcv.cache.GetPieces(blockID, row, col)
			for _, pPayload := range pList {
				if len(latestPieces) >= rcv.kPiece {
					break
				}
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
					_, _, numBroadcast := p2pcommon.ComputeSeedingPlan(numCustodyPeers, rcv.kPiece)
					if numBroadcast < 1 {
						numBroadcast = 1
					}
					pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
					rcv.cellMu.Lock()
					allPieces := rcv.cache.GetPieces(blockID, row, col)
					rcv.cellMu.Unlock()
					if len(allPieces) >= rcv.kPiece {
						n := rcv.getBlockN(blockID)
						targetPieces := allPieces[:rcv.kPiece]
						for i := 0; i < numBroadcast; i++ {
							recodedPiece, err := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5, n)
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
			}
		} else if isCustody && !isNativeRetention {
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
				_, _, numBroadcast := p2pcommon.ComputeSeedingPlan(numCustodyPeers, rcv.kPiece)
				if numBroadcast < 1 {
					numBroadcast = 1
				}
				pieceCommits, _ := rcv.cache.GetAnchoredCommitments(blockID, col)
				rcv.cellMu.Lock()
				allPieces := rcv.cache.GetPieces(blockID, row, col)
				rcv.cellMu.Unlock()
				if len(allPieces) >= rcv.kPiece {
					n := rcv.getBlockN(blockID)
					targetPieces := allPieces[:rcv.kPiece]
					for i := 0; i < numBroadcast; i++ {
						recodedPiece, err := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5, n)
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
			n := rcv.getBlockN(blockID)

			if len(latestPieces) >= minPieces && numSources >= minSources {
				recoded, err := rcv.rm.RecodePiecesWithVerify(latestPieces[:minPieces], pieceCommits, 5, n)
				if err == nil && recoded != nil {
					finalPiece = recoded
				} else {
					finalPiece = &latestPieces[0]
				}
			} else if len(latestPieces) >= 1 {
				if len(latestPieces) >= 2 {
					recoded, err := rcv.rm.RecodePiecesWithVerify(latestPieces[:2], pieceCommits, 5, n)
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

	// Prioritize assigned Custody Nodes for this row
	custodySet := make(map[string]bool)
	for _, cp := range rcv.getCustodyPeersForRow(row) {
		custodySet[cp.PeerID] = true
	}
	sort.SliceStable(peersCopy, func(i, j int) bool {
		ci := custodySet[peersCopy[i].PeerID]
		cj := custodySet[peersCopy[j].PeerID]
		if ci != cj {
			return ci
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
		n := rcv.getBlockN(blockID)
		isPullCustody, isPullNativeRetention, numCustodyPeers := rcv.evalCustody(row)

		targetPieces := allPieces
		if len(targetPieces) > rcv.kPiece {
			targetPieces = targetPieces[:rcv.kPiece]
		}

		firstRecoded, err := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5, n)
		if err == nil {
			if !isPullNativeRetention {
				rcv.cellMu.Lock()
				rcv.cache.StoreRecodedPiece(blockID, row, col, *firstRecoded)
				rcv.cellMu.Unlock()
			}
			log.Printf("[StoreNode] Successfully recoded cell [%d, %d] in active pull.", row, col)
			rcv.CheckAndLogCompletion(blockID)

			if isPullCustody {
				_, _, numBroadcast := p2pcommon.ComputeSeedingPlan(numCustodyPeers, rcv.kPiece)
				if numBroadcast < 1 {
					numBroadcast = 1
				}
				broadcastSucceeded := 0
				for i := 0; i < numBroadcast; i++ {
					recodedPiece, rerr := rcv.rm.RecodePiecesWithVerify(targetPieces, pieceCommits, 5, n)
					if rerr != nil {
						log.Printf("[GossipSub] Active pull: recode round %d/%d failed for cell [%d, %d]: %v", i+1, numBroadcast, row, col, rerr)
						continue
					}
					if berr := rcv.broadcaster.BroadcastRecodedPiece(blockID, row, col, recodedPiece, pieceCommits); berr != nil {
						log.Printf("[GossipSub] Active pull: broadcast round %d/%d failed for cell [%d, %d] (Native=%v): %v", i+1, numBroadcast, row, col, isPullNativeRetention, berr)
					} else {
						broadcastSucceeded++
						log.Printf("[GossipSub] Active pull: broadcast round %d/%d succeeded for cell [%d, %d] (Native=%v).", i+1, numBroadcast, row, col, isPullNativeRetention)
						cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
						rcv.broadcastMu.Lock()
						rcv.cellBroadcastCount[cellKey]++
						rcv.broadcastMu.Unlock()
						rcv.CheckAndLogCompletion(blockID)
					}
				}

				if !isPullNativeRetention {
					rcv.cellMu.Lock()
					rcv.cache.PruneRawPieces(blockID, row, col, rcv.rm)
					rcv.cellMu.Unlock()
					cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
					rcv.pruneMu.Lock()
					rcv.prunedCells[cellKey] = true
					rcv.pruneMu.Unlock()
					log.Printf("[StoreNode] Borrowed Backup: active pull cell [%d, %d] dissemination finished (%d/%d rounds); raw pieces pruned.", row, col, broadcastSucceeded, numBroadcast)
				}
			} else {
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

	custodyPeers := rcv.getCustodyPeersForRow(row)
	if len(custodyPeers) == 0 {
		return
	}

	var pulledPieces []cda.ReceivedPiece
	for _, pInfo := range custodyPeers {
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
	n := rcv.getBlockN(blockID)
	var finalPiece *cda.ReceivedPiece
	if len(pulledPieces) >= 2 {
		recoded, err := rcv.rm.RecodePiecesWithVerify(pulledPieces[:2], pieceCommits, 5, n)
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
	if rcv.IsPeerBlacklisted(remotePeer.String()) {
		log.Printf("[StoreNode] Rejected fetch request from blacklisted peer %s", remotePeer)
		rcv.respondWithError(stream, "peer is blacklisted")
		return
	}
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

		// Prioritize honest custody peers for req.Row % effNetRows, then remaining column peers
		var prioritizedPeers []p2pcommon.PeerInfo
		seenPID := make(map[string]bool)
		cmap := rcv.rowCustodyMap
		if cmap != nil {
			effNetRows := rcv.kBlock * 2
			if rcv.numCols > 0 {
				effNetRows = (rcv.kBlock * 2) / rcv.numCols
			}
			if effNetRows <= 0 {
				effNetRows = 8
			}
			custodyPeers := cmap[req.Row%effNetRows]
			for _, cp := range custodyPeers {
				if cp.PeerID != "" && cp.PeerID != rcv.selfPeerID && !rcv.IsPeerBlacklisted(cp.PeerID) && !seenPID[cp.PeerID] {
					seenPID[cp.PeerID] = true
					prioritizedPeers = append(prioritizedPeers, cp)
				}
			}
		}
		for _, p := range peersCopy {
			if p.PeerID != "" && p.PeerID != rcv.selfPeerID && !rcv.IsPeerBlacklisted(p.PeerID) && !seenPID[p.PeerID] {
				seenPID[p.PeerID] = true
				prioritizedPeers = append(prioritizedPeers, p)
			}
		}

		for _, pInfo := range prioritizedPeers {
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

func (rcv *Receiver) setBlockN(blockID string, n int) {
	if n <= 0 {
		return
	}
	rcv.blockNMu.Lock()
	if rcv.blockN == nil {
		rcv.blockN = make(map[string]int)
	}
	if n > rcv.blockN[blockID] {
		rcv.blockN[blockID] = n
	}
	rcv.blockNMu.Unlock()
}

func (rcv *Receiver) getBlockN(blockID string) int {
	rcv.blockNMu.RLock()
	if n, ok := rcv.blockN[blockID]; ok && n > 0 {
		rcv.blockNMu.RUnlock()
		return n
	}
	rcv.blockNMu.RUnlock()

	rcv.blockNFetchMu.Lock()
	defer rcv.blockNFetchMu.Unlock()

	rcv.blockNMu.RLock()
	if n, ok := rcv.blockN[blockID]; ok && n > 0 {
		rcv.blockNMu.RUnlock()
		return n
	}
	rcv.blockNMu.RUnlock()

	for attempt := 0; attempt < 3; attempt++ {
		header, err := verifier.FetchBlockHeader(rcv.publisherAddr, blockID)
		if err == nil && header != nil && len(header.ColumnComm) > 0 {
			n := len(header.ColumnComm)
			rcv.setBlockN(blockID, n)
			return n
		}
		time.Sleep(15 * time.Millisecond)
	}

	return 2 * rcv.kBlock
}

func (rcv *Receiver) DiagnoseCompletion(blockID string) (bool, map[string]interface{}) {
	n := rcv.getBlockN(blockID)
	numCols := rcv.numCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}

	netColIdx := rcv.getNetColIdx()
	startCol := netColIdx * colsPerNetCol
	endCol := startCol + colsPerNetCol
	if endCol > n {
		endCol = n
	}

	minBroadcast := 1

	rcv.broadcastMu.Lock()
	defer rcv.broadcastMu.Unlock()

	var unmetReasons []string

	for c := startCol; c < endCol; c++ {
		for r := 0; r < n; r++ {
			isCustody, isNativeRetention, _ := rcv.evalCustody(r)
			cellKey := fmt.Sprintf("%s_%d_%d", blockID, r, c)
			if isCustody && isNativeRetention {
				// Native custody node (including all M nodes in a multi-node cell):
				// must retain >= kPiece raw pieces for long-term storage AND have disseminated >= 1 recoded piece
				pCount := rcv.cache.GetPieceCount(blockID, r, c)
				bCount := rcv.cellBroadcastCount[cellKey]
				if pCount < rcv.kPiece {
					unmetReasons = append(unmetReasons, fmt.Sprintf("custody [%d,%d]: pieces=%d < %d", r, c, pCount, rcv.kPiece))
				}
				if bCount < minBroadcast {
					unmetReasons = append(unmetReasons, fmt.Sprintf("custody [%d,%d]: broadcast=%d < %d", r, c, bCount, minBroadcast))
				}
			} else if isCustody && !isNativeRetention {
				// Borrowed backup custody node: must have >= 1 piece stored AND (disseminated >= minBroadcast OR pruned down to 1 piece)
				bCount := rcv.cellBroadcastCount[cellKey]
				rcv.pruneMu.Lock()
				pruned := rcv.prunedCells[cellKey]
				rcv.pruneMu.Unlock()
				total := rcv.cache.GetTotalPiecesForCell(blockID, r, c)
				if total == 0 || (bCount < minBroadcast && (!pruned || total != 1)) {
					unmetReasons = append(unmetReasons, fmt.Sprintf("backup [%d,%d]: broadcast=%d < %d, pruned=%v, pieces=%d", r, c, bCount, minBroadcast, pruned, total))
				}
			} else {
				// Non-custody: must store exactly 1 piece and be locked!
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
	tickerMs := getEnvInt("STORE_COMPLETION_CHECK_TICKER_MS", 15)
	ticker := time.NewTicker(time.Duration(tickerMs) * time.Millisecond)
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

// nonCustodyFallbackLoop sweeps pending non-custody cells that have waited >= STORE_NON_CUSTODY_RECODE_WAIT_MS
// in a single background ticker, eliminating per-cell timer goroutine storms on large blocks (e.g. K=64).
func (rcv *Receiver) nonCustodyFallbackLoop(ctx context.Context) {
	waitMs := getEnvInt("STORE_NON_CUSTODY_RECODE_WAIT_MS", 100)
	waitDur := time.Duration(waitMs) * time.Millisecond
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	minPieces := 2
	if rcv.kPiece < minPieces {
		minPieces = rcv.kPiece
	}

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			type expiredEntry struct {
				key  string
				cell nonCustodyPendingCell
			}
			var expired []expiredEntry

			rcv.nonCustodyLockedMu.Lock()
			for k, item := range rcv.nonCustodyPending {
				if rcv.nonCustodyLocked[k] {
					delete(rcv.nonCustodyPending, k)
					continue
				}
				if now.Sub(item.firstSeen) >= waitDur {
					rcv.nonCustodyLocked[k] = true
					delete(rcv.nonCustodyPending, k)
					expired = append(expired, expiredEntry{key: k, cell: item})
				}
			}
			rcv.nonCustodyLockedMu.Unlock()

			if len(expired) == 0 {
				continue
			}

			touchedBlocks := make(map[string]bool)
			for _, exp := range expired {
				bID := exp.cell.blockID
				r := exp.cell.row
				c := exp.cell.col
				cKey := exp.key

				currPieces := rcv.cache.GetPieces(bID, r, c)
				if len(currPieces) == 0 {
					continue
				}

				var chosen *cda.ReceivedPiece
				if len(currPieces) >= minPieces {
					pCommits, _ := rcv.cache.GetAnchoredCommitments(bID, c)
					if len(pCommits) > 0 {
						n := rcv.getBlockN(bID)
						if recVer, vErr := rcv.rm.RecodePiecesWithVerify(currPieces[:minPieces], pCommits, 5, n); vErr == nil && recVer != nil {
							chosen = recVer
						}
					}
					if chosen == nil {
						rec, err := rcv.rm.RecodePieces(currPieces[:minPieces])
						if err == nil && rec != nil {
							chosen = rec
						} else {
							chosen = &currPieces[0]
						}
					}
				} else {
					chosen = &currPieces[0]
				}

				rcv.cache.StoreRecodedPiece(bID, r, c, *chosen)
				rcv.cache.PruneRawPieces(bID, r, c, rcv.rm)

				rcv.pruneMu.Lock()
				rcv.prunedCells[cKey] = true
				rcv.pruneMu.Unlock()

				touchedBlocks[bID] = true
			}

			for bID := range touchedBlocks {
				rcv.CheckAndLogCompletion(bID)
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
			custodyNetRows := rcv.getCustodyNetRows()
			activePeers := rcv.getActivePeersInCol()
			netColIdx := rcv.getNetColIdx()
			go func(bID string, h int, netCol int, col int, row int, stores int, cRows []int, actPeers int) {
				for attempt := 1; attempt <= 5; attempt++ {
					_ = rcv.broadcaster.BroadcastStoreReady(bID, h, netCol, col, row, stores, cRows, actPeers)
					time.Sleep(200 * time.Millisecond)
				}
			}(blockID, height, netColIdx, rcv.colIdx, rcv.rowIdx, rcv.storesPerCol, custodyNetRows, activePeers)
		}

		// Final Block Cleanup: Prune all raw pieces for Borrowed Backup and Non-Custody cells,
		// and delete any errant recoded pieces for Native Custody cells to guarantee strict steady-state piece counts.
		go func(bID string) {
			n := rcv.getBlockN(bID)
			numCols := rcv.numCols
			if numCols <= 0 {
				numCols = 8
			}
			colsPerNetCol := n / numCols
			if colsPerNetCol == 0 {
				colsPerNetCol = 1
			}
			netColIdx := rcv.getNetColIdx()
			startCol := netColIdx * colsPerNetCol
			endCol := startCol + colsPerNetCol
			if endCol > n {
				endCol = n
			}

			for c := startCol; c < endCol; c++ {
				for r := 0; r < n; r++ {
					_, isNativeRetention, _ := rcv.evalCustody(r)
					if isNativeRetention {
						rcv.cellMu.Lock()
						rcv.cache.DeleteRecodedPieces(bID, r, c)
						rcv.cellMu.Unlock()
					} else if rcv.cache.GetTotalPiecesForCell(bID, r, c) > 0 {
						rcv.cellMu.Lock()
						rcv.cache.PruneRawPieces(bID, r, c, rcv.rm)
						rcv.cellMu.Unlock()
						cellKey := fmt.Sprintf("%s_%d_%d", bID, r, c)
						rcv.pruneMu.Lock()
						rcv.prunedCells[cellKey] = true
						rcv.pruneMu.Unlock()
					}
				}
			}

			actualPieces := rcv.cache.GetTotalPieceCount()
			custodyPieces := rcv.cache.GetCustodyPieceCount(rcv.storesPerCol, rcv.rowIdx)
			recodedPieces := rcv.cache.GetRecodedPieceCount()
			rcv.totalStoredPiecesMu.Lock()
			rcv.totalStoredPieces = int64(actualPieces)
			rcv.totalStoredPiecesMu.Unlock()
			LinearIndependentPiecesCount.Set(float64(actualPieces))
			CustodyPiecesCount.Set(float64(custodyPieces))
			RecodedPiecesCount.Set(float64(recodedPieces))
		}(blockID)
	}
}

// catchUpRecentBlocksOnJoin queries Bootstrap /bootstrap/sync/recent on node startup/join
// and pulls any recent blocks' missing pieces from column peers so newly joined Store Nodes
// synchronize custody state for recent blocks.
func (rcv *Receiver) catchUpRecentBlocksOnJoin(ctx context.Context) {
	// Wait briefly for peer connections to settle
	select {
	case <-time.After(1500 * time.Millisecond):
	case <-ctx.Done():
		return
	}

	rcv.peersMu.RLock()
	bootAddr := rcv.bootstrapHTTPAddr
	rcv.peersMu.RUnlock()
	if bootAddr == "" || !strings.HasPrefix(bootAddr, "http") {
		return
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(strings.TrimSuffix(bootAddr, "/") + "/bootstrap/sync/recent?limit=3")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}

	var recent struct {
		BlockIDs []string `json:"block_ids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&recent); err != nil || len(recent.BlockIDs) == 0 {
		return
	}

	for _, bID := range recent.BlockIDs {
		if rcv.IsComplete(bID) {
			continue
		}
		log.Printf("[StoreNode CatchUp] Newly joined Store Node syncing recent block %s from column peers...", bID)
		rcv.fallbackPullMissingCells(bID, 2)
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
						_, isNativeRetention, _ := rcv.evalCustody(row)
						if !isNativeRetention {
							rcv.pruneMu.Lock()
							if rcv.prunedCells[key] {
								rcv.pruneMu.Unlock()
								continue
							}
							rcv.prunedCells[key] = true
							rcv.pruneMu.Unlock()

							log.Printf("[Pruning] Cell [%d, %d] of block %s is non-native custody. Compressing raw pieces into 1 recoded piece and pruning raw pieces...", row, col, blockID)
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

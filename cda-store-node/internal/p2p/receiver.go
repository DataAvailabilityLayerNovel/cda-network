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
	"sort"
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
	contribMu        sync.Mutex
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

	// Track globally subscribed non-custody topics to prevent duplicate subscriptions across SetPeers calls
	subscribedTopicsMu sync.Mutex
	subscribedTopics   map[string]bool

	// Sequential block processing queue
	taskChan chan func()

	// Context for subscribeToNonCustodyCells goroutines lifetime
	ctx context.Context
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
		completedBlocks:   make(map[string]bool),
		cellBroadcastCount: make(map[string]int),
		subscribedTopics:   make(map[string]bool),
		peerContributions: make(map[string]map[string]int),
		taskChan:          make(chan func(), 20000),
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

	// Start sequential task worker
	go rcv.workerLoop(ctx)
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

	// Keep generic fetch handlers (used by light nodes and active-pull)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreFetch, rcv.handleFetchStream)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreGetPieces, rcv.handleFetchStream)

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
			select {
			case rcv.taskChan <- func() { rcv.processGossipMessage(data) }:
			default:
				log.Printf("[StoreNode] taskChan full, dropping gossip message on node topic")
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
				select {
				case rcv.taskChan <- func() { rcv.processAnchorMessage(data) }:
				default:
					log.Printf("[StoreNode] taskChan full, dropping anchor gossip message")
				}
			}
		}(sub)
	}
}

// workerLoop drains taskChan sequentially — one task at a time.
func (rcv *Receiver) workerLoop(ctx context.Context) {
	for {
		select {
		case task, ok := <-rcv.taskChan:
			if !ok {
				return
			}
			task()
		case <-ctx.Done():
			return
		}
	}
}

// processGossipMessage handles incoming messages on this node's own TopicNode topic.
// Messages here are recoded RLNC pieces published by other custody nodes.
func (rcv *Receiver) processGossipMessage(data []byte) {
	var payload p2pcommon.SeedCellRequest
	if err := json.Unmarshal(data, &payload); err != nil {
		return
	}
	recordGossipMessage()
	rcv.processPiece(payload.BlockID, payload.Row, payload.Col, payload.Data, payload.Coeffs, payload.Proof, payload.PieceCommits, payload.SenderPeerID, true)
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

func (rcv *Receiver) processPiece(blockID string, row, col int, dataStr, coeffsStr, proofStr string, pieceCommitsStr []string, senderPeerID string, isGossip bool) error {
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

	if !rcv.rm.VerifyPiece(piece, combinedCommit) {
		ByzantineDetectionsTotal.Inc()
		log.Printf("[StoreNode] Piece verification failed (KZG pairing mismatch) for cell [%d, %d]", row, col)
		return fmt.Errorf("piece verification failed")
	}

	rcv.cellMu.Lock()
	// 4. Rank Filtering (Gaussian Elimination)
	existingPieces := rcv.cache.GetPieces(blockID, row, col)
	existingCoeffs := make([][]byte, len(existingPieces))
	for i, p := range existingPieces {
		existingCoeffs[i] = p.Data.Coeffs
	}

	if !engine.IsLinearlyIndependent(existingCoeffs, decodedCoeffs, rcv.kPiece) {
		log.Printf("[P2P] Received piece for cell [%d, %d]. Linear independence check: dependent (redundant). Dropping piece.", row, col)
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

	go rcv.CheckAndLogCompletion(blockID)

	// 6. P2P Recoding & GossipSub forwarding
	updatedPieces := rcv.cache.GetPieces(blockID, row, col)
	rcv.cellMu.Unlock()

	isPrimary := (row % rcv.storesPerCol) == rcv.rowIdx
	isBackup := ((row + 1) % rcv.storesPerCol) == rcv.rowIdx
	isCustody := isPrimary || isBackup

	if isCustody {
		// Custody nodes (primary + backup): recode as soon as rank >= 2 and broadcast k_piece pieces
		if !isGossip && len(updatedPieces) >= 2 {
			go func(bID string, r, c int, pieces []cda.ReceivedPiece, primary bool) {
				if !primary {
					// Backup Node: stagger 25ms to ensure piece diversity
					time.Sleep(25 * time.Millisecond)
				}
				log.Printf("[GossipSub] Local rank for cell [%d, %d] is %d/%d (>=2, Primary=%v). Recoding and publishing to node topic...", r, c, len(pieces), rcv.kPiece, primary)
				recodedPiece, err := rcv.rm.RecodePieces(pieces)
				if err != nil {
					log.Printf("[GossipSub] Failed to recode pieces for cell [%d, %d]: %v", r, c, err)
					return
				}
				rcv.cellMu.Lock()
				rcv.cache.StoreRecodedPiece(bID, r, c, *recodedPiece)
				rcv.cellMu.Unlock()

				if rcv.pruneEnable {
					rcv.pruneMu.Lock()
					key := fmt.Sprintf("%s_%d_%d", bID, r, c)
					if !rcv.prunedCells[key] {
						rcv.cellLastActivity[key] = time.Now()
					}
					rcv.pruneMu.Unlock()
				}
				log.Printf("[GossipSub] Publishing recoded piece for cell [%d, %d] (Primary=%v) to node topic...", r, c, primary)
				if err := rcv.broadcaster.BroadcastRecodedPiece(bID, r, c, recodedPiece, pieceCommits); err != nil {
					log.Printf("[GossipSub] Failed to publish recoded piece for cell [%d, %d]: %v", r, c, err)
				} else {
					log.Printf("[GossipSub] Successfully published recoded piece for cell [%d, %d] (Primary=%v).", r, c, primary)
					cellKey := fmt.Sprintf("%s_%d_%d", bID, r, c)
					rcv.broadcastMu.Lock()
					rcv.cellBroadcastCount[cellKey]++
					rcv.broadcastMu.Unlock()
					go rcv.CheckAndLogCompletion(bID)

					if !primary {
						// Backup custody node: prune raw pieces after successful dissemination to release backup storage
						rcv.cellMu.Lock()
						rcv.cache.PruneRawPieces(bID, r, c, rcv.rm)
						rcv.cellMu.Unlock()
						log.Printf("[StoreNode] Backup: cell [%d, %d] dissemination completed; raw pieces pruned.", r, c)
					}
				}
			}(blockID, row, col, updatedPieces, isPrimary)
		}
	} else {
		// Non-custody nodes: recode only when we have k_piece pieces from >=2 different custody sources.
		// Then keep exactly 1 recoded piece and discard raw pieces to save space.
		cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
		rcv.contribMu.Lock()
		numSources := len(rcv.peerContributions[cellKey])
		rcv.contribMu.Unlock()

		if len(updatedPieces) >= rcv.kPiece && numSources >= 2 {
			go func(bID string, r, c int, pieces []cda.ReceivedPiece) {
				log.Printf("[StoreNode] Non-custody: cell [%d, %d] has %d pieces from %d sources. Recoding and keeping 1 piece.", r, c, len(pieces), numSources)
				recodedPiece, err := rcv.rm.RecodePieces(pieces[:rcv.kPiece])
				if err != nil {
					log.Printf("[StoreNode] Non-custody recode failed for cell [%d, %d]: %v", r, c, err)
					return
				}
				rcv.cellMu.Lock()
				rcv.cache.StoreRecodedPiece(bID, r, c, *recodedPiece)
				// Prune raw pieces — non-custody node keeps only the 1 recoded piece
				rcv.cache.PruneRawPieces(bID, r, c, rcv.rm)
				rcv.cellMu.Unlock()
				log.Printf("[StoreNode] Non-custody: cell [%d, %d] recoded and raw pieces pruned.", r, c)
			}(blockID, row, col, updatedPieces)
		}
	}

	// 7. Active Pull: if custody node still lacks pieces, pull from column peers
	if isCustody && len(updatedPieces) < rcv.kPiece {
		go rcv.pullMissingPiecesFromPeers(blockID, row, col)
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
					select {
					case rcv.taskChan <- func() { rcv.processGossipMessage(data) }:
					default:
						log.Printf("[StoreNode] taskChan full, dropping non-custody gossip message")
					}
				}
			}(sub)
		}
	}
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

	// Prioritize Backup Node for this row (Row == (row + 1) % storesPerCol)
	backupRow := (row + 1) % rcv.storesPerCol
	sort.SliceStable(peersCopy, func(i, j int) bool {
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
					go rcv.CheckAndLogCompletion(blockID)
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
		recodedPiece, err := rcv.rm.RecodePieces(allPieces)
		if err == nil {
			rcv.cellMu.Lock()
			rcv.cache.StoreRecodedPiece(blockID, row, col, *recodedPiece)
			rcv.cellMu.Unlock()
			log.Printf("[StoreNode] Successfully recoded cell [%d, %d] in active pull.", row, col)
			go rcv.CheckAndLogCompletion(blockID)

			// Try to gossip it
			isCustody := (row % rcv.storesPerCol) == rcv.rowIdx
			if isCustody {
				if err := rcv.broadcaster.BroadcastRecodedPiece(blockID, row, col, recodedPiece, pieceCommits); err != nil {
					log.Printf("[GossipSub] Failed to gossip recoded piece for cell [%d, %d]: %v", row, col, err)
				} else {
					log.Printf("[GossipSub] Successfully gossiped recoded piece for cell [%d, %d] to peers.", row, col)
					cellKey := fmt.Sprintf("%s_%d_%d", blockID, row, col)
					rcv.broadcastMu.Lock()
					rcv.cellBroadcastCount[cellKey]++
					rcv.broadcastMu.Unlock()
					go rcv.CheckAndLogCompletion(blockID)
				}
			}
		} else {
			log.Printf("[StoreNode] Failed to recode cell [%d, %d] in active pull: %v", row, col, err)
		}
	}
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
	log.Printf("[Height: %d] [StoreNode] Retrieving cell [%d, %d] for block %s. Local pieces: %d", height, req.Row, req.Col, req.BlockID, len(allPieces))

	// Get commitments
	pieceCommits, _ := rcv.cache.GetAnchoredCommitments(req.BlockID, req.Col)
	pieceCommitsStr := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		pieceCommitsStr[i] = hex.EncodeToString(c)
	}

	// 2. Query peers in column network if needed and allowed
	if len(allPieces) < rcv.kPiece && !req.IsRemoteHop {
		log.Printf("[StoreNode] Not enough local pieces (%d/%d). Querying peers in column network...", len(allPieces), rcv.kPiece)
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
			log.Printf("[StoreNode] Fetch query: requesting pieces for cell [%d, %d] from peer %s...", req.Row, req.Col, pid)

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
			log.Printf("[StoreNode] Fetch query: received response from peer %s with %d pieces", pid, len(peerResp.Pieces))

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
					log.Printf("[StoreNode] Added independent piece from peer %s. Current count: %d", pid, len(allPieces))
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
	log.Printf("[StoreNode] Fetch query completed for cell [%d, %d]. Total independent pieces gathered: %d/%d (Recovery Success: %v)", req.Row, req.Col, len(allPieces), rcv.kPiece, recovered)

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

func (rcv *Receiver) IsComplete(blockID string) bool {
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

	rcv.broadcastMu.Lock()
	defer rcv.broadcastMu.Unlock()

	for c := startCol; c < endCol; c++ {
		for r := 0; r < n; r++ {
			if r%storesPerCol == rcv.rowIdx {
				// 1. Storage completion check: MUST have >= kPiece pieces stored
				if rcv.cache.GetPieceCount(blockID, r, c) < rcv.kPiece {
					return false
				}
				// 2. Channel dissemination completion check: MUST have published recoded pieces to dedicated node topic
				cellKey := fmt.Sprintf("%s_%d_%d", blockID, r, c)
				if rcv.cellBroadcastCount[cellKey] < 1 {
					return false
				}
			}
		}
	}
	return true
}

func (rcv *Receiver) CheckAndLogCompletion(blockID string) {
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
	ticker := time.NewTicker(10 * time.Second)
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
						}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

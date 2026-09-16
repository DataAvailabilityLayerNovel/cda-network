package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"cda-bootstrap-node/internal/engine"
	"cda-bootstrap-node/internal/storage"
	"cda-bootstrap-node/internal/verifier"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	"github.com/dgraph-io/badger/v4"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

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

	// Dynamic Peer Registry
	peersMu      sync.RWMutex
	activePeers  map[peer.ID]p2pcommon.PeerInfo
	lastSeenPeer map[peer.ID]time.Time

	pruneEnable bool
	pruneTTL    time.Duration

	// Multi-block Pipelining & Pre-computation State
	pipelineMu            sync.Mutex
	pipelineCond          *sync.Cond
	latestCompletedHeight int
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
		cache:         cache,
		proofGen:      proofGen,
		encoder:       encoder,
		broadcaster:   broadcaster,
		crashOnFail:   crashOnFail,
		activePeers:   make(map[peer.ID]p2pcommon.PeerInfo),
		lastSeenPeer:  make(map[peer.ID]time.Time),
		pruneEnable:   pruneEnable,
		pruneTTL:      pruneTTL,
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
						rcv.lastSeenPeer[pid] = time.Now() // Give 15 seconds to re-register
					}
					return nil
				})
			}
			return nil
		})
	}

	return rcv
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

	// Periodically clean up stale peers (older than 15s)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				rcv.peersMu.Lock()
				now := time.Now()
				for pid, lastSeen := range rcv.lastSeenPeer {
					if now.Sub(lastSeen) > 15*time.Second {
						log.Printf("[P2P Registry] Removing stale peer: %s", pid)
						if rcv.db != nil {
							_ = rcv.db.Update(func(txn *badger.Txn) error {
								return txn.Delete([]byte("peer_" + pid.String()))
							})
						}
						delete(rcv.activePeers, pid)
						delete(rcv.lastSeenPeer, pid)
					}
				}
				rcv.peersMu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Periodically prune local cache (if enabled)
	if rcv.pruneEnable {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
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

		// Generate KZG Opening Proofs for Column
		start := time.Now()
		proofs, err := rcv.proofGen.GenerateColumnProofs(colIdx, columnData)
		duration := time.Since(start).Seconds()

		if err != nil {
			log.Printf("[Height: %d] Async Proof Generator error for Column %d: %v", height, colIdx, err)
			return
		}
		KZGProofDuration.Observe(duration)
		rcv.cache.SetProofs(payload.BlockID, proofs)
		log.Printf("[Height: %d] Successfully generated opening proofs async for Column %d", height, colIdx)

		// Encode RLNC pieces and pre-compute all seeds in memory buffer for active Store Nodes
		n := len(columnData)
		totalSeeds := 2 * rcv.k

		type precomputedPiece struct {
			row      int
			pieceIdx int
			piece    *cda.ReceivedPiece
		}
		var precomputedPieces []precomputedPiece

		for row := 0; row < n; row++ {
			codedPieces, err := rcv.encoder.EncodeRowNSeeds(row, colIdx, columnData, proofs[row], totalSeeds)
			if err != nil {
				log.Printf("[Height: %d] Async RLNC encoding error for row %d (col %d): %v", height, row, colIdx, err)
				return
			}

			for pieceIdx, piece := range codedPieces {
				precomputedPieces = append(precomputedPieces, precomputedPiece{
					row:      row,
					pieceIdx: pieceIdx,
					piece:    piece,
				})
			}
		}

		log.Printf("[Height: %d] [Bootstrap Pre-computation] Successfully pre-computed %d opening proofs and %d RLNC seeds in buffer for Column %d, Block %s",
			height, n, len(precomputedPieces), colIdx, payload.BlockID)

		// Sequential Completion Gate: Wait until store nodes complete block-(height-1) before dispatching
		if height > 1 {
			rcv.pipelineMu.Lock()
			if rcv.latestCompletedHeight < height-1 {
				log.Printf("[Height: %d] [Bootstrap Pipeline] Pre-computed buffer ready! Waiting for store nodes to complete block-%d before dispatching (current completed: %d)...",
					height, height-1, rcv.latestCompletedHeight)
				deadline := time.Now().Add(120 * time.Second)
				for rcv.latestCompletedHeight < height-1 && time.Now().Before(deadline) {
					rcv.pipelineMu.Unlock()
					time.Sleep(100 * time.Millisecond)
					rcv.pipelineMu.Lock()
				}
				if rcv.latestCompletedHeight < height-1 {
					log.Printf("[Height: %d] [Bootstrap Pipeline] ERROR: Safety timeout reached without block-%d completion (completed: %d). Aborting dispatch of block-%d seeds to maintain multi-column synchronization!",
						height, height-1, rcv.latestCompletedHeight, height)
					rcv.pipelineMu.Unlock()
					return
				} else {
					log.Printf("[Height: %d] [Bootstrap Pipeline] Store nodes finished block-%d! Instantly dispatching pre-computed seeds for block-%d...",
						height, height-1, height)
				}
			}
			rcv.pipelineMu.Unlock()
		}

		// Dispatch Anchor to GossipSub now that previous block is complete and this block is active
		if err := rcv.broadcaster.BroadcastAnchor(payload.BlockID, colIdx, pieceCommits, merkleProofs); err != nil {
			log.Printf("[Height: %d] Failed to broadcast anchor for Column %d: %v", height, colIdx, err)
		}

		// Group precomputed pieces by target Store Node
		nodeBatches := make(map[string][]p2pcommon.SeedCellRequest)
		nodePeers := make(map[string]p2pcommon.PeerInfo)

		hexPieceCommits := make([]string, len(pieceCommits))
		for i, c := range pieceCommits {
			hexPieceCommits[i] = hex.EncodeToString(c)
		}

		for _, item := range precomputedPieces {
			peers := rcv.GetPeersForCell(item.row, colIdx, item.pieceIdx)
			if len(peers) == 0 {
				continue
			}
			targetPeer := peers[0]
			nodePeers[targetPeer.PeerID] = targetPeer

			seedReq := p2pcommon.SeedCellRequest{
				BlockID:      payload.BlockID,
				Row:          item.row,
				Col:          colIdx,
				Data:         hex.EncodeToString(item.piece.Data.Data),
				Coeffs:       hex.EncodeToString(item.piece.Data.Coeffs),
				Proof:        hex.EncodeToString(item.piece.Proof),
				PieceCommits: hexPieceCommits,
				SenderPeerID: rcv.host.ID().String(),
			}
			nodeBatches[targetPeer.PeerID] = append(nodeBatches[targetPeer.PeerID], seedReq)
		}

		// Dispatch batches concurrently to each store node
		var wg sync.WaitGroup
		sem := make(chan struct{}, 64)
		for peerID, seeds := range nodeBatches {
			targetPeer := nodePeers[peerID]
			wg.Add(1)
			sem <- struct{}{}
			go func(tp p2pcommon.PeerInfo, sList []p2pcommon.SeedCellRequest) {
				defer wg.Done()
				defer func() { <-sem }()

				// Chunks of up to 64 seeds per batch stream
				chunkSize := 64
				for i := 0; i < len(sList); i += chunkSize {
					end := i + chunkSize
					if end > len(sList) {
						end = len(sList)
					}
					batchReq := p2pcommon.BatchSeedCellRequest{
						BlockID: payload.BlockID,
						Seeds:   sList[i:end],
					}
					if err := rcv.broadcaster.BroadcastBatchPieces(tp, batchReq); err != nil {
						log.Printf("[Height: %d] Fallback: broadcast batch to %s failed: %v. Sending individually...", height, tp.PeerID, err)
						for _, s := range sList[i:end] {
							pData, _ := hex.DecodeString(s.Data)
							pCoeffs, _ := hex.DecodeString(s.Coeffs)
							pProof, _ := hex.DecodeString(s.Proof)
							pc := cda.ReceivedPiece{
								Row:   s.Row,
								Col:   s.Col,
								Data:  rlnc.PieceData{Data: pData, Coeffs: pCoeffs},
								Proof: cda.OpeningProof(pProof),
							}
							_ = rcv.broadcaster.BroadcastPiece(payload.BlockID, s.Row, colIdx, 0, &pc, pieceCommits)
						}
					}
				}
			}(targetPeer, seeds)
		}
		wg.Wait()
		log.Printf("[Height: %d] Async Phase 2 finished successfully: seeded %d pieces total (%d per store node) for Column %d", height, len(precomputedPieces), rcv.k, colIdx)
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

	for p, info := range rcv.activePeers {
		if p == pid {
			continue // skip self
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
	rcv.peersMu.Unlock()

	resp := p2pcommon.BootstrapRoutingResponse{
		RowPeers: rowPeers,
		ColPeers: colPeers,
	}

	if err := json.NewEncoder(stream).Encode(resp); err != nil {
		log.Printf("[P2P Registry] Failed to write routing response to %s: %v", pid, err)
	}
}

func (rcv *Receiver) GetPeersForCell(row, col int, pieceIdx int) []p2pcommon.PeerInfo {
	rcv.peersMu.RLock()
	defer rcv.peersMu.RUnlock()

	var colPeers []p2pcommon.PeerInfo
	for _, info := range rcv.activePeers {
		colPeers = append(colPeers, info)
	}

	if len(colPeers) == 0 {
		return nil
	}

	// Sort colPeers deterministically by Row coordinate
	sort.Slice(colPeers, func(i, j int) bool {
		return colPeers[i].Row < colPeers[j].Row
	})

	kVal := rcv.k
	if kVal <= 0 {
		kVal = 4
	}

	// Find the primary store node for this row
	// The primary store node's Row coordinate should match row % len(colPeers)
	primaryIdx := row % len(colPeers)

	// Determine if this pieceIdx is for the primary node or the backup node
	isBackup := (pieceIdx / kVal) > 0

	var targetIdx int
	if isBackup {
		targetIdx = (primaryIdx + 1) % len(colPeers)
	} else {
		targetIdx = primaryIdx
	}

	targetPeer := colPeers[targetIdx]
	return []p2pcommon.PeerInfo{targetPeer}
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


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
	}
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
}

func (rcv *Receiver) handleReceiveColumn(stream network.Stream) {
	defer stream.Close()

	var payload p2pcommon.PublisherPushRequest
	if err := json.NewDecoder(stream).Decode(&payload); err != nil {
		rcv.respondWithError(stream, fmt.Sprintf("failed to decode payload: %v", err))
		return
	}

	colIdx := payload.ColIdx
	log.Printf("[P2P] Received Push stream for Column %d, Block %s", colIdx, payload.BlockID)

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
			log.Fatalf("Verifier error for Column %d (CRASH): %v", colIdx, err)
		}
		log.Printf("Verifier error for Column %d: %v", colIdx, err)
		rcv.respondWithError(stream, "Verification error: "+err.Error())
		return
	}
	if !ok {
		if rcv.crashOnFail {
			log.Fatalf("Verifier failed (CRASH): data from publisher for Column %d is not consistent", colIdx)
		}
		log.Printf("Verifier failed: data from publisher for Column %d is not consistent", colIdx)
		rcv.respondWithError(stream, "Data consistency check failed")
		return
	}

	log.Printf("[P2P] Verification succeeded for Column %d", colIdx)

	// 3. Gossip commitments and Merkle proofs immediately (Fast Path Phase 1)
	if err := rcv.broadcaster.BroadcastAnchor(payload.BlockID, colIdx, pieceCommits, merkleProofs); err != nil {
		log.Printf("Failed to broadcast anchor for Column %d: %v", colIdx, err)
		rcv.respondWithError(stream, "Anchor broadcast failed: "+err.Error())
		return
	}

	// 4. Store in Local Cache
	rcv.cache.Store(payload.BlockID, colIdx, columnData, pieceCommits)

	// Respond with success
	json.NewEncoder(stream).Encode(struct {
		Success bool `json:"success"`
	}{Success: true})

	// 5. Phase 2 (Async Heavy Task)
	go func() {
		log.Printf("[P2P] Starting Phase 2 Async: computing proofs and seeding RLNC pieces for Column %d", colIdx)

		// Generate KZG Opening Proofs for Column
		start := time.Now()
		proofs, err := rcv.proofGen.GenerateColumnProofs(colIdx, columnData)
		duration := time.Since(start).Seconds()

		if err != nil {
			log.Printf("Async Proof Generator error for Column %d: %v", colIdx, err)
			return
		}
		KZGProofDuration.Observe(duration)
		rcv.cache.SetProofs(payload.BlockID, proofs)
		log.Printf("Successfully generated opening proofs async for Column %d", colIdx)

		// Encode RLNC pieces and broadcast 2 seeds to EVERY active Store Node in the column network
		n := len(columnData)
		// We generate 2 * rcv.k seeds per row (k for the primary store node, and k for 1 backup store node)
		totalSeeds := 2 * rcv.k

		for row := 0; row < n; row++ {
			codedPieces, err := rcv.encoder.EncodeRowNSeeds(row, colIdx, columnData, proofs[row], totalSeeds)
			if err != nil {
				log.Printf("Async RLNC encoding error for row %d (col %d): %v", row, colIdx, err)
				return
			}

			// Unicast each pair of coded pieces to its assigned Store Node
			for pieceIdx, piece := range codedPieces {
				if err := rcv.broadcaster.BroadcastPiece(payload.BlockID, row, colIdx, pieceIdx, piece, pieceCommits); err != nil {
					log.Printf("Failed to broadcast piece %d to Store Node for cell [%d, %d]: %v", pieceIdx, row, colIdx, err)
					return
				}
			}
		}
		log.Printf("Async Phase 2 finished successfully: seeded %d pieces total (%d per store node) for Column %d", totalSeeds, rcv.k, colIdx)
	}()
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
			colPeers = append(colPeers, info)
			rowPeers = append(rowPeers, info)
		} else {
			if info.Row == req.Peer.Row {
				rowPeers = append(rowPeers, info)
			}
			if info.Col == req.Peer.Col {
				colPeers = append(colPeers, info)
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


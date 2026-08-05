package p2p

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
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
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

type Receiver struct {
	host          host.Host
	ps            *pubsub.PubSub
	kzg           cda.KZGProvider
	rm            *cda.RecipientManager
	publisherAddr string
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

	// Rate limiting
	rateLimitersMu sync.Mutex
	rateLimiters   map[peer.ID]*tokenBucket

	// In-memory stored piece count to avoid heavy DB iteration
	totalStoredPieces   int64
	totalStoredPiecesMu sync.Mutex
	cellMu              sync.Mutex
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
) *Receiver {
	return &Receiver{
		host:              h,
		ps:                ps,
		kzg:               kzg,
		rm:                cda.NewRecipientManager(kPiece, kzg),
		publisherAddr:     pubAddr,
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
	}
}

func (rcv *Receiver) SetPeers(rowPeers, colPeers []p2pcommon.PeerInfo) {
	rcv.peersMu.Lock()
	rcv.rowPeers = rowPeers
	rcv.colPeers = colPeers
	rcv.peersMu.Unlock()

	// Update broadcaster with column peers for direct gossip fallback
	var pIDs []peer.ID
	for _, p := range colPeers {
		if pid, err := peer.Decode(p.PeerID); err == nil {
			pIDs = append(pIDs, pid)
		}
	}
	rcv.broadcaster.UpdatePeers(pIDs)
}

func (rcv *Receiver) Start(ctx context.Context) {
	StartMetricsTicker(ctx)
	LinearIndependentPiecesCount.Set(float64(rcv.totalStoredPieces))

	// 1. Set stream handlers
	rcv.host.SetStreamHandler(p2pcommon.ProtoBootstrapSeed, rcv.handleSeedStream)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreFetch, rcv.handleFetchStream)
	rcv.host.SetStreamHandler(p2pcommon.ProtoStoreGetPieces, rcv.handleFetchStream)

	// 2. Subscribe to Column GossipSub topics
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
			log.Fatalf("Failed to join column GossipSub topic %s: %v", colTopicName, err)
		}

		sub, err := topic.Subscribe()
		if err != nil {
			log.Fatalf("Failed to subscribe to column GossipSub topic %s: %v", colTopicName, err)
		}

		go func(sub *pubsub.Subscription) {
			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					return
				}
				if msg.ReceivedFrom == rcv.host.ID() {
					continue // skip self
				}
				rcv.processGossipMessage(msg.Data)
			}
		}(sub)
	}
}

func (rcv *Receiver) processGossipMessage(data []byte) {
	// The message can be either GossipAnchorPayload or SeedCellRequest (pieces gossip)
	// Inspect dynamic json fields to determine type
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return
	}

	if _, isAnchor := raw["merkle_proofs"]; isAnchor {
		// Process Anchor Commitments
		var payload p2pcommon.GossipAnchorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return
		}
		rcv.processAnchor(payload.BlockID, payload.ColIdx, payload.PieceCommits, payload.MerkleProofs)
	} else {
		// Process Recoded Piece Gossip
		var payload p2pcommon.SeedCellRequest
		if err := json.Unmarshal(data, &payload); err != nil {
			return
		}
		recordGossipMessage()
		rcv.processPiece(payload.BlockID, payload.Row, payload.Col, payload.Data, payload.Coeffs, payload.Proof, payload.PieceCommits, true)
	}
}

func (rcv *Receiver) processAnchor(blockID string, colIdx int, commitsStr []string, proofsStr []p2pcommon.SerializedMerkleProof) {
	log.Printf("[StoreNode] Received Anchor Gossip for Column %d, Block %s", colIdx, blockID)

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
	log.Printf("[StoreNode] Successfully anchored commitments for Block %s, Column %d", blockID, colIdx)
}

func (rcv *Receiver) handleSeedStream(stream network.Stream) {
	defer stream.Close()

	var payload p2pcommon.SeedCellRequest
	if err := json.NewDecoder(stream).Decode(&payload); err != nil {
		rcv.respondWithError(stream, fmt.Sprintf("failed to decode seed: %v", err))
		return
	}

	err := rcv.processPiece(payload.BlockID, payload.Row, payload.Col, payload.Data, payload.Coeffs, payload.Proof, payload.PieceCommits, false)
	if err != nil {
		rcv.respondWithError(stream, err.Error())
		return
	}

	json.NewEncoder(stream).Encode(struct {
		Success bool `json:"success"`
	}{Success: true})
}

func (rcv *Receiver) processPiece(blockID string, row, col int, dataStr, coeffsStr, proofStr string, pieceCommitsStr []string, isGossip bool) error {
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
		log.Printf("[StoreNode] Anchored commitments not found in cache for block %s. Fetching from publisher...", blockID)
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

	// 6. P2P Recoding & GossipSub forwarding
	updatedPieces := rcv.cache.GetPieces(blockID, row, col)
	rcv.cellMu.Unlock()

	if !isGossip && len(updatedPieces) >= 2 {
		log.Printf("[GossipSub] Local rank for cell [%d, %d] is %d/%d (>=2). Triggering local recoding of all available pieces...", row, col, len(updatedPieces), rcv.kPiece)
		recodedPiece, err := rcv.rm.RecodePieces(updatedPieces)
		if err != nil {
			log.Printf("[GossipSub] Failed to recode pieces for cell [%d, %d]: %v", row, col, err)
		} else {
			rcv.cellMu.Lock()
			rcv.cache.StoreRecodedPiece(blockID, row, col, *recodedPiece)
			rcv.cellMu.Unlock()
			log.Printf("[GossipSub] Recoding success for cell [%d, %d]. Gossiping recoded piece with coeffs %x to column neighbor peers...", row, col, recodedPiece.Data.Coeffs)
			if err := rcv.broadcaster.BroadcastRecodedPiece(blockID, row, col, recodedPiece, pieceCommits); err != nil {
				log.Printf("[GossipSub] Failed to gossip recoded piece for cell [%d, %d]: %v", row, col, err)
			} else {
				log.Printf("[GossipSub] Successfully gossiped recoded piece for cell [%d, %d] to peers.", row, col)
			}
		}
	}

	// 7. Active Pull: if local piece count is still less than kPiece, actively pull from column peers
	if len(updatedPieces) < rcv.kPiece {
		go rcv.pullMissingPiecesFromPeers(blockID, row, col)
	}

	return nil
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

	allPieces := append([]cda.ReceivedPiece(nil), localPieces...)

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
				log.Printf("[StoreNode] Active pull: stored independent piece for cell [%d, %d] from peer %s (count: %d/%d)", row, col, pid, len(latestPieces), rcv.kPiece)
				allPieces = latestPieces
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

	log.Printf("[StoreNode] Retrieving cell [%d, %d] for block %s. Local pieces: %d", req.Row, req.Col, req.BlockID, len(allPieces))

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

	for c := startCol; c < endCol; c++ {
		for r := 0; r < n; r++ {
			if r%storesPerCol == rcv.rowIdx {
				if rcv.cache.GetPieceCount(blockID, r, c) < rcv.kPiece {
					return false
				}
			}
		}
	}
	return true
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

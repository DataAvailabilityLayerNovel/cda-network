package service

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cda-publisher-node/internal/engine"
	"cda-publisher-node/internal/p2p"

	p2pcommon "cda-p2p"
	"github.com/dgraph-io/badger/v4"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type APIService struct {
	pipeline              *engine.Pipeline
	sender                *p2p.Sender
	ps                    *pubsub.PubSub
	headerTopic           *pubsub.Topic
	storeReadyTopic       *pubsub.Topic
	blockReadyTopic       *pubsub.Topic
	db                    *badger.DB
	sequencerPubKey       string
	activeCols            int
	kVal                  int
	mu                    sync.Mutex
	colReadyMu            sync.Mutex
	publishMu             sync.Mutex
	publishCond           *sync.Cond
	latestPublishedHeight int
	latestCompletedHeight int
	storeReadyMap         map[string]map[int]map[int]bool // BlockID -> NetColIdx -> set of RowIdx
	columnReadyMap        map[string]map[int]bool          // BlockID -> set of NetColIdx
	blockReadyEmitted     map[string]bool
}

type HeaderPayload struct {
	CommitsRoot string   `json:"commits_root,omitempty"`
	ColumnComm  []string `json:"column_comm,omitempty"`
	Coeffs      string   `json:"coeffs,omitempty"`
}

type PublishRequest struct {
	BlockID   string         `json:"block_id"`
	Data      []string       `json:"data"` // List of cells as hex strings
	Header    *HeaderPayload `json:"header,omitempty"`
	Signature string         `json:"signature,omitempty"`
}

func NewAPIService(pipeline *engine.Pipeline, sender *p2p.Sender, ps *pubsub.PubSub, dbPath string, sequencerPubKey string, activeCols int, kVal int) *APIService {
	var db *badger.DB
	if dbPath != "" {
		_ = os.MkdirAll(dbPath, 0755)
		opts := badger.DefaultOptions(dbPath).WithLogger(nil)
		var err error
		db, err = badger.Open(opts)
		if err != nil {
			panic(fmt.Sprintf("failed to open badger db on publisher: %v", err))
		}
	}

	if kVal <= 0 {
		kVal = 16
	}

	svc := &APIService{
		pipeline:          pipeline,
		sender:            sender,
		ps:                ps,
		db:                db,
		sequencerPubKey:   sequencerPubKey,
		activeCols:        activeCols,
		kVal:              kVal,
		storeReadyMap:     make(map[string]map[int]map[int]bool),
		columnReadyMap:    make(map[string]map[int]bool),
		blockReadyEmitted: make(map[string]bool),
	}
	svc.publishCond = sync.NewCond(&svc.colReadyMu)

	if ps != nil {
		var err error
		svc.headerTopic, err = ps.Join(p2pcommon.TopicHeader)
		if err != nil {
			log.Printf("[Publisher] Warning: Failed to join GossipSub header topic: %v", err)
		}

		svc.storeReadyTopic, err = ps.Join(p2pcommon.TopicStoreReady)
		if err != nil {
			log.Printf("[Publisher] Warning: Failed to join GossipSub store-ready topic: %v", err)
		} else {
			storeSub, errSub := svc.storeReadyTopic.Subscribe()
			if errSub == nil {
				go svc.listenStoreReady(storeSub)
			} else {
				log.Printf("[Publisher] Warning: Failed to subscribe to store-ready topic: %v", errSub)
			}
		}

		svc.blockReadyTopic, err = ps.Join(p2pcommon.TopicBlockReady)
		if err != nil {
			log.Printf("[Publisher] Warning: Failed to join GossipSub block-ready topic: %v", err)
		}
	}

	return svc
}

func (s *APIService) listenStoreReady(sub *pubsub.Subscription) {
	ctx := context.Background()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		var payload p2pcommon.GossipStoreReadyPayload
		if err := json.Unmarshal(msg.Data, &payload); err == nil && payload.BlockID != "" {
			s.handleStoreReady(payload)
		}
	}
}

func (s *APIService) isNetColActive(colIdx int) bool {
	activeCols := s.activeCols
	if activeCols <= 0 {
		activeCols = 8
	}
	n := 2 * s.kVal
	if n <= 0 {
		n = 32
	}
	colsPerNetCol := n / 8
	if colsPerNetCol <= 0 {
		colsPerNetCol = 1
	}

	netColIdx := (colIdx / colsPerNetCol) * colsPerNetCol
	netColNumber := netColIdx / colsPerNetCol

	return netColNumber < activeCols
}

func (s *APIService) handleStoreReady(payload p2pcommon.GossipStoreReadyPayload) {
	s.colReadyMu.Lock()
	if s.storeReadyMap == nil {
		s.storeReadyMap = make(map[string]map[int]map[int]bool)
	}
	if s.columnReadyMap == nil {
		s.columnReadyMap = make(map[string]map[int]bool)
	}
	if s.blockReadyEmitted == nil {
		s.blockReadyEmitted = make(map[string]bool)
	}

	blockID := payload.BlockID
	if s.blockReadyEmitted[blockID] {
		s.colReadyMu.Unlock()
		return
	}

	netColIdx := payload.NetColIdx
	if !s.isNetColActive(netColIdx) {
		s.colReadyMu.Unlock()
		return
	}

	rowIdx := payload.RowIdx
	storesPerCol := payload.StoresPerCol
	if storesPerCol <= 0 {
		storesPerCol = 8
	}
	height := payload.Height
	if height <= 0 {
		height = p2pcommon.ParseHeightFromBlockID(blockID)
	}

	if s.storeReadyMap[blockID] == nil {
		s.storeReadyMap[blockID] = make(map[int]map[int]bool)
	}
	if s.storeReadyMap[blockID][netColIdx] == nil {
		s.storeReadyMap[blockID][netColIdx] = make(map[int]bool)
	}
	s.storeReadyMap[blockID][netColIdx][rowIdx] = true
	readyStoreCount := len(s.storeReadyMap[blockID][netColIdx])

	expectedCols := s.activeCols
	if expectedCols <= 0 {
		expectedCols = 8
	}

	if s.columnReadyMap[blockID] == nil {
		s.columnReadyMap[blockID] = make(map[int]bool)
	}

	if readyStoreCount >= storesPerCol {
		s.columnReadyMap[blockID][netColIdx] = true
	}
	readyColCount := len(s.columnReadyMap[blockID])

	log.Printf("[Height: %d] [Publisher] Received StoreReady from Col %d Row %d (%d/%d Store Nodes ready for Col %d). Block %s: %d/%d active columns complete",
		height, netColIdx, rowIdx, readyStoreCount, storesPerCol, netColIdx, blockID, readyColCount, expectedCols)

	if readyColCount >= expectedCols && !s.blockReadyEmitted[blockID] {
		s.blockReadyEmitted[blockID] = true
		s.colReadyMu.Unlock()

		s.broadcastBlockReady(blockID, height, expectedCols)
		return
	}
	s.colReadyMu.Unlock()
}

func (s *APIService) broadcastBlockReady(blockID string, height int, activeCols int) {
	s.colReadyMu.Lock()
	if height > s.latestCompletedHeight {
		s.latestCompletedHeight = height
	}
	if s.publishCond != nil {
		s.publishCond.Broadcast()
	}
	s.colReadyMu.Unlock()

	if s.blockReadyTopic == nil {
		return
	}
	payload := p2pcommon.GossipBlockReadyPayload{
		BlockID: blockID,
		Height:  height,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[Publisher] Warning: Failed to marshal block-ready payload for %s: %v", blockID, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.blockReadyTopic.Publish(ctx, data); err != nil {
		log.Printf("[Publisher] Warning: Failed to publish block-ready for %s: %v", blockID, err)
	} else {
		log.Printf("[Height: %d] [Publisher] 🎉 ALL %d active columns (100%% store nodes) complete for block %s — Broadcasted BlockReady signal to Light Nodes!",
			height, activeCols, blockID)
	}
}

func (s *APIService) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *APIService) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/publish", s.handlePublish)
	mux.HandleFunc("/header/", s.handleGetHeader)
}

func VerifySignature(blockID string, data []string, sigHex, pubKeyHex string) error {
	pubKeyBytes, err := hex.DecodeString(pubKeyHex)
	if err != nil {
		return fmt.Errorf("invalid config public key hex: %w", err)
	}
	if len(pubKeyBytes) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid public key size")
	}
	pubKey := ed25519.PublicKey(pubKeyBytes)

	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return fmt.Errorf("invalid signature hex: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString(blockID)
	for _, d := range data {
		buf.WriteString(d)
	}

	if !ed25519.Verify(pubKey, buf.Bytes(), sigBytes) {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

// VerifyCDAHeader checks that the computed header commitments match the BFT Header commitments agreed by CometBFT consensus.
func VerifyCDAHeader(computedHeader *engine.BlockHeader, bftHeader *HeaderPayload) error {
	if bftHeader == nil {
		return nil
	}

	if computedHeader == nil {
		return fmt.Errorf("nil computed header")
	}

	// 1. Verify CommitsRoot
	if bftHeader.CommitsRoot != "" {
		if !strings.EqualFold(computedHeader.CommitsRoot, bftHeader.CommitsRoot) {
			return fmt.Errorf("mismatched CommitsRoot: computed=%s, bft=%s", computedHeader.CommitsRoot, bftHeader.CommitsRoot)
		}
	}

	// 2. Verify ColumnComm
	if len(bftHeader.ColumnComm) > 0 {
		if len(computedHeader.ColumnComm) != len(bftHeader.ColumnComm) {
			return fmt.Errorf("mismatched ColumnComm count: computed=%d, bft=%d", len(computedHeader.ColumnComm), len(bftHeader.ColumnComm))
		}
		for i, expectedHex := range bftHeader.ColumnComm {
			computedHex := hex.EncodeToString(computedHeader.ColumnComm[i])
			if !strings.EqualFold(computedHex, expectedHex) {
				return fmt.Errorf("mismatched ColumnComm at index %d: computed=%s, bft=%s", i, computedHex, expectedHex)
			}
		}
	}

	// 3. Verify Coeffs
	if bftHeader.Coeffs != "" {
		computedCoeffsHex := hex.EncodeToString(computedHeader.Coeffs)
		if !strings.EqualFold(computedCoeffsHex, bftHeader.Coeffs) {
			return fmt.Errorf("mismatched Coeffs: computed=%s, bft=%s", computedCoeffsHex, bftHeader.Coeffs)
		}
	}

	return nil
}

func (s *APIService) handlePublish(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Data) == 0 {
		http.Error(w, "Data cannot be empty", http.StatusBadRequest)
		return
	}

	// 0. Signature Verification
	if s.sequencerPubKey != "" {
		if req.Signature == "" {
			http.Error(w, "Missing signature in request", http.StatusUnauthorized)
			return
		}
		if err := VerifySignature(req.BlockID, req.Data, req.Signature, s.sequencerPubKey); err != nil {
			http.Error(w, "Signature verification failed: "+err.Error(), http.StatusUnauthorized)
			return
		}
	}

	// 0.5. Sequential Completion Gate (Multi-Block Queue Control)
	reqHeight := p2pcommon.ParseHeightFromBlockID(req.BlockID)
	if reqHeight > 1 {
		s.colReadyMu.Lock()
		for s.latestCompletedHeight < reqHeight-1 {
			log.Printf("[Height: %d] [Publisher Queue] Waiting for block-%d to complete before publishing block-%d...", reqHeight, reqHeight-1, reqHeight)
			s.publishCond.Wait()
		}
		s.colReadyMu.Unlock()
	}

	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	s.colReadyMu.Lock()
	if reqHeight > s.latestPublishedHeight {
		s.latestPublishedHeight = reqHeight
	}
	s.colReadyMu.Unlock()

	// 1. Decode hex data cells (ODS)
	decodedData := make([][]byte, len(req.Data))
	totalBytes := 0
	for i, h := range req.Data {
		b, err := hex.DecodeString(h)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid hex string at index %d: %v", i, err), http.StatusBadRequest)
			return
		}
		decodedData[i] = b
		totalBytes += len(b)
	}

	// 2. Run engine pipeline: ODS -> EDS -> Commitments -> Header
	start := time.Now()
	header, pubData, proofs, eds, err := s.pipeline.ProcessODS(decodedData, req.BlockID)
	duration := time.Since(start).Seconds()

	if err != nil {
		http.Error(w, "Engine pipeline failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	height := p2pcommon.ParseHeightFromBlockID(header.BlockID)

	// 2.5. Verify Header against BFT Consensus Commitments (Same as Validator Node Verification)
	if req.Header != nil {
		if err := VerifyCDAHeader(header, req.Header); err != nil {
			log.Printf("[Height: %d] [Publisher] Header verification FAILED for BlockID %s: %v", height, req.BlockID, err)
			http.Error(w, "Header verification failed: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		log.Printf("[Height: %d] [Publisher] SUCCESS: Header verification passed for BlockID %s against BFT consensus!", height, req.BlockID)
	}

	// Record metrics
	RSEncodeDuration.Observe(duration)
	ThroughputBytesTotal.Add(float64(totalBytes))
	if duration > 0 {
		ThroughputBytesRate.Set(float64(totalBytes) / duration)
	}

	// 3. Save Header and Broadcast via GossipSub
	if s.db != nil {
		headerBytes, _ := json.Marshal(header)
		_ = s.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte("header_"+header.BlockID), headerBytes)
		})
	}

	log.Printf("[Height: %d] Successfully generated Block Header for BlockID: %s", height, header.BlockID)

	if s.headerTopic != nil {
		headerBytes, err := json.Marshal(header)
		if err != nil {
			log.Printf("[GossipSub] Failed to marshal header: %v", err)
		} else {
			if err := s.headerTopic.Publish(context.Background(), headerBytes); err != nil {
				log.Printf("[GossipSub] Failed to publish header for %s: %v", header.BlockID, err)
			} else {
				log.Printf("[Height: %d] [GossipSub] Successfully published Block Header for %s on GossipSub topic %s", height, header.BlockID, p2pcommon.TopicHeader)
			}
		}
	}

	// 4. Distribute each column via P2P sender in parallel
	n := int(eds.Width())
	k := len(pubData.PieceComm) / n
	var wg sync.WaitGroup
	for c := 0; c < n; c++ {
		wg.Add(1)
		go func(colIdx int) {
			defer wg.Done()
			colData := eds.Col(uint(colIdx))
			pieceCommits := pubData.PieceComm[colIdx*k : colIdx*k+k]

			pieceCommitsBytes := make([][]byte, k)
			for i := 0; i < k; i++ {
				pieceCommitsBytes[i] = append([]byte(nil), pieceCommits[i]...)
			}

			colProofs := proofs[colIdx*k : colIdx*k+k]

			if err := s.sender.SendColumnChunk(header.BlockID, colIdx, colData, pieceCommitsBytes, colProofs); err != nil {
				log.Printf("[Height: %d] [Publisher] Warning: Failed to distribute column %d to bootstrap (likely offline): %v", height, colIdx, err)
			}
		}(c)
	}
	wg.Wait()

	// Respond with the BlockHeader
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(header)
}

func (s *APIService) handleGetHeader(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	blockID := r.URL.Path[len("/header/"):]
	if blockID == "" {
		http.Error(w, "Missing block ID", http.StatusBadRequest)
		return
	}

	var header *engine.BlockHeader
	var exists bool

	if s.db != nil {
		_ = s.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte("header_" + blockID))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				var h engine.BlockHeader
				if err := json.Unmarshal(val, &h); err == nil {
					header = &h
					exists = true
				}
				return nil
			})
		})
	}

	if !exists {
		http.Error(w, "Header not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(header)
}


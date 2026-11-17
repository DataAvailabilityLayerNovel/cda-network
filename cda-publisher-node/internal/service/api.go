package service

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cda-publisher-node/internal/engine"
	"cda-publisher-node/internal/p2p"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
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
	numCols               int
	kVal                  int
	reconstructor         *engine.BlockReconstructor
	cometbftRPC           string
	mu                    sync.Mutex
	colReadyMu            sync.Mutex
	publishMu             sync.Mutex
	publishCond           *sync.Cond
	latestPublishedHeight int
	latestCompletedHeight int
	inFlightCount         int
	pendingPublishCount   int
	inFlightBlocks        map[string]int                     // BlockID -> Height
	storeReadyMap         map[string]map[int]map[int]bool    // BlockID -> NetColIdx -> set of covered NetRowIdx
	storeReadyPeers       map[string]map[int]map[string]bool // BlockID -> NetColIdx -> set of ready PeerID
	columnReadyMap        map[string]map[int]bool             // BlockID -> set of NetColIdx
	blockReadyEmitted     map[string]bool
	fullCustodyCompleted  map[string]bool
	blockK                map[string]int
	blockStartTime        map[string]time.Time

	rejectedBlocksMu     sync.RWMutex
	rejectedBlocks       map[string]string // blockID -> reason
	latestRejectedHeight int64
}

type HeaderPayload struct {
	ChainID         string    `json:"chain_id,omitempty"`
	Height          int64     `json:"height,omitempty"`
	Time            time.Time `json:"time,omitempty"`
	DataHash        string    `json:"data_hash,omitempty"`
	AppHash         string    `json:"app_hash,omitempty"`
	ProposerAddress string    `json:"proposer_address,omitempty"`

	CommitsRoot string   `json:"commits_root,omitempty"`
	ColumnComm  []string `json:"column_comm,omitempty"`
	Coeffs      string   `json:"coeffs,omitempty"`
}

type TxSummary struct {
	Hash   string `json:"hash"`
	Key    string `json:"key,omitempty"`
	Offset int    `json:"offset"`
	Cells  int    `json:"cells"`
	Size   int    `json:"size"`
}

type PublishRequest struct {
	BlockID   string         `json:"block_id"`
	Height    int64          `json:"height,omitempty"`
	Data      []string       `json:"data"` // List of cells as hex strings
	Header    *HeaderPayload `json:"header,omitempty"`
	Txs       []TxSummary    `json:"txs,omitempty"`
	Signature string         `json:"signature,omitempty"`
}

func NewAPIService(pipeline *engine.Pipeline, sender *p2p.Sender, ps *pubsub.PubSub, dbPath string, sequencerPubKey string, activeCols int, kVal int, numCols ...int) *APIService {
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

	numColsVal := 8
	if len(numCols) > 0 && numCols[0] > 0 {
		numColsVal = numCols[0]
	}

	svc := &APIService{
		pipeline:          pipeline,
		sender:            sender,
		ps:                ps,
		db:                db,
		sequencerPubKey:   sequencerPubKey,
		activeCols:        activeCols,
		numCols:           numColsVal,
		kVal:              kVal,
		storeReadyMap:     make(map[string]map[int]map[int]bool),
		storeReadyPeers:   make(map[string]map[int]map[string]bool),
		columnReadyMap:    make(map[string]map[int]bool),
		blockReadyEmitted:    make(map[string]bool),
		fullCustodyCompleted: make(map[string]bool),
		blockK:               make(map[string]int),
		blockStartTime:    make(map[string]time.Time),
		inFlightBlocks:    make(map[string]int),
		rejectedBlocks:    make(map[string]string),
	}
	svc.publishCond = sync.NewCond(&svc.colReadyMu)

	// Background reconciliation loop: prunes stale in-flight blocks and enforces pipeline completion invariants
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			var cbftHeight int
			if body, status, err := svc.queryCometBFT("status"); err == nil && status == http.StatusOK {
				var statusResp struct {
					Result struct {
						SyncInfo struct {
							LatestBlockHeight string `json:"latest_block_height"`
						} `json:"sync_info"`
					} `json:"result"`
				}
				if json.Unmarshal(body, &statusResp) == nil && statusResp.Result.SyncInfo.LatestBlockHeight != "" {
					if h, err := strconv.Atoi(statusResp.Result.SyncInfo.LatestBlockHeight); err == nil && h > 0 {
						cbftHeight = h
					}
				}
			}

			svc.colReadyMu.Lock()
			effectiveH := svc.latestPublishedHeight
			if svc.latestCompletedHeight > effectiveH {
				effectiveH = svc.latestCompletedHeight
			}
			if cbftHeight > effectiveH {
				effectiveH = cbftHeight
			}
			if effectiveH > 0 {
				LatestPublishedHeight.Set(float64(effectiveH))
				CometBFTConsensusHeight.Set(float64(effectiveH))
			}
			// Invariant 1: When latest completed height >= latest published height, all published blocks are done.
			if svc.latestCompletedHeight >= svc.latestPublishedHeight && svc.latestPublishedHeight > 0 {
				if len(svc.inFlightBlocks) > 0 || svc.inFlightCount > 0 {
					svc.inFlightBlocks = make(map[string]int)
					svc.inFlightCount = 0
					InFlightBlocks.Set(0)
				}
			} else if len(svc.inFlightBlocks) > 0 {
				changed := false
				for bid, h := range svc.inFlightBlocks {
					// Invariant 2: Any block whose height <= latestCompletedHeight has been completed/superseded
					if h > 0 && h <= svc.latestCompletedHeight {
						delete(svc.inFlightBlocks, bid)
						changed = true
						continue
					}
					// Invariant 3: If a block has been in-flight for > 15s and reached >= 75% custody (e.g. 6/8 store nodes),
					// finalize BlockReady since RS 2D can fully recover the ODS from >= 50% data.
					stTime := svc.blockStartTime[bid]
					if !stTime.IsZero() && time.Since(stTime) > 15*time.Second {
						readyCount := 0
						if svc.storeReadyMap[bid] != nil && svc.storeReadyMap[bid][0] != nil {
							readyCount = len(svc.storeReadyMap[bid][0])
						}
						storesPerCol := 8
						if envStores := os.Getenv("STORE_NODES_PER_COL"); envStores != "" {
							if val, err := strconv.Atoi(envStores); err == nil && val > 0 {
								storesPerCol = val
							}
						}
						minQuorum := (storesPerCol * 3) / 4
						if minQuorum < 1 {
							minQuorum = 1
						}
						if readyCount >= minQuorum && !svc.blockReadyEmitted[bid] {
							svc.blockReadyEmitted[bid] = true
							expCols := svc.activeCols
							if expCols <= 0 {
								expCols = 8
							}
							log.Printf("[Publisher Reconcile] Block %s reached %d/%d store custody (>75%% quorum) after 15s timeout. Finalizing BlockReady.", bid, readyCount, storesPerCol)
							go svc.broadcastBlockReady(bid, h, expCols)
						} else if time.Since(stTime) > 60*time.Second {
							// Pipeline timeout fallback: prune dead in-flight entries after 60s
							delete(svc.inFlightBlocks, bid)
							changed = true
						}
					}
				}
				if changed {
					svc.inFlightCount = len(svc.inFlightBlocks)
					InFlightBlocks.Set(float64(svc.inFlightCount))
				}
			}
			svc.colReadyMu.Unlock()
		}
	}()

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

func (s *APIService) SetReconstructor(r *engine.BlockReconstructor) {
	s.reconstructor = r
}

func (s *APIService) SetCometBFTRPC(rpc string) {
	s.cometbftRPC = rpc
}

func (s *APIService) markBlockRejected(blockID string, height int64, reason string) {
	s.rejectedBlocksMu.Lock()
	if s.rejectedBlocks == nil {
		s.rejectedBlocks = make(map[string]string)
	}
	s.rejectedBlocks[blockID] = reason
	if height > s.latestRejectedHeight {
		s.latestRejectedHeight = height
	}
	s.rejectedBlocksMu.Unlock()

	if s.db != nil {
		_ = s.db.Update(func(txn *badger.Txn) error {
			_ = txn.Set([]byte("status_"+blockID), []byte("STATUS_INVALID_REJECTED"))
			_ = txn.Set([]byte("block_status_"+blockID), []byte("STATUS_INVALID_REJECTED"))
			_ = txn.Set([]byte(fmt.Sprintf("block_status_height_%d", height)), []byte("STATUS_INVALID_REJECTED"))
			_ = txn.Set([]byte("reject_reason_"+blockID), []byte(reason))
			return nil
		})
	}

	log.Printf("[Height: %d] [Publisher] 🚫 Block %s REJECTED as STATUS_INVALID_REJECTED: %s. Denying DA BlockReady. Consensus and pipeline continue running.",
		height, blockID, reason)
}

func (s *APIService) MarkBlockRejectedForTest(blockID string, height int64, reason string) {
	s.markBlockRejected(blockID, height, reason)
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

func (s *APIService) isNetColActiveForN(colIdx int, n int) bool {
	activeCols := s.activeCols
	if activeCols <= 0 {
		activeCols = 8
	}
	if n <= 0 {
		n = 2 * s.kVal
	}
	if n <= 0 {
		n = 32
	}
	numCols := s.numCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol <= 0 {
		colsPerNetCol = 1
	}

	netColIdx := (colIdx / colsPerNetCol) * colsPerNetCol
	netColNumber := netColIdx / colsPerNetCol

	return netColNumber < activeCols
}

func (s *APIService) isNetColActive(colIdx int) bool {
	return s.isNetColActiveForN(colIdx, 2*s.kVal)
}

func (s *APIService) handleStoreReady(payload p2pcommon.GossipStoreReadyPayload) {
	s.colReadyMu.Lock()
	if s.storeReadyMap == nil {
		s.storeReadyMap = make(map[string]map[int]map[int]bool)
	}
	if s.storeReadyPeers == nil {
		s.storeReadyPeers = make(map[string]map[int]map[string]bool)
	}
	if s.columnReadyMap == nil {
		s.columnReadyMap = make(map[string]map[int]bool)
	}
	if s.blockReadyEmitted == nil {
		s.blockReadyEmitted = make(map[string]bool)
	}

	blockID := payload.BlockID
	netColIdx := payload.NetColIdx
	kVal := s.blockK[blockID]
	if kVal <= 0 {
		kVal = s.kVal
	}
	nVal := 2 * kVal
	if nVal <= 0 {
		nVal = 32
	}
	if !s.isNetColActiveForN(netColIdx, nVal) {
		s.colReadyMu.Unlock()
		return
	}

	rowIdx := payload.RowIdx
	netRows := payload.NetRows
	if netRows <= 0 {
		netRows = payload.StoresPerCol
	}
	if netRows <= 0 {
		netRows = 8
	}

	activePeersInCol := payload.ActivePeersInCol
	if activePeersInCol <= 0 {
		activePeersInCol = payload.StoresPerCol
	}
	if activePeersInCol <= 0 {
		activePeersInCol = netRows
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
	if rowIdx >= 0 {
		s.storeReadyMap[blockID][netColIdx][rowIdx%netRows] = true
	}
	for _, r := range payload.CustodyNetRows {
		if r >= 0 {
			s.storeReadyMap[blockID][netColIdx][r%netRows] = true
		}
	}
	coveredRowsCount := len(s.storeReadyMap[blockID][netColIdx])

	if s.storeReadyPeers[blockID] == nil {
		s.storeReadyPeers[blockID] = make(map[int]map[string]bool)
	}
	if s.storeReadyPeers[blockID][netColIdx] == nil {
		s.storeReadyPeers[blockID][netColIdx] = make(map[string]bool)
	}
	peerKey := payload.PeerID
	if peerKey == "" {
		peerKey = fmt.Sprintf("row_%d", rowIdx)
	}
	s.storeReadyPeers[blockID][netColIdx][peerKey] = true
	readyPeersCount := len(s.storeReadyPeers[blockID][netColIdx])

	expectedCols := s.activeCols
	if expectedCols <= 0 {
		expectedCols = 8
	}

	if s.columnReadyMap[blockID] == nil {
		s.columnReadyMap[blockID] = make(map[int]bool)
	}

	// BFT Supermajority Quorum: ceil(2*N / 3) = (2*N + 2) / 3
	bftQuorumRows := (2*netRows + 2) / 3
	if bftQuorumRows <= 0 {
		bftQuorumRows = 1
	} else if bftQuorumRows > netRows {
		bftQuorumRows = netRows
	}

	bftQuorumPeers := (2*activePeersInCol + 2) / 3
	if bftQuorumPeers <= 0 {
		bftQuorumPeers = 1
	} else if bftQuorumPeers > activePeersInCol {
		bftQuorumPeers = activePeersInCol
	}

	quorumRows := bftQuorumRows
	quorumPeers := bftQuorumPeers
	if envQuorum := os.Getenv("STORE_READY_QUORUM"); envQuorum != "" {
		if val, err := strconv.Atoi(envQuorum); err == nil && val > 0 {
			if val <= netRows {
				quorumRows = val
			}
			if val <= activePeersInCol {
				quorumPeers = val
			}
		}
	} else if os.Getenv("STORE_READY_STRICT_100") == "true" {
		quorumRows = netRows
		quorumPeers = activePeersInCol
	}

	if coveredRowsCount >= quorumRows && readyPeersCount >= quorumPeers {
		s.columnReadyMap[blockID][netColIdx] = true
	}
	readyColCount := len(s.columnReadyMap[blockID])

	// Track 100% full custody convergence
	if coveredRowsCount >= netRows && readyPeersCount >= activePeersInCol {
		if !s.fullCustodyCompleted[blockID] {
			s.fullCustodyCompleted[blockID] = true
			log.Printf("[Height: %d] [Publisher] 🎉 [FULL_CUSTODY_CONVERGED] Block %s: All %d/%d store nodes (100%%) have successfully completed custody verification!",
				height, blockID, readyPeersCount, activePeersInCol)
		}
	}

	log.Printf("[Height: %d] [Publisher] Received StoreReady from Col %d Row %d Peer %s (RowsCovered=%d/%d, ReadyPeers=%d/%d, quorumRows=%d, quorumPeers=%d). Block %s: %d/%d active columns complete",
		height, netColIdx, rowIdx, peerKey, coveredRowsCount, netRows, readyPeersCount, activePeersInCol, quorumRows, quorumPeers, blockID, readyColCount, expectedCols)

	if readyColCount >= expectedCols && !s.blockReadyEmitted[blockID] {
		s.blockReadyEmitted[blockID] = true
		s.colReadyMu.Unlock()

		s.broadcastBlockReady(blockID, height, expectedCols)

		// Start background reconciliation to monitor/verify lagging store nodes until 100% completion
		go s.monitorStoreConvergence(blockID, height, netColIdx, netRows, activePeersInCol)
		return
	}
	s.colReadyMu.Unlock()
}

func (s *APIService) broadcastBlockReady(blockID string, height int, activeCols int) {
	s.colReadyMu.Lock()
	if s.inFlightBlocks != nil {
		delete(s.inFlightBlocks, blockID)
		// Prune any superseded blocks (older heights that were bypassed by newer completed blocks)
		for bid, h := range s.inFlightBlocks {
			if h > 0 && h <= height {
				delete(s.inFlightBlocks, bid)
			}
		}
		s.inFlightCount = len(s.inFlightBlocks)
	} else if s.inFlightCount > 0 {
		s.inFlightCount--
	}
	if height > s.latestCompletedHeight {
		s.latestCompletedHeight = height
	}
	if s.latestCompletedHeight > s.latestPublishedHeight {
		s.latestPublishedHeight = s.latestCompletedHeight
	}
	if s.latestCompletedHeight >= s.latestPublishedHeight && s.latestPublishedHeight > 0 {
		if s.inFlightBlocks != nil {
			s.inFlightBlocks = make(map[string]int)
		}
		s.inFlightCount = 0
	}
	LatestPublishedHeight.Set(float64(s.latestPublishedHeight))
	CometBFTConsensusHeight.Set(float64(s.latestPublishedHeight))
	LatestCompletedHeight.Set(float64(s.latestCompletedHeight))
	InFlightBlocks.Set(float64(s.inFlightCount))

	if s.publishCond != nil {
		s.publishCond.Broadcast()
	}
	kVal := s.blockK[blockID]
	if kVal <= 0 {
		kVal = s.kVal
	}
	LatestK.Set(float64(kVal))
	BlocksCompletedTotal.WithLabelValues(strconv.Itoa(kVal)).Inc()

	startTime := s.blockStartTime[blockID]
	duration := 0.0
	if !startTime.IsZero() {
		duration = time.Since(startTime).Seconds()
		BlockDuration.Observe(duration)
	}
	s.colReadyMu.Unlock()

	odsCells := kVal * kVal
	edsCells := (2 * kVal) * (2 * kVal)

	log.Printf("[Height: %d] [Publisher] 🎉 [BLOCK_COMPLETED] BlockID=%s Height=%d K=%d ODS=%dx%d (%d cells) EDS=%dx%d (%d cells) ActiveCols=%d StoreNodesCompleted=100%% Duration=%.3fs",
		height, blockID, height, kVal, kVal, kVal, odsCells, 2*kVal, 2*kVal, edsCells, activeCols, duration)

	if s.blockReadyTopic == nil {
		return
	}
	payload := p2pcommon.GossipBlockReadyPayload{
		BlockID: blockID,
		Height:  height,
		K:       kVal,
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
		log.Printf("[Height: %d] [Publisher] Successfully broadcasted BlockReady signal (K=%d) to Light Nodes for %s",
			height, kVal, blockID)
	}
}

func (s *APIService) monitorStoreConvergence(blockID string, height int, netColIdx, netRows, activePeersInCol int) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(8 * time.Second)

	client := &http.Client{Timeout: 500 * time.Millisecond}

	for {
		select {
		case <-timeout:
			s.colReadyMu.Lock()
			isFull := s.fullCustodyCompleted[blockID]
			currPeers := 0
			if s.storeReadyPeers[blockID] != nil {
				currPeers = len(s.storeReadyPeers[blockID][netColIdx])
			}
			s.colReadyMu.Unlock()
			if !isFull {
				log.Printf("[Height: %d] [Publisher] ℹ️ [CUSTODY_CONVERGENCE_STATUS] Block %s: %d/%d store nodes converged (Quorum was reached, some peers took longer than 8s).",
					height, blockID, currPeers, activePeersInCol)
			}
			return
		case <-ticker.C:
			s.colReadyMu.Lock()
			isFull := s.fullCustodyCompleted[blockID]
			reportedPeers := make(map[int]bool)
			if s.storeReadyPeers[blockID] != nil && s.storeReadyPeers[blockID][netColIdx] != nil {
				for peerKey := range s.storeReadyPeers[blockID][netColIdx] {
					var rIdx int
					if n, err := fmt.Sscanf(peerKey, "row_%d", &rIdx); err == nil && n == 1 {
						reportedPeers[rIdx] = true
					}
				}
			}
			s.colReadyMu.Unlock()

			if isFull {
				return
			}

			// Active HTTP check on lagging store nodes
			allDone := true
			for r := 0; r < activePeersInCol; r++ {
				if reportedPeers[r] {
					continue
				}
				// Try querying store node directly (both Docker hostname and localhost port)
				urls := []string{
					fmt.Sprintf("http://store-%d-%d:8080/store/status/%s", netColIdx, r+1, blockID),
					fmt.Sprintf("http://localhost:%d/store/status/%s", 9300+r, blockID),
				}
				storeDone := false
				for _, u := range urls {
					if resp, err := client.Get(u); err == nil {
						if resp.StatusCode == http.StatusOK {
							var res struct {
								Completed bool `json:"completed"`
							}
							if err := json.NewDecoder(resp.Body).Decode(&res); err == nil && res.Completed {
								storeDone = true
							}
						}
						resp.Body.Close()
						if storeDone {
							break
						}
					}
				}

				if storeDone {
					s.colReadyMu.Lock()
					if s.storeReadyPeers[blockID] == nil {
						s.storeReadyPeers[blockID] = make(map[int]map[string]bool)
					}
					if s.storeReadyPeers[blockID][netColIdx] == nil {
						s.storeReadyPeers[blockID][netColIdx] = make(map[string]bool)
					}
					s.storeReadyPeers[blockID][netColIdx][fmt.Sprintf("row_%d", r)] = true
					s.storeReadyMap[blockID][netColIdx][r%netRows] = true
					s.colReadyMu.Unlock()
					log.Printf("[Height: %d] [Publisher Active Check] Verified lagging Store Node (Row %d) reached completed status for %s", height, r, blockID)
				} else {
					allDone = false
				}
			}

			s.colReadyMu.Lock()
			count := 0
			if s.storeReadyPeers[blockID] != nil && s.storeReadyPeers[blockID][netColIdx] != nil {
				count = len(s.storeReadyPeers[blockID][netColIdx])
			}
			if count >= activePeersInCol && !s.fullCustodyCompleted[blockID] {
				s.fullCustodyCompleted[blockID] = true
				log.Printf("[Height: %d] [Publisher] 🎉 [FULL_CUSTODY_CONVERGED] Block %s: All %d/%d store nodes (100%%) have successfully completed custody verification!",
					height, blockID, count, activePeersInCol)
				s.colReadyMu.Unlock()
				return
			}
			s.colReadyMu.Unlock()

			if allDone {
				return
			}
		}
	}
}

func (s *APIService) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *APIService) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"healthy"}`))
	})
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/publish", s.handlePublish)
	mux.HandleFunc("/header/", s.handleGetHeader)
	mux.HandleFunc("/tx", s.handleGetTx)
	mux.HandleFunc("/tx/", s.handleGetTx)
	mux.HandleFunc("/tx/batch", s.handleBatchGetTx)
	mux.HandleFunc("/tx/batch/", s.handleBatchGetTx)
	mux.HandleFunc("/block/", s.handleGetBlockData)
	mux.HandleFunc("/api/v1/toc/", func(w http.ResponseWriter, r *http.Request) {
		target := strings.TrimPrefix(r.URL.Path, "/api/v1/toc/")
		target = strings.Trim(target, "/")
		s.handleGetBlockTOC(w, r, target)
	})
	mux.HandleFunc("/api/v1/namespaces", s.handleNamespaces)
	mux.HandleFunc("/api/v1/namespaces/", s.handleNamespaces)

	// Serve static web dashboard
	fs := http.FileServer(http.Dir("web"))
	mux.Handle("/dashboard/", http.StripPrefix("/dashboard/", fs))
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})
}

func (s *APIService) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.colReadyMu.Lock()
	if s.latestCompletedHeight >= s.latestPublishedHeight && s.latestPublishedHeight > 0 {
		if len(s.inFlightBlocks) > 0 || s.inFlightCount > 0 {
			s.inFlightBlocks = make(map[string]int)
			s.inFlightCount = 0
			InFlightBlocks.Set(0)
		}
	}
	inFlight := s.inFlightCount + s.pendingPublishCount
	completedH := s.latestCompletedHeight
	pubH := s.latestPublishedHeight
	s.colReadyMu.Unlock()

	maxInFlight := 2
	if envMax := os.Getenv("PUBLISHER_MAX_IN_FLIGHT"); envMax != "" {
		if m, err := strconv.Atoi(envMax); err == nil && m > 0 {
			maxInFlight = m
		}
	}

	s.rejectedBlocksMu.RLock()
	rejCount := len(s.rejectedBlocks)
	latestRejH := s.latestRejectedHeight
	s.rejectedBlocksMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":                  "healthy",
		"in_flight_count":         inFlight,
		"max_in_flight":           maxInFlight,
		"latest_published_height": pubH,
		"latest_completed_height": completedH,
		"latest_rejected_height":  latestRejH,
		"rejected_count":          rejCount,
		"active_cols":             s.activeCols,
		"num_cols":                s.numCols,
		"default_k":               s.kVal,
	})
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

	// 0. Verify Height if provided in bftHeader
	if bftHeader.Height > 0 && computedHeader.Height > 0 && bftHeader.Height != computedHeader.Height {
		return fmt.Errorf("mismatched Height: computed=%d, bft=%d", computedHeader.Height, bftHeader.Height)
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var req PublishRequest
	var preDecodedCells [][]byte

	if len(bodyBytes) >= 4 && binary.BigEndian.Uint32(bodyBytes[:4]) == p2pcommon.BinaryPublishBlockMagic {
		binReq, err := p2pcommon.DecodeBinaryPublishBlock(bodyBytes)
		if err != nil {
			http.Error(w, "Invalid binary request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		req.BlockID = binReq.BlockID
		req.Height = binReq.Height
		preDecodedCells = binReq.Cells
		if len(binReq.Signature) > 0 {
			req.Signature = hex.EncodeToString(binReq.Signature)
		}
		if binReq.Header != nil {
			colHex := make([]string, len(binReq.Header.ColumnComm))
			for i, cc := range binReq.Header.ColumnComm {
				colHex[i] = hex.EncodeToString(cc)
			}
			req.Header = &HeaderPayload{
				ChainID:         binReq.Header.ChainID,
				Height:          binReq.Header.Height,
				Time:            time.Unix(0, binReq.Header.TimeUnixNano).UTC(),
				DataHash:        hex.EncodeToString(binReq.Header.DataHash),
				AppHash:         hex.EncodeToString(binReq.Header.AppHash),
				ProposerAddress: binReq.Header.ProposerAddress,
				CommitsRoot:     hex.EncodeToString(binReq.Header.CommitsRoot),
				ColumnComm:      colHex,
				Coeffs:          hex.EncodeToString(binReq.Header.Coeffs),
			}
		}
		if len(binReq.Txs) > 0 {
			req.Txs = make([]TxSummary, len(binReq.Txs))
			for i, t := range binReq.Txs {
				req.Txs[i] = TxSummary{
					Hash:   t.Hash,
					Key:    t.Key,
					Offset: t.Offset,
					Cells:  t.Cells,
					Size:   t.Size,
				}
			}
		}
	} else {
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if len(preDecodedCells) == 0 && len(req.Data) == 0 {
		http.Error(w, "Data cannot be empty", http.StatusBadRequest)
		return
	}

	// 0. Signature Verification
	if s.sequencerPubKey != "" {
		if req.Signature == "" {
			http.Error(w, "Missing signature in request", http.StatusUnauthorized)
			return
		}
		if len(req.Data) == 0 && len(preDecodedCells) > 0 {
			req.Data = make([]string, len(preDecodedCells))
			for i, c := range preDecodedCells {
				req.Data[i] = hex.EncodeToString(c)
			}
		}
		if err := VerifySignature(req.BlockID, req.Data, req.Signature, s.sequencerPubKey); err != nil {
			log.Printf("[Publisher] Signature verification failed for block %s: %v", req.BlockID, err)
			s.markBlockRejected(req.BlockID, req.Height, "Signature verification failed: "+err.Error())
			http.Error(w, "Signature verification failed: "+err.Error(), http.StatusUnauthorized)
			return
		}
	}

	reqHeight := int(req.Height)
	if reqHeight <= 0 {
		reqHeight = p2pcommon.ParseHeightFromBlockID(req.BlockID)
	}

	// 0.4 Idempotency & Monotonic BlockID Protection:
	// - For BFT consensus blocks (req.Header != nil), if block_id is already completed/cached, return cached header idempotently.
	// - For direct/benchmark publish requests (req.Header == nil && s.sequencerPubKey == ""), if block_id or height collides
	//   with an already published/in-flight block, auto-assign the next monotonic height and block-<height>.
	s.colReadyMu.Lock()
	_, alreadyStarted := s.blockStartTime[req.BlockID]
	_, alreadyInFlight := s.inFlightBlocks[req.BlockID]
	alreadyCompleted := s.blockReadyEmitted[req.BlockID] || (reqHeight > 0 && reqHeight <= s.latestCompletedHeight)

	if req.Header == nil && s.sequencerPubKey == "" {
		if alreadyCompleted || alreadyStarted || alreadyInFlight || (reqHeight > 0 && reqHeight <= s.latestPublishedHeight) {
			nextH := s.latestPublishedHeight + 1
			if s.latestCompletedHeight+1 > nextH {
				nextH = s.latestCompletedHeight + 1
			}
			oldBlockID := req.BlockID
			reqHeight = nextH
			req.Height = int64(reqHeight)
			req.BlockID = fmt.Sprintf("block-%d", reqHeight)
			log.Printf("[Height: %d] [Publisher] Remapped colliding direct publish %s -> %s (monotonic height %d)",
				reqHeight, oldBlockID, req.BlockID, reqHeight)
		}
		if reqHeight > s.latestPublishedHeight {
			s.latestPublishedHeight = reqHeight
		}
	} else if alreadyCompleted {
		s.colReadyMu.Unlock()
		log.Printf("[Height: %d] [Publisher] Block %s already completed (latest completed: %d). Returning cached header.", reqHeight, req.BlockID, s.latestCompletedHeight)
		if s.db != nil {
			var cachedHeader *engine.BlockHeader
			_ = s.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get([]byte("header_" + req.BlockID))
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					return json.Unmarshal(val, &cachedHeader)
				})
			})
			if cachedHeader != nil {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(cachedHeader)
				return
			}
		}
		s.colReadyMu.Lock()
	}
	s.pendingPublishCount++
	if reqHeight > s.latestPublishedHeight {
		s.latestPublishedHeight = reqHeight
		LatestPublishedHeight.Set(float64(s.latestPublishedHeight))
		CometBFTConsensusHeight.Set(float64(s.latestPublishedHeight))
	}
	InFlightBlocks.Set(float64(s.inFlightCount + s.pendingPublishCount))
	s.colReadyMu.Unlock()

	pendingActive := true
	releasePending := func() {
		if pendingActive {
			pendingActive = false
			s.colReadyMu.Lock()
			if s.pendingPublishCount > 0 {
				s.pendingPublishCount--
			}
			InFlightBlocks.Set(float64(s.inFlightCount + s.pendingPublishCount))
			s.colReadyMu.Unlock()
		}
	}
	defer releasePending()

	// 1. Extract data cells (ODS): zero-hex fast path for binary wire format, or hex decode for JSON
	var decodedData [][]byte
	totalBytes := 0
	if len(preDecodedCells) > 0 {
		decodedData = make([][]byte, len(preDecodedCells))
		for i, b := range preDecodedCells {
			decodedData[i] = b
			totalBytes += len(b)
		}
	} else {
		decodedData = make([][]byte, len(req.Data))
		for i, h := range req.Data {
			b, err := hex.DecodeString(h)
			if err != nil {
				http.Error(w, fmt.Sprintf("Invalid hex string at index %d: %v", i, err), http.StatusBadRequest)
				return
			}
			decodedData[i] = b
			totalBytes += len(b)
		}
	}

	// Dynamic ODS sizing: find smallest power-of-2 K (>= 8) such that K*K >= len(decodedData)
	kBlock := cda.ComputeOptimalODSK(len(decodedData), cda.DefaultMinODSK, 64)
	targetCells := kBlock * kBlock
	cellSize := 512
	if len(decodedData) > 0 && len(decodedData[0]) > 0 {
		cellSize = len(decodedData[0])
	}
	for i := len(decodedData); i < targetCells; i++ {
		padCell := make([]byte, cellSize)
		padCell[0] = byte(i >> 24)
		padCell[1] = byte(i >> 16)
		padCell[2] = byte(i >> 8)
		padCell[3] = byte(i)
		decodedData = append(decodedData, padCell)
	}

	// 2. Run engine pipeline immediately: ODS -> EDS -> Commitments -> Header
	start := time.Now()
	header, pubData, proofs, eds, err := s.pipeline.ProcessODS(decodedData, req.BlockID)
	duration := time.Since(start).Seconds()

	if err != nil {
		http.Error(w, "Engine pipeline failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	height := req.Height
	if height <= 0 {
		height = int64(p2pcommon.ParseHeightFromBlockID(header.BlockID))
	}
	header.Height = height

	// 2.5. Verify Header against BFT Consensus Commitments (Same as Validator Node Verification)
	if req.Header != nil {
		if err := VerifyCDAHeader(header, req.Header); err != nil {
			log.Printf("[Height: %d] [Publisher] Header verification FAILED for BlockID %s: %v", height, req.BlockID, err)
			s.markBlockRejected(req.BlockID, height, fmt.Sprintf("Header verification failed: %v", err))
			http.Error(w, "Header verification failed: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if req.Header.ChainID != "" {
			header.ChainID = req.Header.ChainID
		}
		if !req.Header.Time.IsZero() {
			header.Time = req.Header.Time
		}
		if req.Header.DataHash != "" {
			header.DataHash = req.Header.DataHash
		}
		if req.Header.AppHash != "" {
			header.AppHash = req.Header.AppHash
		}
		if req.Header.ProposerAddress != "" {
			header.ProposerAddress = req.Header.ProposerAddress
		}
		log.Printf("[Height: %d] [Publisher] SUCCESS: Header verification passed for BlockID %s against BFT consensus!", height, req.BlockID)
	}

	// 3. Immediate ODS Cache & Async WriteBatch Indexing Phase (decoupled from publishMu and P2P in-flight queue)
	s.publishMu.Lock()
	RSEncodeDuration.Observe(duration)
	ThroughputBytesTotal.Add(float64(totalBytes))
	if duration > 0 {
		ThroughputBytesRate.Set(float64(totalBytes) / duration)
	}
	if s.reconstructor != nil {
		s.reconstructor.PutCachedODS(header.BlockID, height, decodedData)
	}
	s.publishMu.Unlock()

	if s.db != nil {
		headerCopy := *header
		txsCopy := append([]TxSummary(nil), req.Txs...)
		dataCopy := append([]string(nil), req.Data...)
		cellsCopy := append([][]byte(nil), decodedData...)

		// Synchronous header write + WriteBatch for transaction indices outside publishMu
		headerBytes, _ := json.Marshal(&headerCopy)
		wb := s.db.NewWriteBatch()
		_ = wb.Set([]byte("header_"+headerCopy.BlockID), headerBytes)
		if height > 0 {
			_ = wb.Set([]byte(fmt.Sprintf("block_height_%d", height)), []byte(headerCopy.BlockID))
		}
		heightBytes := []byte(strconv.FormatInt(height, 10))
		for _, tx := range txsCopy {
			txBytes, _ := json.Marshal(tx)
			payload := extractPayloadFromCells(cellsCopy, tx.Offset, tx.Cells, tx.Size)
			upperHash := strings.ToUpper(strings.TrimPrefix(tx.Hash, "0x"))
			if upperHash == "" && len(payload) > 0 {
				sum := sha256.Sum256(payload)
				upperHash = strings.ToUpper(hex.EncodeToString(sum[:]))
			}
			if upperHash != "" {
				_ = wb.Set([]byte("idx_tx_"+upperHash), heightBytes)
				_ = wb.Set([]byte(fmt.Sprintf("meta_tx_%s_%s", headerCopy.BlockID, upperHash)), txBytes)
				if len(payload) > 0 {
					_ = wb.Set([]byte("data_tx_"+upperHash), payload)
				}
			}
			if tx.Key != "" {
				_ = wb.Set([]byte("idx_key_"+tx.Key), heightBytes)
				if upperHash != "" {
					hashBytes := []byte(upperHash)
					_ = wb.Set([]byte(fmt.Sprintf("meta_key_%s_%s", headerCopy.BlockID, tx.Key)), hashBytes)
					_ = wb.Set([]byte("key_ref_"+tx.Key), hashBytes)
				} else if len(payload) > 0 {
					_ = wb.Set([]byte("data_key_"+tx.Key), payload)
				}
			}
		}
		_ = wb.Flush()

		// Async TOC & Namespace catalog indexing outside the hot publish path
		go func(bID string, h int64, txs []TxSummary, rawData []string, cells [][]byte) {
			_ = s.db.Update(func(txn *badger.Txn) error {
				if len(txs) > 0 {
					s.indexBlockTOCAndNamespaces(txn, bID, h, txs, cells)
				} else if len(rawData) > 0 {
					s.indexTransactionsFromRawCells(txn, bID, h, rawData)
				}
				return nil
			})
		}(headerCopy.BlockID, height, txsCopy, dataCopy, cellsCopy)
	}

	log.Printf("[Height: %d] Successfully generated and indexed Block Header for BlockID: %s (encode=%.3fs)", height, header.BlockID, duration)

	// 4. P2P Pipeline Queue Control & Column Distribution Phase
	s.mu.Lock()
	defer s.mu.Unlock()

	queueTimeout := 300 * time.Second
	if envTimeout := os.Getenv("PUBLISHER_QUEUE_TIMEOUT"); envTimeout != "" {
		if d, err := time.ParseDuration(envTimeout); err == nil && d > 0 {
			queueTimeout = d
		} else if sec, err := strconv.Atoi(envTimeout); err == nil && sec > 0 {
			queueTimeout = time.Duration(sec) * time.Second
		}
	}

	maxInFlight := 2
	if envMax := os.Getenv("PUBLISHER_MAX_IN_FLIGHT"); envMax != "" {
		if m, err := strconv.Atoi(envMax); err == nil && m > 0 {
			maxInFlight = m
		}
	}

	s.colReadyMu.Lock()
	if s.inFlightBlocks == nil {
		s.inFlightBlocks = make(map[string]int)
	}
	if maxInFlight > 0 {
		deadline := time.Now().Add(queueTimeout)
		for len(s.inFlightBlocks) >= maxInFlight && time.Now().Before(deadline) {
			log.Printf("[Height: %d] [Publisher Queue] In-flight pipeline full (%d in-flight, max: %d). Waiting before dispatching P2P columns for block-%d...",
				reqHeight, len(s.inFlightBlocks), maxInFlight, reqHeight)
			s.colReadyMu.Unlock()
			time.Sleep(100 * time.Millisecond)
			s.colReadyMu.Lock()
		}
		if len(s.inFlightBlocks) >= maxInFlight {
			currentInFlight := len(s.inFlightBlocks)
			s.colReadyMu.Unlock()
			http.Error(w, fmt.Sprintf("Block height %d timed out waiting in publisher queue (%d in-flight, max in-flight: %d after waiting %v)", reqHeight, currentInFlight, maxInFlight, queueTimeout), http.StatusUnprocessableEntity)
			return
		}
	}
	if reqHeight > s.latestPublishedHeight {
		s.latestPublishedHeight = reqHeight
		LatestPublishedHeight.Set(float64(s.latestPublishedHeight))
		CometBFTConsensusHeight.Set(float64(s.latestPublishedHeight))
	}
	if pendingActive {
		pendingActive = false
		if s.pendingPublishCount > 0 {
			s.pendingPublishCount--
		}
	}
	s.blockK[header.BlockID] = kBlock
	s.blockStartTime[header.BlockID] = start
	s.inFlightBlocks[header.BlockID] = reqHeight
	s.inFlightCount = len(s.inFlightBlocks)
	InFlightBlocks.Set(float64(s.inFlightCount + s.pendingPublishCount))
	s.colReadyMu.Unlock()

	accepted := false
	defer func() {
		if !accepted {
			s.colReadyMu.Lock()
			if s.inFlightBlocks != nil {
				delete(s.inFlightBlocks, header.BlockID)
				s.inFlightCount = len(s.inFlightBlocks)
			} else if s.inFlightCount > 0 {
				s.inFlightCount--
			}
			InFlightBlocks.Set(float64(s.inFlightCount))
			s.colReadyMu.Unlock()
		}
	}()

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

	// 5. Distribute each column via P2P sender in parallel over QUIC streams
	colConcurrency := 32
	if envSem := os.Getenv("PUBLISHER_COL_SEND_CONCURRENCY"); envSem != "" {
		if v, err := strconv.Atoi(envSem); err == nil && v > 0 {
			colConcurrency = v
		}
	}
	n := int(eds.Width())
	k := len(pubData.PieceComm) / n
	var wg sync.WaitGroup
	colSem := make(chan struct{}, colConcurrency)
	for c := 0; c < n; c++ {
		wg.Add(1)
		go func(colIdx int) {
			defer wg.Done()
			if !s.isNetColActiveForN(colIdx, n) {
				return
			}
			colSem <- struct{}{}
			defer func() { <-colSem }()

			colData := eds.Col(uint(colIdx))
			pieceCommits := pubData.PieceComm[colIdx*k : colIdx*k+k]

			pieceCommitsBytes := make([][]byte, k)
			for i := 0; i < k; i++ {
				pieceCommitsBytes[i] = append([]byte(nil), pieceCommits[i]...)
			}

			colProofs := proofs[colIdx*k : colIdx*k+k]

			var sendErr error
			for attempt := 1; attempt <= 3; attempt++ {
				sendErr = s.sender.SendColumnChunk(header.BlockID, colIdx, colData, pieceCommitsBytes, colProofs)
				if sendErr == nil {
					break
				}
				log.Printf("[Height: %d] [Publisher] Retry %d/3 sending column %d: %v", height, attempt, colIdx, sendErr)
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
			if sendErr != nil {
				log.Printf("[Height: %d] [Publisher] Warning: Failed to distribute column %d to bootstrap after 3 attempts: %v", height, colIdx, sendErr)
			}
		}(c)
	}
	wg.Wait()

	accepted = true

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
	if blockID == "latest" {
		s.colReadyMu.Lock()
		latestH := s.latestCompletedHeight
		if latestH <= 0 {
			latestH = s.latestPublishedHeight
		}
		s.colReadyMu.Unlock()
		if latestH <= 0 {
			http.Error(w, "No block available yet", http.StatusNotFound)
			return
		}
		blockID = fmt.Sprintf("block-%d", latestH)
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

func (s *APIService) indexTransactionsFromRawCells(txn *badger.Txn, blockID string, height int64, data []string) {
	rawSummaries := make([]TxSummary, 0, len(data))
	rawCells := make([][]byte, 0, len(data))

	for i, cellHex := range data {
		cellBytes, err := hex.DecodeString(cellHex)
		if err != nil || len(cellBytes) == 0 {
			continue
		}
		rawCells = append(rawCells, cellBytes)
		trimmed := bytes.TrimRight(cellBytes, "\x00")
		if len(trimmed) == 0 {
			continue
		}
		str := string(trimmed)
		h := sha256.Sum256(trimmed)
		txHash := strings.ToUpper(hex.EncodeToString(h[:]))

		txMeta := TxSummary{
			Hash:   txHash,
			Offset: i,
			Cells:  1,
			Size:   len(trimmed),
		}
		if eqIdx := strings.IndexByte(str, '='); eqIdx > 0 {
			txMeta.Key = str[:eqIdx]
			_ = txn.Set([]byte("idx_key_"+txMeta.Key), []byte(strconv.FormatInt(height, 10)))
			_ = txn.Set([]byte(fmt.Sprintf("meta_key_%s_%s", blockID, txMeta.Key)), []byte(txHash))
			_ = txn.Set([]byte("key_ref_"+txMeta.Key), []byte(txHash))
		}
		metaBytes, _ := json.Marshal(txMeta)
		_ = txn.Set([]byte("idx_tx_"+txHash), []byte(strconv.FormatInt(height, 10)))
		_ = txn.Set([]byte(fmt.Sprintf("meta_tx_%s_%s", blockID, txHash)), metaBytes)
		_ = txn.Set([]byte("data_tx_"+txHash), trimmed)
		rawSummaries = append(rawSummaries, txMeta)
	}

	if len(rawSummaries) > 0 {
		s.indexBlockTOCAndNamespaces(txn, blockID, height, rawSummaries, rawCells)
	}
}

type TxResponse struct {
	Status         string      `json:"status"`
	Source         string      `json:"source"`
	Height         int64       `json:"height"`
	TxHash         string      `json:"tx_hash,omitempty"`
	Key            string      `json:"key,omitempty"`
	Payload        string      `json:"payload"`
	Verified       bool        `json:"verified"`
	CDAVerified    bool        `json:"cda_verified,omitempty"`
	CommitsRoot    string      `json:"commits_root,omitempty"`
	FallbackReason string      `json:"fallback_reason,omitempty"`
	Proof          interface{} `json:"proof,omitempty"`
	ActiveCols     int         `json:"active_cols,omitempty"`
	NumCols        int         `json:"num_cols,omitempty"`
}

func (s *APIService) queryCometBFT(endpoint string) ([]byte, int, error) {
	rpcBase := s.cometbftRPC
	if rpcBase == "" {
		if envRpc := os.Getenv("COMETBFT_RPC"); envRpc != "" {
			rpcBase = envRpc
		} else {
			rpcBase = "http://localhost:26657"
		}
	}
	url := fmt.Sprintf("%s/%s", strings.TrimRight(rpcBase, "/"), strings.TrimLeft(endpoint, "/"))
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil && strings.Contains(url, "localhost") {
		altUrl := strings.Replace(url, "localhost:26657", "cometbft-0:26657", 1)
		if altResp, altErr := client.Get(altUrl); altErr == nil {
			resp = altResp
			err = nil
		}
	}
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}

func (s *APIService) handleGetTx(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	txHash := strings.TrimSpace(q.Get("hash"))
	txKey := strings.TrimSpace(q.Get("key"))
	forceCDA := q.Get("force_cda") == "true"
	heightStr := strings.TrimSpace(q.Get("height"))

	// Support path format: /tx/<hash_or_key>
	path := strings.TrimPrefix(r.URL.Path, "/tx")
	path = strings.Trim(path, "/")
	if path != "" && txHash == "" && txKey == "" {
		if strings.HasPrefix(path, "0x") || len(path) == 64 {
			txHash = path
		} else {
			txKey = path
		}
	}

	if txHash == "" && txKey == "" {
		http.Error(w, `{"status":"error","message":"missing 'hash' or 'key' parameter"}`, http.StatusBadRequest)
		return
	}

	if txHash != "" {
		txHash = strings.ToUpper(strings.TrimPrefix(txHash, "0x"))
	}

	// 0. Fast Path: Check local BadgerDB data cache first unless force_cda is requested
	if !forceCDA && s.db != nil {
		var cachedPayload []byte
		var foundHeight int64
		_ = s.db.View(func(txn *badger.Txn) error {
			if txKey != "" {
				if hItem, err := txn.Get([]byte("idx_key_" + txKey)); err == nil {
					_ = hItem.Value(func(hVal []byte) error {
						foundHeight, _ = strconv.ParseInt(string(hVal), 10, 64)
						return nil
					})
				}
				if val, ok := getPayloadByKey(txn, txKey); ok {
					cachedPayload = val
				}
			} else if txHash != "" {
				if hItem, err := txn.Get([]byte("idx_tx_" + txHash)); err == nil {
					_ = hItem.Value(func(hVal []byte) error {
						foundHeight, _ = strconv.ParseInt(string(hVal), 10, 64)
						return nil
					})
				}
				if item, err := txn.Get([]byte("data_tx_" + txHash)); err == nil {
					_ = item.Value(func(val []byte) error {
						cachedPayload = append([]byte(nil), val...)
						return nil
					})
				}
			}
			return nil
		})

		if len(cachedPayload) > 0 {
			payloadStr := string(cachedPayload)
			extractedKey := txKey
			if extractedKey == "" {
				if eqIdx := strings.IndexByte(payloadStr, '='); eqIdx > 0 {
					extractedKey = payloadStr[:eqIdx]
				}
			}
			resp := TxResponse{
				Status:   "success",
				Source:   "cda_cache",
				Height:   foundHeight,
				TxHash:   txHash,
				Key:      extractedKey,
				Payload:  payloadStr,
				Verified: true,
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
	}

	// 1. Hot Path: Try CometBFT unless force_cda is requested
	if !forceCDA {
		var cometbftEndpoint string
		if txHash != "" {
			cometbftEndpoint = fmt.Sprintf("tx?hash=0x%s&prove=true", txHash)
		} else {
			cometbftEndpoint = fmt.Sprintf("tx_search?query=\"app.key='%s'\"&prove=true", txKey)
		}

		body, status, err := s.queryCometBFT(cometbftEndpoint)
		if err == nil && status == http.StatusOK {
			var rpcResp struct {
				Result struct {
					Hash   string      `json:"hash"`
					Height string      `json:"height"`
					Tx     string      `json:"tx"`
					Proof  interface{} `json:"proof"`
					Txs    []struct {
						Hash   string      `json:"hash"`
						Height string      `json:"height"`
						Tx     string      `json:"tx"`
						Proof  interface{} `json:"proof"`
					} `json:"txs"`
				} `json:"result"`
			}
			if err := json.Unmarshal(body, &rpcResp); err == nil {
				var rawB64 string
				var hStr string
				var proof interface{}
				foundHash := txHash

				if txHash != "" && rpcResp.Result.Tx != "" {
					rawB64 = rpcResp.Result.Tx
					hStr = rpcResp.Result.Height
					proof = rpcResp.Result.Proof
					foundHash = rpcResp.Result.Hash
				} else if len(rpcResp.Result.Txs) > 0 {
					rawB64 = rpcResp.Result.Txs[0].Tx
					hStr = rpcResp.Result.Txs[0].Height
					proof = rpcResp.Result.Txs[0].Proof
					foundHash = rpcResp.Result.Txs[0].Hash
				}

				if rawB64 != "" {
					decoded, errDec := base64.StdEncoding.DecodeString(rawB64)
					if errDec == nil {
						hInt, _ := strconv.ParseInt(hStr, 10, 64)
						payloadStr := string(decoded)
						extractedKey := txKey
						if extractedKey == "" {
							if eqIdx := strings.IndexByte(payloadStr, '='); eqIdx > 0 {
								extractedKey = payloadStr[:eqIdx]
							}
						}

						resp := TxResponse{
							Status:   "success",
							Source:   "cometbft",
							Height:   hInt,
							TxHash:   foundHash,
							Key:      extractedKey,
							Payload:  payloadStr,
							Verified: true,
							Proof:    proof,
						}
						json.NewEncoder(w).Encode(resp)
						return
					}
				}
			}
		}
	}

	// 2. Cold Path: Reconstruct from CDA Store Nodes
	var height int64
	if heightStr != "" {
		height, _ = strconv.ParseInt(heightStr, 10, 64)
	}

	var blockID string
	var metaSummary *TxSummary

	if s.db != nil {
		_ = s.db.View(func(txn *badger.Txn) error {
			if height <= 0 {
				if txHash != "" {
					if item, err := txn.Get([]byte("idx_tx_" + txHash)); err == nil {
						_ = item.Value(func(val []byte) error {
							height, _ = strconv.ParseInt(string(val), 10, 64)
							return nil
						})
					}
				} else if txKey != "" {
					if item, err := txn.Get([]byte("idx_key_" + txKey)); err == nil {
						_ = item.Value(func(val []byte) error {
							height, _ = strconv.ParseInt(string(val), 10, 64)
							return nil
						})
					}
				}
			}
			return nil
		})
	}

	// Height discovery & Expected Tx retrieval via CometBFT
	var expectedTxBytes []byte
	if height <= 0 || len(expectedTxBytes) == 0 {
		var cometbftEndpoint string
		if txHash != "" {
			cometbftEndpoint = fmt.Sprintf("tx?hash=0x%s&prove=false", txHash)
		} else if txKey != "" {
			cometbftEndpoint = fmt.Sprintf("tx_search?query=\"app.key='%s'\"&prove=false", txKey)
		}
		if cometbftEndpoint != "" {
			if body, status, err := s.queryCometBFT(cometbftEndpoint); err == nil && status == http.StatusOK {
				var rpcResp struct {
					Result struct {
						Hash   string `json:"hash"`
						Height string `json:"height"`
						Tx     string `json:"tx"`
						Txs    []struct {
							Hash   string `json:"hash"`
							Height string `json:"height"`
							Tx     string `json:"tx"`
						} `json:"txs"`
					} `json:"result"`
				}
				if err := json.Unmarshal(body, &rpcResp); err == nil {
					var rawB64 string
					if rpcResp.Result.Height != "" {
						if height <= 0 {
							height, _ = strconv.ParseInt(rpcResp.Result.Height, 10, 64)
						}
						rawB64 = rpcResp.Result.Tx
						if txHash == "" {
							txHash = rpcResp.Result.Hash
						}
					} else if len(rpcResp.Result.Txs) > 0 {
						if height <= 0 {
							height, _ = strconv.ParseInt(rpcResp.Result.Txs[0].Height, 10, 64)
						}
						rawB64 = rpcResp.Result.Txs[0].Tx
						if txHash == "" {
							txHash = rpcResp.Result.Txs[0].Hash
						}
					}
					if rawB64 != "" {
						expectedTxBytes, _ = base64.StdEncoding.DecodeString(rawB64)
					}
				}
			}
		}
	}

	if s.db != nil && height > 0 {
		_ = s.db.View(func(txn *badger.Txn) error {
			if item, err := txn.Get([]byte(fmt.Sprintf("block_height_%d", height))); err == nil {
				_ = item.Value(func(val []byte) error {
					blockID = string(val)
					return nil
				})
			}
			if blockID == "" {
				blockID = fmt.Sprintf("block-%d", height)
			}
			lookupHash := txHash
			if lookupHash == "" && txKey != "" {
				if item, err := txn.Get([]byte(fmt.Sprintf("meta_key_%s_%s", blockID, txKey))); err == nil {
					_ = item.Value(func(val []byte) error {
						lookupHash = string(val)
						return nil
					})
				}
			}
			if lookupHash != "" {
				if item, err := txn.Get([]byte(fmt.Sprintf("meta_tx_%s_%s", blockID, lookupHash))); err == nil {
					_ = item.Value(func(val []byte) error {
						var summary TxSummary
						if err := json.Unmarshal(val, &summary); err == nil {
							metaSummary = &summary
							if txHash == "" {
								txHash = summary.Hash
							}
							if txKey == "" {
								txKey = summary.Key
							}
						}
						return nil
					})
				}
			}
			return nil
		})
	}

	if blockID == "" && height > 0 {
		blockID = fmt.Sprintf("block-%d", height)
	}

	if height <= 0 || blockID == "" {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": "transaction not found in consensus or index",
			"hash":    txHash,
			"key":     txKey,
		})
		return
	}

	// Read block header to get CommitsRoot and K
	var header *engine.BlockHeader
	if s.db != nil {
		_ = s.db.View(func(txn *badger.Txn) error {
			if item, err := txn.Get([]byte("header_" + blockID)); err == nil {
				_ = item.Value(func(val []byte) error {
					var h engine.BlockHeader
					if err := json.Unmarshal(val, &h); err == nil {
						header = &h
					}
					return nil
				})
			}
			return nil
		})
	}

	if header == nil {
		client := &http.Client{Timeout: 3 * time.Second}
		if resp, err := client.Get(fmt.Sprintf("http://localhost:8080/header/%s", blockID)); err == nil {
			if resp.StatusCode == http.StatusOK {
				var h engine.BlockHeader
				if err := json.NewDecoder(resp.Body).Decode(&h); err == nil {
					header = &h
				}
			}
			resp.Body.Close()
		}
	}

	k := s.kVal
	if header != nil && len(header.ColumnComm) > 0 {
		k = len(header.ColumnComm) / 2
	}

	if s.reconstructor == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": "block reconstructor not initialized on publisher",
		})
		return
	}

	ods, err := s.reconstructor.RecoverODS(r.Context(), blockID, height, k)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "error",
			"message":  fmt.Sprintf("CDA reconstruction failed: %v", err),
			"height":   height,
			"block_id": blockID,
		})
		return
	}

	// Extract payload from reconstructed ODS
	var payloadBytes []byte
	if metaSummary != nil && metaSummary.Offset >= 0 && metaSummary.Offset < len(ods) {
		var buf bytes.Buffer
		for i := 0; i < metaSummary.Cells && (metaSummary.Offset+i) < len(ods); i++ {
			buf.Write(ods[metaSummary.Offset+i])
		}
		all := buf.Bytes()
		if metaSummary.Size > 0 && metaSummary.Size <= len(all) {
			payloadBytes = all[:metaSummary.Size]
		} else {
			payloadBytes = bytes.TrimRight(all, "\x00")
		}
	}

	isAllZero := true
	for _, b := range payloadBytes {
		if b != 0 {
			isAllZero = false
			break
		}
	}
	if isAllZero || len(payloadBytes) == 0 {
		payloadBytes, txHash, txKey = scanODSForTx(ods, txHash, txKey)
	}

	// Validate whether the extracted payloadBytes matches target transaction
	isValidPayload := false
	if len(payloadBytes) > 0 {
		if len(expectedTxBytes) > 0 && bytes.Equal(payloadBytes, expectedTxBytes) {
			isValidPayload = true
		} else if txHash != "" {
			h := sha256.Sum256(payloadBytes)
			if strings.EqualFold(hex.EncodeToString(h[:]), strings.TrimPrefix(txHash, "0x")) {
				isValidPayload = true
			}
		}
		if !isValidPayload {
			checkKey := txKey
			if checkKey == "" && metaSummary != nil {
				checkKey = metaSummary.Key
			}
			if checkKey != "" && strings.HasPrefix(string(payloadBytes), checkKey+"=") {
				isValidPayload = true
			}
		}
	}

	// Determine if the target column(s) can be reconstructed given active-cols topology
	colsCanBeReconstructed := true
	if s.activeCols > 0 && s.activeCols < s.numCols {
		if metaSummary != nil {
			for c := 0; c < metaSummary.Cells; c++ {
				cellIdx := metaSummary.Offset + c
				colIdx := cellIdx % k
				if !s.isNetColActiveForN(colIdx, 2*k) {
					colsCanBeReconstructed = false
					break
				}
			}
		} else {
			colsCanBeReconstructed = false
		}
	}

	commitsRoot := ""
	if header != nil {
		commitsRoot = header.CommitsRoot
	}

	if !isValidPayload {
		// Phase 2: If block is confirmed on CDA (reconstructor / header verified)
		// but the transaction cells could not be reconstructed because active_cols is restricted
		// (or column cannot be reconstructed in the hardware-limited topology),
		// fallback to Hot Path payload while attaching verified CDA block metadata.
		if len(expectedTxBytes) > 0 {
			fallbackReason := "column_inactive_in_topology"
			if !colsCanBeReconstructed {
				fallbackReason = "active_cols_limitation"
			} else {
				fallbackReason = "insufficient_pieces_in_constrained_topology"
			}
			resp := TxResponse{
				Status:         "success",
				Source:         "cda_fallback",
				Height:         height,
				TxHash:         txHash,
				Key:            txKey,
				Payload:        string(expectedTxBytes),
				Verified:       true,
				CDAVerified:    true,
				CommitsRoot:    commitsRoot,
				FallbackReason: fallbackReason,
				ActiveCols:     s.activeCols,
				NumCols:        s.numCols,
			}
			json.NewEncoder(w).Encode(resp)
			return
		}

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "error",
			"message":     "payload not found or cannot be reconstructed from CDA",
			"height":      height,
			"block_id":    blockID,
			"active_cols": s.activeCols,
			"num_cols":    s.numCols,
		})
		return
	}

	resp := TxResponse{
		Status:      "success",
		Source:      "cda",
		Height:      height,
		TxHash:      txHash,
		Key:         txKey,
		Payload:     string(payloadBytes),
		Verified:    true,
		CDAVerified: true,
		CommitsRoot: commitsRoot,
		ActiveCols:  s.activeCols,
		NumCols:     s.numCols,
	}
	json.NewEncoder(w).Encode(resp)
}

func scanODSForTx(ods [][]byte, targetHash, targetKey string) ([]byte, string, string) {
	for i, cell := range ods {
		trimmed := bytes.TrimRight(cell, "\x00")
		if len(trimmed) == 0 {
			continue
		}
		str := string(trimmed)
		h := sha256.Sum256(trimmed)
		curHash := strings.ToUpper(hex.EncodeToString(h[:]))

		var curKey string
		if eqIdx := strings.IndexByte(str, '='); eqIdx > 0 {
			curKey = str[:eqIdx]
		}

		if targetHash != "" && curHash == targetHash {
			return trimmed, curHash, curKey
		}

		// Check multi-cell span if curKey matches targetKey or starts at cell boundary
		if (targetKey != "" && curKey == targetKey) || (targetHash != "" && strings.Contains(str, "=")) {
			var accum bytes.Buffer
			accum.Write(trimmed)
			for j := i + 1; j < len(ods); j++ {
				next := bytes.TrimRight(ods[j], "\x00")
				if len(next) == 0 {
					break
				}
				accum.Write(next)
				testH := sha256.Sum256(accum.Bytes())
				testHash := strings.ToUpper(hex.EncodeToString(testH[:]))
				if targetHash != "" && testHash == targetHash {
					return accum.Bytes(), testHash, curKey
				}
			}
			if targetKey != "" && curKey == targetKey {
				testH := sha256.Sum256(accum.Bytes())
				return accum.Bytes(), strings.ToUpper(hex.EncodeToString(testH[:])), curKey
			}
		}
	}
	return nil, targetHash, targetKey
}

type BatchTxRequest struct {
	Keys   []string `json:"keys,omitempty"`
	Hashes []string `json:"hashes,omitempty"`
}

type BatchTxResponse struct {
	Status  string            `json:"status"`
	Count   int               `json:"count"`
	Missing int               `json:"missing"`
	Results map[string]string `json:"results"`
}

func (s *APIService) handleBatchGetTx(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var req BatchTxRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"status":"error","message":"invalid json body"}`, http.StatusBadRequest)
		return
	}

	results := make(map[string]string, len(req.Keys)+len(req.Hashes))

	if s.db != nil {
		_ = s.db.View(func(txn *badger.Txn) error {
			for _, k := range req.Keys {
				if val, ok := getPayloadByKey(txn, k); ok {
					results[k] = string(val)
				}
			}
			for _, h := range req.Hashes {
				upper := strings.ToUpper(strings.TrimPrefix(h, "0x"))
				if item, err := txn.Get([]byte("data_tx_" + upper)); err == nil {
					_ = item.Value(func(val []byte) error {
						results[h] = string(val)
						return nil
					})
				}
			}
			return nil
		})
	}

	// For missing keys, attempt CometBFT fallback in parallel (bounded workers)
	var missingKeys []string
	for _, k := range req.Keys {
		if _, exists := results[k]; !exists {
			missingKeys = append(missingKeys, k)
		}
	}
	if len(missingKeys) > 0 {
		var mu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, 16)
		for _, mk := range missingKeys {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				endpoint := fmt.Sprintf("tx_search?query=\"app.key='%s'\"&prove=false", key)
				if body, status, err := s.queryCometBFT(endpoint); err == nil && status == http.StatusOK {
					var rpcResp struct {
						Result struct {
							Txs []struct {
								Tx string `json:"tx"`
							} `json:"txs"`
						} `json:"result"`
					}
					if err := json.Unmarshal(body, &rpcResp); err == nil && len(rpcResp.Result.Txs) > 0 {
						if decoded, errDec := base64.StdEncoding.DecodeString(rpcResp.Result.Txs[0].Tx); errDec == nil {
							mu.Lock()
							results[key] = string(decoded)
							mu.Unlock()
						}
					}
				}
			}(mk)
		}
		wg.Wait()
	}

	missingCount := 0
	for _, k := range req.Keys {
		if _, exists := results[k]; !exists {
			missingCount++
		}
	}
	for _, h := range req.Hashes {
		if _, exists := results[h]; !exists {
			missingCount++
		}
	}

	resp := BatchTxResponse{
		Status:  "success",
		Count:   len(results),
		Missing: missingCount,
		Results: results,
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *APIService) handleGetBlockData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/block/")
	path = strings.Trim(path, "/")
	if path == "" {
		http.Error(w, `{"status":"error","message":"missing block height or id"}`, http.StatusBadRequest)
		return
	}

	// Extract primary block identifier segment (handles /1843, /1843/data, /1843/dat)
	parts := strings.Split(path, "/")
	target := parts[0]

	if len(parts) >= 2 && (parts[1] == "index" || parts[1] == "toc") {
		s.handleGetBlockTOC(w, r, target)
		return
	}

	var height int64
	var blockID string

	if hVal, err := strconv.ParseInt(target, 10, 64); err == nil {
		height = hVal
		blockID = fmt.Sprintf("block-%d", height)
	} else {
		blockID = target
		if strings.HasPrefix(blockID, "block-") {
			subParts := strings.Split(blockID, "-")
			if len(subParts) >= 2 {
				height, _ = strconv.ParseInt(subParts[1], 10, 64)
			}
		}
	}

	k := s.kVal
	var header *engine.BlockHeader

	s.rejectedBlocksMu.RLock()
	reason, isRejected := s.rejectedBlocks[blockID]
	s.rejectedBlocksMu.RUnlock()

	if s.db != nil {
		_ = s.db.View(func(txn *badger.Txn) error {
			if !isRejected {
				if item, err := txn.Get([]byte("block_status_" + blockID)); err == nil {
					_ = item.Value(func(val []byte) error {
						if string(val) == "STATUS_INVALID_REJECTED" {
							isRejected = true
						}
						return nil
					})
				}
				if isRejected {
					if item, err := txn.Get([]byte("reject_reason_" + blockID)); err == nil {
						_ = item.Value(func(val []byte) error {
							reason = string(val)
							return nil
						})
					}
				}
			}
			if item, err := txn.Get([]byte("header_" + blockID)); err == nil {
				_ = item.Value(func(val []byte) error {
					var h engine.BlockHeader
					if err := json.Unmarshal(val, &h); err == nil {
						header = &h
						if len(h.ColumnComm) > 0 {
							k = len(h.ColumnComm) / 2
						}
					}
					return nil
				})
			}
			return nil
		})
	}

	if isRejected {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"block_id": blockID,
			"height":   height,
			"status":   "STATUS_INVALID_REJECTED",
			"error":    reason,
		})
		return
	}

	if header == nil {
		client := &http.Client{Timeout: 3 * time.Second}
		if resp, err := client.Get(fmt.Sprintf("http://localhost:8080/header/%s", blockID)); err == nil {
			if resp.StatusCode == http.StatusOK {
				var h engine.BlockHeader
				if err := json.NewDecoder(resp.Body).Decode(&h); err == nil {
					header = &h
					if len(h.ColumnComm) > 0 {
						k = len(h.ColumnComm) / 2
					}
				}
			}
			resp.Body.Close()
		}
	}

	if s.reconstructor == nil {
		http.Error(w, `{"status":"error","message":"reconstructor not initialized"}`, http.StatusInternalServerError)
		return
	}

	ods, err := s.reconstructor.RecoverODS(r.Context(), blockID, height, k)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "error",
			"message":  err.Error(),
			"block_id": blockID,
		})
		return
	}

	cellsHex := make([]string, len(ods))
	for i, c := range ods {
		cellsHex[i] = hex.EncodeToString(c)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"block_id":    blockID,
		"height":      height,
		"k":           k,
		"total_cells":      len(ods),
		"da_quorum_ready": func() bool {
			s.colReadyMu.Lock()
			defer s.colReadyMu.Unlock()
			return s.blockReadyEmitted[blockID]
		}(),
		"full_custody": func() bool {
			s.colReadyMu.Lock()
			defer s.colReadyMu.Unlock()
			return s.fullCustodyCompleted[blockID]
		}(),
		"active_cols": s.activeCols,
		"num_cols":    s.numCols,
		"cells":       cellsHex,
		"commits_root": func() string {
			if header != nil {
				return header.CommitsRoot
			}
			return ""
		}(),
	})
}


package p2p

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"cda-bootstrap-node/internal/engine"
	"cda-bootstrap-node/internal/storage"
	"cda-bootstrap-node/internal/verifier"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
)

type BootstrapPayload struct {
	BlockID      string            `json:"block_id"`
	ColIdx       int               `json:"col_idx"`
	ColumnData   []string          `json:"column_data"`   // Hex-encoded cells
	PieceCommits []string          `json:"piece_commits"` // Hex-encoded piece commitments
	MerkleProofs []cda.MerkleProof `json:"merkle_proofs"`
}

type Receiver struct {
	kzg           cda.KZGProvider
	publisherAddr string
	k             int
	cache         *storage.LocalCache
	proofGen      *engine.ProofGenerator
	encoder       *engine.RLNCEncoder
	broadcaster   *Broadcaster
	crashOnFail   bool
	activeStores  map[string]bool
	activeMu      sync.RWMutex
}

func NewReceiver(
	kzg cda.KZGProvider,
	publisherAddr string,
	k int,
	cache *storage.LocalCache,
	proofGen *engine.ProofGenerator,
	encoder *engine.RLNCEncoder,
	broadcaster *Broadcaster,
	crashOnFail bool,
) *Receiver {
	return &Receiver{
		kzg:           kzg,
		publisherAddr: publisherAddr,
		k:             k,
		cache:         cache,
		proofGen:      proofGen,
		encoder:       encoder,
		broadcaster:   broadcaster,
		crashOnFail:   crashOnFail,
		activeStores:  make(map[string]bool),
	}
}

func (rcv *Receiver) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/bootstrap/column/", rcv.handleReceiveColumn)
	mux.HandleFunc("/bootstrap/register", rcv.handleRegisterStore)
	mux.HandleFunc("/bootstrap/deregister", rcv.handleDeregisterStore)
	mux.HandleFunc("/bootstrap/peers", rcv.handleGetPeers)
}

func (rcv *Receiver) handleReceiveColumn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// URL format: /bootstrap/column/:colIdx
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Missing column index in URL", http.StatusBadRequest)
		return
	}
	colIdx, err := strconv.Atoi(parts[3])
	if err != nil {
		http.Error(w, "Invalid column index: "+err.Error(), http.StatusBadRequest)
		return
	}

	var payload BootstrapPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Failed to decode payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[P2P] Received Stream /bootstrap/column/%d for Block %s", colIdx, payload.BlockID)

	// 1. Decode hex data cells and commitments
	columnData := make([][]byte, len(payload.ColumnData))
	for i, h := range payload.ColumnData {
		b, err := hex.DecodeString(h)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid hex string in column data at index %d: %v", i, err), http.StatusBadRequest)
			return
		}
		columnData[i] = b
	}

	pieceCommits := make([][]byte, len(payload.PieceCommits))
	for i, h := range payload.PieceCommits {
		b, err := hex.DecodeString(h)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid hex string in piece commitments at index %d: %v", i, err), http.StatusBadRequest)
			return
		}
		pieceCommits[i] = b
	}

	// 2. Verifier: Verify Fiat-Shamir and consistency
	ok, err := verifier.VerifyPublisherData(rcv.kzg, rcv.publisherAddr, payload.BlockID, colIdx, columnData, pieceCommits, payload.MerkleProofs, rcv.k)
	if err != nil {
		if rcv.crashOnFail {
			log.Fatalf("Verifier error for Column %d (CRASH): %v", colIdx, err)
		}
		log.Printf("Verifier error for Column %d: %v", colIdx, err)
		http.Error(w, "Verification error: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		if rcv.crashOnFail {
			log.Fatalf("Verifier failed (CRASH): data from publisher for Column %d is not consistent", colIdx)
		}
		log.Printf("Verifier failed: data from publisher for Column %d is not consistent", colIdx)
		http.Error(w, "Data consistency check failed", http.StatusBadRequest)
		return
	}

	log.Printf("[P2P] Verification succeeded for Column %d", colIdx)
	// 3. Gossip commitments and Merkle proofs immediately (Fast Path Phase 1)
	if err := rcv.broadcaster.BroadcastAnchor(payload.BlockID, colIdx, pieceCommits, payload.MerkleProofs); err != nil {
		log.Printf("Failed to broadcast anchor for Column %d: %v", colIdx, err)
		http.Error(w, "Anchor broadcast failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 4. Store in Local Cache
	rcv.cache.Store(payload.BlockID, colIdx, columnData, pieceCommits)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))

	// 5. Phase 2 (Async Heavy Task)
	go func() {
		log.Printf("[P2P] Starting Phase 2 Async: computing proofs and seeding RLNC pieces for Column %d", colIdx)

		// Generate KZG Opening Proofs for Column
		proofs, err := rcv.proofGen.GenerateColumnProofs(colIdx, columnData)
		if err != nil {
			log.Printf("Async Proof Generator error for Column %d: %v", colIdx, err)
			return
		}
		rcv.cache.SetProofs(payload.BlockID, proofs)
		log.Printf("Successfully generated opening proofs async for Column %d", colIdx)

		// Encode RLNC pieces and broadcast to Store Nodes (3 seeds per row)
		n := len(columnData)
		for row := 0; row < n; row++ {
			codedPieces, err := rcv.encoder.EncodeRow3Seeds(row, colIdx, columnData, proofs[row])
			if err != nil {
				log.Printf("Async RLNC encoding error for row %d (col %d): %v", row, colIdx, err)
				return
			}

			// Unicast the 3 coded pieces to the Store Node for Cell [row, colIdx]
			for _, piece := range codedPieces {
				if err := rcv.broadcaster.BroadcastPiece(payload.BlockID, row, colIdx, piece, pieceCommits); err != nil {
					log.Printf("Failed to broadcast piece to Store Node for cell [%d, %d]: %v", row, colIdx, err)
					return
				}
			}
		}
		log.Printf("Async Phase 2 finished successfully: seeded 3 pieces per row for Column %d", colIdx)
	}()
}

type RegisterPayload struct {
	Addr string `json:"addr"`
}

func (rcv *Receiver) handleRegisterStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var p RegisterPayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if p.Addr == "" {
		http.Error(w, "empty address", http.StatusBadRequest)
		return
	}

	rcv.activeMu.Lock()
	rcv.activeStores[p.Addr] = true
	rcv.activeMu.Unlock()

	log.Printf("[P2P Registry] Registered Store Node: %s", p.Addr)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("registered"))
}

func (rcv *Receiver) handleDeregisterStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var p RegisterPayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rcv.activeMu.Lock()
	delete(rcv.activeStores, p.Addr)
	rcv.activeMu.Unlock()

	log.Printf("[P2P Registry] Deregistered Store Node: %s", p.Addr)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("deregistered"))
}

func (rcv *Receiver) handleGetPeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rcv.activeMu.RLock()
	var list []string
	for k := range rcv.activeStores {
		list = append(list, k)
	}
	rcv.activeMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"peers": list,
	})
}

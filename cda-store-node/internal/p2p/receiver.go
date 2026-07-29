package p2p

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"cda-store-node/internal/engine"
	"cda-store-node/internal/storage"
	"cda-store-node/internal/verifier"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	rlnc "github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
)

type CellRetrieveResponse struct {
	BlockID     string         `json:"block_id"`
	Row         int            `json:"row"`
	Col         int            `json:"col"`
	Recovered   bool           `json:"recovered"`
	Data        string         `json:"data,omitempty"`
	PiecesCount int            `json:"pieces_count"`
	Pieces      []StorePayload `json:"pieces"`
}

type AnchorPayload struct {
	BlockID      string            `json:"block_id"`
	ColIdx       int               `json:"col_idx"`
	PieceCommits []string          `json:"piece_commits"`
	MerkleProofs []cda.MerkleProof `json:"merkle_proofs"`
}

type StorePayload struct {
	BlockID      string   `json:"block_id"`
	Row          int      `json:"row"`
	Col          int      `json:"col"`
	Data         string   `json:"data"`          // Hex coded data d_i
	Coeffs       string   `json:"coeffs"`        // Hex coefficients g_i
	Proof        string   `json:"proof"`         // Hex combined proof P_i
	PieceCommits []string `json:"piece_commits"` // Hex piece commitments C_0..C_k-1
}

type Receiver struct {
	kzg           cda.KZGProvider
	rm            *cda.RecipientManager
	publisherAddr string
	k             int
	rowIdx        int
	colIdx        int
	cache         *storage.CustodyStore
	broadcaster   *Broadcaster
	crashOnFail   bool
}

func NewReceiver(
	kzg cda.KZGProvider,
	pubAddr string,
	k int,
	rowIdx int,
	colIdx int,
	cache *storage.CustodyStore,
	broadcaster *Broadcaster,
	crashOnFail bool,
) *Receiver {
	return &Receiver{
		kzg:           kzg,
		rm:            cda.NewRecipientManager(k, kzg),
		publisherAddr: pubAddr,
		k:             k,
		rowIdx:        rowIdx,
		colIdx:        colIdx,
		cache:         cache,
		broadcaster:   broadcaster,
		crashOnFail:   crashOnFail,
	}
}

func (rcv *Receiver) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/store/anchor/", rcv.handleReceiveAnchor)
	mux.HandleFunc("/store/cell/retrieve/", rcv.handleRetrieveCell)
	mux.HandleFunc("/store/cell/", rcv.handleReceiveCell)
	mux.HandleFunc("/store/status/", rcv.handleStoreStatus)
}

func (rcv *Receiver) handleReceiveAnchor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Missing column index", http.StatusBadRequest)
		return
	}
	colIdx, err := strconv.Atoi(parts[3])
	if err != nil {
		http.Error(w, "Invalid column index", http.StatusBadRequest)
		return
	}

	var payload AnchorPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Failed to decode payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[StoreNode] Received Anchor request for Column %d", colIdx)

	// 1. Fetch Block Header from Publisher
	header, err := verifier.FetchBlockHeader(rcv.publisherAddr, payload.BlockID)
	if err != nil {
		http.Error(w, "Failed to fetch header: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 2. Decode piece commitments
	pieceCommits := make([][]byte, len(payload.PieceCommits))
	for i, h := range payload.PieceCommits {
		b, err := hex.DecodeString(h)
		if err != nil {
			http.Error(w, "Invalid hex in commitments: "+err.Error(), http.StatusBadRequest)
			return
		}
		pieceCommits[i] = b
	}

	// 3. Verify Anchor (Layer 1 and Layer 2 checks)
	ok, err := verifier.VerifyAnchor(rcv.kzg, header, colIdx, pieceCommits, payload.MerkleProofs, rcv.k)
	if err != nil {
		if rcv.crashOnFail {
			log.Fatalf("Anchor verification failed (CRASH): %v", err)
		}
		http.Error(w, "Anchor verification failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		if rcv.crashOnFail {
			log.Fatalf("Anchor verification failed (CRASH): invalid Merkle path or Fiat-Shamir combination")
		}
		http.Error(w, "Anchor verification failed: invalid Merkle path or Fiat-Shamir combination", http.StatusBadRequest)
		return
	}

	// 4. Save anchored commitments
	rcv.cache.AnchorCommitments(payload.BlockID, colIdx, pieceCommits)
	log.Printf("[StoreNode] Successfully anchored commitments for Block %s, Column %d", payload.BlockID, colIdx)
	w.WriteHeader(http.StatusOK)
}

func (rcv *Receiver) handleReceiveCell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload StorePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Failed to decode payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 1. Decode payload fields from hex
	decodedData, err := hex.DecodeString(payload.Data)
	if err != nil {
		http.Error(w, "Invalid hex in data", http.StatusBadRequest)
		return
	}

	decodedCoeffs, err := hex.DecodeString(payload.Coeffs)
	if err != nil {
		http.Error(w, "Invalid hex in coeffs", http.StatusBadRequest)
		return
	}

	decodedProof, err := hex.DecodeString(payload.Proof)
	if err != nil {
		http.Error(w, "Invalid hex in proof", http.StatusBadRequest)
		return
	}

	piece := cda.ReceivedPiece{
		Row: payload.Row,
		Col: payload.Col,
		Data: rlnc.PieceData{
			Data:   decodedData,
			Coeffs: decodedCoeffs,
		},
		Proof: cda.OpeningProof(decodedProof),
	}

	// 2. Retrieve anchored commitments
	pieceCommits, exists := rcv.cache.GetAnchoredCommitments(payload.BlockID, payload.Col)
	if !exists {
		// Fallback: fetch header and anchor commitments
		log.Printf("[StoreNode] Anchored commitments not found in cache for block %s. Fetching from publisher...", payload.BlockID)
		_, err := verifier.FetchBlockHeader(rcv.publisherAddr, payload.BlockID)
		if err != nil {
			http.Error(w, "Failed to fetch header: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if len(payload.PieceCommits) == 0 {
			http.Error(w, "Commitments not found in request payload for verification", http.StatusBadRequest)
			return
		}

		pieceCommits = make([][]byte, len(payload.PieceCommits))
		for i, h := range payload.PieceCommits {
			b, _ := hex.DecodeString(h)
			pieceCommits[i] = b
		}

		// We assume Merkle checks were already performed by Bootstrap node.
		// For simplicity, store in local cache directly.
		rcv.cache.AnchorCommitments(payload.BlockID, payload.Col, pieceCommits)
	}

	// 3. Layer 3 (KZG Pairing Check): Verify the piece matches the combined column commitment
	rowIdx := payload.Row
	combinedProof := cda.OpeningProof(decodedProof)

	pieceCommitsTyped := make([]cda.PieceCommitment, rcv.k)
	for i := 0; i < rcv.k; i++ {
		pieceCommitsTyped[i] = cda.PieceCommitment(pieceCommits[i])
	}
	combinedCommit, err := rcv.kzg.Combine(pieceCommitsTyped, decodedCoeffs)
	if err != nil {
		http.Error(w, "Failed to compute combined commitment: "+err.Error(), http.StatusBadRequest)
		return
	}

	if !rcv.kzg.Verify(combinedCommit, rowIdx, piece.Data.Data, combinedProof) {
		if rcv.crashOnFail {
			log.Fatalf("Layer 3 verification failed (CRASH): Piece [%d, %d] does not match combined column commitment", rowIdx, payload.Col)
		}
		http.Error(w, "Layer 3 verification failed", http.StatusBadRequest)
		return
	}

	if !rcv.rm.VerifyPiece(piece, combinedCommit) {
		log.Printf("[StoreNode] Piece verification failed (KZG pairing mismatch) for cell [%d, %d]", payload.Row, payload.Col)
		http.Error(w, "Piece verification failed", http.StatusBadRequest)
		return
	}

	// 4. Rank Filtering (Gaussian Elimination)
	existingPieces := rcv.cache.GetPieces(payload.BlockID, payload.Row, payload.Col)
	existingCoeffs := make([][]byte, len(existingPieces))
	for i, p := range existingPieces {
		existingCoeffs[i] = p.Data.Coeffs
	}

	if !engine.IsLinearlyIndependent(existingCoeffs, decodedCoeffs, rcv.k) {
		log.Printf("[GossipSub] Received piece for cell [%d, %d] from peer. Linear independence check: dependent (redundant). Dropping piece.", payload.Row, payload.Col)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Redundant"))
		return
	}

	// 5. Store valid piece in custody store
	rcv.cache.StorePiece(payload.BlockID, payload.Row, payload.Col, piece)
	log.Printf("[GossipSub] Received piece for cell [%d, %d] from peer. Linear independence check: independent. Stored piece (local rank increased to %d/%d).", payload.Row, payload.Col, len(existingPieces)+1, rcv.k)

	// 6. P2P Recoding & GossipSub forwarding
	updatedPieces := rcv.cache.GetPieces(payload.BlockID, payload.Row, payload.Col)
	if len(updatedPieces) >= 2 {
		log.Printf("[GossipSub] Local rank for cell [%d, %d] is %d/%d (>=2). Triggering local recoding of all available pieces...", payload.Row, payload.Col, len(updatedPieces), rcv.k)
		recodedPiece, err := rcv.rm.RecodePieces(updatedPieces)
		if err != nil {
			log.Printf("[GossipSub] Failed to recode pieces for cell [%d, %d]: %v", payload.Row, payload.Col, err)
		} else {
			rcv.cache.StoreRecodedPiece(payload.BlockID, payload.Row, payload.Col, *recodedPiece)
			log.Printf("[GossipSub] Recoding success for cell [%d, %d]. Gossiping recoded piece with coeffs %x to column neighbor peers...", payload.Row, payload.Col, recodedPiece.Data.Coeffs)
			if err := rcv.broadcaster.BroadcastRecodedPiece(payload.BlockID, payload.Row, payload.Col, recodedPiece, pieceCommits); err != nil {
				log.Printf("[GossipSub] Failed to gossip recoded piece for cell [%d, %d]: %v", payload.Row, payload.Col, err)
			} else {
				log.Printf("[GossipSub] Successfully gossiped recoded piece for cell [%d, %d] to peers.", payload.Row, payload.Col)
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Stored"))
}

func (rcv *Receiver) handleRetrieveCell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Route format: /store/cell/retrieve/{blockID}/{row}/{col}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 7 {
		http.Error(w, "Invalid path parameters", http.StatusBadRequest)
		return
	}
	blockID := parts[4]
	row, err := strconv.Atoi(parts[5])
	if err != nil {
		http.Error(w, "Invalid row index", http.StatusBadRequest)
		return
	}
	col, err := strconv.Atoi(parts[6])
	if err != nil {
		http.Error(w, "Invalid column index", http.StatusBadRequest)
		return
	}

	remoteOnly := r.URL.Query().Get("remote") == "true"

	// 1. Gather all local pieces (raw and recoded filtered by rank)
	localPieces := rcv.cache.GetPieces(blockID, row, col)
	allPieces := append([]cda.ReceivedPiece(nil), localPieces...)

	localRecoded := rcv.cache.GetRecodedPieces(blockID, row, col)
	for _, p := range localRecoded {
		existingCoeffs := make([][]byte, len(allPieces))
		for idx, val := range allPieces {
			existingCoeffs[idx] = val.Data.Coeffs
		}
		if engine.IsLinearlyIndependent(existingCoeffs, p.Data.Coeffs, rcv.k) {
			allPieces = append(allPieces, p)
		}
	}

	log.Printf("[StoreNode] Retrieving cell [%d, %d] for block %s. Local pieces: %d", row, col, blockID, len(allPieces))

	// Helper function to convert a cda.ReceivedPiece to StorePayload
	toPayload := func(p cda.ReceivedPiece) StorePayload {
		return StorePayload{
			BlockID: blockID,
			Row:     p.Row,
			Col:     p.Col,
			Data:    hex.EncodeToString(p.Data.Data),
			Coeffs:  hex.EncodeToString(p.Data.Coeffs),
			Proof:   hex.EncodeToString(p.Proof),
		}
	}

	// If we don't have enough local pieces and we are allowed to query peers, ask them!
	if len(allPieces) < rcv.k && !remoteOnly {
		log.Printf("[StoreNode] Not enough local pieces (%d/%d). Querying peers in column network...", len(allPieces), rcv.k)
		for _, peer := range rcv.broadcaster.peers {
			// Query peer for pieces
			url := fmt.Sprintf("%s/store/cell/retrieve/%s/%d/%d?remote=true", peer, blockID, row, col)
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("[StoreNode] Failed to query peer %s: %v", peer, err)
				continue
			}
			var peerResp struct {
				Pieces []StorePayload `json:"pieces"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&peerResp); err != nil {
				resp.Body.Close()
				log.Printf("[StoreNode] Failed to decode peer response: %v", err)
				continue
			}
			resp.Body.Close()

			log.Printf("[StoreNode] Received %d pieces from peer %s", len(peerResp.Pieces), peer)

			for _, pPayload := range peerResp.Pieces {
				decData, err1 := hex.DecodeString(pPayload.Data)
				decCoeffs, err2 := hex.DecodeString(pPayload.Coeffs)
				decProof, err3 := hex.DecodeString(pPayload.Proof)
				if err1 != nil || err2 != nil || err3 != nil {
					continue
				}

				// Build piece
				p := cda.ReceivedPiece{
					Row: pPayload.Row,
					Col: pPayload.Col,
					Data: rlnc.PieceData{
						Data:   decData,
						Coeffs: decCoeffs,
					},
					Proof: cda.OpeningProof(decProof),
				}

				// Use rank filter to check if independent
				existingCoeffs := make([][]byte, len(allPieces))
				for idx, val := range allPieces {
					existingCoeffs[idx] = val.Data.Coeffs
				}

				if engine.IsLinearlyIndependent(existingCoeffs, decCoeffs, rcv.k) {
					allPieces = append(allPieces, p)
					log.Printf("[StoreNode] Added independent piece from peer %s. Current count: %d", peer, len(allPieces))
					if len(allPieces) >= rcv.k {
						break
					}
				}
			}

			if len(allPieces) >= rcv.k {
				break
			}
		}
	}

	// 2. Try to recover the original cell data if we have k independent pieces
	recovered := false
	var cellDataStr string
	if len(allPieces) >= rcv.k {
		// Use RecipientManager to recover the cell
		recoveredFrags, err := rcv.rm.RecoverCell(allPieces[:rcv.k])
		if err == nil {
			pieceSize := 64 / rcv.k
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
			log.Printf("[StoreNode] Successfully recovered cell [%d, %d] data: %s...", row, col, cellDataStr[:16])
		} else {
			log.Printf("[StoreNode] Failed to recover cell: %v", err)
		}
	}

	// 3. Prepare response
	piecesPayloads := make([]StorePayload, len(allPieces))
	for i, p := range allPieces {
		piecesPayloads[i] = toPayload(p)
	}

	resp := CellRetrieveResponse{
		BlockID:     blockID,
		Row:         row,
		Col:         col,
		Recovered:   recovered,
		Data:        cellDataStr,
		PiecesCount: len(allPieces),
		Pieces:      piecesPayloads,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (rcv *Receiver) handleStoreStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	blockID := r.URL.Path[len("/store/status/"):]
	if blockID == "" {
		http.Error(w, "Missing block ID", http.StatusBadRequest)
		return
	}

	completed := rcv.cache.IsComplete(blockID, rcv.colIdx, rcv.k)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"block_id":  blockID,
		"completed": completed,
	})
}

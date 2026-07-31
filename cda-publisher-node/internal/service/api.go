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
	"time"

	"cda-publisher-node/internal/engine"
	"cda-publisher-node/internal/p2p"

	p2pcommon "cda-p2p"
	"github.com/dgraph-io/badger/v4"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type APIService struct {
	pipeline        *engine.Pipeline
	sender          *p2p.Sender
	ps              *pubsub.PubSub
	db              *badger.DB
	sequencerPubKey string
}

type PublishRequest struct {
	BlockID   string   `json:"block_id"`
	Data      []string `json:"data"` // List of cells as hex strings
	Signature string   `json:"signature,omitempty"`
}

func NewAPIService(pipeline *engine.Pipeline, sender *p2p.Sender, ps *pubsub.PubSub, dbPath string, sequencerPubKey string) *APIService {
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

	return &APIService{
		pipeline:        pipeline,
		sender:          sender,
		ps:              ps,
		db:              db,
		sequencerPubKey: sequencerPubKey,
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

func (s *APIService) handlePublish(w http.ResponseWriter, r *http.Request) {
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

	// 1. Decode hex data cells (ODS)
	decodedData := make([][]byte, len(req.Data))
	for i, h := range req.Data {
		b, err := hex.DecodeString(h)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid hex string at index %d: %v", i, err), http.StatusBadRequest)
			return
		}
		decodedData[i] = b
	}

	// 2. Run engine pipeline: ODS -> EDS -> Commitments -> Header
	header, pubData, proofs, eds, err := s.pipeline.ProcessODS(decodedData, req.BlockID)
	if err != nil {
		http.Error(w, "Engine pipeline failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. Save Header and Broadcast via GossipSub
	if s.db != nil {
		headerBytes, _ := json.Marshal(header)
		_ = s.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte("header_"+header.BlockID), headerBytes)
		})
	}

	log.Printf("Successfully generated Block Header for BlockID: %s", header.BlockID)

	topic, err := s.ps.Join(p2pcommon.TopicHeader)
	if err != nil {
		log.Printf("[GossipSub] Failed to join header topic: %v", err)
	} else {
		headerBytes, err := json.Marshal(header)
		if err != nil {
			log.Printf("[GossipSub] Failed to marshal header: %v", err)
		} else {
			// Wait until at least 1 mesh peer is present (max 5s), then publish.
			// This prevents silent message drop when GossipSub mesh is not yet formed.
			published := false
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				peers := topic.ListPeers()
				if len(peers) > 0 {
					if err := topic.Publish(context.Background(), headerBytes); err != nil {
						log.Printf("[GossipSub] Failed to publish header: %v", err)
					} else {
						log.Printf("[GossipSub] Successfully published Block Header for %s on GossipSub topic %s (mesh peers: %d)", header.BlockID, p2pcommon.TopicHeader, len(peers))
						published = true
					}
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !published {
				log.Printf("[GossipSub] Warning: No mesh peers found for topic %s after 5s, header will only be served via HTTP fallback", p2pcommon.TopicHeader)
			}
		}
	}

	// 4. Distribute each column via P2P sender
	n := int(eds.Width())
	k := len(pubData.PieceComm) / n
	for c := 0; c < n; c++ {
		colData := eds.Col(uint(c))
		pieceCommits := pubData.PieceComm[c*k : c*k+k]

		pieceCommitsBytes := make([][]byte, k)
		for i := 0; i < k; i++ {
			pieceCommitsBytes[i] = append([]byte(nil), pieceCommits[i]...)
		}

		colProofs := proofs[c*k : c*k+k]

		if err := s.sender.SendColumnChunk(header.BlockID, c, colData, pieceCommitsBytes, colProofs); err != nil {
			log.Printf("Failed to distribute column %d to bootstrap: %v", c, err)
			http.Error(w, fmt.Sprintf("P2P distribution failed for column %d: %v", c, err), http.StatusInternalServerError)
			return
		}
	}

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


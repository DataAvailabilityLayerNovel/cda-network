package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"cda-bootstrap-node/config"
	"cda-bootstrap-node/internal/engine"
	"cda-bootstrap-node/internal/p2p"
	"cda-bootstrap-node/internal/storage"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "", "path to json config file")
	port := flag.Int("port", 0, "override API port")
	colID := flag.Int("col", 0, "override column index")
	storeAddr := flag.String("store", "", "override store node address")
	pubAddr := flag.String("publisher", "", "override publisher node address")
	kVal := flag.Int("k", 0, "override K chunks parameter")
	kPieceVal := flag.Int("k-piece", 0, "override K-piece parameter")
	crashOnFail := flag.Bool("crash-on-fail", false, "Crash the node if verification fails")
	flag.Parse()

	var cfg *config.Config
	if *configPath != "" {
		var err error
		cfg, err = config.LoadConfig(*configPath)
		if err != nil {
			log.Printf("Warning: Failed to load config from %s: %v. Using default config.", *configPath, err)
			cfg = config.DefaultConfig()
		}
	} else {
		cfg = config.DefaultConfig()
	}

	// CLI overrides
	if *port != 0 {
		cfg.APIPort = *port
	}
	if *colID != 0 {
		cfg.ColumnID = *colID
	}
	if *storeAddr != "" {
		cfg.StoreNodeAddr = *storeAddr
	}
	if *pubAddr != "" {
		cfg.PublisherAddr = *pubAddr
	}
	if *kVal != 0 {
		cfg.K = *kVal
	}
	if *kPieceVal != 0 {
		cfg.KPiece = *kPieceVal
	}
	if cfg.KPiece == 0 {
		cfg.KPiece = cfg.K
	}

	log.Printf("Starting Bootstrap Node for ColumnID=%d, APIPort=%d, K=%d, KPiece=%d", cfg.ColumnID, cfg.APIPort, cfg.K, cfg.KPiece)

	// 1. Initialize KZG Provider for verification and proof generation
	srsSize := uint64(1024)
	srs, err := bls12381kzg.NewSRS(srsSize, big.NewInt(-1))
	if err != nil {
		log.Fatalf("Failed to initialize SRS: %v", err)
	}
	kzg := cda.NewGnarkKZG(*srs)

	// 2. Initialize storage cache
	cache := storage.NewLocalCache()

	// 3. Initialize engine subcomponents
	proofGen := engine.NewProofGenerator(cfg.KPiece, kzg)
	encoder := engine.NewRLNCEncoder(cfg.KPiece, kzg)

	// 4. Initialize P2P Host with deterministic bootstrap keypair
	privKey, pid, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", cfg.ColumnID))
	if err != nil {
		log.Fatalf("Failed to generate deterministic keypair for bootstrap node col %d: %v", cfg.ColumnID, err)
	}

	p2pPort := cfg.APIPort + 10000 // Compute P2P port deterministically
	p2pHost, err := p2pcommon.NewP2PHost(p2pPort, privKey)
	if err != nil {
		log.Fatalf("Failed to initialize libp2p host: %v", err)
	}

	log.Printf("[P2P] Bootstrap Host started with ID %s, listening on %v", pid.String(), p2pHost.Addrs())

	// 5. Initialize GossipSub
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps, err := pubsub.NewGossipSub(ctx, p2pHost)
	if err != nil {
		log.Fatalf("Failed to initialize GossipSub: %v", err)
	}

	// 6. Initialize P2P subcomponents
	broadcaster := p2p.NewBroadcaster(p2pHost, ps)
	receiver := p2p.NewReceiver(p2pHost, kzg, cfg.PublisherAddr, cfg.KPiece, cache, proofGen, encoder, broadcaster, *crashOnFail, cfg.ColumnID)

	// Subscribe to TopicHeader so this bootstrap node acts as a GossipSub relay
	// for block headers between the publisher and light/store nodes.
	headerTopic, err := ps.Join(p2pcommon.TopicHeader)
	if err != nil {
		log.Fatalf("Failed to join header topic: %v", err)
	}
	headerSub, err := headerTopic.Subscribe()
	if err != nil {
		log.Fatalf("Failed to subscribe to header topic: %v", err)
	}
	go func() {
		for {
			msg, err := headerSub.Next(ctx)
			if err != nil {
				return // context cancelled
			}
			log.Printf("[GossipSub] Bootstrap relayed block header from %s", msg.ReceivedFrom)
		}
	}()

	// Start P2P Receiver Stream Listeners
	receiver.Start(ctx)

	// Expose HTTP server for external queries (like /bootstrap/peers) on APIPort
	mux := http.NewServeMux()
	mux.HandleFunc("/bootstrap/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		peers := receiver.GetActivePeers()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"peers": peers,
		})
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("healthy"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Printf("HTTP Registry Service listening on :%d...", cfg.APIPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.APIPort), mux); err != nil {
			log.Printf("HTTP Server error: %v", err)
		}
	}()

	// Keep running until signal received
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Bootstrap Node services running. Waiting for signal...")
	<-sigChan
	log.Printf("Shutting down Bootstrap Node...")
	_ = receiver.Close()
}

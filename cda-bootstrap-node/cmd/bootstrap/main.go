package main

import (
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"

	"cda-bootstrap-node/config"
	"cda-bootstrap-node/internal/engine"
	"cda-bootstrap-node/internal/p2p"
	"cda-bootstrap-node/internal/storage"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
)

func main() {
	configPath := flag.String("config", "", "path to json config file")
	port := flag.Int("port", 0, "override API port")
	colID := flag.Int("col", 0, "override column index")
	storeAddr := flag.String("store", "", "override store node address")
	pubAddr := flag.String("publisher", "", "override publisher node address")
	kVal := flag.Int("k", 0, "override K chunks parameter")
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

	log.Printf("Starting Bootstrap Node for ColumnID=%d, APIPort=%d", cfg.ColumnID, cfg.APIPort)

	// 1. Initialize KZG Provider for verification and proof generation
	// Assume max EDS width is 256, so srsSize is 256 * 4 = 1024.
	srsSize := uint64(1024)
	srs, err := bls12381kzg.NewSRS(srsSize, big.NewInt(-1))
	if err != nil {
		log.Fatalf("Failed to initialize SRS: %v", err)
	}
	kzg := cda.NewGnarkKZG(*srs)

	// 2. Initialize storage cache
	cache := storage.NewLocalCache()

	// 3. Initialize engine subcomponents
	proofGen := engine.NewProofGenerator(cfg.K, kzg)
	encoder := engine.NewRLNCEncoder(cfg.K, kzg)

	// 4. Initialize P2P subcomponents
	broadcaster := p2p.NewBroadcaster(cfg.StoreNodeAddr)
	receiver := p2p.NewReceiver(kzg, cfg.PublisherAddr, cfg.K, cache, proofGen, encoder, broadcaster, *crashOnFail)
	syncService := p2p.NewSyncService(cache)

	// 5. Register HTTP routing handlers
	mux := http.NewServeMux()
	receiver.RegisterHandlers(mux)
	syncService.RegisterHandlers(mux)

	// 6. Start HTTP listener
	log.Printf("Bootstrap Node services listening on :%d...", cfg.APIPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.APIPort), mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

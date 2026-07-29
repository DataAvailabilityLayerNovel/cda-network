package main

import (
	"fmt"
	"log"
	"math/big"
	"net/http"

	"cda-light-node/config"
	"cda-light-node/internal/service"
	"cda-light-node/internal/verifier"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
)

func main() {
	cfg := config.LoadConfig()

	log.Printf("Starting Light Node on port %d, publisher=%s, bootstraps=%v, k=%d",
		cfg.Port, cfg.PublisherAddr, cfg.BootstrapsMap, cfg.K)

	// 1. Initialize KZG SRS
	srsSize := uint64(1024)
	srs, err := bls12381kzg.NewSRS(srsSize, big.NewInt(-1))
	if err != nil {
		log.Fatalf("Failed to initialize SRS: %v", err)
	}
	kzg := cda.NewGnarkKZG(*srs)

	// 2. Initialize Verifier
	dasVerifier := verifier.NewDASVerifier(cfg.K, kzg)

	// 3. Initialize HTTP API Service
	apiService := service.NewAPIService(cfg.PublisherAddr, cfg.BootstrapsMap, dasVerifier, cfg.CrashOnFail)

	mux := http.NewServeMux()
	apiService.RegisterHandlers(mux)

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("healthy"))
	})

	// 4. Start HTTP Server
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("Light Node listening on %s...", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Light Node HTTP server failed: %v", err)
	}
}

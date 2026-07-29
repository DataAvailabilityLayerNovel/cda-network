package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cda-store-node/config"
	"cda-store-node/internal/p2p"
	"cda-store-node/internal/storage"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting Store Node at Port=%d, MyAddr=%s for Cell [%d, %d]", cfg.Port, cfg.MyAddr, cfg.RowIdx, cfg.ColIdx)

	// 1. Initialize KZG SRS
	srsSize := uint64(1024)
	srs, err := bls12381kzg.NewSRS(srsSize, big.NewInt(-1))
	if err != nil {
		log.Fatalf("Failed to initialize SRS: %v", err)
	}
	kzg := cda.NewGnarkKZG(*srs)

	// 2. Initialize Custody Cache Storage
	cache := storage.NewCustodyStore()

	// 3. Initialize P2P subcomponents
	broadcaster := p2p.NewBroadcaster(cfg.Peers)
	receiver := p2p.NewReceiver(kzg, cfg.PublisherAddr, cfg.K, cfg.RowIdx, cfg.ColIdx, cache, broadcaster, cfg.CrashOnFail)

	// 4. Register HTTP routing handlers
	mux := http.NewServeMux()
	receiver.RegisterHandlers(mux)

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("healthy"))
	})

	// 5. Dynamic Membership Discovery & Registration
	registerUrl := fmt.Sprintf("%s/bootstrap/register", cfg.BootstrapAddr)
	payloadBytes, _ := json.Marshal(map[string]string{"addr": cfg.MyAddr})

	registerStore := func() {
		resp, err := http.Post(registerUrl, "application/json", bytes.NewReader(payloadBytes))
		if err != nil {
			log.Printf("[P2P Membership] Failed to register to Bootstrap Node %s: %v", cfg.BootstrapAddr, err)
		} else {
			resp.Body.Close()
			log.Printf("[P2P Membership] Registered successfully to Bootstrap Node as %s", cfg.MyAddr)
		}
	}

	// Register on startup
	registerStore()

	// Sync loop
	stopSync := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		peersUrl := fmt.Sprintf("%s/bootstrap/peers", cfg.BootstrapAddr)

		for {
			select {
			case <-ticker.C:
				resp, err := http.Get(peersUrl)
				if err != nil {
					log.Printf("[P2P Membership] Failed to sync peers from Bootstrap Node: %v", err)
					continue
				}

				var data struct {
					Peers []string `json:"peers"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&data); err == nil {
					resp.Body.Close()
					// Filter out own address
					var filtered []string
					for _, p := range data.Peers {
						if p != cfg.MyAddr {
							filtered = append(filtered, p)
						}
					}
					broadcaster.UpdatePeers(filtered)
				} else {
					resp.Body.Close()
				}
			case <-stopSync:
				return
			}
		}
	}()

	// Signal handling for graceful deregistration
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: mux,
	}

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down gracefully...", sig)
		close(stopSync)

		// Deregister from bootstrap node
		deregisterUrl := fmt.Sprintf("%s/bootstrap/deregister", cfg.BootstrapAddr)
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Post(deregisterUrl, "application/json", bytes.NewReader(payloadBytes))
		if err != nil {
			log.Printf("[P2P Membership] Failed to deregister from Bootstrap Node: %v", err)
		} else {
			resp.Body.Close()
			log.Printf("[P2P Membership] Deregistered successfully from Bootstrap Node")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	log.Printf("Store Node services listening on %s...", server.Addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

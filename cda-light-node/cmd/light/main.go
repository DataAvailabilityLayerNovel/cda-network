package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cda-light-node/config"
	"cda-light-node/internal/service"
	"cda-light-node/internal/verifier"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := config.LoadConfig()

	log.Printf("Starting Light Node on port %d, publisher=%s, bootstraps=%v, k=%d, k-piece=%d",
		cfg.Port, cfg.PublisherAddr, cfg.BootstrapsMap, cfg.K, cfg.KPiece)

	// 1. Initialize KZG SRS
	srsSize := uint64(1024)
	srs, err := bls12381kzg.NewSRS(srsSize, big.NewInt(-1))
	if err != nil {
		log.Fatalf("Failed to initialize SRS: %v", err)
	}
	kzg := cda.NewGnarkKZG(*srs)

	// 2. Initialize Verifier
	dasVerifier := verifier.NewDASVerifier(cfg.KPiece, kzg)

	// 3. Initialize P2P Host (using seed based on API port)
	privKey, pid, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-light-%d", cfg.Port))
	if err != nil {
		log.Fatalf("Failed to generate keypair: %v", err)
	}

	p2pPort := cfg.Port + 10000 // e.g. 8095 -> 18095
	p2pHost, err := p2pcommon.NewP2PHost(p2pPort, privKey)
	if err != nil {
		log.Fatalf("Failed to start P2P Host: %v", err)
	}

	log.Printf("[P2P] Light Node Host started with ID %s listening on %v", pid.String(), p2pHost.Addrs())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Initialize GossipSub
	ps, err := pubsub.NewGossipSub(ctx, p2pHost)
	if err != nil {
		log.Fatalf("Failed to start GossipSub: %v", err)
	}

	// 4.5. Connect to Bootstrap Nodes via P2P to join GossipSub mesh
	for colIdx, httpAddrs := range cfg.BootstrapsMap {
		for _, httpAddr := range httpAddrs {
			n := 2 * cfg.K
			numCols := cfg.NumCols
			if numCols <= 0 {
				numCols = 8
			}
			colsPerNetCol := n / numCols
			if colsPerNetCol == 0 {
				colsPerNetCol = 1
			}
			startColID := colIdx * colsPerNetCol
			_, bootPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", startColID))
			if err != nil {
				log.Printf("[P2P] Failed to derive bootstrap keypair for col %d: %v", colIdx, err)
				continue
			}

			// Convert HTTP address to P2P multiaddr: http://host:port -> /dns4/host/tcp/(port+10000)
			bootAddr := httpAddr
			if strings.HasPrefix(bootAddr, "http://") || strings.HasPrefix(bootAddr, "https://") {
				u, err := url.Parse(bootAddr)
				if err == nil {
					hostStr := u.Hostname()
					portStr := u.Port()
					if portVal, err := strconv.Atoi(portStr); err == nil {
						p2pPort := portVal + 10000
						if hostStr == "localhost" || hostStr == "127.0.0.1" {
							bootAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p2pPort)
						} else {
							bootAddr = fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, p2pPort)
						}
					}
				}
			}

			fullAddr := fmt.Sprintf("%s/p2p/%s", bootAddr, bootPID.String())
			maddr, err := multiaddr.NewMultiaddr(fullAddr)
			if err != nil {
				log.Printf("[P2P] Failed to parse bootstrap multiaddr %s: %v", fullAddr, err)
				continue
			}

			bootInfo, err := peer.AddrInfoFromP2pAddr(maddr)
			if err != nil {
				log.Printf("[P2P] Failed to parse AddrInfo for bootstrap col %d: %v", colIdx, err)
				continue
			}

			go func(info peer.AddrInfo) {
				for {
					if err := p2pHost.Connect(ctx, info); err == nil {
						log.Printf("[P2P] Light Node connected to Bootstrap Node %s", info.ID)
						break
					}
					select {
					case <-ctx.Done():
						return
					default:
						time.Sleep(2 * time.Second)
					}
				}
			}(*bootInfo)
		}
	}

	// 5. Initialize HTTP API Service
	apiService := service.NewAPIService(cfg.PublisherAddr, cfg.BootstrapsMap, dasVerifier, cfg.CrashOnFail, p2pHost, ps, cfg.Port, cfg.NumCols)

	// Start sequential DAS worker
	if cfg.AutoDAS {
		apiService.StartDASWorker(ctx, cfg.AutoDASSamples)
	}

	// 6. Subscribe to Header GossipSub topic (cache only — DAS is triggered by BlockReady signal)
	topic, err := ps.Join(p2pcommon.TopicHeader)
	if err != nil {
		log.Fatalf("Failed to join header GossipSub topic: %v", err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		log.Fatalf("Failed to subscribe to header GossipSub topic: %v", err)
	}

	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			var header service.BlockHeader
			if err := json.Unmarshal(msg.Data, &header); err == nil {
				apiService.CacheHeader(&header)
				// NOTE: DAS is no longer triggered immediately on header.
				// It will be triggered when a BlockReady signal arrives from store nodes.
			}
		}
	}()

	// 6b. Subscribe to BlockReady GossipSub topic — triggers sequential DAS
	if cfg.AutoDAS {
		blockReadyTopic, err := ps.Join(p2pcommon.TopicBlockReady)
		if err != nil {
			log.Fatalf("Failed to join block-ready GossipSub topic: %v", err)
		}
		blockReadySub, err := blockReadyTopic.Subscribe()
		if err != nil {
			log.Fatalf("Failed to subscribe to block-ready GossipSub topic: %v", err)
		}
		go func() {
			for {
				msg, err := blockReadySub.Next(ctx)
				if err != nil {
					return
				}
				var payload p2pcommon.GossipBlockReadyPayload
				if err := json.Unmarshal(msg.Data, &payload); err == nil && payload.BlockID != "" {
					apiService.EnqueueBlockReady(payload.BlockID)
				}
			}
		}()
	}

	mux := http.NewServeMux()
	apiService.RegisterHandlers(mux)

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("healthy"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	// 7. Start HTTP Server
	addr := fmt.Sprintf(":%d", cfg.Port)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("Light Node HTTP listening on %s...", addr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Light Node HTTP server failed: %v", err)
		}
	}()

	// Signal handling for clean exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Printf("Shutting down Light Node...")
	server.Shutdown(ctx)
	p2pHost.Close()
	cancel()
}

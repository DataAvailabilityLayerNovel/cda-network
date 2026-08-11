package main

import (
	"context"
	"encoding/json"
	"flag"
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

	"cda-bootstrap-node/config"
	"cda-bootstrap-node/internal/engine"
	"cda-bootstrap-node/internal/p2p"
	"cda-bootstrap-node/internal/storage"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "", "path to json config file")
	port := flag.Int("port", 0, "override API port")
	colID := flag.Int("col", 0, "override column index")
	storeAddr := flag.String("store", "", "override store node address")
	pubAddr := flag.String("publisher", "", "override publisher node address")
	seedAddr := flag.String("seed", "", "override seed bootstrap address")
	kVal := flag.Int("k", 0, "override K chunks parameter")
	kPieceVal := flag.Int("k-piece", 0, "override K-piece parameter")
	crashOnFail := flag.Bool("crash-on-fail", false, "Crash the node if verification fails")
	pruneEnable := flag.Bool("prune-enable", false, "Enable pruning of local cache")
	pruneTTL := flag.String("prune-ttl", "", "TTL duration before pruning local cache")
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
	if *seedAddr != "" {
		cfg.SeedAddr = *seedAddr
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
	
	// Apply CLI prune overrides if specified
	// We check if flag was explicitly provided
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "prune-enable" {
			cfg.PruneEnable = *pruneEnable
		}
		if f.Name == "prune-ttl" {
			cfg.PruneTTL = *pruneTTL
		}
	})

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
	parsedTTL, errTTL := time.ParseDuration(cfg.PruneTTL)
	if errTTL != nil {
		parsedTTL = 5 * time.Minute
	}
	receiver := p2p.NewReceiver(p2pHost, kzg, cfg.PublisherAddr, cfg.KPiece, cache, proofGen, encoder, broadcaster, *crashOnFail, cfg.ColumnID, cfg.PruneEnable, parsedTTL)

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

	// Dynamic Peer Registration with Seed Bootstrap (if configured and not self)
	if cfg.SeedAddr != "" && cfg.ColumnID != 0 {
		_, seedPID, err := p2pcommon.GenerateDeterministicKeypair("cda-bootstrap-0")
		if err == nil {
			seedAddr := cfg.SeedAddr
			if strings.HasPrefix(seedAddr, "http://") || strings.HasPrefix(seedAddr, "https://") {
				u, err := url.Parse(seedAddr)
				if err == nil {
					hostStr := u.Hostname()
					portStr := u.Port()
					if portVal, err := strconv.Atoi(portStr); err == nil {
						seedP2PPort := portVal + 10000
						if hostStr == "localhost" || hostStr == "127.0.0.1" {
							seedAddr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", seedP2PPort)
						} else {
							seedAddr = fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, seedP2PPort)
						}
					}
				}
			}
			seedAddrFull := fmt.Sprintf("%s/p2p/%s", seedAddr, seedPID.String())
			seedMaddr, err := multiaddr.NewMultiaddr(seedAddrFull)
			if err == nil {
				seedInfo, err := peer.AddrInfoFromP2pAddr(seedMaddr)
				if err == nil {
					myP2PAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p2pPort)
					go func() {
						ticker := time.NewTicker(3 * time.Second)
						defer ticker.Stop()
						registerWithSeed := func() {
							connCtx, connCancel := context.WithTimeout(ctx, 3*time.Second)
							defer connCancel()
							if err := p2pHost.Connect(connCtx, *seedInfo); err != nil {
								return
							}
							stream, err := p2pHost.NewStream(connCtx, seedInfo.ID, p2pcommon.ProtoBootstrapRouting)
							if err != nil {
								return
							}
							defer stream.Close()
							req := p2pcommon.BootstrapRoutingRequest{
								Peer: p2pcommon.PeerInfo{
									PeerID:     pid.String(),
									Multiaddrs: []string{myP2PAddr},
									Row:        -2, // Mark as Bootstrap Node for ColumnID
									Col:        cfg.ColumnID,
								},
								TargetCol: cfg.ColumnID,
							}
							_ = json.NewEncoder(stream).Encode(req)
						}
						registerWithSeed()
						for {
							select {
							case <-ticker.C:
								registerWithSeed()
							case <-ctx.Done():
								return
							}
						}
					}()
				}
			}
		}
	}

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

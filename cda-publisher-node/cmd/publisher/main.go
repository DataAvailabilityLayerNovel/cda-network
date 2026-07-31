package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cda-publisher-node/config"
	"cda-publisher-node/internal/engine"
	"cda-publisher-node/internal/p2p"
	"cda-publisher-node/internal/service"

	p2pcommon "cda-p2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "", "path to json config file")
	port := flag.Int("port", 0, "override API port")
	bootstrap := flag.String("bootstrap", "", "override bootstrap address")
	kVal := flag.Int("k", 0, "override K chunks parameter")
	kPieceVal := flag.Int("k-piece", 0, "override K-piece parameter")
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
	if *bootstrap != "" {
		cfg.BootstrapPeers[0] = *bootstrap
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

	log.Printf("Starting Publisher Node with configuration: APIPort=%d, K=%d, KPiece=%d", cfg.APIPort, cfg.K, cfg.KPiece)

	// 1. Initialize P2P Host with deterministic PeerID
	privKey, pid, err := p2pcommon.GenerateDeterministicKeypair("cda-publisher")
	if err != nil {
		log.Fatalf("Failed to generate deterministic keypair: %v", err)
	}

	p2pPort := cfg.APIPort + 10000 // Compute P2P port deterministically
	p2pHost, err := p2pcommon.NewP2PHost(p2pPort, privKey)
	if err != nil {
		log.Fatalf("Failed to initialize libp2p Host: %v", err)
	}

	log.Printf("[P2P] Publisher Host started with ID %s listening on %v", pid.String(), p2pHost.Addrs())

	// 2. Initialize GossipSub Router
	ps, err := pubsub.NewGossipSub(context.Background(), p2pHost)
	if err != nil {
		log.Fatalf("Failed to initialize GossipSub: %v", err)
	}

	// 3. Initialize Discovery & Sender
	disc := p2p.NewDiscovery(cfg.BootstrapPeers)
	sender := p2p.NewSender(p2pHost, disc)

	// 4. Initialize Pipeline Engine
	pipeline, err := engine.NewPipeline(cfg.KPiece)
	if err != nil {
		log.Fatalf("Failed to initialize pipeline engine: %v", err)
	}

	// 4.5 Connect to Bootstraps to maintain GossipSub mesh connectivity
	for colIdx, addrStr := range cfg.BootstrapPeers {
		n := 2 * cfg.K
		colsPerNetCol := n / 8
		if colsPerNetCol == 0 {
			colsPerNetCol = 1
		}
		bootColID := (colIdx / colsPerNetCol) * colsPerNetCol
		_, bootPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", bootColID))
		if err != nil {
			continue
		}

		bootAddr := addrStr
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

		bootstrapAddrFull := fmt.Sprintf("%s/p2p/%s", bootAddr, bootPID.String())
		maddr, err := multiaddr.NewMultiaddr(bootstrapAddrFull)
		if err != nil {
			log.Printf("[P2P Gossip] Invalid bootstrap multiaddr %s: %v", bootstrapAddrFull, err)
			continue
		}

		bootInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			log.Printf("[P2P Gossip] Failed to parse PeerInfo: %v", err)
			continue
		}

		go func(info peer.AddrInfo) {
			for {
				if err := p2pHost.Connect(context.Background(), info); err == nil {
					log.Printf("[P2P Gossip] Connected to Bootstrap Node %s", info.ID)
					break
				}
				time.Sleep(2 * time.Second)
			}
		}(*bootInfo)
	}

	// 5. Initialize API Service
	dbPath := "data/publisher/badger"
	apiService := service.NewAPIService(pipeline, sender, ps, dbPath, cfg.SequencerPublicKey)
	mux := http.NewServeMux()
	apiService.RegisterHandlers(mux)
	mux.Handle("/metrics", promhttp.Handler())

	// 6. Start HTTP server for external triggers
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.APIPort),
		Handler: mux,
	}

	go func() {
		log.Printf("API Service listening on %s...", server.Addr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server failed: %v", err)
		}
	}()

	// Signal handling for clean exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Printf("Shutting down Publisher Node...")
	ctxShut, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()
	server.Shutdown(ctxShut)
	_ = apiService.Close()
	p2pHost.Close()
}

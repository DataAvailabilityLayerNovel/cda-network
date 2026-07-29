package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"cda-publisher-node/config"
	"cda-publisher-node/internal/engine"
	"cda-publisher-node/internal/p2p"
	"cda-publisher-node/internal/service"
)

func main() {
	configPath := flag.String("config", "", "path to json config file")
	port := flag.Int("port", 0, "override API port")
	bootstrap := flag.String("bootstrap", "", "override bootstrap address")
	kVal := flag.Int("k", 0, "override K chunks parameter")
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

	log.Printf("Starting Publisher Node with configuration: APIPort=%d, K=%d", cfg.APIPort, cfg.K)

	// 1. Initialize P2P Host (simulated)
	hostAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", cfg.APIPort)
	p2p.NewHost("QmPublisher", []string{hostAddr})

	// 2. Initialize Discovery & Sender
	disc := p2p.NewDiscovery(cfg.BootstrapPeers)
	sender := p2p.NewSender(disc)

	// 3. Initialize Pipeline Engine
	pipeline, err := engine.NewPipeline(cfg.K)
	if err != nil {
		log.Fatalf("Failed to initialize pipeline engine: %v", err)
	}

	// 4. Initialize API Service
	apiService := service.NewAPIService(pipeline, sender)
	mux := http.NewServeMux()
	apiService.RegisterHandlers(mux)

	// 5. Start HTTP server
	log.Printf("API Service listening on :%d...", cfg.APIPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.APIPort), mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

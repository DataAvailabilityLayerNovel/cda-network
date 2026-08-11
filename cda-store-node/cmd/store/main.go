package main

import (
	"context"
	"crypto/rand"
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

	"cda-store-node/config"
	"cda-store-node/internal/p2p"
	"cda-store-node/internal/storage"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting Store Node at Port=%d, MyAddr=%s (Requested Row=%d, Col=%d)", cfg.Port, cfg.MyAddr, cfg.RowIdx, cfg.ColIdx)

	// 1. Initialize KZG SRS
	srsSize := uint64(1024)
	srs, err := bls12381kzg.NewSRS(srsSize, big.NewInt(-1))
	if err != nil {
		log.Fatalf("Failed to initialize SRS: %v", err)
	}
	kzg := cda.NewGnarkKZG(*srs)

	// 2. Initialize Custody Cache Storage
	cache := storage.NewCustodyStore(cfg.Port)

	// 3. Generate or load keypair (Auto-derive custody coordinates or use targeted coordinates)
	keyFileName := fmt.Sprintf("store_%d.key", cfg.Port)
	var privKey crypto.PrivKey
	var pid peer.ID

	if keyData, err := os.ReadFile(keyFileName); err == nil {
		privKey, err = crypto.UnmarshalPrivateKey(keyData)
		if err == nil {
			pid, err = peer.IDFromPrivateKey(privKey)
		}
		if err == nil {
			log.Printf("[P2P Identity] Loaded cached keypair from %s", keyFileName)
		} else {
			log.Printf("[P2P Identity] Cached key file invalid, regenerating...")
			privKey = nil
		}
	}

	gridRows := 2 * cfg.K
	gridCols := 2 * cfg.K

	if privKey == nil {
		if cfg.RowIdx >= 0 && cfg.ColIdx >= 0 {
			// Targeted cell key generation (for deterministic testing)
			privKey, pid, err = p2pcommon.GenerateKeypairForCell(cfg.RowIdx, cfg.ColIdx, gridRows, gridCols, "cda-salt-2026")
			if err != nil {
				log.Fatalf("Failed to generate deterministic keypair for cell coordinate: %v", err)
			}
		} else {
			// Production Auto-derive Mode: generate random Ed25519 key and compute deterministic cell
			privKey, _, err = crypto.GenerateEd25519Key(rand.Reader)
			if err != nil {
				log.Fatalf("Failed to generate random private key: %v", err)
			}
			pid, err = peer.IDFromPrivateKey(privKey)
			if err != nil {
				log.Fatalf("Failed to extract PeerID: %v", err)
			}
		}

		keyData, err := crypto.MarshalPrivateKey(privKey)
		if err == nil {
			err = os.WriteFile(keyFileName, keyData, 0600)
			if err == nil {
				log.Printf("[P2P Identity] Saved generated keypair to %s", keyFileName)
			}
		}
	}

	// Calculate and assign custody coordinates from PeerID
	autoRow, autoCol := p2pcommon.CalculateCell(pid, gridRows, gridCols, "cda-salt-2026")
	if cfg.RowIdx < 0 || cfg.ColIdx < 0 {
		cfg.RowIdx = autoRow
		cfg.ColIdx = autoCol
		log.Printf("[P2P Identity] Auto-derived custody coordinates: Row=%d, Col=%d from PeerID %s", cfg.RowIdx, cfg.ColIdx, pid.String())
	} else {
		log.Printf("[P2P Identity] Using custody coordinates: Row=%d, Col=%d (PeerID=%s)", cfg.RowIdx, cfg.ColIdx, pid.String())
	}

	p2pPort := cfg.Port + 10000 // Compute P2P port deterministically (e.g. 8080 -> 18080)
	p2pHost, err := p2pcommon.NewP2PHost(p2pPort, privKey)
	if err != nil {
		log.Fatalf("Failed to initialize libp2p host: %v", err)
	}

	log.Printf("[P2P] Store Node P2P Host started with ID %s listening on %v", pid.String(), p2pHost.Addrs())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Initialize GossipSub
	ps, err := pubsub.NewGossipSub(ctx, p2pHost)
	if err != nil {
		log.Fatalf("Failed to initialize GossipSub: %v", err)
	}

	// 5. Initialize P2P subcomponents
	broadcaster := p2p.NewBroadcaster(p2pHost, ps)
	receiver := p2p.NewReceiver(p2pHost, ps, kzg, cfg.PublisherAddr, cfg.K, cfg.KPiece, cfg.NumCols, cfg.StoresPerCol, cfg.RowIdx, cfg.ColIdx, cache, broadcaster, cfg.CrashOnFail, cfg.PruneEnable, cfg.PruneTTL)

	// Expose HTTP server for local status check and health endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/store/status/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		completed := receiver.IsComplete(r.URL.Path[len("/store/status/"):])
		json.NewEncoder(w).Encode(map[string]interface{}{
			"block_id":  r.URL.Path[len("/store/status/"):],
			"completed": completed,
		})
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("healthy"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: mux,
	}

	go func() {
		log.Printf("HTTP Status Service listening on %s...", server.Addr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP Status Server error: %v", err)
		}
	}()

	// 6. Connect to column Bootstrap Node (with Single-Seed Discovery) and Register coordinates
	n := 2 * cfg.K
	numCols := cfg.NumCols
	if numCols <= 0 {
		numCols = 8
	}
	colsPerNetCol := n / numCols
	if colsPerNetCol == 0 {
		colsPerNetCol = 1
	}
	targetNetCol := cfg.ColIdx / colsPerNetCol
	targetColStart := targetNetCol * colsPerNetCol

	_, targetBootPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", targetColStart))
	if err != nil {
		log.Fatalf("Failed to calculate bootstrap PeerID: %v", err)
	}

	// Dynamic resolution of own multiaddr
	hostName := "localhost"
	u, err := url.Parse(cfg.MyAddr)
	if err == nil {
		hostName = u.Hostname()
	}
	myP2PAddr := fmt.Sprintf("/dns4/%s/tcp/%d", hostName, p2pPort)

	// Helper to resolve address to multiaddr
	resolveToMultiaddr := func(addrStr string, bootPID peer.ID) (multiaddr.Multiaddr, error) {
		resolved := addrStr
		if strings.HasPrefix(resolved, "http://") || strings.HasPrefix(resolved, "https://") {
			u, err := url.Parse(resolved)
			if err == nil {
				hostStr := u.Hostname()
				portStr := u.Port()
				if portVal, err := strconv.Atoi(portStr); err == nil {
					pPort := portVal + 10000
					if hostStr == "localhost" || hostStr == "127.0.0.1" {
						resolved = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", pPort)
					} else {
						resolved = fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, pPort)
					}
				}
			}
		}
		if !strings.Contains(resolved, "/p2p/") {
			resolved = fmt.Sprintf("%s/p2p/%s", resolved, bootPID.String())
		}
		return multiaddr.NewMultiaddr(resolved)
	}

	targetBootAddr := cfg.BootstrapAddr

	// If Store node's column is NOT managed by Seed Node (0), query Seed Node to discover Column Bootstrap
	if targetColStart != 0 && cfg.SeedAddr != "" {
		_, seedPID, err := p2pcommon.GenerateDeterministicKeypair("cda-bootstrap-0")
		if err == nil {
			seedMaddr, err := resolveToMultiaddr(cfg.SeedAddr, seedPID)
			if err == nil {
				if seedInfo, err := peer.AddrInfoFromP2pAddr(seedMaddr); err == nil {
					log.Printf("[P2P Discovery] Querying Seed Node %s for Column %d Bootstrap...", seedInfo.ID, targetColStart)
					ctxSeed, cancelSeed := context.WithTimeout(context.Background(), 5*time.Second)
					if err := p2pHost.Connect(ctxSeed, *seedInfo); err == nil {
						if stream, err := p2pHost.NewStream(ctxSeed, seedInfo.ID, p2pcommon.ProtoBootstrapRouting); err == nil {
							req := p2pcommon.BootstrapRoutingRequest{
								Peer: p2pcommon.PeerInfo{
									PeerID:     pid.String(),
									Multiaddrs: []string{myP2PAddr},
									Row:        cfg.RowIdx,
									Col:        cfg.ColIdx,
								},
								TargetCol: targetColStart,
								TargetRow: cfg.RowIdx,
							}
							if err := json.NewEncoder(stream).Encode(req); err == nil {
								var resp p2pcommon.BootstrapRoutingResponse
								if err := json.NewDecoder(stream).Decode(&resp); err == nil {
									for _, p := range resp.ColPeers {
										if p.Row == -2 && p.Col == targetColStart && len(p.Multiaddrs) > 0 {
											targetBootAddr = p.Multiaddrs[0]
											log.Printf("[P2P Discovery] Discovered Column %d Bootstrap: %s", targetColStart, targetBootAddr)
											break
										}
									}
								}
							}
							stream.Close()
						}
					}
					cancelSeed()
				}
			}
		}
	}

	bootstrapMaddr, err := resolveToMultiaddr(targetBootAddr, targetBootPID)
	if err != nil {
		log.Fatalf("Invalid bootstrap multiaddr %s: %v", targetBootAddr, err)
	}

	bootInfo, err := peer.AddrInfoFromP2pAddr(bootstrapMaddr)
	if err != nil {
		log.Fatalf("Failed to parse bootstrap PeerInfo: %v", err)
	}

	registerAndSyncPeers := func() {
		connCtx, connCancel := context.WithTimeout(ctx, 3*time.Second)
		defer connCancel()

		if err := p2pHost.Connect(connCtx, *bootInfo); err != nil {
			log.Printf("[P2P Membership] Failed to connect to Bootstrap Node %s: %v", bootInfo.ID, err)
			return
		}

		stream, err := p2pHost.NewStream(connCtx, bootInfo.ID, p2pcommon.ProtoBootstrapRouting)
		if err != nil {
			log.Printf("[P2P Membership] Failed to open routing stream on Bootstrap: %v", err)
			return
		}
		defer stream.Close()

		req := p2pcommon.BootstrapRoutingRequest{
			Peer: p2pcommon.PeerInfo{
				PeerID:     pid.String(),
				Multiaddrs: []string{myP2PAddr},
				Row:        cfg.RowIdx,
				Col:        cfg.ColIdx,
			},
			TargetRow: cfg.RowIdx,
			TargetCol: cfg.ColIdx,
		}

		if err := json.NewEncoder(stream).Encode(req); err != nil {
			log.Printf("[P2P Membership] Failed to encode registration request: %v", err)
			return
		}

		var resp p2pcommon.BootstrapRoutingResponse
		if err := json.NewDecoder(stream).Decode(&resp); err != nil {
			log.Printf("[P2P Membership] Failed to decode registration response: %v", err)
			return
		}

		receiver.SetPeers(resp.RowPeers, resp.ColPeers)

		// Connect to row and column peers (Persistent Cliques)
		allPeers := append([]p2pcommon.PeerInfo(nil), resp.RowPeers...)
		allPeers = append(allPeers, resp.ColPeers...)

		for _, pInfo := range allPeers {
			pID, err := peer.Decode(pInfo.PeerID)
			if err != nil {
				continue
			}
			for _, mStr := range pInfo.Multiaddrs {
				m, err := multiaddr.NewMultiaddr(mStr)
				if err != nil {
					continue
				}
				p2pHost.Peerstore().AddAddr(pID, m, 10*time.Minute)
			}
			p2pHost.Connect(connCtx, peer.AddrInfo{ID: pID})
		}
	}

	// Trigger registration and start sync loop
	registerAndSyncPeers()

	// 7. Start P2P Receiver stream handlers and GossipSub loop
	receiver.Start(ctx)

	// Sync loop
	stopSync := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				registerAndSyncPeers()
			case <-stopSync:
				return
			}
		}
	}()

	// Signal handling for clean exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down store node gracefully...", sig)
		
		// Send graceful leave notification to Bootstrap Node
		deregCtx, deregCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := p2pHost.Connect(deregCtx, *bootInfo); err == nil {
			if stream, err := p2pHost.NewStream(deregCtx, bootInfo.ID, p2pcommon.ProtoBootstrapRouting); err == nil {
				req := p2pcommon.BootstrapRoutingRequest{
					Peer: p2pcommon.PeerInfo{
						PeerID: pid.String(),
					},
					IsLeave: true,
				}
				json.NewEncoder(stream).Encode(req)
				stream.Close()
				log.Printf("[P2P Membership] Gracefully deregistered from Bootstrap Node")
			}
		}
		deregCancel()

		close(stopSync)

		ctxShut, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutCancel()
		server.Shutdown(ctxShut)
		_ = cache.Close()
		p2pHost.Close()
		cancel()
	}()

	log.Printf("Store Node services fully active. Waiting for exit signal...")
	select {}
}

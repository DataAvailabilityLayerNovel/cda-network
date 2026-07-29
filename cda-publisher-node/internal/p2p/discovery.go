package p2p

import (
	"fmt"
	"log"
)

type Discovery struct {
	peers map[int]string // colIdx -> peer address
}

func NewDiscovery(peers map[int]string) *Discovery {
	return &Discovery{
		peers: peers,
	}
}

// FindBootstrapNode returns the bootstrap node address for a column index
func (d *Discovery) FindBootstrapNode(colIdx int) (string, error) {
	addr, exists := d.peers[colIdx]
	if !exists {
		// Fallback to colIdx 0 as the default bootstrap server in simulation
		fallbackAddr, hasFallback := d.peers[0]
		if hasFallback {
			log.Printf("[P2P] Discovery fallback for column %d -> using %s", colIdx, fallbackAddr)
			return fallbackAddr, nil
		}
		return "", fmt.Errorf("no bootstrap peer found for column %d and no fallback configured", colIdx)
	}
	log.Printf("[P2P] Discovery: found bootstrap node for column %d -> %s", colIdx, addr)
	return addr, nil
}

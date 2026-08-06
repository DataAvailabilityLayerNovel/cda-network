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

func (d *Discovery) FindBootstrapNode(colIdx int) (string, error) {
	addr, exists := d.peers[colIdx]
	if !exists {
		return "", fmt.Errorf("no bootstrap peer configured for column %d", colIdx)
	}
	log.Printf("[P2P] Discovery: found bootstrap node for column %d -> %s", colIdx, addr)
	return addr, nil
}

// GetBootstrapColID returns the lowest column index that maps to the same bootstrap address
func (d *Discovery) GetBootstrapColID(colIdx int) int {
	addr, exists := d.peers[colIdx]
	if !exists {
		return 0
	}
	smallestCol := colIdx
	for c, a := range d.peers {
		if a == addr && c < smallestCol {
			smallestCol = c
		}
	}
	return smallestCol
}

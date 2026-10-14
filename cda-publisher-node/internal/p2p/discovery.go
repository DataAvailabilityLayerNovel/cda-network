package p2p

import (
	"fmt"
	"log"
)

type Discovery struct {
	peers   map[int]string // netCol -> peer address
	numCols int
	baseK   int
}

func NewDiscovery(peers map[int]string, opts ...int) *Discovery {
	numCols := 8
	baseK := 16
	if len(opts) > 0 && opts[0] > 0 {
		numCols = opts[0]
	}
	if len(opts) > 1 && opts[1] > 0 {
		baseK = opts[1]
	}

	// Normalize peers to netCol index (0..numCols-1)
	baseColsPerNetCol := (2 * baseK) / numCols
	if baseColsPerNetCol <= 0 {
		baseColsPerNetCol = 1
	}

	netPeers := make(map[int]string)
	for colKey, addr := range peers {
		netCol := colKey
		if colKey >= numCols {
			netCol = colKey / baseColsPerNetCol
		}
		netPeers[netCol] = addr
	}

	return &Discovery{
		peers:   netPeers,
		numCols: numCols,
		baseK:   baseK,
	}
}

func (d *Discovery) FindBootstrapNode(colIdx int, nOpt ...int) (string, error) {
	n := 2 * d.baseK
	if len(nOpt) > 0 && nOpt[0] > 0 {
		n = nOpt[0]
	}
	colsPerNetCol := n / d.numCols
	if colsPerNetCol <= 0 {
		colsPerNetCol = 1
	}
	netCol := colIdx / colsPerNetCol

	addr, exists := d.peers[netCol]
	if !exists {
		// Fallback check
		if fallbackAddr, directExists := d.peers[colIdx]; directExists {
			return fallbackAddr, nil
		}
		return "", fmt.Errorf("no bootstrap peer configured for column %d (netCol %d, n %d)", colIdx, netCol, n)
	}
	log.Printf("[P2P] Discovery: found bootstrap node for column %d (netCol %d, n %d) -> %s", colIdx, netCol, n, addr)
	return addr, nil
}

// GetBootstrapColID returns the deterministic base column ID that bootstrap node was initialized with
func (d *Discovery) GetBootstrapColID(colIdx int, nOpt ...int) int {
	n := 2 * d.baseK
	if len(nOpt) > 0 && nOpt[0] > 0 {
		n = nOpt[0]
	}
	colsPerNetCol := n / d.numCols
	if colsPerNetCol <= 0 {
		colsPerNetCol = 1
	}
	netCol := colIdx / colsPerNetCol

	baseColsPerNetCol := (2 * d.baseK) / d.numCols
	if baseColsPerNetCol <= 0 {
		baseColsPerNetCol = 1
	}
	return netCol * baseColsPerNetCol
}

package p2pcommon

import (
	"fmt"
	"testing"
)

func TestMultiNodeCellCustodyStripingAndRotation(t *testing.T) {
	netRows := 8
	// Create 4 nodes in Cell [Row 0, Col 0] and 1 node each in Rows 1..7
	var colPeers []PeerInfo
	cell0Peers := []string{"peer-0-A", "peer-0-B", "peer-0-C", "peer-0-D"}
	for _, pid := range cell0Peers {
		colPeers = append(colPeers, PeerInfo{PeerID: pid, Row: 0, Col: 0})
	}
	for r := 1; r < netRows; r++ {
		colPeers = append(colPeers, PeerInfo{PeerID: fmt.Sprintf("peer-%d", r), Row: r, Col: 0})
	}

	custodyMap := ComputeColumnCustodyMap(colPeers, netRows)
	if len(custodyMap[0]) != 4 {
		t.Fatalf("expected 4 peers in custodyMap[0], got %d", len(custodyMap[0]))
	}

	// 1. Verify that for a large block with N = 128 data rows (16 sub-rows mapping to netRow 0: dataRow = 0, 8, 16, ..., 120),
	// each dataRow has EXACTLY 1 Primary Custody (isNativeRetention=true) and 1 Backup Custody (isNativeRetention=false),
	// while the other 2 nodes in Cell 0 act as Non-Custody (isCustody=false).
	// Across the 16 sub-rows of block-100, each of the 4 nodes in Cell 0 should be Primary for exactly 4 sub-rows
	// and Backup for exactly 4 sub-rows (50% reduction in active custody work and 75% reduction in primary retention!).
	primaryCounts := make(map[string]int)
	backupCounts := make(map[string]int)
	nonCustodyCounts := make(map[string]int)

	blockID := "block-100"
	for subRow := 0; subRow < 16; subRow++ {
		dataRow := subRow * netRows // all map to targetNetRow = 0
		primaries := 0
		backups := 0
		for _, pid := range cell0Peers {
			isCustody, isNative, activeSize := EvaluateNodeCustodyForBlockRow(pid, 0, blockID, dataRow, netRows, custodyMap)
			if activeSize != 2 {
				t.Fatalf("expected active committee size 2 for multi-node cell, got %d", activeSize)
			}
			if isCustody && isNative {
				primaries++
				primaryCounts[pid]++
			} else if isCustody && !isNative {
				backups++
				backupCounts[pid]++
			} else {
				nonCustodyCounts[pid]++
			}
		}
		if primaries != 1 || backups != 1 {
			t.Fatalf("subRow %d (dataRow %d): expected 1 primary and 1 backup, got %d primaries and %d backups",
				subRow, dataRow, primaries, backups)
		}
	}

	for _, pid := range cell0Peers {
		if primaryCounts[pid] != 4 {
			t.Errorf("peer %s: expected 4 primary rows out of 16, got %d", pid, primaryCounts[pid])
		}
		if backupCounts[pid] != 4 {
			t.Errorf("peer %s: expected 4 backup rows out of 16, got %d", pid, backupCounts[pid])
		}
		if nonCustodyCounts[pid] != 8 {
			t.Errorf("peer %s: expected 8 non-custody rows out of 16, got %d", pid, nonCustodyCounts[pid])
		}
	}

	// 2. Verify that across different blocks, the starting primary node for dataRow=0 rotates
	seenPrimaryForRow0 := make(map[string]bool)
	for b := 1; b <= 20; b++ {
		bID := fmt.Sprintf("block-%d", b)
		p, bkp, size := SelectActiveCustodyPairForBlockRow(bID, 0, netRows, custodyMap)
		if size != 2 || p == nil || bkp == nil || p.PeerID == bkp.PeerID {
			t.Fatalf("invalid active custody pair for %s", bID)
		}
		seenPrimaryForRow0[p.PeerID] = true
	}
	if len(seenPrimaryForRow0) < 3 {
		t.Errorf("expected primary custody for dataRow=0 to rotate across multiple peers over 20 blocks, only saw %v", seenPrimaryForRow0)
	}

	// 3. Verify that single-node rows (Rows 1..7) borrow their backup from the surplus multi-node Cell 0
	// rather than overloading other single-node rows!
	for r := 1; r < netRows; r++ {
		assigned := custodyMap[r]
		if len(assigned) != 2 {
			t.Fatalf("row %d: expected 2 assigned custody peers (1 native + 1 borrowed), got %d", r, len(assigned))
		}
		if assigned[0].Row != r {
			t.Errorf("row %d: expected native peer first, got row %d", r, assigned[0].Row)
		}
		if assigned[1].Row != 0 {
			t.Errorf("row %d: expected borrowed backup to come from surplus multi-node Row 0, got row %d (%s)",
				r, assigned[1].Row, assigned[1].PeerID)
		}
	}
}

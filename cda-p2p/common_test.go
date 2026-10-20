package p2pcommon

import (
	"fmt"
	"testing"
)

func TestTopicCellRow(t *testing.T) {
	got := TopicCellRow(2, 5)
	want := "/cda/1.0.0/col/2/row/5"
	if got != want {
		t.Fatalf("TopicCellRow(2, 5) = %q, want %q", got, want)
	}
}

func TestGenerateKeypairForCellInstance_MultiNodeSameCell(t *testing.T) {
	netRows, netCols := 8, 8
	targetRow, targetCol := 3, 2
	salt := "cda-salt-2026"

	seenPIDs := make(map[string]bool)
	for replica := 1; replica <= 4; replica++ {
		instanceID := fmt.Sprintf("http://store-2-4-r%d:8080", replica)
		_, pid, err := GenerateKeypairForCellInstance(targetRow, targetCol, netRows, netCols, salt, instanceID)
		if err != nil {
			t.Fatalf("replica %d failed: %v", replica, err)
		}
		if seenPIDs[pid.String()] {
			t.Fatalf("duplicate PeerID generated for replica %d: %s", replica, pid.String())
		}
		seenPIDs[pid.String()] = true

		r, c := CalculateCell(pid, netRows, netCols, salt)
		if r != targetRow || c != targetCol {
			t.Fatalf("replica %d mapped to (%d, %d), expected (%d, %d)", replica, r, c, targetRow, targetCol)
		}
	}
}

func TestComputeColumnCustodyMap_SingleNodePerCell(t *testing.T) {
	netRows := 4
	var peers []PeerInfo
	for r := 0; r < netRows; r++ {
		peers = append(peers, PeerInfo{
			PeerID: fmt.Sprintf("peer-r%d", r),
			Row:    r,
			Col:    0,
		})
	}

	custodyMap := ComputeColumnCustodyMap(peers, netRows)
	for r := 0; r < netRows; r++ {
		holders := custodyMap[r]
		if len(holders) != 2 {
			t.Fatalf("row %d: expected 2 custody holders (primary + 1 borrowed), got %d", r, len(holders))
		}
		if holders[0].PeerID != fmt.Sprintf("peer-r%d", r) {
			t.Fatalf("row %d: expected primary peer-r%d, got %s", r, r, holders[0].PeerID)
		}
		expectedBackup := fmt.Sprintf("peer-r%d", (r+1)%netRows)
		if holders[1].PeerID != expectedBackup {
			t.Fatalf("row %d: expected backup %s, got %s", r, expectedBackup, holders[1].PeerID)
		}
	}
}

func TestComputeColumnCustodyMap_MultiNodeCellAndEmptyRow(t *testing.T) {
	netRows := 4
	// Row 0 has 3 nodes (multi-node cell M=3)
	// Row 1 has 0 nodes (empty / left)
	// Row 2 has 2 nodes (multi-node cell M=2)
	// Row 3 has 1 node (single-node cell M=1)
	peers := []PeerInfo{
		{PeerID: "peer-r0-a", Row: 0, Col: 0},
		{PeerID: "peer-r0-b", Row: 0, Col: 0},
		{PeerID: "peer-r0-c", Row: 0, Col: 0},
		{PeerID: "peer-r2-a", Row: 2, Col: 0},
		{PeerID: "peer-r2-b", Row: 2, Col: 0},
		{PeerID: "peer-r3-a", Row: 3, Col: 0},
	}

	custodyMap := ComputeColumnCustodyMap(peers, netRows)

	// Row 0 should have all 3 in-cell peers
	if len(custodyMap[0]) != 3 {
		t.Fatalf("row 0: expected 3 custody peers, got %d", len(custodyMap[0]))
	}

	// Row 1 (empty) should borrow 2 peers from next active row (Row 2)
	if len(custodyMap[1]) != 2 {
		t.Fatalf("row 1 (empty): expected 2 borrowed custody peers, got %d", len(custodyMap[1]))
	}

	// Row 2 should have its 2 in-cell peers
	if len(custodyMap[2]) != 2 {
		t.Fatalf("row 2: expected 2 custody peers, got %d", len(custodyMap[2]))
	}

	// Row 3 (1 node) should have peer-r3-a + 1 borrowed peer from Row 0
	if len(custodyMap[3]) != 2 {
		t.Fatalf("row 3: expected 2 custody peers, got %d", len(custodyMap[3]))
	}
}

func TestComputeSeedingPlan(t *testing.T) {
	tests := []struct {
		m           int
		kPiece      int
		wantPerPeer int
		wantTotal   int
	}{
		{m: 1, kPiece: 4, wantPerPeer: 8, wantTotal: 8},
		{m: 2, kPiece: 4, wantPerPeer: 4, wantTotal: 8},
		{m: 3, kPiece: 4, wantPerPeer: 3, wantTotal: 9},
		{m: 4, kPiece: 4, wantPerPeer: 2, wantTotal: 8},
	}

	for _, tc := range tests {
		total, perPeer, _ := ComputeSeedingPlan(tc.m, tc.kPiece)
		if perPeer != tc.wantPerPeer || total != tc.wantTotal {
			t.Fatalf("M=%d, kPiece=%d: got (perPeer=%d, total=%d), want (%d, %d)",
				tc.m, tc.kPiece, perPeer, total, tc.wantPerPeer, tc.wantTotal)
		}
	}
}

package p2pcommon

import (
	"bytes"
	"fmt"
	"testing"
)

func TestTopicCellRowSeedAndRecode(t *testing.T) {
	gotSeed := TopicCellRowSeed(2, 5)
	wantSeed := "/cda/1.0.0/col/2/row/5/seed"
	if gotSeed != wantSeed {
		t.Fatalf("TopicCellRowSeed(2, 5) = %q, want %q", gotSeed, wantSeed)
	}

	gotRecode := TopicCellRowRecode(2, 5)
	wantRecode := "/cda/1.0.0/col/2/row/5/recode"
	if gotRecode != wantRecode {
		t.Fatalf("TopicCellRowRecode(2, 5) = %q, want %q", gotRecode, wantRecode)
	}

	if TopicCellRow(2, 5) != wantRecode {
		t.Fatalf("TopicCellRow(2, 5) = %q, want %q", TopicCellRow(2, 5), wantRecode)
	}
}

func TestEncodeDecodeBinaryBatchSeed(t *testing.T) {
	req := &BinaryBatchSeedRequest{
		BlockID:      "block-test-seed",
		ColIdx:       3,
		PieceCommits: [][]byte{[]byte("commit0"), []byte("commit1")},
		SenderPeerID: "bootstrap-peer-0",
		Seeds: []BinarySeedPiece{
			{Row: 1, Col: 3, Data: []byte("data-0"), Coeffs: []byte{1, 2, 3, 4}, Proof: []byte("proof-0")},
			{Row: 1, Col: 3, Data: []byte("data-1"), Coeffs: []byte{5, 6, 7, 8}, Proof: []byte("proof-1")},
		},
	}

	raw, err := EncodeBinaryBatchSeed(req)
	if err != nil {
		t.Fatalf("EncodeBinaryBatchSeed failed: %v", err)
	}

	decoded, err := DecodeBinaryBatchSeed(raw)
	if err != nil {
		t.Fatalf("DecodeBinaryBatchSeed failed: %v", err)
	}

	if decoded.BlockID != req.BlockID || decoded.ColIdx != req.ColIdx || len(decoded.Seeds) != len(req.Seeds) {
		t.Fatalf("decoded mismatch: got %+v, want %+v", decoded, req)
	}
	if !bytes.Equal(decoded.Seeds[1].Data, req.Seeds[1].Data) {
		t.Fatalf("seed[1].Data mismatch")
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
	peers := []PeerInfo{
		{PeerID: "peer-r0-a", Row: 0, Col: 0},
		{PeerID: "peer-r0-b", Row: 0, Col: 0},
		{PeerID: "peer-r0-c", Row: 0, Col: 0},
		{PeerID: "peer-r2-a", Row: 2, Col: 0},
		{PeerID: "peer-r2-b", Row: 2, Col: 0},
		{PeerID: "peer-r3-a", Row: 3, Col: 0},
	}

	custodyMap := ComputeColumnCustodyMap(peers, netRows)

	if len(custodyMap[0]) != 3 {
		t.Fatalf("row 0: expected 3 custody peers, got %d", len(custodyMap[0]))
	}
	if len(custodyMap[1]) != 2 {
		t.Fatalf("row 1 (empty): expected 2 borrowed custody peers, got %d", len(custodyMap[1]))
	}
	if len(custodyMap[2]) != 2 {
		t.Fatalf("row 2: expected 2 custody peers, got %d", len(custodyMap[2]))
	}
	if len(custodyMap[3]) != 2 {
		t.Fatalf("row 3: expected 2 custody peers, got %d", len(custodyMap[3]))
	}
}

func TestComputeSeedingPlan(t *testing.T) {
	tests := []struct {
		m           int
		kPiece      int
		wantTotal   int
		wantPerPeer int
		wantRecodes int
	}{
		{m: 1, kPiece: 4, wantTotal: 8, wantPerPeer: 8, wantRecodes: 5},
		{m: 2, kPiece: 4, wantTotal: 8, wantPerPeer: 4, wantRecodes: 3},
		{m: 3, kPiece: 4, wantTotal: 8, wantPerPeer: 3, wantRecodes: 3},
		{m: 4, kPiece: 4, wantTotal: 8, wantPerPeer: 2, wantRecodes: 2},
	}

	for _, tc := range tests {
		total, perPeer, recodes := ComputeSeedingPlan(tc.m, tc.kPiece)
		if total != tc.wantTotal || perPeer != tc.wantPerPeer || recodes != tc.wantRecodes {
			t.Fatalf("M=%d, kPiece=%d: got (total=%d, perPeer=%d, recodes=%d), want (%d, %d, %d)",
				tc.m, tc.kPiece, total, perPeer, recodes, tc.wantTotal, tc.wantPerPeer, tc.wantRecodes)
		}
	}
}

func TestBinaryPublisherPushAndPublishBlockRoundTrip(t *testing.T) {
	colReq := &BinaryPublisherPushRequest{
		BlockID:      "block-77",
		ColIdx:       5,
		ColumnData:   [][]byte{[]byte("cell-0-data"), []byte("cell-1-data")},
		PieceCommits: [][]byte{[]byte("commit-0"), []byte("commit-1")},
		MerkleProofs: []SerializedMerkleProof{
			{Index: 4, Siblings: [][]byte{[]byte("sib-0"), []byte("sib-1")}},
		},
	}

	var buf bytes.Buffer
	if err := WriteBinaryPublisherPush(&buf, colReq); err != nil {
		t.Fatalf("WriteBinaryPublisherPush failed: %v", err)
	}
	decodedCol, err := ReadBinaryPublisherPush(&buf)
	if err != nil {
		t.Fatalf("ReadBinaryPublisherPush failed: %v", err)
	}
	if decodedCol.BlockID != colReq.BlockID || decodedCol.ColIdx != colReq.ColIdx || len(decodedCol.ColumnData) != 2 {
		t.Fatalf("decoded column push mismatch: %+v", decodedCol)
	}
	if !bytes.Equal(decodedCol.ColumnData[1], colReq.ColumnData[1]) || decodedCol.MerkleProofs[0].Index != 4 {
		t.Fatalf("decoded column data or merkle proof mismatch")
	}

	pubReq := &BinaryPublishBlockRequest{
		BlockID: "block-77",
		Height:  77,
		K:       16,
		Cells:   [][]byte{[]byte("ods-cell-0"), []byte("ods-cell-1")},
		Header: &BinaryHeaderPayload{
			ChainID:         "cda-test",
			Height:          77,
			TimeUnixNano:    1700000000,
			DataHash:        []byte("datahash"),
			AppHash:         []byte("apphash"),
			ProposerAddress: "prop1",
			CommitsRoot:     []byte("root"),
			ColumnComm:      [][]byte{[]byte("c0"), []byte("c1")},
			Coeffs:          []byte("coeffs"),
		},
		Txs: []BinaryTxSummary{
			{Hash: "AB12", Key: "cda/ns/b1/blob", Offset: 0, Cells: 2, Size: 900},
		},
	}
	rawPub, err := EncodeBinaryPublishBlock(pubReq)
	if err != nil {
		t.Fatalf("EncodeBinaryPublishBlock failed: %v", err)
	}
	decodedPub, err := DecodeBinaryPublishBlock(rawPub)
	if err != nil {
		t.Fatalf("DecodeBinaryPublishBlock failed: %v", err)
	}
	if decodedPub.BlockID != "block-77" || decodedPub.Height != 77 || len(decodedPub.Cells) != 2 || len(decodedPub.Txs) != 1 {
		t.Fatalf("decoded publish block mismatch: %+v", decodedPub)
	}
	if !bytes.Equal(decodedPub.Header.CommitsRoot, []byte("root")) || decodedPub.Txs[0].Key != "cda/ns/b1/blob" {
		t.Fatalf("decoded header or tx summary mismatch")
	}
}

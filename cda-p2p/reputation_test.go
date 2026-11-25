package p2pcommon

import (
	"testing"
)

func TestReputationEngine_ContentionImmunityAndStragglerForgiveness(t *testing.T) {
	re := NewReputationEngine(nil)
	peerA := "peer-straggler-A"

	// 1. Peer starts at 60.0 (STANDARD)
	info := re.EnsurePeer(PeerInfo{PeerID: peerA, Row: 0, Col: 0})
	if info.TrustScore != 60.0 || info.TrustTier != TrustTierStandard {
		t.Fatalf("expected initial 60.0 STANDARD, got %.1f %s", info.TrustScore, info.TrustTier)
	}

	// 2. Simulate single-machine contention: peerA is initially marked as missing block-10 (-8.0 -> 52.0),
	// but 300ms later its StoreReady arrives! Straggler forgiveness must reverse the -8.0 penalty and award +2.0 (-> 62.0).
	recAfterMiss := re.RecordEvent(peerA, RepEventMissedBlock, "block-10", 1)
	if recAfterMiss.TrustScore != 52.0 || recAfterMiss.BlocksMissed != 1 {
		t.Fatalf("expected 52.0 with 1 missed block, got %.1f (missed=%d)", recAfterMiss.TrustScore, recAfterMiss.BlocksMissed)
	}

	recAfterLateReady := re.RecordBlockCompleted(peerA, "block-10", 0, 0)
	if recAfterLateReady.TrustScore != 62.0 || recAfterLateReady.BlocksMissed != 0 || recAfterLateReady.StragglerForgiven != 1 {
		t.Fatalf("straggler forgiveness failed: expected 62.0, missed=0, forgiven=1; got score=%.1f, missed=%d, forgiven=%d",
			recAfterLateReady.TrustScore, recAfterLateReady.BlocksMissed, recAfterLateReady.StragglerForgiven)
	}

	// 3. Duplicate StoreReady broadcasts for the same block-10 must be idempotent
	recDup := re.RecordBlockCompleted(peerA, "block-10", 0, 0)
	if recDup.TrustScore != 62.0 || recDup.BlocksCompleted != 1 {
		t.Fatalf("expected duplicate StoreReady to be idempotent at 62.0, got %.1f", recDup.TrustScore)
	}
}

func TestReputationEngine_SoftFaultFloorNeverQuarantinesOrAborts(t *testing.T) {
	re := NewReputationEngine(nil)
	peerSlow := "peer-slow-machine"

	// Even under 50 consecutive soft faults (missed blocks / fetch timeouts / drops on a heavily overloaded host),
	// the node's score must never drop below SoftFaultMinScore (25.0 = PROBATION) and never enter QUARANTINED.
	rec := re.RecordEvent(peerSlow, RepEventMissedBlock, "", 20)
	if rec.TrustScore != SoftFaultMinScore || rec.TrustTier != TrustTierProbation {
		t.Fatalf("expected soft fault floor %.1f (%s), got %.1f (%s)",
			SoftFaultMinScore, TrustTierProbation, rec.TrustScore, rec.TrustTier)
	}

	// When load subsides and the node completes 8 blocks (+16.0 -> 41.0), it self-heals back to STANDARD!
	recHealed := re.RecordEvent(peerSlow, RepEventBlockCompleted, "", 8)
	if recHealed.TrustScore != 41.0 || recHealed.TrustTier != TrustTierStandard {
		t.Fatalf("expected self-healed score 41.0 STANDARD, got %.1f %s", recHealed.TrustScore, recHealed.TrustTier)
	}

	// Only a cryptographic Byzantine fault drops score to 0.0 (QUARANTINED)
	recByz := re.RecordEvent(peerSlow, RepEventByzantineFault, "block-99", 1)
	if recByz.TrustScore != 0.0 || recByz.TrustTier != TrustTierQuarantined {
		t.Fatalf("expected byzantine fault to quarantine at 0.0, got %.1f %s", recByz.TrustScore, recByz.TrustTier)
	}
}

func TestTrustWeightedCustodySelectionAndProbationProtection(t *testing.T) {
	netRows := 4
	// Cell 0 has 3 nodes: 2 healthy (peer-0-trusted=85, peer-0-std=60) and 1 on probation (peer-0-prob=30).
	// Cell 1 has 1 node on probation (peer-1-prob=28).
	// Cell 2 has 1 trusted node (peer-2-trusted=92).
	// Cell 3 has 1 standard node (peer-3-std=60).
	colPeers := []PeerInfo{
		{PeerID: "peer-0-trusted", Row: 0, Col: 0, TrustScore: 85.0, TrustTier: TrustTierTrusted},
		{PeerID: "peer-0-std", Row: 0, Col: 0, TrustScore: 60.0, TrustTier: TrustTierStandard},
		{PeerID: "peer-0-prob", Row: 0, Col: 0, TrustScore: 30.0, TrustTier: TrustTierProbation},
		{PeerID: "peer-1-prob", Row: 1, Col: 0, TrustScore: 28.0, TrustTier: TrustTierProbation},
		{PeerID: "peer-2-trusted", Row: 2, Col: 0, TrustScore: 92.0, TrustTier: TrustTierTrusted},
		{PeerID: "peer-3-std", Row: 3, Col: 0, TrustScore: 60.0, TrustTier: TrustTierStandard},
	}

	cmap := ComputeColumnCustodyMap(colPeers, netRows)

	// 1. In Multi-Node Cell 0 (2 healthy + 1 probation), Primary and Backup must ONLY be chosen from the 2 healthy nodes!
	// The PROBATION node (peer-0-prob) must act as Non-Custody (never Primary) until its score heals >= 40.0.
	for sr := 0; sr < 16; sr++ {
		dataRow := sr * netRows // maps to row 0
		primary, backup, _ := SelectActiveCustodyPairForBlockRow("block-77", dataRow, netRows, cmap)
		if primary == nil || backup == nil {
			t.Fatalf("expected non-nil primary and backup for subRow %d", sr)
		}
		if primary.PeerID == "peer-0-prob" || backup.PeerID == "peer-0-prob" {
			t.Fatalf("probation node peer-0-prob should not be selected as Primary/Backup when >=2 healthy nodes exist in cell (got P=%s, B=%s)",
				primary.PeerID, backup.PeerID)
		}
	}

	// 2. In Single-Node Cell 1 where the sole native node (peer-1-prob) is in PROBATION (28.0 < 40.0):
	// ComputeColumnCustodyMap must borrow a healthy backup node, AND EvaluateNodeCustodyForBlockRow
	// must promote the healthy borrowed backup to co-retain kPiece pieces (isNativeRetention = true)
	// so Cell 1 never relies solely on a PROBATION node!
	cell1Assigned := cmap[1]
	if len(cell1Assigned) != 2 {
		t.Fatalf("expected 2 custody peers for Cell 1, got %d", len(cell1Assigned))
	}
	borrowedGuardian := cell1Assigned[1]
	if !IsEligibleForPrimaryCustody(borrowedGuardian) {
		t.Fatalf("expected borrowed backup for Cell 1 to be healthy (score >= 40), got %s (%.1f)",
			borrowedGuardian.PeerID, borrowedGuardian.TrustScore)
	}
	isCust, isNativeRet, _ := EvaluateNodeCustodyForBlockRow(borrowedGuardian.PeerID, borrowedGuardian.Row, "block-77", 1, netRows, cmap)
	if !isCust || !isNativeRet {
		t.Fatalf("expected healthy borrowed guardian %s for PROBATION Cell 1 to have co-Primary retention (isNativeRetention=true), got isCust=%v isNative=%v",
			borrowedGuardian.PeerID, isCust, isNativeRet)
	}
}

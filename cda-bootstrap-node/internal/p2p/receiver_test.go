package p2p

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"cda-bootstrap-node/internal/engine"
	"cda-bootstrap-node/internal/storage"
	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func setupTestReceiver(t *testing.T, n int) (*Receiver, *cda.GnarkKZG) {
	srs, err := bls12381kzg.NewSRS(512, big.NewInt(-1))
	require.NoError(t, err)

	gnarkKZG := cda.NewGnarkKZG(*srs)
	require.NoError(t, gnarkKZG.InitFK20(n))

	h, err := libp2p.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })

	rcv := &Receiver{
		host:             h,
		kzg:              gnarkKZG,
		kBlock:           n / 2,
		activePeers:      make(map[peer.ID]p2pcommon.PeerInfo),
		lastSeenPeer:     make(map[peer.ID]time.Time),
		blacklistedPeers: make(map[peer.ID]time.Time),
		broadcaster:      NewBroadcaster(h, nil),
	}
	return rcv, gnarkKZG
}

func TestReceiver_BlacklistPeer(t *testing.T) {
	rcv, _ := setupTestReceiver(t, 16)

	_, badPID, err := p2pcommon.GenerateDeterministicKeypair("test-bad-store-1")
	require.NoError(t, err)

	require.False(t, rcv.IsPeerBlacklisted(badPID))

	// Register peer
	rcv.activePeers[badPID] = p2pcommon.PeerInfo{
		PeerID: badPID.String(),
		Row:    0,
		Col:    0,
	}

	// Blacklist peer
	rcv.BlacklistPeer(badPID, 1*time.Hour, "testing blacklist")
	require.True(t, rcv.IsPeerBlacklisted(badPID))

	// Assert evicted from activePeers
	rcv.peersMu.RLock()
	_, exists := rcv.activePeers[badPID]
	rcv.peersMu.RUnlock()
	require.False(t, exists)

	blacklist := rcv.GetBlacklist()
	require.Contains(t, blacklist, badPID.String())
}

func TestReceiver_VerifyAndHandleByzantineReport(t *testing.T) {
	n := 16
	kPiece := 4
	cellSize := 512
	pieceSize := cellSize / kPiece
	rcv, kzgProv := setupTestReceiver(t, n)

	_, badPID, err := p2pcommon.GenerateDeterministicKeypair("byzantine-peer-reported")
	require.NoError(t, err)
	_, reporterPID, err := p2pcommon.GenerateDeterministicKeypair("honest-reporter")
	require.NoError(t, err)

	// Generate synthetic column data
	columnData := make([][]byte, n)
	for r := 0; r < n; r++ {
		columnData[r] = make([]byte, cellSize)
		_, _ = rand.Read(columnData[r])
	}

	// Piece commitments
	pieceCommits := make([]cda.PieceCommitment, kPiece)
	hexCommits := make([]string, kPiece)
	for p := 0; p < kPiece; p++ {
		pieceCol := make([][]byte, n)
		for r := 0; r < n; r++ {
			pieceCol[r] = columnData[r][p*pieceSize : (p+1)*pieceSize]
		}
		commit, err := kzgProv.Commit(pieceCol)
		require.NoError(t, err)
		pieceCommits[p] = commit
		hexCommits[p] = hex.EncodeToString(commit)
	}

	// Proof generator & RLNC encoder
	proofGen := engine.NewProofGenerator(kPiece, kzgProv)
	proofs, err := proofGen.GenerateColumnProofs(0, columnData)
	require.NoError(t, err)

	encoder := engine.NewRLNCEncoder(kPiece, kzgProv)
	pieces, err := encoder.EncodeRowNSeeds(0, 0, columnData, proofs[0], 2)
	require.NoError(t, err)
	validPiece := pieces[0]

	// Scenario 1: Genuinely corrupted piece data -> should verify byzantine report and blacklist reported peer
	corruptedData := make([]byte, len(validPiece.Data.Data))
	copy(corruptedData, validPiece.Data.Data)
	corruptedData[0] ^= 0xFF // Corrupt data

	reportCorrupted := p2pcommon.ByzantineNodeReport{
		BlockID:        "block-byz-test-1",
		Height:         1,
		Row:            0,
		Col:            0,
		ReportedPeerID: badPID.String(),
		ReporterPeerID: reporterPID.String(),
		Data:           hex.EncodeToString(corruptedData),
		Coeffs:         hex.EncodeToString(validPiece.Data.Coeffs),
		Proof:          hex.EncodeToString(validPiece.Proof),
		PieceCommits:   hexCommits,
		Reason:         "tampered_data_piece",
		Timestamp:      time.Now().Unix(),
	}

	verified, err := rcv.VerifyAndHandleByzantineReport(reportCorrupted)
	require.NoError(t, err)
	require.True(t, verified, "Corrupted piece should be confirmed as byzantine")
	require.True(t, rcv.IsPeerBlacklisted(badPID), "Accused peer must be blacklisted")
	require.False(t, rcv.IsPeerBlacklisted(reporterPID), "Honest reporter must NOT be blacklisted")

	// Scenario 2: Griefing attack (Piece is completely valid, reporter made false accusation) -> blacklist reporter!
	_, griefingReporterPID, err := p2pcommon.GenerateDeterministicKeypair("griefing-reporter")
	require.NoError(t, err)
	_, innocentPID, err := p2pcommon.GenerateDeterministicKeypair("innocent-store")
	require.NoError(t, err)

	reportGriefing := p2pcommon.ByzantineNodeReport{
		BlockID:        "block-byz-test-2",
		Height:         2,
		Row:            0,
		Col:            0,
		ReportedPeerID: innocentPID.String(),
		ReporterPeerID: griefingReporterPID.String(),
		Data:           hex.EncodeToString(validPiece.Data.Data), // Legit data!
		Coeffs:         hex.EncodeToString(validPiece.Data.Coeffs),
		Proof:          hex.EncodeToString(validPiece.Proof),
		PieceCommits:   hexCommits,
		Reason:         "false_accusation_claim",
		Timestamp:      time.Now().Unix(),
	}

	verifiedGrief, err := rcv.VerifyAndHandleByzantineReport(reportGriefing)
	require.NoError(t, err)
	require.False(t, verifiedGrief, "Legit piece should reject byzantine report")
	require.False(t, rcv.IsPeerBlacklisted(innocentPID), "Innocent peer must NOT be blacklisted")
	require.True(t, rcv.IsPeerBlacklisted(griefingReporterPID), "Griefing reporter must be blacklisted")
}

func TestReceiver_BlockAggregatorBarrier(t *testing.T) {
	t.Setenv("BOOTSTRAP_BARRIER_TIMEOUT_MS", "100")
	n := 16
	kPiece := 4
	cellSize := 512
	rcv, kzgProv := setupTestReceiver(t, n)

	rcv.cache = storage.NewLocalCache()
	rcv.proofGen = engine.NewProofGenerator(kPiece, kzgProv)
	rcv.encoder = engine.NewRLNCEncoder(kPiece, kzgProv)
	rcv.netCols = 4 // expectedCols = 16 / 4 = 4
	rcv.netRows = 4
	rcv.pendingBatches = make(map[string]*pendingBlockBatch)

	// Scenario 1: All expected columns arrive -> triggers immediate batch flush
	blockID1 := "block-barrier-1"
	colDataList := make([][][]byte, 4)
	pieceCommitsList := make([][][]byte, 4)
	merkleProofsList := make([][]cda.MerkleProof, 4)

	for c := 0; c < 4; c++ {
		colData := make([][]byte, n)
		for r := 0; r < n; r++ {
			colData[r] = make([]byte, cellSize)
			_, _ = rand.Read(colData[r])
		}
		colDataList[c] = colData

		pieceCommits := make([][]byte, kPiece)
		for p := 0; p < kPiece; p++ {
			pieceCol := make([][]byte, n)
			for r := 0; r < n; r++ {
				pieceCol[r] = colData[r][p*128 : (p+1)*128]
			}
			commit, err := kzgProv.Commit(pieceCol)
			require.NoError(t, err)
			pieceCommits[p] = commit
		}
		pieceCommitsList[c] = pieceCommits
		merkleProofsList[c] = []cda.MerkleProof{}

		rcv.cache.Store(blockID1, c, colData, pieceCommits)
		rcv.enqueueColumnForPhase2(blockID1, 1, c, colData, pieceCommits, merkleProofsList[c])
	}

	// Wait for async batch to complete
	require.Eventually(t, func() bool {
		for c := 0; c < 4; c++ {
			entry, ok := rcv.cache.GetCol(blockID1, c)
			if !ok || entry == nil || entry.Proofs == nil {
				return false
			}
		}
		return true
	}, 3*time.Second, 20*time.Millisecond, "Batch proofs should be generated for all 4 columns")

	for c := 0; c < 4; c++ {
		entry, ok := rcv.cache.GetCol(blockID1, c)
		require.True(t, ok)
		require.NotNil(t, entry.Proofs)
		require.Len(t, entry.Proofs, n)
	}

	// Scenario 2: Partial columns arrive (only 2 out of 4) -> fallback timer flushes automatically
	blockID2 := "block-barrier-2"
	for c := 0; c < 2; c++ {
		colData := make([][]byte, n)
		for r := 0; r < n; r++ {
			colData[r] = make([]byte, cellSize)
			_, _ = rand.Read(colData[r])
		}
		pieceCommits := make([][]byte, kPiece)
		for p := 0; p < kPiece; p++ {
			pieceCommits[p] = pieceCommitsList[0][p]
		}
		rcv.cache.Store(blockID2, c, colData, pieceCommits)
		rcv.enqueueColumnForPhase2(blockID2, 2, c, colData, pieceCommits, []cda.MerkleProof{})
	}

	// Fallback timer is 100ms -> should complete within 1.5s
	require.Eventually(t, func() bool {
		e0, ok0 := rcv.cache.GetCol(blockID2, 0)
		e1, ok1 := rcv.cache.GetCol(blockID2, 1)
		return ok0 && ok1 && e0 != nil && e1 != nil && e0.Proofs != nil && e1.Proofs != nil
	}, 2*time.Second, 20*time.Millisecond, "Fallback timer should flush partial batch for block 2")
}

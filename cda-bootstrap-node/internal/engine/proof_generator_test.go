package engine

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"runtime"
	"sync"
	"testing"
	"time"

	p2pcommon "cda-p2p"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFK20_ProofGenerator_And_StoreVerification_EndToEnd(t *testing.T) {
	n := 128 // EDS width
	kPiece := 8
	cellSize := 512 // 512 bytes per cell, 64 bytes per piece
	pieceSize := cellSize / kPiece

	// 1. Generate synthetic column data
	columnData := make([][]byte, n)
	for r := 0; r < n; r++ {
		columnData[r] = make([]byte, cellSize)
		_, err := rand.Read(columnData[r])
		require.NoError(t, err)
	}

	// 2. Initialize SRS & KZG Providers
	srs, err := bls12381kzg.NewSRS(1024, big.NewInt(-1))
	require.NoError(t, err)

	kzgBootstrap := cda.NewGnarkKZG(*srs)
	require.NoError(t, kzgBootstrap.InitFK20(n))

	kzgStore := cda.NewGnarkKZG(*srs)
	require.NoError(t, kzgStore.SetDomainSize(n))

	// 3. Compute ground truth piece commitments
	pieceCommits := make([]cda.PieceCommitment, kPiece)
	for p := 0; p < kPiece; p++ {
		pieceCol := make([][]byte, n)
		for r := 0; r < n; r++ {
			pieceCol[r] = columnData[r][p*pieceSize : (p+1)*pieceSize]
		}
		commit, err := kzgBootstrap.Commit(pieceCol)
		require.NoError(t, err)
		pieceCommits[p] = commit
	}

	// 4. Generate all column proofs via FK20
	proofGen := NewProofGenerator(kPiece, kzgBootstrap)

	startFK20 := time.Now()
	proofs, err := proofGen.GenerateColumnProofs(0, columnData)
	fk20Duration := time.Since(startFK20)
	require.NoError(t, err)
	require.Len(t, proofs, n)

	t.Logf("FK20 GenerateColumnProofs for N=%d, K=%d took %v", n, kPiece, fk20Duration)
	assert.Less(t, fk20Duration, 250*time.Millisecond, "FK20 proof generation must complete in <250ms")

	// 5. RLNC Encoder encodes seeds for row r
	encoder := NewRLNCEncoder(kPiece, kzgBootstrap)
	testRows := []int{0, 1, 42, 63, 100, 127}

	for _, targetRow := range testRows {
		numSeeds := 16
		pieces, err := encoder.EncodeRowNSeeds(targetRow, 0, columnData, proofs[targetRow], numSeeds)
		require.NoError(t, err)
		require.Len(t, pieces, numSeeds)

		// 6. Verify each individual piece as Store Node does
		batchItems := make([]cda.BatchVerifyItem, numSeeds)
		for i, piece := range pieces {
			combinedCommit, err := kzgStore.Combine(pieceCommits, piece.Data.Coeffs)
			require.NoError(t, err)

			verified := kzgStore.Verify(combinedCommit, piece.Row, piece.Data.Data, piece.Proof)
			require.True(t, verified, "Piece %d verification failed for row %d", i, targetRow)

			batchItems[i] = cda.BatchVerifyItem{
				Commitment: combinedCommit,
				Row:        piece.Row,
				Data:       piece.Data.Data,
				Proof:      piece.Proof,
			}
		}

		// 7. Verify all pieces via BatchVerify
		batchVerified := kzgStore.BatchVerify(batchItems)
		require.True(t, batchVerified, "BatchVerify failed for row %d", targetRow)

		// 8. Multi-hop recoding test: Recode pieces and verify recoded piece
		rm := cda.NewRecipientManager(kPiece, kzgStore)
		recodedPiecesIn := make([]cda.ReceivedPiece, 4)
		for i := 0; i < 4; i++ {
			recodedPiecesIn[i] = *pieces[i]
		}
		recoded, err := rm.RecodePieces(recodedPiecesIn)
		require.NoError(t, err)

		recodedCommit, err := kzgStore.Combine(pieceCommits, recoded.Data.Coeffs)
		require.NoError(t, err)

		recodedVerified := kzgStore.Verify(recodedCommit, recoded.Row, recoded.Data.Data, recoded.Proof)
		require.True(t, recodedVerified, "Recoded piece verification failed for row %d", targetRow)
	}
}

func TestEncodeRowNSeeds_StrictLinearIndependence(t *testing.T) {
	kPiece := 8
	n := 16
	cellSize := 512
	columnData := make([][]byte, n)
	for r := 0; r < n; r++ {
		columnData[r] = make([]byte, cellSize)
		_, _ = rand.Read(columnData[r])
	}

	srs, err := bls12381kzg.NewSRS(128, big.NewInt(-1))
	require.NoError(t, err)
	kzgBootstrap := cda.NewGnarkKZG(*srs)
	require.NoError(t, kzgBootstrap.InitFK20(n))

	proofGen := NewProofGenerator(kPiece, kzgBootstrap)
	proofs, err := proofGen.GenerateColumnProofs(0, columnData)
	require.NoError(t, err)

	encoder := NewRLNCEncoder(kPiece, kzgBootstrap)

	// Test 5 random rows, verifying both primary (seeds 0..7) and backup (seeds 8..15)
	for row := 0; row < 5; row++ {
		seeds, err := encoder.EncodeRowNSeeds(row, 0, columnData, proofs[row], 16)
		require.NoError(t, err)
		require.Len(t, seeds, 16)

		// Group 1: Primary seeds (0..7) must have rank exactly kPiece
		primaryBasis := NewEchelonBasis(kPiece)
		for i := 0; i < kPiece; i++ {
			added := primaryBasis.TryAdd(seeds[i].Data.Coeffs)
			require.True(t, added, "Primary seed %d must be linearly independent", i)
		}
		require.Equal(t, kPiece, primaryBasis.Rank(), "Primary group must have full rank kPiece")

		// Group 2: Backup seeds (8..15) must have rank exactly kPiece
		backupBasis := NewEchelonBasis(kPiece)
		for i := kPiece; i < 2*kPiece; i++ {
			added := backupBasis.TryAdd(seeds[i].Data.Coeffs)
			require.True(t, added, "Backup seed %d must be linearly independent", i)
		}
		require.Equal(t, kPiece, backupBasis.Rank(), "Backup group must have full rank kPiece")
	}
}

func TestPrecomputeColumnFragments_FFT_EquivalenceAndVerification(t *testing.T) {
	n := 128
	kPiece := 4
	cellSize := 512
	frSize := cellSize / kPiece

	columnData := make([][]byte, n)
	for r := 0; r < n; r++ {
		columnData[r] = make([]byte, cellSize)
		_, err := rand.Read(columnData[r])
		require.NoError(t, err)
	}

	srs, err := bls12381kzg.NewSRS(256, big.NewInt(-1))
	require.NoError(t, err)
	kzgBootstrap := cda.NewGnarkKZG(*srs)
	require.NoError(t, kzgBootstrap.InitFK20(n))

	encoder := NewRLNCEncoder(kPiece, kzgBootstrap)

	startFFT := time.Now()
	fftFragments := encoder.PrecomputeColumnFragments(columnData)
	fftDuration := time.Since(startFFT)
	require.Len(t, fftFragments, n)

	startDirect := time.Now()
	for r := 0; r < n; r++ {
		z := kzgBootstrap.GnarkGetEvaluationPointForDomain(r, n)
		zPowers := ComputeZPowersFromPoint(z, n)
		for j := 0; j < kPiece; j++ {
			expected := EvaluatePieceColumnWithPowers(columnData, j, r, kPiece, frSize, zPowers)
			require.Equal(t, expected, fftFragments[r][j], "FFT fragment mismatch at row %d piece %d", r, j)
		}
	}
	directDuration := time.Since(startDirect)
	t.Logf("Column fragment evaluation (N=%d, k=%d): FFT=%v vs O(N^2) Direct=%v", n, kPiece, fftDuration, directDuration)
}

func TestStageByStageK64Profiler(t *testing.T) {
	kBlock := 64
	n := 2 * kBlock // 128x128 EDS (16,384 cells = 8 MB)
	kPiece := 4
	cellSize := 512
	numWorkers := runtime.GOMAXPROCS(0)

	srs, err := bls12381kzg.NewSRS(256, big.NewInt(-1))
	require.NoError(t, err)
	kzgProvider := cda.NewGnarkKZG(*srs)
	require.NoError(t, kzgProvider.InitFK20(n))

	// Generate 64x64 ODS (4,096 cells = 2 MB)
	odsCells := make([][]byte, kBlock*kBlock)
	for i := range odsCells {
		odsCells[i] = make([]byte, cellSize)
		_, _ = rand.Read(odsCells[i])
	}

	// =========================================================================
	// STAGE 1: Wire Serialization + Publisher ProcessODS (RS 2D + 512 KZG + Merkle)
	// =========================================================================
	type legacyPublishJSON struct {
		BlockID string   `json:"block_id"`
		Height  int64    `json:"height"`
		K       int      `json:"k"`
		Cells   []string `json:"cells"`
	}
	startJSONWire := time.Now()
	b64Cells := make([]string, len(odsCells))
	for i, c := range odsCells {
		b64Cells[i] = base64.StdEncoding.EncodeToString(c)
	}
	jsonBytes, err := json.Marshal(legacyPublishJSON{
		BlockID: "block-64",
		Height:  64,
		K:       kBlock,
		Cells:   b64Cells,
	})
	require.NoError(t, err)
	var decodedJSON legacyPublishJSON
	require.NoError(t, json.Unmarshal(jsonBytes, &decodedJSON))
	for _, s := range decodedJSON.Cells {
		_, _ = base64.StdEncoding.DecodeString(s)
	}
	stage1WireBefore := time.Since(startJSONWire)

	startBinWire := time.Now()
	binReq := &p2pcommon.BinaryPublishBlockRequest{
		BlockID: "block-64",
		Height:  64,
		K:       kBlock,
		Cells:   odsCells,
	}
	binEncoded, err := p2pcommon.EncodeBinaryPublishBlock(binReq)
	require.NoError(t, err)
	_, err = p2pcommon.DecodeBinaryPublishBlock(binEncoded)
	require.NoError(t, err)
	stage1WireAfter := time.Since(startBinWire)

	// Publisher ProcessODS: 64x64 -> 128x128 Leopard RS + 512 KZG Commitments + Merkle
	startRS := time.Now()
	eds, err := cda.ComputeExtendedDataSquareWithLeopard(odsCells)
	require.NoError(t, err)
	rsDuration := time.Since(startRS)

	startKZGCommit := time.Now()
	codec := rlnc.NewRLNCCodec(kPiece)
	pubData, err := cda.ComputeAndSetKateCommitments(codec, &eds, kzgProvider, 0)
	require.NoError(t, err)
	pieceCommitsBytes := make([][]byte, len(pubData.PieceComm))
	for i := range pubData.PieceComm {
		pieceCommitsBytes[i] = append([]byte(nil), pubData.PieceComm[i]...)
	}
	_, _ = cda.BuildMerkleTree(pieceCommitsBytes)
	kzgCommitDuration := time.Since(startKZGCommit)

	allColData := make([][][]byte, n)
	allPieceCommits := make([][][]byte, n)
	for c := 0; c < n; c++ {
		allColData[c] = eds.Col(uint(c))
		allPieceCommits[c] = make([][]byte, kPiece)
		for p := 0; p < kPiece; p++ {
			allPieceCommits[c][p] = pubData.PieceComm[c*kPiece+p]
		}
	}

	// Also measure Publisher -> Bootstrap 128-column wire serialization (JSON vs Binary CDA\x02)
	startPubPushJSON := time.Now()
	for c := 0; c < n; c++ {
		hexCol := make([]string, n)
		for r := 0; r < n; r++ {
			hexCol[r] = hex.EncodeToString(allColData[c][r])
		}
		hexComm := make([]string, kPiece)
		for p := 0; p < kPiece; p++ {
			hexComm[p] = hex.EncodeToString(allPieceCommits[c][p])
		}
		msg := p2pcommon.PublisherPushRequest{
			BlockID: "block-64", ColIdx: c,
			ColumnData: hexCol, PieceCommits: hexComm,
		}
		b, _ := json.Marshal(msg)
		var out p2pcommon.PublisherPushRequest
		_ = json.Unmarshal(b, &out)
		for _, s := range out.ColumnData {
			_, _ = hex.DecodeString(s)
		}
	}
	pubPushJSONDur := time.Since(startPubPushJSON)

	startPubPushBin := time.Now()
	for c := 0; c < n; c++ {
		var b bytes.Buffer
		msg := &p2pcommon.BinaryPublisherPushRequest{
			BlockID: "block-64", ColIdx: uint16(c),
			ColumnData: allColData[c], PieceCommits: allPieceCommits[c],
		}
		_ = p2pcommon.WriteBinaryPublisherPush(&b, msg)
		_, _ = p2pcommon.ReadBinaryPublisherPush(&b)
	}
	pubPushBinDur := time.Since(startPubPushBin)

	// =========================================================================
	// STAGE 2: Bootstrap Node (FK20 Proofs + Column Fragment Eval + RLNC Seeding)
	// =========================================================================
	proofGen := NewProofGenerator(kPiece, kzgProvider)
	encoder := NewRLNCEncoder(kPiece, kzgProvider)

	// FK20 for 1 NetCol (16 columns) & extrapolated to 8 NetCols (128 columns)
	colsPerNetCol := 16
	colMap16 := make(map[int][][]byte, colsPerNetCol)
	for i := 0; i < colsPerNetCol; i++ {
		colMap16[i] = allColData[i]
	}
	startFK20 := time.Now()
	multiProofs16, err := proofGen.GenerateMultiColumnProofs(colMap16)
	require.NoError(t, err)
	fk20NetColDur := time.Since(startFK20)
	fk20Full128Dur := fk20NetColDur * 8

	// Old Stage 2 Seeding (O(N^2) direct polynomial evaluation per row across 16 columns, scaled to 128 columns)
	startSeedOld := time.Now()
	var oldSeedWG sync.WaitGroup
	oldColCh := make(chan int, colsPerNetCol)
	for c := 0; c < colsPerNetCol; c++ {
		oldColCh <- c
	}
	close(oldColCh)
	for w := 0; w < numWorkers; w++ {
		oldSeedWG.Add(1)
		go func() {
			defer oldSeedWG.Done()
			for c := range oldColCh {
				for r := 0; r < n; r++ {
					_, _ = encoder.EncodeRowNSeeds(r, c, allColData[c], multiProofs16[c][r], 8)
				}
			}
		}()
	}
	oldSeedWG.Wait()
	seedOldNetColDur := time.Since(startSeedOld)
	seedOldFull128Dur := seedOldNetColDur * 8

	// New Stage 2 Seeding (O(N log N) FFT PrecomputeColumnFragments + EncodeRowNSeedsWithFragments)
	seededPiecesNetCol := make([][][]*cda.ReceivedPiece, colsPerNetCol)
	for c := 0; c < colsPerNetCol; c++ {
		seededPiecesNetCol[c] = make([][]*cda.ReceivedPiece, n)
	}
	startSeedNew := time.Now()
	var newSeedWG sync.WaitGroup
	newColCh := make(chan int, colsPerNetCol)
	for c := 0; c < colsPerNetCol; c++ {
		newColCh <- c
	}
	close(newColCh)
	for w := 0; w < numWorkers; w++ {
		newSeedWG.Add(1)
		go func() {
			defer newSeedWG.Done()
			for c := range newColCh {
				frags := encoder.PrecomputeColumnFragments(allColData[c])
				for r := 0; r < n; r++ {
					pieces, err := encoder.EncodeRowNSeedsWithFragments(r, c, frags[r], multiProofs16[c][r], 8)
					require.NoError(t, err)
					seededPiecesNetCol[c][r] = pieces
				}
			}
		}()
	}
	newSeedWG.Wait()
	seedNewNetColDur := time.Since(startSeedNew)
	seedNewFull128Dur := seedNewNetColDur * 8

	// =========================================================================
	// STAGE 3: Store Node (1 Store Node Custody Column = 16 cols x 128 rows = 2,048 cells)
	// =========================================================================
	// In an 8x2 Store matrix at K=64, each Store Node has custody of 16 columns x 64 rows = 1,024 custody cells
	// (2 seeds per cell = 2,048 incoming seed pieces) + 1,024 non-custody cells (2 recoded pieces per cell = 2,048 pieces).
	var verifyItems []cda.BatchVerifyItem
	var recodeReqs []cda.RecodeBatchRequest
	for c := 0; c < colsPerNetCol; c++ {
		pCommits := make([]cda.PieceCommitment, kPiece)
		for p := 0; p < kPiece; p++ {
			pCommits[p] = allPieceCommits[c][p]
		}
		for r := 0; r < n; r++ {
			pieces := seededPiecesNetCol[c][r]
			// Take 2 pieces per cell (as Store Node does in M=2 custody / non-custody)
			inPieces := make([]cda.ReceivedPiece, 2)
			for idx := 0; idx < 2; idx++ {
				inPieces[idx] = *pieces[idx]
				comb, _ := kzgProvider.Combine(pCommits, pieces[idx].Data.Coeffs)
				verifyItems = append(verifyItems, cda.BatchVerifyItem{
					Commitment: comb,
					Row:        r,
					Data:       pieces[idx].Data.Data,
					Proof:      pieces[idx].Proof,
				})
			}
			recodeReqs = append(recodeReqs, cda.RecodeBatchRequest{
				Pieces:       inPieces,
				PieceCommits: allPieceCommits[c],
				Count:        2,
			})
		}
	}

	// Verify 1x (Deduplicated Seed Batch) vs 2x (Duplicate GossipSub + Direct TCP Seed Batch)
	startVerifyOnce := time.Now()
	// Include Combine cost + BatchVerifyForDomain in chunks of 256 (matching Store Node worker batching)
	var vWG sync.WaitGroup
	chunkSize := 256
	numChunks := (len(verifyItems) + chunkSize - 1) / chunkSize
	chunkCh := make(chan int, numChunks)
	for i := 0; i < numChunks; i++ {
		chunkCh <- i
	}
	close(chunkCh)
	for w := 0; w < numWorkers; w++ {
		vWG.Add(1)
		go func() {
			defer vWG.Done()
			for i := range chunkCh {
				s := i * chunkSize
				e := s + chunkSize
				if e > len(verifyItems) {
					e = len(verifyItems)
				}
				ok := kzgProvider.BatchVerifyForDomain(verifyItems[s:e], n)
				require.True(t, ok)
			}
		}()
	}
	vWG.Wait()
	storeSeedVerifyNew := time.Since(startVerifyOnce)
	storeSeedVerifyOld := storeSeedVerifyNew * 2 // Old code verified both GossipSub and Direct TCP batches

	// Compare Old RecodeBatchWithVerify vs New RecodeBatchTrusted on 2,048 cells
	rm := cda.NewRecipientManager(kPiece, kzgProvider)

	startRecodeOld := time.Now()
	outOld, err := rm.RecodeBatchWithVerify(recodeReqs, 5, n)
	require.NoError(t, err)
	require.Len(t, outOld, len(recodeReqs))
	storeRecodeOld := time.Since(startRecodeOld)

	startRecodeNew := time.Now()
	outNew, err := rm.RecodeBatchTrusted(recodeReqs, 5)
	require.NoError(t, err)
	require.Len(t, outNew, len(recodeReqs))
	storeRecodeNew := time.Since(startRecodeNew)

	t.Logf("\n================================================================================")
	t.Logf("STAGE-BY-STAGE K=64 (128x128 EDS = 16,384 CELLS = 8 MB) PROFILER RESULTS (GOMAXPROCS=%d)", numWorkers)
	t.Logf("================================================================================")
	t.Logf("[Stage 1: Wire + Publisher]")
	t.Logf("  - CometBFT -> Publisher Wire (2 MB ODS) : Before (JSON) = %v | After (Binary CDA\\x03) = %v", stage1WireBefore, stage1WireAfter)
	t.Logf("  - Publisher -> Bootstrap Wire (8 MB EDS): Before (JSON) = %v | After (Binary CDA\\x02) = %v", pubPushJSONDur, pubPushBinDur)
	t.Logf("  - Publisher Leopard RS 2D + Merkle      : %v", rsDuration)
	t.Logf("  - Publisher 512 KZG Commitments         : %v", kzgCommitDuration)
	t.Logf("[Stage 2: Bootstrap Node (128 Columns)]")
	t.Logf("  - FK20 Multi-Column Proofs (128 cols)   : %v (16 cols = %v)", fk20Full128Dur, fk20NetColDur)
	t.Logf("  - Fragment Eval + RLNC Seeding (128 col): Before (O(N^2)) = %v | After (FFT O(N log N)) = %v", seedOldFull128Dur, seedNewFull128Dur)
	t.Logf("[Stage 3: Store Node (1 Node = 16 Cols x 128 Rows = 2,048 Cells)]")
	t.Logf("  - Seed Batch KZG Verification           : Before (2x Dup) = %v | After (1x Dedup) = %v", storeSeedVerifyOld, storeSeedVerifyNew)
	t.Logf("  - Batch RLNC Recoding (2,048 cells)     : Before (RecodeBatchWithVerify) = %v | After (RecodeBatchTrusted) = %v", storeRecodeOld, storeRecodeNew)
	t.Logf("================================================================================")
}



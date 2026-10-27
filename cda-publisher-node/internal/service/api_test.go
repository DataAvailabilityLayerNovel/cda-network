package service

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"cda-publisher-node/internal/engine"
	"github.com/dgraph-io/badger/v4"
)

func TestVerifyCDAHeader_Success(t *testing.T) {
	computed := &engine.BlockHeader{
		BlockID:     "block-1",
		CommitsRoot: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
		ColumnComm: [][]byte{
			[]byte("col1_commitment_bytes_000000000"),
			[]byte("col2_commitment_bytes_000000000"),
		},
		Coeffs: []byte("coeffs_bytes_1234567890123456789"),
	}

	bftHeader := &HeaderPayload{
		CommitsRoot: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
		ColumnComm: []string{
			hex.EncodeToString([]byte("col1_commitment_bytes_000000000")),
			hex.EncodeToString([]byte("col2_commitment_bytes_000000000")),
		},
		Coeffs: hex.EncodeToString([]byte("coeffs_bytes_1234567890123456789")),
	}

	err := VerifyCDAHeader(computed, bftHeader)
	if err != nil {
		t.Fatalf("expected VerifyCDAHeader to succeed, got: %v", err)
	}
}

func TestVerifyCDAHeader_NilBFTHeader(t *testing.T) {
	computed := &engine.BlockHeader{
		BlockID:     "block-1",
		CommitsRoot: "abcdef",
	}

	// Should pass for backwards compatibility when no BFT header is provided
	err := VerifyCDAHeader(computed, nil)
	if err != nil {
		t.Fatalf("expected nil error for nil BFT header, got: %v", err)
	}
}

func TestVerifyCDAHeader_MismatchedCommitsRoot(t *testing.T) {
	computed := &engine.BlockHeader{
		BlockID:     "block-1",
		CommitsRoot: "1111111111111111111111111111111111111111111111111111111111111111",
	}

	bftHeader := &HeaderPayload{
		CommitsRoot: "2222222222222222222222222222222222222222222222222222222222222222",
	}

	err := VerifyCDAHeader(computed, bftHeader)
	if err == nil {
		t.Fatalf("expected error for mismatched CommitsRoot, got nil")
	}
}

func TestVerifyCDAHeader_MismatchedColumnCommCount(t *testing.T) {
	computed := &engine.BlockHeader{
		BlockID:     "block-1",
		CommitsRoot: "abcdef",
		ColumnComm: [][]byte{
			[]byte("col1"),
		},
	}

	bftHeader := &HeaderPayload{
		CommitsRoot: "abcdef",
		ColumnComm: []string{
			hex.EncodeToString([]byte("col1")),
			hex.EncodeToString([]byte("col2")),
		},
	}

	err := VerifyCDAHeader(computed, bftHeader)
	if err == nil {
		t.Fatalf("expected error for mismatched ColumnComm count, got nil")
	}
}

func TestVerifyCDAHeader_MismatchedColumnCommData(t *testing.T) {
	computed := &engine.BlockHeader{
		BlockID:     "block-1",
		CommitsRoot: "abcdef",
		ColumnComm: [][]byte{
			[]byte("col1_actual"),
		},
	}

	bftHeader := &HeaderPayload{
		CommitsRoot: "abcdef",
		ColumnComm: []string{
			hex.EncodeToString([]byte("col1_tampered")),
		},
	}

	err := VerifyCDAHeader(computed, bftHeader)
	if err == nil {
		t.Fatalf("expected error for tampered ColumnComm data, got nil")
	}
}

func TestVerifyCDAHeader_MismatchedCoeffs(t *testing.T) {
	computed := &engine.BlockHeader{
		BlockID:     "block-1",
		CommitsRoot: "abcdef",
		Coeffs:      []byte("coeffs_actual"),
	}

	bftHeader := &HeaderPayload{
		CommitsRoot: "abcdef",
		Coeffs:      hex.EncodeToString([]byte("coeffs_tampered")),
	}

	err := VerifyCDAHeader(computed, bftHeader)
	if err == nil {
		t.Fatalf("expected error for tampered Coeffs, got nil")
	}
}

func TestHandleGetTx_BadRequest(t *testing.T) {
	svc := &APIService{}
	req, _ := http.NewRequest(http.MethodGet, "/tx", nil)
	rr := httptest.NewRecorder()
	svc.handleGetTx(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for missing params, got %d", rr.Code)
	}
}

func TestHandleGetTx_HotPath_CometBFT(t *testing.T) {
	expectedPayload := "tx_test_key_0=deadbeef1234"
	expectedB64 := base64.StdEncoding.EncodeToString([]byte(expectedPayload))

	mockComet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"result": map[string]interface{}{
				"hash":   "AA11BB22CC33",
				"height": "42",
				"tx":     expectedB64,
				"proof":  map[string]string{"type": "mock_merkle_proof"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer mockComet.Close()

	svc := &APIService{
		cometbftRPC: mockComet.URL,
	}

	req, _ := http.NewRequest(http.MethodGet, "/tx?hash=AA11BB22CC33", nil)
	rr := httptest.NewRecorder()
	svc.handleGetTx(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp TxResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Source != "cometbft" {
		t.Errorf("expected source cometbft, got %s", resp.Source)
	}
	if resp.Height != 42 {
		t.Errorf("expected height 42, got %d", resp.Height)
	}
	if resp.Payload != expectedPayload {
		t.Errorf("expected payload %s, got %s", expectedPayload, resp.Payload)
	}
	if resp.Key != "tx_test_key_0" {
		t.Errorf("expected key tx_test_key_0, got %s", resp.Key)
	}
}

func TestHandleGetTx_ColdPath_CDA(t *testing.T) {
	dbDir := t.TempDir()
	opts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger db: %v", err)
	}
	defer db.Close()

	testHash := "F1E2D3C4B5A600112233"
	testKey := "tx_cold_key_1"
	testPayload := "tx_cold_key_1=998877665544332211"
	expectedCommitsRoot := "0xmock_cda_commits_root_abcdef"
	blockID := "block-5"
	height := int64(5)

	header := &engine.BlockHeader{
		BlockID:     blockID,
		CommitsRoot: expectedCommitsRoot,
		ColumnComm:  [][]byte{[]byte("c0"), []byte("c1"), []byte("c2"), []byte("c3")},
	}
	headerBytes, _ := json.Marshal(header)

	meta := TxSummary{
		Hash:   testHash,
		Key:    testKey,
		Offset: 0,
		Cells:  1,
		Size:   len(testPayload),
	}
	metaBytes, _ := json.Marshal(meta)

	// Seed BadgerDB with index and metadata
	_ = db.Update(func(txn *badger.Txn) error {
		_ = txn.Set([]byte("idx_tx_"+testHash), []byte("5"))
		_ = txn.Set([]byte("idx_key_"+testKey), []byte("5"))
		_ = txn.Set([]byte("block_height_5"), []byte(blockID))
		_ = txn.Set([]byte("header_"+blockID), headerBytes)
		_ = txn.Set([]byte("meta_tx_"+blockID+"_"+testHash), metaBytes)
		_ = txn.Set([]byte("meta_key_"+blockID+"_"+testKey), []byte(testHash))
		return nil
	})

	reconstructor := engine.NewBlockReconstructor(nil, nil, nil)
	// Seed LRU cache with the recovered cell
	cellData := make([]byte, 512)
	copy(cellData, []byte(testPayload))
	reconstructor.PutCachedODS(blockID, height, [][]byte{cellData, make([]byte, 512), make([]byte, 512), make([]byte, 512)})

	svc := &APIService{
		db:            db,
		reconstructor: reconstructor,
		kVal:          2,
	}

	// 1. Query by hash with force_cda=true
	req, _ := http.NewRequest(http.MethodGet, "/tx?hash="+testHash+"&force_cda=true", nil)
	rr := httptest.NewRecorder()
	svc.handleGetTx(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp TxResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Source != "cda" {
		t.Errorf("expected source cda, got %s", resp.Source)
	}
	if resp.Height != 5 {
		t.Errorf("expected height 5, got %d", resp.Height)
	}
	if resp.Payload != testPayload {
		t.Errorf("expected payload %s, got %s", testPayload, resp.Payload)
	}
	if resp.CommitsRoot != expectedCommitsRoot {
		t.Errorf("expected commits_root %s, got %s", expectedCommitsRoot, resp.CommitsRoot)
	}

	// 2. Query by key with force_cda=true
	reqKey, _ := http.NewRequest(http.MethodGet, "/tx?key="+testKey+"&force_cda=true", nil)
	rrKey := httptest.NewRecorder()
	svc.handleGetTx(rrKey, reqKey)

	if rrKey.Code != http.StatusOK {
		t.Fatalf("expected status 200 for key query, got %d: %s", rrKey.Code, rrKey.Body.String())
	}
	var respKey TxResponse
	if err := json.Unmarshal(rrKey.Body.Bytes(), &respKey); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if respKey.Payload != testPayload {
		t.Errorf("expected payload %s, got %s", testPayload, respKey.Payload)
	}
}

func TestPublisher_ByzantineBlockRejection(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	svc := &APIService{
		db:             db,
		kVal:           2,
		rejectedBlocks: make(map[string]string),
	}

	blockID := "block-99"
	height := int64(99)
	reason := "BFT Header commits_root mismatch: expected abc got def"

	// 1. Mark block as rejected byzantine block
	svc.markBlockRejected(blockID, height, reason)

	// 2. Query block data via handleGetBlockData
	req, _ := http.NewRequest(http.MethodGet, "/block/"+blockID, nil)
	rr := httptest.NewRecorder()
	svc.handleGetBlockData(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200 with rejection JSON, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response JSON: %v", err)
	}

	if resp["status"] != "STATUS_INVALID_REJECTED" {
		t.Errorf("expected status STATUS_INVALID_REJECTED, got %v", resp["status"])
	}
	if resp["error"] != reason {
		t.Errorf("expected error %s, got %v", reason, resp["error"])
	}
	if resp["block_id"] != blockID {
		t.Errorf("expected block_id %s, got %v", blockID, resp["block_id"])
	}

	// 3. Verify handleStatus reflects the rejection
	statusReq, _ := http.NewRequest(http.MethodGet, "/status", nil)
	statusRR := httptest.NewRecorder()
	svc.handleStatus(statusRR, statusReq)

	var statusResp map[string]interface{}
	if err := json.Unmarshal(statusRR.Body.Bytes(), &statusResp); err != nil {
		t.Fatalf("failed to parse status JSON: %v", err)
	}
	if int(statusResp["rejected_count"].(float64)) != 1 {
		t.Errorf("expected rejected_count 1, got %v", statusResp["rejected_count"])
	}
	if int64(statusResp["latest_rejected_height"].(float64)) != height {
		t.Errorf("expected latest_rejected_height %d, got %v", height, statusResp["latest_rejected_height"])
	}
}

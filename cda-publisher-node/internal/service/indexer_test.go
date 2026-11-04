package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestParseCDAKey(t *testing.T) {
	tests := []struct {
		key       string
		wantNs    string
		wantBlob  string
		wantType  string
		wantSeq   string
	}{
		{
			key:      "cda/rollup-a/batch-10/meta",
			wantNs:   "rollup-a",
			wantBlob: "batch-10",
			wantType: "manifest",
			wantSeq:  "",
		},
		{
			key:      "cda/rollup-a/batch-10/s/000_0001",
			wantNs:   "rollup-a",
			wantBlob: "batch-10",
			wantType: "slice",
			wantSeq:  "000_0001",
		},
		{
			key:      "cda/my-app/file-1",
			wantNs:   "my-app",
			wantBlob: "file-1",
			wantType: "blob",
			wantSeq:  "",
		},
		{
			key:      "tx_legacy_uuid_1234",
			wantNs:   "default",
			wantBlob: "tx_legacy_uuid_1234",
			wantType: "tx",
			wantSeq:  "",
		},
	}

	for _, tt := range tests {
		ns, blob, kType, seq := ParseCDAKey(tt.key)
		if ns != tt.wantNs || blob != tt.wantBlob || kType != tt.wantType || seq != tt.wantSeq {
			t.Errorf("ParseCDAKey(%q) = (%q, %q, %q, %q); want (%q, %q, %q, %q)",
				tt.key, ns, blob, kType, seq, tt.wantNs, tt.wantBlob, tt.wantType, tt.wantSeq)
		}
	}
}

func TestBlockTOCAndNamespaceIndexing(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatalf("failed to open in-memory badger db: %v", err)
	}
	defer db.Close()

	svc := &APIService{db: db}

	blockID := "block-10"
	height := int64(10)
	ns := "test-ns"
	blobID := "blob-abc"

	manifest := OnChainManifest{
		Version:     1,
		Namespace:   ns,
		BlobID:      blobID,
		Filename:    "test.txt",
		TotalSize:   1000,
		SHA256:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		TotalChunks: 1,
		SliceSize:   500,
		TotalSlices: 2,
	}
	manifestBytes, _ := json.Marshal(manifest)
	manifestPayload := fmt.Sprintf("cda/%s/%s/meta=%s", ns, blobID, string(manifestBytes))

	slice0 := fmt.Sprintf("cda/%s/%s/s/000_0000=AABBCCDDEE", ns, blobID)
	slice1 := fmt.Sprintf("cda/%s/%s/s/000_0001=1122334455", ns, blobID)

	txs := []TxSummary{
		{
			Hash:   stringsToUpper("hash-meta"),
			Key:    fmt.Sprintf("cda/%s/%s/meta", ns, blobID),
			Offset: 0,
			Cells:  1,
			Size:   len(manifestPayload),
		},
		{
			Hash:   stringsToUpper("hash-s0"),
			Key:    fmt.Sprintf("cda/%s/%s/s/000_0000", ns, blobID),
			Offset: 1,
			Cells:  1,
			Size:   len(slice0),
		},
		{
			Hash:   stringsToUpper("hash-s1"),
			Key:    fmt.Sprintf("cda/%s/%s/s/000_0001", ns, blobID),
			Offset: 2,
			Cells:  1,
			Size:   len(slice1),
		},
	}

	decodedCells := [][]byte{
		[]byte(manifestPayload),
		[]byte(slice0),
		[]byte(slice1),
	}

	err = db.Update(func(txn *badger.Txn) error {
		svc.indexBlockTOCAndNamespaces(txn, blockID, height, txs, decodedCells)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to index block toc: %v", err)
	}

	// 1. Verify Block TOC retrieval
	reqTOC, _ := http.NewRequest(http.MethodGet, "/block/10/index", nil)
	rrTOC := httptest.NewRecorder()
	svc.handleGetBlockTOC(rrTOC, reqTOC, "10")

	if rrTOC.Code != http.StatusOK {
		t.Fatalf("expected status 200 for /block/10/index, got %d: %s", rrTOC.Code, rrTOC.Body.String())
	}

	var tocResp struct {
		Status string   `json:"status"`
		TOC    BlockTOC `json:"toc"`
	}
	if err := json.Unmarshal(rrTOC.Body.Bytes(), &tocResp); err != nil {
		t.Fatalf("failed to decode TOC response: %v", err)
	}
	if tocResp.TOC.TotalTxs != 3 {
		t.Errorf("expected 3 txs in TOC, got %d", tocResp.TOC.TotalTxs)
	}
	if len(tocResp.TOC.Namespaces) == 0 || tocResp.TOC.Namespaces[0] != ns {
		t.Errorf("expected namespace %s in TOC, got %v", ns, tocResp.TOC.Namespaces)
	}

	// 2. Verify Namespace Blobs list: /api/v1/namespaces/{ns}/blobs
	reqBlobs, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%s/blobs", ns), nil)
	rrBlobs := httptest.NewRecorder()
	svc.handleNamespaces(rrBlobs, reqBlobs)

	if rrBlobs.Code != http.StatusOK {
		t.Fatalf("expected status 200 for blobs list, got %d: %s", rrBlobs.Code, rrBlobs.Body.String())
	}
	var blobsResp struct {
		Status string         `json:"status"`
		Count  int            `json:"count"`
		Blobs  []BlobMetadata `json:"blobs"`
	}
	if err := json.Unmarshal(rrBlobs.Body.Bytes(), &blobsResp); err != nil {
		t.Fatalf("failed to decode blobs response: %v", err)
	}
	if blobsResp.Count != 1 || len(blobsResp.Blobs) != 1 {
		t.Fatalf("expected 1 blob, got count=%d, len=%d", blobsResp.Count, len(blobsResp.Blobs))
	}
	blobMeta := blobsResp.Blobs[0]
	if blobMeta.BlobID != blobID {
		t.Errorf("expected blobID %s, got %s", blobID, blobMeta.BlobID)
	}
	if blobMeta.Status != "READY" {
		t.Errorf("expected status READY (2/2 slices received), got %s", blobMeta.Status)
	}
	if blobMeta.TotalSize != 1000 {
		t.Errorf("expected total size 1000, got %d", blobMeta.TotalSize)
	}

	// 3. Verify Single Blob Metadata: /api/v1/namespaces/{ns}/blobs/{id}
	reqSingle, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%s/blobs/%s", ns, blobID), nil)
	rrSingle := httptest.NewRecorder()
	svc.handleNamespaces(rrSingle, reqSingle)

	if rrSingle.Code != http.StatusOK {
		t.Fatalf("expected status 200 for single blob query, got %d", rrSingle.Code)
	}
	var singleResp struct {
		Status string       `json:"status"`
		Blob   BlobMetadata `json:"blob"`
	}
	if err := json.Unmarshal(rrSingle.Body.Bytes(), &singleResp); err != nil {
		t.Fatalf("failed to decode single blob response: %v", err)
	}
	if singleResp.Blob.Filename != "test.txt" {
		t.Errorf("expected filename test.txt, got %s", singleResp.Blob.Filename)
	}
}

func stringsToUpper(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func TestBatchTxQuery(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatalf("failed to open in-memory badger db: %v", err)
	}
	defer db.Close()

	svc := &APIService{db: db}

	key1 := "cda/test/blob-1/s/000_0000"
	val1 := key1 + "=01020304"
	key2 := "cda/test/blob-1/s/000_0001"
	val2 := key2 + "=05060708"
	txHash := "A1B2C3D4E5F6"
	txVal := "my-tx-payload"

	_ = db.Update(func(txn *badger.Txn) error {
		_ = txn.Set([]byte("data_key_"+key1), []byte(val1))
		_ = txn.Set([]byte("data_key_"+key2), []byte(val2))
		_ = txn.Set([]byte("data_tx_"+txHash), []byte(txVal))
		return nil
	})

	reqBody := `{"keys":["` + key1 + `","` + key2 + `","non-existent-key"],"hashes":["` + txHash + `"]}`
	req, _ := http.NewRequest(http.MethodPost, "/tx/batch", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()
	svc.handleBatchGetTx(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp BatchTxResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal batch response: %v", err)
	}

	if resp.Count != 3 {
		t.Errorf("expected 3 found items, got %d", resp.Count)
	}
	if resp.Missing != 1 {
		t.Errorf("expected 1 missing item, got %d", resp.Missing)
	}
	if resp.Results[key1] != val1 {
		t.Errorf("expected %s, got %s", val1, resp.Results[key1])
	}
	if resp.Results[key2] != val2 {
		t.Errorf("expected %s, got %s", val2, resp.Results[key2])
	}
	if resp.Results[txHash] != txVal {
		t.Errorf("expected %s, got %s", txVal, resp.Results[txHash])
	}
}

func TestHandleBlobDownload_FastInMemory(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatalf("failed to open in-memory badger db: %v", err)
	}
	defer db.Close()

	svc := &APIService{db: db}

	ns := "media-ns"
	blobID := "video-clip"
	rawContent := []byte("Antigravity CDA Network Ultra Fast Direct Download Test Content 1234567890")
	h := sha256.Sum256(rawContent)
	expectedSHA256 := hex.EncodeToString(h[:])

	// Split rawContent into 2 slices
	slice1Bytes := rawContent[:40]
	slice2Bytes := rawContent[40:]
	sliceKey0 := fmt.Sprintf("cda/%s/%s/s/000_0000", ns, blobID)
	sliceKey1 := fmt.Sprintf("cda/%s/%s/s/000_0001", ns, blobID)

	meta := BlobMetadata{
		Namespace:   ns,
		BlobID:      blobID,
		Filename:    "clip.mp4",
		TotalSize:   int64(len(rawContent)),
		SHA256:      expectedSHA256,
		TotalChunks: 1,
		ChunkSize:   1000,
		SliceSize:   40,
		TotalSlices: 2,
		Status:      "READY",
	}

	// Store slices in BadgerDB
	_ = db.Update(func(txn *badger.Txn) error {
		_ = txn.Set([]byte("data_key_"+sliceKey0), []byte(sliceKey0+"="+hex.EncodeToString(slice1Bytes)))
		_ = txn.Set([]byte("data_key_"+sliceKey1), []byte(sliceKey1+"="+hex.EncodeToString(slice2Bytes)))
		metaBytes, _ := json.Marshal(meta)
		_ = txn.Set([]byte(fmt.Sprintf("ns_blob_%s_%s", ns, blobID)), metaBytes)
		return nil
	})

	// Perform download request
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%s/blobs/%s/download", ns, blobID), nil)
	rr := httptest.NewRecorder()
	svc.handleNamespaces(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	if rr.Header().Get("Content-Type") != "application/octet-stream" {
		t.Errorf("unexpected content-type: %s", rr.Header().Get("Content-Type"))
	}
	if rr.Header().Get("Content-Length") != fmt.Sprintf("%d", len(rawContent)) {
		t.Errorf("unexpected content-length: %s", rr.Header().Get("Content-Length"))
	}
	if !bytes.Equal(rr.Body.Bytes(), rawContent) {
		t.Fatalf("downloaded content mismatch: got %q, want %q", rr.Body.String(), string(rawContent))
	}
}

func TestHandleBlobDownload_NativeBlob(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open in-memory badger: %v", err)
	}
	defer db.Close()

	svc := &APIService{db: db}

	ns := "rollup-zk"
	blobID := "batch-4096"
	rawContent := []byte("Antigravity Native Single Blob Transaction - No micro-slices, zero fragmentation, 100% contiguous!")

	blobKey := fmt.Sprintf("cda/%s/%s/blob", ns, blobID)

	meta := BlobMetadata{
		Namespace:   ns,
		BlobID:      blobID,
		Filename:    "batch-4096.dat",
		TotalSize:   int64(len(rawContent)),
		TotalSlices: 1,
		Status:      "READY",
	}

	// Store single native blob in BadgerDB
	_ = db.Update(func(txn *badger.Txn) error {
		_ = txn.Set([]byte("data_key_"+blobKey), []byte(blobKey+"="+hex.EncodeToString(rawContent)))
		metaBytes, _ := json.Marshal(meta)
		_ = txn.Set([]byte(fmt.Sprintf("ns_blob_%s_%s", ns, blobID)), metaBytes)
		return nil
	})

	// Perform download request
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%s/blobs/%s/download", ns, blobID), nil)
	rr := httptest.NewRecorder()
	svc.handleNamespaces(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	if !bytes.Equal(rr.Body.Bytes(), rawContent) {
		t.Fatalf("downloaded content mismatch: got %q, want %q", rr.Body.String(), string(rawContent))
	}
}



package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// BlockTOCItem describes a single indexed entry in a block's Table of Contents.
type BlockTOCItem struct {
	Key        string `json:"key"`
	Namespace  string `json:"namespace"`
	BlobID     string `json:"blob_id"`
	Type       string `json:"type"` // "manifest", "slice", "tx"
	Seq        string `json:"seq,omitempty"`
	CellOffset int    `json:"cell_offset"`
	NumCells   int    `json:"num_cells"`
	Size       int    `json:"size"`
	TxHash     string `json:"tx_hash"`
}

// BlockTOC represents the full Table of Contents of a CDA block.
type BlockTOC struct {
	BlockID    string         `json:"block_id"`
	Height     int64          `json:"height"`
	Namespaces []string       `json:"namespaces"`
	TotalTxs   int            `json:"total_txs"`
	Txs        []BlockTOCItem `json:"txs"`
}

// BlobMetadata records the aggregated on-chain state and manifest of a user data blob.
type BlobMetadata struct {
	Namespace      string   `json:"namespace"`
	BlobID         string   `json:"blob_id"`
	Filename       string   `json:"filename,omitempty"`
	TotalSize      int64    `json:"total_size"`
	SHA256         string   `json:"sha256"`
	TotalChunks    int      `json:"total_chunks"`
	ChunkSize      int      `json:"chunk_size,omitempty"`
	SliceSize      int      `json:"slice_size"`
	TotalSlices    int      `json:"total_slices"`
	ReceivedSlices int      `json:"received_slices"`
	BlockHeights   []int64  `json:"block_heights"`
	Status         string   `json:"status"` // "UPLOADING", "READY"
	CreatedAt      int64    `json:"created_at"`
	UpdatedAt      int64    `json:"updated_at"`
}

// OnChainManifest represents the schema of the manifest payload stored in cda/<ns>/<blob_id>/meta.
type OnChainManifest struct {
	Version     int    `json:"version,omitempty"`
	Namespace   string `json:"namespace"`
	BlobID      string `json:"blob_id"`
	Filename    string `json:"filename,omitempty"`
	TotalSize   int64  `json:"total_size"`
	SHA256      string `json:"sha256"`
	TotalChunks int    `json:"total_chunks"`
	ChunkSize   int    `json:"chunk_size,omitempty"`
	SliceSize   int    `json:"slice_size"`
	TotalSlices int    `json:"total_slices"`
	CreatedAt   int64  `json:"created_at,omitempty"`
}

// ParseCDAKey parses standardized URI keys:
//   - "cda/<namespace>/<blob_id>/meta" -> namespace, blobID, "manifest", ""
//   - "cda/<namespace>/<blob_id>/s/<seq>" -> namespace, blobID, "slice", seq
//
// For legacy keys (e.g. "tx_...", "f_..."), defaults to namespace="default", blobID=key, keyType="tx".
func ParseCDAKey(key string) (namespace, blobID, keyType, seq string) {
	if strings.HasPrefix(key, "cda/") {
		parts := strings.Split(key, "/")
		if len(parts) >= 4 {
			namespace = parts[1]
			blobID = parts[2]
			switch parts[3] {
			case "meta", "manifest":
				keyType = "manifest"
				if len(parts) >= 5 {
					seq = parts[4]
				}
			case "blob":
				keyType = "blob"
				if len(parts) >= 5 {
					seq = parts[4]
				}
			case "part":
				keyType = "part"
				if len(parts) >= 5 {
					seq = parts[4]
				}
			case "s", "slice":
				keyType = "slice"
				if len(parts) >= 5 {
					seq = parts[4]
				}
			default:
				keyType = parts[3]
				if len(parts) >= 5 {
					seq = parts[4]
				}
			}
			return
		} else if len(parts) == 3 {
			namespace = parts[1]
			blobID = parts[2]
			keyType = "blob"
			return
		}
	}
	namespace = "default"
	blobID = key
	keyType = "tx"
	return
}

func getBadgerJSON(txn *badger.Txn, key string, out interface{}) bool {
	item, err := txn.Get([]byte(key))
	if err != nil {
		return false
	}
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, out)
	})
	return err == nil
}

func setBadgerJSON(txn *badger.Txn, key string, in interface{}) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return txn.Set([]byte(key), data)
}

func extractPayloadFromCells(cells [][]byte, offset, numCells, size int) []byte {
	if offset < 0 || offset >= len(cells) {
		return nil
	}
	var buf bytes.Buffer
	for i := 0; i < numCells && (offset+i) < len(cells); i++ {
		buf.Write(cells[offset+i])
	}
	all := buf.Bytes()
	if size > 0 && size <= len(all) {
		return all[:size]
	}
	return bytes.TrimRight(all, "\x00")
}

// indexBlockTOCAndNamespaces indexes the block transactions into the Block TOC
// and updates the namespace catalog in BadgerDB.
func (s *APIService) indexBlockTOCAndNamespaces(txn *badger.Txn, blockID string, height int64, txs []TxSummary, decodedCells [][]byte) {
	now := time.Now().Unix()
	toc := BlockTOC{
		BlockID:  blockID,
		Height:   height,
		TotalTxs: len(txs),
		Txs:      make([]BlockTOCItem, 0, len(txs)),
	}

	nsSet := make(map[string]bool)

	// Fetch global namespace list
	var allNamespaces []string
	getBadgerJSON(txn, "ns_all_namespaces", &allNamespaces)
	allNsMap := make(map[string]bool)
	for _, ns := range allNamespaces {
		allNsMap[ns] = true
	}

	for _, tx := range txs {
		ns, blobID, kType, seq := ParseCDAKey(tx.Key)
		nsSet[ns] = true

		if !allNsMap[ns] {
			allNsMap[ns] = true
			allNamespaces = append(allNamespaces, ns)
		}

		tocItem := BlockTOCItem{
			Key:        tx.Key,
			Namespace:  ns,
			BlobID:     blobID,
			Type:       kType,
			Seq:        seq,
			CellOffset: tx.Offset,
			NumCells:   tx.Cells,
			Size:       tx.Size,
			TxHash:     tx.Hash,
		}
		toc.Txs = append(toc.Txs, tocItem)

		// Namespace catalog tracking
		if ns != "default" && blobID != "" {
			// Update namespace blob list
			var blobList []string
			getBadgerJSON(txn, "ns_blobs_"+ns, &blobList)
			hasBlob := false
			for _, b := range blobList {
				if b == blobID {
					hasBlob = true
					break
				}
			}
			if !hasBlob {
				blobList = append(blobList, blobID)
				_ = setBadgerJSON(txn, "ns_blobs_"+ns, blobList)
			}

			// Load or init BlobMetadata
			var meta BlobMetadata
			metaKey := fmt.Sprintf("ns_blob_%s_%s", ns, blobID)
			found := getBadgerJSON(txn, metaKey, &meta)
			if !found {
				meta = BlobMetadata{
					Namespace: ns,
					BlobID:    blobID,
					Status:    "UPLOADING",
					CreatedAt: now,
				}
			}
			meta.UpdatedAt = now

			// Add height if not present
			hasHeight := false
			for _, h := range meta.BlockHeights {
				if h == height {
					hasHeight = true
					break
				}
			}
			if !hasHeight && height > 0 {
				meta.BlockHeights = append(meta.BlockHeights, height)
			}

			if kType == "manifest" {
				payload := extractPayloadFromCells(decodedCells, tx.Offset, tx.Cells, tx.Size)
				payloadStr := string(payload)
				if eqIdx := strings.IndexByte(payloadStr, '='); eqIdx >= 0 {
					valStr := payloadStr[eqIdx+1:]
					jsonBytes := []byte(valStr)
					if raw, err := hex.DecodeString(valStr); err == nil && len(raw) > 0 {
						jsonBytes = raw
					}
					var manifest OnChainManifest
					if err := json.Unmarshal(jsonBytes, &manifest); err == nil {
						meta.Filename = manifest.Filename
						meta.TotalSize = manifest.TotalSize
						meta.SHA256 = manifest.SHA256
						meta.TotalChunks = manifest.TotalChunks
						meta.ChunkSize = manifest.ChunkSize
						meta.SliceSize = manifest.SliceSize
						meta.TotalSlices = manifest.TotalSlices
						if manifest.CreatedAt > 0 {
							meta.CreatedAt = manifest.CreatedAt
						}
					}
				}
			} else if kType == "blob" {
				meta.ReceivedSlices = 1
				if meta.TotalSlices <= 0 {
					meta.TotalSlices = 1
				}
				meta.Status = "READY"

				payload := extractPayloadFromCells(decodedCells, tx.Offset, tx.Cells, tx.Size)
				if len(payload) > 0 {
					payloadStr := string(payload)
					var rawData []byte
					if eqIdx := strings.IndexByte(payloadStr, '='); eqIdx >= 0 {
						valStr := payloadStr[eqIdx+1:]
						if decoded, errDec := hex.DecodeString(valStr); errDec == nil && len(decoded) > 0 {
							rawData = decoded
						} else {
							rawData = []byte(valStr)
						}
					} else {
						if decoded, errDec := hex.DecodeString(payloadStr); errDec == nil && len(decoded) > 0 {
							rawData = decoded
						} else {
							rawData = payload
						}
					}
					meta.TotalSize = int64(len(rawData))
					sum := sha256.Sum256(rawData)
					meta.SHA256 = hex.EncodeToString(sum[:])
				} else if tx.Size > 0 {
					meta.TotalSize = int64(tx.Size)
				}
			} else if kType == "part" {
				meta.ReceivedSlices++
				totalExpected := meta.TotalChunks
				if totalExpected <= 0 {
					totalExpected = meta.TotalSlices
				}
				if totalExpected > 0 && meta.ReceivedSlices >= totalExpected {
					meta.Status = "READY"
				}
			} else if kType == "slice" {
				meta.ReceivedSlices++
			}

			if meta.TotalSlices > 0 && meta.ReceivedSlices >= meta.TotalSlices {
				meta.Status = "READY"
			}
			_ = setBadgerJSON(txn, metaKey, meta)
		}
	}

	for ns := range nsSet {
		toc.Namespaces = append(toc.Namespaces, ns)
	}

	_ = setBadgerJSON(txn, "block_toc_"+blockID, toc)
	if height > 0 {
		_ = txn.Set([]byte(fmt.Sprintf("block_toc_height_%d", height)), []byte(blockID))
	}
	_ = setBadgerJSON(txn, "ns_all_namespaces", allNamespaces)
}

// handleGetBlockTOC returns the Table of Contents of a specified block.
func (s *APIService) handleGetBlockTOC(w http.ResponseWriter, r *http.Request, target string) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var blockID string
	var height int64

	if hVal, err := strconv.ParseInt(target, 10, 64); err == nil {
		height = hVal
		blockID = fmt.Sprintf("block-%d", height)
	} else {
		blockID = target
		if strings.HasPrefix(blockID, "block-") {
			parts := strings.Split(blockID, "-")
			if len(parts) >= 2 {
				height, _ = strconv.ParseInt(parts[1], 10, 64)
			}
		}
	}

	if s.db == nil {
		http.Error(w, `{"status":"error","message":"database not initialized"}`, http.StatusInternalServerError)
		return
	}

	var toc BlockTOC
	var found bool

	_ = s.db.View(func(txn *badger.Txn) error {
		found = getBadgerJSON(txn, "block_toc_"+blockID, &toc)
		if !found && height > 0 {
			if item, err := txn.Get([]byte(fmt.Sprintf("block_toc_height_%d", height))); err == nil {
				_ = item.Value(func(val []byte) error {
					blockID = string(val)
					found = getBadgerJSON(txn, "block_toc_"+blockID, &toc)
					return nil
				})
			}
		}
		return nil
	})

	if !found {
		// Fallback: If not indexed, try to construct a minimal TOC
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "not_found",
			"message":  fmt.Sprintf("Table of contents not found for %s", target),
			"block_id": blockID,
			"height":   height,
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"toc":    toc,
	})
}

// handleNamespaces handles /api/v1/namespaces routing:
//   - GET /api/v1/namespaces                     -> List all namespaces
//   - GET /api/v1/namespaces/{ns}/blobs          -> List all blobs in namespace
//   - GET /api/v1/namespaces/{ns}/blobs/{id}     -> Get metadata for blob
//   - GET /api/v1/namespaces/{ns}/blobs/{id}/download -> Direct file download
func (s *APIService) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces")
	path = strings.Trim(path, "/")

	if path == "" {
		// List all namespaces
		var namespaces []string
		if s.db != nil {
			_ = s.db.View(func(txn *badger.Txn) error {
				getBadgerJSON(txn, "ns_all_namespaces", &namespaces)
				return nil
			})
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     "success",
			"namespaces": namespaces,
		})
		return
	}

	parts := strings.Split(path, "/")
	namespace := parts[0]

	if len(parts) == 1 || (len(parts) == 2 && parts[1] == "blobs") {
		// List blobs in namespace: /api/v1/namespaces/{ns}/blobs
		var blobIDs []string
		var blobs []BlobMetadata
		if s.db != nil {
			_ = s.db.View(func(txn *badger.Txn) error {
				getBadgerJSON(txn, "ns_blobs_"+namespace, &blobIDs)
				for _, bID := range blobIDs {
					var meta BlobMetadata
					if getBadgerJSON(txn, fmt.Sprintf("ns_blob_%s_%s", namespace, bID), &meta) {
						if meta.TotalSize <= 0 {
							keysToTry := []string{
								fmt.Sprintf("data_key_cda/%s/%s", namespace, bID),
								fmt.Sprintf("data_key_cda/%s/%s/blob", namespace, bID),
							}
							for _, k := range keysToTry {
								if item, err := txn.Get([]byte(k)); err == nil {
									_ = item.Value(func(val []byte) error {
										sVal := string(val)
										var rawBytes []byte
										if eqIdx := strings.IndexByte(sVal, '='); eqIdx >= 0 {
											valStr := sVal[eqIdx+1:]
											if dec, e := hex.DecodeString(valStr); e == nil && len(dec) > 0 {
												rawBytes = dec
											} else {
												rawBytes = []byte(valStr)
											}
										} else {
											if dec, e := hex.DecodeString(sVal); e == nil && len(dec) > 0 {
												rawBytes = dec
											} else {
												rawBytes = val
											}
										}
										if len(rawBytes) > 0 {
											meta.TotalSize = int64(len(rawBytes))
											sum := sha256.Sum256(rawBytes)
											meta.SHA256 = hex.EncodeToString(sum[:])
										}
										return nil
									})
									if meta.TotalSize > 0 {
										break
									}
								}
							}
						}
						blobs = append(blobs, meta)
					}
				}
				return nil
			})
		}

		// Fallback: search CometBFT for on-chain manifests if local index is empty
		if len(blobs) == 0 {
			endpoint := fmt.Sprintf("tx_search?query=\"app.key CONTAINS 'cda/%s/' AND app.key CONTAINS '/meta'\"&per_page=50", namespace)
			if body, status, err := s.queryCometBFT(endpoint); err == nil && status == http.StatusOK {
				var rpcResp struct {
					Result struct {
						Txs []struct {
							Tx string `json:"tx"`
						} `json:"txs"`
					} `json:"result"`
				}
				if err := json.Unmarshal(body, &rpcResp); err == nil {
					for _, t := range rpcResp.Result.Txs {
						if decoded, errDec := base64.StdEncoding.DecodeString(t.Tx); errDec == nil {
							sVal := string(decoded)
							if eqIdx := strings.IndexByte(sVal, '='); eqIdx >= 0 {
								rawHex := sVal[eqIdx+1:]
								var manifestBytes []byte
								if mBytes, errHex := hex.DecodeString(rawHex); errHex == nil {
									manifestBytes = mBytes
								} else {
									manifestBytes = []byte(rawHex)
								}
								var onChain OnChainManifest
								if errM := json.Unmarshal(manifestBytes, &onChain); errM == nil && onChain.Namespace == namespace {
									bMeta := BlobMetadata{
										Namespace:      onChain.Namespace,
										BlobID:         onChain.BlobID,
										Filename:       onChain.Filename,
										TotalSize:      onChain.TotalSize,
										SHA256:         onChain.SHA256,
										TotalChunks:    onChain.TotalChunks,
										ChunkSize:      onChain.ChunkSize,
										SliceSize:      onChain.SliceSize,
										TotalSlices:    onChain.TotalSlices,
										ReceivedSlices: onChain.TotalSlices,
										Status:         "READY",
										CreatedAt:      onChain.CreatedAt,
										UpdatedAt:      time.Now().Unix(),
									}
									blobs = append(blobs, bMeta)
									if s.db != nil {
										_ = s.db.Update(func(txn *badger.Txn) error {
											setBadgerJSON(txn, fmt.Sprintf("ns_blob_%s_%s", namespace, onChain.BlobID), bMeta)
											return nil
										})
									}
								}
							}
						}
					}
				}
			}
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "success",
			"namespace": namespace,
			"count":     len(blobs),
			"blobs":     blobs,
		})
		return
	}

	if len(parts) >= 3 && parts[1] == "blobs" {
		blobID := parts[2]
		isDownload := len(parts) >= 4 && parts[3] == "download"

		var meta BlobMetadata
		var found bool
		if s.db != nil {
			_ = s.db.View(func(txn *badger.Txn) error {
				found = getBadgerJSON(txn, fmt.Sprintf("ns_blob_%s_%s", namespace, blobID), &meta)
				return nil
			})
		}

		// Fallback: discover on-chain manifest from CometBFT if missing locally
		if !found {
			metaKey := fmt.Sprintf("cda/%s/%s/meta", namespace, blobID)
			endpoint := fmt.Sprintf("tx_search?query=\"app.key='%s'\"&prove=false", metaKey)
			if body, status, err := s.queryCometBFT(endpoint); err == nil && status == http.StatusOK {
				var rpcResp struct {
					Result struct {
						Txs []struct {
							Tx string `json:"tx"`
						} `json:"txs"`
					} `json:"result"`
				}
				if err := json.Unmarshal(body, &rpcResp); err == nil && len(rpcResp.Result.Txs) > 0 {
					if decoded, errDec := base64.StdEncoding.DecodeString(rpcResp.Result.Txs[0].Tx); errDec == nil {
						sVal := string(decoded)
						if eqIdx := strings.IndexByte(sVal, '='); eqIdx >= 0 {
							rawHex := sVal[eqIdx+1:]
							var manifestBytes []byte
							if mBytes, errHex := hex.DecodeString(rawHex); errHex == nil {
								manifestBytes = mBytes
							} else {
								manifestBytes = []byte(rawHex)
							}
							var onChain OnChainManifest
							if errM := json.Unmarshal(manifestBytes, &onChain); errM == nil {
								meta = BlobMetadata{
									Namespace:      onChain.Namespace,
									BlobID:         onChain.BlobID,
									Filename:       onChain.Filename,
									TotalSize:      onChain.TotalSize,
									SHA256:         onChain.SHA256,
									TotalChunks:    onChain.TotalChunks,
									ChunkSize:      onChain.ChunkSize,
									SliceSize:      onChain.SliceSize,
									TotalSlices:    onChain.TotalSlices,
									ReceivedSlices: onChain.TotalSlices,
									Status:         "READY",
									CreatedAt:      onChain.CreatedAt,
									UpdatedAt:      time.Now().Unix(),
								}
								found = true
								if s.db != nil {
									_ = s.db.Update(func(txn *badger.Txn) error {
										setBadgerJSON(txn, fmt.Sprintf("ns_blob_%s_%s", namespace, blobID), meta)
										var bIDs []string
										getBadgerJSON(txn, "ns_blobs_"+namespace, &bIDs)
										hasB := false
										for _, id := range bIDs {
											if id == blobID {
												hasB = true
												break
											}
										}
										if !hasB {
											bIDs = append(bIDs, blobID)
											setBadgerJSON(txn, "ns_blobs_"+namespace, bIDs)
										}
										return nil
									})
								}
							}
						}
					}
				}
			}
		}

		if !found {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":    "error",
				"message":   fmt.Sprintf("Blob '%s' not found in namespace '%s'", blobID, namespace),
				"namespace": namespace,
				"blob_id":   blobID,
			})
			return
		}

		if !isDownload {
			// Return blob metadata
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "success",
				"blob":   meta,
			})
			return
		}

		// Direct file download
		s.handleBlobDownload(w, r, meta)
		return
	}

	http.Error(w, `{"status":"error","message":"invalid namespace path"}`, http.StatusBadRequest)
}

// handleBlobDownload streams the reconstructed file data directly to the HTTP client.
func (s *APIService) handleBlobDownload(w http.ResponseWriter, r *http.Request, meta BlobMetadata) {
	// 0. Fast Path: Native Single Blob (cda/<ns>/<blobID> OR cda/<ns>/<blobID>/blob)
	keysToTry := []string{
		fmt.Sprintf("cda/%s/%s", meta.Namespace, meta.BlobID),
		fmt.Sprintf("cda/%s/%s/blob", meta.Namespace, meta.BlobID),
	}
	var nativeBlobBytes []byte
	if s.db != nil {
		_ = s.db.View(func(txn *badger.Txn) error {
			for _, bKey := range keysToTry {
				if item, err := txn.Get([]byte("data_key_" + bKey)); err == nil {
					_ = item.Value(func(val []byte) error {
						sVal := string(val)
						if eqIdx := strings.IndexByte(sVal, '='); eqIdx >= 0 {
							valStr := sVal[eqIdx+1:]
							if rawBytes, errDec := hex.DecodeString(valStr); errDec == nil && len(rawBytes) > 0 {
								nativeBlobBytes = rawBytes
							} else {
								nativeBlobBytes = []byte(valStr)
							}
						} else {
							if rawBytes, errDec := hex.DecodeString(sVal); errDec == nil && len(rawBytes) > 0 {
								nativeBlobBytes = rawBytes
							} else {
								nativeBlobBytes = val
							}
						}
						return nil
					})
					if len(nativeBlobBytes) > 0 {
						break
					}
				}
			}
			return nil
		})
	}
	if len(nativeBlobBytes) > 0 {
		w.Header().Set("Content-Type", "application/octet-stream")
		if meta.Filename != "" {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", meta.Filename))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(nativeBlobBytes)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(nativeBlobBytes)
		return
	}

	// 0.1 Fast Path: Native Multi-Part Blob (cda/<ns>/<blobID>/part/<pIdx>)
	if meta.TotalChunks > 0 {
		partsData := make([][]byte, meta.TotalChunks)
		allPartsFound := true
		if s.db != nil {
			_ = s.db.View(func(txn *badger.Txn) error {
				for pIdx := 0; pIdx < meta.TotalChunks; pIdx++ {
					item, err := txn.Get([]byte(fmt.Sprintf("data_key_cda/%s/%s/part/%03d", meta.Namespace, meta.BlobID, pIdx)))
					if err != nil {
						item, err = txn.Get([]byte(fmt.Sprintf("data_key_cda/%s/%s/part/%d", meta.Namespace, meta.BlobID, pIdx)))
					}
					if err != nil {
						allPartsFound = false
						return nil
					}
					_ = item.Value(func(val []byte) error {
						sVal := string(val)
						if eqIdx := strings.IndexByte(sVal, '='); eqIdx >= 0 {
							valStr := sVal[eqIdx+1:]
							if rawBytes, errDec := hex.DecodeString(valStr); errDec == nil && len(rawBytes) > 0 {
								partsData[pIdx] = rawBytes
							} else {
								partsData[pIdx] = []byte(valStr)
							}
						} else {
							if rawBytes, errDec := hex.DecodeString(sVal); errDec == nil && len(rawBytes) > 0 {
								partsData[pIdx] = rawBytes
							} else {
								partsData[pIdx] = val
							}
						}
						return nil
					})
				}
				return nil
			})
		}
		if allPartsFound && len(partsData) > 0 && len(partsData[0]) > 0 {
			w.Header().Set("Content-Type", "application/octet-stream")
			if meta.Filename != "" {
				w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", meta.Filename))
			}
			w.WriteHeader(http.StatusOK)
			for _, p := range partsData {
				_, _ = w.Write(p)
			}
			return
		}
	}

	if meta.TotalSlices <= 0 || meta.TotalSize <= 0 {
		http.Error(w, `{"status":"error","message":"blob has no recorded slices"}`, http.StatusBadRequest)
		return
	}

	chunkSize := meta.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 512000
	}
	sliceSize := meta.SliceSize
	if sliceSize <= 0 {
		sliceSize = 500
	}
	totalChunks := meta.TotalChunks
	if totalChunks <= 0 {
		totalChunks = 1
	}

	allSliceKeys := make([]string, 0, meta.TotalSlices)
	for cIdx := 0; cIdx < totalChunks; cIdx++ {
		offset := int64(cIdx) * int64(chunkSize)
		actualChunkSize := chunkSize
		if meta.TotalSize-offset < int64(actualChunkSize) {
			actualChunkSize = int(meta.TotalSize - offset)
		}
		if actualChunkSize <= 0 {
			break
		}
		numSlices := int((actualChunkSize + sliceSize - 1) / sliceSize)
		for sIdx := 0; sIdx < numSlices; sIdx++ {
			sliceKey := fmt.Sprintf("cda/%s/%s/s/%03d_%04d", meta.Namespace, meta.BlobID, cIdx, sIdx)
			allSliceKeys = append(allSliceKeys, sliceKey)
		}
	}

	sliceDataMap := make(map[string][]byte, len(allSliceKeys))

	// 1. Fast in-memory lookup via local BadgerDB data_key_<sliceKey>
	if s.db != nil {
		_ = s.db.View(func(txn *badger.Txn) error {
			for _, key := range allSliceKeys {
				if item, err := txn.Get([]byte("data_key_" + key)); err == nil {
					_ = item.Value(func(val []byte) error {
						sVal := string(val)
						if eqIdx := strings.IndexByte(sVal, '='); eqIdx >= 0 {
							hexStr := sVal[eqIdx+1:]
							if rawBytes, errDec := hex.DecodeString(hexStr); errDec == nil {
								sliceDataMap[key] = rawBytes
							}
						} else {
							if rawBytes, errDec := hex.DecodeString(sVal); errDec == nil {
								sliceDataMap[key] = rawBytes
							}
						}
						return nil
					})
				}
			}
			return nil
		})
	}

	// 2. Identify missing slices (if any) and fallback to CometBFT RPC in parallel
	var missingKeys []string
	for _, key := range allSliceKeys {
		if _, ok := sliceDataMap[key]; !ok {
			missingKeys = append(missingKeys, key)
		}
	}

	if len(missingKeys) > 0 {
		log.Printf("[Publisher] handleBlobDownload: %d / %d slices missing in BadgerDB, querying fallback...", len(missingKeys), len(allSliceKeys))
		var mu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, 32)

		for _, mk := range missingKeys {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				endpoint := fmt.Sprintf("tx_search?query=\"app.key='%s'\"&prove=false", key)
				if body, status, err := s.queryCometBFT(endpoint); err == nil && status == http.StatusOK {
					var rpcResp struct {
						Result struct {
							Txs []struct {
								Tx string `json:"tx"`
							} `json:"txs"`
						} `json:"result"`
					}
					if err := json.Unmarshal(body, &rpcResp); err == nil && len(rpcResp.Result.Txs) > 0 {
						if decoded, errDec := base64.StdEncoding.DecodeString(rpcResp.Result.Txs[0].Tx); errDec == nil {
							sVal := string(decoded)
							if eqIdx := strings.IndexByte(sVal, '='); eqIdx >= 0 {
								hexStr := sVal[eqIdx+1:]
								if rawBytes, errDecHex := hex.DecodeString(hexStr); errDecHex == nil {
									mu.Lock()
									sliceDataMap[key] = rawBytes
									mu.Unlock()
								}
							}
						}
					}
				}
			}(mk)
		}
		wg.Wait()

		// Cache fetched fallback slices into local BadgerDB for future instant memory retrieval
		if s.db != nil {
			_ = s.db.Update(func(txn *badger.Txn) error {
				for _, mk := range missingKeys {
					if b, ok := sliceDataMap[mk]; ok {
						_ = txn.Set([]byte("data_key_"+mk), []byte(mk+"="+hex.EncodeToString(b)))
					}
				}
				return nil
			})
		}
	}

	// 3. Assemble binary stream
	assembled := make([]byte, 0, meta.TotalSize)
	for _, key := range allSliceKeys {
		raw, ok := sliceDataMap[key]
		if !ok {
			log.Printf("[Publisher] Download error: slice %s still missing after lookup", key)
			http.Error(w, fmt.Sprintf(`{"status":"error","message":"failed to retrieve slice %s"}`, key), http.StatusInternalServerError)
			return
		}
		assembled = append(assembled, raw...)
	}

	if int64(len(assembled)) > meta.TotalSize {
		assembled = assembled[:meta.TotalSize]
	}

	// 4. Verify SHA-256
	h := sha256.Sum256(assembled)
	calcSHA256 := hex.EncodeToString(h[:])
	if meta.SHA256 != "" && !strings.EqualFold(calcSHA256, meta.SHA256) {
		http.Error(w, fmt.Sprintf(`{"status":"error","message":"integrity verification failed: SHA256 mismatch (got %s, expected %s)"}`, calcSHA256, meta.SHA256), http.StatusInternalServerError)
		return
	}

	filename := meta.Filename
	if filename == "" {
		filename = fmt.Sprintf("%s.bin", meta.BlobID)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.Header().Set("Content-Length", strconv.Itoa(len(assembled)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(assembled)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

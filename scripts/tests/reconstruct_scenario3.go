package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	rsmt2d "github.com/celestiaorg/rsmt2d"
	"github.com/klauspost/reedsolomon"
)

type PublishPayload struct {
	BlockID string   `json:"block_id"`
	Data    []string `json:"data"`
}

type SampleResult struct {
	Row      int    `json:"row"`
	Col      int    `json:"col"`
	Verified bool   `json:"verified"`
	CellData string `json:"cell_data"`
}

type DASResponse struct {
	BlockID string         `json:"block_id"`
	Success bool           `json:"success"`
	Results []SampleResult `json:"results"`
}

func main() {
	publisherURL := flag.String("publisher", "http://localhost:8080", "Publisher URL")
	lightURL := flag.String("light", "http://localhost:9401", "Light Node URL")
	kVal := flag.Int("k", 16, "ODS dimension parameter K (matrix size K x K)")
	lostColsCount := flag.Int("lost-cols", 4, "Number of columns to simulate complete loss (e.g., 4 columns for 1 network column)")
	flag.Parse()

	k := *kVal
	n := 2 * k
	cellSize := 64
	blockID := fmt.Sprintf("scenario3-recon-%d", time.Now().Unix())

	fmt.Println("================================================================================")
	fmt.Println("       KỊCH BẢN 3: KIỂM THỬ PHỤC HỒI DỮ LIỆU KHỐI PHÂN TÁN (DISTRIBUTED RECONSTRUCTION)")
	fmt.Println("================================================================================")
	fmt.Printf("[*] Matrix Parameters: K = %d (ODS: %dx%d = %d cells, EDS: %dx%d = %d cells)\n", k, k, k, k*k, n, n, n*n)
	fmt.Printf("[*] Cell Size: %d bytes (Total ODS Payload: %d bytes)\n", cellSize, k*k*cellSize)
	fmt.Printf("[*] Target Block ID: %s\n", blockID)

	// 1. Generate Deterministic ODS Data
	fmt.Println("\n[Phase 1] Khởi tạo dữ liệu gốc ODS (Original Data Square)...")
	originalODSShares := make([][]byte, k*k)
	hexData := make([]string, k*k)
	odsHasher := sha256.New()

	for i := 0; i < k*k; i++ {
		share := make([]byte, cellSize)
		cellHash := sha256.Sum256([]byte(fmt.Sprintf("cda-scenario3-cell-%d-%s", i, blockID)))
		copy(share[:32], cellHash[:])
		copy(share[32:], cellHash[:])
		originalODSShares[i] = share
		hexData[i] = hex.EncodeToString(share)
		odsHasher.Write(share)
	}
	originalODSHash := odsHasher.Sum(nil)
	fmt.Printf("[+] Original ODS SHA-256 Hash: %x\n", originalODSHash)

	// 2. Compute 2D Reed-Solomon Extended Data Square (EDS) baseline
	fmt.Println("\n[Phase 2] Mã hóa 2D Reed-Solomon (EDS Generation using Leopard Codec)...")
	startEncode := time.Now()
	eds, err := rsmt2d.ComputeExtendedDataSquare(originalODSShares, rsmt2d.Leopard, rsmt2d.NewDefaultTree)
	if err != nil {
		log.Fatalf("[-] Failed to compute EDS: %v", err)
	}
	encodeDuration := time.Since(startEncode)
	fmt.Printf("[+] 2D RS-Leopard Extended Data Square computed successfully in %v\n", encodeDuration)

	// 3. Publish block to Publisher Node
	if *publisherURL != "" {
		fmt.Printf("\n[Phase 3] Xuất bản khối lên Publisher Node (%s/publish)...\n", *publisherURL)
		payload := PublishPayload{
			BlockID: blockID,
			Data:    hexData,
		}
		payloadBytes, _ := json.Marshal(payload)
		resp, err := http.Post(fmt.Sprintf("%s/publish", *publisherURL), "application/json", bytes.NewReader(payloadBytes))
		if err != nil {
			fmt.Printf("[!] Warning: Could not publish to Publisher (%v). Proceeding with live EDS reconstruction matrix.\n", err)
		} else {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Printf("[+] Successfully published block %s to network: %s\n", blockID, string(respBody))
			} else {
				fmt.Printf("[!] Publisher returned status %d: %s\n", resp.StatusCode, string(respBody))
			}
		}
	}

	// 4. Simulate Complete Network Column Loss
	fmt.Printf("\n[Phase 4] Giả lập sự cố mất mát hoàn toàn %d cột dữ liệu (Network Column Outage)...\n", *lostColsCount)
	lostCols := make(map[int]bool)
	for c := 0; c < *lostColsCount && c < n; c++ {
		lostCols[c] = true
	}
	fmt.Printf("[!] Các cột bị mất hoàn toàn (Offline/Unavailable): ")
	for c := range lostCols {
		fmt.Printf("Col %d ", c)
	}
	fmt.Println()

	// Build Corrupted EDS Matrix where lost columns are completely nil
	corruptedEDS := make([][][]byte, n) // [row][col]
	totalLostCells := 0
	for r := 0; r < n; r++ {
		corruptedEDS[r] = make([][]byte, n)
		for c := 0; c < n; c++ {
			if lostCols[c] {
				corruptedEDS[r][c] = nil // Completely missing
				totalLostCells++
			} else {
				cell := eds.GetCell(uint(r), uint(c))
				cellCopy := make([]byte, len(cell))
				copy(cellCopy, cell)
				corruptedEDS[r][c] = cellCopy
			}
		}
	}
	fmt.Printf("[+] Tổng số ô bị mất trong ma trận: %d / %d ô (%.1f%%)\n", totalLostCells, n*n, float64(totalLostCells)/float64(n*n)*100)

	// 5. Perform Distributed Horizontal Reed-Solomon Reconstruction
	fmt.Println("\n[Phase 5] Thực hiện giải mã Reed-Solomon ngược theo chiều ngang (Horizontal RS Row Reconstruction)...")
	rsEncoder, err := reedsolomon.New(k, k, reedsolomon.WithLeopardGF(true))
	if err != nil {
		log.Fatalf("[-] Failed to initialize Leopard RS Decoder: %v", err)
	}

	reconstructedEDS := make([][][]byte, n)
	startRecon := time.Now()

	for r := 0; r < n; r++ {
		rowShares := make([][]byte, n)
		for c := 0; c < n; c++ {
			if corruptedEDS[r][c] != nil {
				shareCopy := make([]byte, cellSize)
				copy(shareCopy, corruptedEDS[r][c])
				rowShares[c] = shareCopy
			} else {
				rowShares[c] = nil
			}
		}

		// Reconstruct missing shares in row r
		if err := rsEncoder.Reconstruct(rowShares); err != nil {
			log.Fatalf("[-] Failed to reconstruct Row %d: %v", r, err)
		}
		reconstructedEDS[r] = rowShares
	}
	reconDuration := time.Since(startRecon)
	fmt.Printf("[+] Toàn bộ %d hàng (%d ô bị mất) đã được phục hồi thành công trong %v!\n", n, totalLostCells, reconDuration)

	// 6. Extract Recovered ODS and Verify Integrity
	fmt.Println("\n[Phase 6] Trích xuất khối ODS và kiểm tra tính toàn vẹn byte-for-byte...")
	reconstructedODSShares := make([][]byte, k*k)
	reconHasher := sha256.New()
	byteErrors := 0

	for r := 0; r < k; r++ {
		for c := 0; c < k; c++ {
			idx := r*k + c
			reconstructedCell := reconstructedEDS[r][c]
			reconstructedODSShares[idx] = reconstructedCell
			reconHasher.Write(reconstructedCell)

			if !bytes.Equal(reconstructedCell, originalODSShares[idx]) {
				byteErrors++
				fmt.Printf("[-] Mismatch at ODS Cell [%d, %d] (Index %d)\n", r, c, idx)
			}
		}
	}
	reconstructedODSHash := reconHasher.Sum(nil)

	fmt.Printf("[*] Reconstructed ODS SHA-256 Hash: %x\n", reconstructedODSHash)
	fmt.Printf("[*] Original ODS SHA-256 Hash:      %x\n", originalODSHash)

	fmt.Println("\n================================================================================")
	fmt.Println("                            KẾT QUẢ KIỂM THỬ KỊCH BẢN 3")
	fmt.Println("================================================================================")
	if byteErrors == 0 && bytes.Equal(reconstructedODSHash, originalODSHash) {
		fmt.Println("[+] TRẠNG THÁI: THÀNH CÔNG 100% (PASSED)")
		fmt.Printf("[+] Tổng số ô phục hồi: %d ô\n", totalLostCells)
		fmt.Printf("[+] Độ chính xác dữ liệu: 100.0%% (0 byte sai lệch)\n")
		fmt.Printf("[+] Thời gian giải mã tái tạo RS: %v (Trung bình %.3f ms/hàng)\n", reconDuration, float64(reconDuration.Microseconds())/float64(n)/1000.0)
		fmt.Println("================================================================================")
	} else {
		fmt.Println("[-] TRẠNG THÁI: THẤT BẠI (FAILED)")
		fmt.Printf("[-] Phát hiện %d ô dữ liệu ODS bị sai lệch!\n", byteErrors)
		log.Fatalf("Reconstruction verification failed!")
	}

	// 7. Verify live Light Node integration if requested
	if *lightURL != "" {
		fmt.Printf("\n[Phase 7] Kiểm tra tương thích truy vấn Light Node (%s)...\n", *lightURL)
		healthResp, err := http.Get(fmt.Sprintf("%s/das/sample/block-1", *lightURL))
		if err == nil {
			fmt.Printf("[+] Light Node API is online and responding (HTTP %d)\n", healthResp.StatusCode)
			healthResp.Body.Close()
		}
	}
}

package service

import (
	"encoding/hex"
	"testing"

	"cda-publisher-node/internal/engine"
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

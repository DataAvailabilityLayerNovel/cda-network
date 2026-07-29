package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"

	rsmt2d "github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	bls12381kzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
)

type Pipeline struct {
	k   int
	kzg *cda.GnarkKZG
}

func NewPipeline(k int) (*Pipeline, error) {
	// Initialize default SRS size. In production, this is loaded from files.
	// For simulation, we create a fresh SRS.
	// Assume max EDS width is 256, so srsSize is 256 * 4 = 1024.
	srsSize := uint64(1024)
	srs, err := bls12381kzg.NewSRS(srsSize, big.NewInt(-1))
	if err != nil {
		return nil, fmt.Errorf("failed to create SRS: %w", err)
	}

	return &Pipeline{
		k:   k,
		kzg: cda.NewGnarkKZG(*srs),
	}, nil
}

func (p *Pipeline) ProcessODS(odsData [][]byte, blockID string) (*BlockHeader, *cda.PublishData, []cda.MerkleProof, *rsmt2d.ExtendedDataSquare, error) {
	// 1. Extend ODS using Reed-Solomon (Leopard Codec)
	eds, err := cda.ComputeExtendedDataSquareWithLeopard(odsData)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("extend ODS to EDS: %w", err)
	}

	// 2. Compute piece commitments and combined column commitments using Fiat-Shamir
	codec := rlnc.NewRLNCCodec(p.k)
	pubData, err := cda.ComputeAndSetKateCommitments(codec, &eds, p.kzg, 0)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("compute Kate commitments: %w", err)
	}

	// 3. Build Merkle tree of N * k piece commitments
	pieceCommitsBytes := make([][]byte, len(pubData.PieceComm))
	for i := range pubData.PieceComm {
		pieceCommitsBytes[i] = append([]byte(nil), pubData.PieceComm[i]...)
	}
	root, proofs := cda.BuildMerkleTree(pieceCommitsBytes)

	// 4. Generate Block ID if not provided
	if blockID == "" {
		h := sha256.New()
		h.Write(root)
		blockID = hex.EncodeToString(h.Sum(nil))
	}

	// 5. Create BlockHeader
	header := &BlockHeader{
		BlockID:     blockID,
		CommitsRoot: hex.EncodeToString(root),
		ColumnComm:  make([][]byte, len(pubData.ColumnComm)),
		Coeffs:      nil,
	}

	for i := range pubData.ColumnComm {
		header.ColumnComm[i] = append([]byte(nil), pubData.ColumnComm[i]...)
	}

	if len(pubData.Coeffs) > 0 {
		header.Coeffs = append([]byte(nil), pubData.Coeffs[0]...)
	}

	return header, pubData, proofs, &eds, nil
}

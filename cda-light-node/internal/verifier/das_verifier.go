package verifier

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/rlnc"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	gnarkkzg "github.com/consensys/gnark-crypto/ecc/bls12-381/kzg"
)

type DASVerifier struct {
	kzg   cda.KZGProvider
	codec *rlnc.RLNCCodec
	k     int
}

func NewDASVerifier(k int, kzg cda.KZGProvider) *DASVerifier {
	return &DASVerifier{
		kzg:   kzg,
		codec: rlnc.NewRLNCCodec(k),
		k:     k,
	}
}

func (v *DASVerifier) K() int {
	return v.k
}

// IsLinearlyIndependent checks if the new coefficient vector is linearly independent of existing ones.
func IsLinearlyIndependent(existingCoeffs [][]byte, newCoeff []byte, k int) bool {
	m := len(existingCoeffs)
	if m >= k {
		return false
	}

	A := make([][]byte, k)
	for i := 0; i < m; i++ {
		A[i] = append([]byte(nil), existingCoeffs[i]...)
	}
	A[m] = append([]byte(nil), newCoeff...)

	for i := m + 1; i < k; i++ {
		row := make([]byte, k)
		row[i] = 1
		A[i] = row
	}

	B := make([][]byte, k)
	for i := 0; i < k; i++ {
		B[i] = make([]byte, 32)
	}

	_, err := rlnc.SolveGaussian(A, B)
	return err == nil
}

// InvertMatrixFr computes the inverse of a k x k matrix in Fr by solving k systems using rlnc.SolveGaussian.
func InvertMatrixFr(matrix [][]byte, k int) ([][]fr.Element, error) {
	inv := make([][]fr.Element, k)
	for i := 0; i < k; i++ {
		inv[i] = make([]fr.Element, k)
	}

	for j := 0; j < k; j++ {
		A_copy := make([][]byte, k)
		for i := 0; i < k; i++ {
			A_copy[i] = append([]byte(nil), matrix[i]...)
		}

		B := make([][]byte, k)
		for i := 0; i < k; i++ {
			B[i] = make([]byte, 32)
			if i == j {
				var one fr.Element
				one.SetOne()
				val := one.Bytes()
				copy(B[i], val[:])
			}
		}

		res, err := rlnc.SolveGaussian(A_copy, B)
		if err != nil {
			return nil, fmt.Errorf("SolveGaussian failed to invert column %d: %w", j, err)
		}

		for i := 0; i < k; i++ {
			inv[i][j].SetBytes(res[i])
		}
	}

	return inv, nil
}

// CombineProofsFr performs a homomorphic linear combination of G1 proofs using general fr.Element coefficients.
func CombineProofsFr(proofs [][]byte, coeffs []fr.Element) ([]byte, error) {
	if len(proofs) == 0 {
		return nil, fmt.Errorf("proofs cannot be empty")
	}
	if len(proofs) != len(coeffs) {
		return nil, fmt.Errorf("coeffs length does not match proofs length")
	}

	var combinedH bls12381.G1Affine
	var combinedValue fr.Element

	for i, proofBytes := range proofs {
		var tempProof gnarkkzg.OpeningProof
		if _, err := tempProof.ReadFrom(bytes.NewReader(proofBytes)); err != nil {
			return nil, fmt.Errorf("failed to read proof %d: %w", i, err)
		}

		var scaledH bls12381.G1Affine
		scaledH.ScalarMultiplication(&tempProof.H, coeffs[i].BigInt(new(big.Int)))
		combinedH.Add(&combinedH, &scaledH)

		var scaledValue fr.Element
		scaledValue.Mul(&tempProof.ClaimedValue, &coeffs[i])
		combinedValue.Add(&combinedValue, &scaledValue)
	}

	combinedProof := gnarkkzg.OpeningProof{
		H:            combinedH,
		ClaimedValue: combinedValue,
	}

	var out bytes.Buffer
	if _, err := combinedProof.WriteTo(&out); err != nil {
		return nil, fmt.Errorf("failed to marshal combined proof: %w", err)
	}

	return out.Bytes(), nil
}

// VectorMulAddFr performs modular multiplication and addition in Fr: dst = dst + coeff * src
func VectorMulAddFr(dst, src []byte, coeff fr.Element) {
	var dstEl fr.Element
	var srcEl fr.Element
	var term fr.Element

	dstEl.SetBytes(dst)
	srcEl.SetBytes(src)

	term.Mul(&srcEl, &coeff)
	dstEl.Add(&dstEl, &term)

	out := dstEl.Bytes()
	copy(dst, out[:])
}

// VerifyReconstructedCell performs the direct algebraic combined verification of the cell pieces against the Block Header.
func (v *DASVerifier) VerifyReconstructedCell(
	pieces []cda.ReceivedPiece,
	columnCommitment []byte,
	globalCoeffs []byte, // k * 32 bytes from Block Header
	row int,
) (string, bool, error) {
	if len(pieces) < v.k {
		return "", false, fmt.Errorf("not enough pieces to reconstruct: got %d, expected %d", len(pieces), v.k)
	}

	// 1. Recover fragments
	rlncPieces := make([]rlnc.PieceData, v.k)
	for i := 0; i < v.k; i++ {
		rlncPieces[i] = pieces[i].Data
	}

	recoveredFragments, err := v.codec.Decode(rlncPieces)
	if err != nil {
		return "", false, fmt.Errorf("failed to decode fragments using Gaussian elimination: %w", err)
	}

	// 2. Invert coefficient matrix A
	A_recode := make([][]byte, v.k)
	recodedProofs := make([][]byte, v.k)
	for i := 0; i < v.k; i++ {
		A_recode[i] = pieces[i].Data.Coeffs
		recodedProofs[i] = pieces[i].Proof
	}

	invA, err := InvertMatrixFr(A_recode, v.k)
	if err != nil {
		return "", false, fmt.Errorf("failed to invert coefficient matrix: %w", err)
	}

	// 3. Reconstruct proofs for original fragments
	reconstructedProofs := make([][]byte, v.k)
	for j := 0; j < v.k; j++ {
		reconProof, err := CombineProofsFr(recodedProofs, invA[j])
		if err != nil {
			return "", false, fmt.Errorf("failed to reconstruct proof for fragment %d: %w", j, err)
		}
		reconstructedProofs[j] = reconProof
	}

	// 4. Parse global challenge coeffs x from Block Header
	// globalCoeffs is k * 32 bytes
	if len(globalCoeffs) != v.k*32 {
		return "", false, fmt.Errorf("invalid global challenge vector size: got %d, expected %d", len(globalCoeffs), v.k*32)
	}

	xFr := make([]fr.Element, v.k)
	for j := 0; j < v.k; j++ {
		xFr[j].SetBytes(globalCoeffs[j*32 : (j+1)*32])
	}

	// 5. Combine recovered fragments using global challenge x
	combinedData := make([]byte, 32)
	for j := 0; j < v.k; j++ {
		VectorMulAddFr(combinedData, recoveredFragments[j], xFr[j])
	}

	// 6. Combine reconstructed proofs using global challenge x
	finalCombinedProof, err := CombineProofsFr(reconstructedProofs, xFr)
	if err != nil {
		return "", false, fmt.Errorf("failed to combine final proof: %w", err)
	}

	// 7. Verify combined data and combined proof against the Column Commitment from Block Header
	verified := v.kzg.Verify(columnCommitment, row, combinedData, finalCombinedProof)
	if !verified {
		return "", false, nil
	}

	// 8. Reconstruct cell string (trim zero-padding from fragments)
	pieceSize := 64 / v.k
	var buf bytes.Buffer
	for _, frag := range recoveredFragments {
		if len(frag) >= pieceSize {
			buf.Write(frag[len(frag)-pieceSize:])
		} else {
			buf.Write(frag)
		}
	}

	return hex.EncodeToString(buf.Bytes()), true, nil
}

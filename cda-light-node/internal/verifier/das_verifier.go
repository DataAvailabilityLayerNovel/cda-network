package verifier

import (
	"bytes"
	"encoding/binary"
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

// VerifySinglePiece verifies an individual coded piece against its column PieceCommits in O(1) pairing time.
func (v *DASVerifier) VerifySinglePiece(piece cda.ReceivedPiece, pieceCommitsHex []string, domainSize int) bool {
	if len(pieceCommitsHex) == 0 || len(piece.Data.Data) == 0 || len(piece.Proof) == 0 {
		return false
	}
	if len(piece.Proof) == 80 && !cda.VerifyClaimedValue(piece.Data.Data, piece.Proof) {
		return false
	}
	commits := make([]cda.PieceCommitment, len(pieceCommitsHex))
	for i, cHex := range pieceCommitsHex {
		b, err := hex.DecodeString(cHex)
		if err != nil {
			return false
		}
		commits[i] = cda.PieceCommitment(b)
	}
	combinedCommit, err := v.kzg.Combine(commits, piece.Data.Coeffs)
	if err != nil {
		return false
	}
	return v.kzg.VerifyForDomain(combinedCommit, piece.Row, piece.Data.Data, piece.Proof, domainSize)
}

func parseCoeffRowFr(coeff []byte, k int) ([]fr.Element, bool) {
	if len(coeff) != k && len(coeff) != 2*k {
		return nil, false
	}
	v := make([]fr.Element, k)
	isTwoByte := len(coeff) == 2*k
	for j := 0; j < k; j++ {
		if isTwoByte {
			cVal := binary.BigEndian.Uint16(coeff[j*2 : (j+1)*2])
			v[j].SetUint64(uint64(cVal))
		} else {
			v[j].SetUint64(uint64(coeff[j]))
		}
	}
	return v, true
}

// IsLinearlyIndependent checks if the new coefficient vector is linearly independent of existing ones over Fr.
func IsLinearlyIndependent(existingCoeffs [][]byte, newCoeff []byte, k int) bool {
	if len(existingCoeffs) >= k {
		return false
	}

	basis := make([][]fr.Element, k)
	addRow := func(coeff []byte) bool {
		v, ok := parseCoeffRowFr(coeff, k)
		if !ok {
			return false
		}
		var term fr.Element
		for col := 0; col < k; col++ {
			if v[col].IsZero() {
				continue
			}
			if basis[col] != nil {
				factor := v[col]
				bRow := basis[col]
				v[col].SetZero()
				for j := col + 1; j < k; j++ {
					if !bRow[j].IsZero() {
						term.Mul(&bRow[j], &factor)
						v[j].Sub(&v[j], &term)
					}
				}
			} else {
				var inv fr.Element
				inv.Inverse(&v[col])
				v[col].SetOne()
				for j := col + 1; j < k; j++ {
					if !v[j].IsZero() {
						v[j].Mul(&v[j], &inv)
					}
				}
				basis[col] = v
				return true
			}
		}
		return false
	}

	for _, c := range existingCoeffs {
		addRow(c)
	}
	return addRow(newCoeff)
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
// Tối ưu hóa bằng Jacobian Accumulator (G1Jac) và AddMixed, loại bỏ hoàn toàn các phép nghịch đảo modulo trong vòng lặp.
func CombineProofsFr(proofs [][]byte, coeffs []fr.Element) ([]byte, error) {
	n := len(proofs)
	if n == 0 {
		return nil, fmt.Errorf("proofs cannot be empty")
	}
	if n != len(coeffs) {
		return nil, fmt.Errorf("coeffs length does not match proofs length")
	}

	var accJac bls12381.G1Jac
	var combinedValue fr.Element
	bInt := new(big.Int)

	for i, proofBytes := range proofs {
		var tempProof gnarkkzg.OpeningProof
		if _, err := tempProof.ReadFrom(bytes.NewReader(proofBytes)); err != nil {
			return nil, fmt.Errorf("failed to read proof %d: %w", i, err)
		}

		if coeffs[i].IsZero() {
			continue
		}

		var scaledValue fr.Element
		scaledValue.Mul(&tempProof.ClaimedValue, &coeffs[i])
		combinedValue.Add(&combinedValue, &scaledValue)

		if coeffs[i].IsOne() {
			accJac.AddMixed(&tempProof.H)
			continue
		}

		var ptJac, scaledJac bls12381.G1Jac
		ptJac.FromAffine(&tempProof.H)
		scaledJac.ScalarMultiplication(&ptJac, coeffs[i].BigInt(bInt))
		accJac.AddAssign(&scaledJac)
	}

	var combinedH bls12381.G1Affine
	combinedH.FromJacobian(&accJac)

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

	chunkSize := 32
	for offset := 0; offset < len(dst); offset += chunkSize {
		end := offset + chunkSize
		if end > len(dst) {
			end = len(dst)
		}
		dstEl.SetBytes(dst[offset:end])
		if offset < len(src) {
			srcEnd := offset + chunkSize
			if srcEnd > len(src) {
				srcEnd = len(src)
			}
			srcEl.SetBytes(src[offset:srcEnd])
		} else {
			srcEl.SetZero()
		}

		term.Mul(&srcEl, &coeff)
		dstEl.Add(&dstEl, &term)

		out := dstEl.Bytes()
		copy(dst[offset:end], out[32-(end-offset):])
	}
}

// VerifyReconstructedCell performs the direct algebraic combined verification of the cell pieces against the Block Header.
func (v *DASVerifier) VerifyReconstructedCell(
	pieces []cda.ReceivedPiece,
	columnCommitment []byte,
	globalCoeffs []byte, // k * 32 bytes from Block Header
	row int,
	domainSize ...int,
) (string, bool, error) {
	if len(pieces) < v.k {
		return "", false, fmt.Errorf("not enough pieces to reconstruct: got %d, expected %d", len(pieces), v.k)
	}

	// 1. Recover fragments using SolveGaussianFr
	A_recode := make([][]byte, v.k)
	recodedProofs := make([][]byte, v.k)
	workingData := make([][]byte, v.k)
	for i := 0; i < v.k; i++ {
		A_recode[i] = pieces[i].Data.Coeffs
		recodedProofs[i] = pieces[i].Proof
		workingData[i] = append([]byte(nil), pieces[i].Data.Data...)
	}

	recoveredFragments, err := rlnc.SolveGaussianFr(A_recode, workingData)
	if err != nil {
		return "", false, fmt.Errorf("failed to decode fragments using Gaussian elimination: %w", err)
	}

	// 2. Invert coefficient matrix A
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
	fragSize := len(recoveredFragments[0])
	combinedData := make([]byte, fragSize)
	for j := 0; j < v.k; j++ {
		VectorMulAddFr(combinedData, recoveredFragments[j], xFr[j])
	}

	// 6. Combine reconstructed proofs using global challenge x
	finalCombinedProof, err := CombineProofsFr(reconstructedProofs, xFr)
	if err != nil {
		return "", false, fmt.Errorf("failed to combine final proof: %w", err)
	}

	// 7. Verify combined data and combined proof against the Column Commitment from Block Header
	n := 0
	if len(domainSize) > 0 {
		n = domainSize[0]
	}
	verified := v.kzg.VerifyForDomain(columnCommitment, row, combinedData, finalCombinedProof, n)
	if !verified {
		return "", false, nil
	}

	// 8. Reconstruct cell string
	var buf bytes.Buffer
	for _, frag := range recoveredFragments {
		if fragSize <= 32 && fragSize > 16 {
			pieceSize := 64 / v.k
			if len(frag) >= pieceSize {
				buf.Write(frag[len(frag)-pieceSize:])
			} else {
				buf.Write(frag)
			}
		} else {
			buf.Write(frag)
		}
	}

	return hex.EncodeToString(buf.Bytes()), true, nil
}

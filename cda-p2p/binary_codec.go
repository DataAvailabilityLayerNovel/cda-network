package p2pcommon

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// BinarySeedMagic identifies the binary seeding wire protocol
	BinarySeedMagic uint32 = 0x43444101 // "CDA\x01"
)

// BinarySeedPiece represents a raw binary RLNC piece with KZG proof
type BinarySeedPiece struct {
	Row    uint16
	Col    uint16
	Data   []byte // Raw cell data (512 bytes)
	Coeffs []byte // Raw RLNC coefficients (8 bytes)
	Proof  []byte // Raw KZG opening proof (48 bytes)
}

// BinaryBatchSeedRequest represents a compact binary batch payload
type BinaryBatchSeedRequest struct {
	BlockID      string
	ColIdx       uint16
	PieceCommits [][]byte // Sent ONCE per batch (8 x 48 bytes)
	Seeds        []BinarySeedPiece
	SenderPeerID string
}

// BinaryBatchSeedResponse represents ACK from Store Node
type BinaryBatchSeedResponse struct {
	Success bool
	Error   string
}

// WriteBinaryBatchSeed encodes a BinaryBatchSeedRequest directly to an io.Writer with zero reflection
func WriteBinaryBatchSeed(w io.Writer, req *BinaryBatchSeedRequest) error {
	// Magic (4 bytes)
	if err := binary.Write(w, binary.BigEndian, BinarySeedMagic); err != nil {
		return err
	}

	// BlockID (1 byte len + string)
	bIDBytes := []byte(req.BlockID)
	if len(bIDBytes) > 255 {
		return fmt.Errorf("blockID too long: %d", len(bIDBytes))
	}
	if err := binary.Write(w, binary.BigEndian, uint8(len(bIDBytes))); err != nil {
		return err
	}
	if _, err := w.Write(bIDBytes); err != nil {
		return err
	}

	// ColIdx (2 bytes)
	if err := binary.Write(w, binary.BigEndian, req.ColIdx); err != nil {
		return err
	}

	// SenderPeerID (1 byte len + string)
	spBytes := []byte(req.SenderPeerID)
	if len(spBytes) > 255 {
		return fmt.Errorf("senderPeerID too long: %d", len(spBytes))
	}
	if err := binary.Write(w, binary.BigEndian, uint8(len(spBytes))); err != nil {
		return err
	}
	if len(spBytes) > 0 {
		if _, err := w.Write(spBytes); err != nil {
			return err
		}
	}

	// PieceCommits (2 bytes count, then for each commit: 2 bytes len + bytes)
	numCommits := uint16(len(req.PieceCommits))
	if err := binary.Write(w, binary.BigEndian, numCommits); err != nil {
		return err
	}
	for _, c := range req.PieceCommits {
		cLen := uint16(len(c))
		if err := binary.Write(w, binary.BigEndian, cLen); err != nil {
			return err
		}
		if cLen > 0 {
			if _, err := w.Write(c); err != nil {
				return err
			}
		}
	}

	// Seeds (2 bytes count)
	numSeeds := uint16(len(req.Seeds))
	if err := binary.Write(w, binary.BigEndian, numSeeds); err != nil {
		return err
	}

	// Pre-allocate a scratch buffer to batch write seeds
	for i := range req.Seeds {
		s := &req.Seeds[i]
		if err := binary.Write(w, binary.BigEndian, s.Row); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, s.Col); err != nil {
			return err
		}

		// Coeffs (1 byte len + bytes)
		if err := binary.Write(w, binary.BigEndian, uint8(len(s.Coeffs))); err != nil {
			return err
		}
		if len(s.Coeffs) > 0 {
			if _, err := w.Write(s.Coeffs); err != nil {
				return err
			}
		}

		// Proof (2 bytes len + bytes)
		if err := binary.Write(w, binary.BigEndian, uint16(len(s.Proof))); err != nil {
			return err
		}
		if len(s.Proof) > 0 {
			if _, err := w.Write(s.Proof); err != nil {
				return err
			}
		}

		// Data (2 bytes len + bytes)
		if err := binary.Write(w, binary.BigEndian, uint16(len(s.Data))); err != nil {
			return err
		}
		if len(s.Data) > 0 {
			if _, err := w.Write(s.Data); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReadBinaryBatchSeed decodes a BinaryBatchSeedRequest directly from an io.Reader
func ReadBinaryBatchSeed(r io.Reader) (*BinaryBatchSeedRequest, error) {
	var magic uint32
	if err := binary.Read(r, binary.BigEndian, &magic); err != nil {
		return nil, err
	}
	if magic != BinarySeedMagic {
		return nil, fmt.Errorf("invalid binary seed magic: 0x%X (expected 0x%X)", magic, BinarySeedMagic)
	}

	// BlockID
	var bIDLen uint8
	if err := binary.Read(r, binary.BigEndian, &bIDLen); err != nil {
		return nil, err
	}
	bIDBuf := make([]byte, bIDLen)
	if _, err := io.ReadFull(r, bIDBuf); err != nil {
		return nil, err
	}
	blockID := string(bIDBuf)

	// ColIdx
	var colIdx uint16
	if err := binary.Read(r, binary.BigEndian, &colIdx); err != nil {
		return nil, err
	}

	// SenderPeerID
	var spLen uint8
	if err := binary.Read(r, binary.BigEndian, &spLen); err != nil {
		return nil, err
	}
	var senderPeerID string
	if spLen > 0 {
		spBuf := make([]byte, spLen)
		if _, err := io.ReadFull(r, spBuf); err != nil {
			return nil, err
		}
		senderPeerID = string(spBuf)
	}

	// PieceCommits
	var numCommits uint16
	if err := binary.Read(r, binary.BigEndian, &numCommits); err != nil {
		return nil, err
	}
	commits := make([][]byte, numCommits)
	for i := 0; i < int(numCommits); i++ {
		var cLen uint16
		if err := binary.Read(r, binary.BigEndian, &cLen); err != nil {
			return nil, err
		}
		cBuf := make([]byte, cLen)
		if cLen > 0 {
			if _, err := io.ReadFull(r, cBuf); err != nil {
				return nil, err
			}
		}
		commits[i] = cBuf
	}

	// Seeds
	var numSeeds uint16
	if err := binary.Read(r, binary.BigEndian, &numSeeds); err != nil {
		return nil, err
	}

	seeds := make([]BinarySeedPiece, numSeeds)
	for i := 0; i < int(numSeeds); i++ {
		var row, col uint16
		if err := binary.Read(r, binary.BigEndian, &row); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &col); err != nil {
			return nil, err
		}

		// Coeffs
		var coeffLen uint8
		if err := binary.Read(r, binary.BigEndian, &coeffLen); err != nil {
			return nil, err
		}
		coeffBuf := make([]byte, coeffLen)
		if coeffLen > 0 {
			if _, err := io.ReadFull(r, coeffBuf); err != nil {
				return nil, err
			}
		}

		// Proof
		var proofLen uint16
		if err := binary.Read(r, binary.BigEndian, &proofLen); err != nil {
			return nil, err
		}
		proofBuf := make([]byte, proofLen)
		if proofLen > 0 {
			if _, err := io.ReadFull(r, proofBuf); err != nil {
				return nil, err
			}
		}

		// Data
		var dataLen uint16
		if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		dataBuf := make([]byte, dataLen)
		if dataLen > 0 {
			if _, err := io.ReadFull(r, dataBuf); err != nil {
				return nil, err
			}
		}

		seeds[i] = BinarySeedPiece{
			Row:    row,
			Col:    col,
			Data:   dataBuf,
			Coeffs: coeffBuf,
			Proof:  proofBuf,
		}
	}

	return &BinaryBatchSeedRequest{
		BlockID:      blockID,
		ColIdx:       colIdx,
		PieceCommits: commits,
		Seeds:        seeds,
		SenderPeerID: senderPeerID,
	}, nil
}

// WriteBinaryBatchResponse encodes an ACK/Error response
func WriteBinaryBatchResponse(w io.Writer, resp *BinaryBatchSeedResponse) error {
	var flag uint8
	if resp.Success {
		flag = 1
	}
	if err := binary.Write(w, binary.BigEndian, flag); err != nil {
		return err
	}
	errBytes := []byte(resp.Error)
	if err := binary.Write(w, binary.BigEndian, uint16(len(errBytes))); err != nil {
		return err
	}
	if len(errBytes) > 0 {
		if _, err := w.Write(errBytes); err != nil {
			return err
		}
	}
	return nil
}

// ReadBinaryBatchResponse decodes an ACK/Error response
func ReadBinaryBatchResponse(r io.Reader) (*BinaryBatchSeedResponse, error) {
	var flag uint8
	if err := binary.Read(r, binary.BigEndian, &flag); err != nil {
		return nil, err
	}
	var errLen uint16
	if err := binary.Read(r, binary.BigEndian, &errLen); err != nil {
		return nil, err
	}
	var errStr string
	if errLen > 0 {
		errBuf := make([]byte, errLen)
		if _, err := io.ReadFull(r, errBuf); err != nil {
			return nil, err
		}
		errStr = string(errBuf)
	}
	return &BinaryBatchSeedResponse{
		Success: flag == 1,
		Error:   errStr,
	}, nil
}

const (
	// BinaryGossipMagic identifies a binary recoded piece sent over GossipSub
	BinaryGossipMagic uint8 = 0x47 // 'G'
)

// BinaryGossipPiece represents a recoded piece transmitted over GossipSub
type BinaryGossipPiece struct {
	BlockID      string
	Row          uint16
	Col          uint16
	Coeffs       []byte
	Proof        []byte
	Data         []byte
	SenderPeerID string
}

// EncodeBinaryGossipPiece serializes a recoded piece into a compact binary buffer
func EncodeBinaryGossipPiece(p *BinaryGossipPiece) []byte {
	bIDBytes := []byte(p.BlockID)
	spBytes := []byte(p.SenderPeerID)

	totalLen := 1 + 1 + len(bIDBytes) + 2 + 2 + 1 + len(p.Coeffs) + 2 + len(p.Proof) + 2 + len(p.Data) + 1 + len(spBytes)
	buf := make([]byte, totalLen)

	idx := 0
	buf[idx] = BinaryGossipMagic
	idx++

	buf[idx] = uint8(len(bIDBytes))
	idx++
	copy(buf[idx:], bIDBytes)
	idx += len(bIDBytes)

	binary.BigEndian.PutUint16(buf[idx:], p.Row)
	idx += 2
	binary.BigEndian.PutUint16(buf[idx:], p.Col)
	idx += 2

	buf[idx] = uint8(len(p.Coeffs))
	idx++
	copy(buf[idx:], p.Coeffs)
	idx += len(p.Coeffs)

	binary.BigEndian.PutUint16(buf[idx:], uint16(len(p.Proof)))
	idx += 2
	copy(buf[idx:], p.Proof)
	idx += len(p.Proof)

	binary.BigEndian.PutUint16(buf[idx:], uint16(len(p.Data)))
	idx += 2
	copy(buf[idx:], p.Data)
	idx += len(p.Data)

	buf[idx] = uint8(len(spBytes))
	idx++
	copy(buf[idx:], spBytes)
	idx += len(spBytes)

	return buf[:idx]
}

// DecodeBinaryGossipPiece deserializes a recoded piece from a binary buffer
func DecodeBinaryGossipPiece(data []byte) (*BinaryGossipPiece, error) {
	if len(data) < 13 {
		return nil, fmt.Errorf("binary gossip piece payload too short: %d", len(data))
	}
	if data[0] != BinaryGossipMagic {
		return nil, fmt.Errorf("invalid binary gossip magic: 0x%X", data[0])
	}

	idx := 1
	bIDLen := int(data[idx])
	idx++
	if idx+bIDLen > len(data) {
		return nil, fmt.Errorf("truncated blockID")
	}
	blockID := string(data[idx : idx+bIDLen])
	idx += bIDLen

	if idx+4 > len(data) {
		return nil, fmt.Errorf("truncated row/col")
	}
	row := binary.BigEndian.Uint16(data[idx:])
	idx += 2
	col := binary.BigEndian.Uint16(data[idx:])
	idx += 2

	if idx+1 > len(data) {
		return nil, fmt.Errorf("truncated coeffs len")
	}
	cLen := int(data[idx])
	idx++
	if idx+cLen > len(data) {
		return nil, fmt.Errorf("truncated coeffs data")
	}
	coeffs := make([]byte, cLen)
	copy(coeffs, data[idx:idx+cLen])
	idx += cLen

	if idx+2 > len(data) {
		return nil, fmt.Errorf("truncated proof len")
	}
	pLen := int(binary.BigEndian.Uint16(data[idx:]))
	idx += 2
	if idx+pLen > len(data) {
		return nil, fmt.Errorf("truncated proof data")
	}
	proof := make([]byte, pLen)
	copy(proof, data[idx:idx+pLen])
	idx += pLen

	if idx+2 > len(data) {
		return nil, fmt.Errorf("truncated data len")
	}
	dLen := int(binary.BigEndian.Uint16(data[idx:]))
	idx += 2
	if idx+dLen > len(data) {
		return nil, fmt.Errorf("truncated data payload")
	}
	dBuf := make([]byte, dLen)
	copy(dBuf, data[idx:idx+dLen])
	idx += dLen

	var senderPeerID string
	if idx < len(data) {
		spLen := int(data[idx])
		idx++
		if idx+spLen <= len(data) {
			senderPeerID = string(data[idx : idx+spLen])
		}
	}

	return &BinaryGossipPiece{
		BlockID:      blockID,
		Row:          row,
		Col:          col,
		Coeffs:       coeffs,
		Proof:        proof,
		Data:         dBuf,
		SenderPeerID: senderPeerID,
	}, nil
}

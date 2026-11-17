package p2pcommon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// BinarySeedMagic identifies the binary seeding wire protocol
	BinarySeedMagic uint32 = 0x43444101 // "CDA\x01"
)

// EncodeBinaryBatchSeed serializes a BinaryBatchSeedRequest into a byte slice for GossipSub TopicCellRowSeed.
func EncodeBinaryBatchSeed(req *BinaryBatchSeedRequest) ([]byte, error) {
	var buf bytes.Buffer
	if err := WriteBinaryBatchSeed(&buf, req); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeBinaryBatchSeed deserializes a BinaryBatchSeedRequest from a byte slice received on GossipSub TopicCellRowSeed.
func DecodeBinaryBatchSeed(data []byte) (*BinaryBatchSeedRequest, error) {
	return ReadBinaryBatchSeed(bytes.NewReader(data))
}

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

const (
	// BinaryPublisherPushMagic identifies the binary column push wire protocol (Publisher -> Bootstrap)
	BinaryPublisherPushMagic uint32 = 0x43444102 // "CDA\x02"
	// BinaryPublishBlockMagic identifies the binary block publish wire protocol (CometBFT -> Publisher)
	BinaryPublishBlockMagic uint32 = 0x43444103 // "CDA\x03"
)

// BinaryPublisherPushRequest represents a raw binary column push from Publisher to Bootstrap Node
type BinaryPublisherPushRequest struct {
	BlockID      string
	ColIdx       uint16
	ColumnData   [][]byte
	PieceCommits [][]byte
	MerkleProofs []SerializedMerkleProof
}

// WriteBinaryPublisherPush encodes a BinaryPublisherPushRequest directly to an io.Writer without hex/JSON overhead
func WriteBinaryPublisherPush(w io.Writer, req *BinaryPublisherPushRequest) error {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, BinaryPublisherPushMagic); err != nil {
		return err
	}

	bIDBytes := []byte(req.BlockID)
	if len(bIDBytes) > 255 {
		return fmt.Errorf("blockID too long: %d", len(bIDBytes))
	}
	buf.WriteByte(uint8(len(bIDBytes)))
	buf.Write(bIDBytes)

	_ = binary.Write(&buf, binary.BigEndian, req.ColIdx)

	// ColumnData
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(req.ColumnData)))
	for _, cell := range req.ColumnData {
		_ = binary.Write(&buf, binary.BigEndian, uint16(len(cell)))
		if len(cell) > 0 {
			buf.Write(cell)
		}
	}

	// PieceCommits
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(req.PieceCommits)))
	for _, c := range req.PieceCommits {
		_ = binary.Write(&buf, binary.BigEndian, uint16(len(c)))
		if len(c) > 0 {
			buf.Write(c)
		}
	}

	// MerkleProofs
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(req.MerkleProofs)))
	for _, mp := range req.MerkleProofs {
		_ = binary.Write(&buf, binary.BigEndian, uint32(mp.Index))
		_ = binary.Write(&buf, binary.BigEndian, uint16(len(mp.Siblings)))
		for _, sib := range mp.Siblings {
			_ = binary.Write(&buf, binary.BigEndian, uint16(len(sib)))
			if len(sib) > 0 {
				buf.Write(sib)
			}
		}
	}

	_, err := w.Write(buf.Bytes())
	return err
}

// ReadBinaryPublisherPush decodes a BinaryPublisherPushRequest from an io.Reader
func ReadBinaryPublisherPush(r io.Reader) (*BinaryPublisherPushRequest, error) {
	var magic uint32
	if err := binary.Read(r, binary.BigEndian, &magic); err != nil {
		return nil, err
	}
	if magic != BinaryPublisherPushMagic {
		return nil, fmt.Errorf("invalid binary publisher push magic: 0x%X (expected 0x%X)", magic, BinaryPublisherPushMagic)
	}

	var bIDLen uint8
	if err := binary.Read(r, binary.BigEndian, &bIDLen); err != nil {
		return nil, err
	}
	bIDBuf := make([]byte, bIDLen)
	if _, err := io.ReadFull(r, bIDBuf); err != nil {
		return nil, err
	}

	var colIdx uint16
	if err := binary.Read(r, binary.BigEndian, &colIdx); err != nil {
		return nil, err
	}

	var numCells uint16
	if err := binary.Read(r, binary.BigEndian, &numCells); err != nil {
		return nil, err
	}
	colData := make([][]byte, numCells)
	for i := 0; i < int(numCells); i++ {
		var cellLen uint16
		if err := binary.Read(r, binary.BigEndian, &cellLen); err != nil {
			return nil, err
		}
		cellBuf := make([]byte, cellLen)
		if cellLen > 0 {
			if _, err := io.ReadFull(r, cellBuf); err != nil {
				return nil, err
			}
		}
		colData[i] = cellBuf
	}

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

	var numProofs uint16
	if err := binary.Read(r, binary.BigEndian, &numProofs); err != nil {
		return nil, err
	}
	proofs := make([]SerializedMerkleProof, numProofs)
	for i := 0; i < int(numProofs); i++ {
		var idxVal uint32
		if err := binary.Read(r, binary.BigEndian, &idxVal); err != nil {
			return nil, err
		}
		var numSibs uint16
		if err := binary.Read(r, binary.BigEndian, &numSibs); err != nil {
			return nil, err
		}
		sibs := make([][]byte, numSibs)
		for j := 0; j < int(numSibs); j++ {
			var sLen uint16
			if err := binary.Read(r, binary.BigEndian, &sLen); err != nil {
				return nil, err
			}
			sBuf := make([]byte, sLen)
			if sLen > 0 {
				if _, err := io.ReadFull(r, sBuf); err != nil {
					return nil, err
				}
			}
			sibs[j] = sBuf
		}
		proofs[i] = SerializedMerkleProof{
			Index:    int(idxVal),
			Siblings: sibs,
		}
	}

	return &BinaryPublisherPushRequest{
		BlockID:      string(bIDBuf),
		ColIdx:       colIdx,
		ColumnData:   colData,
		PieceCommits: commits,
		MerkleProofs: proofs,
	}, nil
}

// BinaryHeaderPayload holds consensus header commitments in raw binary form
type BinaryHeaderPayload struct {
	ChainID         string
	Height          int64
	TimeUnixNano    int64
	DataHash        []byte
	AppHash         []byte
	ProposerAddress string
	CommitsRoot     []byte
	ColumnComm      [][]byte
	Coeffs          []byte
}

// BinaryTxSummary holds transaction cell offset metadata
type BinaryTxSummary struct {
	Hash   string
	Key    string
	Offset int
	Cells  int
	Size   int
}

// BinaryPublishBlockRequest represents a raw binary block publish request from CometBFT to Publisher
type BinaryPublishBlockRequest struct {
	BlockID   string
	Height    int64
	K         int
	Cells     [][]byte
	Header    *BinaryHeaderPayload
	Txs       []BinaryTxSummary
	Signature []byte
}

func writeString16(buf *bytes.Buffer, s string) {
	b := []byte(s)
	_ = binary.Write(buf, binary.BigEndian, uint16(len(b)))
	if len(b) > 0 {
		buf.Write(b)
	}
}

func readString16(r io.Reader) (string, error) {
	var l uint16
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return "", err
	}
	if l == 0 {
		return "", nil
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func writeBytes16(buf *bytes.Buffer, b []byte) {
	_ = binary.Write(buf, binary.BigEndian, uint16(len(b)))
	if len(b) > 0 {
		buf.Write(b)
	}
}

func readBytes16(r io.Reader) ([]byte, error) {
	var l uint16
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return nil, err
	}
	if l == 0 {
		return nil, nil
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

// EncodeBinaryPublishBlock serializes a BinaryPublishBlockRequest into a byte slice
func EncodeBinaryPublishBlock(req *BinaryPublishBlockRequest) ([]byte, error) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, BinaryPublishBlockMagic)

	writeString16(&buf, req.BlockID)
	_ = binary.Write(&buf, binary.BigEndian, req.Height)
	_ = binary.Write(&buf, binary.BigEndian, uint16(req.K))

	// Raw ODS cells
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(req.Cells)))
	for _, cell := range req.Cells {
		writeBytes16(&buf, cell)
	}

	// Optional Header
	if req.Header != nil {
		buf.WriteByte(1)
		writeString16(&buf, req.Header.ChainID)
		_ = binary.Write(&buf, binary.BigEndian, req.Header.Height)
		_ = binary.Write(&buf, binary.BigEndian, req.Header.TimeUnixNano)
		writeBytes16(&buf, req.Header.DataHash)
		writeBytes16(&buf, req.Header.AppHash)
		writeString16(&buf, req.Header.ProposerAddress)
		writeBytes16(&buf, req.Header.CommitsRoot)
		_ = binary.Write(&buf, binary.BigEndian, uint16(len(req.Header.ColumnComm)))
		for _, cc := range req.Header.ColumnComm {
			writeBytes16(&buf, cc)
		}
		writeBytes16(&buf, req.Header.Coeffs)
	} else {
		buf.WriteByte(0)
	}

	// Txs
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(req.Txs)))
	for _, tx := range req.Txs {
		writeString16(&buf, tx.Hash)
		writeString16(&buf, tx.Key)
		_ = binary.Write(&buf, binary.BigEndian, uint32(tx.Offset))
		_ = binary.Write(&buf, binary.BigEndian, uint32(tx.Cells))
		_ = binary.Write(&buf, binary.BigEndian, uint32(tx.Size))
	}

	// Signature
	writeBytes16(&buf, req.Signature)

	return buf.Bytes(), nil
}

// DecodeBinaryPublishBlock deserializes a BinaryPublishBlockRequest from a byte slice
func DecodeBinaryPublishBlock(data []byte) (*BinaryPublishBlockRequest, error) {
	r := bytes.NewReader(data)
	var magic uint32
	if err := binary.Read(r, binary.BigEndian, &magic); err != nil {
		return nil, err
	}
	if magic != BinaryPublishBlockMagic {
		return nil, fmt.Errorf("invalid binary publish block magic: 0x%X", magic)
	}

	blockID, err := readString16(r)
	if err != nil {
		return nil, err
	}
	var height int64
	if err := binary.Read(r, binary.BigEndian, &height); err != nil {
		return nil, err
	}
	var kVal uint16
	if err := binary.Read(r, binary.BigEndian, &kVal); err != nil {
		return nil, err
	}

	var numCells uint32
	if err := binary.Read(r, binary.BigEndian, &numCells); err != nil {
		return nil, err
	}
	cells := make([][]byte, numCells)
	for i := 0; i < int(numCells); i++ {
		c, err := readBytes16(r)
		if err != nil {
			return nil, err
		}
		cells[i] = c
	}

	var hasHeader uint8
	if err := binary.Read(r, binary.BigEndian, &hasHeader); err != nil {
		return nil, err
	}
	var hdr *BinaryHeaderPayload
	if hasHeader == 1 {
		chainID, err := readString16(r)
		if err != nil {
			return nil, err
		}
		var hHeight, timeNano int64
		if err := binary.Read(r, binary.BigEndian, &hHeight); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &timeNano); err != nil {
			return nil, err
		}
		dataHash, err := readBytes16(r)
		if err != nil {
			return nil, err
		}
		appHash, err := readBytes16(r)
		if err != nil {
			return nil, err
		}
		propAddr, err := readString16(r)
		if err != nil {
			return nil, err
		}
		commitsRoot, err := readBytes16(r)
		if err != nil {
			return nil, err
		}
		var numCols uint16
		if err := binary.Read(r, binary.BigEndian, &numCols); err != nil {
			return nil, err
		}
		colComm := make([][]byte, numCols)
		for i := 0; i < int(numCols); i++ {
			cc, err := readBytes16(r)
			if err != nil {
				return nil, err
			}
			colComm[i] = cc
		}
		coeffs, err := readBytes16(r)
		if err != nil {
			return nil, err
		}
		hdr = &BinaryHeaderPayload{
			ChainID:         chainID,
			Height:          hHeight,
			TimeUnixNano:    timeNano,
			DataHash:        dataHash,
			AppHash:         appHash,
			ProposerAddress: propAddr,
			CommitsRoot:     commitsRoot,
			ColumnComm:      colComm,
			Coeffs:          coeffs,
		}
	}

	var numTxs uint32
	if err := binary.Read(r, binary.BigEndian, &numTxs); err != nil {
		return nil, err
	}
	txs := make([]BinaryTxSummary, numTxs)
	for i := 0; i < int(numTxs); i++ {
		txHash, err := readString16(r)
		if err != nil {
			return nil, err
		}
		txKey, err := readString16(r)
		if err != nil {
			return nil, err
		}
		var offset, numC, sz uint32
		if err := binary.Read(r, binary.BigEndian, &offset); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &numC); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
			return nil, err
		}
		txs[i] = BinaryTxSummary{
			Hash:   txHash,
			Key:    txKey,
			Offset: int(offset),
			Cells:  int(numC),
			Size:   int(sz),
		}
	}

	sig, err := readBytes16(r)
	if err != nil {
		return nil, err
	}

	return &BinaryPublishBlockRequest{
		BlockID:   blockID,
		Height:    height,
		K:         int(kVal),
		Cells:     cells,
		Header:    hdr,
		Txs:       txs,
		Signature: sig,
	}, nil
}

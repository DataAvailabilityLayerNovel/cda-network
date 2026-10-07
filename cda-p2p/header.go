package p2pcommon

import (
	"encoding/hex"
	"encoding/json"
	"time"
)

// BlockHeader defines the unified block header across the CDA Network,
// synchronized directly with the CometBFT consensus layer.
type BlockHeader struct {
	// Consensus Layer Metadata (from CometBFT Header)
	ChainID         string    `json:"chain_id,omitempty"`
	Height          int64     `json:"height"`
	Time            time.Time `json:"time,omitempty"`
	BlockID         string    `json:"block_id"`
	DataHash        string    `json:"data_hash,omitempty"`
	AppHash         string    `json:"app_hash,omitempty"`
	ProposerAddress string    `json:"proposer_address,omitempty"`

	// Data Availability Root-of-Trust Commitments
	CommitsRoot string   `json:"commits_root"`
	ColumnComm  [][]byte `json:"column_comm"` // 2K Kate column commitments (each is a G1Affine point)
	Coeffs      []byte   `json:"coeffs"`      // Global Fiat-Shamir challenge vector (k * 32 bytes)
}

// Serialize encodes the BlockHeader into JSON bytes.
func (h *BlockHeader) Serialize() ([]byte, error) {
	return json.Marshal(h)
}

// DeserializeHeader decodes a BlockHeader from JSON bytes.
func DeserializeHeader(data []byte) (*BlockHeader, error) {
	var h BlockHeader
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// ColumnCommHex returns the column commitments as a slice of hex strings.
func (h *BlockHeader) ColumnCommHex() []string {
	res := make([]string, len(h.ColumnComm))
	for i, c := range h.ColumnComm {
		res[i] = hex.EncodeToString(c)
	}
	return res
}

package engine

import (
	"encoding/json"
)

type BlockHeader struct {
	BlockID     string   `json:"block_id"`
	Height      int      `json:"height,omitempty"`
	CommitsRoot string   `json:"commits_root"`
	ColumnComm  [][]byte `json:"column_comm"` // N column commitments (each is a G1Affine byte slice)
	Coeffs      []byte   `json:"coeffs"`      // Global Fiat-Shamir challenge vector (k * 32 bytes)
}

// Serialize encodes the BlockHeader into json bytes
func (h *BlockHeader) Serialize() ([]byte, error) {
	return json.Marshal(h)
}

// DeserializeHeader decodes the BlockHeader from json bytes
func DeserializeHeader(data []byte) (*BlockHeader, error) {
	var h BlockHeader
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

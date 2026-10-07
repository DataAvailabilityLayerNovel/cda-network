package engine

import p2pcommon "cda-p2p"

// BlockHeader aliases p2pcommon.BlockHeader to maintain backward compatibility
// while centralizing the definition according to the CometBFT consensus layer.
type BlockHeader = p2pcommon.BlockHeader

// DeserializeHeader decodes the BlockHeader from json bytes
func DeserializeHeader(data []byte) (*BlockHeader, error) {
	return p2pcommon.DeserializeHeader(data)
}

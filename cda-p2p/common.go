package p2pcommon

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/zeebo/blake3"
)

// ParseHeightFromBlockID extracts the numeric block height from a block ID string.
// Supports formats like "block-123", "block-1-hash", "bot-block-456", "height_789", etc.
func ParseHeightFromBlockID(blockID string) int {
	parts := strings.FieldsFunc(blockID, func(r rune) bool {
		return r == '-' || r == '_'
	})
	for i, p := range parts {
		if (strings.EqualFold(p, "block") || strings.EqualFold(p, "height")) && i+1 < len(parts) {
			if val, err := strconv.Atoi(parts[i+1]); err == nil {
				return val
			}
		}
	}
	for i := len(parts) - 1; i >= 0; i-- {
		val, err := strconv.Atoi(parts[i])
		if err == nil {
			return val
		}
	}
	return 0
}

func CalculateCell(pid peer.ID, k1, k2 int, salt string) (row, col int) {
	if k1 <= 0 || k2 <= 0 {
		return 0, 0
	}
	h := blake3.New()
	h.Write([]byte(pid.String()))
	h.Write([]byte(salt))
	sum := h.Sum(nil)

	// Use first 8 bytes for row, and next 8 bytes for column to ensure independence
	valRow := binary.BigEndian.Uint64(sum[:8])
	valCol := binary.BigEndian.Uint64(sum[8:16])
	row = int(valRow % uint64(k1))
	col = int(valCol % uint64(k2))
	return row, col
}

// GenerateKeypairForCell searches deterministically for a private key whose PeerID maps to the target (row, col)
func GenerateKeypairForCell(row, col, k1, k2 int, salt string) (crypto.PrivKey, peer.ID, error) {
	for counter := 0; counter < 1000000; counter++ {
		seedStr := fmt.Sprintf("cda-node-%d-%d-%d", row, col, counter)
		seed := sha256.Sum256([]byte(seedStr))
		priv, _, err := crypto.GenerateEd25519Key(bytes.NewReader(seed[:]))
		if err != nil {
			return nil, "", err
		}
		pid, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			return nil, "", err
		}
		r, c := CalculateCell(pid, k1, k2, salt)
		if r == row && c == col {
			return priv, pid, nil
		}
	}
	return nil, "", fmt.Errorf("failed to generate key for cell [%d, %d] in 1M iterations", row, col)
}

// GenerateDeterministicKeypair generates a private key deterministically from a simple string seed
func GenerateDeterministicKeypair(seedStr string) (crypto.PrivKey, peer.ID, error) {
	seed := sha256.Sum256([]byte(seedStr))
	priv, _, err := crypto.GenerateEd25519Key(bytes.NewReader(seed[:]))
	if err != nil {
		return nil, "", err
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, "", err
	}
	return priv, pid, nil
}

// NewP2PHost creates a libp2p Host with standard settings
func NewP2PHost(listenPort int, privKey crypto.PrivKey) (host.Host, error) {
	opts := []libp2p.Option{
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)),
	}
	return libp2p.New(opts...)
}

// Protocol Names
const (
	ProtoPublisherPush    = "/cda/publisher/push-chunk/1.0.0"
	ProtoBootstrapRouting = "/cda/bootstrap/routing/1.0.0"
	ProtoStoreFetch       = "/cda/store/fetch-pieces/1.0.0"
	ProtoStoreBatchFetch  = "/cda/store/fetch-batch-pieces/1.0.0"
	ProtoStoreGetPieces   = "/cda/store/get-cell-pieces/1.0.0"
)

// ProtoNodeSeed returns the per-node libp2p stream protocol for seeding pieces.
// Bootstrap node opens a stream to this protocol when delivering RLNC pieces to a store node.
func ProtoNodeSeed(peerID string) string {
	return fmt.Sprintf("/cda/store/%s/seed/1.0.0", peerID)
}

// ProtoNodeBatchSeed returns the per-node libp2p stream protocol for seeding a batch of pieces.
func ProtoNodeBatchSeed(peerID string) string {
	return fmt.Sprintf("/cda/store/%s/seed-batch/1.0.0", peerID)
}

// GossipSub Topics
const (
	TopicHeader     = "/cda/1.0.0/header"
	TopicStoreReady = "/cda/1.0.0/store-ready"
	TopicBlockReady = "/cda/1.0.0/block-ready"
)

// TopicCol returns GossipSub column topic name (used for anchor commitment broadcast)
func TopicCol(col int) string {
	return fmt.Sprintf("/cda/1.0.0/col/%d", col)
}

// TopicRow returns GossipSub row topic name
func TopicRow(row int) string {
	return fmt.Sprintf("/cda/1.0.0/row/%d", row)
}

// TopicNode returns the per-node GossipSub topic for a store node identified by its PeerID.
// Custody nodes publish their RLNC pieces here; non-custody nodes subscribe to get pieces.
func TopicNode(peerID string) string {
	return fmt.Sprintf("/cda/1.0.0/node/%s", peerID)
}

// PeerInfo wraps Peer ID and Multiaddrs
type PeerInfo struct {
	PeerID     string   `json:"peer_id"`
	Multiaddrs []string `json:"multiaddrs"`
	Row        int      `json:"row"`
	Col        int      `json:"col"`
}

// BootstrapRoutingRequest payload for Discovery Stream
type BootstrapRoutingRequest struct {
	Peer      PeerInfo `json:"peer"`
	TargetRow int      `json:"target_row"`
	TargetCol int      `json:"target_col"`
	IsLeave   bool     `json:"is_leave"`
}

// BootstrapRoutingResponse payload for Discovery Stream
type BootstrapRoutingResponse struct {
	RowPeers []PeerInfo `json:"row_peers"`
	ColPeers []PeerInfo `json:"col_peers"`
}

// PublisherPushRequest payload for chunk pushing
type PublisherPushRequest struct {
	BlockID      string       `json:"block_id"`
	ColIdx       int          `json:"col_idx"`
	ColumnData   []string     `json:"column_data"`   // Hex-encoded data cells
	PieceCommits []string     `json:"piece_commits"` // Hex-encoded piece commitments
	MerkleProofs []SerializedMerkleProof `json:"merkle_proofs"`
}

// SerializedMerkleProof matches the cda.MerkleProof structure for serializability
type SerializedMerkleProof struct {
	Index    int      `json:"index"`
	Siblings [][]byte `json:"siblings"`
}

// SeedCellRequest payload for seeding Store nodes
type SeedCellRequest struct {
	BlockID      string   `json:"block_id"`
	Row          int      `json:"row"`
	Col          int      `json:"col"`
	Data         string   `json:"data"`          // Hex coded data d_i
	Coeffs       string   `json:"coeffs"`        // Hex coefficients g_i
	Proof        string   `json:"proof"`         // Hex combined proof P_i
	PieceCommits []string `json:"piece_commits"` // Hex piece commitments C_0..C_k-1
	SenderPeerID string   `json:"sender_peer_id,omitempty"` // PeerID of the custody node sending this piece
}

// BatchSeedCellRequest payload for sending multiple seed pieces in a single stream
type BatchSeedCellRequest struct {
	BlockID string            `json:"block_id"`
	Seeds   []SeedCellRequest `json:"seeds"`
}

// StoreFetchRequest is the request to retrieve pieces from a Store node
type StoreFetchRequest struct {
	BlockID     string `json:"block_id"`
	Row         int    `json:"row"`
	Col         int    `json:"col"`
	IsRemoteHop bool   `json:"is_remote_hop"`
}

// StoreFetchResponse contains recovered or cached pieces from a Store node
type StoreFetchResponse struct {
	BlockID     string         `json:"block_id"`
	Row         int            `json:"row"`
	Col         int            `json:"col"`
	Recovered   bool           `json:"recovered"`
	Data        string         `json:"data,omitempty"`
	PiecesCount int            `json:"pieces_count"`
	Pieces      []CodedPiece   `json:"pieces"`
}

// CellCoord identifies a cell by its row and column coordinates
type CellCoord struct {
	Row int `json:"row"`
	Col int `json:"col"`
}

// StoreBatchFetchRequest is a request to retrieve pieces for multiple cells in a single stream
type StoreBatchFetchRequest struct {
	BlockID string      `json:"block_id"`
	Cells   []CellCoord `json:"cells"`
}

// StoreBatchFetchResponse returns slices of pieces keyed by "row_col"
type StoreBatchFetchResponse struct {
	BlockID string                  `json:"block_id"`
	Cells   map[string][]CodedPiece `json:"cells"` // Key format: "row_col"
}

// CodedPiece represents one coded slice stored by a custody node
type CodedPiece struct {
	Row          int      `json:"row"`
	Col          int      `json:"col"`
	Data         string   `json:"data"`
	Coeffs       string   `json:"coeffs"`
	Proof        string   `json:"proof"`
	PieceCommits []string `json:"piece_commits"`
}

// GossipAnchorPayload is used on the GossipSub topic for anchoring commitments
type GossipAnchorPayload struct {
	BlockID      string                  `json:"block_id"`
	ColIdx       int                     `json:"col_idx"`
	PieceCommits []string                `json:"piece_commits"`
	MerkleProofs []SerializedMerkleProof `json:"merkle_proofs"`
}

// GossipStoreReadyPayload signals that an individual store node has completed storing its custody cells for a block
type GossipStoreReadyPayload struct {
	BlockID      string `json:"block_id"`
	Height       int    `json:"height"`
	NetColIdx    int    `json:"net_col_idx"`
	ColIdx       int    `json:"col_idx"`
	RowIdx       int    `json:"row_idx"`
	StoresPerCol int    `json:"stores_per_col"`
}

// GossipBlockReadyPayload signals that all active columns are ready and light nodes can start DAS
type GossipBlockReadyPayload struct {
	BlockID string `json:"block_id"`
	Height  int    `json:"height"`
}

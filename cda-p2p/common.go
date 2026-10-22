package p2pcommon

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
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
	return GenerateKeypairForCellInstance(row, col, k1, k2, salt, "")
}

// GenerateKeypairForCellInstance searches deterministically for a private key for a specific node instance
// whose PeerID maps to the target (row % k1, col % k2) under CalculateCell.
func GenerateKeypairForCellInstance(row, col, k1, k2 int, salt string, instanceID string) (crypto.PrivKey, peer.ID, error) {
	if k1 <= 0 {
		k1 = 8
	}
	if k2 <= 0 {
		k2 = 8
	}
	targetRow := row % k1
	if targetRow < 0 {
		targetRow += k1
	}
	targetCol := col % k2
	if targetCol < 0 {
		targetCol += k2
	}
	for counter := 0; counter < 1000000; counter++ {
		var seedStr string
		if instanceID == "" {
			seedStr = fmt.Sprintf("cda-node-%d-%d-%d", row, col, counter)
		} else {
			seedStr = fmt.Sprintf("cda-node-%s-%d-%d-%d", instanceID, row, col, counter)
		}
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
		if r == targetRow && c == targetCol {
			return priv, pid, nil
		}
	}
	return nil, "", fmt.Errorf("failed to generate key for cell [%d, %d] (instance %s) in 1M iterations", row, col, instanceID)
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
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", listenPort),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort),
		),
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

// ProtoNodePersistentSeed returns the per-node libp2p stream protocol for a persistent seeding stream.
// Bootstrap maintains a long-lived stream to this protocol for delivering batches of RLNC pieces.
func ProtoNodePersistentSeed(peerID string) string {
	return fmt.Sprintf("/cda/store/%s/seed-stream/1.0.0", peerID)
}

// ProtoNodeBinaryPersistentSeed returns the per-node libp2p stream protocol for binary batch seeding.
func ProtoNodeBinaryPersistentSeed(peerID string) string {
	return fmt.Sprintf("/cda/store/%s/seed-binary/1.0.0", peerID)
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

// TopicCellRowSeed returns the GossipSub topic where Bootstrap publishes the 2*kPiece seed pool
// for network cell (netCol, netRow). Only Custody Store Nodes assigned to netRow subscribe to this channel.
func TopicCellRowSeed(netCol, netRow int) string {
	return fmt.Sprintf("/cda/1.0.0/col/%d/row/%d/seed", netCol, netRow)
}

// TopicCellRowRecode returns the GossipSub topic where Custody Store Nodes assigned to (netCol, netRow)
// publish their recoded RLNC pieces. Both Custody Nodes and Non-Custody Nodes in the column subscribe.
func TopicCellRowRecode(netCol, netRow int) string {
	return fmt.Sprintf("/cda/1.0.0/col/%d/row/%d/recode", netCol, netRow)
}

// TopicCellRow returns the GossipSub recode topic for a specific network cell (netCol, netRow).
func TopicCellRow(netCol, netRow int) string {
	return TopicCellRowRecode(netCol, netRow)
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
	RowPeers         []PeerInfo         `json:"row_peers"`
	ColPeers         []PeerInfo         `json:"col_peers"`
	RowCustodyMap    map[int][]PeerInfo `json:"row_custody_map,omitempty"`
	NetRows          int                `json:"net_rows,omitempty"`
	ActivePeersInCol int                `json:"active_peers_in_col,omitempty"`
}

// GetActiveColumnStores filters and deduplicates valid Store Node peers (Row >= 0),
// sorted deterministically by (Row, PeerID).
func GetActiveColumnStores(colPeers []PeerInfo) []PeerInfo {
	seen := make(map[string]bool, len(colPeers))
	var active []PeerInfo
	for _, p := range colPeers {
		if p.Row >= 0 && p.PeerID != "" && !seen[p.PeerID] {
			seen[p.PeerID] = true
			active = append(active, p)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		if active[i].Row != active[j].Row {
			return active[i].Row < active[j].Row
		}
		return active[i].PeerID < active[j].PeerID
	})
	return active
}

// ComputeColumnCustodyMap deterministically computes the Custody Store Nodes assigned to each
// network row netRow in [0, netRows-1] for a given column's active store peers:
//  1. If cell (netRow, netCol) has >= 2 store nodes: all M store nodes in that cell are active Custody Nodes.
//  2. If cell (netRow, netCol) has == 1 store node: that node is the primary custody node, and 1 additional
//     store node is borrowed from the next active row in the column so M_effective = 2.
//  3. If cell (netRow, netCol) has == 0 store nodes (empty/left): up to 2 store nodes are borrowed from
//     subsequent active rows in the column so no data row is ever orphaned.
func ComputeColumnCustodyMap(colPeers []PeerInfo, netRows int) map[int][]PeerInfo {
	if netRows <= 0 {
		netRows = 8
	}
	active := GetActiveColumnStores(colPeers)
	custodyMap := make(map[int][]PeerInfo, netRows)
	if len(active) == 0 {
		return custodyMap
	}

	grouped := make(map[int][]PeerInfo, netRows)
	for _, p := range active {
		r := p.Row % netRows
		if r < 0 {
			r += netRows
		}
		grouped[r] = append(grouped[r], p)
	}

	for targetRow := 0; targetRow < netRows; targetRow++ {
		cellPeers := grouped[targetRow]
		if len(cellPeers) >= 2 {
			assigned := make([]PeerInfo, len(cellPeers))
			copy(assigned, cellPeers)
			custodyMap[targetRow] = assigned
			continue
		}

		if len(cellPeers) == 1 {
			assigned := []PeerInfo{cellPeers[0]}
			// Borrow 1 additional peer from the next available row in the column
			// Note: in legacy 1-node-per-cell mode, row r was backed up by rowIdx = (r + 1) % netRows.
			for offset := 1; offset < netRows; offset++ {
				candRow := (targetRow + offset) % netRows
				candPeers := grouped[candRow]
				if len(candPeers) > 0 {
					// If candRow has multiple nodes, pick a secondary node to balance load; otherwise pick candPeers[0]
					pickIdx := 0
					if len(candPeers) > 1 {
						pickIdx = (targetRow + offset) % len(candPeers)
					}
					if candPeers[pickIdx].PeerID != assigned[0].PeerID {
						assigned = append(assigned, candPeers[pickIdx])
						break
					}
				}
			}
			custodyMap[targetRow] = assigned
			continue
		}

		// len(cellPeers) == 0: borrow up to 2 distinct peers from subsequent active rows
		var assigned []PeerInfo
		used := make(map[string]bool)
		for offset := 1; offset < netRows && len(assigned) < 2; offset++ {
			candRow := (targetRow + offset) % netRows
			for _, p := range grouped[candRow] {
				if !used[p.PeerID] {
					used[p.PeerID] = true
					assigned = append(assigned, p)
					if len(assigned) >= 2 {
						break
					}
				}
			}
		}
		custodyMap[targetRow] = assigned
	}

	return custodyMap
}

// ComputeSeedingPlan returns:
// - totalSeeds: fixed seed pool size S_pool = 2 * kPiece published by Bootstrap onto TopicCellRowSeed
// - seedsPerPeer: random subset size S_node = max(2, ceil(2*kPiece/M)) sampled by each of the M custody peers from TopicCellRowSeed
// - recodesPerPeer: broadcast quota Q = max(1, ceil(kPiece/M)) + 1 recoded pieces each custody peer publishes onto TopicCellRowRecode
func ComputeSeedingPlan(numCustodyPeers int, kPiece int) (totalSeeds int, seedsPerPeer int, recodesPerPeer int) {
	if kPiece <= 0 {
		kPiece = 4
	}
	if numCustodyPeers <= 0 {
		numCustodyPeers = 2
	}
	totalSeeds = 2 * kPiece
	seedsPerPeer = (totalSeeds + numCustodyPeers - 1) / numCustodyPeers
	if seedsPerPeer < 2 {
		seedsPerPeer = 2
	}
	if seedsPerPeer > totalSeeds {
		seedsPerPeer = totalSeeds
	}
	baseQuota := (kPiece + numCustodyPeers - 1) / numCustodyPeers
	if baseQuota < 1 {
		baseQuota = 1
	}
	recodesPerPeer = baseQuota + 1
	return totalSeeds, seedsPerPeer, recodesPerPeer
}

// EvaluateNodeCustodyForRow checks a Store Node's custody role for dataRow given custodyMap:
// - isCustody: true if selfPeerID is assigned in custodyMap[dataRow % netRows]
// - isNativeRetention: true if this node retains all kPiece raw pieces long-term (native cell member, or first borrowed holder when cell is empty)
// - numCustodyPeers: M (number of custody peers assigned to this cell)
func EvaluateNodeCustodyForRow(selfPeerID string, selfNetRow int, dataRow int, netRows int, custodyMap map[int][]PeerInfo) (isCustody bool, isNativeRetention bool, numCustodyPeers int) {
	if netRows <= 0 {
		netRows = 8
	}
	targetNetRow := dataRow % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}

	if len(custodyMap) > 0 {
		assigned := custodyMap[targetNetRow]
		numCustodyPeers = len(assigned)
		for idx, p := range assigned {
			if p.PeerID == selfPeerID {
				isCustody = true
				pNetRow := p.Row % netRows
				if pNetRow == targetNetRow || idx == 0 {
					isNativeRetention = true
				}
				return isCustody, isNativeRetention, numCustodyPeers
			}
		}
		return false, false, numCustodyPeers
	}

	// Fallback if custodyMap is not yet populated from Bootstrap
	normSelfRow := selfNetRow % netRows
	isPrimary := targetNetRow == normSelfRow
	isBackup := ((targetNetRow + 1) % netRows) == normSelfRow
	return isPrimary || isBackup, isPrimary, 2
}

// GetNodeCustodyNetRows returns the list of network rows (0..netRows-1) for which selfPeerID holds custody.
func GetNodeCustodyNetRows(selfPeerID string, selfNetRow int, netRows int, custodyMap map[int][]PeerInfo) []int {
	if netRows <= 0 {
		netRows = 8
	}
	var rows []int
	if len(custodyMap) > 0 {
		for r := 0; r < netRows; r++ {
			for _, p := range custodyMap[r] {
				if p.PeerID == selfPeerID {
					rows = append(rows, r)
					break
				}
			}
		}
		return rows
	}
	normSelfRow := selfNetRow % netRows
	rows = append(rows, normSelfRow)
	prevRow := (normSelfRow - 1 + netRows) % netRows
	if prevRow != normSelfRow {
		rows = append(rows, prevRow)
	}
	return rows
}

// PublisherPushRequest payload for chunk pushing
type PublisherPushRequest struct {
	BlockID      string                  `json:"block_id"`
	ColIdx       int                     `json:"col_idx"`
	ColumnData   []string                `json:"column_data"`   // Hex-encoded data cells
	PieceCommits []string                `json:"piece_commits"` // Hex-encoded piece commitments
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
	Data         string   `json:"data"`                     // Hex coded data d_i
	Coeffs       string   `json:"coeffs"`                   // Hex coefficients g_i
	Proof        string   `json:"proof"`                    // Hex combined proof P_i
	PieceCommits []string `json:"piece_commits"`            // Hex piece commitments C_0..C_k-1
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
	BlockID     string       `json:"block_id"`
	Row         int          `json:"row"`
	Col         int          `json:"col"`
	Recovered   bool         `json:"recovered"`
	Data        string       `json:"data,omitempty"`
	PiecesCount int          `json:"pieces_count"`
	Pieces      []CodedPiece `json:"pieces"`
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
	BlockID          string `json:"block_id"`
	Height           int    `json:"height"`
	PeerID           string `json:"peer_id,omitempty"`
	NetColIdx        int    `json:"net_col_idx"`
	ColIdx           int    `json:"col_idx"`
	RowIdx           int    `json:"row_idx"`
	StoresPerCol     int    `json:"stores_per_col"`
	NetRows          int    `json:"net_rows,omitempty"`
	CustodyNetRows   []int  `json:"custody_net_rows,omitempty"`
	ActivePeersInCol int    `json:"active_peers_in_col,omitempty"`
}

// GossipBlockReadyPayload signals that all active columns are ready and light nodes can start DAS
type GossipBlockReadyPayload struct {
	BlockID string `json:"block_id"`
	Height  int    `json:"height"`
	K       int    `json:"k,omitempty"`
}

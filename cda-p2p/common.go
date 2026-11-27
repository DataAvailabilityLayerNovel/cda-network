package p2pcommon

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
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

// FormatHostMultiaddrPrefix returns "/ip4/<ip>", "/ip6/<ip>", or "/dns4/<hostname>"
// so raw IPv4 addresses are never mistakenly wrapped in /dns4/... across multi-host Docker deployments.
func FormatHostMultiaddrPrefix(hostStr string) string {
	h := strings.TrimSpace(hostStr)
	if h == "" || strings.EqualFold(h, "localhost") {
		return "/ip4/127.0.0.1"
	}
	if ip := net.ParseIP(h); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			return fmt.Sprintf("/ip4/%s", ip4.String())
		}
		return fmt.Sprintf("/ip6/%s", ip.String())
	}
	return fmt.Sprintf("/dns4/%s", h)
}

// BuildAdvertisedP2PAddrs constructs the externally reachable QUIC and TCP libp2p multiaddrs
// for a node given its advertised myAddr (e.g. "http://203.0.113.50:9301" or "http://store-0-1:8080")
// and its local container HTTP port. If myAddr includes an external mapped port (e.g. 9301 while
// localHTTPPort inside the container is 8080), the advertised P2P port automatically uses 9301+10000 = 19301.
func BuildAdvertisedP2PAddrs(myAddr string, localHTTPPort int) []string {
	trimmed := strings.TrimSpace(myAddr)
	hostName := "127.0.0.1"
	advertisedHTTPPort := localHTTPPort

	if strings.HasPrefix(trimmed, "/ip4/") || strings.HasPrefix(trimmed, "/dns4/") {
		parts := strings.Split(trimmed, "/")
		if len(parts) >= 5 {
			hostName = parts[2]
			if pVal, err := strconv.Atoi(parts[4]); err == nil && pVal > 0 {
				p2pPort := pVal
				if p2pPort < 10000 {
					p2pPort += 10000
				}
				prefix := FormatHostMultiaddrPrefix(hostName)
				return []string{
					fmt.Sprintf("%s/udp/%d/quic-v1", prefix, p2pPort),
					fmt.Sprintf("%s/tcp/%d", prefix, p2pPort),
				}
			}
		}
	}

	parsedStr := trimmed
	if parsedStr != "" && !strings.Contains(parsedStr, "://") {
		parsedStr = "http://" + parsedStr
	}
	if u, err := url.Parse(parsedStr); err == nil && u.Hostname() != "" {
		hostName = u.Hostname()
		if pStr := u.Port(); pStr != "" {
			if pVal, err := strconv.Atoi(pStr); err == nil && pVal > 0 {
				advertisedHTTPPort = pVal
			}
		}
	}

	p2pPort := advertisedHTTPPort + 10000
	if advertisedHTTPPort >= 10000 {
		p2pPort = advertisedHTTPPort
	}
	prefix := FormatHostMultiaddrPrefix(hostName)
	var addrs []string

	// If CDA_PUBLIC_IP (or MAIN_HOST_IP / WORKER_PUBLIC_IP) is set in a multi-host Docker setup,
	// advertise the host's external LAN/Public IPv4 first so remote machines can dial this container directly,
	// followed by the internal Docker DNS multiaddr for same-host container communication.
	pubIP := strings.TrimSpace(os.Getenv("CDA_PUBLIC_IP"))
	if pubIP != "" && pubIP != "127.0.0.1" && !strings.EqualFold(pubIP, "localhost") && !strings.EqualFold(pubIP, hostName) {
		pubPrefix := FormatHostMultiaddrPrefix(pubIP)
		addrs = append(addrs,
			fmt.Sprintf("%s/udp/%d/quic-v1", pubPrefix, p2pPort),
			fmt.Sprintf("%s/tcp/%d", pubPrefix, p2pPort),
		)
	}
	addrs = append(addrs,
		fmt.Sprintf("%s/udp/%d/quic-v1", prefix, p2pPort),
		fmt.Sprintf("%s/tcp/%d", prefix, p2pPort),
	)
	return addrs
}

// ResolvePeerMultiaddr converts either an HTTP endpoint ("http://192.168.1.10:9200") or a libp2p
// multiaddr ("/ip4/192.168.1.10/tcp/19200" or "/dns4/bootstrap-0/udp/19200/quic-v1/p2p/...")
// into a valid multiaddr.Multiaddr with "/p2p/<peerID>" appended once.
func ResolvePeerMultiaddr(addrStr string, targetPID peer.ID, preferQUIC bool) (multiaddr.Multiaddr, error) {
	resolved := strings.TrimSpace(addrStr)
	if strings.HasPrefix(resolved, "http://") || strings.HasPrefix(resolved, "https://") {
		u, err := url.Parse(resolved)
		if err == nil && u.Hostname() != "" {
			prefix := FormatHostMultiaddrPrefix(u.Hostname())
			if portVal, err := strconv.Atoi(u.Port()); err == nil && portVal > 0 {
				pPort := portVal
				if pPort < 10000 {
					pPort += 10000
				}
				if preferQUIC {
					resolved = fmt.Sprintf("%s/udp/%d/quic-v1", prefix, pPort)
				} else {
					resolved = fmt.Sprintf("%s/tcp/%d", prefix, pPort)
				}
			}
		}
	} else if strings.HasPrefix(resolved, "/dns4/") {
		// Auto-correct "/dns4/<ipv4>/..." to "/ip4/<ipv4>/..." if an IP literal was placed after /dns4/
		parts := strings.Split(resolved, "/")
		if len(parts) >= 3 {
			if ip := net.ParseIP(parts[2]); ip != nil && ip.To4() != nil {
				parts[1] = "ip4"
				parts[2] = ip.To4().String()
				resolved = strings.Join(parts, "/")
			}
		}
	}
	if !strings.Contains(resolved, "/p2p/") && targetPID != "" {
		resolved = fmt.Sprintf("%s/p2p/%s", strings.TrimSuffix(resolved, "/"), targetPID.String())
	}
	return multiaddr.NewMultiaddr(resolved)
}

// DeriveHTTPAddrFromP2POrHTTP returns an HTTP base URL (e.g. "http://bootstrap-0:9200" or "http://192.168.1.10:9200")
// from either an HTTP URL or a libp2p multiaddr ("/dns4/bootstrap-0/tcp/19200" or "/ip4/192.168.1.10/udp/19200/quic-v1").
func DeriveHTTPAddrFromP2POrHTTP(addrStr string) string {
	s := strings.TrimSpace(addrStr)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return strings.TrimSuffix(s, "/")
	}
	if strings.HasPrefix(s, "/ip4/") || strings.HasPrefix(s, "/dns4/") {
		parts := strings.Split(s, "/")
		if len(parts) >= 5 {
			host := parts[2]
			if portVal, err := strconv.Atoi(parts[4]); err == nil && portVal > 0 {
				httpPort := portVal
				if httpPort > 10000 {
					httpPort -= 10000
				}
				return fmt.Sprintf("http://%s:%d", host, httpPort)
			}
		}
	}
	return ""
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
	TopicHeader          = "/cda/1.0.0/header"
	TopicStoreReady      = "/cda/1.0.0/store-ready"
	TopicBlockReady      = "/cda/1.0.0/block-ready"
	TopicByzantineReport = "/cda/1.0.0/byzantine-report"
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

// PeerInfo wraps Peer ID, Multiaddrs, Matrix Coordinates, and Node Reputation Metadata
type PeerInfo struct {
	PeerID          string   `json:"peer_id"`
	Multiaddrs      []string `json:"multiaddrs"`
	Row             int      `json:"row"`
	Col             int      `json:"col"`
	TrustScore      float64  `json:"trust_score,omitempty"`
	TrustTier       string   `json:"trust_tier,omitempty"`
	BlocksCompleted int      `json:"blocks_completed,omitempty"`
	BlocksMissed    int      `json:"blocks_missed,omitempty"`
	FaultCount      int      `json:"fault_count,omitempty"`
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

// GetActiveColumnStores filters and deduplicates valid non-quarantined Store Node peers (Row >= 0),
// sorted deterministically by (Row, TrustScore DESC, PeerID).
func GetActiveColumnStores(colPeers []PeerInfo) []PeerInfo {
	seen := make(map[string]bool, len(colPeers))
	var active []PeerInfo
	for _, p := range colPeers {
		if p.Row >= 0 && p.PeerID != "" && !seen[p.PeerID] {
			if p.TrustTier == TrustTierQuarantined || (p.TrustTier != "" && EffectiveTrustScore(p) < ThresholdProbation) {
				continue
			}
			if p.TrustScore <= 0 && p.TrustTier == "" {
				p.TrustScore = DefaultInitialTrustScore
				p.TrustTier = TrustTierStandard
			} else if p.TrustTier == "" {
				p.TrustTier = ClassifyTrustTier(p.TrustScore)
			}
			seen[p.PeerID] = true
			active = append(active, p)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		if active[i].Row != active[j].Row {
			return active[i].Row < active[j].Row
		}
		si := EffectiveTrustScore(active[i])
		sj := EffectiveTrustScore(active[j])
		if si != sj {
			return si > sj
		}
		return active[i].PeerID < active[j].PeerID
	})
	return active
}

// ComputeColumnCustodyMap deterministically computes the Custody Store Nodes assigned to each
// network row netRow in [0, netRows-1] for a given column's active store peers, taking into account
// each node's TrustScore and TrustTier:
//  1. If cell (netRow, netCol) has >= 2 store nodes with at least 1 healthy node (score >= 40.0):
//     all M store nodes in that cell form the cell's custody pool (if all M nodes in the cell are in PROBATION,
//     1 healthy node from another row is also borrowed as co-custody safety guardian).
//  2. If cell (netRow, netCol) has == 1 store node: that node is included, and 1 additional healthy store node
//     is borrowed from another active row (prioritizing TRUSTED/STANDARD nodes from surplus cells M >= 2,
//     lowest borrowCount, and highest TrustScore). If the sole native node is in PROBATION (score < 40.0),
//     the borrowed healthy node is promoted to co-Primary retention so the cell never relies solely on a PROBATION node.
//  3. If cell (netRow, netCol) has == 0 store nodes (empty/left): up to 2 distinct healthy store nodes are borrowed.
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

	borrowCount := make(map[string]int, len(active))

	// pickBestBorrowedPeer selects a backup peer not in usedPeerIDs, prioritizing:
	// 1. Healthy eligibility (TrustScore >= 40.0, i.e., TRUSTED or STANDARD over PROBATION).
	// 2. Peers from surplus multi-node cells (len(grouped[candRow]) >= 2) over single-node cells.
	// 3. Peers with the lowest borrowCount so backup load is spread evenly across the column.
	// 4. Peers with the highest TrustScore.
	// 5. Deterministic circular row distance offset = 1..netRows-1 from targetRow.
	pickBestBorrowedPeer := func(targetRow int, usedPeerIDs map[string]bool) (PeerInfo, bool) {
		var bestPeer PeerInfo
		found := false
		bestHealthy := -1
		bestSurplus := -1
		bestBorrows := int(^uint(0) >> 1)
		bestScore := -1.0

		for offset := 1; offset < netRows; offset++ {
			candRow := (targetRow + offset) % netRows
			candPeers := grouped[candRow]
			if len(candPeers) == 0 {
				continue
			}
			surplusTier := 0
			if len(candPeers) >= 2 {
				surplusTier = 1
			}
			startIdx := 0
			if len(candPeers) > 1 {
				startIdx = (targetRow + offset) % len(candPeers)
			}
			for i := 0; i < len(candPeers); i++ {
				cand := candPeers[(startIdx+i)%len(candPeers)]
				if usedPeerIDs[cand.PeerID] {
					continue
				}
				healthyTier := 0
				if IsEligibleForPrimaryCustody(cand) {
					healthyTier = 1
				}
				bCount := borrowCount[cand.PeerID]
				cScore := EffectiveTrustScore(cand)

				better := false
				if !found {
					better = true
				} else if healthyTier != bestHealthy {
					better = healthyTier > bestHealthy
				} else if surplusTier != bestSurplus {
					better = surplusTier > bestSurplus
				} else if bCount != bestBorrows {
					better = bCount < bestBorrows
				} else if cScore != bestScore {
					better = cScore > bestScore
				}

				if better {
					bestPeer = cand
					found = true
					bestHealthy = healthyTier
					bestSurplus = surplusTier
					bestBorrows = bCount
					bestScore = cScore
				}
			}
		}
		return bestPeer, found
	}

	for targetRow := 0; targetRow < netRows; targetRow++ {
		cellPeers := grouped[targetRow]
		if len(cellPeers) >= 2 {
			assigned := make([]PeerInfo, len(cellPeers))
			copy(assigned, cellPeers)
			// Check if at least one native peer in the cell is healthy (score >= 40.0).
			// If all native peers in the cell are in PROBATION, borrow 1 healthy guardian from another row.
			hasHealthyNative := false
			used := make(map[string]bool, len(assigned))
			for _, p := range assigned {
				used[p.PeerID] = true
				if IsEligibleForPrimaryCustody(p) {
					hasHealthyNative = true
				}
			}
			if !hasHealthyNative {
				if borrowed, ok := pickBestBorrowedPeer(targetRow, used); ok && IsEligibleForPrimaryCustody(borrowed) {
					assigned = append([]PeerInfo{borrowed}, assigned...)
					borrowCount[borrowed.PeerID]++
				}
			}
			custodyMap[targetRow] = assigned
			continue
		}

		if len(cellPeers) == 1 {
			assigned := []PeerInfo{cellPeers[0]}
			used := map[string]bool{cellPeers[0].PeerID: true}
			if borrowed, ok := pickBestBorrowedPeer(targetRow, used); ok {
				assigned = append(assigned, borrowed)
				borrowCount[borrowed.PeerID]++
			}
			custodyMap[targetRow] = assigned
			continue
		}

		// len(cellPeers) == 0: borrow up to 2 distinct peers from active rows
		var assigned []PeerInfo
		used := make(map[string]bool)
		for len(assigned) < 2 {
			borrowed, ok := pickBestBorrowedPeer(targetRow, used)
			if !ok {
				break
			}
			used[borrowed.PeerID] = true
			assigned = append(assigned, borrowed)
			borrowCount[borrowed.PeerID]++
		}
		custodyMap[targetRow] = assigned
	}

	return custodyMap
}

// DeterministicCustodyOffset computes a deterministic rotation index for (blockID, targetNetRow, subRowIdx).
// Within a given blockID and targetNetRow, consecutive sub-rows (subRowIdx = dataRow / netRows = 0, 1, 2...)
// increment the offset by +1, guaranteeing strict round-robin striping across all M nodes in the cell.
// Across different blockIDs, the starting offset rotates deterministically via FNV-1a.
func DeterministicCustodyOffset(blockID string, targetNetRow int, subRowIdx int) uint64 {
	if subRowIdx < 0 {
		subRowIdx = 0
	}
	if blockID == "" {
		return uint64(subRowIdx)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(blockID))
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(targetNetRow))
	_, _ = h.Write(buf[:])
	baseOffset := h.Sum64() % 1048576
	return baseOffset + uint64(subRowIdx)
}

// SelectActiveCustodyPairForBlockRow deterministically selects the active (Primary, Backup) custody pair
// for a specific (blockID, dataRow) from custodyMap[dataRow % netRows], factoring in Node Reputation Tiers:
//   - Only healthy peers (TRUSTED or STANDARD, TrustScore >= 40.0) are eligible for Primary Custody when
//     at least one healthy peer exists in the cell's custody pool.
//   - If a cell has >= 2 healthy peers, (Primary, Backup) are striped across the healthy peers, while any
//     PROBATION peer (20.0 <= TrustScore < 40.0) in that cell serves as lightweight Non-Custody (1 piece)
//     and continues earning +2.0/block until it recovers to STANDARD.
//   - If a single-node cell's native peer drops to PROBATION (< 40.0), its healthy borrowed backup peer is
//     promoted to co-retain kPiece pieces alongside it so no cell ever depends solely on a PROBATION node.
func SelectActiveCustodyPairForBlockRow(blockID string, dataRow int, netRows int, custodyMap map[int][]PeerInfo) (primary *PeerInfo, backup *PeerInfo, activeCommitteeSize int) {
	if netRows <= 0 {
		netRows = 8
	}
	targetNetRow := dataRow % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}
	assigned := custodyMap[targetNetRow]
	if len(assigned) == 0 {
		return nil, nil, 0
	}
	if len(assigned) == 1 {
		p0 := assigned[0]
		return &p0, nil, 1
	}

	// Case 1: Single native peer (assigned[0]) + 1 cross-row borrowed backup (assigned[1])
	p0NetRow := assigned[0].Row % netRows
	if p0NetRow < 0 {
		p0NetRow += netRows
	}
	p1NetRow := assigned[1].Row % netRows
	if p1NetRow < 0 {
		p1NetRow += netRows
	}
	if len(assigned) == 2 && p0NetRow == targetNetRow && p1NetRow != targetNetRow {
		p0 := assigned[0]
		p1 := assigned[1]
		return &p0, &p1, 2
	}

	// Case 2: Multi-node cell (M >= 2) or empty cell with 2 borrowed peers.
	// Partition into healthy peers (score >= 40.0) and probation peers (20.0 <= score < 40.0).
	var healthyPool []PeerInfo
	var probationPool []PeerInfo
	for _, p := range assigned {
		if IsEligibleForPrimaryCustody(p) {
			healthyPool = append(healthyPool, p)
		} else {
			probationPool = append(probationPool, p)
		}
	}

	subRowIdx := dataRow / netRows
	if subRowIdx < 0 {
		subRowIdx = 0
	}
	rot := DeterministicCustodyOffset(blockID, targetNetRow, subRowIdx)

	if len(healthyPool) >= 2 {
		primaryIdx := int(rot % uint64(len(healthyPool)))
		backupIdx := (primaryIdx + 1) % len(healthyPool)
		pPrimary := healthyPool[primaryIdx]
		pBackup := healthyPool[backupIdx]
		return &pPrimary, &pBackup, 2
	}
	if len(healthyPool) == 1 && len(probationPool) >= 1 {
		pPrimary := healthyPool[0]
		bIdx := int(rot % uint64(len(probationPool)))
		pBackup := probationPool[bIdx]
		return &pPrimary, &pBackup, 2
	}

	// Fallback if all peers in assigned are in probationPool
	primaryIdx := int(rot % uint64(len(assigned)))
	backupIdx := (primaryIdx + 1) % len(assigned)
	pPrimary := assigned[primaryIdx]
	pBackup := assigned[backupIdx]
	return &pPrimary, &pBackup, 2
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

// EvaluateNodeCustodyForBlockRow checks a Store Node's dynamic custody role for (blockID, dataRow):
//   - isCustody: true if selfPeerID is selected as either the Primary Custody Node or Backup Custody Node for (blockID, dataRow)
//   - isNativeRetention: true if selfPeerID is the Primary Custody Node for (blockID, dataRow), OR if the Primary Custody Node
//     is in PROBATION (< 40.0) and selfPeerID is the healthy Backup guardian co-retaining kPiece pieces.
//   - numCustodyPeers: active custody committee size for this dataRow (2 when Primary + Backup exist, 1 if single node)
func EvaluateNodeCustodyForBlockRow(selfPeerID string, selfNetRow int, blockID string, dataRow int, netRows int, custodyMap map[int][]PeerInfo) (isCustody bool, isNativeRetention bool, numCustodyPeers int) {
	if netRows <= 0 {
		netRows = 8
	}
	targetNetRow := dataRow % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}

	if len(custodyMap) > 0 {
		primary, backup, activeSize := SelectActiveCustodyPairForBlockRow(blockID, dataRow, netRows, custodyMap)
		if activeSize == 0 {
			return false, false, 0
		}
		if primary != nil && primary.PeerID == selfPeerID {
			return true, true, activeSize
		}
		if backup != nil && backup.PeerID == selfPeerID {
			// If the sole native primary peer in a single-node cell is in PROBATION (< 40.0),
			// promote the healthy borrowed backup peer to also retain kPiece pieces (co-Primary safety guardian).
			coRetain := primary != nil && !IsEligibleForPrimaryCustody(*primary) && IsEligibleForPrimaryCustody(*backup)
			return true, coRetain, activeSize
		}
		return false, false, activeSize
	}

	// Fallback if custodyMap is not yet populated from Bootstrap
	normSelfRow := selfNetRow % netRows
	if normSelfRow < 0 {
		normSelfRow += netRows
	}
	isPrimary := targetNetRow == normSelfRow
	isBackup := ((targetNetRow + 1) % netRows) == normSelfRow
	return isPrimary || isBackup, isPrimary, 2
}

// EvaluateNodeCustodyForRow checks a Store Node's custody role for dataRow given custodyMap (using empty blockID offset).
func EvaluateNodeCustodyForRow(selfPeerID string, selfNetRow int, dataRow int, netRows int, custodyMap map[int][]PeerInfo) (isCustody bool, isNativeRetention bool, numCustodyPeers int) {
	return EvaluateNodeCustodyForBlockRow(selfPeerID, selfNetRow, "", dataRow, netRows, custodyMap)
}

// GetOrderedCustodyPeersForBlockRow returns the prioritized custody peers for (blockID, dataRow),
// placing the active Primary Custody Node first, the active Backup Custody Node second, and any remaining
// cell custody peers after them (excluding selfPeerID).
func GetOrderedCustodyPeersForBlockRow(selfPeerID string, blockID string, dataRow int, netRows int, custodyMap map[int][]PeerInfo) []PeerInfo {
	if netRows <= 0 {
		netRows = 8
	}
	targetNetRow := dataRow % netRows
	if targetNetRow < 0 {
		targetNetRow += netRows
	}
	if len(custodyMap) == 0 {
		return nil
	}
	primary, backup, _ := SelectActiveCustodyPairForBlockRow(blockID, dataRow, netRows, custodyMap)
	var res []PeerInfo
	seen := make(map[string]bool)
	if selfPeerID != "" {
		seen[selfPeerID] = true
	}
	if primary != nil && !seen[primary.PeerID] {
		seen[primary.PeerID] = true
		res = append(res, *primary)
	}
	if backup != nil && !seen[backup.PeerID] {
		seen[backup.PeerID] = true
		res = append(res, *backup)
	}
	for _, p := range custodyMap[targetNetRow] {
		if !seen[p.PeerID] {
			seen[p.PeerID] = true
			res = append(res, p)
		}
	}
	return res
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

// ByzantineNodeReport contains self-verifiable cryptographic evidence of a peer distributing an invalid piece.
type ByzantineNodeReport struct {
	BlockID        string   `json:"block_id"`
	Height         int      `json:"height"`
	Row            int      `json:"row"`
	Col            int      `json:"col"`
	ReportedPeerID string   `json:"reported_peer_id"` // Node that served the bad piece
	ReporterPeerID string   `json:"reporter_peer_id"` // Node reporting the violation
	Data           string   `json:"data"`             // Hex-encoded piece data
	Coeffs         string   `json:"coeffs"`           // Hex-encoded piece coefficients
	Proof          string   `json:"proof"`            // Hex-encoded piece proof
	PieceCommits   []string `json:"piece_commits"`    // Hex-encoded piece commitments
	Reason         string   `json:"reason"`           // Reason description
	Timestamp      int64    `json:"timestamp"`
}

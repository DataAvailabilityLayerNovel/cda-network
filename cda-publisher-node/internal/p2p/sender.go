package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	p2pcommon "cda-p2p"

	"github.com/DataAvailabilityLayerNovel/rlnc-rsmt2d/cda"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type Sender struct {
	host host.Host
	disc *Discovery
}

func NewSender(h host.Host, disc *Discovery) *Sender {
	return &Sender{
		host: h,
		disc: disc,
	}
}

// SendColumnChunk transmits column cells, commitments, and Merkle proofs to the bootstrap node over a libp2p stream
func (s *Sender) SendColumnChunk(blockID string, colIdx int, colData [][]byte, pieceCommits [][]byte, merkleProofs []cda.MerkleProof) error {
	addr, err := s.disc.FindBootstrapNode(colIdx)
	if err != nil {
		return fmt.Errorf("resolve bootstrap address for column %d: %w", colIdx, err)
	}

	// Support HTTP address conversion to multiaddr
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		u, err := url.Parse(addr)
		if err == nil {
			hostStr := u.Hostname()
			portStr := u.Port()
			if portVal, err := strconv.Atoi(portStr); err == nil {
				p2pPort := portVal + 10000
				if hostStr == "localhost" || hostStr == "127.0.0.1" {
					addr = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p2pPort)
				} else {
					addr = fmt.Sprintf("/dns4/%s/tcp/%d", hostStr, p2pPort)
				}
			}
		}
	}

	// Append PeerID if not already present
	if !strings.Contains(addr, "/p2p/") && !strings.Contains(addr, "/ipfs/") {
		bootColID := s.disc.GetBootstrapColID(colIdx)
		_, bootPID, err := p2pcommon.GenerateDeterministicKeypair(fmt.Sprintf("cda-bootstrap-%d", bootColID))
		if err != nil {
			return fmt.Errorf("failed to generate bootstrap PeerID: %w", err)
		}
		addr = fmt.Sprintf("%s/p2p/%s", addr, bootPID.String())
	}

	height := p2pcommon.ParseHeightFromBlockID(blockID)
	log.Printf("[P2P] [Height: %d] Connecting to peer at %s to send Column %d", height, addr, colIdx)

	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return fmt.Errorf("invalid bootstrap multiaddr %s: %w", addr, err)
	}

	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer info from multiaddr %s: %w", addr, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := s.host.Connect(ctx, *info); err != nil {
		return fmt.Errorf("failed to connect to bootstrap node %s: %w", info.ID, err)
	}

	log.Printf("[P2P] [Height: %d] Dialing stream %s to %s", height, p2pcommon.ProtoPublisherPush, info.ID)
	stream, err := s.host.NewStream(ctx, info.ID, p2pcommon.ProtoPublisherPush)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	hexColData := make([]string, len(colData))
	for i, d := range colData {
		hexColData[i] = hex.EncodeToString(d)
	}

	hexPieceCommits := make([]string, len(pieceCommits))
	for i, c := range pieceCommits {
		hexPieceCommits[i] = hex.EncodeToString(c)
	}

	serializedProofs := make([]p2pcommon.SerializedMerkleProof, len(merkleProofs))
	for i, p := range merkleProofs {
		serializedProofs[i] = p2pcommon.SerializedMerkleProof{
			Index:    p.Index,
			Siblings: p.Siblings,
		}
	}

	payload := p2pcommon.PublisherPushRequest{
		BlockID:      blockID,
		ColIdx:       colIdx,
		ColumnData:   hexColData,
		PieceCommits: hexPieceCommits,
		MerkleProofs: serializedProofs,
	}

	if err := json.NewEncoder(stream).Encode(payload); err != nil {
		return fmt.Errorf("failed to encode payload to stream: %w", err)
	}

	// Read acknowledgement response
	var response struct {
		Success bool   `json:"success"`
		Error   string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(stream).Decode(&response); err != nil {
		return fmt.Errorf("failed to read response from remote peer: %w", err)
	}

	if !response.Success {
		return fmt.Errorf("remote peer returned error: %s", response.Error)
	}

	log.Printf("[P2P] [Height: %d] Stream finished successfully for Column %d", height, colIdx)
	return nil
}


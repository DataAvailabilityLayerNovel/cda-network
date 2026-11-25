package p2pcommon

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Trust score constants and tier thresholds.
const (
	DefaultInitialTrustScore = 60.0
	MaxTrustScore            = 100.0
	MinTrustScore            = 0.0
	// SoftFaultMinScore is the floor for non-cryptographic (soft/contention) faults.
	// Soft faults can demote a node to PROBATION (25.0) so it is protected by healthy backups,
	// but NEVER drop it into QUARANTINED (<20.0) or abort/disconnect it.
	SoftFaultMinScore = 25.0

	ThresholdTrusted   = 75.0
	ThresholdStandard  = 40.0
	ThresholdProbation = 20.0

	TrustTierTrusted     = "TRUSTED"
	TrustTierStandard    = "STANDARD"
	TrustTierProbation   = "PROBATION"
	TrustTierQuarantined = "QUARANTINED"
)

// Reputation event types.
const (
	RepEventBlockCompleted = "block_completed" // +2.0 (StoreReady / completed custody for block)
	RepEventDASServed      = "das_served"      // +0.5 (Served valid pieces for DAS/Fetch)
	RepEventMissedBlock    = "missed_block"    // -8.0 (Soft fault: missed block after grace window; floor 25.0)
	RepEventFetchTimeout   = "fetch_timeout"   // -3.0 (Soft fault: P2P fetch timeout or empty; floor 25.0)
	RepEventUngracefulDrop = "ungraceful_drop" // -10.0 (Soft fault: crash/ungraceful drop; floor 25.0)
	RepEventByzantineFault = "byzantine_fault" // -> 0.0 QUARANTINED (Cryptographically proven invalid KZG piece)
	RepEventGriefingFault  = "griefing_fault"  // -> 0.0 QUARANTINED (Cryptographically proven false accusation)
	RepEventManualReset    = "manual_reset"    // -> 60.0 STANDARD
)

// Score deltas per event.
const (
	DeltaBlockCompleted = +2.0
	DeltaDASServed      = +0.5
	DeltaMissedBlock    = -8.0
	DeltaFetchTimeout   = -3.0
	DeltaUngracefulDrop = -10.0
)

// ClassifyTrustTier maps a numeric trust score in [0, 100] to its canonical TrustTier.
func ClassifyTrustTier(score float64) string {
	switch {
	case score >= ThresholdTrusted:
		return TrustTierTrusted
	case score >= ThresholdStandard:
		return TrustTierStandard
	case score >= ThresholdProbation:
		return TrustTierProbation
	default:
		return TrustTierQuarantined
	}
}

// EffectiveTrustScore returns a PeerInfo's effective trust score, defaulting to 60.0 (STANDARD)
// when TrustScore and TrustTier are uninitialized.
func EffectiveTrustScore(p PeerInfo) float64 {
	if p.TrustTier == TrustTierQuarantined {
		return 0.0
	}
	if p.TrustScore <= 0 && p.TrustTier == "" {
		return DefaultInitialTrustScore
	}
	return p.TrustScore
}

// IsEligibleForPrimaryCustody returns true if the peer is in TRUSTED or STANDARD tier (score >= 40.0).
func IsEligibleForPrimaryCustody(p PeerInfo) bool {
	if p.TrustTier == TrustTierQuarantined || p.TrustTier == TrustTierProbation {
		return false
	}
	return EffectiveTrustScore(p) >= ThresholdStandard
}

// PeerReputationRecord stores the persistent and runtime reputation state of a Store Node.
type PeerReputationRecord struct {
	PeerID            string  `json:"peer_id"`
	Row               int     `json:"row"`
	Col               int     `json:"col"`
	TrustScore        float64 `json:"trust_score"`
	TrustTier         string  `json:"trust_tier"`
	BlocksCompleted   int     `json:"blocks_completed"`
	BlocksMissed      int     `json:"blocks_missed"`
	DASServedCount    int     `json:"das_served_count"`
	SoftFaultCount    int     `json:"soft_fault_count"`
	ByzantineStrikes  int     `json:"byzantine_strikes"`
	StragglerForgiven int     `json:"straggler_forgiven"`
	LastEvent         string  `json:"last_event,omitempty"`
	LastUpdatedUnix   int64   `json:"last_updated_unix"`
}

// ReputationEngine manages contention-safe node trust scores, straggler forgiveness, and tier classification.
type ReputationEngine struct {
	mu      sync.RWMutex
	records map[string]*PeerReputationRecord
	// blockCompletions tracks blockID -> peerID -> true (deduplicates multiple StoreReady broadcasts per block)
	blockCompletions map[string]map[string]bool
	// blockMissed tracks blockID -> peerID -> true so late stragglers on single-host contention can be forgiven
	blockMissed map[string]map[string]bool
	onSave      func(rec PeerReputationRecord)
}

// NewReputationEngine creates a new ReputationEngine with an optional persistence callback.
func NewReputationEngine(onSave func(rec PeerReputationRecord)) *ReputationEngine {
	return &ReputationEngine{
		records:          make(map[string]*PeerReputationRecord),
		blockCompletions: make(map[string]map[string]bool),
		blockMissed:      make(map[string]map[string]bool),
		onSave:           onSave,
	}
}

func (re *ReputationEngine) getOrCreateLocked(peerID string, row, col int) *PeerReputationRecord {
	rec, ok := re.records[peerID]
	if !ok {
		rec = &PeerReputationRecord{
			PeerID:          peerID,
			Row:             row,
			Col:             col,
			TrustScore:      DefaultInitialTrustScore,
			TrustTier:       ClassifyTrustTier(DefaultInitialTrustScore),
			LastEvent:       "joined",
			LastUpdatedUnix: time.Now().Unix(),
		}
		re.records[peerID] = rec
	} else {
		if row >= 0 {
			rec.Row = row
		}
		if col >= 0 {
			rec.Col = col
		}
	}
	return rec
}

// LoadRecord restores a persisted PeerReputationRecord (e.g. from BadgerDB on startup).
func (re *ReputationEngine) LoadRecord(rec PeerReputationRecord) {
	if rec.PeerID == "" {
		return
	}
	re.mu.Lock()
	defer re.mu.Unlock()
	if rec.TrustScore <= 0 && rec.TrustTier == "" {
		rec.TrustScore = DefaultInitialTrustScore
	}
	rec.TrustTier = ClassifyTrustTier(rec.TrustScore)
	copyRec := rec
	re.records[rec.PeerID] = &copyRec
}

// EnsurePeer initializes a peer at DefaultInitialTrustScore (60.0) if not already tracked,
// and returns an enriched PeerInfo with TrustScore, TrustTier, and counters populated.
func (re *ReputationEngine) EnsurePeer(p PeerInfo) PeerInfo {
	if p.PeerID == "" {
		return p
	}
	re.mu.Lock()
	rec := re.getOrCreateLocked(p.PeerID, p.Row, p.Col)
	p.TrustScore = math.Round(rec.TrustScore*10) / 10
	p.TrustTier = rec.TrustTier
	p.BlocksCompleted = rec.BlocksCompleted
	p.BlocksMissed = rec.BlocksMissed
	p.FaultCount = rec.SoftFaultCount + rec.ByzantineStrikes
	re.mu.Unlock()
	return p
}

// EnrichPeers populates TrustScore, TrustTier, and counters on a slice of PeerInfo.
func (re *ReputationEngine) EnrichPeers(peers []PeerInfo) []PeerInfo {
	if len(peers) == 0 {
		return peers
	}
	out := make([]PeerInfo, len(peers))
	for i, p := range peers {
		out[i] = re.EnsurePeer(p)
	}
	return out
}

// RecordBlockCompleted credits a peer for completing custody of blockID (+2.0, deduplicated per block).
// If the peer was previously marked as having missed blockID due to single-machine CPU/IO contention
// (straggler finishing after the 2/3 BlockReady threshold), the miss penalty (-8.0) is automatically forgiven!
func (re *ReputationEngine) RecordBlockCompleted(peerID string, blockID string, row, col int) PeerReputationRecord {
	re.mu.Lock()
	rec := re.getOrCreateLocked(peerID, row, col)

	if blockID != "" {
		if re.blockCompletions[blockID] == nil {
			re.blockCompletions[blockID] = make(map[string]bool)
		}
		if re.blockCompletions[blockID][peerID] {
			// Already credited for this blockID (StoreNode broadcasts StoreReady 5 times for reliability)
			snap := *rec
			re.mu.Unlock()
			return snap
		}
		re.blockCompletions[blockID][peerID] = true

		// Straggler Forgiveness: if this peer was penalized as a straggler on this block, reverse the penalty!
		if re.blockMissed[blockID] != nil && re.blockMissed[blockID][peerID] {
			delete(re.blockMissed[blockID], peerID)
			if rec.BlocksMissed > 0 {
				rec.BlocksMissed--
			}
			if rec.SoftFaultCount > 0 {
				rec.SoftFaultCount--
			}
			rec.StragglerForgiven++
			rec.TrustScore = math.Min(MaxTrustScore, rec.TrustScore-DeltaMissedBlock)
		}
	}

	if rec.TrustTier != TrustTierQuarantined {
		rec.BlocksCompleted++
		rec.TrustScore = math.Min(MaxTrustScore, rec.TrustScore+DeltaBlockCompleted)
		rec.TrustTier = ClassifyTrustTier(rec.TrustScore)
		rec.LastEvent = RepEventBlockCompleted
		rec.LastUpdatedUnix = time.Now().Unix()
	}

	snap := *rec
	onSave := re.onSave
	re.mu.Unlock()

	if onSave != nil {
		onSave(snap)
	}
	return snap
}

// RecordEvent applies a reputation event (repeated count times) to peerID.
// Soft faults (missed_block, fetch_timeout, ungraceful_drop) are clamped at SoftFaultMinScore (25.0 = PROBATION)
// so single-host resource contention can NEVER quarantine or abort an honest node.
// Cryptographic faults (byzantine_fault, griefing_fault) drop TrustScore to 0.0 (QUARANTINED).
func (re *ReputationEngine) RecordEvent(peerID string, eventType string, blockID string, count int) PeerReputationRecord {
	if count <= 0 {
		count = 1
	}
	if eventType == RepEventBlockCompleted && count == 1 && blockID != "" {
		return re.RecordBlockCompleted(peerID, blockID, -1, -1)
	}

	re.mu.Lock()
	rec := re.getOrCreateLocked(peerID, -1, -1)

	for i := 0; i < count; i++ {
		switch eventType {
		case RepEventBlockCompleted:
			if rec.TrustTier != TrustTierQuarantined {
				rec.BlocksCompleted++
				rec.TrustScore = math.Min(MaxTrustScore, rec.TrustScore+DeltaBlockCompleted)
			}
		case RepEventDASServed:
			if rec.TrustTier != TrustTierQuarantined {
				rec.DASServedCount++
				rec.TrustScore = math.Min(MaxTrustScore, rec.TrustScore+DeltaDASServed)
			}
		case RepEventMissedBlock:
			if blockID != "" {
				if re.blockCompletions[blockID] != nil && re.blockCompletions[blockID][peerID] {
					// Peer already completed this block; do not penalize!
					continue
				}
				if re.blockMissed[blockID] == nil {
					re.blockMissed[blockID] = make(map[string]bool)
				}
				if re.blockMissed[blockID][peerID] {
					continue
				}
				re.blockMissed[blockID][peerID] = true
			}
			if rec.TrustTier != TrustTierQuarantined {
				rec.BlocksMissed++
				rec.SoftFaultCount++
				rec.TrustScore = math.Max(SoftFaultMinScore, rec.TrustScore+DeltaMissedBlock)
			}
		case RepEventFetchTimeout:
			if rec.TrustTier != TrustTierQuarantined {
				rec.SoftFaultCount++
				rec.TrustScore = math.Max(SoftFaultMinScore, rec.TrustScore+DeltaFetchTimeout)
			}
		case RepEventUngracefulDrop:
			if rec.TrustTier != TrustTierQuarantined {
				rec.SoftFaultCount++
				rec.TrustScore = math.Max(SoftFaultMinScore, rec.TrustScore+DeltaUngracefulDrop)
			}
		case RepEventByzantineFault, RepEventGriefingFault:
			rec.ByzantineStrikes++
			rec.TrustScore = MinTrustScore
		case RepEventManualReset:
			rec.TrustScore = DefaultInitialTrustScore
			rec.ByzantineStrikes = 0
		}
	}

	rec.TrustScore = math.Round(rec.TrustScore*10) / 10
	rec.TrustTier = ClassifyTrustTier(rec.TrustScore)
	rec.LastEvent = eventType
	rec.LastUpdatedUnix = time.Now().Unix()
	snap := *rec
	onSave := re.onSave
	re.mu.Unlock()

	if onSave != nil {
		onSave(snap)
	}
	return snap
}

// GetRecord returns the PeerReputationRecord for peerID (or a default 60.0 record if not yet tracked).
func (re *ReputationEngine) GetRecord(peerID string) PeerReputationRecord {
	re.mu.RLock()
	defer re.mu.RUnlock()
	if rec, ok := re.records[peerID]; ok {
		return *rec
	}
	return PeerReputationRecord{
		PeerID:     peerID,
		Row:        -1,
		Col:        -1,
		TrustScore: DefaultInitialTrustScore,
		TrustTier:  TrustTierStandard,
	}
}

// GetAllRecords returns all tracked PeerReputationRecords sorted by (Row, TrustScore DESC, PeerID).
func (re *ReputationEngine) GetAllRecords() []PeerReputationRecord {
	re.mu.RLock()
	defer re.mu.RUnlock()
	out := make([]PeerReputationRecord, 0, len(re.records))
	for _, rec := range re.records {
		out = append(out, *rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Row != out[j].Row {
			return out[i].Row < out[j].Row
		}
		if out[i].TrustScore != out[j].TrustScore {
			return out[i].TrustScore > out[j].TrustScore
		}
		return out[i].PeerID < out[j].PeerID
	})
	return out
}

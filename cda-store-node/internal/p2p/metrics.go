package p2p

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ReconstructDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cda_store_reconstruct_duration_seconds",
		Help:    "Duration of cell reconstruction (RLNC decoding) on store node in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	P2PRequestsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_store_p2p_requests_total",
		Help: "Total P2P store fetch requests processed by the store node.",
	})

	P2PRequestRateGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_store_p2p_request_rate",
		Help: "Instantaneous rate of P2P requests in requests/second.",
	})

	GossipSubMessagesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_gossipsub_messages_propagated_total",
		Help: "Total GossipSub messages propagated by the store node.",
	})

	GossipSubMessageRateGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_gossipsub_message_propagation_rate",
		Help: "Instantaneous rate of GossipSub message propagation in messages/second.",
	})

	ByzantineDetectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cda_store_byzantine_detection_count",
		Help: "Total number of Byzantine/forged pieces detected and blocked by the store node.",
	})

	LinearIndependentPiecesCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_store_linear_independent_pieces_count",
		Help: "Number of linear independent pieces stored in custody.",
	})

	CustodyPiecesCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_store_custody_pieces_count",
		Help: "Number of pieces stored strictly for assigned custody cells.",
	})

	RecodedPiecesCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_store_recoded_pieces_count",
		Help: "Number of pieces retained for non-custody network column cells.",
	})

	DatabaseSizeBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cda_store_db_size_bytes",
		Help: "Total size of the BadgerDB directory in bytes.",
	})
)

var (
	requestsCount int64
	gossipCount   int64
	metricsMu     sync.Mutex
)

func init() {
	prometheus.MustRegister(
		ReconstructDuration,
		P2PRequestsTotal,
		P2PRequestRateGauge,
		GossipSubMessagesTotal,
		GossipSubMessageRateGauge,
		ByzantineDetectionsTotal,
		LinearIndependentPiecesCount,
		CustodyPiecesCount,
		RecodedPiecesCount,
		DatabaseSizeBytes,
	)
}

func StartMetricsTicker(ctx interface{}) {
	c, ok := ctx.(interface{ Done() <-chan struct{} })
	if !ok {
		return
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				metricsMu.Lock()
				reqs := requestsCount
				gossip := gossipCount
				requestsCount = 0
				gossipCount = 0
				metricsMu.Unlock()

				P2PRequestRateGauge.Set(float64(reqs) / 5.0)
				GossipSubMessageRateGauge.Set(float64(gossip) / 5.0)
			case <-c.Done():
				return
			}
		}
	}()
}

func recordP2PRequest() {
	metricsMu.Lock()
	requestsCount++
	metricsMu.Unlock()
	P2PRequestsTotal.Inc()
}

func recordGossipMessage() {
	metricsMu.Lock()
	gossipCount++
	metricsMu.Unlock()
	GossipSubMessagesTotal.Inc()
}
